"""langgraph-node-deadline — one binding deadline for every inner timeout.

The problem this solves
-----------------------
A LangGraph node that does real work usually has *several* layers each
re-deriving their own clock: an outer ``TimeoutPolicy`` watchdog, an inner
agent/tool budget, a retry loop, a sub-planner that "wants" 60 seconds. When
those clocks disagree, the inner layers dispatch work the outer watchdog is
guaranteed to kill — and the kill is uncooperative: it cancels the node and
**discards everything**, including any partial result you could have salvaged.
(See the long-standing upstream report: langchain-ai/langgraph#5672, "Run
Cancellation Causes Loss of Streamed State Not Yet Persisted as a Checkpoint".)

The fix
-------
Establish **one** binding deadline at node entry and make every inner timeout
*clamp to it* instead of re-deriving its own. Then your inner calls always
yield at the node boundary — with a few hundred ms of grace — *before* the
watchdog fires, so a ``try/except`` around the inner call actually runs and you
return a complete-but-shorter answer instead of nothing.

How it threads through async code
---------------------------------
The deadline lives in a :class:`contextvars.ContextVar`. ``asyncio`` tasks copy
the ambient context at creation, so a scope opened before you ``await`` is
visible to the agent task *and every subagent task it spawns*. The default is
``None`` (no scope) and every consumer **fails open** — code that runs outside a
scope (unit tests, direct invocations) keeps its existing arithmetic unchanged.

Quick start
-----------
>>> import asyncio
>>> from langgraph_node_deadline import node_deadline_in, cooperative_wait_for
>>> async def node():
...     # this node is allowed ~1.8s of cooperative runtime
...     with node_deadline_in(1.8):
...         try:
...             # the planner "wants" 5s but will be clamped to what's left
...             return await cooperative_wait_for(planner(), budget_secs=5.0)
...         except asyncio.TimeoutError:
...             return salvage_partial()   # runs BEFORE the outer watchdog kills us

See ``examples/salvage_demo.py`` for a runnable, dependency-free contrast
between the naive path (work discarded) and the clamped path (work salvaged).
"""

from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Awaitable, Iterator, Optional, TypeVar

__all__ = [
    # --- v0.1 kernel ---
    "node_deadline",
    "node_deadline_scope",
    "node_deadline_in",
    "get_node_deadline_remaining_secs",
    "node_deadline_exceeded",
    "clamp_to_node_deadline",
    "cooperative_wait_for",
    "cooperative_poll",
    # --- v0.2 hourglass (run-wide budget) ---
    "Hourglass",
    "Grant",
    "Mode",
    "Reserve",
    "protected",
]

__version__ = "0.2.0.dev0"

_T = TypeVar("_T")

_node_deadline_monotonic: ContextVar[Optional[float]] = ContextVar(
    "node_deadline_monotonic", default=None
)


@contextmanager
def node_deadline_scope(deadline_monotonic: Optional[float]) -> Iterator[None]:
    """Scope the binding cooperative deadline, on a ``time.monotonic()`` basis.

    Args:
        deadline_monotonic: An absolute ``time.monotonic()`` timestamp by which
            inner work should have yielded. Pass ``None`` to explicitly clear
            the scope (fail-open) — used when the outer layer has no hard cap.

    The scope is restored on exit, so nesting is safe: an inner, tighter
    deadline reverts to the outer one when its ``with`` block ends.
    """
    token = _node_deadline_monotonic.set(deadline_monotonic)
    try:
        yield
    finally:
        _node_deadline_monotonic.reset(token)


# Primary public alias — reads naturally at call sites that already hold an
# absolute monotonic deadline: ``with node_deadline(executor_deadline): ...``
node_deadline = node_deadline_scope


@contextmanager
def node_deadline_in(seconds: float) -> Iterator[None]:
    """Convenience scope expressed as a *relative* budget from now.

    ``with node_deadline_in(30): ...`` is exactly
    ``with node_deadline_scope(time.monotonic() + 30): ...`` — use it at node
    entry when you think in "this node gets N seconds" rather than in absolute
    monotonic timestamps.
    """
    with node_deadline_scope(time.monotonic() + seconds):
        yield


def get_node_deadline_remaining_secs() -> Optional[float]:
    """Seconds until the binding deadline, or ``None`` when no scope is active.

    Never negative — a passed deadline reports ``0.0``.
    """
    deadline = _node_deadline_monotonic.get()
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def node_deadline_exceeded() -> bool:
    """``True`` only when a scope is active *and* its deadline has passed.

    Returns ``False`` when no scope is active (fail-open), so it is safe to use
    as a cooperative loop guard: ``while not node_deadline_exceeded(): ...``.
    """
    remaining = get_node_deadline_remaining_secs()
    return remaining is not None and remaining <= 0.0


def clamp_to_node_deadline(budget_secs: float, *, reserve_secs: float = 0.0) -> float:
    """Clamp a proposed budget/timeout to the real deadline remaining.

    This is the core primitive: every inner layer that is about to start a
    timed operation passes its desired budget through here, so it can never
    exceed the binding node deadline.

    Args:
        budget_secs: The timeout the inner layer *wants*.
        reserve_secs: Headroom to carve below the deadline (e.g. a phase
            transition / finalize buffer) so the clamped work still has time to
            wrap up before the cooperative cancel.

    Returns:
        ``budget_secs`` unchanged when no deadline scope is active (fail-open);
        otherwise ``min(budget_secs, remaining - reserve_secs)``, floored at
        ``0.0`` so it is always a valid ``asyncio.wait_for`` timeout.
    """
    remaining = get_node_deadline_remaining_secs()
    if remaining is None:
        return budget_secs
    return max(0.0, min(budget_secs, remaining - reserve_secs))


async def cooperative_wait_for(
    awaitable: Awaitable[_T],
    budget_secs: float,
    *,
    reserve_secs: float = 0.0,
) -> _T:
    """``asyncio.wait_for`` that never outlasts the binding node deadline.

    Equivalent to ``asyncio.wait_for(awaitable, clamp_to_node_deadline(...))``.
    Because the clamped timeout fires *inside* the node, an
    ``asyncio.TimeoutError`` you catch here runs your salvage path **before**
    an outer watchdog can cancel the node and discard the work.

    With no active deadline scope this is a plain ``wait_for(awaitable,
    budget_secs)`` — fail-open, no behavior change.

    Raises:
        asyncio.TimeoutError: if the clamped budget elapses first.
    """
    timeout = clamp_to_node_deadline(budget_secs, reserve_secs=reserve_secs)
    return await asyncio.wait_for(awaitable, timeout)


async def cooperative_poll(aiter, *, predicates=None, bound_each_chunk=True):
    """Stream an async iterable, stopping cleanly when the deadline (or any
    predicate) trips — so the consumer keeps whatever it accumulated.

    The streaming sibling of :func:`cooperative_wait_for`. Wrap an
    ``agent.astream(...)`` (or any async iterator) and iterate it::

        sections = []
        with node_deadline_in(120):
            async for chunk in cooperative_poll(agent.astream(state)):
                sections.append(chunk)        # whatever arrives before the deadline
        return assemble(sections)             # a complete-but-shorter answer

    Before each chunk it checks the predicates; if any returns ``True`` it stops,
    closing the underlying iterator. With ``bound_each_chunk`` (default), each
    pull is *also* clamped to the remaining node-deadline, so a single slow chunk
    can't overrun. Stopping is **silent** — the ``async for`` just ends and your
    accumulated chunks are the salvaged partial result; it never raises
    ``TimeoutError`` at the consumer.

    Args:
        aiter: Any async iterable / async generator (e.g. an ``astream``).
        predicates: No-arg callables; iteration stops when any returns ``True``.
            Defaults to ``[node_deadline_exceeded]`` — so with no active scope it
            is fail-open and yields everything.
        bound_each_chunk: Clamp each chunk pull to the remaining node-deadline.
    """
    preds = list(predicates) if predicates is not None else [node_deadline_exceeded]
    it = aiter.__aiter__()
    try:
        while True:
            if any(p() for p in preds):
                break
            remaining = (
                get_node_deadline_remaining_secs() if bound_each_chunk else None
            )
            try:
                if remaining is not None:
                    chunk = await asyncio.wait_for(it.__anext__(), remaining)
                else:
                    chunk = await it.__anext__()
            except StopAsyncIteration:
                break
            except asyncio.TimeoutError:
                break  # ran out of runway mid-chunk → salvage what we have
            yield chunk
    finally:
        aclose = getattr(it, "aclose", None)
        if aclose is not None:
            try:
                await aclose()  # best-effort close so the upstream stream tears down
            except Exception:
                pass


# The run-wide budget layer builds on the kernel above. Imported at the end so
# the kernel names exist before hourglass binds them (no circular-import hazard).
from .hourglass import Grant, Hourglass, Mode, Reserve, protected  # noqa: E402
