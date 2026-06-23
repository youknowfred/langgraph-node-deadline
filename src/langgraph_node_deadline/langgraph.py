"""Optional LangGraph integration sugar — import only if you use LangGraph.

This module imports ``langgraph`` **lazily**; it is *not* a runtime dependency of
the core package. Install the integration with the optional extra::

    pip install "langgraph-node-deadline[langgraph]"

It threads an :class:`~langgraph_node_deadline.Hourglass` through LangGraph's
``runtime.context`` (the zero-magic hook LangGraph already provides) and opens a
:meth:`~langgraph_node_deadline.Hourglass.grant` around a node — and derives the
node's watchdog from your cap so you never pin them equal (the "equal timeouts
lose" trap). Compile your graph with the context schema and pass the budget in::

    from langgraph.graph import StateGraph, START, END
    from langgraph_node_deadline import Hourglass, protected
    from langgraph_node_deadline.langgraph import DeadlineContext, with_grant, add_budgeted_node

    @with_grant("research", cap=400)
    async def research_node(state):
        ...

    g = StateGraph(State, context_schema=DeadlineContext)
    g.add_node("research", research_node)                 # or: add_budgeted_node(g, "research", research_node, cap=400)
    ...
    app = g.compile()
    await app.ainvoke(state, context=DeadlineContext(budget=Hourglass(900, {"finalize": protected(135)})))

Everything here is **fail-open**: outside a graph run (a unit test, langgraph not
installed, or no budget on the context) the decorated node runs exactly as written.
"""
from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, Type, TypeVar

from . import Hourglass, recommended_watchdog_secs

_T = TypeVar("_T")
_AsyncNode = Callable[..., Awaitable[_T]]


@dataclass
class DeadlineContext:
    """Default LangGraph ``context_schema`` carrying an :class:`Hourglass`.

    Compile with ``StateGraph(State, context_schema=DeadlineContext)`` and invoke
    with ``app.ainvoke(state, context=DeadlineContext(budget=hg))``. Bring your own
    schema instead if you like — any context object with a ``budget`` attribute (or
    a custom ``budget_attr``) works.
    """

    budget: Optional[Hourglass] = None


def _budget_from_runtime(
    context_schema: Type[Any], budget_attr: str
) -> Optional[Hourglass]:
    """Read the :class:`Hourglass` off the active LangGraph runtime context.

    Returns ``None`` (fail-open) if langgraph is absent, we're outside a graph run,
    or the context carries no budget.
    """
    try:
        from langgraph.runtime import (  # type: ignore[import-not-found, unused-ignore]
            get_runtime,
        )
    except ImportError:
        return None
    try:
        runtime = get_runtime(context_schema)
    except RuntimeError:
        # get_runtime raises outside a graph run — fail open.
        return None
    ctx = getattr(runtime, "context", None)
    budget = getattr(ctx, budget_attr, None) if ctx is not None else None
    return budget if isinstance(budget, Hourglass) else None


def with_grant(
    phase: str,
    *,
    cap: Optional[float] = None,
    context_schema: Type[Any] = DeadlineContext,
    budget_attr: str = "budget",
) -> Callable[[_AsyncNode[_T]], _AsyncNode[_T]]:
    """Decorate an async LangGraph node so it runs inside ``budget.grant(phase)``.

    The decorated node reads an :class:`Hourglass` off the LangGraph
    ``runtime.context`` and opens a phase grant around its body, so every inner
    ``cooperative_wait_for`` / ``clamp_to_node_deadline`` and subagent task inherits
    the phase deadline. **Fail-open:** with no runtime or no budget, the node runs
    unchanged — so the same function still works in a plain unit test.

    Args:
        phase: The Hourglass phase name for this node.
        cap: Optional per-phase cap passed through to ``grant``.
        context_schema: The LangGraph ``context_schema`` to read (default
            :class:`DeadlineContext`).
        budget_attr: The attribute on the context object holding the Hourglass.
    """

    def deco(fn: _AsyncNode[_T]) -> _AsyncNode[_T]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> _T:
            budget = _budget_from_runtime(context_schema, budget_attr)
            if budget is None:
                return await fn(*args, **kwargs)
            with budget.grant(phase, cap=cap):
                return await fn(*args, **kwargs)

        return wrapper

    return deco


def add_budgeted_node(
    builder: Any,
    name: str,
    fn: _AsyncNode[Any],
    *,
    cap: float,
    phase: Optional[str] = None,
    grace: float = 1.0,
    **kwargs: Any,
) -> Any:
    """``builder.add_node`` that wires the whole cooperative pattern in one call.

    Wraps ``fn`` with :func:`with_grant` (``phase`` defaults to ``name``) *and* sets
    the node's LangGraph watchdog to ``recommended_watchdog_secs(cap, grace)`` —
    i.e. ``cap + grace`` — so the cooperative deadline always fires before the
    watchdog kills the node. Equivalent to::

        builder.add_node(
            name,
            with_grant(name, cap=cap)(fn),
            timeout=recommended_watchdog_secs(cap, grace_secs=grace),
        )
    """
    wrapped = with_grant(phase or name, cap=cap)(fn)
    watchdog = recommended_watchdog_secs(cap, grace_secs=grace)
    return builder.add_node(name, wrapped, timeout=watchdog, **kwargs)
