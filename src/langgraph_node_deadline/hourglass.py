"""hourglass — a run-wide deadline OS built on the node-deadline kernel.

`langgraph_node_deadline` (the kernel) solves ONE node: clamp inner timeouts to
a single binding deadline so work salvages instead of getting hard-killed.

`Hourglass` is the orchestration layer on top: a run-wide time budget split
across a multi-node graph, a *protected output reserve* so the finalize/synthesis
phases always have runway, and a forward-only *degradation ladder* nodes can read
to shed optional work. It never re-implements deadline arithmetic — every
`grant()` opens a kernel `node_deadline` scope, so the binding deadline still
propagates into every subagent task for free.

The shape of a run::

    from langgraph_node_deadline import Hourglass, protected, Mode

    budget = Hourglass(
        total_secs=900,
        reserve={"synthesis": protected(160), "finalize": protected(135)},
    ).validate()                                   # locks the ladder at startup

    with budget.grant("research", cap=400) as g:   # leak-proof; sync `with`
        if budget.mode >= Mode.FINISH_ONLY:        # ("finish_only" also works)
            skip_optional_enrichment()
        await g.poll()                             # cooperative cancel point

A non-reserved phase ("research") can use everything *except* the runway still
owed to phases that haven't completed yet ("synthesis", "finalize") — so it can
never starve the output. A reserve is released once its phase completes, so the
last phase gets whatever is left.

Phases are modelled as running **one at a time**. ``grant()`` / ``deadline_for``
each see the full remaining runway; they do not partition a budget across
*concurrent* phases (two grants launched under ``asyncio.gather`` would each be
handed the whole remaining runway). Per-task contextvar isolation still holds,
but the reserve accounting assumes sequential phases.

NOTE: ``clock`` is injectable for deterministic tests of the budget *math*; the
``grant()`` context manager and the kernel's cooperative helpers assume the
default ``time.monotonic`` clock in production (see ``deadline_for``).
"""

from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import IntEnum
from typing import Callable, Dict, Iterator, Mapping, Optional, Union

from . import (
    get_node_deadline_remaining_secs,
    node_deadline_exceeded,
    node_deadline_scope,
)

__all__ = ["Hourglass", "Grant", "Mode", "Reserve", "protected"]


class Mode(IntEnum):
    """The degradation ladder, ordered by severity.

    Compare with ``>=`` against either a member or its name::

        if budget.mode >= Mode.FINISH_ONLY: ...
        if budget.mode >= "finish_only":    ...   # equivalent (case-insensitive)

    Both are the idiom for "we're short on runway, drop anything optional."
    """

    NORMAL = 0
    CONSERVE = 1
    FINISH_ONLY = 2
    HALT = 3

    @classmethod
    def coerce(cls, value: Union["Mode", str, int]) -> "Mode":
        """Accept a Mode, its (case-insensitive) name, or its int rank."""
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls[value.strip().upper()]
            except KeyError:
                raise ValueError(f"unknown Mode {value!r}") from None
        return cls(value)

    # Ordering coerces the other operand so the string idiom above works. We do
    # NOT override __eq__/__hash__ (so `Mode.X == 2` stays true and members stay
    # hashable); only the ordering comparisons accept names.
    def __ge__(self, other: object) -> bool:
        try:
            return int(self) >= int(Mode.coerce(other))  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return NotImplemented

    def __gt__(self, other: object) -> bool:
        try:
            return int(self) > int(Mode.coerce(other))  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return NotImplemented

    def __le__(self, other: object) -> bool:
        try:
            return int(self) <= int(Mode.coerce(other))  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return NotImplemented

    def __lt__(self, other: object) -> bool:
        try:
            return int(self) < int(Mode.coerce(other))  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return NotImplemented

    def __str__(self) -> str:  # nice for logging
        return self.name.lower()


@dataclass(frozen=True)
class Reserve:
    """A protected runway floor for a phase. Earlier phases cannot borrow it."""

    secs: float


def protected(secs: float) -> Reserve:
    """Sugar: ``reserve={"finalize": protected(135)}``."""
    return Reserve(secs)


def _coerce_reserve(value: Union[Reserve, float, int]) -> Reserve:
    return value if isinstance(value, Reserve) else Reserve(float(value))


class Grant:
    """The handle yielded by :meth:`Hourglass.grant`.

    Exposes the runway left for the phase and a cooperative ``poll()`` checkpoint.
    """

    def __init__(self, hourglass: "Hourglass", phase: str, deadline_monotonic: float):
        self.phase = phase
        self.deadline_monotonic = deadline_monotonic
        self._hourglass = hourglass

    def remaining_secs(self) -> Optional[float]:
        """Seconds left for this phase (from the binding node deadline)."""
        return get_node_deadline_remaining_secs()

    async def poll(self) -> Optional[float]:
        """Yield to the event loop and bail if the phase deadline has passed.

        Use inside an inner loop as a cooperative cancel point. ``await
        asyncio.sleep(0)`` first delivers any queued cancellation; then, once the
        binding deadline is exceeded, raises ``asyncio.TimeoutError`` so a
        ``try/except`` around the phase runs your salvage path *before* an outer
        watchdog can discard the work. Returns the remaining seconds otherwise.
        """
        await asyncio.sleep(0)  # cancellation point
        if node_deadline_exceeded():
            raise asyncio.TimeoutError(
                f"node deadline for phase '{self.phase}' exceeded"
            )
        return get_node_deadline_remaining_secs()


class Hourglass:
    """A run-wide time budget partitioned across a multi-node graph.

    Args:
        total_secs: Total cooperative runtime for the whole run.
        reserve: Map of ``phase -> protected(secs)`` (or a bare float). These
            floors are runway that earlier, non-reserved phases cannot consume.
        conserve_margin_secs: How much runway *above* the pending reserves still
            counts as "getting tight" (``CONSERVE``). Defaults to 20% of the
            *slack* above the reserves (``total_secs - total_reserved``), so a
            heavily-reserved envelope does not start the run already degraded.
        clock: Monotonic time source. Override only in tests — see the note on
            :meth:`deadline_for`.
    """

    def __init__(
        self,
        total_secs: float,
        reserve: Optional[Mapping[str, Union[Reserve, float, int]]] = None,
        *,
        conserve_margin_secs: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.total_secs = float(total_secs)
        self._reserve: Dict[str, Reserve] = {
            k: _coerce_reserve(v) for k, v in (reserve or {}).items()
        }
        total_reserved = sum(r.secs for r in self._reserve.values())
        self._conserve_margin = (
            float(conserve_margin_secs)
            if conserve_margin_secs is not None
            else 0.2 * max(0.0, self.total_secs - total_reserved)
        )
        self._clock = clock
        self._start = clock()
        self._completed: set[str] = set()
        self._active_phase: Optional[str] = None
        self._mode_floor = Mode.NORMAL  # forward-only; advanced at grant boundaries

    # -- validation ------------------------------------------------------- #

    def validate(self) -> "Hourglass":
        """Fail loud at startup on an incoherent envelope. Returns self for chaining.

        Raises ``ValueError`` if the total is non-positive, any reserve is
        non-positive, the reserves sum past the total (you've promised runway you
        don't have), or the reserves plus the conserve margin leave no ``NORMAL``
        band at all (the run would start already degraded). Catching these at
        startup turns a budget misconfiguration into a crash, not a production
        cascade.
        """
        if self.total_secs <= 0:
            raise ValueError(f"total_secs must be > 0, got {self.total_secs}")
        if self._conserve_margin < 0:
            raise ValueError("conserve_margin_secs must be >= 0")
        total_reserved = 0.0
        for phase, r in self._reserve.items():
            if r.secs <= 0:
                raise ValueError(f"reserve for '{phase}' must be > 0, got {r.secs}")
            total_reserved += r.secs
        if total_reserved > self.total_secs:
            raise ValueError(
                f"reserves total {total_reserved}s but total_secs is "
                f"{self.total_secs}s — you cannot reserve more than you have"
            )
        if total_reserved + self._conserve_margin >= self.total_secs:
            raise ValueError(
                f"reserves ({total_reserved}s) + conserve margin "
                f"({self._conserve_margin}s) >= total ({self.total_secs}s): the "
                f"run would start already degraded (NORMAL unreachable). Lower "
                f"the reserves or the conserve_margin_secs."
            )
        return self

    # -- runway arithmetic ------------------------------------------------ #

    def elapsed_secs(self) -> float:
        return self._clock() - self._start

    def remaining_secs(self) -> float:
        return max(0.0, self.total_secs - self.elapsed_secs())

    def _pending_reserve(self, exclude: Optional[str] = None) -> float:
        """Reserve owed to phases that have not completed via grant()/mark_completed()
        and are not the one excluded (the currently-active or being-granted phase)."""
        return sum(
            r.secs
            for phase, r in self._reserve.items()
            if phase != exclude and phase not in self._completed
        )

    def deadline_for(self, phase: str, *, cap: Optional[float] = None) -> float:
        """Absolute ``time.monotonic()`` deadline this phase may run to.

        The phase gets the remaining runway minus the reserves owed to *other*
        phases that have not completed via :meth:`grant` / :meth:`mark_completed`,
        optionally capped.

        Two caveats when calling this directly instead of via :meth:`grant`:

        * **You must call** :meth:`mark_completed` (or run the phase through
          :meth:`grant`) when the phase finishes, or its reserve is never released
          and every later phase is starved by that amount.
        * The returned timestamp is in the injected ``clock`` frame. Only feed it
          to the kernel's ``node_deadline(...)`` when ``clock`` is the default
          ``time.monotonic``; an injected test clock makes the budget math
          deterministic but is **not** compatible with the live kernel scope.
        """
        now = self._clock()
        remaining = max(0.0, self._start + self.total_secs - now)
        available = max(0.0, remaining - self._pending_reserve(exclude=phase))
        if cap is not None:
            available = min(available, cap)
        return now + available

    def mark_completed(self, phase: str) -> None:
        """Release a phase's reserve. :meth:`grant` calls this for you on exit;
        call it yourself only if you drove a reserved phase via
        :meth:`deadline_for` instead of :meth:`grant`."""
        self._completed.add(phase)
        self._ratchet()

    # -- the degradation ladder ------------------------------------------- #

    def _natural_mode(self) -> Mode:
        """The mode implied by the current runway, ignoring the forward-only
        floor. Excludes the active phase's own reserve (it is spending it)."""
        remaining = self.remaining_secs()
        pending = self._pending_reserve(exclude=self._active_phase)
        if remaining <= 0:
            return Mode.HALT
        if remaining <= pending:
            return Mode.FINISH_ONLY
        if remaining <= pending + self._conserve_margin:
            return Mode.CONSERVE
        return Mode.NORMAL

    def _ratchet(self) -> None:
        natural = self._natural_mode()
        if natural > self._mode_floor:
            self._mode_floor = natural

    @property
    def mode(self) -> Mode:
        """Current degradation mode — the more degraded of the forward-only floor
        and the live natural mode.

        Forward-only on purpose: a transient slow phase must not bounce the run
        back to ``NORMAL`` and re-arm the overrun it just escaped. The floor is
        advanced at :meth:`grant` boundaries (and :meth:`mark_completed`), so
        **reading this property is side-effect-free** — safe to log or branch on
        without ratcheting the run.
        """
        return Mode(max(int(self._mode_floor), int(self._natural_mode())))

    def deadline_predicate(self) -> Callable[[], bool]:
        """Return a no-arg predicate that is ``True`` once the *currently active*
        node-deadline scope has passed — typically the scope opened by an
        enclosing :meth:`grant`.

        It reads the ambient kernel contextvar, so it is not bound to this
        Hourglass or a specific phase; outside any scope it is ``False``
        (fail-open). Intended for feeding a cooperative streaming loop from
        inside a grant.
        """
        return node_deadline_exceeded

    # -- the leak-proof grant --------------------------------------------- #

    @contextmanager
    def grant(self, phase: str, cap: Optional[float] = None) -> Iterator[Grant]:
        """Run a phase under a binding deadline derived from the run budget.

        A **sync** context manager (use ``with``, not ``async with``; awaiting
        inside the body is fine). It opens a kernel ``node_deadline`` scope for
        the phase's deadline — so every inner ``clamp_to_node_deadline`` /
        ``cooperative_wait_for`` and every subagent task inherits it — and records
        the phase as completed on exit, **even if the body raises or is
        cancelled**, so its reserve is released to later phases.
        """
        deadline = self.deadline_for(phase, cap=cap)
        self._active_phase = phase
        self._ratchet()  # advance the floor on entry
        try:
            with node_deadline_scope(deadline):
                yield Grant(self, phase, deadline)
        finally:
            self._active_phase = None
            self._completed.add(phase)
            self._ratchet()  # runway has shrunk; advance again on exit
