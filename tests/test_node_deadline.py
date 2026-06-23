"""Behavioral + invariant tests for langgraph-node-deadline.

asyncio_mode = "auto" (set in pyproject) runs ``async def`` tests directly.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from langgraph_node_deadline import (
    aclosing,
    clamp_to_node_deadline,
    cooperative_poll,
    cooperative_wait_for,
    get_node_deadline_remaining_secs,
    node_deadline_exceeded,
    node_deadline_in,
    node_deadline_scope,
)


# --------------------------------------------------------------------------- #
# Fail-open: no scope active                                                   #
# --------------------------------------------------------------------------- #

def test_no_scope_is_fail_open():
    assert get_node_deadline_remaining_secs() is None
    assert node_deadline_exceeded() is False
    # clamp returns the requested budget unchanged
    assert clamp_to_node_deadline(60.0) == 60.0
    assert clamp_to_node_deadline(60.0, reserve_secs=5.0) == 60.0


# --------------------------------------------------------------------------- #
# Core clamp behavior under an active scope                                    #
# --------------------------------------------------------------------------- #

def test_clamp_under_scope_caps_to_remaining():
    with node_deadline_in(10.0):
        clamped = clamp_to_node_deadline(60.0)
        # never more than what's left, never more than the request
        assert 9.0 < clamped <= 10.0


def test_clamp_passes_through_when_budget_is_smaller():
    with node_deadline_in(10.0):
        # a 2s request well under the 10s deadline is unchanged
        assert clamp_to_node_deadline(2.0) == pytest.approx(2.0, abs=0.05)


def test_reserve_carves_headroom():
    with node_deadline_in(10.0):
        clamped = clamp_to_node_deadline(60.0, reserve_secs=2.0)
        assert 7.0 < clamped <= 8.0


def test_remaining_never_negative_and_exceeded_flips():
    # a deadline one second in the past
    with node_deadline_scope(time.monotonic() - 1.0):
        assert get_node_deadline_remaining_secs() == 0.0
        assert node_deadline_exceeded() is True
        assert clamp_to_node_deadline(5.0) == 0.0


# --------------------------------------------------------------------------- #
# Nesting: an inner tighter deadline reverts to the outer one                  #
# --------------------------------------------------------------------------- #

def test_nested_scope_restores_outer():
    with node_deadline_in(10.0):
        assert 9.0 < get_node_deadline_remaining_secs() <= 10.0
        with node_deadline_in(1.0):
            assert get_node_deadline_remaining_secs() <= 1.0
        # back to the outer scope
        assert 8.5 < get_node_deadline_remaining_secs() <= 10.0
    # back to fail-open
    assert get_node_deadline_remaining_secs() is None


# --------------------------------------------------------------------------- #
# The invariant the package exists to guarantee                               #
# --------------------------------------------------------------------------- #

def test_clamp_monotonicity_invariant():
    """clamp(b) is always <= b AND <= remaining, for every budget."""
    with node_deadline_in(5.0):
        remaining = get_node_deadline_remaining_secs()
        assert remaining is not None
        for budget in (0.0, 0.5, 1.0, 4.9, 5.0, 50.0, 5000.0):
            clamped = clamp_to_node_deadline(budget)
            assert clamped <= budget + 1e-9
            assert clamped <= remaining + 1e-9
            assert clamped >= 0.0


# --------------------------------------------------------------------------- #
# Context propagation into child tasks (the whole reason it's a contextvar)    #
# --------------------------------------------------------------------------- #

async def test_scope_propagates_to_child_task():
    async def child_reads_deadline():
        return get_node_deadline_remaining_secs()

    with node_deadline_in(5.0):
        # a task created inside the scope inherits the deadline
        remaining = await asyncio.create_task(child_reads_deadline())
    assert remaining is not None
    assert 4.0 < remaining <= 5.0


async def test_scope_not_visible_to_task_created_outside():
    async def child_reads_deadline():
        # yield once so the parent's scope would be active if it leaked
        await asyncio.sleep(0)
        return get_node_deadline_remaining_secs()

    # task created BEFORE entering the scope must not see it
    task = asyncio.create_task(child_reads_deadline())
    with node_deadline_in(5.0):
        result = await task
    assert result is None


# --------------------------------------------------------------------------- #
# cooperative_wait_for                                                         #
# --------------------------------------------------------------------------- #

async def test_cooperative_wait_for_clamps_to_deadline():
    start = time.monotonic()
    with node_deadline_in(0.2):
        with pytest.raises(asyncio.TimeoutError):
            # asks for 5s but the node only has ~0.2s left
            await cooperative_wait_for(asyncio.sleep(5.0), budget_secs=5.0)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0  # fired at the node deadline, not the 5s request


async def test_cooperative_wait_for_returns_when_work_fits():
    with node_deadline_in(5.0):
        result = await cooperative_wait_for(_quick(), budget_secs=5.0)
    assert result == "done"


async def test_cooperative_wait_for_fail_open_without_scope():
    # no scope: behaves like a plain wait_for(awaitable, budget_secs)
    result = await cooperative_wait_for(_quick(), budget_secs=5.0)
    assert result == "done"


async def _quick():
    await asyncio.sleep(0.01)
    return "done"


async def test_cooperative_wait_for_returns_already_done_result_at_blown_deadline():
    # T2.4: a finished result is salvage, not waste — an already-complete awaitable
    # returns its value even though the deadline has passed; a bare coroutine times out.
    async def instant():
        return "RESULT"

    done = asyncio.ensure_future(instant())
    await asyncio.sleep(0)  # let it complete
    with node_deadline_scope(time.monotonic() - 1.0):  # deadline already blown
        assert await cooperative_wait_for(done, budget_secs=5.0) == "RESULT"
    with node_deadline_scope(time.monotonic() - 1.0):
        with pytest.raises(asyncio.TimeoutError):
            await cooperative_wait_for(asyncio.sleep(5.0), budget_secs=5.0)


# --------------------------------------------------------------------------- #
# cooperative_poll (streaming salvage)                                         #
# --------------------------------------------------------------------------- #

async def _gen(n, delay=0.0):
    for i in range(n):
        if delay:
            await asyncio.sleep(delay)
        yield i


async def test_cooperative_poll_yields_all_within_deadline():
    with node_deadline_in(5.0):
        out = [c async for c in cooperative_poll(_gen(5))]
    assert out == [0, 1, 2, 3, 4]


async def test_cooperative_poll_is_fail_open_without_scope():
    out = [c async for c in cooperative_poll(_gen(4))]
    assert out == [0, 1, 2, 3]


async def test_cooperative_poll_stops_and_salvages_when_deadline_trips():
    out = []
    with node_deadline_in(0.25):
        async for c in cooperative_poll(_gen(100, delay=0.1)):
            out.append(c)
    assert 0 < len(out) < 100  # kept the early chunks, didn't run all 100


async def test_cooperative_poll_closes_underlying_generator_on_early_stop():
    closed = {"v": False}

    async def gen():
        try:
            i = 0
            while True:
                await asyncio.sleep(0.1)
                yield i
                i += 1
        finally:
            closed["v"] = True

    with node_deadline_in(0.25):
        async for _ in cooperative_poll(gen()):
            pass
    assert closed["v"] is True


async def test_cooperative_poll_honors_a_custom_predicate():
    seen = []
    stop = {"v": False}
    async for c in cooperative_poll(_gen(10), predicates=[lambda: stop["v"]]):
        seen.append(c)
        if len(seen) == 3:
            stop["v"] = True  # checked before the next pull -> stops after 3
    assert seen == [0, 1, 2]


# --------------------------------------------------------------------------- #
# T1.1 — custom predicates AUGMENT the deadline guard, never replace it        #
# --------------------------------------------------------------------------- #

async def _infinite(delay):
    i = 0
    while True:
        await asyncio.sleep(delay)
        yield i
        i += 1


async def test_cooperative_poll_custom_predicate_still_honors_deadline():
    """A custom predicate that never trips must not let an active scope be
    ignored — even with bound_each_chunk=False. Before the fix the custom
    predicate REPLACED the deadline guard and this never terminated."""

    async def drain():
        out = []
        with node_deadline_in(0.2):
            async for c in cooperative_poll(
                _infinite(0.01), predicates=[lambda: False], bound_each_chunk=False
            ):
                out.append(c)
        return out

    # The outer timeout turns a regression (infinite loop) into a failure, not a hang.
    out = await asyncio.wait_for(drain(), timeout=2.0)
    assert 0 < len(out) < 100  # stopped at the 0.2s deadline


# --------------------------------------------------------------------------- #
# T1.6 — aclosing(): deterministic teardown on early consumer exit             #
# --------------------------------------------------------------------------- #

async def test_aclosing_closes_upstream_on_consumer_early_exit():
    closed = {"v": False}

    async def upstream():
        try:
            i = 0
            while True:
                await asyncio.sleep(0.01)
                yield i
                i += 1
        finally:
            closed["v"] = True

    seen = []
    async with aclosing(cooperative_poll(upstream())) as stream:
        async for c in stream:
            seen.append(c)
            if len(seen) == 2:
                break  # consumer abandons early — aclosing must still tear down

    assert closed["v"] is True  # deterministic close, no GC needed
    assert seen == [0, 1]


async def test_aclosing_yields_all_then_closes_when_consumed_fully():
    closed = {"v": False}

    async def upstream():
        try:
            for i in range(3):
                yield i
        finally:
            closed["v"] = True

    out = []
    async with aclosing(cooperative_poll(upstream())) as stream:
        async for c in stream:
            out.append(c)

    assert out == [0, 1, 2]
    assert closed["v"] is True


# --------------------------------------------------------------------------- #
# T1.2 / T1.4 — clamp robustness: negative reserve, NaN/inf inputs             #
# --------------------------------------------------------------------------- #

def test_negative_reserve_does_not_exceed_remaining():
    with node_deadline_in(10.0):
        rem = get_node_deadline_remaining_secs()
        assert rem is not None
        # a negative reserve must NOT widen the clamp past the binding deadline
        assert clamp_to_node_deadline(100.0, reserve_secs=-5.0) <= rem + 1e-9
    # fail-open path is unchanged — no scope returns the budget verbatim
    assert clamp_to_node_deadline(60.0, reserve_secs=-5.0) == 60.0


def test_clamp_nan_and_inf_inputs_are_conservative():
    with node_deadline_in(10.0):
        rem = get_node_deadline_remaining_secs()
        assert rem is not None
        # NaN budget collapses to 0 (deterministic, not order-dependent)
        assert clamp_to_node_deadline(float("nan")) == 0.0
        # NaN reserve is ignored (treated as no reserve), never widens the clamp
        assert clamp_to_node_deadline(5.0, reserve_secs=float("nan")) <= rem + 1e-9
        # inf budget clamps DOWN to remaining (not to 0)
        assert 9.0 < clamp_to_node_deadline(float("inf")) <= 10.0
    # fail-open: NaN budget passes through untouched with no scope (forgiving)
    import math as _math

    assert _math.isnan(clamp_to_node_deadline(float("nan")))


# --------------------------------------------------------------------------- #
# T1.5 — cooperative_poll is typed (and its hints resolve on 3.9)              #
# --------------------------------------------------------------------------- #

def test_cooperative_poll_type_hints_resolve():
    import typing

    # A PEP 604 ``X | Y`` annotation here would raise on CPython 3.9; the typed
    # signature must resolve via get_type_hints on every supported version.
    hints = typing.get_type_hints(cooperative_poll)
    assert "aiter" in hints
    assert "return" in hints
