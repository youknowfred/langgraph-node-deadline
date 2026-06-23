"""Tests for the Hourglass run-wide budget layer.

Budget *math* (deadline_for, reserves, mode ladder, validate) is tested with an
injected fake clock for determinism. The async grant()/poll() integration uses
the real monotonic clock, because those feed the kernel's contextvar deadline.
"""

from __future__ import annotations

import asyncio

import pytest

from langgraph_node_deadline import (
    Hourglass,
    Mode,
    Reserve,
    cooperative_wait_for,
    protected,
)


class FakeClock:
    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def tick(self, d: float) -> None:
        self.t += d


# --------------------------------------------------------------------------- #
# protected() / Reserve                                                        #
# --------------------------------------------------------------------------- #

def test_protected_makes_a_reserve():
    r = protected(135)
    assert isinstance(r, Reserve)
    assert r.secs == 135


# --------------------------------------------------------------------------- #
# validate(): the startup invariant-lock                                       #
# --------------------------------------------------------------------------- #

def test_validate_accepts_a_coherent_envelope_and_chains():
    hg = Hourglass(900, {"synthesis": protected(160), "finalize": protected(135)})
    assert hg.validate() is hg  # returns self for chaining


def test_validate_rejects_reserves_exceeding_total():
    hg = Hourglass(100, {"a": protected(60), "b": protected(60)})
    with pytest.raises(ValueError, match="cannot reserve more than you have"):
        hg.validate()


def test_validate_rejects_nonpositive_reserve():
    with pytest.raises(ValueError, match="must be > 0"):
        Hourglass(100, {"a": protected(0)}).validate()


def test_validate_rejects_nonpositive_total():
    with pytest.raises(ValueError, match="total_secs must be > 0"):
        Hourglass(0).validate()


def test_reserve_accepts_bare_floats():
    hg = Hourglass(900, {"finalize": 135}).validate()
    assert hg._reserve["finalize"].secs == 135.0


def test_validate_rejects_margin_that_starts_run_degraded():
    # reserves (50) + margin (60) >= total (100): NORMAL band is unreachable
    hg = Hourglass(100, {"a": protected(50)}, conserve_margin_secs=60)
    with pytest.raises(ValueError, match="already degraded"):
        hg.validate()


# --------------------------------------------------------------------------- #
# T1.3 — validate() rejects non-finite envelopes (NaN slips through `<= 0`)    #
# --------------------------------------------------------------------------- #

def test_validate_rejects_nan_total():
    # nan <= 0 is False, so without an explicit finiteness check a NaN total
    # would validate and silently degrade the whole run to HALT.
    with pytest.raises(ValueError, match="finite"):
        Hourglass(float("nan")).validate()


def test_validate_rejects_inf_total():
    with pytest.raises(ValueError, match="finite"):
        Hourglass(float("inf")).validate()


def test_validate_rejects_nan_reserve():
    with pytest.raises(ValueError, match="finite"):
        Hourglass(900, {"a": protected(float("nan"))}).validate()


def test_validate_rejects_nan_conserve_margin():
    with pytest.raises(ValueError, match="finite"):
        Hourglass(900, conserve_margin_secs=float("nan")).validate()


def test_mark_completed_releases_reserve_for_standalone_use():
    clk = FakeClock()
    hg = Hourglass(900, {"synthesis": protected(160), "finalize": protected(135)}, clock=clk)
    assert hg.deadline_for("finalize") - clk() == pytest.approx(900 - 160)
    hg.mark_completed("synthesis")  # standalone deadline_for() path
    assert hg.deadline_for("finalize") - clk() == pytest.approx(900)


# --------------------------------------------------------------------------- #
# runway arithmetic                                                            #
# --------------------------------------------------------------------------- #

def test_elapsed_and_remaining_track_the_clock():
    clk = FakeClock()
    hg = Hourglass(900, clock=clk)
    assert hg.elapsed_secs() == 0
    assert hg.remaining_secs() == 900
    clk.tick(300)
    assert hg.elapsed_secs() == 300
    assert hg.remaining_secs() == 600


def test_remaining_floors_at_zero_after_overrun():
    clk = FakeClock()
    hg = Hourglass(100, clock=clk)
    clk.tick(150)
    assert hg.remaining_secs() == 0.0


# --------------------------------------------------------------------------- #
# deadline_for(): the reserve mechanic                                         #
# --------------------------------------------------------------------------- #

def test_non_reserved_phase_cannot_eat_others_reserves():
    clk = FakeClock()
    hg = Hourglass(900, {"synthesis": protected(160), "finalize": protected(135)}, clock=clk)
    # research has no reserve; it may use 900 - (160 + 135) = 605
    assert hg.deadline_for("research") - clk() == pytest.approx(605)


def test_reserved_phase_excludes_its_own_reserve():
    clk = FakeClock()
    hg = Hourglass(900, {"synthesis": protected(160), "finalize": protected(135)}, clock=clk)
    # synthesis only yields to finalize's 135
    assert hg.deadline_for("synthesis") - clk() == pytest.approx(900 - 135)


def test_cap_limits_the_grant():
    clk = FakeClock()
    hg = Hourglass(900, {"finalize": protected(135)}, clock=clk)
    assert hg.deadline_for("research", cap=50) - clk() == pytest.approx(50)


def test_completed_phase_releases_its_reserve():
    clk = FakeClock()
    hg = Hourglass(900, {"synthesis": protected(160), "finalize": protected(135)}, clock=clk)
    # before: finalize must yield to synthesis's 160
    assert hg.deadline_for("finalize") - clk() == pytest.approx(900 - 160)
    with hg.grant("synthesis"):
        pass
    assert "synthesis" in hg._completed
    # after: synthesis done, its reserve released → finalize gets all of it
    assert hg.deadline_for("finalize") - clk() == pytest.approx(900)


# --------------------------------------------------------------------------- #
# grant(): leak-proof completion bookkeeping                                   #
# --------------------------------------------------------------------------- #

def test_grant_records_completion_even_on_exception():
    clk = FakeClock()
    hg = Hourglass(900, {"finalize": protected(135)}, clock=clk)
    with pytest.raises(RuntimeError):
        with hg.grant("research"):
            raise RuntimeError("boom")
    assert "research" in hg._completed


# --------------------------------------------------------------------------- #
# the degradation ladder                                                       #
# --------------------------------------------------------------------------- #

def test_mode_ladder_advances_with_dwindling_runway():
    clk = FakeClock()
    hg = Hourglass(100, {"out": protected(30)}, conserve_margin_secs=20, clock=clk)
    assert hg.mode == Mode.NORMAL          # remaining 100 > 30 + 20
    clk.tick(55)                            # remaining 45 -> 30 < 45 <= 50
    assert hg.mode == Mode.CONSERVE
    clk.tick(20)                            # remaining 25 -> <= 30
    assert hg.mode == Mode.FINISH_ONLY
    clk.tick(30)                            # remaining 0
    assert hg.mode == Mode.HALT


def test_mode_is_forward_only_via_grant_boundary():
    clk = FakeClock()
    hg = Hourglass(100, {"out": protected(30)}, conserve_margin_secs=20, clock=clk)
    clk.tick(80)                            # remaining 20 -> natural FINISH_ONLY
    with hg.grant("work"):                  # the grant boundary ratchets the floor
        pass
    assert hg.mode == Mode.FINISH_ONLY
    clk.t = 1000.0                          # rewind to full runway (must not relax)
    assert hg.mode == Mode.FINISH_ONLY      # forward-only floor holds


def test_reading_mode_is_side_effect_free():
    # Without a grant boundary, reading .mode must NOT ratchet the floor.
    clk = FakeClock()
    hg = Hourglass(100, {"out": protected(30)}, conserve_margin_secs=20, clock=clk)
    clk.tick(80)
    assert hg.mode == Mode.FINISH_ONLY      # reflects the live (natural) mode
    clk.t = 1000.0                          # rewind; no grant happened
    assert hg.mode == Mode.NORMAL           # floor never advanced -> relaxes


def test_mode_excludes_the_active_phase_own_reserve():
    # The bug the review caught: a reserved phase must not read FINISH_ONLY just
    # because its OWN protected floor is the runway it is there to spend.
    clk = FakeClock()
    hg = Hourglass(900, {"finalize": protected(135)}, conserve_margin_secs=50, clock=clk)
    clk.tick(765)                           # remaining == finalize's 135 floor
    assert hg.mode == Mode.FINISH_ONLY      # outside a grant, the floor counts
    with hg.grant("finalize"):              # inside its own grant, it is excluded
        assert hg.mode < Mode.FINISH_ONLY   # holds its floor; sheds nothing


def test_mode_enum_is_ordered_and_stringy():
    assert Mode.NORMAL < Mode.CONSERVE < Mode.FINISH_ONLY < Mode.HALT
    assert Mode.HALT >= Mode.FINISH_ONLY
    assert str(Mode.FINISH_ONLY) == "finish_only"


def test_mode_accepts_string_comparison():
    # The north-star idiom from issue #1: budget.mode >= "finish_only".
    assert Mode.FINISH_ONLY >= "finish_only"
    assert Mode.HALT >= "finish_only"
    assert not (Mode.CONSERVE >= "finish_only")
    assert Mode.NORMAL < "conserve"
    assert Mode.FINISH_ONLY >= "FINISH_ONLY"     # case-insensitive


def test_mode_comparison_with_bogus_string_raises():
    with pytest.raises(TypeError):
        _ = Mode.NORMAL >= "not_a_mode"


# --------------------------------------------------------------------------- #
# async integration (real monotonic clock — these feed the kernel contextvar)  #
# --------------------------------------------------------------------------- #

async def test_grant_clamps_inner_cooperative_wait_for():
    hg = Hourglass(total_secs=1000)
    with hg.grant("x", cap=0.2) as g:
        rem = g.remaining_secs()
        assert rem is not None and rem <= 0.2
        with pytest.raises(asyncio.TimeoutError):
            await cooperative_wait_for(asyncio.sleep(5.0), budget_secs=5.0)


async def test_poll_returns_runway_then_raises_after_deadline():
    hg = Hourglass(total_secs=1000)
    with hg.grant("x", cap=0.05) as g:
        assert (await g.poll()) is not None     # runway left
        await asyncio.sleep(0.1)                 # blow past the 0.05 grant
        with pytest.raises(asyncio.TimeoutError):
            await g.poll()


async def test_deadline_predicate_tracks_the_active_grant():
    hg = Hourglass(total_secs=1000)
    pred = hg.deadline_predicate()
    assert pred() is False  # no active grant -> fail-open
    with hg.grant("x", cap=0.05):
        assert pred() is False
        await asyncio.sleep(0.1)
        assert pred() is True
    assert pred() is False  # scope released
