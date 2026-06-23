"""Property-based tests (Hypothesis) for the load-bearing invariants.

Issue #1 promised the budget machinery would ship with property tests so that
"any value edit must keep green." These prove, over thousands of generated inputs,
the three guarantees the package's correctness rests on:

  1. clamp monotonicity — the clamp can never widen past the deadline, go negative,
     or return NaN, for *any* float inputs (including NaN/inf).
  2. the Hourglass reserve floor — a phase is never granted more than the runway,
     and never eats reserves owed to other unfinished phases.
  3. the Mode ladder is forward-only — once the floor ratchets up it never relaxes.
"""
from __future__ import annotations

import math

from hypothesis import assume, given
from hypothesis import strategies as st

import time

from langgraph_node_deadline import (
    Hourglass,
    clamp_to_node_deadline,
    get_node_deadline_remaining_secs,
    node_deadline_scope,
    protected,
)

any_float = st.floats(allow_nan=True, allow_infinity=True)
pos_runway = st.floats(min_value=1e-3, max_value=1e7, allow_nan=False, allow_infinity=False)


# --------------------------------------------------------------------------- #
# 1. clamp monotonicity                                                        #
# --------------------------------------------------------------------------- #

@given(budget=any_float, reserve=any_float, runway=pos_runway)
def test_clamp_never_widens_or_breaks(budget, reserve, runway):
    with node_deadline_scope(time.monotonic() + runway):
        # read remaining FIRST so the clamp's own (slightly later, smaller) remaining
        # can never appear to exceed it by the elapsed delta.
        rem = get_node_deadline_remaining_secs()
        out = clamp_to_node_deadline(budget, reserve_secs=reserve)
        assert rem is not None
        assert not math.isnan(out)          # never NaN
        assert out >= 0.0                   # never negative
        assert out <= rem + 1e-9            # never widens past the binding deadline
        if math.isfinite(budget) and budget >= 0.0:
            assert out <= budget + 1e-9     # never exceeds a sane (>=0, finite) request


@given(budget=any_float, reserve=any_float)
def test_clamp_is_fail_open_without_scope(budget, reserve):
    # No scope: the requested budget passes through verbatim (fail-open), for any input.
    out = clamp_to_node_deadline(budget, reserve_secs=reserve)
    if math.isnan(budget):
        assert math.isnan(out)
    else:
        assert out == budget


# --------------------------------------------------------------------------- #
# 2. Hourglass reserve floor                                                   #
# --------------------------------------------------------------------------- #

phase_names = st.text(alphabet="abcdefghijklmnop", min_size=1, max_size=4)


@st.composite
def coherent_hourglass(draw):
    total = draw(st.floats(min_value=100, max_value=10_000, allow_nan=False, allow_infinity=False))
    names = draw(st.lists(phase_names, min_size=0, max_size=4, unique=True))
    reserves = {n: draw(st.floats(min_value=1, max_value=300, allow_nan=False, allow_infinity=False)) for n in names}
    assume(sum(reserves.values()) < total * 0.5)  # stay coherent
    return Hourglass(total, {n: protected(s) for n, s in reserves.items()}, clock=lambda: 0.0)


@given(coherent_hourglass())
def test_deadline_for_stays_within_runway_and_honors_others(hg):
    now = hg._clock()
    remaining = hg.remaining_secs()
    candidates = list(hg._reserve) + ["__unreserved_phase__"]
    for phase in candidates:
        avail = hg.deadline_for(phase) - now
        assert 0.0 <= avail <= remaining + 1e-9                 # never grants past the runway
        others = sum(r.secs for p, r in hg._reserve.items() if p != phase)
        assert avail <= remaining - others + 1e-9              # never eats others' reserves


@given(coherent_hourglass())
def test_coherent_envelope_always_validates(hg):
    assert hg.validate() is hg


@given(
    total=st.floats(min_value=10, max_value=1000, allow_nan=False, allow_infinity=False),
    a=st.floats(min_value=1, max_value=1000, allow_nan=False, allow_infinity=False),
    b=st.floats(min_value=1, max_value=1000, allow_nan=False, allow_infinity=False),
)
def test_overcommitted_reserves_always_fail_validation(total, a, b):
    assume(a + b > total)  # promised more runway than exists
    try:
        Hourglass(total, {"a": protected(a), "b": protected(b)}).validate()
        raised = False
    except ValueError:
        raised = True
    assert raised


# --------------------------------------------------------------------------- #
# 3. Mode ladder is forward-only                                               #
# --------------------------------------------------------------------------- #

class _Clock:
    def __init__(self, t=500.0):
        self.t = t

    def __call__(self):
        return self.t


@given(
    deltas=st.lists(
        st.floats(min_value=-200, max_value=200, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=25,
    )
)
def test_mode_floor_is_monotone_nondecreasing(deltas):
    clk = _Clock(500.0)
    hg = Hourglass(1000, {"out": protected(100)}, conserve_margin_secs=50, clock=clk)
    prev = int(hg.mode)
    for i, dt in enumerate(deltas):
        clk.t = max(0.0, clk.t + dt)   # runway may jitter up or down
        hg.mark_completed(f"p{i}")     # a grant-boundary ratchet
        m = int(hg.mode)
        assert m >= prev               # forward-only: the floor never relaxes
        prev = m
