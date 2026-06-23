"""Hermetic integration test: prove the salvage claim against a *real* LangGraph
``StateGraph`` under its actual timeout watchdogs.

Runs only where ``langgraph`` is installed (the optional ``[langgraph]`` extra);
skipped cleanly everywhere else, so the zero-runtime-dependency core is untouched.
No network or LLM — ``asyncio.sleep`` stands in for heavy work.

The package's whole pitch is "a ``try/except`` salvage path runs *before* the
watchdog discards the node." These tests pin that to real framework behavior:
the clamp pattern salvages, while the intuitive DIY fix (catch ``CancelledError``,
return partial) does **not** — LangGraph discards the late return and still raises.
"""
from __future__ import annotations

import asyncio
import inspect
from typing import List

import pytest

pytest.importorskip("langgraph")

from langgraph.graph import START, END, StateGraph  # noqa: E402

from langgraph_node_deadline import (  # noqa: E402
    Hourglass,
    cooperative_wait_for,
    node_deadline_in,
)
from langgraph_node_deadline.langgraph import (  # noqa: E402
    DeadlineContext,
    add_budgeted_node,
    with_grant,
)

try:
    from typing import TypedDict
except ImportError:  # pragma: no cover - 3.9+ has typing.TypedDict
    from typing_extensions import TypedDict

# Recent LangGraph adds a per-node timeout (add_node(..., timeout=...)). Older
# versions only have the graph-wide step_timeout; feature-detect so this test
# stays green across the supported LangGraph range.
_HAS_PER_NODE_TIMEOUT = "timeout" in inspect.signature(StateGraph.add_node).parameters

CAP = 1.5      # the node/super-step watchdog
GRACE = 0.5    # how far under it we set the cooperative scope


class State(TypedDict):
    progress: List[str]
    outcome: str


async def _heavy(progress: List[str]) -> None:
    """Wants ~10s and accretes partial work — pure sleeps, no network."""
    for i in range(500):
        await asyncio.sleep(0.02)
        progress.append(f"step {i + 1}")


async def _clamped_node(state: State) -> dict:
    progress: List[str] = []
    with node_deadline_in(CAP - GRACE):
        try:
            await cooperative_wait_for(_heavy(progress), budget_secs=10.0)
        except asyncio.TimeoutError:
            return {"progress": progress, "outcome": "salvaged"}  # runs before the kill
    return {"progress": progress, "outcome": "complete"}


async def _naive_node(state: State) -> dict:
    progress: List[str] = []
    try:
        await _heavy(progress)  # no clamp — the watchdog cancels this mid-flight
    except asyncio.CancelledError:
        # The intuitive fix — which LangGraph discards.
        return {"progress": progress, "outcome": "salvaged-naive"}
    return {"progress": progress, "outcome": "complete"}


def _build(node, *, per_node_timeout=None, step_timeout=None):
    g = StateGraph(State)
    if per_node_timeout is not None:
        g.add_node("work", node, timeout=per_node_timeout)
    else:
        g.add_node("work", node)
    g.add_edge(START, "work")
    g.add_edge("work", END)
    app = g.compile()
    if step_timeout is not None:
        app.step_timeout = step_timeout
    return app


# --------------------------------------------------------------------------- #
# Graph-wide step_timeout (available on every supported LangGraph)             #
# --------------------------------------------------------------------------- #

async def test_clamp_salvages_under_step_timeout():
    app = _build(_clamped_node, step_timeout=CAP)
    res = await app.ainvoke({"progress": [], "outcome": ""})
    assert res["outcome"] == "salvaged"
    assert 0 < len(res["progress"]) < 500  # kept the early steps, not all of them


async def test_naive_cancel_catch_loses_everything_under_step_timeout():
    app = _build(_naive_node, step_timeout=CAP)
    # LangGraph discards the node's late return and raises the super-step timeout.
    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        await app.ainvoke({"progress": [], "outcome": ""})


# --------------------------------------------------------------------------- #
# Per-node TimeoutPolicy (recent LangGraph: add_node(..., timeout=...))        #
# --------------------------------------------------------------------------- #

@pytest.mark.skipif(
    not _HAS_PER_NODE_TIMEOUT, reason="per-node add_node(timeout=...) needs recent LangGraph"
)
async def test_clamp_salvages_under_per_node_timeout():
    app = _build(_clamped_node, per_node_timeout=CAP)
    res = await app.ainvoke({"progress": [], "outcome": ""})
    assert res["outcome"] == "salvaged"
    assert 0 < len(res["progress"]) < 500


@pytest.mark.skipif(
    not _HAS_PER_NODE_TIMEOUT, reason="per-node add_node(timeout=...) needs recent LangGraph"
)
async def test_naive_cancel_catch_loses_everything_under_per_node_timeout():
    from langgraph.errors import NodeTimeoutError

    app = _build(_naive_node, per_node_timeout=CAP)
    with pytest.raises(NodeTimeoutError):
        await app.ainvoke({"progress": [], "outcome": ""})


# --------------------------------------------------------------------------- #
# Positioning guard: keep the README's wiring honest about the real API        #
# --------------------------------------------------------------------------- #

def test_documented_langgraph_surface_exists():
    # step_timeout is a settable attribute on the compiled graph.
    app = _build(_clamped_node)
    app.step_timeout = 5.0
    assert app.step_timeout == 5.0
    # If this LangGraph exposes a per-node timeout, it is the documented kwarg.
    if _HAS_PER_NODE_TIMEOUT:
        from langgraph.types import TimeoutPolicy

        assert hasattr(TimeoutPolicy(run_timeout=1.0), "run_timeout")


# --------------------------------------------------------------------------- #
# The optional langgraph integration submodule, against a real StateGraph      #
# --------------------------------------------------------------------------- #

_COOP_CAP = 0.7


async def _grantless_research(state):
    # No node_deadline_in here — the grant opened by with_grant supplies the scope.
    progress = []
    try:
        await cooperative_wait_for(_heavy(progress), budget_secs=10.0)
    except asyncio.TimeoutError:
        return {"progress": progress, "outcome": "salvaged"}
    return {"progress": progress, "outcome": "complete"}


async def test_with_grant_salvages_under_step_timeout():
    g = StateGraph(State, context_schema=DeadlineContext)
    g.add_node("research", with_grant("research", cap=_COOP_CAP)(_grantless_research))
    g.add_edge(START, "research")
    g.add_edge("research", END)
    app = g.compile()
    app.step_timeout = _COOP_CAP + 0.5
    res = await app.ainvoke(
        {"progress": [], "outcome": ""},
        context=DeadlineContext(budget=Hourglass(1000)),
    )
    assert res["outcome"] == "salvaged"
    assert 0 < len(res["progress"]) < 500


async def test_with_grant_without_budget_runs_fail_open_and_is_killed():
    # No budget on the context -> with_grant fails open -> no cooperative deadline ->
    # heavy outlasts the watchdog and the run is killed. The budget is what salvages.
    g = StateGraph(State, context_schema=DeadlineContext)
    g.add_node("research", with_grant("research", cap=_COOP_CAP)(_grantless_research))
    g.add_edge(START, "research")
    g.add_edge("research", END)
    app = g.compile()
    app.step_timeout = _COOP_CAP + 0.5
    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        await app.ainvoke(
            {"progress": [], "outcome": ""},
            context=DeadlineContext(budget=None),
        )


@pytest.mark.skipif(
    not _HAS_PER_NODE_TIMEOUT, reason="add_budgeted_node needs per-node timeout support"
)
async def test_add_budgeted_node_derives_watchdog_and_salvages():
    # add_budgeted_node wires the grant AND sets the node timeout to cap+grace, so the
    # cooperative deadline always fires before the watchdog.
    g = StateGraph(State, context_schema=DeadlineContext)
    add_budgeted_node(g, "research", _grantless_research, cap=_COOP_CAP, grace=0.5)
    g.add_edge(START, "research")
    g.add_edge("research", END)
    app = g.compile()
    res = await app.ainvoke(
        {"progress": [], "outcome": ""},
        context=DeadlineContext(budget=Hourglass(1000)),
    )
    assert res["outcome"] == "salvaged"
    assert 0 < len(res["progress"]) < 500
