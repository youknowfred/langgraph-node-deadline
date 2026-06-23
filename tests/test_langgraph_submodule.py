"""Fail-open tests for the optional LangGraph submodule — no langgraph required.

The submodule imports langgraph lazily, so these run on the core matrix too: a
``with_grant``-decorated node must run unchanged when there is no graph runtime
(a unit test, or langgraph not installed). The real-StateGraph behavior is in
tests/test_langgraph_integration.py.
"""
from __future__ import annotations

from langgraph_node_deadline import Hourglass
from langgraph_node_deadline.langgraph import (
    DeadlineContext,
    _budget_from_runtime,
    with_grant,
)


def test_deadline_context_carries_a_budget():
    ctx = DeadlineContext(budget=Hourglass(100))
    assert isinstance(ctx.budget, Hourglass)
    assert DeadlineContext().budget is None


def test_budget_from_runtime_is_none_outside_a_graph():
    # No active graph run (and possibly no langgraph) -> fail open to None.
    assert _budget_from_runtime(DeadlineContext, "budget") is None


async def test_with_grant_runs_the_node_fail_open_without_a_runtime():
    seen = []

    @with_grant("research", cap=1.0)
    async def node(state):
        seen.append(state)
        return {"ok": True}

    out = await node({"x": 1})
    assert out == {"ok": True}      # decorated node runs unchanged
    assert seen == [{"x": 1}]       # args pass through


async def test_with_grant_preserves_function_metadata():
    @with_grant("research")
    async def my_node(state):
        """doc."""
        return state

    assert my_node.__name__ == "my_node"
    assert my_node.__doc__ == "doc."
