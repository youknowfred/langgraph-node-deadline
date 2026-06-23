"""The README hero, runnable with zero dependencies.

Run it:

    python examples/salvage_demo.py

Two nodes do the SAME work and live under the SAME outer watchdog. The only
difference is whether the inner planner clamps its budget to the binding node
deadline. One loses everything; one salvages a partial result.

This simulates a LangGraph node under ``step_timeout`` (LangGraph's only built-in
watchdog — it bounds the whole super-step and cancels uncooperatively). No
LangGraph install is needed — ``asyncio.wait_for`` stands in for ``step_timeout``
so you can see the mechanics directly.
"""

import asyncio
import time

from langgraph_node_deadline import node_deadline_in, cooperative_wait_for

OUTER_WATCHDOG = 2.0  # stands in for LangGraph's step_timeout (cancels uncooperatively)
NODE_CAP = 1.8        # the cooperative deadline we enforce INSIDE the node (fires first)


async def long_planner(progress):
    """An inner agent/LLM loop that 'wants' ~5s and accretes partial work."""
    for i in range(10):
        await asyncio.sleep(0.5)
        progress.append(f"step {i + 1}")


# --- Naive node: the inner call re-derives its OWN 5s budget, blind to the watchdog.
async def naive_node():
    progress = []
    try:
        await asyncio.wait_for(long_planner(progress), timeout=5.0)
    except asyncio.TimeoutError:
        return "salvaged", progress  # never reached — the watchdog kills us first
    return "complete", progress


# --- Clamped node: the inner call clamps to the binding node deadline.
async def clamped_node():
    progress = []
    with node_deadline_in(NODE_CAP):
        try:
            await cooperative_wait_for(long_planner(progress), budget_secs=5.0)
        except asyncio.TimeoutError:
            return "salvaged", progress  # fires at ~1.8s, BEFORE the 2.0s watchdog
    return "complete", progress


async def run(label, node):
    start = time.monotonic()
    try:
        outcome, progress = await asyncio.wait_for(node(), timeout=OUTER_WATCHDOG)
        elapsed = time.monotonic() - start
        print(
            f"  {label}\n"
            f"    -> {outcome.upper()} in {elapsed:.2f}s — kept {len(progress)} steps: {progress}\n"
        )
    except asyncio.TimeoutError:
        elapsed = time.monotonic() - start
        print(
            f"  {label}\n"
            f"    -> LOST in {elapsed:.2f}s — outer watchdog cancelled the node, "
            f"salvage code never ran, ALL work discarded\n"
        )


async def main():
    print(
        f"\nOuter watchdog (LangGraph step_timeout): {OUTER_WATCHDOG}s"
        f"  |  inner planner wants ~5s\n"
    )
    await run("NAIVE   (inner ignores the node deadline)", naive_node)
    await run("CLAMPED (inner clamps to the node deadline)", clamped_node)
    print("Same work, same watchdog. One import decides whether you keep anything.\n")


if __name__ == "__main__":
    asyncio.run(main())
