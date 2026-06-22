"""The v0.2 hero demo: a heavy input returns a *shorter answer*, not nothing.

Run it:

    python examples/hourglass_demo.py

Two runs of the SAME three-phase "memo generation" (research → synthesis →
finalize) under the SAME wall-clock budget. A greedy research phase wants far
more time than the run has.

- NAIVE: research runs to its own budget, the outer watchdog kills the whole run
  mid-research, and you get NOTHING — after spending the entire budget.
- HOURGLASS: research is clamped to the runway that isn't reserved for output, so
  finalize keeps its protected slice and streams a complete-but-shorter memo.

`total_secs=3.0` stands in for a 20-minute production budget; no LangGraph needed.
"""

import asyncio
import time

from langgraph_node_deadline import (
    Hourglass,
    protected,
    cooperative_wait_for,
    cooperative_poll,
)

TOTAL = 3.0            # stands in for the run's whole budget (e.g. 20 minutes)
OUTER_WATCHDOG = 3.3   # the platform-level hard kill (LangGraph TimeoutPolicy)
FINALIZE_FLOOR = 1.2   # runway we protect for writing the answer


async def research(findings):
    """A greedy retrieval/agent phase that 'wants' ~10s and accretes findings."""
    for i in range(40):
        await asyncio.sleep(0.25)
        findings.append(f"finding {i + 1}")


async def stream_memo_sections(findings):
    """Finalize streams the memo section by section (~0.2s each)."""
    for section in ["Summary", "Market", "Sponsor", "Financials", "Risks", "Returns", "Recommendation"]:
        await asyncio.sleep(0.2)
        yield f"## {section}  (from {len(findings)} findings)"


# --- NAIVE: no run budget; research is blind to the wall clock --------------- #
async def naive_run():
    findings = []
    await asyncio.wait_for(research(findings), timeout=10.0)  # research's own budget
    sections = [s async for s in stream_memo_sections(findings)]  # never reached
    return findings, sections


# --- HOURGLASS: run budget + protected finalize reserve + streaming salvage --- #
async def hourglass_run():
    budget = Hourglass(total_secs=TOTAL, reserve={"finalize": protected(FINALIZE_FLOOR)}).validate()

    findings = []
    with budget.grant("research"):                 # gets TOTAL minus finalize's reserve
        try:
            await cooperative_wait_for(research(findings), budget_secs=10.0)
        except asyncio.TimeoutError:
            pass                                    # out of research runway — keep findings

    sections = []
    with budget.grant("finalize"):                 # its 1.2s floor is guaranteed
        async for section in cooperative_poll(
            stream_memo_sections(findings),
            predicates=[budget.deadline_predicate()],
        ):
            sections.append(section)               # salvage every section that lands in time
    return findings, sections, budget.mode


async def main():
    print(f"\nBudget: {TOTAL}s total  |  outer watchdog: {OUTER_WATCHDOG}s  |  research wants ~10s\n")

    start = time.monotonic()
    try:
        findings, sections = await asyncio.wait_for(naive_run(), timeout=OUTER_WATCHDOG)
        print(f"  NAIVE     -> {len(sections)} sections in {time.monotonic() - start:.1f}s")
    except asyncio.TimeoutError:
        print(
            f"  NAIVE     -> FAILED in {time.monotonic() - start:.1f}s — watchdog killed the run "
            f"mid-research; NO memo produced (full budget spent, nothing returned)"
        )

    start = time.monotonic()
    findings, sections, mode = await hourglass_run()
    print(
        f"  HOURGLASS -> {len(sections)}-section memo in {time.monotonic() - start:.1f}s "
        f"from {len(findings)} findings (ended in mode={mode})"
    )
    for s in sections:
        print(f"               {s}")
    print("\nSame budget. One returns nothing; the other returns a shorter answer.\n")


if __name__ == "__main__":
    asyncio.run(main())
