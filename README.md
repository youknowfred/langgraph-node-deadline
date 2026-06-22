# langgraph-node-deadline

**One binding deadline for every inner timeout in a LangGraph node.** Clamp inner
budgets to the node's cooperative deadline so heavy work **salvages a partial
result** instead of getting hard-killed by the watchdog and discarding everything.

Zero runtime dependencies. Python 3.9+. The kernel is ~120 lines; an optional
`Hourglass` budget layer ([v0.2](#run-wide-budgets-hourglass-v02--in-progress)) builds on it.

```bash
pip install langgraph-node-deadline
```

---

## The problem

A LangGraph node that does real work has *several layers each re-deriving their
own clock*: an outer `TimeoutPolicy` watchdog, an inner agent/tool budget, a
retry loop, a sub-planner that "wants" 60 seconds. When those clocks disagree,
the inner layers happily dispatch work the outer watchdog is **guaranteed to
kill** — and the kill is uncooperative. It cancels the node and **throws away
everything**, including the partial answer you could have returned.

You've seen the symptom: a long run times out into *nothing* after burning
minutes of paid LLM calls, and the user just sees "it failed." The upstream
issue is real and open: [langchain-ai/langgraph#5672 — *Run Cancellation Causes
Loss of Streamed State Not Yet Persisted*](https://github.com/langchain-ai/langgraph/issues/5672).

The trap, distilled: if your cooperative cancel and the watchdog are pinned to
the **same** number, the watchdog clock starts at *node entry — before your code
runs* — so your cancel loses the race deterministically. Equal timeouts lose.

## The fix

Set **one** deadline at node entry. Make every inner timeout *clamp to it*
instead of re-deriving its own. Now inner calls yield at the node boundary, with
a little grace, **before** the watchdog fires — so your `try/except` actually
runs and you return a complete-but-shorter answer.

```python
import asyncio
from langgraph_node_deadline import node_deadline_in, cooperative_wait_for

async def my_node(state):
    # this node gets ~1.8s of cooperative runtime (a hair under its watchdog)
    with node_deadline_in(1.8):
        try:
            # the planner asks for 5s, but gets clamped to what's actually left
            result = await cooperative_wait_for(plan_and_write(state), budget_secs=5.0)
            return {"draft": result}
        except asyncio.TimeoutError:
            # runs BEFORE the watchdog can kill us — keep the partial work
            return {"draft": salvage_partial(state)}
```

## See it lose vs. salvage (30 seconds, no LangGraph needed)

```bash
python examples/salvage_demo.py
```

```
Outer watchdog (LangGraph TimeoutPolicy): 2.0s  |  inner planner wants ~5s

  NAIVE   (inner ignores the node deadline)
    -> LOST in 2.00s — outer watchdog cancelled the node, salvage code never ran, ALL work discarded

  CLAMPED (inner clamps to the node deadline)
    -> SALVAGED in 1.80s — kept 3 steps: ['step 1', 'step 2', 'step 3']
```

Same work, same watchdog. One import decides whether you keep anything.

## Wiring it into a real LangGraph node

Set the scope to a hair under whatever cap the executor enforces, then clamp
every inner timed call through it:

```python
from langgraph_node_deadline import node_deadline_in, clamp_to_node_deadline, cooperative_wait_for

NODE_CAP_SECS = 30.0   # match this to your TimeoutPolicy, minus a small grace

async def research_node(state):
    with node_deadline_in(NODE_CAP_SECS - 1.0):   # leave 1s of grace under the watchdog
        # an inner retry loop, sub-agent, or tool call — all clamp to the same deadline
        per_call = clamp_to_node_deadline(15.0, reserve_secs=2.0)  # reserve finalize headroom
        chunks = await cooperative_wait_for(retrieve(state), budget_secs=per_call)
        return {"chunks": chunks}
```

Because the deadline lives in a `contextvars.ContextVar`, and `asyncio` copies
the ambient context when it creates a task, the scope you open before you
`await` is visible to the agent task **and every subagent task it spawns** — no
threading the deadline through call signatures.

## API

| Symbol | What it does |
| --- | --- |
| `node_deadline_in(seconds)` | Context manager. Set the binding deadline to `now + seconds`. Use at node entry. |
| `node_deadline_scope(deadline_monotonic)` | Context manager. Set the deadline to an absolute `time.monotonic()` timestamp (or `None` to clear). `node_deadline` is an alias. |
| `clamp_to_node_deadline(budget_secs, *, reserve_secs=0.0)` | **The core primitive.** Returns `min(budget_secs, remaining - reserve_secs)`, floored at 0. Returns `budget_secs` unchanged when no scope is active. |
| `cooperative_wait_for(awaitable, budget_secs, *, reserve_secs=0.0)` | `asyncio.wait_for` that never outlasts the node deadline. Raises `asyncio.TimeoutError` on the clamped budget. |
| `get_node_deadline_remaining_secs()` | Seconds left, or `None` if no scope. Never negative. |
| `node_deadline_exceeded()` | `True` only when a scope is active *and* its deadline has passed. Safe loop guard. |

**Fail-open by design.** With no active scope, every function behaves as if it
weren't there — so adding it to one node never changes the behavior of the rest
of your graph, your tests, or direct invocations.

## Run-wide budgets: `Hourglass` (v0.2 — in progress)

The kernel protects one node. `Hourglass` is the optional layer that spreads a
single time budget across a whole graph and **guarantees your output phase its
runway** — so a heavy run degrades to a shorter answer instead of timing out into
nothing.

```python
from langgraph_node_deadline import Hourglass, protected, Mode

budget = Hourglass(
    total_secs=900,
    reserve={"synthesis": protected(160), "finalize": protected(135)},
).validate()                                    # crashes now on an incoherent envelope

with budget.grant("research", cap=400) as g:    # sync `with`; awaiting inside is fine
    if budget.mode >= Mode.FINISH_ONLY:         # ("finish_only" also works)
        skip_optional_enrichment()              # shed optional work when runway is short
    await g.poll()                              # cooperative cancel point

with budget.grant("finalize"):                  # gets its protected 135s no matter what
    ...
```

- **Reserves are floors, not caps.** `research` can use everything *except* the
  runway still owed to phases that haven't completed — so it can't starve
  `finalize`. A reserve is released once its phase completes.
- **The `Mode` ladder is forward-only:** `NORMAL → CONSERVE → FINISH_ONLY → HALT`.
  Reading `budget.mode` is side-effect-free; the floor ratchets only at `grant()`
  boundaries, so a transient slow phase can't bounce the run back to `NORMAL`.
- **`validate()` fails loud at startup** on reserves that exceed the total or
  leave no `NORMAL` band — a budget misconfiguration becomes a crash, not a 3am
  cascade.
- Phases are modelled as running **one at a time** (sequential). Concurrent
  grants each see the full remaining runway — they don't split it.

> `Hourglass` lives on the `v0.2` branch, tracked in
> [issue #1](https://github.com/youknowfred/langgraph-node-deadline/issues/1).
> The kernel above is stable and shipped in `0.1.0`.

## Why a package for something so small

Because the *lesson* is the hard part, not the code. This is the
[`derive-don't-pin`](https://github.com/langchain-ai/langgraph/issues/5672)
discipline extracted from a production agent that paid for it: a synthesis pool
that believed it had 43.5 seconds left *nine seconds before* the watchdog killed
the node — because four inner layers each trusted their own clock and none knew
the one the executor was actually enforcing. One binding deadline fixes the
entire class of bug.

## License

MIT © 2026 Fred Becker. See [LICENSE](LICENSE).
