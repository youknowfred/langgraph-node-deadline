# langgraph-node-deadline

**One binding deadline for every inner timeout in a LangGraph node.** Clamp inner
budgets to the node's cooperative deadline so heavy work **salvages a partial
result** instead of getting hard-killed by the watchdog and discarding everything.

Zero runtime dependencies. Python 3.9+. The kernel is ~120 lines; an optional
`Hourglass` budget layer ([v0.2](#run-wide-budgets-hourglass-v02)) builds on it.

```bash
pip install langgraph-node-deadline
```

---

## The problem

A LangGraph node that does real work has *several layers each re-deriving their
own clock*: an outer node-timeout watchdog (`add_node(..., timeout=...)` / a
`TimeoutPolicy`, or the graph-wide `step_timeout`), an inner agent/tool budget, a
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
Outer watchdog (LangGraph node timeout): 2.0s  |  inner planner wants ~5s

  NAIVE   (inner ignores the node deadline)
    -> LOST in 2.00s — outer watchdog cancelled the node, salvage code never ran, ALL work discarded

  CLAMPED (inner clamps to the node deadline)
    -> SALVAGED in 1.80s — kept 3 steps: ['step 1', 'step 2', 'step 3']
```

Same work, same watchdog. One import decides whether you keep anything.

## Wiring it into a real LangGraph node

LangGraph cancels a node *uncooperatively* on two kinds of timeout, and the clamp
works under both:

- **Per-node** — `add_node("research", node, timeout=30)` (a wall-clock cap, or a
  `TimeoutPolicy(run_timeout=30)` for an idle-timeout variant). On expiry LangGraph
  raises `NodeTimeoutError` and cancels the node. *(Recent LangGraph; this is the
  natural "this node gets N seconds" knob.)*
- **Graph-wide** — `app.step_timeout = 30.0`, which bounds the whole *super-step*
  (every node running in one parallel tick) and raises `TimeoutError`.

Set your node's scope a hair under whichever cap binds it, then clamp every inner
timed call through it:

```python
from langgraph.types import TimeoutPolicy
from langgraph_node_deadline import node_deadline_in, clamp_to_node_deadline, cooperative_wait_for

from langgraph_node_deadline import node_deadline_in_under

NODE_CAP = 30.0
builder.add_node("research", research_node, timeout=NODE_CAP)  # LangGraph's per-node watchdog

async def research_node(state):
    with node_deadline_in_under(NODE_CAP):    # a safe grace below the watchdog (no hand math)
        # an inner retry loop, sub-agent, or tool call — all clamp to the same deadline
        per_call = clamp_to_node_deadline(15.0, reserve_secs=2.0)  # reserve finalize headroom
        chunks = await cooperative_wait_for(retrieve(state), budget_secs=per_call)
        return {"chunks": chunks}
```

> **Never pin the watchdog equal to your cap** — equal timeouts lose (the watchdog
> clock starts at node entry, before your code). `node_deadline_in_under(watchdog)`
> sizes the scope a `grace` *below* a known watchdog; its mirror,
> `recommended_watchdog_secs(cap)`, gives the watchdog to set for a known cap
> (`timeout=recommended_watchdog_secs(cap)`). Use whichever quantity you fix first.

> **If you rely on the graph-wide `step_timeout` instead, it is super-step-wide,
> not per-node.** When nodes run in parallel in one tick, it bounds the *whole
> tick* and cancels every node in it — so a clamp that perfectly fits your node can
> still be killed if a *sibling* overruns the shared step. Size `node_deadline_in`
> against whichever cap actually binds your node.

> **On older LangGraph** (before the per-node `timeout=` / `TimeoutPolicy`), only
> `app.step_timeout` exists — set the scope against that. The salvage mechanic is
> identical; both watchdogs cancel via asyncio cancellation.

Because the deadline lives in a `contextvars.ContextVar`, and `asyncio` copies
the ambient context when it creates a task, the scope you open before you `await`
is visible to the agent task **and every asyncio task it spawns** — no threading
the deadline through call signatures.

> **Threads and executors are the exception.** `asyncio.create_task` and
> `asyncio.to_thread` copy the context, so the deadline carries into them. But a
> plain `loop.run_in_executor(None, fn)` or a raw `threading.Thread` does **not**
> copy it — there the helpers fail open (the deadline simply isn't enforced). To
> offload a blocking call *and* keep the deadline binding inside it, use
> `run_off_loop`, which copies the context for you:
> ```python
> from langgraph_node_deadline import run_off_loop
> with node_deadline_in(30):
>     rows = await run_off_loop(blocking_db_query, sql)   # deadline binds inside the worker
> ```
> (`asyncio.to_thread(blocking_call)` already does this for you.)

### Optional sugar: the `langgraph` submodule

If you'd rather not wire the scope and watchdog by hand, the optional
`langgraph_node_deadline.langgraph` submodule does both. It threads an `Hourglass`
through LangGraph's `runtime.context` and opens the grant for you — and
`add_budgeted_node` sets the node's watchdog to `cap + grace` so you can't pin them
equal. Install it with `pip install "langgraph-node-deadline[langgraph]"` (still no
runtime dependency in the core — the import is lazy):

```python
from langgraph.graph import StateGraph, START, END
from langgraph_node_deadline import Hourglass, protected, cooperative_wait_for
from langgraph_node_deadline.langgraph import DeadlineContext, add_budgeted_node

async def research_node(state):                       # a plain node — no manual scope
    result = await cooperative_wait_for(plan_and_write(state), budget_secs=600)
    return {"draft": result}

g = StateGraph(State, context_schema=DeadlineContext)
add_budgeted_node(g, "research", research_node, cap=400)   # opens grant("research") AND sets timeout=cap+grace
g.add_edge(START, "research"); g.add_edge("research", END)
app = g.compile()

budget = Hourglass(900, reserve={"finalize": protected(135)})
await app.ainvoke(state, context=DeadlineContext(budget=budget))
```

`add_budgeted_node` wraps the node in `grant("research")` and sets the node's
watchdog to `cap + grace` in one call. (Prefer the explicit `@with_grant("research",
cap=400)` decorator + a normal `add_node` if you set timeouts yourself.) It uses the
`runtime.context` hook — not a middleware, which LangGraph has no equivalent of — and
it's **fail-open**: with no graph runtime or no budget, the node runs exactly as
written, so the same function still works in a plain unit test.

## API

| Symbol | What it does |
| --- | --- |
| `node_deadline_in(seconds)` | Context manager. Set the binding deadline to `now + seconds`. Use at node entry. |
| `node_deadline_in_under(watchdog, *, grace=1.0)` | Context manager. Open the scope a `grace` *below* a known outer watchdog (`max(0, watchdog - grace)`) — no hand math, no sign mistakes. |
| `node_deadline_scope(deadline_monotonic)` | Context manager. Set the deadline to an absolute `time.monotonic()` timestamp (or `None` to clear). `node_deadline` is an alias. |
| `clamp_to_node_deadline(budget_secs, *, reserve_secs=0.0)` | **The core primitive.** Returns `min(budget_secs, remaining - reserve_secs)`, floored at 0. Returns `budget_secs` unchanged when no scope is active. |
| `recommended_watchdog_secs(cap, *, grace=1.0)` | `cap + grace` — the outer watchdog to set so the inner deadline fires first. The mirror of `node_deadline_in_under`. |
| `cooperative_wait_for(awaitable, budget_secs, *, reserve_secs=0.0)` | `asyncio.wait_for` that never outlasts the node deadline. Raises `asyncio.TimeoutError` on the clamped budget. |
| `cooperative_poll(aiter, *, predicates=None, bound_each_chunk=True)` | Stream an `astream`, stopping cleanly at the deadline (or any predicate) so you keep what you accumulated. |
| `aclosing(thing)` | Async context manager for deterministic `aclose()` on early exit (a 3.9-compatible `contextlib.aclosing`). |
| `run_off_loop(fn, /, *args, **kwargs)` | Run a blocking callable in a worker thread with the deadline contextvar carried in. |
| `get_node_deadline_remaining_secs()` | Seconds left, or `None` if no scope. Never negative. |
| `node_deadline_exceeded()` | `True` only when a scope is active *and* its deadline has passed. Safe loop guard. |

**Fail-open by design.** With no active scope, every function behaves as if it
weren't there — so adding it to one node never changes the behavior of the rest
of your graph, your tests, or direct invocations.

## Run-wide budgets: `Hourglass` (v0.2)

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
- Reserve accounting is **sequential** (phases run one after another). Time itself
  is a *shared* budget, not a split one — see fan-out below.

**Concurrent fan-out.** Launch parallel branches *inside one* `grant` (or its alias
`fan_out`): they all inherit the single binding deadline and share the same
wall-clock window. A time budget is shared, not split — concurrent branches overlap
in time, so there is nothing to partition, and `.mode` reads correctly *per branch*:

```python
with budget.fan_out("research") as g:
    results = await asyncio.gather(query_a(), query_b(), query_c())  # one shared deadline
```

(Per-branch *splitting* only makes sense for a serial, additive resource like
tokens — a separate axis from wall-clock time.)

**Streaming salvage.** `cooperative_poll` wraps an `astream` so the output phase
keeps whatever it managed to write when the deadline hits — a shorter memo, not a
crash:

```python
from langgraph_node_deadline import cooperative_poll

sections = []
with budget.grant("finalize"):
    async for chunk in cooperative_poll(agent.astream(state),
                                        predicates=[budget.deadline_predicate()]):
        sections.append(chunk)          # every section that lands before the deadline
return assemble(sections)               # complete-but-shorter, never nothing
```

Custom `predicates` are checked *in addition to* the binding deadline, never
instead of it — an active scope is always honored. If you might leave the
`async for` **early** (`break`/`return` once you have enough), wrap the stream in
`aclosing` so the upstream `astream` is torn down right then instead of whenever
the garbage collector gets to it:

```python
from langgraph_node_deadline import cooperative_poll, aclosing

async with aclosing(cooperative_poll(agent.astream(state))) as stream:
    async for chunk in stream:
        sections.append(chunk)
        if have_enough(sections):
            break                       # upstream closed here, deterministically
```

See it run — a greedy phase eats the budget but the memo still ships:

```bash
python examples/hourglass_demo.py
#   NAIVE     -> FAILED — watchdog killed the run mid-research; NO memo produced
#   HOURGLASS -> 5-section memo from 7 findings (ended in mode=halt)
```

> `Hourglass` ships in `0.2.0`, tracked in
> [issue #1](https://github.com/youknowfred/langgraph-node-deadline/issues/1).
> The kernel (everything above this section) shipped in `0.1.0` and is stable.

## When NOT to use this

This is a small, sharp tool for one failure mode. Reach for something else when:

- **You don't have an outer watchdog at all.** If nothing is hard-killing your
  node, you don't need to clamp to it — a plain `asyncio.wait_for` or
  `asyncio.timeout` is simpler.
- **Your work isn't cooperative.** The salvage trick needs your inner calls to
  `await` (so a clamped timeout can fire) and a `try/except` that returns partial
  state. A single blocking C call or a tight CPU loop with no `await` can't be
  interrupted cooperatively — clamp won't help.
- **You want retries, not salvage.** If the right answer to a timeout is "try
  again with backoff," use [tenacity](https://github.com/jd/tenacity) or
  LangGraph's `RetryPolicy`. This package is about *keeping partial work*, not
  re-running.
- **You need cost/token budgets.** For per-node *spend* enforcement,
  [Cycles](https://runcycles.io/how-to/integrating-cycles-with-langgraph) does
  that. `Hourglass` is wall-clock time only (token/$ axes are deferred to v0.3).

### Honest comparison

| | What it gives you | What it doesn't |
| --- | --- | --- |
| `asyncio.timeout` / `wait_for` | One timeout around one call | No *shared* deadline across nested layers; each call re-derives its own clock — the exact trap this package closes |
| tenacity / `RetryPolicy` | Retry with backoff | Re-runs from scratch; doesn't salvage the partial work a timeout discards |
| Cycles | Per-node **cost/token** budget | Not a wall-clock deadline; no partial-output salvage |
| **`langgraph-node-deadline`** | One binding deadline every inner timeout clamps to, so work **salvages** before the watchdog kills it | Not a retrier, not a cost meter — wall-clock salvage only |

The kernel is stdlib-only and `asyncio.wait_for`-based under the hood; the value
isn't new machinery, it's the *discipline* of one deadline instead of four.

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
