# v0.2.0 — `Hourglass` run-wide budgets

`langgraph-node-deadline` keeps a LangGraph node from timing out into **nothing**:
clamp every inner timeout to one binding deadline, so heavy work **salvages a
partial result** instead of being hard-killed by the watchdog and discarding
everything.

v0.1 protects a single node. **v0.2 adds `Hourglass`** — an optional layer that
spreads one time budget across a whole graph and *guarantees your output phase its
runway*, so a heavy run degrades to a shorter answer instead of timing out after
minutes of paid LLM calls.

Still **zero runtime dependencies**. Python **3.9–3.13**.

```bash
pip install -U langgraph-node-deadline
```

## ✨ New: the `Hourglass` budget layer

```python
from langgraph_node_deadline import Hourglass, protected, Mode

budget = Hourglass(
    total_secs=900,
    reserve={"synthesis": protected(160), "finalize": protected(135)},
).validate()                                    # crashes now on an incoherent envelope

with budget.grant("research", cap=400) as g:    # sync `with`; awaiting inside is fine
    if budget.mode >= Mode.FINISH_ONLY:         # ("finish_only" also works)
        skip_optional_enrichment()
    await g.poll()                              # cooperative cancel point
```

- **Protected output reserves** — `reserve={"finalize": protected(135)}` carves a
  runway floor that earlier phases can't borrow against, so your finalize/synthesis
  phase always has time to write the answer. Released once its phase completes.
- **Forward-only degradation ladder** — `NORMAL → CONSERVE → FINISH_ONLY → HALT`.
  Nodes read `budget.mode` to shed optional work; reading it is side-effect-free, and
  it never bounces back to `NORMAL` to re-arm an overrun it just escaped.
- **`validate()` startup invariant-lock** — a budget misconfiguration becomes a crash
  at startup, not a 3am production cascade.
- **Leak-proof `grant()`** — opens a kernel `node_deadline` scope for the phase and
  releases its reserve on exit, even on exception or cancellation.
- **Streaming salvage** — `cooperative_poll(astream, ...)` keeps whatever the output
  phase managed to write when the deadline hits, and the new **`aclosing()`** helper
  gives deterministic upstream teardown when you stop iterating early.

See it run: `python examples/hourglass_demo.py` — a greedy phase eats the budget, but
the memo still ships.

## 🛡️ Hardened for production

This release shipped after a security + compatibility audit
([`docs/PRELAUNCH_HARDENING.md`](docs/PRELAUNCH_HARDENING.md)). Highlights:

- **Proven against real LangGraph.** A `StateGraph` integration test shows the clamp
  pattern salvages a partial result *before* the watchdog fires — under both the
  per-node timeout (`add_node(..., timeout=...)` / `TimeoutPolicy`) and the graph-wide
  `step_timeout` — while the intuitive "catch `CancelledError` and return partial" fix
  loses everything.
- **No silently-ignored deadlines.** `cooperative_poll` custom predicates now *augment*
  the binding deadline instead of replacing it.
- **Tighter invariants.** `clamp_to_node_deadline` treats `reserve_secs` as a
  non-negative floor and normalizes NaN/inf; `Hourglass.validate()` rejects non-finite
  budgets.
- **Honest docs.** Accurate LangGraph timeout framing, an honest "when *not* to use
  this" comparison to `asyncio.timeout` / tenacity / Cycles, and the real
  thread/executor context-propagation contract.
- **Typed & gated.** Clean under `mypy --strict` and `pyright`; CI now runs the type
  checkers, `ruff`, a 3.9 compatibility guard, and a deprecations-as-errors gate across
  3.9–3.13.

Fail-open semantics and the zero-dependency core are unchanged.

## Compatibility

- Python 3.9, 3.10, 3.11, 3.12, 3.13.
- Zero runtime dependencies (optional `dev` / `lint` / `langgraph` extras are for
  development only).
- The v0.1 kernel API is unchanged — v0.2 is purely additive.

## Links

- Full changelog: [`CHANGELOG.md`](CHANGELOG.md)
- Roadmap & design: [issue #1](https://github.com/youknowfred/langgraph-node-deadline/issues/1)
- Feedback welcome — this is built for non-traditional developers taking LangGraph
  agents to production.
