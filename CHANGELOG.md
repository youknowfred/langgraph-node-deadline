# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
[Semantic Versioning](https://semver.org/).

## [0.2.0] — unreleased (launch candidate)

Grow the node-deadline kernel into a run-wide budget — the first `hourglass`
layer. Tracks milestone v0.2.0-a in issue #1.

### Added (round-2 hardening + enrichment)

See [`docs/HARDENING_ROUND2.md`](docs/HARDENING_ROUND2.md).

- `recommended_watchdog_secs(cap, *, grace=1.0)` and `node_deadline_in_under(
  watchdog, *, grace=1.0)` — the two directions of the "never pin the watchdog
  equal to the cap" guard.
- `run_off_loop(fn, /, *args, **kwargs)` — run a blocking callable in a worker
  thread with the binding deadline carried in.
- `Hourglass.fan_out(phase, *, cap=None)` — `grant()` named for the concurrent
  pattern; branches under `asyncio.gather` share one wall-clock window.
- `langgraph_node_deadline.langgraph` — an optional, lazily-imported integration
  submodule (`DeadlineContext`, `with_grant`, `add_budgeted_node`) that opens a
  grant around a LangGraph node and derives its watchdog. No runtime dependency;
  install the `[langgraph]` extra.
- `tests/test_property.py` — Hypothesis property tests for the clamp/reserve/Mode
  invariants. A `bench/overhead.py` micro-benchmark (and a README "Overhead"
  table). `SECURITY.md`, `CONTRIBUTING.md`, and Dependabot.

### Added (strong nice-to-have fast-follow)

- `collect_until_deadline(aiter, *, predicates=None, max_items=None)` — the batch
  sibling of `cooperative_poll`: drains an `astream` under the binding deadline and
  **returns** the accumulated list, tearing the upstream down deterministically
  (including the `max_items` early break) so the consumer needn't hand-manage the
  accumulator and the `aclosing` teardown.
- CI now enforces a **90% branch-coverage gate** (`pytest-cov`, in the existing
  `test` matrix step — not a separate job); the gated core kernel + hourglass are at
  100%. The lazily-imported `langgraph` submodule is excluded (it is covered by the
  separate, langgraph-gated integration job).
- **OpenSSF Scorecard** workflow (`.github/workflows/scorecard.yml`) and README
  badge — weekly + on-push supply-chain scoring, SARIF uploaded to the Security tab.

### Fixed (round 2)

- `Hourglass.mode` read the wrong phase's reserve under `asyncio.gather` and nested
  grants (`_active_phase` was a single shared instance slot). The active grant is
  now tracked per-task via a contextvar, so concurrent and nested grants each
  exclude their own reserve correctly.

### Changed (round 2)

- Supply-chain: SHA-pin all GitHub Actions (Dependabot keeps them fresh); move the
  PyPI publish action off the floating `release/v1` branch to a tagged release
  (PEP 740 attestations on by default); add a post-build wheel smoke test and a
  re-validation step to `release.yml`. CI lint gate now also covers `bench/`.

### Added
- `Hourglass(total_secs, reserve={...})` — a run-wide time budget partitioned
  across a multi-node graph, built entirely on the v0.1 kernel.
- `protected(secs)` / `Reserve` — protected runway floors that earlier,
  non-reserved phases cannot consume; released once their phase completes.
- `Hourglass.grant(phase, cap)` — leak-proof context manager that opens a kernel
  `node_deadline` scope for the phase and records completion even on exception.
- `Hourglass.deadline_for(phase, cap=...)` — the absolute deadline a phase may
  run to, after honoring other phases' reserves.
- `Mode` degradation ladder (`NORMAL → CONSERVE → FINISH_ONLY → HALT`),
  forward-only, exposed via the `Hourglass.mode` property.
- `Hourglass.validate()` — startup invariant-lock; raises on an incoherent
  envelope (reserves exceeding the total, non-positive floors).
- `Grant.poll()` cooperative checkpoint and `Hourglass.deadline_predicate()`.
- `cooperative_poll(aiter, predicates=[...])` — the streaming sibling of
  `cooperative_wait_for`: wrap an `astream`, and when the deadline (or any
  predicate) trips it stops cleanly and closes the upstream iterator, so the
  consumer keeps the chunks it already accumulated. Silent salvage; never raises
  at the consumer.
- `examples/hourglass_demo.py` — the hero demo: a heavy input returns a
  complete-but-shorter memo instead of timing out into nothing.
- `aclosing(...)` — a 3.9-compatible `contextlib.aclosing` stand-in for
  deterministic upstream teardown when a `cooperative_poll` consumer exits early.

### Changed (pre-launch hardening)

Findings from a security + compatibility audit (see
[`docs/PRELAUNCH_HARDENING.md`](docs/PRELAUNCH_HARDENING.md)):

- `cooperative_poll` now treats custom `predicates` as *additional* stop
  conditions; the binding node deadline is always honored, so an active scope can
  never be silently ignored.
- `cooperative_poll` is fully type-annotated (clean under `mypy --strict`).
- `clamp_to_node_deadline` treats `reserve_secs` as a non-negative floor and
  normalizes NaN/inf, so the clamp can never widen past the deadline.
- `Hourglass.validate()` rejects non-finite `total_secs` / reserves /
  `conserve_margin` (a `NaN` no longer slips through `nan <= 0`).
- Docs corrected: the LangGraph watchdog is `step_timeout` (super-step-wide),
  not the non-existent `TimeoutPolicy`; thread/executor context-propagation is
  documented accurately; added a "when NOT to use this" comparison.
- Packaging/CI: pinned `hatchling>=1.27` for deterministic PEP 639 license
  metadata; added `mypy`/`pyright`/`ruff`/`vermin` and a deprecation-as-error
  gate to CI; added a Trusted-Publishing (OIDC) release workflow.

v0.2 is feature-complete for the v0.2.0-a milestone in issue #1.

## [0.1.0] — unreleased

Initial release.

### Added
- `node_deadline_scope` / `node_deadline` — context manager binding a deadline
  on a `time.monotonic()` basis.
- `node_deadline_in(seconds)` — relative-budget convenience scope.
- `clamp_to_node_deadline(budget_secs, *, reserve_secs=0.0)` — the core
  primitive; clamps any proposed timeout to the binding deadline, fail-open.
- `cooperative_wait_for(awaitable, budget_secs, *, reserve_secs=0.0)` — a
  deadline-clamped `asyncio.wait_for`.
- `get_node_deadline_remaining_secs()` and `node_deadline_exceeded()` readers.
- Runnable, dependency-free `examples/salvage_demo.py`.
- Full test suite covering fail-open semantics, the clamp monotonicity
  invariant, nested-scope restoration, and async context propagation into and
  out of child tasks.
