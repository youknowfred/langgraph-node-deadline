# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
[Semantic Versioning](https://semver.org/).

## [0.2.0] — unreleased (launch candidate)

Grow the node-deadline kernel into a run-wide budget — the first `hourglass`
layer. Tracks milestone v0.2.0-a in issue #1.

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
