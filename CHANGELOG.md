# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
[Semantic Versioning](https://semver.org/).

## [0.2.0] — unreleased (in progress on the `v0.2` branch)

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

### Still to come in 0.2 (issue #1)
- `cooperative_poll(astream, predicates=[...])` streaming salvage.
- The "20-min input returns a shorter answer instead of nothing" hero demo.

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
