# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); this project uses
[Semantic Versioning](https://semver.org/).

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
