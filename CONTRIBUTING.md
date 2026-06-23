# Contributing

Thanks for considering a contribution. This is a deliberately small, sharp library;
keeping it that way is a feature.

## Dev setup

No virtualenv is committed. Create one and install the dev + lint extras:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev,lint]"
# for the LangGraph integration tests as well:
pip install -e ".[dev,lint,langgraph]"
```

## Running the checks

CI runs exactly these — run them locally before opening a PR:

```bash
pytest -q                                          # the test suite
pytest -q -W error::DeprecationWarning             # how CI runs it (deprecations are errors)
ruff check src tests examples
mypy --strict src/langgraph_node_deadline
pyright src/langgraph_node_deadline
vermin --target=3.9- --eval-annotations --violations src   # the 3.9 floor holds
python examples/salvage_demo.py && python examples/hourglass_demo.py
```

## Ground rules

These are non-negotiable — a change that breaks one of them won't be merged:

1. **Fail-open.** With no active deadline scope, every function must behave as if it
   weren't there. Never make a helper raise or change behavior just because no scope
   is active.
2. **Zero runtime dependencies in the core.** `[dev]` / `[lint]` / `[langgraph]` are
   optional extras for development and the optional integration only. The
   `langgraph_node_deadline.langgraph` submodule imports langgraph lazily.
3. **Every behavior or value change ships with a regression test.** Numeric envelopes
   and invariants are pinned by `tests/test_property.py`; keep them green.
4. **CPython 3.9–3.13.** No `3.10+`-only syntax/stdlib in the runtime path (vermin
   guards this).
5. **Stay in scope.** This package owns *time-deadline orchestration* for LangGraph
   nodes — nothing else. Token/$ budgets, surplus reallocation, and a full graph
   auto-wrapper are tracked for a later version; see the roadmap in
   [issue #1](https://github.com/youknowfred/langgraph-node-deadline/issues/1) and
   [`docs/HARDENING_ROUND2.md`](docs/HARDENING_ROUND2.md).

## Releasing

Maintainer-only; see [`docs/RELEASING.md`](docs/RELEASING.md).
