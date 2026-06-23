# Hardening round 2 (pre-launch)

A second pass — deeper hardening + targeted enrichment — before the public launch,
following the round-1 audit ([`PRELAUNCH_HARDENING.md`](PRELAUNCH_HARDENING.md)).
Scoped by a five-track investigation (test rigor, API ergonomics, LangGraph sugar,
supply-chain/OSS hygiene, deferred axes + docs), every proposal verified live in the
dev venv before being accepted. Same invariants hold throughout: **fail-open**,
**zero runtime dependencies in the core**, **CPython 3.9–3.13**, a regression test per
change. All of this folds into the still-unreleased `0.2.0` launch candidate.

## Headline correctness finding

**`Hourglass.mode` reads the wrong phase's reserve under `asyncio.gather` and nested
grants.** The binding deadline is correctly per-task (a `contextvars.ContextVar`), but
`_active_phase` was a single shared instance slot. Under `gather`, two concurrent
reserved grants both saw the *last* phase as active, so the other excluded the wrong
reserve from its degradation-ladder read; nested grants reset it to `None` instead of
restoring the outer phase. **Fix:** make the active grant per-task (a module-level
contextvar keyed by `(id(hourglass), phase)`, token-restored like `node_deadline_scope`),
so nested *and* concurrent grants read `.mode` against the correct reserve. Ships with
gather + nesting regression tests.

## Ship before launch (12)

**Test rigor**
1. **Hypothesis property suite** (`tests/test_property.py`, `hypothesis` dev-only) for the
   three load-bearing invariants: clamp monotonicity (`clamp(b) ≤ b`, `≤ remaining`, `≥ 0`,
   never NaN, for arbitrary floats incl. NaN/inf); the Hourglass reserve floor; the Mode
   ladder's forward-only monotonicity. Closes the property test issue #1 promised.
2. **Targeted mutant-killing tests** for the real survivors found by a live `mutmut` run
   (78% baseline): `cooperative_wait_for`'s `reserve_secs` (currently **zero** behavioral
   coverage), `validate()`'s exact error messages, the clamp reserve-normalization value,
   boundary `<= 0` conditions.
3. **The concurrency fix above** + explicit gather/nesting tests, and document the
   run-wide-vs-per-task state model.

**API enrichment (core, stdlib-only)**
4. **Watchdog-derivation helpers** — `recommended_watchdog_secs(cap, *, grace=1.0)` and
   `node_deadline_in_under(watchdog, *, grace=1.0)`, so the "equal timeouts lose" trap has
   a callable answer instead of hand-computed `CAP - 1.0`.
5. **`run_off_loop(fn, *args, **kwargs)`** — carries the deadline contextvar into a
   thread/executor call (ships the round-1 documented escape-hatch as a helper).
6. **`Hourglass.fan_out(phase, *, cap=None)`** + a documented "concurrent fan-out under one
   grant" pattern (N `gather` branches inherit one shared wall-clock window — proven, not a
   runway splitter; true per-branch splitting only matters for the deferred token axis).

**LangGraph integration sugar (optional, lazy import — no runtime dep)**
7. **`langgraph_node_deadline.langgraph`** submodule: a `with_grant(phase, cap=…)` node
   decorator that reads an `Hourglass` off `runtime.context` via `get_runtime()`, plus an
   `add_node` helper that auto-derives the node watchdog to `cap + grace` (proven to salvage
   where an equal watchdog raises `NodeTimeoutError`). Decorator/context path — *not*
   middleware (langgraph 1.2.6 has no node middleware hook).

**Supply-chain / OSS hygiene**
8. **SHA-pin all GitHub Actions** (+ `# vX.Y.Z` comments) and **add Dependabot** (actions +
   pip dev deps) to keep the pins fresh.
9. **Post-build wheel smoke test** in `release.yml` — install the built *wheel* in a clean
   env and assert every `__all__` symbol imports and `__version__` matches the tag.
10. **`SECURITY.md` + `CONTRIBUTING.md`** (honest threat model; the dev/test/release flow).

**Docs / benchmark**
11. **Overhead micro-benchmark** (`bench/overhead.py` + README table): 54 ns fail-open clamp,
    2.4 µs full grant cycle — 5–6 orders of magnitude under an LLM/tool call.

> (Items 4–6 group the API helpers; 7 the LangGraph submodule; 8–10 supply-chain; 11 docs.
> 12 in the scope list is the fan-out docs, folded into item 6.)

## Deferred / skipped

- **Strong nice-to-have, fast-follow:** 90% coverage gate, `collect_until_deadline()`
  accumulator, PEP 740 attestations (free once the publish action is bumped), OpenSSF
  Scorecard badge, `release.yml` publish-path hardening, integration-surface README
  section, multi-node cookbook example.
- **Deferred to v0.3:** mutation testing as a one-shot audit (not a CI gate — flaky under
  its own harness), an opt-in observability callback (0.2.1), a full StateGraph
  auto-wrapper, the HALT→graceful-terminal handler, the token/$ budget axis (where
  concurrent runway-splitting becomes mathematically real), surplus reallocation.
- **Skipped:** langchain `AgentMiddleware` (not present in langgraph 1.2.6), CodeQL
  (near-zero attack surface for a pure-stdlib lib), a mkdocs/Sphinx docs site (overkill).
