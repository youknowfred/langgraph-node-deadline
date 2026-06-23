# Pre-launch hardening roadmap (v0.2)

A security- and compatibility-first pass over `langgraph-node-deadline` before its
public launch. This document **first surfaces** every concern, then fences each into
a tier with a concrete fix and a regression test. It is the source of truth for the
hardening PRs that follow; nothing here weakens the package's two load-bearing
guarantees — **fail-open** (no active scope ⇒ every function behaves as if absent)
and **zero runtime dependencies** (the core imports stdlib only).

> Status: **audit complete; Tiers 1–4 implemented and CI-green** on
> [PR #2](https://github.com/youknowfred/langgraph-node-deadline/pull/2)
> (folded into the v0.2 line). 59 tests pass (54 unit + 5 LangGraph integration);
> `ruff` / `mypy --strict` / `pyright` / `vermin` (3.9) clean. The only remaining
> launch step is a tagged release through the new Trusted-Publishing workflow
> (owner action; not done here).

## How this was produced

A deliberately adversarial review across **ten** dimensions — five security, five
compatibility — each tasked with *disproving* the package's claims rather than
confirming them. Every candidate finding was then independently re-verified (an
attempt to refute it, plus a check that the proposed fix preserves fail-open /
zero-dep / 3.9 support) before it was allowed to count. Empirical claims were
reproduced against real toolchains:

- **3.9 compatibility** — `vermin --target=3.9-` (exit 0) and an `--eval-annotations`
  pass; the CI matrix already runs 3.9–3.13 green on PR #2.
- **Async-cancellation safety** — adversarial probes under `python -W error::RuntimeWarning`
  (early break, deadline-trip mid-`__anext__`, external cancel, no-`aclose` iterator).
- **Packaging** — `python -m build` + `twine check --strict` (both pass), wheel/sdist
  metadata inspection, byte-for-byte reproducible-build check, runtime dependency
  closure audit.
- **Typing** — `mypy --strict` and `pyright` (default + strict) on `src/`.
- **LangGraph framing** — claims reproduced against a real installed LangGraph
  (`StateGraph` + `step_timeout`).

Result: **21 confirmed findings, 2 candidate findings rejected on verification.**
The core is sound — no code-execution/injection surface, no `CancelledError`
swallowing, no resource leaks under cancellation, genuinely zero runtime deps, and
3.9–3.13 clean. The findings are hardening and accuracy, not architecture.

## Dimension scorecard

| Dimension | Verdict | Confirmed |
| --- | --- | --- |
| Security · code-exec / injection | **Clean** — no eval/exec/pickle/subprocess/dynamic-import; stdlib-only | 0 |
| Security · async-cancellation | **Strong** — no swallowed cancels, no warnings/leaks under `-W error` | 1 |
| Security · resource / DoS / state | Robust core; one fail-unsafe edge in `cooperative_poll` | 4 |
| Security · event-loop & threads | Correct; one documentation **overclaim** | 1 |
| Security · supply-chain / packaging | Strong artifact integrity; release **process** is the exposure | 4 |
| Compat · CPython 3.9+ | **Confirmed** (min 3.7; one forward-proofing edge) | 1 |
| Compat · asyncio drift 3.9→3.13 | Stable salvage contract across versions | 3 |
| Compat · typing / static analysis | One real `mypy --strict` gap; rest is strict-mode noise | 3 |
| Compat · packaging / install matrix | Builds clean; backend floor unpinned | 2 |
| Compat · LangGraph framing | Core is honest, but docs cite a **non-existent API** | 4 |

## Findings, tiered

Severity is the *verified* severity. "Reg test" names the regression guard that must
ship with the fix so the value/behavior can never silently regress.

### Tier 1 — Code correctness (each ships with a regression test)

| # | Sev | Finding | Fix | Reg test |
| --- | --- | --- | --- | --- |
| T1.1 | med* | **`cooperative_poll` silently drops the deadline guard when a caller passes custom `predicates`.** Passing `predicates=` *replaces* the implicit `[node_deadline_exceeded]` rather than augmenting it; combined with `bound_each_chunk=False` an **active** node-deadline scope is ignored entirely (fail-*unsafe*, distinct from fail-open). | Union, never replace: `preds = [node_deadline_exceeded] + (list(predicates) or [])`. The node deadline is always a stop condition; custom predicates are additional. Document `bound_each_chunk` precisely. | Under `node_deadline_in(0.3)` with `predicates=[lambda: False]`, the loop still stops at the deadline; `test_cooperative_poll_honors_a_custom_predicate` stays green. |
| T1.2 | low | **Negative `reserve_secs` lets `clamp_to_node_deadline` exceed the binding deadline** (`min(budget, remaining − reserve)` widens when `reserve<0`), reopening the uncooperative-kill window. | Floor it: `reserve_secs = max(0.0, reserve_secs)` (after the fail-open early return). Document `reserve_secs` as a non-negative headroom floor. | `clamp_to_node_deadline(100, reserve_secs=-5)` under a 10s scope is `<= remaining`. |
| T1.3 | low | **`validate()` accepts `total_secs=NaN`** (`nan <= 0` is `False`), defeating the "fail loud at startup" contract — the run silently degrades to HALT instead of crashing. | `math.isfinite` guards in `validate()` for `total_secs`, each reserve, and `conserve_margin` (stdlib, 3.9-safe). | `Hourglass(float('nan')).validate()` raises `ValueError(match="finite")`; coherent envelope still validates. |
| T1.4 | info | **`clamp_to_node_deadline` swallows NaN in an order-dependent way** (`min/max` NaN semantics). Benign today, latent. Bundle with T1.3. | After the fail-open early return, normalize: non-finite `budget_secs`/`reserve_secs` → `0.0` (conservative; a NaN can never widen the clamp). | `clamp_to_node_deadline(nan) == 0.0`; `clamp(5, reserve_secs=nan) <= remaining`. |
| T1.5 | low | **`cooperative_poll` is fully untyped** — the sole `mypy --strict` error (`__init__.py:187`) and the bulk of `pyright --strict` errors; downstream callers lose the chunk type. | Annotate with **`typing.Union`/`AsyncIterable`, not PEP 604 `\|`** (which breaks `get_type_hints()` on 3.9): `aiter: AsyncIterable[_T]`, `predicates: Optional[Sequence[Callable[[], bool]]] = None`, `bound_each_chunk: bool = True`, `-> AsyncIterator[_T]`. | `mypy --strict src/` clean; `get_type_hints(cooperative_poll)['return']` resolves on 3.9. |
| T1.6 | med | **`cooperative_poll` leaks the upstream stream when the *consumer* exits the `async for` early** (`return`/`break`/`raise`). Because `cooperative_poll` is itself an async generator suspended at `yield`, its `finally` (and thus `aclose()`) doesn't run until non-deterministic GC/loop-shutdown — the most common production path (a node returns the moment it has enough). | Keep the ergonomic `async for`, but make teardown deterministic & documented: show `async with aclosing(cooperative_poll(...))` and ship a 4-line **3.9-compatible `aclosing` shim** (stdlib `contextlib.aclosing` is 3.10+) or a `cooperative_poll_cm(...)` helper. Zero deps. | Upstream `closed is True` on consumer `return`/`break`/`raise` when wrapped; fail-open (no scope ⇒ yields all, then closes). |

\* T1.1 is rated *low* on raw severity (needs a non-default flag + custom predicate)
but is treated as **Tier 1** because "an active scope is silently ignored" cuts
against the package's core promise for exactly its target audience.

### Tier 2 — Documentation & positioning accuracy (credibility-critical)

| # | Sev | Finding | Fix |
| --- | --- | --- | --- |
| T2.1 | **high** | **Inaccurate LangGraph timeout framing.** The README and both demos cited `TimeoutPolicy` loosely as "the watchdog." The audit (run against LangGraph **1.1.2**) found no such class and flagged it as fictitious. Re-verifying against **current LangGraph (1.2.6)** refined the picture: `TimeoutPolicy` **is real now** — a *per-node* timeout via `add_node(..., timeout=...)` that raises `NodeTimeoutError` — and there is also the graph-wide `step_timeout` (super-step) that raises `TimeoutError`. The docs must name both precisely so a reader can wire it without hitting a dead end. | State both watchdogs accurately: per-node `add_node(timeout=...)` / `TimeoutPolicy` (recent LangGraph) and graph-wide `step_timeout`; show real wiring for the per-node path; note both cancel via asyncio cancellation so the salvage mechanic is identical. Proven by the T4.1 integration test. |
| T2.2 | med | **"every subagent task" overclaims thread propagation.** `create_task` and `asyncio.to_thread` copy the deadline contextvar; **`loop.run_in_executor(None, fn)` and raw `threading.Thread` do not** — there the helpers silently fail open and the deadline is *not* enforced. The target audience routinely wraps sync tools in executors. | Tighten the wording to the truth; add the escape hatch (`asyncio.to_thread`, or `ctx = contextvars.copy_context(); loop.run_in_executor(None, lambda: ctx.run(fn))`), optionally an opt-in `run_off_loop` helper (pure stdlib). |
| T2.3 | med | **`step_timeout` is super-step-wide, not per-node.** "this node gets N seconds" / "match this to your TimeoutPolicy" implies a per-node cap; LangGraph cancels the *whole tick* (all parallel nodes) on expiry. A clamp that fits one node can still die if a sibling overruns the shared step. | One paragraph in "Wiring it into a real LangGraph node": size the deadline against the shared `step_timeout`; a sibling overrun can cancel the tick. Qualify the headline once in the body. |
| T2.4 | info | **`cooperative_wait_for` returns an already-complete result at a blown deadline** (the `wait_for` done-fast-path) instead of raising — correct and version-stable, but undocumented. | One docstring line: "If the awaitable is already complete, its result is returned rather than raising — a finished value is not discarded." |
| T2.5 | — | **Missing "when NOT to use this" + honest comparison** to `asyncio.timeout` / tenacity / Cycles (Step B docs). | Add a short, honest positioning section. |

### Tier 3 — Packaging, CI & supply-chain hardening

| # | Sev | Finding | Fix |
| --- | --- | --- | --- |
| T3.1 | **high** | **No release workflow; publishing relies on a long-lived `~/.pypirc` token** — the single largest supply-chain exposure. A leaked token lets anyone push arbitrary releases of a package that runs inside production agents. | Adopt **PyPI Trusted Publishing (OIDC)**: add `.github/workflows/release.yml` (`pypa/gh-action-pypi-publish`, `id-token: write`, tag-triggered, `environment: pypi`), register the GitHub publisher on PyPI, then **revoke the long-lived token** in `~/.pypirc`. No package code touched. |
| T3.2 | low | **Unpinned `hatchling` mis-emits PEP 639 license metadata in non-isolated builds.** `requires = ["hatchling"]` + SPDX `license = "MIT"` needs hatchling ≥ 1.27; older silently downgrades to Metadata 2.3 (or hard-fails). | Pin `requires = ["hatchling>=1.27"]`. Keeps PEP 639; deterministic 2.4 / `License-Expression` everywhere. |
| T3.3 | low | **Stale `dist/` 0.1.0 artifacts in the working tree** — a naive `twine upload dist/*` would re-publish 0.1.0 over the 0.2 release (a PyPI version can never be re-uploaded). `dist/` is correctly gitignored/untracked. | `rm -rf dist/` before building; release flow `rm -rf dist && python -m build && twine check dist/* && twine upload`. Document in a release checklist; `release.yml` sidesteps it by building fresh in CI. |
| T3.4 | low | **CI has no warnings-as-errors gate and runs only `salvage_demo.py`.** An asyncio `DeprecationWarning` on a new CPython would pass silently; `hourglass_demo.py` (which exercises `cooperative_poll`+`grant`) is never run. | Add `python examples/hourglass_demo.py`; run `pytest -q -W error::DeprecationWarning -W error::PendingDeprecationWarning` (scope unrelated pytest-asyncio warnings via `filterwarnings`). |
| T3.5 | info | **Neither `mypy` nor `pyright` runs in CI** for a `py.typed` package. | Add a single-version lint job: `mypy --strict src/` + `pyright src/` (after T1.5 makes it green). Tools live in an extra, not core deps. |
| T3.6 | low | **Strict-pyright `reportDeprecated`** on `@contextmanager` `-> Iterator[...]` (×3). Strict-mode only. | Optional: switch the three returns to `Generator[..., None, None]` (3-arg, 3.9-safe), or suppress via pyright config. |
| T3.7 | info | **`set[str]` annotation at `hourglass.py:203`** is the one construct that would break sub-3.9 if `from __future__ import annotations` were ever removed. Inert today. | Optional forward-proof: `Set[str]` (add `Set` to the `typing` import); add a `vermin --eval-annotations` CI guard. |
| T3.8 | low | **Version `0.2.0.dev0` vs README advertising the v0.2 API.** `pip install langgraph-node-deadline` skips pre-releases by default → a user could get 0.1.0 and hit `ImportError: Hourglass`. | **Launch decision** (owner): cut the launch as non-dev `0.2.0`, or gate the Hourglass section behind an "available in 0.2+" note. Add a wheel-import smoke test so README symbols can't drift from the package. |

### Tier 4 — Enhancements (Step B)

| # | Finding / opportunity | Decision |
| --- | --- | --- |
| T4.1 | **No real-LangGraph proof of the salvage claim.** The whole pitch ("`try/except` runs *before* the kill") is only demonstrated against a simulated `asyncio.wait_for`. Verified against LangGraph 1.1.2: the clamp pattern salvages, and the *intuitive* DIY fix (catch `CancelledError`, return partial) does **not** — LangGraph discards the late return. The package's mechanism is the only one that works; that's a selling point currently left unproven. | **Build it.** Hermetic `tests/test_langgraph_integration.py` under an optional `[langgraph]` extra; `pytest.importorskip("langgraph")` CI guard; one-node `StateGraph` with `app.step_timeout`, node uses `node_deadline_in` + `cooperative_wait_for` (pure `asyncio.sleep`, no network). Zero runtime dep. |
| T4.2 | **`asyncio.timeout` fast-path for 3.11+.** | **Skip.** `wait_for` is correct on all supported versions and is itself implemented on `asyncio.timeout` on 3.12+; a manual branch adds a version fork for zero benefit. Record the rationale in a one-line comment. |
| T4.3 | **`ruff` not gated in CI** (already passes). | Add a lint step. |
| T4.4 | **`cooperative_poll` ergonomics** — `on_salvage` hook / return-value accumulator. | Evaluate after T1.6; only if the API stays clean. |
| T4.5 | **Concurrent fan-out sub-budget** (deferred from v0.2). | Defer to v0.3 — only build if it lands clean (per issue #1's out-of-scope discipline). |

## Rejected on verification (recorded for honesty)

- **"Package name unclaimed → squat window."** Refuted: the package is published as
  v0.1.0, owned by the maintainer, and the published wheel is byte-identical
  (same sha256) to a local build. Generic 2FA/Trusted-Publishing advice still applies
  (→ T3.1), but the squat premise is false.
- **"#5672 addresses an adjacent failure."** The mechanism is verified, but the README
  framing ("prevent the cancel so partial work survives") is honest. The genuine
  nuance (super-step-wide cancellation) is captured in **T2.3** instead.

## Deferred to v0.3+ (out of scope here)

Surplus reallocation / debt repayment, token & dollar budget axes, a forward-degrade
`HALT`→graceful-terminal handler, and the concurrent fan-out sub-budget API. The
static reserve already captures ~80% of the value; `langgraph-node-deadline` owns
**time orchestration**, nothing else.

## Execution plan

1. **Tier 1** (code, regression-tested) → PR. CI green on 3.9–3.13.
2. **Tier 2** (docs/positioning) → PR. The fictitious-API fix (T2.1) is the highest
   single credibility item.
3. **Tier 3** (packaging/CI/supply-chain) → PR, incl. Trusted-Publishing `release.yml`.
4. **Tier 4** (LangGraph integration test + lint gates) → PR.

Owner sign-off requested between tiers. No public launch and no PyPI publish without
explicit owner approval (TestPyPI is fine for validation).
