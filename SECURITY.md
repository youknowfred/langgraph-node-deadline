# Security policy

## Supported versions

The latest `0.2.x` release receives security fixes.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue.

Use GitHub's private vulnerability reporting: go to the repository's **Security**
tab → **Report a vulnerability** (this requires "Private vulnerability reporting"
to be enabled in repo settings → Security). You'll get a private advisory thread
with the maintainer.

You can expect an acknowledgement within about **7 days**. Once a fix is ready,
we'll coordinate a release and a disclosure timeline with you, and credit you in
the advisory unless you'd prefer to remain anonymous.

## Threat model (what's realistic here)

`langgraph-node-deadline` is a small, pure-Python, **zero-runtime-dependency**
library. It has no network, subprocess, `eval`/`exec`, deserialization, or file I/O
surface — so the realistic risk areas are narrow and worth being explicit about:

- **Supply chain (build & release).** A compromised release is the highest-impact
  risk for any package that runs inside production agents. This repo mitigates that
  with **PyPI Trusted Publishing** (OIDC, no long-lived token — see
  [`docs/RELEASING.md`](docs/RELEASING.md)), **SHA-pinned GitHub Actions** kept fresh
  by Dependabot, a tag-triggered release with a required-reviewer environment gate,
  and PEP 740 build attestations on published artifacts.
- **Incorrect-deadline logic.** Because the package's job is to make timeouts behave,
  a bug that lets the clamp *widen* past the binding deadline (reopening the
  uncooperative-kill window) is a reliability defect worth reporting here. The core
  invariants are pinned by property-based tests.

The library is **fail-open by design**: with no active deadline scope, every
function behaves as if absent. That is intentional, not a vulnerability.
