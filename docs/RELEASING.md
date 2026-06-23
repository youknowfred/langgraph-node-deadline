# Releasing

`langgraph-node-deadline` publishes to PyPI via **Trusted Publishing (OIDC)** —
GitHub Actions mints a short-lived, per-run token, so **no long-lived API token
is stored** in the repo, in GitHub secrets, or in `~/.pypirc`. A leaked standing
token is the single largest supply-chain risk for a package that runs inside
production agents; Trusted Publishing removes it.

## One-time setup

1. **Register the GitHub publisher on PyPI.** On the project page →
   *Settings → Publishing → Add a new publisher → GitHub Actions*:
   - Owner: `youknowfred`
   - Repository: `langgraph-node-deadline`
   - Workflow name: `release.yml`
   - Environment name: `pypi`

   (If the project did not exist yet you would use PyPI's *pending publisher*
   form instead; it already exists at v0.1.0, so use the project settings.)

2. **Create the `pypi` GitHub Environment** (repo → *Settings → Environments → New
   environment* → `pypi`). Optionally add yourself as a *required reviewer* so a
   publish needs an explicit approval click.

3. **Revoke the old long-lived token.** Once a Trusted-Publishing release has
   succeeded:
   - PyPI → *Account settings → API tokens* → delete the `langgraph-node-deadline`
     (and any account-scoped) upload token.
   - Remove the `[pypi]` / `[testpypi]` password entries from `~/.pypirc`
     (keep only the repository URLs, or delete the file).
   - Confirm nothing references a token: `git grep -nE 'PYPI_TOKEN|TWINE_PASSWORD|pypi-'`
     must come back empty.

4. **Enable 2FA** on the PyPI account if it is not already on.

## Cutting a release

1. Land all changes on the release branch; CI (test matrix + lint) must be green.
2. Bump `version` in `pyproject.toml` **and** `__version__` in
   `src/langgraph_node_deadline/__init__.py` to the same value, and move the
   `CHANGELOG.md` entry from *unreleased* to the dated version.
3. Tag and push:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
4. The `Release` workflow builds a clean sdist + wheel, runs `twine check`, and
   (after the `pypi` environment approval, if configured) publishes via OIDC.

## Local build hygiene (if you ever build by hand)

`python -m build` only overwrites the files **it** produces — it does **not**
delete unrelated artifacts already in `dist/`. A naive `twine upload dist/*` can
therefore republish a stale build (e.g. an old `0.1.0` wheel) alongside the new
one, and a PyPI version can never be re-uploaded once consumed. Always start clean
and upload a version-pinned glob:

```bash
rm -rf dist
python -m build
twine check dist/*
# validate on TestPyPI first (separate Trusted Publisher or a scoped token):
twine upload --repository testpypi dist/langgraph_node_deadline-0.2.0*
# then the real index — but prefer the tag-triggered release.yml over manual upload:
twine upload dist/langgraph_node_deadline-0.2.0*
```

> `dist/` is git-ignored and must stay that way — built artifacts are never
> committed.
