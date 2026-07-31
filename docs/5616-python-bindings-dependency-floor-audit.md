# 5616 - bindings/python is outside dependency scanning

Issue: https://github.com/ArcadeData/arcadedb/issues/5616

## Root cause

`bindings/python/pyproject.toml` carries:

```toml
[tool.uv]
managed = false
```

`uv lock` refuses outright on an unmanaged project, so Meterian's `UvDependencyGenerator` logs
`Unable to generate lockfile` on every run and the folder is never scanned. The flag is deliberate: it
makes uv skip this directory during project discovery, so `uv run` from here resolves to the dev project
rather than treating the wheel definition as a uv project.

Verified locally against a copy of the manifest, `uv 0.12.0`:

| command | exit | result |
|---|---|---|
| `uv lock` | 2 | `error: The project is marked as unmanaged` |
| `uv lock` with a `uv.lock` already present | 2 | same refusal, the lockfile is not consulted |
| `uv lock` with `managed = false` removed | 0 | resolves 51 packages |
| `uv pip compile --all-extras` | 0 | works, the pip interface does no project discovery |

The second row rules out the issue's direction 1 on its own: committing a `uv.lock` does not help, because
`uv lock` still refuses before reading it. Making Meterian work therefore requires deleting `managed = false`,
which is the one thing the issue rules out.

## Fix

A `dependency-floors` job in `.github/workflows/test-python-bindings.yml`, beside the existing Bandit job,
which scans the folder with our own tooling instead of waiting on Meterian:

```
uv pip compile pyproject.toml --extra test --extra vector --extra examples --resolution lowest-direct
pip-audit --no-deps --disable-pip -r <result>
```

`pyproject.toml` is untouched apart from the floors themselves. Three details are load-bearing:

**`--resolution lowest-direct`.** The default resolution picks the newest release each constraint admits, so
it audits versions nobody is pinned to and a floor left open to a vulnerable release stays invisible. On the
current manifest the default resolves `numpy` to 2.5.1 while the declared floor is 1.26.4. `lowest-direct`
pins each declared dependency to the oldest release its own constraint admits, which is the version the
manifest actually promises to work with, and resolves transitive dependencies normally. It also absorbs a
floor that names a release which was never published: `bandit>=1.9.0` resolves to 1.9.1, the oldest that
exists.

**The `dev` extra is excluded.** `black`, `isort`, `mypy` and `pytest-cov` declare no lower bound at all, so
`lowest-direct` drops them onto their earliest PyPI releases: `mypy` 0.1, which depends on `yaro`, which
depends on `wsgiref` 0.1.2, which is Python 2 source and fails to build. Those four are contributor tooling
and are never installed from the wheel. The three extras that a wheel user can install are all covered.

**`--disable-pip`.** Without it `pip-audit` hands the pinned file to pip, which needs metadata and will build
an sdist when no wheel matches the runner's interpreter. A regressed `numpy>=1.20.0` failed that way during
testing, with `Cannot import 'setuptools.build_meta'` instead of the advisory that caused it. Since the file
is fully pinned there is nothing to resolve, and `--disable-pip` reads it directly.

### Floors raised

The job was red on the manifest as it stood, on floors that PR #5548 did not reach:

| dependency | was | now | advisories at the old floor |
|---|---|---|---|
| `requests` | `>=2.0.0` | `>=2.33.0` | PYSEC-2014-13, PYSEC-2014-14, PYSEC-2018-28, PYSEC-2026-1872, PYSEC-2026-1873, PYSEC-2026-2275 |
| `pytest` | `>=7.0.0` | `>=9.0.3` | PYSEC-2026-1845 |
| `bandit` | `>=1.9.0` | `>=1.9.1` | none; 1.9.0 was never published to PyPI |

Each new floor is the lowest release that clears every advisory against it, confirmed by auditing
`requests==2.33.0` and `pytest==9.0.3` directly (exit 0, "No known vulnerabilities found"). `pytest>=9.0.3`
resolves on Python 3.10, the oldest interpreter in the test matrix.

## Verification

Both directions were run, so the job is known to fail as well as to pass.

**Passes on the fixed manifest.** `uv pip compile` exit 0, 27 pinned requirements, `pip-audit` exit 0,
"No known vulnerabilities found".

**Fails on a regressed manifest.** Reverting the two floors PR #5548 raised (`py7zr>=0.20.0`,
`numpy>=1.20.0`) makes `pip-audit` exit 1 and name exactly the advisories the issue cites:

```
Found 7 known vulnerabilities in 2 packages
numpy 1.20.0  CVE-2021-33430    1.21
numpy 1.20.0  CVE-2021-34141    1.22
py7zr 0.20.0  PYSEC-2022-42998  0.20.1     <- CVE-2022-44900
py7zr 0.20.0  PYSEC-2026-2974   1.1.3
py7zr 0.20.0  PYSEC-2026-2973   1.1.3
py7zr 0.20.0  PYSEC-2026-2972   1.1.3
```

`PYSEC-2022-42998` is CVE-2022-44900, the directory traversal in `SevenZipFile.extractall` that
`examples/download_data.py` calls on downloaded archives, and the three `PYSEC-2026-297x` entries are the
decompression-bomb advisories. This is the regression the issue says currently lands silently.

**No regression in the existing jobs.** Both Bandit commands still exit 0 against the edited
`pyproject.toml` (which holds the `[tool.bandit]` config), and the file still parses as TOML.

Raising the floors cannot affect what CI installs: no workflow or script installs an extra. The bindings
test job installs by name (`uv pip install --system test-install/*.whl pytest pytest-cov requests numpy`)
and the examples job does the same, so the only consumer of `--extra` anywhere in the repo is the new job.
The extras are a declarative promise to whoever installs `arcadedb-embedded[test]` from PyPI, and until now
nothing exercised them at all.

## Impact

The one Python artifact we publish now has its declared dependency floors checked on every push and pull
request that touches `bindings/python/**`. The job is cheap: one resolve and one advisory lookup, no build,
no wheel, roughly a minute, and it does not join the existing build matrix.

The signal is deterministic. It changes only when someone edits the manifest or when a new advisory is
published against a version already at a floor, and in both cases the action is the same: raise the floor.
That is deliberately narrower than auditing the resolved-newest set, which would also go red for advisories
in contributor-only tooling and would drift toward being ignored, which is how the Meterian failure went
unnoticed in the first place.

The gap this does not close: an advisory affecting only releases above the floor. The floor stays clean, so
the job stays green, while a user installing today gets the newest release. Meterian would cover that if it
could read the folder.

## Follow-ups

- Meterian still cannot scan `bindings/python`, and the issue's four directions all remain open. This makes
  the coverage gap non-silent rather than resolving it. Closing it properly means either restructuring so
  the wheel definition is not the file that must be unmanaged, or dropping `managed = false` and accepting
  the change to `uv run` discovery.
- `black`, `isort`, `mypy` and `pytest-cov` declare no lower bound, so no tool can audit them. Giving them
  floors would let the `dev` extra join the job.
- The job only runs on pushes touching `bindings/python/**`. A new advisory against an unchanged floor is
  not noticed until the next such push. A schedule would catch that, but this workflow also carries the
  20-cell build matrix, so it needs a separate trigger rather than a `schedule:` on this file.
- Meterian is red on `main` continuously for an unrelated reason (`e2e-go/go.mod` pulling
  `google.golang.org/grpc@v1.67.0`, CVE-2026-33186), which is what made this failure easy to miss.
