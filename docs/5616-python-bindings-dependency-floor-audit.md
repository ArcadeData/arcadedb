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
uv pip compile pyproject.toml --extra test --extra vector --extra examples \
  --resolution lowest-direct --python-version <each of 3.10 ... 3.14>
pip-audit --no-deps --disable-pip -r <result>
```

`pyproject.toml` is untouched apart from the floors themselves. Four details are load-bearing:

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

**Every interpreter the package claims to support is resolved.** The declared floors do not vary by
interpreter, but their transitive closure does: Python 3.10 pulls `exceptiongroup`, `tomli` and
`typing-extensions` that 3.12 does not need, and 3.14 drops one package further, so the audited set is
30 / 27 / 27 / 27 / 26 packages across 3.10 to 3.14. Auditing only one of them would leave the transitive
half of the claim untested for the rest of the supported range. `--python-version` selects the resolution
target without installing anything, which keeps this a single job rather than a five-cell matrix.

The list is read from the `Programming Language :: Python :: 3.x` classifiers rather than repeated in the
workflow, so adding an interpreter to the package cannot leave it unaudited. Reading it back has its own
failure mode, and it is guarded: an empty result exits 1 rather than looping zero times and reporting
success, which would be exactly the silent gap this job exists to close.

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

**Passes on the fixed manifest, on every supported interpreter.** `uv pip compile` and `pip-audit` both exit
0 for 3.10, 3.11, 3.12, 3.13 and 3.14 (30 / 27 / 27 / 27 / 26 pinned requirements), each reporting
"No known vulnerabilities found".

Confirmed in CI on the final commit, not only locally: the job logged
`auditing declared floors for: 3.10 3.11 3.12 3.13 3.14` and the same 30 / 27 / 27 / 27 / 26 counts, all
clean, in 27s. That run also settles the assumption the loop rests on, since the runner has only 3.12
installed: `--python-version` drives marker and `requires-python` evaluation rather than interpreter
selection, so no interpreter beyond the runner's own is needed.

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

`pip-audit` queries the advisory service over the network, so an outage there reddens the job independently
of the manifest. That is inherent to any advisory check and the reason the tool itself is pinned while the
database stays live: the version of the checker should not move on its own, but its data must.

## Review

PR: https://github.com/ArcadeData/arcadedb/pull/5632

### Cycle 1 - `9995248eb`

`claude[bot]` reviewed with nothing blocking and four observations. Two were applied, two declined.

**1. "The uv installer is unpinned while everything else in this job is pinned" - applied.** The four other
workflows that install uv all use the unpinned `https://astral.sh/uv/install.sh`, so this diverges from the
house convention on purpose: this job's output depends on how uv resolves, and `--resolution lowest-direct`
is exactly the behavior a resolver change could alter silently. Pinned to `0.12.0`, the version every result
in this document was produced with. The comment above the step says why it differs from its neighbors.

**2. "The audit only runs on Python 3.12 while the package targets 3.10 to 3.14" - applied.** The claim
checked out: `uv pip compile` resolves against the running interpreter, and the closures genuinely differ
(30 / 27 / 27 / 27 / 26 packages, with 3.10 pulling `exceptiongroup`, `tomli` and `typing-extensions`). The
job now loops over all five. It uses `--python-version` rather than the suggested job matrix, since nothing
is installed (`--disable-pip`), so selecting the resolution target is enough and it stays one ~1 minute job.
All five audit clean today.

**3. "The job inherits `workflow_run` and `workflow_call` triggers, so it re-runs where the manifest has not
changed" - declined.** Correct, but the sibling Bandit job in this same workflow has exactly the same
property and no `if:` guard. Adding one only here would make the two security jobs behave differently for no
stated reason. Both are around a minute. The follow-up below about a separate trigger is the right place to
address it, for both jobs at once.

**4. "`export PATH` in the Setup UV step only affects that step shell" - no action, and the review agrees.**
That block is copied verbatim from the existing uv setup in this same workflow. It is correct as written:
`$GITHUB_PATH` carries uv to later steps and the export exists so `uv --version` works in the same step.

### Cycle 2 - `e291992d8`

`claude[bot]` reviewed again, nothing blocking, three observations. Two were applied, one needs no change.

**1. "The hardcoded `3.10 3.11 3.12 3.13 3.14` can drift from `requires-python`" - applied, more strongly
than suggested.** The review proposed a "keep in sync" comment next to the classifiers. A comment rots on
the same schedule as the list it guards, and the drift it warns about is silent: the job stays green while
covering less. The loop now reads the versions from the classifiers, which removes the coupling rather than
documenting it. That substitutes one silent-failure mode for another, so the empty result is guarded and
exits 1; verified by stripping the classifiers from a copy of the manifest and confirming the step fails
with `no Python version classifiers found`.

**2. "`::endgroup::` is skipped on failure, leaving the group unclosed" - applied.** The group now closes
after the resolve and the `cat`, which is the verbose part worth collapsing, and `pip-audit` runs outside
it. A failure is therefore never inside an open group, and its output is not collapsed on a red run, which
is when it most needs reading.

**3. "The `bandit` floor bump is the one floor this job cannot check" - correct, no change.** `bandit` lives
only in the `dev` extra, which is excluded for the reasons above, and the Bandit job pins `bandit==1.9.4`
regardless, so `>=1.9.1` is purely declarative. It is still worth correcting: `1.9.0` was never published to
PyPI, so the old floor named a release that does not exist. This is a specific instance of the `dev` extra
follow-up below.

### Cycle 3 - `b163362e0`

`claude[bot]` reviewed again, nothing blocking. Two points were acted on, two need no change.

**1. "The trailing dot in the classifier prefix is load-bearing and worth an inline note" - applied.**
Verified: the classifiers carry a bare `Programming Language :: Python :: 3` alongside the five versioned
ones, and dropping the dot yields `3 3.10 3.11 3.12 3.13 3.14`, whose first element becomes
`--python-version 3` and fails. The review is right that this breaks silently under tidying, so the prefix
now carries a comment saying why the dot is there.

**2. "The loop is verified only locally; a wrong `--python-version` assumption would break every cell except
3.12" - applied.** The concern was fair: the CI figure quoted in this document came from the earlier
single-interpreter version of the job. The loop has now run in CI on the final commit and the section above
quotes that run instead, which also confirms `--python-version` needs no interpreter beyond the runner's.

**3. "`pip-audit` depends on the network, so the job can flake on an advisory-service outage" - no change,
noted.** Recorded under Impact. Inherent to any advisory check, and the reason the checker is pinned while
its data is not.

**4. "The uv installer is pinned but not checksum-verified" - declined, and the review expects no change.**
Consistent with every other `curl | sh` install in this repo. Adding a checksum here alone would not raise
the floor for the workflows that surround it, and pinning the version already removes the drift that
mattered for this job's output.

### Cycle 4 - `9dc2df33a`

`claude[bot]` reviewed again, nothing blocking, three observations. None resulted in a change; the first was
checked against the repo and its premise does not hold.

**1. "This job now sits in the Release path via `workflow_call`, so an advisory-service outage can redden a
release run" - declined, the premise is not true of this repo.** The workflow does declare `workflow_call`,
but nothing calls it: `grep -rn "uses: \./\.github/workflows/"` over `.github/workflows/` returns no
matches, and `mvn-release.yml`, the only release workflow, calls no reusable workflow at all. The trigger is
declared and unused. The related `workflow_run: workflows: ["Release"]` trigger runs this workflow *after* a
release completes, so it cannot gate one either. Nothing here can redden a release, and the `if:` guard the
review suggests would be guarding a path that does not exist. The trigger-tightening follow-up below still
stands on its own merits, for this job and the Bandit job together.

**2. "`jpype1==1.5.0` is the cell most likely to break when a new interpreter joins the classifiers" - no
change, recorded as a watch item.** Accurate: because the loop now derives its versions from the
classifiers, adding 3.15 there automatically adds a resolution the floor may not satisfy, and that surfaces
as a `uv pip compile` error rather than an advisory. That is the correct failure: it says the declared floor
does not support the interpreter the package claims to support, which is a real defect in the manifest
rather than a defect in the job.

**3. "`--vulnerability-service osv` aggregates more feeds" - declined.** The PyPI default is what produced
every result in this document, including the `PYSEC-2022-42998` the falsification run turns on. Switching
sources changes what the gate reports and would need its own before-and-after, which is not worth bundling
into the change that introduces the gate.

## Follow-ups

- Meterian still cannot scan `bindings/python`, and the issue's four directions all remain open. This makes
  the coverage gap non-silent rather than resolving it. Closing it properly means either restructuring so
  the wheel definition is not the file that must be unmanaged, or dropping `managed = false` and accepting
  the change to `uv run` discovery.
- `black`, `isort`, `mypy` and `pytest-cov` declare no lower bound, so no tool can audit them. Giving them
  floors would let the `dev` extra join the job.
- The job only runs on pushes touching `bindings/python/**`. A new advisory against an unchanged floor is
  not noticed until the next such push. A schedule would catch that, but this workflow also carries the
  20-cell build matrix, so it needs a separate trigger rather than a `schedule:` on this file. Moving this
  job and the Bandit job to a workflow of their own would give them that trigger and would also drop the
  `workflow_run` and `workflow_call` triggers they inherit today and do not use.
- When an interpreter is added to the classifiers the loop picks it up automatically, and a declared floor
  that has no dist for it will fail as a `uv pip compile` error rather than an advisory. `jpype1>=1.5.0` is
  the likeliest such floor.
- Meterian is red on `main` continuously for an unrelated reason (`e2e-go/go.mod` pulling
  `google.golang.org/grpc@v1.67.0`, CVE-2026-33186), which is what made this failure easy to miss.
