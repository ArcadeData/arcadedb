# Bolt CI gating + nightly driver-version matrix (#4891)

Part of epic #4882 (Bolt Driver Compatibility Certification), Group D.

## Problem

The five per-language Bolt e2e suites all exist and already PR-gate in
`mvn-test.yml` (`java-e2e-tests`, `js-e2e-tests`, `python-e2e-tests`,
`csharp-e2e-tests`, `go-e2e-tests`, each `needs: build-and-package`). Three
gaps remain before Bolt compatibility is continuously certified:

1. **No comparable report artifact for four of five suites.** Only the Java
   e2e job uses `dorny/test-reporter`. JS/Python/C#/Go emit nothing
   machine-readable, so their pass/fail is invisible in the PR checks summary
   and cannot be aggregated.
2. **No scheduled run.** No cron-triggered workflow touches any Bolt suite
   (confirmed: only `benchmark-tests`, `ha-resilience-tests`, `load-tests`,
   `license-compliance`, `studio-security-audit` have `schedule:` triggers).
   Each PR run tests exactly one driver version per language; a driver-side
   release that breaks compatibility would go unnoticed.
3. **No merged, machine-readable compatibility matrix.** The publishing issue
   #4892 (site page + badge) needs a single normalized `driver x scenario x
   version` document to render; nothing produces it today.

## Scope

In scope:
- Add JUnit report aggregation to the JS/Python/C#/Go PR-gating e2e jobs.
- A new standalone nightly workflow `bolt-nightly.yml` that runs all five Bolt
  suites against the full pinned driver-version matrix from the conformance
  spec (`bolt/conformance/spec.yaml` `driver_version_bands`).
- Two small tools (with unit tests) that turn per-suite JUnit into a normalized
  per-language JSON and merge all languages into one compatibility matrix JSON.
- An auto-opened/updated tracking issue when a nightly cell regresses.

Out of scope (belongs to #4892): rendering the matrix as a site page or badge;
this issue only emits the matrix JSON artifact #4892 consumes.

Explicitly NOT changing: which suites PR-gate (all five already do); the
advertised `server_agent` version; any Bolt protocol/serialization behavior
(that is Group C: #4890 and children).

## Governing facts (verified in-repo)

- Every suite already embeds the conformance scenario ID (`CONN-001`,
  `AUTH-003`, `TX-004`, `PROTO-002`, ...) in its test name/description. JUnit
  `<testcase name>` therefore carries the scenario ID uniformly across all five
  languages, which is what makes a single JUnit-based merge possible without
  each suite hand-emitting bespoke JSON.
- Driver-version pin points, each overridable at the package-manager level
  (no test reads a driver version from an env var today):
  - Java: root `pom.xml` `<neo4j-driver.version>6.2.0`, override
    `-Dneo4j-driver.version=X`; legacy `4.4.20` via the `e2e`
    `bolt-driver-legacy` Maven profile (`RemoteBoltLegacyDriverIT`).
  - JS: `e2e-js/package.json` `neo4j-driver ^6.0.1`, override
    `npm install neo4j-driver@X`.
  - Python: `e2e-python/pyproject.toml` `neo4j>=6.2.0`, override
    `uv pip install neo4j==X`.
  - C#: `e2e-csharp/ArcadeDB.E2ETests/*.csproj` `Neo4j.Driver 6.2.1`, override
    `dotnet add package Neo4j.Driver --version X`.
  - Go: `e2e-go/go.mod` `neo4j-go-driver/v5 v5.28.4`, override
    `go get github.com/neo4j/neo4j-go-driver/v5@vX`.
- The A1 conformance spec (`bolt/conformance/spec.yaml`) already declares
  `driver_version_bands` per language: Java/JS use
  `[oldest-supported-4.x, latest-5.x, latest-6.x]`; Python/C#/Go use
  `[lts, current, latest]`. Its resolution notes defer the actual multi-version
  matrix run to this issue (#4891).

## Design

### Component 1 - JUnit aggregation on the PR-gating jobs (`mvn-test.yml`)

For each non-Java e2e job, produce a JUnit XML report, feed it to
`dorny/test-reporter` (matching the Java/unit/IT pattern already in the file),
and upload it as an artifact. No change to what these jobs test or gate.

- **JS** (`js-e2e-tests`, jest): add `jest-junit` dev dependency and a jest
  reporter config writing `e2e-js/reports/js-junit.xml`.
- **Python** (`python-e2e-tests`, pytest): run
  `pytest --junitxml=reports/python-junit.xml tests/`.
- **C#** (`csharp-e2e-tests`, dotnet): add the `JunitXml.TestLogger` package and
  `--logger "junit;LogFilePath=reports/csharp-junit.xml"` (keep the existing
  `trx` logger).
- **Go** (`go-e2e-tests`): run tests through `gotestsum`
  (`--junitfile reports/go-junit.xml`) or `go-junit-report`.

Each job gains a `dorny/test-reporter` step (`if: success() || failure()`,
`reporter: java-junit` - dorny's generic JUnit parser) and an
`actions/upload-artifact` step for its `reports/*.xml`.

### Component 2 - `bolt-nightly.yml` (new standalone workflow)

Triggers: `schedule` (`cron: "0 3 * * *"`, 03:00 UTC) and `workflow_dispatch`.

Jobs:
1. `build-image`: builds the branch Docker image once (reusing the
   `build-and-package` Maven-Docker steps) and caches it with a run-scoped key,
   so every matrix cell loads the same image rather than rebuilding.
2. Five per-language jobs (`bolt-nightly-java`, `-js`, `-python`, `-csharp`,
   `-go`), each `needs: build-image`, each:
   - `strategy: { fail-fast: false, matrix: { driver-version: [...] } }`.
   - Loads the cached image.
   - Overrides the driver version via that language's package manager
     (see pin points above).
   - Runs only the Bolt conformance suite for that language (not the whole e2e
     module - e.g. Java runs `RemoteBoltDatabaseIT` + `RemoteBoltLegacyDriverIT`
     via the `bolt-driver-legacy` profile; Python runs `tests/test_bolt.py`;
     etc.).
   - Emits JUnit XML, converts it to normalized JSON (Component 3), and uploads
     `bolt-matrix-<lang>-<version>.json` as an artifact.

Driver-version matrix (concrete versions resolved at implementation from the
band names; the `latest` band floats to the newest release so new releases are
caught, the `lts`/`oldest` bands are hard pins):

| Lang   | Nightly cells                                  |
|--------|------------------------------------------------|
| Java   | `4.4.20` (legacy profile), a `5.x`, `6.2.0`     |
| JS     | oldest `4.x`, latest `5.x`, latest `6.x`        |
| Python | `lts`, `current`, `latest`                      |
| C#     | `lts`, `current`, `latest`                      |
| Go     | `lts`, `current`, `latest`                      |

This exercises bands currently tested nowhere (Java `latest-5.x`, JS
`oldest-4.x`).

### Component 3 - normalized JSON + cross-language merge

Two Python tools under `bolt/conformance/tools/`, each with a `unittest`/pytest
test file mirroring the existing `bolt/conformance/test_validate_spec.py`:

- `junit_to_matrix.py <junit.xml> --language L --driver-version V -o out.json`:
  parses the JUnit XML, extracts the scenario ID from each `<testcase name>`
  via a regex anchored to the spec's ID pattern (`[A-Z]+-\d{3}`), maps
  `failure`/`error` -> `fail`, `skipped` -> `skip`, otherwise `pass`, and
  cross-checks every extracted ID against the ID set in `spec.yaml` (unknown ID
  -> non-zero exit, so a renamed/dropped scenario is caught, not silently
  dropped). Output:
  `{"language": L, "driver_version": V, "scenarios": {"CONN-001": "pass", ...}}`.
- `merge_matrix.py <per-cell-json...> -o bolt-compat-matrix.json`: a final
  `merge-matrix` job (`needs:` all five language jobs, `if: always()`) downloads
  every per-cell JSON artifact and produces one document keyed by
  `scenario -> language -> version -> status`, plus a `spec_version` and the
  band metadata, uploaded as the `bolt-compat-matrix` artifact. This is the sole
  input #4892 consumes.

Rationale for JUnit-name extraction over per-suite bespoke JSON: it reuses the
report artifacts Component 1 already produces, keeps all normalization logic in
one reviewed place (versus five language-specific emitters that could drift),
and the scenario IDs are already present in the test names.

### Component 4 - auto-issue on regression

A final `report` job that runs when any matrix cell failed
(`if: always()` + a step that inspects the downloaded results, or gating on the
merge job detecting a `fail`). It opens-or-updates a single tracking issue
labelled `bolt-compat-regression` via `actions/github-script`:
- Search open issues with that label and a fixed title
  (e.g. `Bolt driver compatibility regression (nightly)`).
- If found, update the body with the latest failing `language / version /
  scenario` list and a link to the run; if not, create it.
Idempotent so consecutive red nights update one issue rather than spamming.
The matrix stays `fail-fast: false` so the report reflects every cell.

## Verification

- Workflows: `actionlint` over both YAML files, plus a `workflow_dispatch` trial
  run of `bolt-nightly.yml` on the feature branch to confirm the matrix, image
  cache, artifact upload, and merge job wire up end to end.
- `junit_to_matrix.py` / `merge_matrix.py`: pytest/unittest with sample JUnit
  fixtures covering pass, `failure`, `error`, `skipped`, a testcase with no
  scenario ID, and an unknown scenario ID; plus a merge test over two
  languages/versions asserting the combined shape.
- Existing `bolt-conformance-spec.yml` continues to gate `spec.yaml` structural
  integrity.

## Acceptance criteria (from #4891)

- [ ] All five per-language Bolt suites PR-gating (already true; verify no
      regression).
- [ ] JUnit-style report aggregation added to JS/Python/C#/Go e2e jobs.
- [ ] A nightly scheduled workflow runs all five suites against the full pinned
      driver-version matrix.
- [ ] Aggregated results emitted as `bolt-compat-matrix.json` in a format #4892
      can consume.

## Risks / open points

- Concrete `latest`/`5.x`/`lts` version numbers must be resolved against actual
  releases at implementation time and recorded in the workflow (and cross-linked
  from `spec.yaml` resolution notes).
- C# JUnit logger (`JunitXml.TestLogger`) adds a NuGet dev dependency; if
  undesirable, fall back to dorny's `dotnet-trx` reporter on the existing `.trx`
  and a small trx->JSON step. Design defaults to the JUnit logger for
  uniformity.
- Nightly image build duplicates `build-and-package`; acceptable for isolation
  per the standalone-workflow decision. If build time becomes a problem, the
  nightly can instead pull `arcadedata/arcadedb:latest`.
