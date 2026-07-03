# Bolt Conformance Spec

Shared, language-neutral certification scenarios for ArcadeDB's Bolt
protocol implementation, referenced by issue #4883 (part of epic #4882).
`spec.yaml` in this directory is the single source of truth for what
"certified" means across all five official Neo4j driver ecosystems (Java,
JavaScript, Python, C#, Go).

Every scenario here is only meaningful relative to the server identity
ArcadeDB advertises to drivers - see
[`../SERVER_IDENTITY.md`](../SERVER_IDENTITY.md) for the exact advertised
strings, why `5.26.0` was chosen, and the 4 confirmed gaps behind that
claim (already reflected in this spec's `expected-fail` scenarios below).

## Consumption model

**No language's test suite parses `spec.yaml` at runtime.** This file is a
reference document: for each scenario, a human hand-writes the equivalent
test in that language's existing idiomatic framework - jest (`e2e-js`),
pytest (`e2e-python`), xUnit (`e2e-csharp`), `go test` (`e2e-go`, new
module), or JUnit (`bolt`/`e2e` for Java). Do not build a YAML-driven test
generator or interpreter for this spec in any language - that is explicitly
out of scope.

The only place `spec.yaml` is ever parsed programmatically is the future CI
aggregation step (issue #4891/#4892), which cross-references JUnit-style
test report output against the scenario list to build the published
compatibility matrix. That is one script, not five.

## Traceability convention

Every hand-written test for a scenario in this spec must embed that
scenario's `id` so coverage can be checked by grepping test names - for
example:

- Java/JUnit: `@DisplayName("[TYPE-011] Duration round-trips as a native Bolt Duration structure")`
- JS/Jest: `test('[TYPE-011] duration round-trip', async () => { ... })`
- Python/pytest: `def test_TYPE_011_duration_roundtrip(): ...`
- C#/xUnit: `[Fact(DisplayName = "TYPE-011: Duration round-trip")]`
- Go/`go test`: `func TestTYPE011_DurationRoundtrip(t *testing.T) { ... }`

## Scenario fields

Each entry in `scenarios:` has:

| Field | Meaning |
|---|---|
| `id` | Stable identifier, `AREA-NNN` |
| `area` | One of the 9 feature-matrix areas (see below) |
| `title` / `description` | What the scenario exercises |
| `fixture` | `beer` or `type_matrix` (see Fixtures below) |
| `preconditions` | Setup required beyond the fixture, if any |
| `steps` | Given/When/Then-style steps a human translates into test code |
| `applicable_driver_versions` | `all`, or a list of `language:band_name` refs from `driver_version_bands` |
| `current_status` | `passing`, `expected-fail`, `not-applicable`, or `unverified` |
| `known_limitation` / `tracking_issue` | Required when `current_status: expected-fail` |

**`current_status` is a starting hint, not a verdict.** `passing` and
`expected-fail` reflect what direct source-code inspection confirmed at
spec-authoring time (2026-07-03); `unverified` scenarios were not directly
confirmed either way and are exactly what each B-task's first implementation
run is meant to determine. If a B-task finds an `unverified` scenario
actually fails and the failure looks like a real gap (not scenario error),
open a new tracking issue (or fold it under `#4890` if related) and update
`tracking_issue`/`known_limitation` here in the same PR.

## Areas

`connection`, `auth`, `transactions`, `causal-consistency`,
`multi-database`, `result-handling`, `type-roundtrip`, `errors`, `protocol` -
verbatim from the epic #4882 feature-matrix table, so spec coverage can be
checked directly against that table.

Each area has a fixed `id` prefix, enforced by `validate_spec.py` - not
derivable from the area string, so it's tabulated here rather than
reverse-engineered:

| Area | id prefix |
|---|---|
| `connection` | `CONN` |
| `auth` | `AUTH` |
| `transactions` | `TX` |
| `causal-consistency` | `CAUSAL` |
| `multi-database` | `MDB` |
| `result-handling` | `RESULT` |
| `type-roundtrip` | `TYPE` |
| `errors` | `ERR` |
| `protocol` | `PROTO` |

## Driver version bands

`driver_version_bands` in `spec.yaml` defines symbolic bands per language
(e.g. `latest-6.x`, `lts`) rather than concrete version numbers, so the spec
doesn't go stale between now and whenever each B-task actually starts. Each
B-task resolves its own bands to concrete versions and records them in its
own build manifest (pom.xml / package.json / requirements.txt / .csproj /
go.mod) at implementation time.

## Fixtures

- **`beer`**: the pre-existing OpenBeer dataset, imported remotely at
  container boot via `ArcadeContainerTemplate` - already shared by Java
  (over Bolt) and Python (over Postgres wire, not Bolt) e2e today. Covers
  most scenarios.
- **`type_matrix`**: `fixtures/type-matrix.cypher`, a small local fixture
  supplementing `beer` with rows for the type-round-trip matrix (temporal,
  Duration, Point, nested collections, nulls). **Must be seeded via HTTP
  `/command` (SQL or Cypher) or the embedded API - never via a Bolt
  session** - seeding through Bolt would exercise the very serialization
  path several `type-roundtrip` scenarios exist to test, invalidating the
  result.

## Validating the spec

Requires PyYAML (`pip install -r requirements.txt`, or just `pip install
pyyaml` - it's a dev-tooling dependency for this validator only, not part of
any shipped ArcadeDB artifact).

```bash
cd bolt/conformance
pip install -r requirements.txt   # once
python3 validate_spec.py spec.yaml
python3 -m unittest test_validate_spec.py -v
```

Checks structural integrity: unique scenario IDs, valid `area`/
`current_status` values matching the declared `id` prefix for that area,
fixture references that resolve to a declared fixture, driver-version
references that resolve to a declared language/band, every area has at
least one scenario, and the 4 epic-confirmed gaps (single-node-only ROUTE,
missing `Neo.TransientError.*`, no Bolt 5.x negotiation, type-fidelity gaps)
each have a `#4890`-tracked `expected-fail` scenario. Run this after any
edit to `spec.yaml`.

`test_validate_spec.py` imports `validate_spec` as a bare module, so it must
be run from inside `bolt/conformance/` (as shown above) - there's no
`conftest.py`/package install step for this small dev script.
