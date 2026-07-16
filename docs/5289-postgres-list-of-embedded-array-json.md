# #5289 - Postgres plugin treats LIST OF EMBEDDED properties as ARRAY[TEXT]

PR: https://github.com/ArcadeData/arcadedb/pull/5310

## Symptom

A `LIST OF <DocumentType>` property is advertised over the Postgres wire protocol as `text[]` (OID 1009)
instead of `json[]` (OID 199), so clients treat each element as an opaque string and re-escape the nested
JSON. Follow-up to #5253, which fixed the same asymmetry for scalar `EMBEDDED` / `MAP` properties.

## Root cause

`PostgresType.getTypeFromArcade` mapped `Type.LIST` to `ARRAY_TEXT` unconditionally, ignoring the property's
declared `OF <type>` element type. `EMBEDDED` and `MAP` were already mapped to `JSON`, so only lists were
affected.

The value-based path (`getTypeForValue`) infers the array type from the first non-null element, so a
*non-empty* list of embedded documents already resolved to `ARRAY_JSON`. The defect surfaces whenever no
sample element is available and the declared schema is the only source of truth:

1. **Empty result set** - `getColumnsFromQuerySchema` falls back to the declared schema. Reached by client
   schema introspection (DbVisualizer, PhpStorm) and Spark/PySpark `WHERE 1=0` probes.
2. **Empty list value** - `getTypeForValue` has no element to inspect and falls back to `ARRAY_TEXT`.

Case 2 also made a column's advertised OID depend on whether the first row's list happened to be empty, so
the same column could flap between `text[]` and `json[]` across result sets.

## Fix

- `PostgresType.getTypeFromArcade(Type, String ofType)` - new overload resolving a LIST's element type from
  the declared `OF` clause. An `ofType` that names no scalar `Type` refers to an embedded document type and
  maps to `ARRAY_JSON`; scalars map to their matching array type; an undeclared or blank `ofType` stays
  `ARRAY_TEXT`. This reuses the discriminator convention already established by `Type.coerceCollectionOfType`
  (#5261).

  Every scalar branch must agree with `getArrayTypeForElementType` (the value path), otherwise the OID would
  again depend on whether the list is empty. `DECIMAL` therefore maps to `ARRAY_TEXT`, not `ARRAY_DOUBLE`:
  `getArrayTypeForElementType` has no `BigDecimal`/`Number` branch, so a populated list of `BigDecimal` is
  `ARRAY_TEXT`. Locked by `getTypeFromArcadeListOfDecimalMatchesValuePath`.
- `PostgresNetworkExecutor.getColumnsFromQuerySchema` - passes `prop.getOfType()` (schema path).
- `PostgresNetworkExecutor.getColumns` - for an empty list, prefers the declared `LIST OF <type>` via the new
  `getDeclaredListType` helper (value path), keeping the OID stable across rows.

The single-argument `getTypeFromArcade` overload is retained and unchanged.

## Tests

- `PostgresWJdbcIT.embeddedListPropertyReportedAsJsonArray` - reporter's fixture: `LIST OF Product` with one
  element reports `_json`, elements parse as un-escaped JSON, plain `LIST` stays `_text`.
- `PostgresWJdbcIT.embeddedListReportedAsJsonArrayWithoutSampleElement` - the two failing paths: empty result
  set (schema fallback) and empty list value. Also guards `LIST` and `LIST OF STRING` staying `_text`.
- `PostgresTypeTest` - unit coverage for the new overload: embedded document ofType, scalar ofTypes, null
  ofType, and non-LIST types ignoring ofType.

Full `postgresw` suite green: 210 unit + 108 IT.

## Impact

Confined to the Postgres wire protocol's RowDescription column typing. `LIST OF <DocumentType>` is now
advertised as `json[]`, and `LIST OF <scalar>` as its matching array type, aligning the schema path with the
value path. Untyped `LIST` behaviour is unchanged.

Note: for a `json[]` column pgJDBC returns `String` elements, matching stock PostgreSQL behaviour. The fix
corrects the advertised OID, which is what clients use to decide whether to parse elements as JSON.

## Review cycles

### Cycle 1 - c73cd0e46

- `claude[bot]`: recommended merge. Highlighted the schema/value path mapping parity as correct. Three
  non-blocking notes.
- `gemini-code-assist[bot]`: COMMENTED, one medium inline suggestion with two parts.

Applied:
- Blank `ofType` now returns `ARRAY_TEXT` (gemini). Real defect: `Type.getTypeByName("")` returns null, which
  the code read as "not a scalar, therefore an embedded document type", yielding `json[]` for a blank string.
- Dropped "/MAP" from the `ofType` javadoc (claude): only `LIST` resolves `ofType`.

Declined, with reasoning posted to the review thread:
- `DECIMAL -> ARRAY_DOUBLE` (gemini). Superficially consistent with the scalar `DECIMAL -> DOUBLE` mapping,
  but the parity target here is the *value* path, and `getArrayTypeForElementType` has no
  `BigDecimal`/`Number` branch, so a populated `LIST OF DECIMAL` is `ARRAY_TEXT`. Adopting the suggestion
  would make the column report `_float8` when empty and `_text` once populated - reintroducing the OID
  flapping this fix removes - and would advertise `BigDecimal` as lossy float8. Locked by
  `getTypeFromArcadeListOfDecimalMatchesValuePath`. The correct version of that change would add a
  `BigDecimal` branch to `getArrayTypeForElementType` and update both paths together, separately from this
  regression fix.

Skipped:
- Empty non-`Collection` arrays bypass the empty-list guard (claude). Not reachable today: ArcadeDB LIST
  properties surface as `java.util.List`, as the reviewer noted.
- `getColumnsFromQuerySchema` uses non-polymorphic `getProperty` (claude). Verified a non-issue:
  `getPropertyNames()` is own-only, with `getPolymorphicPropertyNames()` as the separate polymorphic variant,
  so no inherited name can reach `getProperty`.

### Cycle 2 - 665365ab7

Timeout: neither bot posted a new review within the 15-minute window (the `claude-review` workflow job ran
and passed, posting no new findings; gemini's inline comment is the cycle-1 one re-anchored by GitHub to the
new SHA). No unaddressed feedback outstanding.

## Final state

`timeout` (cycle 2). No deferred items - every cycle-1 comment was applied, declined with reasoning, or
verified as a non-issue.

CI: three red checks, all verified pre-existing and repo-wide rather than caused by this change.

- `unit-tests` / `integration-tests`: the test steps themselves pass (`Run Unit Tests with Coverage` and
  `Run Integration Tests with Coverage` both succeed); the jobs go red on the `Unit Tests Reporter` /
  `IT Tests Reporter` step. The same step fails on #5308 and #5305, where the test step likewise succeeds.
- `Meterian client scan`: fails on `security: 0`, a dependency-score gate. Reproduces identically on #5308,
  #5305, #5300 and #5200; this change touches no dependency files.
- `ha-integration-tests` was still running at handoff.

Everything else green, including `build-and-package`, `claude-review`, CodeQL and all e2e suites.
