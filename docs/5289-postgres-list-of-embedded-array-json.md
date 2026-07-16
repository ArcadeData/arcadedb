# #5289 - Postgres plugin treats LIST OF EMBEDDED properties as ARRAY[TEXT]

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
