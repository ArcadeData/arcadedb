# #5311 - Postgres schema-path type mapping collapses ARRAY_OF_*/DATETIME_* to VARCHAR

## Symptom

Two defects in how the Postgres wire types a RowDescription column:

1. `SELECT` on a type with an `ARRAY_OF_SHORTS` property failed outright:
   `PSQLException: ERROR: Error on executing query: Unexpected value: [S@38013adc`.
2. The five `ARRAY_OF_*` and the three sub-second `DATETIME_*` types were advertised as scalar `varchar`
   when a query returned no rows, but as their real array/timestamp OID once a row existed. The column's
   advertised type therefore depended on whether the table happened to be empty.

## Root cause

`PostgresType` resolves a column via two independent paths that had drifted apart:

- **Value path** (`getTypeForValue`) - used when a sample row exists. Its array `switch` had cases for
  `int[]`, `long[]`, `double[]`, `float[]`, `boolean[]`, `char[]`, `String[]` and their boxed forms, but
  none for `short[]`/`Short[]`, so an `ARRAY_OF_SHORTS` value reached `default -> throw new
  IllegalStateException`. `Type.ARRAY_OF_SHORTS` declares `short[]` as its default Java type, so a plain
  property declaration was enough to reach it.
- **Schema path** (`getTypeFromArcade`) - used when a query returns 0 rows (client schema introspection,
  Spark `WHERE 1=0` probes). Its `switch` had no case for 8 of the 24 `Type` values, so they fell through
  to `default -> VARCHAR`.

## Fix

Scoped to items 1 and 2 of the issue's suggested scope.

- `getTypeForValue`: added `short[]`/`Short[]` -> `ARRAY_INT`. Shorts widen to `int4[]` because
  `getArrayTypeForElementType` already answers `ARRAY_INT` for a `Short` element and `PostgresType` has no
  `int2[]` entry to pair with a narrower answer.
- `getTypeFromArcade`: added the 8 missing cases - `ARRAY_OF_SHORTS`/`ARRAY_OF_INTEGERS` -> `ARRAY_INT`,
  `ARRAY_OF_LONGS` -> `ARRAY_LONG`, `ARRAY_OF_FLOATS` -> `ARRAY_REAL`, `ARRAY_OF_DOUBLES` -> `ARRAY_DOUBLE`,
  and `DATETIME_MICROS`/`DATETIME_NANOS`/`DATETIME_SECOND` -> `TIMESTAMP` (folded into the existing
  `DATETIME` branch).

`convertPrimitiveArrayToCollection` needed no change: it already handles `short[]`, and `Short[]` is matched
by its `Object[]` branch.

`DECIMAL` (item 3) and `BINARY` (item 4) are deliberately out of scope - both need a new `PostgresType`
entry (`NUMERIC` OID 1700 / `BYTEA` OID 17) with a binary encoder, not a missing switch case.

## Tests

`PostgresTypeResolutionPathTest` (new) is the table-driven regression test the issue asked for: it asserts
that for every one of the 24 `Type` values the schema path and the value path resolve to the same
`PostgresType`, with a `KNOWN_DISAGREEMENTS` table pinning the documented exceptions so that fixing one
forces its entry to be removed rather than leaving a stale exemption. `everyTypeHasASampleValue` guards the
table against silently skipping a `Type` added to the enum later.

`PostgresWJdbcIT` (2 new methods) reproduce both findings end-to-end over pgJDBC:
- `shortArrayPropertyIsQueryable` - the issue's exact repro; previously threw.
- `arrayAndDatetimeColumnsKeepTheirTypeWhenTheResultSetIsEmpty` - asserts the same OIDs are advertised for
  an empty and a populated result set across all 8 affected columns.

Verified the tests genuinely catch the bug by reverting the production change and confirming they fail
(4 failures + 1 `IllegalStateException`), then restoring it.

Results: `postgresw` 218 unit + 110 integration tests pass.

## Impact

`getTypeFromArcade` / `getTypeForValue` are consumed only by `PostgresNetworkExecutor` within `postgresw`,
so the blast radius is limited to the Postgres wire.

Behaviour change for clients: columns of the 8 affected types now advertise their real OID on empty result
sets instead of `varchar`. A client that had adapted to reading these as text from an empty-result-set probe
will now receive the array/timestamp OID. This is the intended correction and matches what those clients
already received for non-empty result sets.

## Follow-up: three disagreements not in the issue's audit table

Running the new table-driven test against unpatched `main` surfaced three pairs that also disagree, none of
which the issue's audit lists as broken (it states the other 14 types "agree and are fine"):

| `Type` | Schema path | Value path |
|---|---|---|
| `SHORT` | `SMALLINT` | `INTEGER` |
| `BYTE` | `SMALLINT` | `INTEGER` |
| `DATETIME` | `TIMESTAMP` | `DATE` |

- `SHORT`/`BYTE`: the value path widens `Short`/`Byte` to `INTEGER`. Reconciling on `SMALLINT` would change
  the OID clients already receive for populated rows, so the direction is a judgment call rather than a
  mechanical fix.
- `DATETIME`: `java.util.Date` is the default Java type of both `DATE` and `DATETIME`, so the value path
  cannot distinguish them and answers `DATE` for either. Not fixable by a switch case.

These are recorded in `KNOWN_DISAGREEMENTS` alongside `DECIMAL` and `BINARY` and are left for a separate
issue, since each changes behaviour beyond the scope this issue audited.
