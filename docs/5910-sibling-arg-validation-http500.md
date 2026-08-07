# Issue #5910: Sibling argument-validation paths still return HTTP 500

## Root cause

Follow-up to #5794 / PR #5909. That fix (still open as of this writing, branch
`fix/5794-cypher-function-arg-validation-500`) converts four Cypher argument-validation
paths named in #5794's reproduction steps from a bare
`com.arcadedb.exception.CommandExecutionException` (HTTP 500) to
`com.arcadedb.exception.CommandSemanticException` (HTTP 400): `range()` zero step,
`date()`/`datetime()` invalid strings, and `point()` non-numeric coordinates.

The same defect class exists in sibling paths that were outside #5794's named
reproduction steps, left untouched by #5909:

- `localdatetime()` / `localtime()` / `time()` - both the "cannot parse as a temporal
  value or timezone" branch and the "expects a string, map, or temporal argument" branch.
- `date()` / `datetime()` - only the "expects a string, map, or temporal argument"
  branch (the parse-failure branch is #5909's scope, not touched here).
- `point()` - the remaining structural-validation branches: non-map argument, missing
  `x`/`y` or `longitude`/`latitude` keys, and non-numeric `srid` (the `coerceCoordinate()`
  numeric-coordinate branches are #5909's scope, not touched here).

Each failure is determined entirely by the supplied query arguments and cannot succeed
on retry, so it belongs to the same client-error class already established for
`abs()` (#5484), `left()`/`right()` (#5296), arithmetic errors (#5545), and #5794/#5909.

## Affected components

- `engine/src/main/java/com/arcadedb/function/temporal/LocalDateTimeConstructorFunction.java`
- `engine/src/main/java/com/arcadedb/function/temporal/LocalTimeConstructorFunction.java`
- `engine/src/main/java/com/arcadedb/function/temporal/TimeConstructorFunction.java`
- `engine/src/main/java/com/arcadedb/function/temporal/DateConstructorFunction.java`
- `engine/src/main/java/com/arcadedb/function/temporal/DateTimeConstructorFunction.java`
- `engine/src/main/java/com/arcadedb/function/geo/CypherPointFunction.java`

## Expected vs actual behavior

| Query | Before | After |
| --- | --- | --- |
| `RETURN localdatetime('not-a-date')` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |
| `RETURN localtime('not-a-time')` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |
| `RETURN time('not-a-time')` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |
| `RETURN point('not-a-map')` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |
| `RETURN point({x: 1, y: 1, srid: 'abc'})` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |

## Fix

Swapped `CommandExecutionException` for `CommandSemanticException` (no message changes)
at exactly the throw sites named above:

- `LocalDateTimeConstructorFunction`: parse-failure branch and "expects..." branch (2 sites).
- `LocalTimeConstructorFunction`: parse-failure branch and "expects..." branch (2 sites).
- `TimeConstructorFunction`: parse-failure branch and "expects..." branch (2 sites).
- `DateConstructorFunction`: only the "expects..." branch (1 site) - the parse-failure
  branch is #5909's scope and was left as `CommandExecutionException` here, still pending
  in that open PR.
- `DateTimeConstructorFunction`: only the "expects..." branch (1 site), same rationale.
- `CypherPointFunction`: non-map argument, missing x/y or longitude/latitude keys, and
  non-numeric srid (3 sites) - the `coerceCoordinate()` numeric-coordinate branches are
  #5909's scope and were left untouched.

`CommandExecutionException` imports were removed where no longer used in a file
(`LocalDateTimeConstructorFunction`, `LocalTimeConstructorFunction`, `TimeConstructorFunction`)
and kept where the file still throws it at an out-of-scope site
(`DateConstructorFunction`, `DateTimeConstructorFunction`, `CypherPointFunction`).

Base branch note: PR #5909 (issue #5794) is still open, not yet merged into `main`. This
branch is based on plain `main` and only touches the sibling sites #5910 scopes - it does
not depend on #5909 landing first, and should merge cleanly regardless of merge order since
the two PRs touch disjoint line ranges in the three files they share.

## TDD approach

1. Wrote `engine/src/test/java/com/arcadedb/query/opencypher/CypherFunctionArgumentValidationIssue5910Test.java`,
   mirroring the style of #5909's `CypherFunctionArgumentValidationIssue5794Test`: one
   reproducer per named throw site (11 tests) plus a `validInputsStillSucceed()` control.
2. Ran the new test class against unfixed `main`: 11/12 failed as expected (each failure's
   stack trace pinpointed the exact throw-site line), 1/12 (the control) passed - confirmed
   the bug and the throw-site inventory.
3. Applied the minimal fix (exception class swap only, no behavior/message change).
4. Re-ran the same test class: 12/12 pass.
5. Ran a broader related-test pass: `CypherNumericFunctionArgumentIssue5484Test`,
   `CypherFollowUpsIssue5602Test`, `CypherOptionalArgumentNullIssue5629Test`,
   `OpenCypherSpatialFunctionsComprehensiveTest`, `CypherFunctionFactoryExtendedTest`,
   `CypherFunctionArityRegistryTest`, `OpenCypherTemporalFunctionsComprehensiveTest`,
   `OpenCypherTimestampTest`, `Issue4305Test`, `Issue4577And4578Test`,
   `OpenCypherSpatialFunctionsTest`, `FunctionCachingTest`, `OpenCypherMergeTest`:
   all pass, no regressions.

## Test results

- `CypherFunctionArgumentValidationIssue5910Test`: 12/12 pass (was 1/12 before the fix,
  11 reproducer tests failing as expected).
- Broader related suite (listed above): all pass, no regressions.

## Impact analysis

- Client-facing: the listed queries now report a descriptive HTTP 400 instead of a
  misleading HTTP 500, matching the behavior already shipped/pending for the neighboring
  argument-validation fixes (#5484, #5296, #5545, #5794/#5909).
- No change to error message text, so no downstream string-matching client code is
  affected beyond the (already-desired) HTTP status code and exception class.
- No new dependency, no schema change, no performance impact (exception construction is
  off the hot path - argument validation only fires on invalid input).
