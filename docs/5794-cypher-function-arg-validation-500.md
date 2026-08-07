# Issue #5794: Cypher argument-validation errors surfaced as HTTP 500

## Root cause

Four argument-validation paths in Cypher functions threw a bare
`com.arcadedb.exception.CommandExecutionException`, which
`AbstractServerHttpHandler` maps to HTTP 500 ("internal server error").
`CommandSemanticException` (a subclass of `CommandParsingException`) is the
class the HTTP layer maps to HTTP 400 for deterministic, client-caused
argument errors — the pattern already established for `abs()` (#5484),
`left()`/`right()` (#5296), and arithmetic errors (#5545).

Each of the four failures below is determined entirely by the supplied query
arguments and cannot succeed when retried unchanged, so it belongs to the
same class of client error, not a server fault.

## Affected components

- `engine/src/main/java/com/arcadedb/function/coll/RangeFunction.java`
  - `range()` with a zero step.
- `engine/src/main/java/com/arcadedb/function/temporal/DateConstructorFunction.java`
  - `date()` with a string that parses as neither a date nor a timezone.
- `engine/src/main/java/com/arcadedb/function/temporal/DateTimeConstructorFunction.java`
  - `datetime()` with a string that parses as neither a datetime nor a timezone.
- `engine/src/main/java/com/arcadedb/function/geo/CypherPointFunction.java`
  - `point()` with a non-numeric coordinate, in `coerceCoordinate()`. Both
    branches were fixed (a non-numeric `String` and a value that is neither a
    `Number` nor a numeric-looking `String`), since they are the same
    validation guard for the same class of error.

## Expected vs actual behavior

| Query | Before | After |
| --- | --- | --- |
| `RETURN range(1, 5, 0)` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |
| `RETURN date('not-a-date')` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |
| `RETURN datetime('not-a-date')` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |
| `RETURN point({x: 'a', y: 1})` | HTTP 500 `CommandExecutionException` | HTTP 400 `CommandSemanticException` |

## Fix

Changed the four throw sites (five call sites counting both branches of
`coerceCoordinate()`) from `CommandExecutionException` to
`CommandSemanticException`, keeping the existing descriptive messages
unchanged. Updated the class-level Javadoc on `CypherPointFunction` and
removed a now-unused `CommandExecutionException` import in
`RangeFunction.java`.

## TDD approach

1. Wrote `engine/src/test/java/com/arcadedb/query/opencypher/CypherFunctionArgumentValidationIssue5794Test.java`
   reproducing all four named paths (plus the second `coerceCoordinate`
   branch) and asserting `CommandSemanticException`, plus a
   `validInputsStillSucceed()` control.
2. Confirmed all 5 reproducer tests failed against `main` (asserting
   `CommandExecutionException` was thrown instead), proving the bug.
3. Applied the minimal fix (exception class swap only, no behavior/message
   change).
4. Re-ran the same test class: all 6 tests pass.
5. Ran a broader related-test pass (numeric-function argument validation
   #5484, optional-argument-null #5629, spatial functions comprehensive
   test, function-factory extended test, function-arity registry test):
   all pass, no regressions.

## Test results

- `CypherFunctionArgumentValidationIssue5794Test`: 6/6 pass (was 1/6 before
  the fix, with the 5 reproducer tests failing as expected).
- Broader related suite (`CypherNumericFunctionArgumentIssue5484Test`,
  `CypherFollowUpsIssue5602Test`, `CypherOptionalArgumentNullIssue5629Test`,
  `OpenCypherSpatialFunctionsComprehensiveTest`,
  `CypherFunctionFactoryExtendedTest`, `CypherFunctionArityRegistryTest`):
  all pass, no regressions.

## Impact analysis

- Client-facing: the four listed queries now report a descriptive HTTP 400
  instead of a misleading HTTP 500, matching the behavior already shipped
  for the neighboring numeric/string/arithmetic argument-validation fixes.
- No change to error message text, so no downstream string-matching client
  code is affected beyond the (already-desired) HTTP status code and
  exception class.
- Scope: limited to the four paths named in the issue's reproduction steps.
  Sibling constructors (`localdatetime()`, `localtime()`, `time()`) and a
  few other structural `point()` validation branches (missing `x`/`y` or
  `longitude`/`latitude`, non-map argument, non-numeric `srid`) share the
  same `CommandExecutionException`-for-a-client-error defect but were left
  untouched, being outside this issue's reported repro steps. Recommended
  as a follow-up issue for the same systematic scan.

## Recommendations for monitoring or future improvements

- File a follow-up issue for the sibling temporal constructors
  (`localdatetime()`, `localtime()`, `time()`) and the remaining
  `CypherPointFunction` structural-validation branches, which have the
  identical defect class but were not part of this issue's named repro.
  Done: filed as #5910.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5909

## Review cycles

- **Cycle 1** - head `d40023d05a4fcc639965a6ef4cf4a832e7441c91` (the fix
  commit). `claude[bot]` posted its review as a PR issue comment (not a
  formal GitHub review) ~4 minutes after the push: "Didn't find any
  bugs... LGTM pending the tracking-issue note above." The one suggestion
  (link a tracking issue for the documented out-of-scope follow-up) was
  actioned without a code change: filed #5910 and linked it from a PR
  comment. Working tree stayed clean, so the loop exited on a
  clean-approval after cycle 1 with no further push needed.

## Deferred items

None. The single review suggestion was actionable-and-clear and was
resolved by filing #5910, not by deferring to the developer.

## Final state

`clean-approval` after 1 review cycle.
