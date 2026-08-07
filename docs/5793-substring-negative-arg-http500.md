# Issue #5793: `substring()` with a negative start or length returns HTTP 500 instead of HTTP 400

## Root cause

`com.arcadedb.query.opencypher.executor.CypherSubstringFunction` (the implementation the OpenCypher engine
resolves `substring()` to) threw a plain `CommandExecutionException` when the `start` or `length` argument was
negative. `AbstractServerHttpHandler` maps `CommandExecutionException` to HTTP 500 - a server-error
classification for what is really an invalid-argument client error.

The sibling functions `left()` and `right()` (`com.arcadedb.function.text.LeftFunction` /
`RightFunction`) were already fixed for issue #5296 to throw `CommandSemanticException`, which extends
`CommandParsingException`. The HTTP handler maps `CommandParsingException` to 400. `substring()`'s two
negative-argument checks (negative `start`, negative `length`) were never updated the same way.

## Fix

Changed both throw sites in `CypherSubstringFunction.execute()` from `CommandExecutionException` to
`CommandSemanticException`, mirroring `LeftFunction`'s fix for #5296. Messages were also split to identify
which argument (start vs. length) was negative, matching the specificity of `LeftFunction`'s message.

## Scope note

A second, structurally similar class - `com.arcadedb.function.text.SubstringFunction` - also throws
`CommandExecutionException` for a negative length and silently returns `""` for a negative start. That class
is not registered anywhere in `CypherFunctionFactory` or any other function registry found in `engine/src/main`;
it is exercised only directly by unit tests (`TextStatelessFunctionsTest`), not reachable through any real
SQL or Cypher query path. Left untouched: fixing dead code is out of scope for this issue, and changing its
negative-start behavior (currently returns `""`, not documented as a bug in #5793) would be a separate,
unrequested behavior change. Noted here in case a future issue asks about it directly.

## Tests

- New regression test: `engine/src/test/java/com/arcadedb/query/opencypher/CypherSubstringNegativeArgumentIssue5793Test.java`
  - `substringNegativeStartIsAClientError` - `substring('hi', -1)` throws `CommandSemanticException`
    (and NOT `CommandExecutionException`), matching the issue's exact repro.
  - `substringNegativeStartWithLengthIsAClientError` - `substring('hi', -3, 2)`.
  - `substringNegativeLengthIsAClientError` - `substring('hi', 1, -2)`.
  - `substringNegativeStartAndLengthIsAClientError` - `substring('hi', -1, 2)`.
  - `substringNonNegativeArgumentsStillWork` - guards against an over-broad fix rejecting legitimate calls.
- Confirmed the new tests fail against the pre-fix code (4/5 failures, all `CommandExecutionException` instead
  of `CommandSemanticException`) before applying the fix, then pass after.
- Ran related existing suites for regressions, all green:
  - `CypherSubstringNullLengthBoundaryIssue5809Test` (4 tests) - null-length propagation, issues #5193/#5809.
  - `TextStatelessFunctionsTest` (54 tests) - `left`/`right`/`substring` (function.text package) unit tests.
  - `OpenCypherFunctionTest` (68 tests) - general Cypher function coverage.
  - `CypherFunctionSecurityTest` (28 tests, 2 skipped pre-existing) - argument validation / security tests.
  - `CypherFunctionArityRegistryTest` (5 tests) - arity error classification.
  - `SQLMethodSubStringTest` (3 tests) - unrelated SQL `.substring()` method syntax, unaffected.
- `mvn -pl engine -am compile` succeeds with no warnings/errors introduced.

## Impact

Clients calling `substring()` with a negative `start` or `length` via Cypher now receive HTTP 400 with a
descriptive client-error message, consistent with `left()` and `right()` on the same build. No behavior change
for valid (non-negative) arguments.
