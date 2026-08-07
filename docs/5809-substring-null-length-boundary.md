# Issue #5809 - substring() skips null propagation when the start index is at or beyond the string length

## Problem

`CypherSubstringFunction.execute()` checked `start >= str.length()` and returned `""` before
checking whether the explicit three-argument `length` was `null`. This meant:

- `substring('ab', 1, null)` correctly returned `null` (the fix for #5193 - `start < length`).
- `substring('ab', 2, null)` and `substring('ab', 3, null)` incorrectly returned `""` instead of
  `null`, because `start >= str.length()` was true and the method returned before ever reaching
  the `args[2] == null` check.

This violated the null-propagation rule settled in #5629/#5699: an explicit `null` in an optional
argument position is never "argument omitted" and must propagate, regardless of what the other
arguments are. The boundary-dependent behavior meant the same explicit `null` length propagated
or didn't depending on where `start` landed relative to the string length, which no
null-propagation rule depends on.

## Fix

`engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherSubstringFunction.java`

Moved the `args.length == 3 && args[2] == null` check ahead of the `start >= str.length()`
short-circuit, so the explicit-null-length propagation is decided before the two-argument
"empty tail" boundary behavior can intercept it. The two-argument (length omitted) form is
untouched - it still legitimately returns `""` at and past the boundary.

## Tests

New regression test:
`engine/src/test/java/com/arcadedb/query/opencypher/CypherSubstringNullLengthBoundaryIssue5809Test.java`

- `substringPropagatesNullLengthWhenStartIsBeforeTheEnd` - pins the #5193 witness so it can't
  regress alongside this fix.
- `substringPropagatesNullLengthWhenStartIsAtTheEnd` - the exact #5809 repro (`start == length`).
- `substringPropagatesNullLengthWhenStartIsPastTheEnd` - the exact #5809 repro (`start > length`).
- `substringStillReturnsTheEmptyTailWhenLengthIsOmittedAtOrPastTheEnd` - pins that the two-argument
  form's boundary behavior is unchanged.

Confirmed the new test fails against the pre-fix code (2/4 failures, matching the two out-of-range
cases from the issue) before applying the fix, then passes after.

Also re-ran the related existing suites to confirm no regression:
- `CypherOptionalArgumentNullIssue5629Test` (17 tests) - the #5629/#5699 null-propagation rule
  test suite, including its existing substring coverage.
- `OpenCypherStringFunctionsComprehensiveTest` (77 tests)
- `CypherFunctionFactoryExtendedTest` (30 tests)
- `CypherFunctionArityRegistryTest` (5 tests)
- `SQLMethodSubStringTest` (3 tests) - the separate SQL-side substring implementation, unaffected.

All 136 tests pass with 0 failures.
