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

## PR

https://github.com/ArcadeData/arcadedb/pull/5882

## Review cycles

### Cycle 1 - `b9664b2d5`

`claude[bot]` posted a review as an issue comment (not a formal PR review) verifying the fix's
control-flow ordering line by line and confirming it is correct and minimal. It flagged one
non-blocking consistency suggestion: reuse `CypherFunctionHelper.isExplicitNull(args, position)`
- the codebase's canonical helper for exactly this check, already used by `RoundFunction`,
`NormalizeFunction`, `FormatFunction`, the `*TruncateFunction` family, `VectorDistanceFunction`,
and the sibling `com.arcadedb.function.text.SubstringFunction` - instead of the hand-rolled
`args.length == 3 && args[2] == null`.

Applied: replaced the hand-rolled check with `CypherFunctionHelper.isExplicitNull(args, 2)` and
added the import. Re-ran the same 136-test suite; all pass with 0 failures.

No deferred items from this cycle.

### Cycle 2 - `42aab29a1`

`claude[bot]` re-reviewed after the cycle 1 push (issue comment, not a formal PR review).
Confirmed the fix and the `isExplicitNull` refactor are correct by tracing all four cases by
hand and cross-checking against the sibling `com.arcadedb.function.text.SubstringFunction`
implementation. Raised one non-blocking observation, explicitly marked pre-existing and out of
scope: `CypherSubstringFunction`'s `length < 0` check only runs inside the `args.length == 3`
branch, which is unreachable once `start >= str.length()` has already returned `""`, so
`substring('ab', 5, -1)` returns `""` instead of throwing. This predates this PR (the old code
had the same gap before either fix) and was suggested only as a possible follow-up issue, not a
change to make here.

No actionable items, no code changes required, working tree clean. Treated as a clean approval -
loop exited after 2 cycles.

### Deferred items

None. The one non-blocking observation from cycle 2 (pre-existing negative-length validation
gap in `CypherSubstringFunction`, unrelated to this PR's fix) was intentionally left for a
possible separate follow-up issue rather than filed here, since it predates this change and
touches a different code path (`length < 0` validation, not null propagation).

### Final state

`clean-approval`
