# Issue #5798 - String functions silently coerce non-STRING inputs

https://github.com/ArcadeData/arcadedb/issues/5798

## Root cause

Several OpenCypher string functions declare a `STRING` input domain but implement it by calling
`args[0].toString()` (or the equivalent for a later argument) without checking the runtime type of
the value first. Any value - `INTEGER`, `FLOAT`, `BOOLEAN`, `LIST<ANY>`, ... - is therefore silently
turned into text instead of being rejected, making `toUpper(5)` observationally identical to the
explicit conversion `toUpper(toString(5))`.

Affected functions (as reported):

- `toUpper()` - `com.arcadedb.function.text.ToUpperFunction`
- `toLower()` - `com.arcadedb.function.text.ToLowerFunction`
- `trim()` / `btrim()` - `com.arcadedb.query.opencypher.executor.CypherTrimFunction`
- `lTrim()` - `com.arcadedb.function.text.LTrimFunction`
- `rTrim()` - `com.arcadedb.function.text.RTrimFunction`
- `split()` - `com.arcadedb.query.opencypher.executor.CypherSplitFunction`
- `replace()` - `com.arcadedb.function.text.ReplaceFunction`

This is the same class of defect already fixed for the numeric family in issue #5484 (a
`CommandExecutionException`/500 vs. a client-facing `CommandSemanticException`/400 distinction), and
for `head()`/`last()`/`tail()`/`size()` in #5476/#5477: a function's declared input domain must be
enforced, not silently widened by `Object.toString()`.

## Fix

- Added `CypherFunctionHelper.requireStringArgument(Object, String)` (mirrors the existing
  `requireNumberArgument`/`requireListArgument` helpers): returns the value as a `String` when it is
  one, returns `null` when the value is `null` (Cypher null propagation), and otherwise throws a
  `CommandSemanticException` via the shared `typeMismatch()` builder so the HTTP layer answers 400,
  not 500.
- `ToUpperFunction`, `ToLowerFunction`, `LTrimFunction`, `RTrimFunction`, `CypherTrimFunction`,
  `CypherSplitFunction` and `ReplaceFunction` now call `requireStringArgument()` on their primary
  text argument(s) instead of blindly calling `.toString()`.
- `CypherSemanticValidator.checkStaticallyKnownArgType()` (the parse-time check that already covers
  `size()`/`head()`/`last()`/`tail()`) was extended to reject a statically-known non-STRING literal
  for the single-argument spellings of `toUpper`/`upper`, `toLower`/`lower`, `trim`/`btrim`,
  `lTrim`/`ltrim` and `rTrim`/`rtrim` too, so `MATCH (n) WHERE false RETURN toUpper(5)` fails before
  the query runs, matching Neo4j and the existing #5484/#5477/#5476 precedent.

`toString()` conversion is still available explicitly via `toString()`, and `null` still propagates
to `null` per Cypher semantics.

## Test plan

New test class
`engine/src/test/java/com/arcadedb/query/opencypher/CypherStringFunctionArgumentIssue5798Test.java`:

- Each of the seven functions rejects a non-STRING argument (`INTEGER`) with a `CommandSemanticException`
  naming the function and `STRING`; `toUpper()` additionally covers `BOOLEAN` and `LIST<ANY>` as
  representative extra cases of the same input-domain check shared by all seven.
- `null` still propagates to `null` for every affected function.
- A valid `STRING` argument still works as before (regression guard).
- `toUpper(toString(5))` still returns `"5"` (explicit conversion keeps working).
- The parse-time static check rejects a literal even when no row would reach the projection, for the
  single-argument spellings.
- A property read whose runtime value is a non-STRING type is rejected too (exercises the runtime
  path, not just the parse-time literal check).

## Results

- New test class `CypherStringFunctionArgumentIssue5798Test`: 14/14 pass.
- Confirmed TDD red-green: before the fix, 11 of the 14 tests failed (the reproducers, the runtime
  property-read check and the parse-time literal check); after the fix, all 14 pass.
- Targeted regression classes (numeric family #5484, list family #5476, string-function comprehensive
  suite, optional-argument-null #5629, `OpenCypherMissingFunctionsTest`, `OpenCypherAdvancedFunctionTest`,
  `CypherFunctionFactoryExtendedTest`, `CypherMissingFunctionsTest`, `Issue5383BooleanNullFunctionTest`,
  `OpenCypherFunctionTest`, `OpenCypherExpressionTest`, `CypherFollowUpsIssue5602Test`): all pass.
- Full `com.arcadedb.query.opencypher.**` package (337 test classes, 7652 tests, includes the OpenCypher
  TCK suite): 0 failures, 0 errors.
