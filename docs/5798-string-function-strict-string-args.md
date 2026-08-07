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

- New test class `CypherStringFunctionArgumentIssue5798Test`: 14/14 pass as of the original PR
  (grew to 17/17 after the alias-spelling and 3-arg-trim tests added during review cycles 2-3; see
  "Review cycles" below).
- Confirmed TDD red-green: before the fix, 11 of the 14 tests failed (the reproducers, the runtime
  property-read check and the parse-time literal check); after the fix, all 14 pass.
- Targeted regression classes (numeric family #5484, list family #5476, string-function comprehensive
  suite, optional-argument-null #5629, `OpenCypherMissingFunctionsTest`, `OpenCypherAdvancedFunctionTest`,
  `CypherFunctionFactoryExtendedTest`, `CypherMissingFunctionsTest`, `Issue5383BooleanNullFunctionTest`,
  `OpenCypherFunctionTest`, `OpenCypherExpressionTest`, `CypherFollowUpsIssue5602Test`): all pass.
- Full `com.arcadedb.query.opencypher.**` package (337 test classes, 7652 tests, includes the OpenCypher
  TCK suite): 0 failures, 0 errors.

## PR

https://github.com/ArcadeData/arcadedb/pull/5901

## Review cycles

### Cycle 1 - head `019f4352`

Claude's bot review (posted as a PR issue comment, not a formal review) found one correctness gap:
`replace()`, `split()`, and the 2-argument forms of `lTrim()`/`rTrim()`/`trim()`/`btrim()` checked
`args[0] == null || args[1] == null || ...` for `null` *before* calling `requireStringArgument()` on
the primary argument, so an out-of-domain primary argument (e.g. `replace(5, null, 'b')`) silently
returned `null` instead of raising the type error whenever a *secondary* argument happened to be
`null` too. This contradicted the ordering convention already established in #5484
(`MathBinaryFunction`/`RoundFunction` type-check every argument before null propagation decides the
answer). The reviewer also confirmed `CypherTrimFunction`'s 3-argument SQL-style form did not have
this problem, and that the parse-time static check, the alias-name mapping and the test structure
were all correct.

Fix applied: reordered the five affected call sites so `requireStringArgument()` on the primary
argument runs first, and the null check on the remaining arguments runs after, mirroring the #5484
convention. Added a new test
(`thePrimaryArgumentIsCheckedEvenWhenASecondaryArgumentIsNull`) covering
`replace(5, null, 'b')`, `split(5, null)`, `lTrim(5, null)`, `rTrim(5, null)` and `btrim(5, null)`.
Confirmed red (via `git stash` of only the reordering) before the fix, green after. Full new test
class (15/15) and the same targeted regression suites re-run clean after the fix.

Outcome: actionable and clear, applied. No deferred items from this cycle.

### Cycle 2 - head `55fb0ca9`

Claude's bot review (again a PR issue comment) confirmed the cycle-1 fix is correct and covered by a
regression test, and found no further bugs. It raised three non-blocking observations:

- `btrim(5, null)` reports a message naming `trim()` rather than `btrim()`, because
  `CypherTrimFunction.getName()` returns `"trim"` unconditionally - pre-existing behavior, not
  introduced by this PR, and internally consistent between the parse-time and runtime paths (the new
  test already asserts this exact spelling). **Skipped**: out of scope for this PR, asked only to
  "confirm this is intentional" rather than requesting a change.
- The parse-time static check accepts any `CharSequence` literal while the runtime check requires
  `instanceof String`; the reviewer notes this is not a real gap today since literals and STRING
  properties are always plain `String`. **Skipped**: informational only, no requested change.
- The new `docs/` tracking file follows the existing convention - no concern raised.
- Optional: add test coverage for the alias spellings (`upper`, `lower`, `ltrim`, `rtrim`) directly,
  since `canonicalStringFunctionName()`'s alias mapping was previously exercised only implicitly.
  **Applied**: added `theAliasSpellingsRejectANonStringArgumentToo()` covering all four aliases.

Outcome: one actionable-and-clear item applied (alias test coverage); two informational observations
skipped with rationale above (both explicitly non-blocking and either pre-existing or already-covered
behavior, not requests for a code change).

### Cycle 3 - head `cbdc82da`

Claude's bot review (again a PR issue comment) confirmed the cycle-1 null-ordering fix is correct
across all five affected call sites and traced the 3-argument SQL-style `trim(BOTH/LEADING/TRAILING
char FROM string)` path too, finding it correct but untested:

- **Applied**: added `theSqlStyleThreeArgumentTrimFormRejectsANonStringSource()` covering
  `trim(BOTH 'x' FROM 5)`, since this branch of `CypherTrimFunction` was directly modified in cycle 1
  (the `source` computation moved after the `args[2] == null` check) but had no dedicated regression
  test.
- `left()`/`right()` have the same class of defect (no type check on their primary argument) but
  were not named in #5798. **Skipped/deferred**: explicitly called out by the reviewer as "out of
  scope here since #5798 didn't name them" - noted as a follow-up in the PR's Additional Notes
  section rather than fixed in this PR.
- A dedicated consistency test between `canonicalStringFunctionName()`'s alias mapping and
  `CypherFunctionFactory`, mirroring the numeric family's `everyNumericFunctionNameResolvesToA...`
  test. **Skipped**: reviewer explicitly says "not urgent given the alias tests added in cycle 3
  exercise it end-to-end."
- Two previously-raised informational points (btrim() message naming, CharSequence/String asymmetry)
  reiterated as fine to leave as documented.
- Suggested calling out the behavior change (`toUpper(5)` now throws instead of returning `"5"`)
  explicitly as a breaking change in the PR description. **Applied**: added a "Breaking change" note
  and a mention of the `left()`/`right()` follow-up to the PR body (not a tracking-doc-only change,
  since it needed to be visible on the PR itself).

Outcome: one actionable-and-clear code/test item applied (3-arg trim test); one out-of-scope
follow-up noted in the PR description rather than fixed; one optional consistency-test suggestion
skipped as explicitly non-urgent; PR description updated per the reviewer's process suggestion.

### Cycle 4 - head `4c4b6606` (final cycle, `--max-cycles=4`)

Claude's bot review re-verified the cycle-1 ordering fix and the cycle-3 3-arg-trim path against the
grammar/parser directly (confirmed `CypherExpressionBuilder`'s trim-function builder produces
`[mode, trimChar, source]`), found no bugs ("no gaps found" in test coverage, "no concerns" on
security/performance), and raised four minor/non-blocking observations:

- `CypherTrimFunction`'s 3-arg branch could collapse the redundant `args[2] == null` check into
  `requireStringArgument()`'s own null handling, matching the style of the 1-arg/2-arg branches
  above it. **Applied**: simplified to `final String source = requireStringArgument(args[2], ...); if
  (source == null || trimChar == null) return null;`.
- The tracking doc's "Results" section still said "14/14 pass" after later commits grew the test
  class to 17 tests. **Applied**: corrected the line to note the growth across cycles.
- Scope boundary (secondary STRING-typed arguments, `left()`/`right()` follow-up) is clearly
  documented - no action needed, reviewer confirms it reads as "a reasonable, well-communicated scope
  boundary rather than an oversight."
- Suggested confirming whether the project tracks a CHANGELOG/release-notes entry for breaking
  changes. **Checked and skipped**: no `CHANGELOG` file exists at the repo root: the project has no
  such convention to update.

Outcome: two small, safe touch-ups applied (readability simplification, stale doc line); two
informational items require no code change. Re-ran the new test class plus
`CypherOptionalArgumentNullIssue5629Test`, `OpenCypherMissingFunctionsTest`,
`OpenCypherStringFunctionsComprehensiveTest` and `CypherFunctionArityRegistryTest` after the
simplification: all pass.

This was the fourth and final review cycle allowed by `--max-cycles=4`. No further review round was
requested after this push; the review itself was a clean approval in substance ("Nice work - this
closes the gap cleanly and consistently with the established pattern") with only optional polish
items, none of which is a deferred/blocking item for the developer.

## Final state

`max-cycles-reached` (4/4 cycles run; each cycle's feedback was applied or explicitly and
transparently skipped with rationale - no unresolved blocking feedback and no deferred-items file was
produced in any cycle). PR #5901 is open, mergeable, and unmerged. Merge remains the developer's
decision.
