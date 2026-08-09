# Issue #5849: MCPToolUtils.quoteIdentifier() does not escape backslashes

## Root cause

`MCPToolUtils.quoteIdentifier()` makes an identifier safe for inclusion in a generated statement by
rejecting a literal back-tick and wrapping the value in back-ticks. That is correct for Cypher, whose
lexer (`ESCAPED_SYMBOLIC_NAME`) escapes a back-tick by doubling it and gives a backslash no special
meaning. It is not correct for the SQL dialect: the SQL lexer's `QUOTED_IDENTIFIER` rule treats a
backslash as an escape character, so an identifier ending in `\` escapes the closing back-tick and lets
the lexer run past the intended end of the token.

`quoteIdentifier` is shared by both dialects (Cypher call sites: `appendNodeMerge`, `appendSetClause`,
`UpsertRelationshipTool`; SQL call site: `SampleRecordsTool.sampleType`), so a guard that is correct only
for Cypher leaves the SQL call site with a broken safety contract.

## Impact on 26.8.1

Not independently exploitable today: `SampleRecordsTool` only reaches `quoteIdentifier` with a type name
that already exists in the schema (`existsType` check) and appends only an `int` limit after it, so there
is no second identifier or literal for a trailing backslash to swallow. The practical effect on main was a
confusing parse failure instead of a clean rejection when sampling a type whose name ends in `\`. The
report is explicit that a second SQL call site rendering two identifiers, or a literal after the
identifier, would turn the same helper into an injection primitive.

## Fix

`quoteIdentifier` now rejects a backslash the same way it already rejects a back-tick, instead of passing
it through. This keeps the helper's contract dialect-agnostic and correct for whichever caller uses it,
at the cost of identifiers containing a backslash not being reachable through the MCP tools (backslash is
not a realistic character in a type/property name in practice, and no existing test or code path relies
on it).

Dialect-specific escaping (doubling backslashes) was considered and rejected: Cypher's lexer does not
treat a doubled backslash as an unescape sequence, so escaping that way would silently change the
identifier's value for Cypher call sites rather than merely quoting it.

## Changes

- `mcp/src/main/java/com/arcadedb/mcp/tools/MCPToolUtils.java`: `quoteIdentifier` also rejects a
  backslash; javadoc updated to explain why.
- `mcp/src/test/java/com/arcadedb/mcp/MCPToolUtilsTest.java`: two new regression tests,
  `rejectsBackslashToBlockSqlEscapeAmbiguity` (the exact `X\` case from the issue) and
  `rejectsBackslashInTheMiddleOfAnIdentifier`.

## Test results

- `MCPToolUtilsTest`: 6/6 pass (4 pre-existing + 2 new). The 2 new tests were confirmed to fail against
  the pre-fix code (`Expecting code to raise a throwable`), proving they reproduce the bug.
- Full `mcp` module test suite (`mvn -pl mcp test`): 294/294 pass, no failures or errors, no regressions.
- `quoteIdentifier` is used only inside the `mcp` module (`SampleRecordsTool`, `UpsertRelationshipTool`,
  `MCPToolUtils` itself), so the `mcp` module test suite is the full affected surface.

## PR

https://github.com/ArcadeData/arcadedb/pull/6026

## Review cycles

- **Cycle 1** (head `e5c1a77`): `claude[bot]` posted an issue-comment review (not a `reviews`/inline-comment
  entry) approving the change with no blocking issues. It confirmed the grammar-level claims (SQL
  `QUOTED_IDENTIFIER` treats backslash as an escape char, Cypher `ESCAPED_SYMBOLIC_NAME` does not), confirmed
  the reject-over-escape design tradeoff was sound for a single shared helper, and called out one nit: the
  tracking doc's title used an em dash where sibling docs use a colon or hyphen, conflicting with the repo's
  stated no-em-dash convention. Applied: retitled the doc and removed a second em dash found in the same
  file. No deferred items. Pushed as `5a254d3`.

- **Cycle 2** (head `5a254d3`): `claude[bot]` posted a second issue-comment review, independently
  re-verifying the same two grammar rules and the call-site claims, and concluded "No blocking issues
  found." It raised one non-blocking suggestion: add an integration-level test that exercises the fix
  through an actual call site (e.g. `SampleRecordsTool` or `UpsertRelationshipTool` rejecting a
  trailing-backslash name end-to-end), rather than only the unit-level `MCPToolUtils.quoteIdentifier` calls.
  **Skipped, with rationale**: the reviewer itself framed this as "not blocking... worth considering," and
  both call sites are one-line delegations straight into `quoteIdentifier` with no intervening logic of
  their own, so an end-to-end test would exercise the same code path the unit tests already cover directly,
  without adding meaningfully different coverage. Left as a documented option for a future call site that
  does more than delegate. No code or doc change applied for this item, so this cycle closed with a clean
  (empty) diff.

## Final state

**clean-approval.** Two review cycles ran; cycle 1's actionable nit was applied and pushed, cycle 2's bot
review found no blocking issues and its one suggestion was a documented, justified skip. No deferred items
remain for the developer.
