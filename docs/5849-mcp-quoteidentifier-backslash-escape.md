# Issue #5849 — MCPToolUtils.quoteIdentifier() does not escape backslashes

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
- `mcp/src/test/java/com/arcadedb/mcp/MCPToolUtilsTest.java`: two new regression tests —
  `rejectsBackslashToBlockSqlEscapeAmbiguity` (the exact `X\` case from the issue) and
  `rejectsBackslashInTheMiddleOfAnIdentifier`.

## Test results

- `MCPToolUtilsTest`: 6/6 pass (4 pre-existing + 2 new). The 2 new tests were confirmed to fail against
  the pre-fix code (`Expecting code to raise a throwable`), proving they reproduce the bug.
- Full `mcp` module test suite (`mvn -pl mcp test`): 294/294 pass, no failures or errors, no regressions.
- `quoteIdentifier` is used only inside the `mcp` module (`SampleRecordsTool`, `UpsertRelationshipTool`,
  `MCPToolUtils` itself), so the `mcp` module test suite is the full affected surface.
