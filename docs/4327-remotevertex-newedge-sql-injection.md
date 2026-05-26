# Fix #4327: RemoteVertex.newEdge SQL injection via unescaped property values

## Summary

`RemoteVertex.newEdge()` builds the `CREATE EDGE … SET prop = 'value'` SQL by concatenating
property values with no escaping. A value containing a single quote (`O'Brien`) breaks the
query; a malicious value injects arbitrary SQL.

## Root Cause

Lines 212-228 of `network/.../RemoteVertex.java`: the property-value loop writes string values
directly into the query `StringBuilder` wrapped in literal single quotes. No escaping of the
value content.

## Fix

Replace string interpolation with named SQL parameters (`:p0`, `:p1`, …) and pass the values
via the existing `command(language, sql, Map<String,Object>)` overload. This is the
suggested approach from the issue reporter.

## Files Changed

- `network/src/main/java/com/arcadedb/remote/RemoteVertex.java` — parameter binding in `newEdge`
- `network/src/test/java/com/arcadedb/remote/RemoteVertexTest.java` — new test class (regression)

## Test Results

`mvn test -pl network` - 373 tests, 0 failures, 0 errors.
New tests: `RemoteVertexTest` (5 tests) - all pass.

## PR

https://github.com/ArcadeData/arcadedb/pull/4340

## Review Cycles

### Cycle 1 - fc636fe0

gemini-code-assist posted 4 comments (state: COMMENTED), all variations of the same claim:
"map keys should not have a leading colon - use `p0` instead of `:p0`."

**Verdict: rejected (technically incorrect).** `NamedParameter.getValue()` tries
`params.get(":" + key)` first, so `:p0` map keys resolve correctly. This is also
the established convention in `QueryTest.java` (`params.put(":name", "Jay")`).
Pushed back with technical reasoning in the comment thread.

Working tree: clean after review - no changes needed.

**Final state: clean-approval** (no actionable review items)

## Verification

Run: `mvn test -pl network -Dtest=RemoteVertexTest`
