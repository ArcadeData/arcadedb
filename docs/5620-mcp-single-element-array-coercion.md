# 5620 - MCP: a single-element array is coerced to a string

Issue: https://github.com/ArcadeData/arcadedb/issues/5620

## Root cause

`MCPDispatcher` read every string-typed protocol member through
`JSONObject.getString(name, default)`, which is `if (isNull(name)) return default;` followed by
`getElement(name).getAsString()`. That last call is Gson's, and Gson converts rather than checks:

| value          | `getAsString()`                |
|----------------|--------------------------------|
| `"ping"`       | `"ping"`                       |
| `["ping"]`     | `"ping"` - unwrapped silently  |
| `["ping","x"]` | `IllegalStateException`        |
| `42` / `true`  | `"42"` / `"true"`              |
| `{}`           | `UnsupportedOperationException` |

`JsonArray.getAsString()` delegates to its single element when `size() == 1`. The arity of the array
therefore decided the outcome: `{"jsonrpc":"2.0","id":1,"method":["ping"]}` executed `ping` and returned a
success result, while the same member holding two elements was refused. JSON-RPC 2.0 draws no such
distinction - `method` is a String or the request is invalid.

The bare-primitive conversion is the same defect seen from another side: `{"method":42}` reached the
dispatch switch as `"42"` and was answered `-32601 Method not found` rather than as an invalid request.

Four members were affected, all of them string-typed by the protocol:

- `method` on the request object
- `params.uri` on `resources/read`
- `params.name` on `tools/call`
- `params.name` on `prompts/get`

## Fix

`MCPDispatcher.stringMember(json, name, default)` decides on the value's own JSON type instead of on what a
converting accessor makes of it. `JSONObject.opt` maps each JSON type to its Java counterpart without
converting between them and never raises, so comparing its result against `String` admits exactly the JSON
strings. Absent and JSON-null still fall back to the default, matching the accessor it replaces; any other
shape raises `IllegalArgumentException`, which each caller translates into the error that fits where the
member sits:

- `method` - `-32600 Invalid Request`, since the member belongs to the request envelope itself.
- `params.uri`, `params.name` - `-32602 Invalid params`.

For `tools/call` the guard deliberately answers with a JSON-RPC error rather than the `isError` tool
envelope its `try` block produces: no tool was named, so no tool can have failed. An absent `name` still
defaults to the empty string and still reaches the unknown-tool path as before, which keeps a malformed
member distinguishable from a missing one.

The object-typed members (`params`, `params.arguments`) are untouched: `getAsJsonObject()` raises for a
JSON array of any size, so the arity coercion never applied to them.

## Relationship to #5619

PR #5619 (issue #5585) is open on `issue-5585-mcp-malformed-params` and wraps the same four reads in
`try`/`catch (IllegalStateException | UnsupportedOperationException)` so a wrong-shaped member produces a
JSON-RPC envelope instead of a bodiless HTTP 500. This change and that one overlap on the string-typed
members and will conflict textually.

They are not redundant, and the resolution is mechanical. Type-checking subsumes catching *for the string
members*: once `stringMember` refuses every non-string shape up front, nothing in those reads can raise
`IllegalStateException` or `UnsupportedOperationException`, so the #5619 guards around them become dead.
What #5619 still carries and this change does not touch:

- the object-typed reads (`params`, `params.arguments`), where the raise is still the only signal
- the `isResponse` probe reading `jsonrpc` through `opt`
- the widened `UnsupportedOperationException` catch in `promptsGet`

Whichever merges second should keep the `stringMember` reads and keep #5619's guards only on the
object-typed members and `isResponse`.

## Tests

Six methods added to `server/src/test/java/com/arcadedb/server/mcp/MCPTransportConformanceTest.java`. Every
payload names a target that exists, so before the fix the coerced value was dispatched and the call
succeeded - the assertions were verified to fail on the unfixed code, not merely to pass on the fixed one.

| test | payload | expected |
|---|---|---|
| `requestWithSingleElementArrayMethodIsRejected` | `"method":["ping"]` | `-32600` |
| `requestWithNonStringPrimitiveMethodIsRejected` | `"method":42`, `"method":true` | `-32600` |
| `toolsCallWithSingleElementArrayNameIsRejected` | `"name":["list_databases"]` | `-32602` |
| `resourcesReadWithSingleElementArrayUriIsRejected` | `"uri":["arcadedb://graph/schema"]` | `-32602` |
| `promptsGetWithSingleElementArrayNameIsRejected` | `"name":["graphrag_query"]` with both required arguments | `-32602` |
| `absentAndNullStringMembersStillFallBackToTheirDefault` | no `params`, `{}`, `{"name":null}` | `isError` tool envelope, not `-32602` |

The `prompts/get` case supplies `database` and `question` on purpose: without them the request fails on the
missing-required-argument path with the same `-32602`, and the test would pass against the unfixed code
while proving nothing.

### Results

- Before the fix: `Tests run: 30, Failures: 5` - the five new malformed-member tests, and only those.
- After the fix: `Tests run: 30, Failures: 0`.
- Regression sweep over the MCP surface (`MCP*Test`, `HybridSearch*Test`, `GetServerSettings*`):
  `Tests run: 276, Failures: 0, Errors: 0, Skipped: 0`.

## Impact

Requests that were previously accepted through the coercion are now refused. That is the intent of the
issue, and the blast radius is small: a client sending `["ping"]` where `ping` belongs is malformed, and no
ArcadeDB client library produces that shape. No security consequence either way - the coerced method always
went through the same authentication, profile and permission gates.

The guards sit on the request-parsing path and cost one `instanceof` per member, replacing an accessor call
that did strictly more work.

## Follow-ups

- Reconcile with #5619 at merge time as described above.
- Other handlers reading request JSON through `getString(name, default)` inherit the same coercion. Nothing
  outside `MCPDispatcher` was surveyed here; a sweep of the HTTP handler surface would be a separate change.
