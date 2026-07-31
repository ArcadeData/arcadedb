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

PR #5619 (issue #5585) merged to main as `bfdc3a837` while this branch was already open, so `origin/main`
was merged in and the overlap resolved here rather than left for the maintainer. #5619 wraps the same reads
in `try`/`catch (IllegalStateException | UnsupportedOperationException)` so a wrong-shaped member produces a
JSON-RPC envelope instead of a bodiless HTTP 500.

The two are complementary, and the merged result keeps both:

- **Where a member is string-typed** (`method`, `params.uri`, `params.name`) the read now goes through
  `stringMember`. Its guard placement, error codes and messages are #5619's; only the read itself changed.
  `IllegalArgumentException` was added to each catch, since that is what `stringMember` raises.
- **Where a member is object-typed** (`params`, `params.arguments`) #5619's accessor and catch are kept
  untouched. `getAsJsonObject()` raises for a JSON array of any size, so arity never decided anything there
  and there is nothing for a type check to add.
- **`isResponse`** keeps #5619's `opt("jsonrpc")` comparison unchanged.
- **`promptsGet`** keeps #5619's widened catch, which already covers `IllegalArgumentException`.

Both test suites are kept in full: `MCPTransportConformanceTest` now runs 36 methods, the 24 that predate
both changes plus 6 from #5619 and 6 from this one. One comment carried over from #5619 needed correcting
rather than moving: `requestWithNonStringMethodIsRejected` explained its use of `{}` by noting that
`"method":["ping"]` "would silently succeed", which this change makes untrue. It now points at the test that
pins the array shape instead.

## Why the string members are narrowed and `arguments.key` is not

`feedback_guard_a_json_read_without_narrowing_its_coercion` records that PR #5622 rejected a proposal to
rewrite `formatArgs`' read of `arguments.key` as `opt` plus `instanceof String`. That hazard is real and it
is specifically a *divergence* hazard: `formatArgs` reads `arguments.key` to decide whether to mask a secret
`value` in the request log, and `SetServerSettingTool` reads the same member to decide what to write.
Narrowing one reader while the other still coerces means `"key":["arcadedb.server.rootPassword"]` stops
being recognised as a hidden setting and the secret is logged in clear, while the tool still applies it -
reopening what #5508 closed.

Every member narrowed here was checked against that test, by grepping for other readers of the same member:

| member | other readers | coercing? |
|---|---|---|
| `method` | `has("method")` in `dispatch` and `isResponse` | existence checks only |
| `params.uri` | none (`MCPResources` writes `uri`, never reads it from params) | - |
| `params.name` (tools/call) | `formatArgs(toolName, args)` | receives the value resolved here, not a second read |
| `params.name` (prompts/get) | none | - |

Each has exactly one coercing reader, which is the one narrowed, so no two readers can disagree.
`arguments.key` is deliberately left alone: it is the member with two readers, and it is #5622's to fix by
catching.

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
- After merging `origin/main` (which brought #5619's six tests in): `Tests run: 36, Failures: 0`.
- Regression sweep over the MCP surface (`MCP*Test`, `HybridSearch*Test`, `GetServerSettings*`):
  `Tests run: 276, Failures: 0` before the merge, `Tests run: 282, Failures: 0, Errors: 0, Skipped: 0` after.

## Impact

Requests that were previously accepted through the coercion are now refused. That is the intent of the
issue, and the blast radius is small: a client sending `["ping"]` where `ping` belongs is malformed, and no
ArcadeDB client library produces that shape. No security consequence either way - the coerced method always
went through the same authentication, profile and permission gates.

The guards sit on the request-parsing path and cost one `instanceof` per member, replacing an accessor call
that did strictly more work.

## Review

PR: https://github.com/ArcadeData/arcadedb/pull/5623

### Cycle 1 - `216cfe73a`

No `claude` review arrived within the 15-minute window; only `codacy-production[bot]` reported (0 issues,
0 complexity). While that cycle was waiting, #5619 merged to main, so `origin/main` was merged in and the
overlap resolved as described above. That produced `61bb20fa5`.

### Cycle 2 - `61bb20fa5`

`claude[bot]` reviewed: LGTM, with three observations marked non-blocking. All three were assessed and none
resulted in a code change. Nothing was deferred.

**1. "`resourcesRead` hardcodes the catch message while `toolsCall`/`promptsGet` use `e.getMessage()`" -
declined, premise is inverted.** Three of the four guards hardcode their message (`dispatch`,
`resourcesRead`, `toolsCall`); only `promptsGet` relays `e.getMessage()`. The split is structural rather
than accidental:

- A guard wrapped tightly around member reads states the expected shape, because its catch also covers
  Gson's `IllegalStateException` / `UnsupportedOperationException` from the object-typed read beside it.
  Those carry internal text such as `Not a JSON Object: ["a"]`, so relaying them would both read as noise
  and echo fragments of the request payload back to the client.
- `promptsGet`'s catch is the broad handler one: it also covers `MCPPrompts.get` raising
  `IllegalArgumentException` for an unknown prompt, where the message is the whole point of the answer.

Aligning `resourcesRead` to `e.getMessage()` would make it inconsistent with the two guards it sits next to
in order to match the one that is a different kind of catch. The messages are also #5619's, unchanged here
on purpose.

**2. "`promptsGet`'s catch now also absorbs `IllegalArgumentException` from `MCPPrompts.get`" - no action,
and the review agrees.** That catch already listed `IllegalArgumentException` before this change, so the
mapping of unknown-prompt to `-32602` is pre-existing and untouched.

**3. "The doc still cites pre-merge counts (30/276) while the PR body says 36/282" - declined, already
correct.** The committed doc carries both, deliberately: the 30/276 figures are the before-and-after of the
fix itself and are what demonstrate the tests failed first, while the post-merge 36/282 figures follow on
the next two lines.

## Follow-ups

- PR #5622 is still open and fixes `formatArgs`' read of `arguments.key` by catching. It does not conflict
  with this change, which leaves that member alone.
- Other handlers reading request JSON through `getString(name, default)` inherit the same coercion. Nothing
  outside `MCPDispatcher` was surveyed here; a sweep of the HTTP handler surface would be a separate change,
  and each site needs the two-reader check above before anything is narrowed.
