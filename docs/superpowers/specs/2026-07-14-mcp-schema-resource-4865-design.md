# MCP: expose schema as a Resource (#4865)

**Issue:** [#4865](https://github.com/ArcadeData/arcadedb/issues/4865)
**Epic:** [#4859 - MCP GraphRAG & Agent-Memory Surface](https://github.com/ArcadeData/arcadedb/issues/4859) (Wave 1)
**Date:** 2026-07-14
**Status:** approved

## Problem

ArcadeDB's MCP server implements exactly one MCP protocol primitive: **Tools**. `handleInitialize` advertises `capabilities.tools` and nothing else; there are zero matches for `resources/list` or `resources/read` in the `com.arcadedb.server.mcp` package.

The cost is a wasted turn. Every agent session that touches an unfamiliar database must spend a tool call on `get_schema` before it can write a single query, and the server's own `instructions` block tells it to (rule 4). Schema is static reference material, which is precisely what the Resources primitive exists to carry: a client that supports Resources can auto-load it at session start, with no model turn spent at all.

This is epic justification **#4 (structured shaping)**, delivered through the correct protocol primitive instead of yet another tool. It is the one child issue that *reduces* pressure on the tool-count discipline rather than adding to it.

## Governing constraint

The MCP layer stays **schema-agnostic**, per the maintainer's ruling on the epic thread: MCP is "just another protocol/api, like the http api or grpc or bolt." The resource returns the database's real structure and imposes no governance or memory vocabulary on top of it.

## Design

Three layers. Two are new; one is the refactor the issue asks for.

### 1. `MCPDispatcher` (new, `com.arcadedb.server.mcp`)

`MCPHttpHandler` and `MCPStdioServer` are today near-duplicates. Each carries its own static `TOOLS_LIST`, its own `MCP_PROTOCOL_VERSION`, its own method `switch`, and its own copies of `jsonRpcResult`, `jsonRpcError`, and `toolError`. Adding a second protocol primitive would pay that duplication twice more, and the divergence between them is already leaking real bugs (see *Behavior reconciliation* below).

`MCPDispatcher` becomes the single source of truth for the JSON-RPC method surface: protocol version, `TOOLS_LIST`, the `instructions` text, the gating checks, the method `switch`, and one copy each of the envelope helpers. It is instantiated per transport with `(server, config, transportName)` so log lines stay attributable.

**Boundary: transports own I/O and parsing; the dispatcher owns protocol.** HTTP already receives a parsed `JSONObject` from `AbstractServerHttpHandler`. Stdio parses its own line and raises its own `-32700` on malformed JSON. Neither concern belongs in the dispatcher, so `dispatch()` accepts an already-parsed request and never sees a byte of transport.

Return type is transport-neutral:

```java
public record MCPResponse(int httpStatus, JSONObject json) {}   // json == null => notification, no reply
```

- `MCPHttpHandler` maps it to `ExecutionResponse`: `json == null` becomes `204 ""`, otherwise `(httpStatus, json.toString())`.
- `MCPStdioServer` ignores `httpStatus` entirely, prints `json.toString()`, or prints nothing when `json` is null.

Both transports collapse to thin adapters.

### 2. `MCPResources` (new, `com.arcadedb.server.mcp`)

All Resources logic lives here, so neither transport learns anything about URIs.

```java
public static JSONObject list(ArcadeDBServer server, ServerSecurityUser user, MCPConfiguration config);
public static JSONObject read(ArcadeDBServer server, ServerSecurityUser user, MCPConfiguration config, String uri);
```

### 3. `GetSchemaTool` refactor

The schema walk (currently the body of `execute()`, lines 62-127) is extracted verbatim to:

```java
public static JSONObject buildSchema(final Database database, final String databaseName);
```

`execute()` shrinks to: `allowReads` check, `MCPToolUtils.resolveDatabase`, `buildSchema`. `MCPResources.read` calls the same `buildSchema`.

This is what makes the issue's headline acceptance criterion, that tool output and resource content are identical, true **by construction** rather than by coincidence. It cannot drift, because there is only one implementation.

## URI scheme

`arcadedb://{database}/schema`, per the issue.

**Parse it with plain string operations, deliberately not `java.net.URI`.** `URI.getHost()` applies hostname syntax rules: it lowercases the authority and returns `null` for an authority containing an underscore. ArcadeDB database names are case-sensitive and may legally contain `_`, so `arcadedb://My_DB/schema` would silently resolve to nothing, or to the wrong database, under `java.net.URI`.

The parse is therefore: strip the `arcadedb://` prefix, require the `/schema` suffix, take the middle as the database name verbatim. Anything that does not match that shape is a resource-not-found. Database names cannot contain `/` (they are directory names on disk), so the form round-trips exactly against what `list` emits.

## `resources/list`

Filters `server.getDatabaseNames()` through `user.canAccessToDatabase(db)`, sorted by database name for deterministic output.

`canAccessToDatabase` is `databasesNames.contains(SecurityManager.ANY) || databasesNames.contains(databaseName)`, which is exactly the predicate `ListDatabasesTool` open-codes as `getAuthorizedDatabases()` plus a `*` wildcard check, and exactly the one `MCPToolUtils.resolveDatabase` (and therefore `get_schema`) already applies. Using it satisfies the issue's "access control matches `get_schema`" criterion literally, and keeps the resource list consistent with `list_databases`. A test asserts the two sets are equal.

One entry per accessible database:

```json
{
  "uri": "arcadedb://graph/schema",
  "name": "graph schema",
  "description": "Schema of the ArcadeDB database 'graph': types (vertex, edge, document), properties, indexes, inheritance.",
  "mimeType": "application/json"
}
```

## `resources/read`

```json
{
  "contents": [
    {
      "uri": "arcadedb://graph/schema",
      "mimeType": "application/json",
      "text": "{\"database\":\"graph\",\"types\":[...]}"
    }
  ]
}
```

The `text` value is `buildSchema(...).toString()`, byte-identical to what the `get_schema` tool returns.

## Capability advertisement

In the dispatcher's single `handleInitialize`:

```java
capabilities.put("resources", new JSONObject().put("listChanged", false).put("subscribe", false));
```

Both flags are `false` and both are honest: the schema resource list changes only when a database is created or dropped, and no change-notification or subscription machinery is being built.

## The `instructions` block

The issue asks that "docs should note Resources as the preferred path for clients that support it." The out-of-tree `arcadedb-docs` repository is a follow-up (see *Out of scope*), but the `instructions` string returned by `initialize` is documentation that ships in this repo and is read by every connecting agent, so it is updated here.

Rule 4 currently reads:

> 4. Call get_schema before writing queries against an unfamiliar database to understand its types and properties.

It gains one sentence:

> 4. Call get_schema before writing queries against an unfamiliar database to understand its types and properties. If your client supports MCP Resources, prefer reading `arcadedb://{database}/schema` instead: it carries the same content without spending a tool call.

The other four rules are unchanged. `get_schema` stays, and stays recommended, for clients that do not implement Resources.

## Error handling

`MCPResources` signals failure with exception types; the dispatcher maps them to JSON-RPC codes.

| Condition | Signal | JSON-RPC code |
|---|---|---|
| `allowReads=false`, on `resources/list` | none, returns `{"resources": []}` | 200 (success) |
| `allowReads=false`, on `resources/read` | `SecurityException` | `-32600` |
| unknown database, on `read` | `MCPResourceNotFoundException` | `-32002` |
| unauthorized database, on `read` | `MCPResourceNotFoundException` | `-32002` |
| malformed URI, on `read` | `MCPResourceNotFoundException` | `-32002` |
| anything else | `Exception` | `-32603` |

`MCPResourceNotFoundException` is a new `RuntimeException` in `com.arcadedb.server.mcp`. `-32002` is the MCP-standard "resource not found" code.

Two deliberate asymmetries:

**`list` hides, `read` errors.** `resources/list` is a discovery call that most clients issue unprompted at session start. Returning an empty array when reads are disabled keeps a reads-disabled server quiet, where an error would show a banner to every connecting agent. `resources/read` is an explicit request, so it fails loudly rather than returning nothing and letting the caller infer emptiness means absence.

**Unknown and unauthorized collapse to one indistinguishable `-32002`.** This departs from the house `MCPToolUtils.resolveDatabase` pattern, which enumerates the available databases in its error text so the model can self-correct without a round-trip. That pattern is right for tools, where the model supplies the database name from memory and can guess wrong. It is wrong here: `resources/read` URIs come from `resources/list`, so a legitimate caller never guesses one, and distinguishing the two errors would turn `resources/read` into a probe for which databases exist.

## Behavior reconciliation

Unifying the two transports forces the places where they currently disagree to resolve. Both resolve toward the HTTP behavior, which is the more correct one in each case.

| Behavior | HTTP today | Stdio today | Unified |
|---|---|---|---|
| `isEnabled()` gate | 503 if disabled | absent | gated |
| `isUserAllowed()` gate | 403 if not allowed | absent | gated |
| `instructions` in `initialize` | present | absent | present |
| unauthenticated (`user == null`) | 401 | n/a | 401 |

Check order in `dispatch()` preserves HTTP's current ordering, auth before enablement, so server state is not leaked to unauthenticated callers.

**This is a behavior change for stdio, and warrants a release note.** Stdio will now honor `allowedUsers`. The default path is unaffected: `MCPStdioServer.main()` already calls `config.setEnabled(true)` and authenticates as `root`, and `root` is the default sole entry in `allowedUsers`. A stdio deployment that had rewritten `allowedUsers` to omit `root` would newly be refused, which is the correct outcome and is arguably a latent bug being fixed.

Stdio gaining `instructions` is a straightforward win: it is the transport Claude Desktop and Cursor actually use, and it is the one that was missing the guidance text.

## Files touched

**New**
- `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java`
- `server/src/main/java/com/arcadedb/server/mcp/MCPResources.java`
- `server/src/main/java/com/arcadedb/server/mcp/MCPResourceNotFoundException.java`

**Modified**
- `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java` (shrinks to an HTTP adapter)
- `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java` (shrinks to a stdio adapter)
- `server/src/main/java/com/arcadedb/server/mcp/tools/GetSchemaTool.java` (extract `buildSchema`)
- `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java` (additions only)
- `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java` (additions only)

**New test**
- `server/src/test/java/com/arcadedb/server/mcp/MCPResourcesTest.java`

`MCPPermissionsTest` is deliberately **not** touched. It is a pure unit test that constructs a bare `MCPConfiguration` with no server, and both `MCPResources.list` and `MCPResources.read` require an `ArcadeDBServer` and a `ServerSecurityUser`. The `allowReads=false` coverage therefore lands in `MCPResourcesTest` (direct calls) and `MCPServerPluginTest` (over the wire).

## Testing

The dispatcher extraction is the risky part of this change, and the existing MCP suites are its safety net. **Every current test must stay green, and no existing test method may be edited at all.** New coverage arrives as new test methods, never as amended assertions on old ones, so that the old suites remain an untainted control. If an existing test method needs editing to pass, that is a signal the refactor changed behavior, and must be investigated rather than accommodated.

This is stricter than it first appears. Stdio gaining `instructions` could have been folded into the existing `MCPStdioServerTest.initialize` assertion; instead it gets its own test method, leaving `initialize` free to keep proving that nothing else about the initialize response moved.

| Suite | Coverage |
|---|---|
| `MCPResourcesTest` (new) | Direct, transport-free calls into `MCPResources`: `list` shapes one resource per accessible database; `list` is empty when `allowReads=false`; `read` content equals `GetSchemaTool.execute` output; `read` raises `MCPResourceNotFoundException` for an unknown database and for each malformed URI shape; `read` raises `SecurityException` when `allowReads=false`; `parseSchemaURI` preserves case and underscores (the `java.net.URI` trap) and round-trips against `schemaURI` |
| `MCPServerPluginTest` (HTTP) | `initialize` advertises the `resources` capability; `resources/list` contains `arcadedb://graph/schema` with `mimeType: application/json`; **`get_schema` tool output equals `resources/read` content text** (the issue's explicit criterion); the `resources/list` database set equals the `list_databases` set; unknown database and malformed URI both return `-32002`; a user unauthorized for a database does not see it listed; `allowReads=false` makes `resources/list` empty and `resources/read` return `-32600` |
| `MCPStdioServerTest` | `resources/list` and `resources/read` work over stdio; `initialize` advertises `resources` and now carries `instructions` |
| all existing MCP tests | Regression net for the dispatcher extraction: `tools/list` still returns 11 tools, every `tools/call` behaves as before, `ping` and `notifications/initialized` unchanged |

The existing `capabilities` assertions use `capabilities.has("tools")` rather than asserting `tools` is the *only* key, so adding `resources` alongside it is a no-op for them. Verify this during implementation rather than assuming it.

`git diff --stat main -- server/src/test/java/com/arcadedb/server/mcp/` at the end of the work must show additions only, plus the new `MCPResourcesTest.java`.

## Risks

**Sequencing against the rest of Wave 1.** This rewrites both files that #4860, #4863, and #4864 also edit, and the epic flags that collision explicitly. **#4865 should land first**, with the siblings rebased onto it. That ordering is cheaper in both directions: each sibling adds a tool, which after this refactor means one `TOOLS_LIST` line and one `switch` arm instead of two of each. Landing #4865 last would instead force it to absorb every sibling's duplicated edits before deleting them.

**Stdio behavior change.** Covered above under *Behavior reconciliation*. Needs a release note, not a code change.

**Log attribution.** `LogManager.instance().log(this, ...)` will now report `MCPDispatcher` as the source class rather than `MCPHttpHandler` or `MCPStdioServer`. The `transportName` field is carried into the message text so the two transports stay distinguishable in logs.

## Out of scope

- **`resources/templates/list`.** `resources/list` enumerates every accessible database concretely, so a URI template adds no reachability. Justified only if per-database scoping (#4868) later makes concrete enumeration impractical.
- **Subscriptions.** `subscribe` and `listChanged` are advertised `false`; no `notifications/resources/updated` machinery is built.
- **Any resource other than schema.** Sample records, server status, and index statistics are all plausible future resources; none is needed to satisfy #4865, and the epic enforces surface discipline.
- **Collapsing the remaining transport duplication.** The dispatcher extraction takes the JSON-RPC surface. Whatever incidental duplication survives in the two adapters is left alone.
- **Documentation.** The MCP docs live in the out-of-tree `arcadedb-docs` repository and are tracked as a separate follow-up, consistent with how #4862 handled it.
- **Any relaxation of default permissions.** `enabled=false` and all write flags `false` are unchanged; this issue adds no new configuration flag.
