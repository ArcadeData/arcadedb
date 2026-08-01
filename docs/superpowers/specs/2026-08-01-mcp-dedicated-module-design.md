# Extract the MCP server into a dedicated `arcadedb-mcp` module

**Date:** 2026-08-01
**Status:** design approved, implementation plan pending

## Problem

The MCP (Model Context Protocol) server lives inside the `server` module at
`server/src/main/java/com/arcadedb/server/mcp/`. It has grown to 29 main classes and roughly
5,200 lines, plus about 6,400 lines of tests. It is now the largest single feature inside the
server module, and it keeps growing: tools, prompts, resources, hybrid and vector search legs.

Living inside `server` costs three things:

1. **No enforced boundary.** MCP code can reach any server internal, and server code can reach
   into MCP. Both directions have already happened.
2. **Not optional.** Every distribution ships MCP, and the modular builder cannot exclude it.
3. **Coupled build and test cycles.** MCP changes rebuild and retest the whole server module.

## Goals

- MCP becomes its own Maven module with a one-way dependency on `arcadedb-server`.
- MCP becomes excludable from custom distributions built with `arcadedb-builder.sh`.
- Zero behavior change for anyone using a standard distribution, including no configuration
  migration and no loss of the runtime enable/disable toggle.

## Non-goals

- Changing the MCP protocol surface, tool set, or configuration file format.
- Splitting `MCPServerPluginTest` (147 KB, one class). Tracked as a follow-up.
- End-user documentation. The docs live in a separate repository; an install/enable page update
  is a follow-up issue there.

## Decisions

Four decisions shape everything below. They were settled before design.

| # | Decision | Chosen |
|---|---|---|
| 1 | Optionality | Optional, excludable plugin module |
| 2 | Java package | Rename `com.arcadedb.server.mcp` to `com.arcadedb.mcp` |
| 3 | AI coupling | Extract the shared JSON producers down into `server` |
| 4 | Activation | Auto-discover when the jar is on the classpath |

Decision 4 is what keeps decision 1 from becoming a breaking change: module-level optionality
comes from whether the jar was built into the distribution, not from a `SERVER_PLUGINS` entry a
user must add on upgrade.

## Section 1: Module boundary and dependency direction

New module `mcp/`, artifact `arcadedb-mcp`, listed in the root `pom.xml` immediately after
`server`. The artifact is not named `mcpw`: the `w` suffix in `postgresw`, `mongodbw`, `redisw`,
and `grpcw` marks a reimplementation of a foreign wire protocol, which MCP is not.

The module follows the wire-protocol module standard in `CLAUDE.md` verbatim:

| Dependency | Scope |
|---|---|
| `arcadedb-server` | `provided` |
| `arcadedb-server` test-jar | `test` |
| no other protocol module | n/a |

All of `server/src/main/java/com/arcadedb/server/mcp/**` moves to
`mcp/src/main/java/com/arcadedb/mcp/**`. Package renames:

- `com.arcadedb.server.mcp` to `com.arcadedb.mcp`
- `com.arcadedb.server.mcp.tools` to `com.arcadedb.mcp.tools`
- `com.arcadedb.server.mcp.prompts` to `com.arcadedb.mcp.prompts`

**The resulting dependency direction is strictly one-way: `arcadedb-mcp` depends on
`arcadedb-server`, and nothing in the server module names an MCP type.** This is enforced by
compilation, not by review: the server module simply does not have the classes on its compile
classpath.

The inbound surface MCP consumes from server is already public API and needs no new exports:
`ArcadeDBServer`, `HttpServer`, `AbstractServerHttpHandler`, `ExecutionResponse`,
`ServerSecurityUser`, `ServerQueryProfiler`, `HAServerPlugin`. This is the same set `ha-raft` and
`metrics` already consume at `provided` scope.

Three things stay in the server module deliberately:

- **`McpApiSpec`** stays at `server/http/handler/openapi/`. The `PluginApiSpec` Javadoc already
  documents the governing decision: plugin routes are declared in the server module and declared
  unconditionally, because the specification has to be deterministic for client generation and
  because plugin modules do not have swagger on their `provided` classpath. `McpApiSpec` gains a
  `Requires MCPPlugin:` precondition sentence matching the existing `RAFT_REQUIRED` and
  `METRICS_REQUIRED` wording. This follows the existing decision rather than reopening it.
- **The `mcp.json` config file location** stays at the server root path. No user migration.
- **The AI feature** stays at `server/ai/`, with its MCP dependency severed (Section 2).

## Section 2: Severing the AI to MCP dependency

### The problem

`AiChatHandler`, `AiAnalyzeProfilerHandler`, and `ToolDispatcher` (about 1,600 lines under
`server/ai/`) import `MCPConfiguration`, `GetSchemaTool`, and `ServerStatusTool`. With
`arcadedb-server` at `provided` scope in the new module, the server can no longer compile against
those types. The dependency must be cut, not relocated.

The AI code is not really using MCP. `ToolDispatcher.effectiveMcpConfig()` constructs
`new MCPConfiguration(null)` with reads forced on, with a comment stating it deliberately does not
want to inherit MCP's policy. It is borrowing two JSON producers and paying for a policy object it
then neutralizes.

### The extraction

**`com.arcadedb.server.info.SchemaInfo#toJSON(Database, String)`** - the body of today's
`GetSchemaTool.buildSchema`, moved verbatim. It already takes a `Database`, takes no
`MCPConfiguration`, and its Javadoc already declares that it performs no permission or
authorization check and that the caller is responsible for both. MCP's `GetSchemaTool` and
`MCPResources` (which renders `arcadedb://{database}/schema` from the same method, deliberately,
so the two cannot drift) both delegate to it.

**`com.arcadedb.server.info.ServerInfo#toJSON(ArcadeDBServer, Predicate<String>, boolean)`** -
carved out of `ServerStatusTool.execute`. Parameters are the server, a database-visibility
predicate, and whether to include the HA block. It emits version, serverName, languages, the
visible-database `TreeSet`, and the optional HA block.

The MCP policy stays in MCP:

```java
// MCP: unchanged behavior, policy layered on top
if (!config.isAllowReads())
  throw new SecurityException("Read operations are not allowed by MCP configuration");
return ServerInfo.toJSON(server, db -> MCPToolUtils.canReadDatabase(user, config, db), config.isAllowAdmin());

// AI: what the synthesized permissive config actually meant all along
return ServerInfo.toJSON(server, user::canAccessToDatabase, false);
```

The AI call is behavior-identical to today. With a permissive config, `canReadDatabase` collapses
to `user.canAccessToDatabase` (the config's per-database allowlist is empty, so
`isUserAllowed` passes and `isAllowReads` is true), and `isAllowAdmin()` is false, so the HA block
was already omitted. `ToolDispatcher.effectiveMcpConfig()` is deleted outright.

**`com.arcadedb.server.security.DatabaseUserContext`** - a small helper that exists to fix a
defect the move would otherwise force us to duplicate.

`MCPToolUtils.bindCurrentUser` binds the authenticated principal onto the thread-local
`DatabaseContext`. That binding is the fix for GHSA-6x73-v3rc-f57c, and `MCPDispatcher` clears it
in a `finally` so the principal never leaks onto a pooled worker thread. The AI handlers call
`GetSchemaTool.execute`, and therefore `bindCurrentUser`, but never clear it: `AiChatHandler`
extends `AbstractServerHttpHandler`, not `DatabaseAbstractHandler`, and only the latter cleans the
thread-local.

The practical blast radius today is limited. `DatabaseAbstractHandler` sets `currentUser` on every
database-scoped request and overwrites a value that differs from the requesting user, so a stale
binding does not grant escalation on the normal routes. But it is an unbalanced bind sitting on a
pooled thread, and copying that shape into a second module is not acceptable.

So: `DatabaseUserContext.runAs(DatabaseInternal, ServerSecurityUser, Supplier<T>)` binds, runs, and
clears in a `finally`. `MCPToolUtils.bindCurrentUser` delegates to its `bind` method so the GHSA
fix has exactly one implementation. The AI call sites wrap their schema reads in `runAs`.

### Resulting invariant

`grep -rn "com.arcadedb.mcp" server/src/main` returns nothing, and the server module compiles with
`arcadedb-mcp` entirely absent from its classpath.

## Section 3: Plugin lifecycle and wiring

**`com.arcadedb.mcp.MCPPlugin implements ServerPlugin`**, declared in
`mcp/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin`. Same shape as
`RaftHAPlugin` and `PrometheusMetricsPlugin`.

### Configuration ownership

The `MCPConfiguration` field and the `getMCPConfiguration()` accessor leave `ArcadeDBServer`
entirely. `MCPPlugin.configure(server, configuration)` performs what `ArcadeDBServer` lines
309-312 do today:

```java
mcpConfiguration = new MCPConfiguration(server.getRootPath());
mcpConfiguration.load();
mcpConfiguration.warnUnknownDatabaseOverrides(server.getDatabaseNames());
```

The ordering works unchanged: `loadDatabases()` runs at `ArcadeDBServer:304` and `BEFORE_HTTP_ON`
plugins start at `:320`, so database names are available when the override warning runs.

### Route registration

`MCPPlugin.registerAPI(httpServer, routes)` takes the four lines out of
`HttpServer.setupRoutes()` verbatim, including the comment explaining that the routes are always
registered so the runtime toggle keeps working:

```java
routes.addExactPath("/api/v1/mcp", new MCPHttpHandler(httpServer, server, mcpConfiguration));
routes.addExactPath("/api/v1/mcp/config", new MCPConfigHandler(httpServer, mcpConfiguration));
```

The plugin uses the default `BEFORE_HTTP_ON` priority, and that choice is load-bearing:
`startPlugins(BEFORE_HTTP_ON)` runs at `ArcadeDBServer:320`, before `httpServer.startService()` at
`:322`, and `setupRoutes()` is called from inside `startService()` at `HttpServer:182`. So
`configure()` has already loaded the configuration by the time `registerAPI` needs it. This is the
opposite of `RaftHAPlugin`'s situation, whose Javadoc warns that `registerAPI` precedes
`configure()` for `AFTER_HTTP_ON` plugins.

`HttpServer` loses those four lines and the two MCP imports.

### Auto-discovery

`PluginManager.discoverPluginsOnMainClassLoader()` currently activates a classpath plugin only if
it is named in `SERVER_PLUGINS`, with one hardcoded exception:

```java
final boolean isRaftPlugin = autoDiscoverRaft && "RaftHAPlugin".equals(name);
if (configured || isRaftPlugin) {
```

Rather than adding a second hardcoded name, `ServerPlugin` gains one default method:

```java
default boolean isAutoDiscovered(final ContextConfiguration configuration) {
  return false;
}
```

- `MCPPlugin` overrides it to return `true`.
- `RaftHAPlugin` overrides it with the check `PluginManager.isHAEnabled()` performs today:
  `configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) || configuration.isHAImplicitlyEnabled()`.
- `PluginManager`'s condition becomes `configured || pluginInstance.isAutoDiscovered(configuration)`,
  and it stops knowing plugin names. `isHAEnabled()` and the `autoDiscoverRaft` local are removed.

Each plugin owns its own activation rule, which is where that knowledge belongs.

### Stdio transport

`MCPStdioServer.main` currently calls `server.getMCPConfiguration()` and force-enables it. It now
locates the plugin instead, via a static helper in the mcp module:

```java
public static MCPPlugin of(final ArcadeDBServer server)   // scans server.getPlugins()
```

then force-enables as before. No new server API is required, and the same helper serves the tests
that reach for the configuration today.

`package/src/main/scripts/mcp-stdio.sh` updates its main class to
`com.arcadedb.mcp.MCPStdioServer`. Its classpath is already a `lib/*` glob, so nothing else moves.

## Section 4: Packaging, OpenAPI, and Studio

### Standard distribution is unchanged

`package/pom.xml` gains an `arcadedb-mcp` dependency alongside `bolt`, `redisw`, `postgresw`, and
the others, so the jar lands in `lib/` of every normal build, is auto-discovered, and MCP behaves
exactly as today. `coverage/pom.xml` gains the module so MCP coverage keeps being reported.

### Modular builder

`package/arcadedb-builder.sh` gains `mcp` in `REGULAR_MODULES`, not `SHADED_MODULES`: MCP brings no
third-party dependencies of its own, only `server` and `engine`. It also gains a description line
and an entry in the `--modules=` help text, and `package/README-BUILDER.md` gains the matching
entry.

This is the surface where "optional" becomes real. A build such as `--modules=postgresw` produces a
distribution with no MCP jar, no plugin, and no MCP routes.

### Docker

The `io.modelcontextprotocol.server.name` label in `package/src/main/docker/Dockerfile` is
unchanged.

### OpenAPI

`McpApiSpec` stays in the server module and stays unconditional, per Section 1. Its three
operations gain a `Requires MCPPlugin:` precondition sentence. Because the specification remains
deterministic, `OpenApiSpecGenerationIT`'s exact-operation-inventory assertion (which lists
`POST /api/v1/mcp`, `GET /api/v1/mcp/config`, `POST /api/v1/mcp/config`) is untouched.

### Studio

`loadMCPConfig()` at `studio-server.js:870` has a `.fail()` handler that pipes
`jqXHR.responseText` straight into `globalNotifyError`. On a distribution built without MCP,
`/api/v1/mcp/config` returns 404 and the user gets an error toast with an unhelpful body.

Fix: branch on `jqXHR.status === 404` and put the MCP panel into a disabled
"MCP module not installed in this distribution" state instead of raising an error. The save path at
line 964 gets the same treatment; it should never fire once the panel is disabled, but it must not
produce a mystery toast if it does.

## Section 5: Test migration and verification

### Bulk move

All 13 classes under `server/src/test/java/com/arcadedb/server/mcp/**` (about 6,400 lines) move to
`mcp/src/test/java/com/arcadedb/mcp/**`. `mcp/pom.xml` takes `arcadedb-server` test-jar at `test`
scope; the parent pom already builds test-jars and `bolt` consumes the server's exactly this way,
so `BaseGraphServerTest` is available with no new plumbing.

The eight `getServer(0).getMCPConfiguration()` call sites become
`MCPPlugin.of(getServer(0)).getConfiguration()`.

### Two MCP-dependent tests filed outside the mcp package

- **`MCPAuthorizationBindingIT`** (`server/src/test/java/com/arcadedb/server/security/`) is an MCP
  test that happens to live under security. It moves to the mcp module with the rest.
- **`HttpJsonArrayPayloadTest`** (`server/src/test/java/com/arcadedb/server/http/`) is **not** an
  MCP test. It is the regression test for issue #5415, covering `AbstractServerHttpHandler`'s
  top-level-JSON-array parsing; its own comment states MCP is used only because it "is the in-tree
  handler that opts in to an array body." Moving it would relocate a server-pipeline guard out of
  the server module. It stays put and gets a test-only handler in server's test sources that
  overrides `acceptsArrayPayload()`, which makes it test the mechanism directly rather than through
  a borrowed handler. JSON-RPC batch behavior stays covered by `MCPTransportConformanceTest`; that
  coverage is to be confirmed during implementation, not assumed.

### New tests

Four, each targeting something the refactor could silently break:

1. **Auto-discovery** (mcp module): start a server with no `SERVER_PLUGINS` entry, assert
   `MCPPlugin` is present in `server.getPlugins()` and that `/api/v1/mcp/config` answers 200. This
   is the assertion that decision 4 actually holds.
2. **`isAutoDiscovered` contract** (server module): a `PluginManager` unit test with a fake plugin
   returning true and false, proving activation follows the SPI and not a hardcoded name. This is
   the only coverage the new SPI would otherwise have.
3. **AI works with MCP absent** (server module): assert `SchemaInfo.toJSON` and `ServerInfo.toJSON`
   produce the expected JSON for a known schema, and that the AI handlers reach them. Because the
   server module no longer has MCP on its classpath, this test passing is the proof the coupling is
   severed; no grep-based guard is needed, the compiler is the enforcement.
4. **Balanced binding** (server module): after an AI schema read, assert the thread's
   `DatabaseContext` has no current user. This is the test that catches the Section 2 defect
   regressing.

Each new test is stubbed to a no-op first to confirm it goes red before the real implementation
lands. This matters most for test 4, which would pass trivially if the assertion targets the wrong
thread-local.

### Verification sequence

```
mvn -q -pl mcp -am install                 # new module and its dependencies
mvn -q -pl server -am install              # server compiles with MCP absent
mvn -q -pl server,mcp -am install -DskipITs=false -Dit.test='MCP*IT,OpenApiSpec*IT'
mvn -q install                             # full reactor
```

Notes:

- `-am` is required. A shared local `.m2` otherwise feeds a stale `arcadedb-server` jar into the
  mcp module's run.
- Use `verify` or `install`, never bare `mvn test`, for a full reactor run: `arcadedb-gremlin-it`
  consumes `arcadedb-gremlin`'s package-phase artifacts.
- ITs are skipped unless `-DskipITs=false` is passed.
- Server ITs that bind port 2480 cannot run while a Homebrew ArcadeDB service holds the port. Stop
  it, or state explicitly which ITs went unverified. Do not report a clean run that did not happen.

### Follow-up, not in scope

`MCPServerPluginTest` is 147 KB in a single class. Splitting it along tool boundaries would make the
mcp module much easier to work in, but it is an independent change and folding it into the
extraction would make the diff unreviewable. File as a separate issue.

## Risk register

| Risk | Mitigation |
|---|---|
| Existing MCP users lose the endpoint on upgrade | Decision 4: auto-discovery on classpath presence. Standard distribution ships the jar, so behavior is identical. |
| A hand-rolled launcher targets `com.arcadedb.server.mcp.MCPStdioServer` | Accepted cost of decision 2. The FQN is referenced only by `mcp-stdio.sh` inside the repository. Call it out in the release notes. |
| AI feature silently changes behavior | Test 3 pins the JSON shape; the `ServerInfo` predicate and `includeHA` arguments are shown above to be equivalent to the permissive-config path. |
| Principal leaks on a pooled thread | `DatabaseUserContext.runAs` binds and clears in a `finally`; test 4 asserts it. |
| Studio shows a mystery error on an MCP-less build | Explicit 404 branch and a disabled-panel state. |
| Raft auto-discovery regresses when its rule moves into the plugin | The moved expression is identical to `PluginManager.isHAEnabled()`; existing ha-raft ITs cover activation. |
