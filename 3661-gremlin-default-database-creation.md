# Issue #3661 — Gremlin default `graph` database not created on Docker start

## Problem

Since ArcadeDB 2026.2.1, a fresh Docker container no longer auto-creates the default `graph`
database even though `gremlin-server.properties` still configures:

```
gremlin.graph=com.arcadedb.gremlin.ArcadeGraph
gremlin.arcadedb.directory=./databases/graph
```

### Root cause

Commit `a4a4569f4` ("fix: gremlin now supports multiple databases") replaced the static
`graphs:` section in `gremlin-server.yaml` with dynamic registration via `ArcadeGraphManager`.
The old static entry caused TinkerPop to call `ArcadeGraph.open(configuration)` at startup,
which created the database on disk if it did not exist. `ArcadeGraphManager` only opens
*existing* databases registered with `ArcadeDBServer`, so on a fresh container the database
was never created.

## Fix

### 1. Restore `graphs:` in `gremlin-server.yaml`

Both the production config (`package/src/main/config/gremlin-server.yaml`) and the test
resource (`gremlin/src/test/resources/gremlin-server.yaml`) now contain:

```yaml
graphs:
  graph: config/gremlin-server.properties
```

### 2. `GremlinServerPlugin.initPreConfiguredDatabases()`

A new private method processes every entry in `settings.graphs` before the Gremlin server
starts:

1. Resolves the properties file path (relative to `server.getRootPath()`).
2. Reads `gremlin.arcadedb.directory` from the file.
3. Derives the database name as the last path component (e.g. `./databases/graph` → `graph`).
4. Calls `server.getDatabase(dbName, createIfNotExists=true, allowLoad=true)` to create or
   open the database in ArcadeDBServer before Gremlin is started.

If the database already exists (normal restart or explicitly created via
`arcadedb.server.defaultDatabases`) the step is skipped.

## Test results

| Test | Result |
|------|--------|
| `GremlinDefaultGraphCreationIT#defaultGraphDatabaseCreatedOnGremlinPluginStart` | ✅ PASS |
| `GremlinDefaultGraphCreationIT#canConnectViaGremlinToAutoCreatedDatabase`        | ✅ PASS |
| `GremlinServerTest#getAllVertices`                                                | ✅ PASS |
| `GremlinTransactionIT` (3 tests)                                                 | ✅ PASS |
| `GremlinServerSecurityTest` (6 tests, 4 skipped)                                 | ✅ PASS |

## Files changed

- `package/src/main/config/gremlin-server.yaml` — restored `graphs:` section
- `gremlin/src/test/resources/gremlin-server.yaml` — restored `graphs:` section
- `gremlin/src/main/java/com/arcadedb/server/gremlin/GremlinServerPlugin.java` — added
  `initPreConfiguredDatabases()` and `resolveConfigPath()`
- `gremlin/src/test/java/com/arcadedb/server/gremlin/GremlinDefaultGraphCreationIT.java` — new
  regression test
