# ArcadeDB v26.2.1 Release Notes

## New Features

### Gremlin Multi-Database Support with ArcadeGraphManager

ArcadeDB now includes `ArcadeGraphManager`, a custom implementation of TinkerPop's `GraphManager` interface that provides **dynamic, on-demand registration** of databases as Gremlin graphs.

**Key Features:**

- **Automatic Registration**: When a Gremlin client requests a graph/traversal source by database name, it is automatically registered if the database exists
- **Multi-Database Access**: Access any database on the server through Gremlin without static configuration
- **The `g` Alias**: The standard `g` alias automatically maps to the default database ("graph" if it exists, otherwise the first available database)
- **Transaction Support**: Gremlin transactions now work correctly via the remote driver (fixes issue #1446)

**Example - Accessing Multiple Databases:**

```java
var cluster = Cluster.build()
    .port(8182)
    .addContactPoint("localhost")
    .credentials("root", "password")
    .create();

// Access any database by name - registered automatically on first access
var customers = traversal().withRemote(DriverRemoteConnection.using(cluster, "customers"));
var products = traversal().withRemote(DriverRemoteConnection.using(cluster, "products"));

// Or use "g" for the default database
var g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
```

---

## Migration

### Gremlin Server Configuration Changes

Starting with v26.2.1, the Gremlin Server uses `ArcadeGraphManager` for dynamic database registration. This simplifies configuration and enables multi-database support.

#### What Changed

1. **Graph Registration**: Graphs are now registered dynamically by `ArcadeGraphManager` instead of statically in configuration files
2. **Configuration Files**: The `graphs:` section in `gremlin-server.yaml` is no longer required
3. **The `graph` Binding**: The internal `graph` variable is now managed by `ArcadeGraphManager` to properly support TinkerPop sessions and transactions

#### Backward Compatibility

- **Existing Gremlin queries continue to work without modification**
- The `g` alias still works and maps to the default database
- Script-based queries using `g` are fully supported
- Bytecode-based queries work with any database name as the alias

#### Configuration Migration

**gremlin-server.yaml**

If you had a custom `gremlin-server.yaml` with static graph configuration:

```yaml
# OLD configuration (no longer needed)
graphs:
  graph: conf/arcadedb.properties
```

This static configuration is now **optional**. `ArcadeGraphManager` will automatically register any database when it is first accessed. You can safely remove the `graphs:` section.

**gremlin-server.groovy**

If you customized `gremlin-server.groovy`, ensure you are **NOT** overwriting the `graph` binding:

```groovy
// CORRECT: Only define the traversal source 'g'
globals << [g : traversal().withEmbedded(graph)]

// WRONG: Do NOT overwrite 'graph' - this breaks session/transaction support
// globals << [graph : traversal().withEmbedded(graph)]  // DON'T DO THIS
```

The `graph` variable **must remain a `Graph` object** (not a `GraphTraversalSource`) for TinkerPop's `SessionOpProcessor` to work correctly with transactions.

#### Accessing Multiple Databases

To access a specific database, use the database name as the traversal source alias:

```java
// Use the database name directly as the alias
var g = traversal().withRemote(DriverRemoteConnection.using(cluster, "customers"));
var g2 = traversal().withRemote(DriverRemoteConnection.using(cluster, "products"));
```

#### Transactions via Remote Driver

Gremlin transactions now work correctly via the remote driver:

```java
GraphTraversalSource g = traversal().withRemote(
    DriverRemoteConnection.using(cluster, "mydb"));

Transaction tx = g.tx();
GraphTraversalSource gtx = tx.begin();
try {
    gtx.addV("Person").property("name", "Alice").iterate();
    tx.commit();
} catch (Exception e) {
    tx.rollback();
}
```

---

## Bug Fixes

- Fixed: Gremlin transactions failing with "gremlin-groovy is not an available GremlinScriptEngine" when using remote driver (#1446)

---

## Breaking Changes

None. All existing Gremlin functionality remains backward compatible.
