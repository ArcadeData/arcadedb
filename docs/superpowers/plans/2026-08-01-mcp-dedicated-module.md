# Extract MCP into a dedicated `arcadedb-mcp` module - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the MCP server out of `arcadedb-server` into a new optional `arcadedb-mcp` Maven module with a strictly one-way dependency, without changing behavior for anyone using a standard distribution.

**Architecture:** MCP becomes a `ServerPlugin` in its own module holding `arcadedb-server` at `provided` scope. It auto-discovers when its jar is on the classpath, registers its own HTTP routes via `registerAPI`, and owns its own `MCPConfiguration`. The server-to-MCP dependency that the Studio AI feature currently has is cut by extracting two JSON producers down into the server module.

**Tech Stack:** Java 21, Maven multi-module reactor, JUnit 5 + AssertJ, Undertow, ServiceLoader-based plugin discovery, Node's built-in test runner for Studio JS.

**Spec:** `docs/superpowers/specs/2026-08-01-mcp-dedicated-module-design.md`

## Global Constraints

- Parent version is `26.8.1-SNAPSHOT`. Module poms use `${project.parent.version}` for sibling ArcadeDB dependencies, never a literal.
- Every new `.java`, `.xml`, and `.js` file starts with the Apache 2.0 header used throughout the repo (copy it verbatim from any neighboring file in the same directory).
- No new third-party dependencies. `arcadedb-mcp` depends only on `arcadedb-server` (`provided`) and its test-jar (`test`).
- Never use the em dash character (`—`) in any file or commit message. Use `-`, a comma, or rephrase.
- Use `final` on variables and parameters. Single-statement `if` bodies take no braces. Import classes; do not use fully qualified names inline.
- Tests assert with AssertJ in the form `assertThat(x.isY()).isTrue();`.
- Do not add Claude as an author on any source file. Do not add issue references to Javadoc.
- **Do not run `git commit` unless the human asks.** Each task below ends with a "Stage and report" step instead: stage the files, run `git status --short`, and report. The human commits after review.
- Run `mvn` with `verify` or `install`, never bare `mvn test`, for any multi-module run. Always pass `-am` when building a single module so a stale `arcadedb-server` jar in the shared `~/.m2` cannot poison the run.
- Integration tests (`*IT.java`) are skipped unless `-DskipITs=false` is passed.
- Server ITs bind port 2480. If a Homebrew ArcadeDB service holds it, stop the service or report explicitly which ITs went unverified. Never report a clean run that did not happen.

---

## File Structure

**Task 1 - plugin activation SPI**
- Modify `server/src/main/java/com/arcadedb/server/ServerPlugin.java` - add `isAutoDiscovered`
- Modify `server/src/main/java/com/arcadedb/server/plugin/PluginManager.java:87-117` - use the SPI, drop name matching
- Modify `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java` - own its activation rule
- Create `server/src/test/java/com/arcadedb/server/plugin/PluginAutoDiscoveryTest.java`

**Task 2 - shared JSON producers**
- Create `server/src/main/java/com/arcadedb/server/info/SchemaInfo.java`
- Create `server/src/main/java/com/arcadedb/server/info/ServerInfo.java`
- Create `server/src/main/java/com/arcadedb/server/security/DatabaseUserContext.java`
- Modify `server/src/main/java/com/arcadedb/server/mcp/tools/GetSchemaTool.java`, `ServerStatusTool.java`, `MCPToolUtils.java`
- Modify `server/src/main/java/com/arcadedb/server/mcp/MCPResources.java`
- Modify `server/src/main/java/com/arcadedb/server/ai/ToolDispatcher.java`, `AiChatHandler.java`, `AiAnalyzeProfilerHandler.java`
- Create `server/src/test/java/com/arcadedb/server/info/SchemaInfoTest.java`, `ServerInfoTest.java`
- Create `server/src/test/java/com/arcadedb/server/security/DatabaseUserContextTest.java`

**Task 3 - decouple the server's array-payload regression test**
- Create `server/src/test/java/com/arcadedb/server/http/ArrayPayloadTestPlugin.java`
- Modify `server/src/test/java/com/arcadedb/server/http/HttpJsonArrayPayloadTest.java`

**Task 4 - the module move**
- Create `mcp/pom.xml`, `mcp/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin`
- Create `mcp/src/main/java/com/arcadedb/mcp/MCPPlugin.java`
- Move 29 classes `server/src/main/java/com/arcadedb/server/mcp/**` to `mcp/src/main/java/com/arcadedb/mcp/**`
- Move 13 test classes plus `MCPAuthorizationBindingIT` to `mcp/src/test/java/com/arcadedb/mcp/**`
- Modify root `pom.xml`, `server/.../ArcadeDBServer.java`, `server/.../http/HttpServer.java`

**Task 5 - new-module regression tests**
- Create `mcp/src/test/java/com/arcadedb/mcp/MCPPluginDiscoveryTest.java`

**Task 6 - packaging**
- Modify `package/pom.xml`, `coverage/pom.xml`, `package/arcadedb-builder.sh`, `package/README-BUILDER.md`, `package/src/main/scripts/mcp-stdio.sh`

**Task 7 - OpenAPI**
- Modify `server/src/main/java/com/arcadedb/server/http/handler/openapi/McpApiSpec.java`

**Task 8 - Studio**
- Modify `studio/src/main/resources/static/js/studio-server.js`
- Create `studio/test/mcp-module-absent.test.js`

Tasks 1, 2, and 3 each leave the repo compiling and green on their own and can be reviewed independently. Task 4 is atomic by necessity: there is no smaller unit that leaves the reactor compiling. Tasks 5 through 8 are independent of each other and all depend on Task 4.

---

## Task 1: Plugin activation SPI

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/ServerPlugin.java`
- Modify: `server/src/main/java/com/arcadedb/server/plugin/PluginManager.java:87-117`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`
- Test: `server/src/test/java/com/arcadedb/server/plugin/PluginAutoDiscoveryTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `boolean ServerPlugin.isAutoDiscovered(ContextConfiguration configuration)`, default `false`. Task 4's `MCPPlugin` overrides it to return `true`.

**Background:** `PluginManager.discoverPluginsOnMainClassLoader()` activates a classpath plugin only when `SERVER_PLUGINS` names it, with `"RaftHAPlugin".equals(name)` hardcoded as the single exception. This task replaces the hardcoded name with a method each plugin owns.

- [ ] **Step 1: Write the failing test**

Create `server/src/test/java/com/arcadedb/server/plugin/PluginAutoDiscoveryTest.java`. Copy the Apache 2.0 header from `server/src/test/java/com/arcadedb/server/http/HttpJsonArrayPayloadTest.java` lines 1-18, then:

```java
package com.arcadedb.server.plugin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the plugin activation contract: a plugin found on the classpath is activated when it is named in
 * SERVER_PLUGINS, or when it declares itself auto-discovered. The second route exists so a plugin owns its
 * own activation rule instead of PluginManager matching hardcoded class names.
 */
class PluginAutoDiscoveryTest {

  private static class OptInPlugin implements ServerPlugin {
    @Override
    public boolean isAutoDiscovered(final ContextConfiguration configuration) {
      return true;
    }

    @Override
    public void startService() {
      // NO-OP
    }
  }

  private static class ConfiguredOnlyPlugin implements ServerPlugin {
    @Override
    public void startService() {
      // NO-OP
    }
  }

  @Test
  void aPluginThatDeclaresItselfAutoDiscoveredIsActivatedWithoutConfiguration() {
    assertThat(new OptInPlugin().isAutoDiscovered(new ContextConfiguration())).isTrue();
  }

  @Test
  void theDefaultIsToRequireAnExplicitServerPluginsEntry() {
    assertThat(new ConfiguredOnlyPlugin().isAutoDiscovered(new ContextConfiguration())).isFalse();
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
mvn -q -pl server -am install -DskipTests -Dmaven.javadoc.skip=true
mvn -q -pl server test -Dtest=PluginAutoDiscoveryTest
```

Expected: compilation FAILS with `cannot find symbol: method isAutoDiscovered(ContextConfiguration)`. If it compiles, the method already exists and this task is a no-op - stop and report.

- [ ] **Step 3: Add the SPI method**

In `server/src/main/java/com/arcadedb/server/ServerPlugin.java`, after the existing `isActive()` default method, add:

```java
  /**
   * Whether this plugin activates on classpath presence alone, without an entry in {@code SERVER_PLUGINS}.
   * <p>
   * The default is {@code false}: a plugin is opt-in and a deployment names it explicitly. A plugin that is
   * part of the standard distribution and must keep answering after an upgrade returns {@code true} instead,
   * so excluding it from a custom build is the only thing that removes it. Owning the rule here rather than
   * in the plugin manager is what lets that manager stay ignorant of plugin identities.
   */
  default boolean isAutoDiscovered(final ContextConfiguration configuration) {
    return false;
  }
```

`ContextConfiguration` is already imported in that file.

- [ ] **Step 4: Run the test to verify it passes**

```bash
mvn -q -pl server test -Dtest=PluginAutoDiscoveryTest
```

Expected: PASS, 2 tests.

- [ ] **Step 5: Switch PluginManager to the SPI**

In `server/src/main/java/com/arcadedb/server/plugin/PluginManager.java`, replace the body of `discoverPluginsOnMainClassLoader()` (currently lines 87-112). The existing code reads:

```java
  private void discoverPluginsOnMainClassLoader() {
    final boolean autoDiscoverRaft = isHAEnabled();
    ...
      final boolean isRaftPlugin = autoDiscoverRaft && "RaftHAPlugin".equals(name);

      if (configured || isRaftPlugin) {
        ...
        LogManager.instance().log(this, Level.INFO, "Discovered plugin on main class loader: %s%s",
            name, isRaftPlugin && !configured ? " (auto-discovered for Raft HA)" : "");
      }
```

Replace it with:

```java
  private void discoverPluginsOnMainClassLoader() {
    // Use the thread context class loader so that modules on the classpath (e.g. ha-raft)
    // that are not in the server module's own class loader are still discovered.
    final ClassLoader cl = Thread.currentThread().getContextClassLoader() != null
        ? Thread.currentThread().getContextClassLoader()
        : getClass().getClassLoader();
    final ServiceLoader<ServerPlugin> serviceLoader = ServiceLoader.load(ServerPlugin.class, cl);

    for (final ServerPlugin pluginInstance : serviceLoader) {
      final String name = pluginInstance.getName();
      final boolean configured = configuredPlugins.contains(name)
          || configuredPlugins.contains(pluginInstance.getClass().getSimpleName())
          || configuredPlugins.contains(pluginInstance.getClass().getName());
      final boolean autoDiscovered = pluginInstance.isAutoDiscovered(configuration);

      if (configured || autoDiscovered) {
        final PluginDescriptor descriptor = new PluginDescriptor(name, getClass().getClassLoader());
        descriptor.setPluginInstance(pluginInstance);
        plugins.put(name, descriptor);

        LogManager.instance().log(this, Level.INFO, "Discovered plugin on main class loader: %s%s",
            name, !configured ? " (auto-discovered)" : "");
      }
    }
  }
```

Then delete the now-unused `isHAEnabled()` method that follows it. If the compiler reports `GlobalConfiguration` as an unused import after that deletion, remove the import too.

- [ ] **Step 6: Move Raft's activation rule into RaftHAPlugin**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`, add this method to the class (place it next to the other `ServerPlugin` overrides, near `getInstallationPriority`):

```java
  /**
   * Raft activates on classpath presence whenever high availability is requested, explicitly via
   * {@code ha.enabled} or implicitly via a non-blank {@code ha.serverList}. A deployment that configures HA
   * must not additionally have to name this plugin in {@code SERVER_PLUGINS}.
   */
  @Override
  public boolean isAutoDiscovered(final ContextConfiguration configuration) {
    return configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) || configuration.isHAImplicitlyEnabled();
  }
```

Add `import com.arcadedb.ContextConfiguration;` and `import com.arcadedb.GlobalConfiguration;` if they are not already present.

- [ ] **Step 7: Verify Raft activation did not regress**

```bash
mvn -q -pl ha-raft -am install -DskipTests
mvn -q -pl ha-raft test
```

Expected: PASS. If any test asserts on the old log suffix `" (auto-discovered for Raft HA)"`, update it to `" (auto-discovered)"` - grep first:

```bash
grep -rn "auto-discovered for Raft HA" --include=*.java . | grep -v /target/
```

- [ ] **Step 8: Run the affected server tests**

```bash
mvn -q -pl server test -Dtest='PluginAutoDiscoveryTest,PluginManagerTest,PluginManagerConcurrencyTest'
```

Expected: PASS on all three.

- [ ] **Step 9: Stage and report**

```bash
git add server/src/main/java/com/arcadedb/server/ServerPlugin.java \
        server/src/main/java/com/arcadedb/server/plugin/PluginManager.java \
        server/src/test/java/com/arcadedb/server/plugin/PluginAutoDiscoveryTest.java \
        ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git status --short
```

Report the staged file list and the test results. Do not commit.

---

## Task 2: Extract the shared JSON producers into the server module

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/info/SchemaInfo.java`
- Create: `server/src/main/java/com/arcadedb/server/info/ServerInfo.java`
- Create: `server/src/main/java/com/arcadedb/server/security/DatabaseUserContext.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/GetSchemaTool.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/ServerStatusTool.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/MCPToolUtils.java:110-115`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPResources.java`
- Modify: `server/src/main/java/com/arcadedb/server/ai/ToolDispatcher.java:123-148`
- Modify: `server/src/main/java/com/arcadedb/server/ai/AiChatHandler.java:176-179`
- Modify: `server/src/main/java/com/arcadedb/server/ai/AiAnalyzeProfilerHandler.java:163-169`
- Test: `server/src/test/java/com/arcadedb/server/info/SchemaInfoTest.java`
- Test: `server/src/test/java/com/arcadedb/server/info/ServerInfoTest.java`
- Test: `server/src/test/java/com/arcadedb/server/security/DatabaseUserContextTest.java`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces, all consumed by Task 4's moved MCP sources:
  - `static JSONObject SchemaInfo.toJSON(Database database, String databaseName)`
  - `static JSONObject SchemaInfo.forUser(ArcadeDBServer server, ServerSecurityUser user, String databaseName)`
  - `static JSONObject ServerInfo.toJSON(ArcadeDBServer server, Predicate<String> databaseVisible, boolean includeHA)`
  - `static void DatabaseUserContext.bind(DatabaseInternal database, ServerSecurityUser user)`
  - `static <T> T DatabaseUserContext.runAs(DatabaseInternal database, ServerSecurityUser user, Supplier<T> action)`

**Background:** `AiChatHandler`, `AiAnalyzeProfilerHandler`, and `ToolDispatcher` import `MCPConfiguration`, `GetSchemaTool`, and `ServerStatusTool`. Once MCP moves to a module holding `arcadedb-server` at `provided` scope, the server cannot compile against those types. `ToolDispatcher.effectiveMcpConfig()` already builds a permissive `MCPConfiguration` specifically to neutralize MCP policy, so the AI code wants the JSON producers, not MCP.

There is also a defect to fix here: `MCPToolUtils.bindCurrentUser` binds the authenticated principal onto the thread-local `DatabaseContext` (the fix for GHSA-6x73-v3rc-f57c), `MCPDispatcher` clears it in a `finally`, but the AI handlers call it and never clear. `AiChatHandler` extends `AbstractServerHttpHandler`, not `DatabaseAbstractHandler`, and only the latter cleans the thread-local.

- [ ] **Step 1: Write the failing tests**

Create `server/src/test/java/com/arcadedb/server/info/SchemaInfoTest.java` (Apache 2.0 header first):

```java
package com.arcadedb.server.info;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the schema document shape. Both the MCP get_schema tool and the Studio AI assistant render from this
 * one producer, so a change here changes two consumer contracts at once.
 */
class SchemaInfoTest extends BaseGraphServerTest {

  @Test
  void schemaDocumentCarriesTypesWithCategoryAndProperties() {
    final JSONObject schema = SchemaInfo.toJSON(getServer(0).getDatabase(getDatabaseName()), getDatabaseName());

    assertThat(schema.getString("database")).isEqualTo(getDatabaseName());

    final JSONArray types = schema.getJSONArray("types");
    assertThat(types.length()).isGreaterThan(0);

    JSONObject vertexType = null;
    for (int i = 0; i < types.length(); i++) {
      final JSONObject type = types.getJSONObject(i);
      if (VERTEX1_TYPE_NAME.equals(type.getString("name", null)))
        vertexType = type;
    }

    assertThat(vertexType).isNotNull();
    assertThat(vertexType.getString("category")).isEqualTo("vertex");
    assertThat(vertexType.has("properties")).isTrue();
  }

  @Test
  void resolvingForAUserRejectsAnUnknownDatabase() {
    assertThatThrownBy(() -> SchemaInfo.forUser(getServer(0), rootUser(), "doesNotExist"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("doesNotExist");
  }
}
```

Add the imports the second test needs: `import static org.assertj.core.api.Assertions.assertThatThrownBy;` and `import com.arcadedb.server.security.ServerSecurityUser;`. Add this helper to the class:

```java
  private ServerSecurityUser rootUser() {
    return getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }
```

`VERTEX1_TYPE_NAME` is a `protected static final String` on `BaseGraphServerTest` (value `"V1"`), inherited by the test, so it needs no import.

Create `server/src/test/java/com/arcadedb/server/info/ServerInfoTest.java`:

```java
package com.arcadedb.server.info;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The visibility predicate is the only thing that decides which databases appear, and the HA block is opt-in.
 * Both are what let one producer serve the MCP server_status tool and the Studio AI assistant, which apply
 * different policies over the same data.
 */
class ServerInfoTest extends BaseGraphServerTest {

  @Test
  void versionNameAndLanguagesAreAlwaysPresent() {
    final JSONObject info = ServerInfo.toJSON(getServer(0), db -> true, false);

    assertThat(info.getString("version")).isNotEmpty();
    assertThat(info.getString("serverName")).isNotEmpty();
    assertThat(info.has("languages")).isTrue();
  }

  @Test
  void theVisibilityPredicateFiltersTheDatabaseList() {
    final JSONObject visible = ServerInfo.toJSON(getServer(0), db -> true, false);
    final JSONObject hidden = ServerInfo.toJSON(getServer(0), db -> false, false);

    assertThat(visible.getJSONArray("databases").length()).isGreaterThan(0);
    assertThat(hidden.getJSONArray("databases").length()).isZero();
  }

  @Test
  void theHaBlockIsOmittedWhenNotRequested() {
    assertThat(ServerInfo.toJSON(getServer(0), db -> true, false).has("ha")).isFalse();
  }
}
```

Create `server/src/test/java/com/arcadedb/server/security/DatabaseUserContextTest.java`:

```java
package com.arcadedb.server.security;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The principal binding that makes the engine's per-user permission gates enforce (GHSA-6x73-v3rc-f57c) runs on
 * pooled worker threads. A bind that is not undone leaks the principal onto whatever request the pool hands that
 * thread next, so runAs must restore the previous binding on every exit path, including a thrown exception.
 */
class DatabaseUserContextTest extends BaseGraphServerTest {

  @Test
  void runAsRestoresThePreviousBindingOnSuccess() {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);

    final String seen = DatabaseUserContext.runAs(database, user, () -> {
      final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(
          database.getDatabasePath());
      return context.getCurrentUser() == null ? null : "bound";
    });

    assertThat(seen).isEqualTo("bound");
    assertThat(currentUserOf(database)).isNull();
  }

  @Test
  void runAsRestoresThePreviousBindingWhenTheActionThrows() {
    final DatabaseInternal database = (DatabaseInternal) getServer(0).getDatabase(getDatabaseName());
    final ServerSecurityUser user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);

    try {
      DatabaseUserContext.runAs(database, user, () -> {
        throw new IllegalStateException("boom");
      });
    } catch (final IllegalStateException expected) {
      // EXPECTED
    }

    assertThat(currentUserOf(database)).isNull();
  }

  private SecurityDatabaseUser currentUserOf(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(
        database.getDatabasePath());
    return context == null ? null : context.getCurrentUser();
  }
}
```

`SecurityDatabaseUser` lives in `com.arcadedb.security`; add the import the compiler asks for.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
mvn -q -pl server test -Dtest='SchemaInfoTest,ServerInfoTest,DatabaseUserContextTest'
```

Expected: compilation FAILS - `package com.arcadedb.server.info does not exist`, `cannot find symbol: class DatabaseUserContext`.

- [ ] **Step 3: Create SchemaInfo**

Create `server/src/main/java/com/arcadedb/server/info/SchemaInfo.java` with the Apache 2.0 header, then:

```java
package com.arcadedb.server.info;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.DatabaseUserContext;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.Set;
import java.util.TreeSet;

/**
 * Renders a database schema as JSON. The single source of truth for schema shaping: the MCP get_schema tool,
 * the arcadedb://{database}/schema MCP resource, and the Studio AI assistant all render from here, so their
 * content cannot drift apart.
 */
public class SchemaInfo {

  private SchemaInfo() {
  }

  /**
   * Builds the JSON schema document for a database. Performs no permission or authorization check; the caller
   * is responsible for both.
   */
  public static JSONObject toJSON(final Database database, final String databaseName) {
    // MOVE THE ENTIRE BODY OF GetSchemaTool.buildSchema HERE VERBATIM.
  }

  /**
   * Resolves a database on behalf of an authenticated user and renders its schema with the principal bound for
   * the duration of the read, so the engine's per-user gates enforce and nothing is left bound on the calling
   * thread afterwards.
   */
  public static JSONObject forUser(final ArcadeDBServer server, final ServerSecurityUser user,
      final String databaseName) {
    if (!server.existsDatabase(databaseName)) {
      final Set<String> installed = new TreeSet<>(server.getDatabaseNames());
      installed.removeIf(db -> !user.canAccessToDatabase(db));
      throw new IllegalArgumentException(
          "Database '" + databaseName + "' does not exist. Available databases: " + installed);
    }
    if (!user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not authorized to access database '"
          + databaseName + "'");

    final ServerDatabase database = server.getDatabase(databaseName);
    return DatabaseUserContext.runAs((DatabaseInternal) database, user, () -> toJSON(database, databaseName));
  }
}
```

For `toJSON`, copy the body of `GetSchemaTool.buildSchema` (currently `server/src/main/java/com/arcadedb/server/mcp/tools/GetSchemaTool.java:67-136`) exactly - the whole loop over `schema.getTypes()`, the category branch, parent types, properties, indexes, and the final `result` assembly. Do not retype it from memory; copy the text.

- [ ] **Step 4: Create ServerInfo**

Create `server/src/main/java/com/arcadedb/server/info/ServerInfo.java` with the header, then:

```java
package com.arcadedb.server.info;

import com.arcadedb.Constants;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;

import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Renders server-level information as JSON: version, name, available query languages, the databases a caller
 * may see, and optionally the high-availability block.
 * <p>
 * Which databases are visible and whether the HA block appears are both the caller's decision, passed in
 * rather than derived here. That is what lets one producer serve callers with different policies over the same
 * data without either of them inheriting the other's rules.
 */
public class ServerInfo {

  private ServerInfo() {
  }

  public static JSONObject toJSON(final ArcadeDBServer server, final Predicate<String> databaseVisible,
      final boolean includeHA) {
    final JSONObject result = new JSONObject();
    result.put("version", Constants.getVersion());
    result.put("serverName", server.getServerName());
    result.put("languages", QueryEngineManager.getInstance().getAvailableLanguages());

    final Set<String> installedDatabases = new TreeSet<>(server.getDatabaseNames());
    installedDatabases.removeIf(databaseName -> !databaseVisible.test(databaseName));
    result.put("databases", new JSONArray(installedDatabases));

    final HAServerPlugin ha = server.getHA();
    if (ha != null && includeHA) {
      final JSONObject haInfo = new JSONObject();
      haInfo.put("clusterName", ha.getClusterName());
      haInfo.put("leader", ha.getLeaderName());
      haInfo.put("electionStatus", ha.getElectionStatus().toString());
      result.put("ha", haInfo);
    }

    return result;
  }
}
```

- [ ] **Step 5: Create DatabaseUserContext**

Create `server/src/main/java/com/arcadedb/server/security/DatabaseUserContext.java` with the header, then:

```java
package com.arcadedb.server.security;

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.function.Supplier;

/**
 * Binds an authenticated principal onto the current thread's {@link DatabaseContext} so the engine's per-user
 * permission gates ({@code LocalDatabase.checkPermissionsOnDatabase} / {@code checkPermissionsOnFile}) actually
 * enforce. Those gates are deliberately no-ops when no user is bound, which is the mechanism embedded and
 * HA-apply contexts use to skip checks, so a transport that fails to bind silently grants every caller
 * unrestricted access (GHSA-6x73-v3rc-f57c).
 * <p>
 * Every transport here runs on pooled worker threads, so a binding that is never undone leaks the principal
 * onto the next request the pool hands that thread. {@link #runAs} is the safe form: it restores whatever was
 * bound before, on every exit path.
 */
public class DatabaseUserContext {

  private DatabaseUserContext() {
  }

  /**
   * Binds the principal without restoring anything. The caller takes responsibility for clearing the thread's
   * contexts afterwards; prefer {@link #runAs} where the scope of the binding is a single call.
   */
  public static void bind(final DatabaseInternal database, final ServerSecurityUser user) {
    contextFor(database).setCurrentUser(user.getDatabaseUser(database));
  }

  /**
   * Runs an action with the principal bound, restoring the previous binding before returning or propagating.
   */
  public static <T> T runAs(final DatabaseInternal database, final ServerSecurityUser user,
      final Supplier<T> action) {
    final DatabaseContext.DatabaseContextTL context = contextFor(database);
    final SecurityDatabaseUser previous = context.getCurrentUser();
    context.setCurrentUser(user.getDatabaseUser(database));
    try {
      return action.get();
    } finally {
      context.setCurrentUser(previous);
    }
  }

  private static DatabaseContext.DatabaseContextTL contextFor(final DatabaseInternal database) {
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(
        database.getDatabasePath());
    return context != null ? context : DatabaseContext.INSTANCE.init(database);
  }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
mvn -q -pl server test -Dtest='SchemaInfoTest,ServerInfoTest,DatabaseUserContextTest'
```

Expected: PASS, 7 tests total.

- [ ] **Step 7: Point the MCP tools at the new producers**

In `server/src/main/java/com/arcadedb/server/mcp/tools/GetSchemaTool.java`, delete the whole `buildSchema` method and replace the last line of `execute` so the class reads:

```java
  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = args.getString("database");

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);

    return SchemaInfo.toJSON(access.database(), databaseName);
  }
```

Add `import com.arcadedb.server.info.SchemaInfo;` and remove now-unused schema imports (`Schema`, `DocumentType`, `VertexType`, `EdgeType`, `Property`, `TypeIndex`, `JSONArray`, `Database`) - let the compiler tell you which.

In `server/src/main/java/com/arcadedb/server/mcp/MCPResources.java`, replace every `GetSchemaTool.buildSchema(` call with `SchemaInfo.toJSON(` and fix imports. Find them with:

```bash
grep -n "buildSchema" server/src/main/java/com/arcadedb/server/mcp/MCPResources.java
```

In `server/src/main/java/com/arcadedb/server/mcp/tools/ServerStatusTool.java`, replace the body of `execute` with:

```java
  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    return ServerInfo.toJSON(server, databaseName -> MCPToolUtils.canReadDatabase(user, config, databaseName),
        config.isAllowAdmin());
  }
```

Add `import com.arcadedb.server.info.ServerInfo;` and remove the imports that go unused (`Constants`, `QueryEngineManager`, `HAServerPlugin`, `Set`, `TreeSet`).

In `server/src/main/java/com/arcadedb/server/mcp/tools/MCPToolUtils.java`, make `bindCurrentUser` delegate so the GHSA fix has one implementation:

```java
  public static void bindCurrentUser(final DatabaseInternal database, final ServerSecurityUser user) {
    DatabaseUserContext.bind(database, user);
  }
```

Keep the existing Javadoc on that method. Add `import com.arcadedb.server.security.DatabaseUserContext;` and remove `DatabaseContext` from the imports if nothing else in the file uses it.

- [ ] **Step 8: Cut the AI feature's MCP imports**

In `server/src/main/java/com/arcadedb/server/ai/ToolDispatcher.java`, replace the two methods and delete `effectiveMcpConfig`:

```java
  private String executeGetSchema(final JSONObject args) {
    final String databaseName = args.getString("database", defaultDatabase);
    if (databaseName == null || databaseName.isEmpty())
      return errorJson("get_schema requires a 'database' argument");

    return SchemaInfo.forUser(server, user, databaseName).toString();
  }

  private String executeServerInfo() {
    return ServerInfo.toJSON(server, user::canAccessToDatabase, false).toString();
  }
```

Delete the `effectiveMcpConfig()` method together with the four-line comment above it. Remove the three MCP imports at lines 27-29 and add:

```java
import com.arcadedb.server.info.SchemaInfo;
import com.arcadedb.server.info.ServerInfo;
```

In `server/src/main/java/com/arcadedb/server/ai/AiChatHandler.java`, replace lines 176-179:

```java
        final JSONObject schema = SchemaInfo.forUser(server, user, database);
        final JSONObject serverInfo = ServerInfo.toJSON(server, user::canAccessToDatabase, false);
        serverInfo.put("metrics", Profiler.INSTANCE.toJSON());
```

Delete the now-unused `schemaArgs` local on the line above. Swap imports 30-32 for the two `com.arcadedb.server.info` imports.

In `server/src/main/java/com/arcadedb/server/ai/AiAnalyzeProfilerHandler.java`, replace lines 163-169:

```java
    for (final String dbName : dbNames) {
      if (!user.canAccessToDatabase(dbName))
        continue;
      try {
        schemas.put(dbName, SchemaInfo.forUser(server, user, dbName));
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Could not fetch schema for database '%s': %s", dbName, e.getMessage());
      }
    }
```

Swap imports 29-30 for `import com.arcadedb.server.info.SchemaInfo;`.

- [ ] **Step 9: Verify the AI package no longer names MCP**

```bash
grep -rn "mcp" server/src/main/java/com/arcadedb/server/ai/ ; echo "EXIT=$?"
```

Expected: no output and `EXIT=1`. Any hit here means an import or reference was missed.

- [ ] **Step 10: Run the full affected test set**

```bash
mvn -q -pl server -am install -DskipTests
mvn -q -pl server test -Dtest='SchemaInfoTest,ServerInfoTest,DatabaseUserContextTest,MCPServerPluginTest,MCPResourcesTest,MCPToolUtilsTest,MCPTransportConformanceTest,MCPDatabaseScopingTest'
```

Expected: PASS on all. `MCPServerPluginTest` and `MCPResourcesTest` are the ones that would catch a schema-shape or status-shape regression from the extraction.

- [ ] **Step 11: Stage and report**

```bash
git add server/src/main/java/com/arcadedb/server/info/ \
        server/src/main/java/com/arcadedb/server/security/DatabaseUserContext.java \
        server/src/main/java/com/arcadedb/server/mcp/ \
        server/src/main/java/com/arcadedb/server/ai/ \
        server/src/test/java/com/arcadedb/server/info/ \
        server/src/test/java/com/arcadedb/server/security/DatabaseUserContextTest.java
git status --short
```

Report which tests ran and their counts. Do not commit.

---

## Task 3: Decouple the array-payload regression test from MCP

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/http/ArrayPayloadTestPlugin.java`
- Modify: `server/src/test/java/com/arcadedb/server/http/HttpJsonArrayPayloadTest.java`

**Interfaces:**
- Consumes: `ServerPlugin.isAutoDiscovered` from Task 1 (the test plugin relies on the default `false` so it activates only when named in `SERVER_PLUGINS`).
- Produces: nothing consumed by later tasks.

**Background:** `HttpJsonArrayPayloadTest` is the regression test for issue #5415, covering `AbstractServerHttpHandler`'s top-level-JSON-array parsing. Its own comment says MCP is used only because it "is the in-tree handler that opts in to an array body." Four of its seven tests go through `/api/v1/mcp`. After Task 4 the server module has no array-accepting handler, so those four cannot stay as written, and moving the whole file to the mcp module would relocate a server-pipeline guard out of the server module.

The fix is a test-only handler in server test sources that opts in via `acceptsArrayPayload()`, which makes the test exercise the mechanism directly instead of borrowing MCP's.

- [ ] **Step 1: Write the test-only plugin and handler**

Create `server/src/test/java/com/arcadedb/server/http/ArrayPayloadTestPlugin.java` with the Apache 2.0 header, then:

```java
package com.arcadedb.server.http;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;

/**
 * Registers /api/v1/test/array, the only route in the server module that opts in to a top-level JSON array
 * body. It exists so the issue #5415 pipeline contract - array bodies parsed once and delivered through
 * getPayloadAsArray, object bodies unaffected, every other route answering 400 - is pinned against the
 * mechanism itself rather than against whichever production handler happens to accept arrays today.
 * <p>
 * Not auto-discovered: the route appears only for a test that names this plugin in SERVER_PLUGINS.
 */
public class ArrayPayloadTestPlugin implements ServerPlugin {

  public static class Handler extends AbstractServerHttpHandler {
    public Handler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected boolean acceptsArrayPayload() {
      return true;
    }

    @Override
    protected boolean requiresDatabase() {
      return false;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      final JSONArray array = getPayloadAsArray(exchange);
      if (array != null)
        return new ExecutionResponse(200, new JSONObject().put("shape", "array").put("size", array.length()).toString());
      return new ExecutionResponse(200, new JSONObject().put("shape", "object")
          .put("id", payload == null ? -1 : payload.getInt("id", -1)).toString());
    }
  }

  @Override
  public void startService() {
    // NO-OP
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    routes.addExactPath("/api/v1/test/array", new Handler(httpServer));
  }
}
```

Before running, confirm the exact `execute` signature and the `AbstractServerHttpHandler` constructor arity:

```bash
grep -n "protected.*ExecutionResponse execute\|public AbstractServerHttpHandler\|protected boolean requiresDatabase" server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java
```

Match whatever is declared there; do not assume.

- [ ] **Step 2: Rewrite the four MCP-dependent tests against the new route**

In `server/src/test/java/com/arcadedb/server/http/HttpJsonArrayPayloadTest.java`:

Delete the `import com.arcadedb.server.mcp.MCPConfiguration;` and `import java.util.List;` lines, delete the `@BeforeEach enableMCP()` method, and add this override so the plugin is loaded:

```java
  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, ArrayPayloadTestPlugin.class.getName());
  }
```

with `import com.arcadedb.ContextConfiguration;` and `import com.arcadedb.GlobalConfiguration;`.

Replace the four MCP tests with these, keeping the three `/api/v1/command/graph` tests exactly as they are:

```java
  @Test
  void arrayBodyReachesAHandlerThatAcceptsIt() throws Exception {
    final JSONArray batch = new JSONArray()
        .put(new JSONObject().put("id", 1))
        .put(new JSONObject().put("id", 2));

    final Response response = post("/api/v1/test/array", batch.toString());

    assertThat(response.status()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("shape")).isEqualTo("array");
    assertThat(json.getInt("size")).isEqualTo(2);
  }

  @Test
  void leadingWhitespaceDoesNotHideTheArray() throws Exception {
    final Response response = post("/api/v1/test/array", "\n  [{\"id\":3}]");

    assertThat(response.status()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("shape")).isEqualTo("array");
    assertThat(json.getInt("size")).isEqualTo(1);
  }

  @Test
  void objectBodyIsUnaffectedOnAHandlerThatAcceptsArrays() throws Exception {
    final Response response = post("/api/v1/test/array", new JSONObject().put("id", 4).toString());

    assertThat(response.status()).isEqualTo(200);
    final JSONObject json = new JSONObject(response.body());
    assertThat(json.getString("shape")).isEqualTo("object");
    assertThat(json.getInt("id")).isEqualTo(4);
  }
```

The fourth MCP test, `malformedArrayBodyIsReportedAsAParseError`, asserts a JSON-RPC `-32700` code, which is MCP protocol behavior rather than server pipeline behavior. Delete it here; Step 3 confirms MCP keeps that coverage.

- [ ] **Step 3: Confirm MCP retains its own batch and parse-error coverage**

```bash
grep -n "32700\|batch\|JSONArray" server/src/test/java/com/arcadedb/server/mcp/MCPTransportConformanceTest.java
```

Expected: hits showing both a JSON-RPC batch test and a `-32700` parse-error test. **If either is missing, add it to `MCPTransportConformanceTest` before proceeding** - porting the assertion body from the test you just deleted - so the coverage moves rather than disappears. Report which case this was.

- [ ] **Step 4: Run the test to verify it passes**

```bash
mvn -q -pl server test -Dtest=HttpJsonArrayPayloadTest
```

Expected: PASS, 6 tests.

- [ ] **Step 5: Prove the test can still fail**

Temporarily change `ArrayPayloadTestPlugin.Handler.acceptsArrayPayload()` to return `false`, then rerun:

```bash
mvn -q -pl server test -Dtest=HttpJsonArrayPayloadTest
```

Expected: the three array tests FAIL with HTTP 400. Revert `acceptsArrayPayload()` to `true` and rerun to confirm green. A test that passes either way is testing nothing.

- [ ] **Step 6: Verify no MCP reference remains**

```bash
grep -rn "mcp" server/src/test/java/com/arcadedb/server/http/HttpJsonArrayPayloadTest.java ; echo "EXIT=$?"
```

Expected: no output, `EXIT=1`.

- [ ] **Step 7: Stage and report**

```bash
git add server/src/test/java/com/arcadedb/server/http/
git status --short
```

Report the Step 3 finding (whether MCP coverage already existed or had to be added) and the Step 5 result. Do not commit.

---

## Task 4: Create the `arcadedb-mcp` module and move MCP into it

**Files:**
- Create: `mcp/pom.xml`
- Create: `mcp/src/main/java/com/arcadedb/mcp/MCPPlugin.java`
- Create: `mcp/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin`
- Move: 29 files from `server/src/main/java/com/arcadedb/server/mcp/` to `mcp/src/main/java/com/arcadedb/mcp/`
- Move: 12 files from `server/src/test/java/com/arcadedb/server/mcp/` plus `tools/HybridSearchSeedsTest.java` to `mcp/src/test/java/com/arcadedb/mcp/`
- Move: `server/src/test/java/com/arcadedb/server/security/MCPAuthorizationBindingIT.java` to `mcp/src/test/java/com/arcadedb/mcp/`
- Modify: `pom.xml` (root, module list at line 139)
- Modify: `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:43,132,308-312,917-919`
- Modify: `server/src/main/java/com/arcadedb/server/http/HttpServer.java:81-82,266-269`

**Interfaces:**
- Consumes: `ServerPlugin.isAutoDiscovered` (Task 1); `SchemaInfo`, `ServerInfo`, `DatabaseUserContext` (Task 2); a server module with no MCP-dependent tests (Task 3).
- Produces: `com.arcadedb.mcp.MCPPlugin` with `static MCPPlugin of(ArcadeDBServer server)` and `MCPConfiguration getConfiguration()`, used by Task 5's tests and by `MCPStdioServer`.

**This task is atomic.** There is no intermediate state where the reactor compiles: the moment MCP sources leave the server module, `ArcadeDBServer` and `HttpServer` stop compiling until their references are removed, and the moved tests stop compiling until the module exists. Do the whole task, then compile.

- [ ] **Step 1: Create the module pom**

Create `mcp/pom.xml`. Copy the header comment block from `bolt/pom.xml` lines 1-19 verbatim, then:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.arcadedb</groupId>
        <artifactId>arcadedb-parent</artifactId>
        <version>26.8.1-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>arcadedb-mcp</artifactId>
    <packaging>jar</packaging>
    <name>ArcadeDB MCP Server</name>
    <description>Model Context Protocol server for ArcadeDB, over HTTP and stdio</description>

    <dependencies>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-server</artifactId>
            <version>${project.parent.version}</version>
            <scope>test</scope>
            <type>test-jar</type>
        </dependency>
    </dependencies>

</project>
```

No `maven-shade-plugin` block: MCP brings no third-party dependencies, so there is nothing to shade.

Verify the parent version matches the repo before proceeding:

```bash
grep -m1 -A2 "<artifactId>arcadedb-parent</artifactId>" bolt/pom.xml
```

- [ ] **Step 2: Register the module in the reactor**

In the root `pom.xml`, add `<module>mcp</module>` on the line immediately after `<module>server</module>` (currently line 139).

- [ ] **Step 3: Move the main sources with git mv**

```bash
mkdir -p mcp/src/main/java/com/arcadedb
git mv server/src/main/java/com/arcadedb/server/mcp mcp/src/main/java/com/arcadedb/mcp
```

Then rewrite the package and import lines across the whole repo:

```bash
grep -rlI "com\.arcadedb\.server\.mcp" --include="*.java" mcp server console integration package \
  | xargs sed -i '' 's/com\.arcadedb\.server\.mcp/com.arcadedb.mcp/g'
```

Verify nothing was missed, and that the scan actually matched something (a scan that silently matches nothing looks identical to success):

```bash
grep -rn "com\.arcadedb\.server\.mcp" --include="*.java" . | grep -v /target/ | grep -v /.worktrees/ | grep -v /.claude/
echo "EXIT=$?"
grep -rc "package com.arcadedb.mcp" mcp/src/main/java/com/arcadedb/mcp/MCPDispatcher.java
```

Expected: first grep prints nothing with `EXIT=1`; second prints `1`. If the second prints `0`, the sed did not apply and the first grep's silence is meaningless.

- [ ] **Step 4: Move the test sources**

```bash
mkdir -p mcp/src/test/java/com/arcadedb
git mv server/src/test/java/com/arcadedb/server/mcp mcp/src/test/java/com/arcadedb/mcp
git mv server/src/test/java/com/arcadedb/server/security/MCPAuthorizationBindingIT.java \
       mcp/src/test/java/com/arcadedb/mcp/MCPAuthorizationBindingIT.java
```

Fix the moved IT's package declaration (it still says `com.arcadedb.server.security`):

```bash
sed -i '' 's/^package com\.arcadedb\.server\.security;/package com.arcadedb.mcp;/' \
  mcp/src/test/java/com/arcadedb/mcp/MCPAuthorizationBindingIT.java
```

It will now need explicit imports for anything it used from `com.arcadedb.server.security` unqualified. The compiler in Step 9 will name them; add each one.

- [ ] **Step 5: Write MCPPlugin**

Create `mcp/src/main/java/com/arcadedb/mcp/MCPPlugin.java` with the Apache 2.0 header, then:

```java
package com.arcadedb.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;

import java.util.logging.Level;

/**
 * Installs the Model Context Protocol server: owns its configuration and registers the HTTP transport.
 * <p>
 * Auto-discovered, so a standard distribution keeps answering on /api/v1/mcp with no SERVER_PLUGINS entry and
 * no migration. What makes MCP optional is building a distribution without this module, not configuration.
 * <p>
 * Runs at BEFORE_HTTP_ON, and that matters: the plugin manager starts those plugins before
 * {@code HttpServer.startService()} assembles the routes, so {@link #configure} has already loaded the
 * configuration by the time {@link #registerAPI} hands it to the handlers. An AFTER_HTTP_ON plugin is called
 * the other way round.
 */
public class MCPPlugin implements ServerPlugin {

  private MCPConfiguration configuration;

  /**
   * Returns the installed plugin, or null when this server was built without the MCP module.
   */
  public static MCPPlugin of(final ArcadeDBServer server) {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof MCPPlugin mcpPlugin)
        return mcpPlugin;
    return null;
  }

  public MCPConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public boolean isAutoDiscovered(final ContextConfiguration contextConfiguration) {
    return true;
  }

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration contextConfiguration) {
    configuration = new MCPConfiguration(arcadeDBServer.getRootPath());
    configuration.load();
    configuration.warnUnknownDatabaseOverrides(arcadeDBServer.getDatabaseNames());
    this.server = arcadeDBServer;
  }

  @Override
  public void startService() {
    // NO-OP: the HTTP transport is installed by registerAPI, and the stdio transport is a separate process.
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    // MCP routes are always registered; the handler checks isEnabled() at request time to support runtime toggling
    routes.addExactPath("/api/v1/mcp", new MCPHttpHandler(httpServer, server, configuration));
    routes.addExactPath("/api/v1/mcp/config", new MCPConfigHandler(httpServer, configuration));

    LogManager.instance().log(this, Level.INFO, "MCP server endpoint registered at /api/v1/mcp");
  }
}
```

Add the field declaration `private ArcadeDBServer server;` above `configuration` (anonymous-class and field ordering convention: fields at the top).

- [ ] **Step 6: Declare the service**

Create `mcp/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin` containing exactly one line, no header comment (service files carry none - check `metrics/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin` to confirm):

```
com.arcadedb.mcp.MCPPlugin
```

- [ ] **Step 7: Remove MCP from ArcadeDBServer**

In `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`:
- Delete the import on line 43 (`import com.arcadedb.mcp.MCPConfiguration;` after the sed in Step 3).
- Delete the field on line 132 (`private MCPConfiguration mcpConfiguration;`).
- Delete lines 308-312, the comment `// INITIALIZE MCP CONFIGURATION (always available, disabled by default)` and the three statements below it.
- Delete the `getMCPConfiguration()` accessor at lines 917-919.

- [ ] **Step 8: Remove MCP from HttpServer**

In `server/src/main/java/com/arcadedb/server/http/HttpServer.java`:
- Delete the two imports on lines 81-82.
- Delete lines 266-269: the `// MCP routes are always registered...` comment, the `final var mcpConfig = ...` line, and the two `routes.addExactPath` calls.

- [ ] **Step 9: Point MCPStdioServer at the plugin**

In `mcp/src/main/java/com/arcadedb/mcp/MCPStdioServer.java`, replace `final MCPConfiguration config = server.getMCPConfiguration();` with:

```java
      final MCPPlugin plugin = MCPPlugin.of(server);
      if (plugin == null) {
        System.err.println("ERROR: the MCP plugin is not installed on this server");
        System.exit(1);
      }
      final MCPConfiguration config = plugin.getConfiguration();
```

Keep the `config.setEnabled(true);` line that follows.

- [ ] **Step 10: Point the moved tests at the plugin**

```bash
grep -rn "getMCPConfiguration" mcp/src/test/java/
```

Replace every `getServer(N).getMCPConfiguration()` with `MCPPlugin.of(getServer(N)).getConfiguration()`, adding `import com.arcadedb.mcp.MCPPlugin;` where the test is not already in that package. Expect 8 call sites across `MCPTransportConformanceTest`, `MCPResourcesTest`, `MCPStdioServerTest`, `MCPDatabaseScopingTest`, and `MCPAuthorizationBindingIT`.

- [ ] **Step 11: Compile the reactor**

```bash
mvn -q -pl server -am install -DskipTests
mvn -q -pl mcp -am install -DskipTests
```

Expected: BUILD SUCCESS on both. Fix compile errors as they appear - the common ones are missing imports in `MCPAuthorizationBindingIT` after its package change, and classes in the moved test tree that referenced package-private server-test helpers.

- [ ] **Step 12: Verify the dependency direction is one-way**

```bash
grep -rn "com\.arcadedb\.mcp" server/src/main server/src/test ; echo "EXIT=$?"
```

Expected: no output, `EXIT=1`. This is the invariant the whole task exists to establish. The server module having just compiled without `arcadedb-mcp` on its classpath is the stronger proof; this grep catches a stray comment or string.

- [ ] **Step 13: Run the moved test suite**

```bash
mvn -q -pl mcp -am install
```

Expected: all MCP unit tests PASS. Then the ITs:

```bash
mvn -q -pl mcp -am install -DskipITs=false -Dit.test='MCPAuthorizationBindingIT,GetServerSettingsToolRedactionIT'
```

If port 2480 is held by a Homebrew ArcadeDB service, stop it first (`brew services stop arcadedb`) or report these two ITs as unverified.

- [ ] **Step 14: Run the server module's tests**

```bash
mvn -q -pl server -am install
```

Expected: PASS. The server module's test count drops by the moved classes; that is expected, not a regression.

- [ ] **Step 15: Stage and report**

```bash
git add mcp/ pom.xml server/src/main/java/com/arcadedb/server/ArcadeDBServer.java \
        server/src/main/java/com/arcadedb/server/http/HttpServer.java
git add -A server/src/main/java/com/arcadedb/server/mcp server/src/test/java/com/arcadedb/server/mcp 2>/dev/null
git status --short
```

Report: the compile result, the Step 12 grep outcome, the mcp test count, the server test count, and any IT left unverified with the reason. Do not commit.

---

## Task 5: Auto-discovery regression test in the new module

**Files:**
- Create: `mcp/src/test/java/com/arcadedb/mcp/MCPPluginDiscoveryTest.java`

**Interfaces:**
- Consumes: `MCPPlugin.of(ArcadeDBServer)` and `MCPPlugin.getConfiguration()` from Task 4.
- Produces: nothing.

**Background:** Decision 4 of the spec is that MCP activates on classpath presence, so that upgrading users keep the endpoint without adding a `SERVER_PLUGINS` entry. Nothing else in the suite asserts that; every other MCP test would pass just as well if the plugin required explicit configuration, because they all reach the config through `MCPPlugin.of(...)` after the server has started however it started.

- [ ] **Step 1: Write the failing test**

Create `mcp/src/test/java/com/arcadedb/mcp/MCPPluginDiscoveryTest.java` with the Apache 2.0 header, then:

```java
package com.arcadedb.mcp;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The MCP endpoint must keep answering on a server that names no plugin at all, which is what an existing
 * installation looks like after upgrading to a distribution where MCP is a separate module. Excluding the
 * module from the build is the only thing that removes the endpoint.
 */
class MCPPluginDiscoveryTest extends BaseGraphServerTest {

  @Test
  void theMcpPluginIsInstalledWithoutAServerPluginsEntry() {
    assertThat(MCPPlugin.of(getServer(0))).isNotNull();
  }

  @Test
  void theConfigurationIsLoadedByTheTimeTheServerIsUp() {
    assertThat(MCPPlugin.of(getServer(0)).getConfiguration()).isNotNull();
  }

  @Test
  void theConfigRouteAnswersOnADefaultServer() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(
            new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/mcp/config"))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .GET()
        .build();

    final HttpResponse<String> response = HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(response.body()).has("enabled")).isTrue();
  }
}
```

- [ ] **Step 2: Run the test**

```bash
mvn -q -pl mcp -am install -DskipTests
mvn -q -pl mcp test -Dtest=MCPPluginDiscoveryTest
```

Expected: PASS, 3 tests.

- [ ] **Step 3: Prove the test can fail**

Temporarily change `MCPPlugin.isAutoDiscovered` to return `false`, then rerun:

```bash
mvn -q -pl mcp -am install -DskipTests
mvn -q -pl mcp test -Dtest=MCPPluginDiscoveryTest
```

Expected: all 3 FAIL - the first two on a null plugin, the third on a 404. Revert to `true`, rebuild, and confirm green. If any test still passes with `false`, it is not testing discovery and must be rewritten before moving on.

- [ ] **Step 4: Stage and report**

```bash
git add mcp/src/test/java/com/arcadedb/mcp/MCPPluginDiscoveryTest.java
git status --short
```

Report the Step 3 result explicitly: how many tests failed with `isAutoDiscovered` returning `false`. Do not commit.

---

## Task 6: Packaging

**Files:**
- Modify: `package/pom.xml` (after the `arcadedb-bolt` dependency block, currently lines 210-221)
- Modify: `coverage/pom.xml` (after the `arcadedb-bolt` dependency block, lines 54-58)
- Modify: `package/arcadedb-builder.sh:30,40-48,84,100-109`
- Modify: `package/README-BUILDER.md:64-73`
- Modify: `package/src/main/scripts/mcp-stdio.sh:76`

**Interfaces:**
- Consumes: the `arcadedb-mcp` artifact from Task 4.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the module to the standard distribution**

In `package/pom.xml`, insert this dependency immediately after the closing `</dependency>` of the `arcadedb-bolt` block. Note there is **no** `<classifier>shaded</classifier>`: MCP is not shaded, so it ships as the plain jar.

```xml
        <dependency>
            <groupId>com.arcadedb</groupId>
            <artifactId>arcadedb-mcp</artifactId>
            <version>${project.parent.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>*</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

- [ ] **Step 2: Add the module to coverage aggregation**

In `coverage/pom.xml`, insert after the `arcadedb-bolt` block:

```xml
        <dependency>
            <groupId>${project.groupId}</groupId>
            <artifactId>arcadedb-mcp</artifactId>
            <version>${project.version}</version>
        </dependency>
```

- [ ] **Step 3: Add `mcp` to the modular builder**

In `package/arcadedb-builder.sh`:

Line 30, add `mcp` to the regular (non-shaded) module list:

```bash
REGULAR_MODULES="console studio graphql mcp"
```

In `get_module_description()`, add a case before the `*)` fallback:

```bash
  mcp) echo "Model Context Protocol server for LLM clients" ;;
```

Line 84, extend the `--modules=` options list:

```
                           Options: console,gremlin,studio,redisw,mongodbw,postgresw,grpcw,graphql,metrics,mcp
```

In the `OPTIONAL MODULES:` help block (around line 100-109), add after the `metrics` line:

```
    mcp          Model Context Protocol server for LLM clients
```

- [ ] **Step 4: Document the module**

In `package/README-BUILDER.md`, add to the `**Optional:**` list after the `metrics` entry:

```markdown
- `mcp` - Model Context Protocol server for LLM clients
```

- [ ] **Step 5: Update the stdio launcher**

In `package/src/main/scripts/mcp-stdio.sh` line 76, change the main class:

```bash
    $ARGS com.arcadedb.mcp.MCPStdioServer "$@"
```

Verify no other script references the old FQN:

```bash
grep -rn "com.arcadedb.server.mcp" package/ ; echo "EXIT=$?"
```

Expected: no output, `EXIT=1`.

- [ ] **Step 6: Verify the builder script parses and lists the module**

```bash
bash -n package/arcadedb-builder.sh && echo "syntax OK"
bash package/arcadedb-builder.sh --help | grep -A12 "OPTIONAL MODULES"
```

Expected: `syntax OK`, and `mcp` appearing in the printed list.

- [ ] **Step 7: Build the distribution**

```bash
mvn -q -pl package -am install -DskipTests
ls package/target/*/lib/ | grep -i mcp
```

Expected: `arcadedb-mcp-26.8.1-SNAPSHOT.jar` present in the assembled `lib/` directory. Adjust the `ls` path to whatever the assembly produces if the glob does not match; find it with `find package/target -name "arcadedb-mcp*.jar"`.

- [ ] **Step 8: Stage and report**

```bash
git add package/pom.xml coverage/pom.xml package/arcadedb-builder.sh \
        package/README-BUILDER.md package/src/main/scripts/mcp-stdio.sh
git status --short
```

Report whether the jar appeared in the distribution `lib/`. Do not commit.

---

## Task 7: OpenAPI precondition wording

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/openapi/McpApiSpec.java`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

**Background:** `McpApiSpec` stays in the server module and stays unconditional. `PluginApiSpec`'s Javadoc documents why: the specification has to be deterministic for client generation, and plugin modules cannot declare path items because swagger is not on their `provided` classpath. Now that MCP is a plugin, its operations need the same precondition sentence the other plugin routes carry.

- [ ] **Step 1: Add the constant**

In `McpApiSpec.java`, add as the first member of the class, matching the style of `PluginApiSpec`'s `RAFT_REQUIRED`:

```java
  private static final String MCP_PLUGIN_REQUIRED =
      "Requires MCPPlugin: present in every standard distribution, absent from a custom build that excludes the MCP module.";
```

- [ ] **Step 2: Append it to the three operation descriptions**

Append `MCP_PLUGIN_REQUIRED` as a final paragraph to each of the three operation descriptions. For `invokeMcp` (the text block starting at line 50), add a trailing line before the closing `"""`:

```

Requires MCPPlugin: present in every standard distribution, absent from a custom build that excludes the MCP module.
```

For `getMcpConfig` (line 99), change the concatenated description to:

```java
        "Returns the MCP server's enablement, permission flags, tool profile, and access lists. "
            + "Restricted to the root user. " + MCP_PLUGIN_REQUIRED);
```

For `updateMcpConfig` (line 107), append to the text block's final paragraph:

```java
            Answers with the full configuration as it stands after the update.

            Requires MCPPlugin: present in every standard distribution, absent from a custom build that \
            excludes the MCP module.""");
```

Use the constant where a concatenation is already in play; inline the sentence inside text blocks, matching how `PluginApiSpec` handles each shape.

- [ ] **Step 3: Verify the spec still generates and the inventory is unchanged**

```bash
mvn -q -pl server -am install -DskipTests
mvn -q -pl server install -DskipITs=false -Dit.test=OpenApiSpecGenerationIT
```

Expected: PASS. The operation inventory assertion covers `POST /api/v1/mcp`, `GET /api/v1/mcp/config`, and `POST /api/v1/mcp/config`; all three must still be present, since only descriptions changed. A failure here means an operation id or path was altered by mistake.

- [ ] **Step 4: Stage and report**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/McpApiSpec.java
git status --short
```

Do not commit.

---

## Task 8: Studio handles a distribution without MCP

**Files:**
- Modify: `studio/src/main/resources/static/js/studio-server.js:868-890,961-977`
- Create: `studio/test/mcp-module-absent.test.js`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

**Background:** `loadMCPConfig()` pipes `jqXHR.responseText` straight into `globalNotifyError`. On a distribution built without the MCP module, `/api/v1/mcp/config` returns 404 and the user sees an error toast with an unhelpful body instead of an explanation.

Studio tests run pure functions extracted from the source with `extractFn` + `eval` (see `studio/test/security-group-select.test.js`), so the decision has to live in a function that touches no DOM.

- [ ] **Step 1: Write the failing test**

Create `studio/test/mcp-module-absent.test.js`. Copy the Apache 2.0 header from `studio/test/security-group-select.test.js` lines 1-20, then:

```javascript
// A distribution built without the MCP module answers 404 on /api/v1/mcp/config. That is a build-time
// choice, not an error, so the MCP tab must explain itself instead of raising the generic error toast
// with an empty body. Run with:
//
//     node --test studio/test/mcp-module-absent.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-server.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-server.js: " + name);
  let i = src.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  return src.substring(start, i);
}

eval(extractFn("isMCPModuleAbsent"));

test("a 404 means the module was excluded from this build", () => {
  assert.equal(isMCPModuleAbsent({ status: 404 }), true);
});

test("a 403 is a real error and must still be reported", () => {
  assert.equal(isMCPModuleAbsent({ status: 403 }), false);
});

test("a 500 is a real error and must still be reported", () => {
  assert.equal(isMCPModuleAbsent({ status: 500 }), false);
});

test("a missing or malformed jqXHR is not treated as an absent module", () => {
  assert.equal(isMCPModuleAbsent(null), false);
  assert.equal(isMCPModuleAbsent(undefined), false);
  assert.equal(isMCPModuleAbsent({}), false);
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
node --test studio/test/mcp-module-absent.test.js
```

Expected: FAIL with `function not found in studio-server.js: isMCPModuleAbsent`.

- [ ] **Step 3: Add the pure predicate and the disabled state**

In `studio/src/main/resources/static/js/studio-server.js`, add above `loadMCPConfig` (near the `var mcpConfigLoaded = false;` declaration at line 868):

```javascript
// A 404 on the MCP routes means this distribution was built without the MCP module, which is a build-time
// choice rather than a failure. Every other status is a genuine error and must still surface.
function isMCPModuleAbsent(jqXHR) {
  return !!jqXHR && jqXHR.status === 404;
}

function showMCPModuleAbsent() {
  $("#mcpConfigForm").html(
    '<div class="alert alert-secondary" style="font-size: 0.85rem;">' +
      "The MCP module is not installed in this distribution. Rebuild with the <code>mcp</code> module to enable it." +
      "</div>"
  );
}
```

Change `loadMCPConfig`'s `.fail()` handler to:

```javascript
    .fail(function (jqXHR, textStatus, errorThrown) {
      mcpConfigLoaded = false;
      if (isMCPModuleAbsent(jqXHR)) {
        showMCPModuleAbsent();
        return;
      }
      globalNotifyError(jqXHR.responseText);
    });
```

Change `saveMCPConfig`'s `.fail()` handler (around line 975) to the same shape:

```javascript
    .fail(function (jqXHR, textStatus, errorThrown) {
      if (isMCPModuleAbsent(jqXHR)) {
        showMCPModuleAbsent();
        return;
      }
      globalNotifyError(jqXHR.responseText);
    });
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
node --test studio/test/mcp-module-absent.test.js
```

Expected: PASS, 4 tests.

- [ ] **Step 5: Run the whole Studio suite**

```bash
cd studio && npm test; cd ..
```

Expected: all test files pass, including the new one picked up automatically by `run-tests.js`.

- [ ] **Step 6: Stage and report**

```bash
git add studio/src/main/resources/static/js/studio-server.js studio/test/mcp-module-absent.test.js
git status --short
```

Note for the reviewer: `studio/src/main/resources/static/dist/` is generated and untracked; do not stage anything from it. Do not commit.

---

## Final verification

Run after all eight tasks are staged.

- [ ] **Step 1: Full reactor build**

```bash
mvn -q install
```

Expected: BUILD SUCCESS across every module. Use `install`, never bare `mvn test`: `arcadedb-gremlin-it` consumes `arcadedb-gremlin`'s package-phase artifacts, which a `test`-phase run never produces.

- [ ] **Step 2: Confirm the one-way dependency**

```bash
grep -rn "com\.arcadedb\.mcp" server/src coverage/src 2>/dev/null | grep -v /target/ ; echo "EXIT=$?"
mvn -q -pl server dependency:tree | grep -i "arcadedb-mcp" ; echo "EXIT=$?"
```

Expected: both print nothing with `EXIT=1`.

- [ ] **Step 3: Confirm nothing still names the old package**

```bash
grep -rnI "com\.arcadedb\.server\.mcp" . \
  | grep -v /target/ | grep -v /.worktrees/ | grep -v /.claude/ | grep -v /docs/
echo "EXIT=$?"
```

Expected: no output, `EXIT=1`. Spec and plan documents under `docs/` legitimately reference the old package when describing the move, hence the exclusion.

- [ ] **Step 4: Report**

Summarize for the human: modules built, total test counts for `server` and `mcp`, any IT skipped and why, and the exact list of staged files (`git status --short`). Do not commit; the human commits after review.

---

## Deferred follow-ups

Not part of this plan. File as separate issues.

1. **Split `MCPServerPluginTest`** (147 KB, one class) along tool boundaries. Folding it into the extraction would make the diff unreviewable.
2. **Docs repository page** for MCP install and enablement, covering the new module and the `--modules=mcp` builder flag.
3. **Release note** for anyone whose launcher targets `com.arcadedb.server.mcp.MCPStdioServer` directly. The shipped `mcp-stdio.sh` is updated by Task 6; a hand-rolled launcher is not.
