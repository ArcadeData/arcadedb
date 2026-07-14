# MCP Schema Resource (#4865) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose each accessible database's schema as an MCP Resource at `arcadedb://{database}/schema` over both the HTTP and stdio transports, and collapse the duplicated JSON-RPC dispatch of the two transports onto a single shared `MCPDispatcher`.

**Architecture:** Three layers. `GetSchemaTool` is refactored so its schema walk becomes a reusable `buildSchema(Database, String)` helper. A new `MCPResources` class holds all Resources logic (URI parsing, access filtering, list/read). A new `MCPDispatcher` becomes the single source of truth for the JSON-RPC method surface (protocol version, tool list, instructions, gating, method switch, envelope helpers), and `MCPHttpHandler` / `MCPStdioServer` shrink to thin transport adapters over it.

**Tech Stack:** Java 21, Maven, JUnit 5 (Jupiter), AssertJ, `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, Undertow (HTTP transport). All changes are confined to the `server` module.

**Spec:** `docs/superpowers/specs/2026-07-14-mcp-schema-resource-4865-design.md`
**Issue:** [#4865](https://github.com/ArcadeData/arcadedb/issues/4865) | **Epic:** [#4859](https://github.com/ArcadeData/arcadedb/issues/4859) (Wave 1)

## Global Constraints

- **Java 21+.** Use `switch` expressions and `record` where the surrounding code already does.
- **No fully qualified names in code.** Import the class and use the bare name.
- **`final` on variables and parameters** wherever possible. This is the house style throughout the MCP package.
- **Single-statement `if` bodies take no curly braces.** Match the existing MCP code exactly.
- **JSON is `com.arcadedb.serializer.json.JSONObject` / `JSONArray`.** Never `org.json`, never Jackson. Use the two-argument getters (`getString(key, default)`) to avoid null checks.
- **No new dependencies.**
- **No `System.out`** left behind. `MCPStdioServer` writes to an injected `PrintStream`, never `System.out` directly.
- **Comments state behavioral invariants only.** No issue numbers, no "this line does X", no attribution.
- **Do not add Claude as an author** of any source file or commit.
- **Do not run `git push`** and do not open a PR. Commit locally only; the maintainer reviews before pushing.
- **Existing MCP tests are the regression net for the dispatcher extraction.** They must stay green *unedited*, with exactly two intentional exceptions, both in Task 5. Any other existing test that needs editing is a signal the refactor broke behavior: stop and investigate rather than adjusting the assertion.
- **Apache 2.0 license header** at the top of every new `.java` file. Copy it verbatim from an existing file in the same package.
- **Tool count stays at 11.** This issue adds a protocol primitive, not a tool. `assertThat(tools.length()).isEqualTo(11)` must remain true.

**Build/test commands.** All work is in the `server` module. If the reactor artifacts are stale, run once from the repo root:

```bash
mvn -DskipTests install
```

Then, from the repo root, run a single test class with:

```bash
mvn -pl server test -Dtest=MCPResourcesTest
```

---

### Task 1: Extract `buildSchema` from `GetSchemaTool`

Pure refactor. No behavior change. This is what makes the tool-output-equals-resource-content criterion true by construction later, because there will be exactly one implementation of the schema walk.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/GetSchemaTool.java:53-128`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java` (existing `getSchema` test is the regression net; no edit)

**Interfaces:**
- Consumes: nothing.
- Produces: `public static JSONObject GetSchemaTool.buildSchema(final Database database, final String databaseName)`, returning `{"database": <name>, "types": [...]}`. Consumed by Task 2.

- [ ] **Step 1: Run the existing test to establish the green baseline**

Run: `mvn -pl server test -Dtest=MCPServerPluginTest#getSchema`
Expected: PASS. If this is already red, stop: the baseline is broken and this refactor cannot be validated.

- [ ] **Step 2: Extract the schema walk into `buildSchema`**

In `GetSchemaTool.java`, replace the whole `execute` method (lines 53-128) with the two methods below. The body of `buildSchema` is the *existing* code from lines 62-127, moved verbatim; only the method wrapper is new.

```java
  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = args.getString("database");

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    return buildSchema(database, databaseName);
  }

  /**
   * Builds the JSON schema document for a database. The single source of truth for schema shaping: the get_schema
   * tool and the arcadedb://{database}/schema resource both render from here, so their content cannot drift apart.
   * Performs no permission or authorization check; the caller is responsible for both.
   */
  public static JSONObject buildSchema(final Database database, final String databaseName) {
    final Schema schema = database.getSchema();
    final JSONArray types = new JSONArray();

    for (final DocumentType type : schema.getTypes()) {
      final JSONObject typeJson = new JSONObject();
      typeJson.put("name", type.getName());

      if (type instanceof VertexType)
        typeJson.put("category", "vertex");
      else if (type instanceof EdgeType)
        typeJson.put("category", "edge");
      else
        typeJson.put("category", "document");

      // Parent types
      final JSONArray parents = new JSONArray();
      for (final DocumentType superType : type.getSuperTypes())
        parents.put(superType.getName());
      if (parents.length() > 0)
        typeJson.put("parentTypes", parents);

      // Properties
      final JSONArray properties = new JSONArray();
      for (final Property prop : type.getProperties()) {
        final JSONObject propJson = new JSONObject();
        propJson.put("name", prop.getName());
        propJson.put("type", prop.getType().name());
        if (prop.isMandatory())
          propJson.put("mandatory", true);
        if (prop.isReadonly())
          propJson.put("readonly", true);
        if (prop.isNotNull())
          propJson.put("notNull", true);
        if (prop.getDefaultValue() != null)
          propJson.put("default", prop.getDefaultValue());
        if (prop.getMin() != null)
          propJson.put("min", prop.getMin());
        if (prop.getMax() != null)
          propJson.put("max", prop.getMax());
        if (prop.getOfType() != null)
          propJson.put("ofType", prop.getOfType());
        properties.put(propJson);
      }
      if (properties.length() > 0)
        typeJson.put("properties", properties);

      // Indexes
      final JSONArray indexes = new JSONArray();
      for (final TypeIndex index : type.getAllIndexes(false)) {
        final JSONObject indexJson = new JSONObject();
        indexJson.put("name", index.getName());
        indexJson.put("type", index.getType().name());
        indexJson.put("properties", new JSONArray(index.getPropertyNames()));
        indexJson.put("unique", index.isUnique());
        indexes.put(indexJson);
      }
      if (indexes.length() > 0)
        typeJson.put("indexes", indexes);

      types.put(typeJson);
    }

    final JSONObject result = new JSONObject();
    result.put("database", databaseName);
    result.put("types", types);
    return result;
  }
```

The imports at the top of the file are unchanged: every type used by `buildSchema` was already imported.

- [ ] **Step 3: Run the test to verify no behavior changed**

Run: `mvn -pl server test -Dtest=MCPServerPluginTest#getSchema`
Expected: PASS, unchanged. The test was not edited, so a pass means the extraction is behavior-preserving.

- [ ] **Step 4: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/GetSchemaTool.java
git commit -m "refactor(mcp): extract reusable buildSchema from GetSchemaTool"
```

---

### Task 2: `MCPResources` and `MCPResourceNotFoundException`

All Resources logic, transport-free and independently testable. Neither transport learns anything about URIs.

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/mcp/MCPResourceNotFoundException.java`
- Create: `server/src/main/java/com/arcadedb/server/mcp/MCPResources.java`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPResourcesTest.java` (create)

**Interfaces:**
- Consumes: `GetSchemaTool.buildSchema(Database, String)` from Task 1.
- Produces, all consumed by Task 3:
  - `MCPResources.list(ArcadeDBServer, ServerSecurityUser, MCPConfiguration)` returns `{"resources": [...]}`; never throws for permission reasons, returns an empty array when reads are disabled.
  - `MCPResources.read(ArcadeDBServer, ServerSecurityUser, MCPConfiguration, String uri)` returns `{"contents": [{uri, mimeType, text}]}`; throws `SecurityException` when reads are disabled and `MCPResourceNotFoundException` for an unknown database, an unauthorized database, or a malformed URI.
  - `MCPResources.schemaURI(String databaseName)` returns `arcadedb://<db>/schema`.
  - `MCPResources.parseSchemaURI(String uri)` returns the database name, or `null` when the URI does not match.
  - `MCPResourceNotFoundException extends RuntimeException`.

- [ ] **Step 1: Write the failing test**

Create `server/src/test/java/com/arcadedb/server/mcp/MCPResourcesTest.java`.

`BaseGraphServerTest` provisions a database named `graph` and the constant `DEFAULT_PASSWORD_FOR_TESTS`. `MCPConfiguration` here is the live server instance, shared across the class, so `@BeforeEach` resets `allowReads` to `true` and the two denial tests flip it locally.

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MCPResourcesTest extends BaseGraphServerTest {

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = getServer(0).getMCPConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void listExposesOneSchemaResourcePerDatabase() {
    final JSONArray resources = MCPResources.list(getServer(0), user, config).getJSONArray("resources");

    JSONObject graphResource = null;
    for (int i = 0; i < resources.length(); i++)
      if ("arcadedb://graph/schema".equals(resources.getJSONObject(i).getString("uri")))
        graphResource = resources.getJSONObject(i);

    assertThat(graphResource).isNotNull();
    assertThat(graphResource.getString("name")).isEqualTo("graph schema");
    assertThat(graphResource.getString("mimeType")).isEqualTo("application/json");
    assertThat(graphResource.getString("description")).contains("graph");
  }

  @Test
  void listIsEmptyWhenReadsDisabled() {
    config.setAllowReads(false);

    final JSONArray resources = MCPResources.list(getServer(0), user, config).getJSONArray("resources");

    assertThat(resources.length()).isZero();
  }

  @Test
  void readReturnsSchemaIdenticalToGetSchemaTool() {
    final JSONObject resource = MCPResources.read(getServer(0), user, config, "arcadedb://graph/schema");

    final JSONArray contents = resource.getJSONArray("contents");
    assertThat(contents.length()).isEqualTo(1);
    assertThat(contents.getJSONObject(0).getString("uri")).isEqualTo("arcadedb://graph/schema");
    assertThat(contents.getJSONObject(0).getString("mimeType")).isEqualTo("application/json");

    final JSONObject toolResult = GetSchemaTool.execute(getServer(0), user, new JSONObject().put("database", "graph"), config);

    assertThat(contents.getJSONObject(0).getString("text")).isEqualTo(toolResult.toString());
  }

  @Test
  void readRejectsUnknownDatabase() {
    assertThatThrownBy(() -> MCPResources.read(getServer(0), user, config, "arcadedb://nosuchdb/schema"))
        .isInstanceOf(MCPResourceNotFoundException.class)
        .hasMessageContaining("Resource not found");
  }

  @Test
  void readRejectsMalformedURI() {
    // Wrong scheme, wrong suffix, empty database, embedded slash, and a URI too short to hold a database name.
    for (final String uri : new String[] { "http://graph/schema", "arcadedb://graph/tables", "arcadedb:///schema",
        "arcadedb://a/b/schema", "arcadedb://schema", "graph", "" })
      assertThatThrownBy(() -> MCPResources.read(getServer(0), user, config, uri))
          .isInstanceOf(MCPResourceNotFoundException.class);
  }

  @Test
  void readDeniedWhenReadsDisabled() {
    config.setAllowReads(false);

    assertThatThrownBy(() -> MCPResources.read(getServer(0), user, config, "arcadedb://graph/schema"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void parseSchemaURIPreservesCaseAndUnderscores() {
    // java.net.URI would lowercase the authority and reject the underscore, silently resolving this to nothing.
    assertThat(MCPResources.parseSchemaURI("arcadedb://My_DB/schema")).isEqualTo("My_DB");
    assertThat(MCPResources.parseSchemaURI("arcadedb://graph/schema")).isEqualTo("graph");
    assertThat(MCPResources.parseSchemaURI("arcadedb://schema")).isNull();
    assertThat(MCPResources.parseSchemaURI(null)).isNull();
  }

  @Test
  void schemaURIRoundTripsThroughParse() {
    assertThat(MCPResources.parseSchemaURI(MCPResources.schemaURI("My_DB"))).isEqualTo("My_DB");
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl server test -Dtest=MCPResourcesTest`
Expected: FAIL at compilation, with "cannot find symbol: class MCPResources" and "cannot find symbol: class MCPResourceNotFoundException".

- [ ] **Step 3: Write `MCPResourceNotFoundException`**

Create `server/src/main/java/com/arcadedb/server/mcp/MCPResourceNotFoundException.java`. Copy the Apache 2.0 header verbatim from `MCPConfiguration.java`.

```java
package com.arcadedb.server.mcp;

/**
 * Raised when an MCP resource URI does not resolve. Unknown databases, databases the user cannot access, and
 * malformed URIs all raise this same exception, so that resources/read cannot be used to probe which databases exist.
 */
public class MCPResourceNotFoundException extends RuntimeException {

  public MCPResourceNotFoundException(final String message) {
    super(message);
  }
}
```

- [ ] **Step 4: Write `MCPResources`**

Create `server/src/main/java/com/arcadedb/server/mcp/MCPResources.java`. Copy the Apache 2.0 header verbatim from `MCPConfiguration.java`.

```java
package com.arcadedb.server.mcp;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.TreeSet;

/**
 * MCP Resources surface. Exposes each database's schema at arcadedb://{database}/schema.
 */
public class MCPResources {
  public static final String URI_SCHEME    = "arcadedb://";
  public static final String SCHEMA_SUFFIX = "/schema";
  public static final String MIME_TYPE     = "application/json";

  private MCPResources() {
  }

  /**
   * Enumerates the schema resources the user may read. A discovery call issued unprompted by most clients at session
   * start, so a denial is expressed as an empty list rather than an error, and databases the user cannot access are
   * omitted rather than reported.
   */
  public static JSONObject list(final ArcadeDBServer server, final ServerSecurityUser user, final MCPConfiguration config) {
    final JSONArray resources = new JSONArray();

    if (config.isAllowReads())
      for (final String databaseName : new TreeSet<>(server.getDatabaseNames())) {
        if (!user.canAccessToDatabase(databaseName))
          continue;

        resources.put(new JSONObject()
            .put("uri", schemaURI(databaseName))
            .put("name", databaseName + " schema")
            .put("description", "Schema of the ArcadeDB database '" + databaseName
                + "': types (vertex, edge, document), properties, indexes, inheritance.")
            .put("mimeType", MIME_TYPE));
      }

    return new JSONObject().put("resources", resources);
  }

  /**
   * Reads a schema resource. An unknown database, an unauthorized database, and a malformed URI are indistinguishable
   * to the caller, so that this method cannot be used to probe which databases exist.
   */
  public static JSONObject read(final ArcadeDBServer server, final ServerSecurityUser user, final MCPConfiguration config,
      final String uri) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = parseSchemaURI(uri);
    if (databaseName == null || !server.existsDatabase(databaseName) || !user.canAccessToDatabase(databaseName))
      throw new MCPResourceNotFoundException("Resource not found: " + uri);

    final Database database = server.getDatabase(databaseName);
    final JSONObject schema = GetSchemaTool.buildSchema(database, databaseName);

    final JSONArray contents = new JSONArray();
    contents.put(new JSONObject()
        .put("uri", uri)
        .put("mimeType", MIME_TYPE)
        .put("text", schema.toString()));

    return new JSONObject().put("contents", contents);
  }

  public static String schemaURI(final String databaseName) {
    return URI_SCHEME + databaseName + SCHEMA_SUFFIX;
  }

  /**
   * Returns the database name in arcadedb://{database}/schema, or null when the URI does not match that shape.
   * Parsed with string operations rather than java.net.URI on purpose: URI applies hostname rules to the authority,
   * lowercasing it and rejecting the underscores that an ArcadeDB database name may legally contain, so
   * arcadedb://My_DB/schema would otherwise resolve to nothing. A database name cannot contain '/', because it is a
   * directory name on disk, so this parse round-trips exactly against what list() emits.
   */
  public static String parseSchemaURI(final String uri) {
    if (uri == null || !uri.startsWith(URI_SCHEME) || !uri.endsWith(SCHEMA_SUFFIX))
      return null;
    if (uri.length() <= URI_SCHEME.length() + SCHEMA_SUFFIX.length())
      return null;

    final String databaseName = uri.substring(URI_SCHEME.length(), uri.length() - SCHEMA_SUFFIX.length());
    if (databaseName.indexOf('/') >= 0)
      return null;

    return databaseName;
  }
}
```

The length guard on line 3 of `parseSchemaURI` is load-bearing, not defensive padding: `arcadedb://schema` both starts with the scheme and ends with the suffix, and without the guard the `substring` call would throw `StringIndexOutOfBoundsException` instead of returning `null`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -pl server test -Dtest=MCPResourcesTest`
Expected: PASS, 8 tests.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/MCPResources.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPResourceNotFoundException.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPResourcesTest.java
git commit -m "feat(mcp): add MCPResources with arcadedb://{database}/schema resource"
```

---

### Task 3: `MCPDispatcher`, and rewire `MCPHttpHandler` onto it

The dispatcher becomes the single source of truth for the JSON-RPC method surface. This task moves the HTTP handler's logic into it wholesale, then reduces `MCPHttpHandler` to a transport adapter. Stdio is not touched yet; it still carries its own copy and must keep working.

The existing `MCPServerPluginTest` is the regression net. It must pass **unedited** at the end of this task.

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java` (full rewrite)
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java` (unedited in this task)

**Interfaces:**
- Consumes: `MCPResources.list/read`, `MCPResourceNotFoundException` from Task 2.
- Produces, consumed by Task 4:
  - `new MCPDispatcher(ArcadeDBServer server, MCPConfiguration config, String transport)`
  - `MCPResponse MCPDispatcher.dispatch(JSONObject request, ServerSecurityUser user)`, where `request` is nullable, meaning an empty body.
  - `record MCPDispatcher.MCPResponse(int httpStatus, JSONObject json)`, where a `null` `json` means "notification, send no reply".

- [ ] **Step 1: Write `MCPDispatcher`**

Create `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java`. Copy the Apache 2.0 header verbatim from `MCPHttpHandler.java`.

The `TOOLS_LIST` static block, the `tools/call` switch, `formatArgs`, `sanitizeForLog`, `formatResult`, and `toolError` are all moved verbatim from `MCPHttpHandler`. The gating checks, `initialize`, and the resources arms are the new parts.

```java
package com.arcadedb.server.mcp;

import com.arcadedb.Constants;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.tools.ExecuteCommandTool;
import com.arcadedb.server.mcp.tools.FullTextSearchTool;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.GetServerSettingsTool;
import com.arcadedb.server.mcp.tools.ListDatabasesTool;
import com.arcadedb.server.mcp.tools.ProfilerStartTool;
import com.arcadedb.server.mcp.tools.ProfilerStatusTool;
import com.arcadedb.server.mcp.tools.ProfilerStopTool;
import com.arcadedb.server.mcp.tools.QueryTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.server.mcp.tools.SetServerSettingTool;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.logging.Level;

/**
 * Transport-neutral MCP JSON-RPC dispatcher. Owns the protocol surface (version, tool list, instructions, gating,
 * method routing, envelope shaping); the HTTP and stdio transports own only their I/O and their own parse errors.
 */
public class MCPDispatcher {
  public static final  String    MCP_PROTOCOL_VERSION = "2025-03-26";
  private static final JSONArray TOOLS_LIST;

  private static final String INSTRUCTIONS =
      """
      You are connected to an ArcadeDB multi-model database server. Follow these rules:
      1. ALWAYS call list_databases first when you do not know the target database name. Never guess it.
      2. Prefer Cypher (language: 'cypher') for graph queries unless SQL is explicitly requested.
      3. Use the 'query' tool for read-only operations (SELECT, MATCH, RETURN) and 'execute_command' for writes (CREATE, INSERT, UPDATE, DELETE, MERGE).
      4. Call get_schema before writing queries against an unfamiliar database to understand its types and properties. If your client supports MCP Resources, prefer reading arcadedb://{database}/schema instead: it carries the same content without spending a tool call.
      5. If a query returns no results, verify the type/property names with get_schema before concluding the data does not exist.""";

  static {
    TOOLS_LIST = new JSONArray();
    TOOLS_LIST.put(ListDatabasesTool.getDefinition());
    TOOLS_LIST.put(GetSchemaTool.getDefinition());
    TOOLS_LIST.put(QueryTool.getDefinition());
    TOOLS_LIST.put(ExecuteCommandTool.getDefinition());
    TOOLS_LIST.put(FullTextSearchTool.getDefinition());
    TOOLS_LIST.put(ServerStatusTool.getDefinition());
    TOOLS_LIST.put(ProfilerStartTool.getDefinition());
    TOOLS_LIST.put(ProfilerStopTool.getDefinition());
    TOOLS_LIST.put(ProfilerStatusTool.getDefinition());
    TOOLS_LIST.put(GetServerSettingsTool.getDefinition());
    TOOLS_LIST.put(SetServerSettingTool.getDefinition());
  }

  /**
   * A transport-neutral reply. A null json means a JSON-RPC notification, for which no reply is sent at all;
   * the httpStatus is meaningful only to the HTTP transport and is ignored by stdio.
   */
  public record MCPResponse(int httpStatus, JSONObject json) {
  }

  private final ArcadeDBServer   server;
  private final MCPConfiguration config;
  private final String           transport;

  public MCPDispatcher(final ArcadeDBServer server, final MCPConfiguration config, final String transport) {
    this.server = server;
    this.config = config;
    this.transport = transport;
  }

  /**
   * Routes one parsed JSON-RPC request. A null request means an empty body. Authentication is checked before
   * anything else, so that server state is not disclosed to unauthenticated callers.
   */
  public MCPResponse dispatch(final JSONObject request, final ServerSecurityUser user) {
    if (user == null)
      return error(null, -32600, "Authentication required", 401);

    if (!config.isEnabled())
      return error(null, -32600, "MCP server is disabled", 503);

    if (request == null)
      return error(null, -32700, "Parse error: empty request body", 200);

    final Object id = request.opt("id");

    if (!config.isUserAllowed(user.getName()))
      return error(id, -32600, "User not authorized for MCP access", 403);

    final String method = request.getString("method", "");
    final JSONObject params = request.getJSONObject("params", new JSONObject());

    LogManager.instance().log(this, Level.INFO, "MCP[%s] %s (user=%s)", transport, method, user.getName());

    return switch (method) {
      case "initialize" -> result(id, initialize());
      case "notifications/initialized" -> new MCPResponse(204, null);
      case "tools/list" -> result(id, new JSONObject().put("tools", TOOLS_LIST));
      case "tools/call" -> toolsCall(id, params, user);
      case "resources/list" -> resourcesList(id, user);
      case "resources/read" -> resourcesRead(id, params, user);
      case "ping" -> result(id, new JSONObject());
      default -> error(id, -32601, "Method not found: " + method, 200);
    };
  }

  private JSONObject initialize() {
    final JSONObject result = new JSONObject();
    result.put("protocolVersion", MCP_PROTOCOL_VERSION);

    final JSONObject serverInfo = new JSONObject();
    serverInfo.put("name", "arcadedb");
    serverInfo.put("version", Constants.getVersion());
    result.put("serverInfo", serverInfo);

    final JSONObject capabilities = new JSONObject();
    capabilities.put("tools", new JSONObject().put("listChanged", false));
    capabilities.put("resources", new JSONObject().put("listChanged", false).put("subscribe", false));
    result.put("capabilities", capabilities);

    result.put("instructions", INSTRUCTIONS);

    return result;
  }

  private MCPResponse resourcesList(final Object id, final ServerSecurityUser user) {
    try {
      return result(id, MCPResources.list(server, user, config));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP[%s] resources/list -> error: %s", transport, e.getMessage());
      return error(id, -32603, "Internal error: " + e.getMessage(), 200);
    }
  }

  private MCPResponse resourcesRead(final Object id, final JSONObject params, final ServerSecurityUser user) {
    final String uri = params.getString("uri", "");

    LogManager.instance().log(this, Level.INFO, "MCP[%s] resources/read '%s' (user=%s)", transport, uri, user.getName());

    try {
      return result(id, MCPResources.read(server, user, config, uri));
    } catch (final SecurityException e) {
      LogManager.instance().log(this, Level.INFO, "MCP[%s] resources/read -> permission denied: %s", transport, e.getMessage());
      return error(id, -32600, e.getMessage(), 200);
    } catch (final MCPResourceNotFoundException e) {
      return error(id, -32002, e.getMessage(), 200);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP[%s] resources/read -> error: %s", transport, e.getMessage());
      return error(id, -32603, "Internal error: " + e.getMessage(), 200);
    }
  }

  private MCPResponse toolsCall(final Object id, final JSONObject params, final ServerSecurityUser user) {
    final String toolName = params.getString("name", "");
    final JSONObject args = params.getJSONObject("arguments", new JSONObject());

    LogManager.instance()
        .log(this, Level.INFO, "MCP[%s] tools/call '%s' %s (user=%s)", transport, toolName, formatArgs(args), user.getName());

    try {
      final JSONObject toolResult = switch (toolName) {
        case "list_databases" -> ListDatabasesTool.execute(server, user, args, config);
        case "get_schema" -> GetSchemaTool.execute(server, user, args, config);
        case "query" -> QueryTool.execute(server, user, args, config);
        case "execute_command" -> ExecuteCommandTool.execute(server, user, args, config);
        case "full_text_search" -> FullTextSearchTool.execute(server, user, args, config);
        case "server_status" -> ServerStatusTool.execute(server, user, args, config);
        case "profiler_start" -> ProfilerStartTool.execute(server, user, args, config);
        case "profiler_stop" -> ProfilerStopTool.execute(server, user, args, config);
        case "profiler_status" -> ProfilerStatusTool.execute(server, user, args, config);
        case "get_server_settings" -> GetServerSettingsTool.execute(server, user, args, config);
        case "set_server_setting" -> SetServerSettingTool.execute(server, user, args, config);
        default -> throw new IllegalArgumentException("Unknown tool: " + toolName);
      };

      LogManager.instance()
          .log(this, Level.INFO, "MCP[%s] tools/call '%s' -> %s", transport, toolName, formatResult(toolName, toolResult));

      final JSONObject result = new JSONObject();
      final JSONArray content = new JSONArray();
      content.put(new JSONObject()
          .put("type", "text")
          .put("text", toolResult.toString()));
      result.put("content", content);
      result.put("isError", false);
      return result(id, result);

    } catch (final SecurityException e) {
      LogManager.instance()
          .log(this, Level.INFO, "MCP[%s] tools/call '%s' -> permission denied: %s", transport, toolName, e.getMessage());
      return toolError(id, e.getMessage());
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.WARNING, "MCP[%s] tools/call '%s' -> error: %s", transport, toolName, e.getMessage());
      return toolError(id, e.getMessage());
    }
  }

  private static String formatArgs(final JSONObject args) {
    if (args.length() == 0)
      return "{}";
    final StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (final String key : args.keySet()) {
      if (!first)
        sb.append(", ");
      first = false;
      final Object value = args.get(key);
      if (value instanceof String s) {
        final String sanitized = sanitizeForLog(s);
        if (sanitized.length() > 100)
          sb.append(key).append("=\"").append(sanitized, 0, 100).append("...\"");
        else
          sb.append(key).append("=\"").append(sanitized).append("\"");
      } else
        sb.append(key).append("=").append(value);
    }
    return sb.append("}").toString();
  }

  private static String sanitizeForLog(final String value) {
    return value.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
  }

  private static String formatResult(final String toolName, final JSONObject result) {
    return switch (toolName) {
      case "list_databases" -> result.getJSONArray("databases", new JSONArray()).length() + " database(s)";
      case "get_schema" -> result.getJSONArray("types", new JSONArray()).length() + " type(s)";
      case "query", "execute_command" -> result.getInt("count", 0) + " record(s)";
      case "full_text_search" -> result.getInt("count", 0) + " hit(s)";
      case "server_status" -> "ok";
      case "profiler_start" -> result.getString("status", "ok");
      case "profiler_stop" -> result.getInt("totalQueries", 0) + " queries captured";
      case "profiler_status" -> result.getBoolean("recording", false) ? "recording" : "idle";
      case "get_server_settings" -> result.getJSONArray("settings", new JSONArray()).length() + " setting(s)";
      case "set_server_setting" -> result.getString("key", "") + " updated";
      default -> "ok";
    };
  }

  private static MCPResponse toolError(final Object id, final String message) {
    final JSONObject result = new JSONObject();
    final JSONArray content = new JSONArray();
    content.put(new JSONObject()
        .put("type", "text")
        .put("text", message));
    result.put("content", content);
    result.put("isError", true);
    return result(id, result);
  }

  private static MCPResponse result(final Object id, final JSONObject result) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("result", result);
    return new MCPResponse(200, response);
  }

  private static MCPResponse error(final Object id, final int code, final String message, final int httpStatus) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("error", new JSONObject().put("code", code).put("message", message));
    return new MCPResponse(httpStatus, response);
  }
}
```

- [ ] **Step 2: Rewrite `MCPHttpHandler` as a transport adapter**

Replace the entire body of `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java` below the license header with this. Everything else in the old file is now dead and is deleted with it.

```java
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.mcp.MCPDispatcher.MCPResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * HTTP transport for MCP. Owns the HTTP envelope only; all protocol routing lives in {@link MCPDispatcher}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MCPHttpHandler extends AbstractServerHttpHandler {
  private final MCPDispatcher dispatcher;

  public MCPHttpHandler(final HttpServer httpServer, final ArcadeDBServer server, final MCPConfiguration config) {
    super(httpServer);
    this.dispatcher = new MCPDispatcher(server, config, "http");
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public boolean isRequireAuthentication() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final MCPResponse response = dispatcher.dispatch(payload, user);

    // A null body is a JSON-RPC notification, which takes no response at all.
    if (response.json() == null)
      return new ExecutionResponse(204, "");

    return new ExecutionResponse(response.httpStatus(), response.json().toString());
  }
}
```

- [ ] **Step 3: Run the full HTTP suite to verify nothing regressed**

Run: `mvn -pl server test -Dtest=MCPServerPluginTest`
Expected: PASS, all tests, **with no edit to the test file**. This is the whole point of the task: `initialize`, `toolsList` (still 11), `disabledMCP` (503), `unauthorizedUserDenied` (403), `notificationReturns204`, `methodNotFound` (-32601), and every `tools/call` test must behave exactly as before.

If any of these fail, the extraction changed behavior. Do not adjust the test. Diff the dispatcher against the original `MCPHttpHandler` and find what moved.

- [ ] **Step 4: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java
git commit -m "refactor(mcp): extract MCPDispatcher and add resources/list and resources/read"
```

---

### Task 4: Rewire `MCPStdioServer` onto the dispatcher

Stdio loses its duplicate `TOOLS_LIST`, `initialize`, `tools/call` switch, and envelope helpers. It keeps `main()`, the read loop, and its own `-32700` parse error, because parsing a line off stdin is its own concern.

This is where the intentional stdio behavior change lands: stdio now honors `isEnabled()` and `isUserAllowed()`, and its `initialize` now returns `instructions`.

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java:52-248`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`

**Interfaces:**
- Consumes: `MCPDispatcher`, `MCPDispatcher.MCPResponse` from Task 3.
- Produces: nothing new. The public constructor signature `MCPStdioServer(ArcadeDBServer, MCPConfiguration, ServerSecurityUser, InputStream, PrintStream)` is unchanged, because `MCPStdioServerTest` constructs it directly.

- [ ] **Step 1: Write the failing tests**

Add these three tests to `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`, above the `// ---- Helpers ----` comment. They use the existing `sendSingleRequest` helper.

```java
  @Test
  void initializeAdvertisesResourcesAndInstructions() throws Exception {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 400)
        .put("method", "initialize")
        .put("params", new JSONObject());

    final JSONObject response = sendSingleRequest(request);

    final JSONObject result = response.getJSONObject("result");
    final JSONObject resources = result.getJSONObject("capabilities").getJSONObject("resources");
    assertThat(resources.getBoolean("listChanged")).isFalse();
    assertThat(resources.getBoolean("subscribe")).isFalse();
    assertThat(result.getString("instructions")).contains("arcadedb://{database}/schema");
  }

  @Test
  void resourcesList() throws Exception {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 401)
        .put("method", "resources/list")
        .put("params", new JSONObject());

    final JSONObject response = sendSingleRequest(request);

    final JSONArray resources = response.getJSONObject("result").getJSONArray("resources");
    boolean foundGraph = false;
    for (int i = 0; i < resources.length(); i++)
      if ("arcadedb://graph/schema".equals(resources.getJSONObject(i).getString("uri")))
        foundGraph = true;
    assertThat(foundGraph).isTrue();
  }

  @Test
  void resourcesRead() throws Exception {
    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 402)
        .put("method", "resources/read")
        .put("params", new JSONObject().put("uri", "arcadedb://graph/schema"));

    final JSONObject response = sendSingleRequest(request);

    final JSONArray contents = response.getJSONObject("result").getJSONArray("contents");
    assertThat(contents.length()).isEqualTo(1);
    assertThat(contents.getJSONObject(0).getString("mimeType")).isEqualTo("application/json");

    final JSONObject schema = new JSONObject(contents.getJSONObject(0).getString("text"));
    assertThat(schema.getString("database")).isEqualTo("graph");
    assertThat(schema.getJSONArray("types").length()).isGreaterThan(0);
  }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -pl server test -Dtest=MCPStdioServerTest`
Expected: the three new tests FAIL. `initializeAdvertisesResourcesAndInstructions` fails with a JSON key-not-found on `resources`; the other two fail because `resources/list` and `resources/read` currently hit the `default ->` arm and return a `-32601` error, so `getJSONObject("result")` finds no `result` key.

- [ ] **Step 3: Rewrite `MCPStdioServer` onto the dispatcher**

Replace everything in `MCPStdioServer.java` below the license header with this. `main()` and `run()` are unchanged from the original; `dispatch` now delegates, and `TOOLS_LIST`, `handleInitialize`, `handleToolsList`, `handleToolsCall`, `toolError`, and `jsonRpcResult` are deleted.

```java
package com.arcadedb.server.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.mcp.MCPDispatcher.MCPResponse;
import com.arcadedb.server.security.ServerSecurityUser;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

/**
 * MCP server using stdio transport (JSON-RPC 2.0 over stdin/stdout, newline-delimited).
 * This allows ArcadeDB to work natively with mcp-proxy, Claude Desktop, Cursor, etc.
 * Owns the stdio envelope only; all protocol routing lives in {@link MCPDispatcher}.
 */
public class MCPStdioServer {
  private final MCPDispatcher    dispatcher;
  private final ServerSecurityUser user;
  private final InputStream      input;
  private final PrintStream      output;

  public MCPStdioServer(final ArcadeDBServer server, final MCPConfiguration config, final ServerSecurityUser user,
      final InputStream input, final PrintStream output) {
    this.dispatcher = new MCPDispatcher(server, config, "stdio");
    this.user = user;
    this.input = input;
    this.output = output;
  }

  public static void main(final String[] args) {
    // Save the real stdout for MCP JSON-RPC before any server code can write to it
    final PrintStream mcpOut = System.out;
    System.setOut(System.err);

    final String rootPassword = GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();
    if (rootPassword == null || rootPassword.isEmpty()) {
      System.err.println("ERROR: arcadedb.server.rootPassword must be set for MCP stdio mode");
      System.exit(1);
    }

    final ArcadeDBServer server = new ArcadeDBServer(new ContextConfiguration());
    try {
      server.start();

      final MCPConfiguration config = server.getMCPConfiguration();
      config.setEnabled(true);

      final ServerSecurityUser user = server.getSecurity().authenticate("root", rootPassword, null);

      final MCPStdioServer stdioServer = new MCPStdioServer(server, config, user, System.in, mcpOut);
      stdioServer.run();
    } catch (final Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      e.printStackTrace(System.err);
    } finally {
      server.stop();
    }
  }

  public void run() {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank())
          continue;

        try {
          final JSONObject request = new JSONObject(line);
          final String response = dispatch(request);
          if (response != null) {
            output.println(response);
            output.flush();
          }
        } catch (final Exception e) {
          // JSON parse error
          final String errorResponse = jsonRpcError(null, -32700, "Parse error: " + e.getMessage());
          output.println(errorResponse);
          output.flush();
        }
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "MCP stdio error: %s", e.getMessage());
    }
  }

  private String dispatch(final JSONObject request) {
    final MCPResponse response = dispatcher.dispatch(request, user);

    // A null body is a JSON-RPC notification, which is written back as nothing at all.
    return response.json() == null ? null : response.json().toString();
  }

  private static String jsonRpcError(final Object id, final int code, final String message) {
    final JSONObject response = new JSONObject();
    response.put("jsonrpc", "2.0");
    response.put("id", id);
    response.put("error", new JSONObject().put("code", code).put("message", message));
    return response.toString();
  }
}
```

Note the deleted `Constants` and `JSONArray` imports, and the deleted per-tool imports: none of them are used any more.

- [ ] **Step 4: Run the stdio suite to verify it passes**

Run: `mvn -pl server test -Dtest=MCPStdioServerTest`
Expected: PASS, including the three new tests and every pre-existing test unedited. `toolsList` still asserts 11 tools. `multipleRequestsInSequence` still emits exactly 3 lines.

The pre-existing tests keep passing through the new gating because `MCPStdioServerTest.setupMCP` calls `config.setEnabled(true)` and authenticates as `root`, and `root` is the default sole entry in `allowedUsers`.

- [ ] **Step 5: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java
git commit -m "refactor(mcp): route stdio transport through MCPDispatcher"
```

---

### Task 5: HTTP resources tests

The dispatcher already serves resources over HTTP (Task 3). This task proves it, and lands the two acceptance criteria the issue calls out by name: the `resources` capability, and schema content identical between tool and resource.

`mcpRequest` reads `connection.getInputStream()`, which throws on a non-2xx status. That is fine here: every JSON-RPC error in this task is returned at HTTP 200 with an `error` member in the body. Only the transport-level denials (401/503/403) use non-200 statuses, and those are already covered by existing tests.

**Files:**
- Modify: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`

**Interfaces:**
- Consumes: the HTTP `resources/list` and `resources/read` methods from Task 3.
- Produces: nothing.

- [ ] **Step 1: Write the failing tests**

Add these to `MCPServerPluginTest`, before the `// ---- Helpers ----` section. They reuse the existing `mcpRequest`, `callTool`, `saveMCPConfig`, and `getBasicAuth` helpers, and the `restricteduser` pattern from the existing `databaseAuthorizationDenied` test.

```java
  @Test
  void initializeAdvertisesResourcesCapability() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 400)
        .put("method", "initialize")
        .put("params", new JSONObject()));

    final JSONObject capabilities = response.getJSONObject("result").getJSONObject("capabilities");
    assertThat(capabilities.has("resources")).isTrue();
    final JSONObject resources = capabilities.getJSONObject("resources");
    assertThat(resources.getBoolean("listChanged")).isFalse();
    assertThat(resources.getBoolean("subscribe")).isFalse();
  }

  @Test
  void resourcesList() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 401)
        .put("method", "resources/list")
        .put("params", new JSONObject()));

    final JSONArray resources = response.getJSONObject("result").getJSONArray("resources");

    JSONObject graphResource = null;
    for (int i = 0; i < resources.length(); i++)
      if ("arcadedb://graph/schema".equals(resources.getJSONObject(i).getString("uri")))
        graphResource = resources.getJSONObject(i);

    assertThat(graphResource).isNotNull();
    assertThat(graphResource.getString("name")).isEqualTo("graph schema");
    assertThat(graphResource.getString("mimeType")).isEqualTo("application/json");
  }

  @Test
  void resourcesListMatchesListDatabases() throws Exception {
    final JSONObject listResponse = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 402)
        .put("method", "resources/list")
        .put("params", new JSONObject()));

    final Set<String> fromResources = new HashSet<>();
    final JSONArray resources = listResponse.getJSONObject("result").getJSONArray("resources");
    for (int i = 0; i < resources.length(); i++) {
      final String uri = resources.getJSONObject(i).getString("uri");
      fromResources.add(uri.substring("arcadedb://".length(), uri.length() - "/schema".length()));
    }

    final JSONObject toolResponse = callTool("list_databases", new JSONObject());
    final JSONArray databases = new JSONObject(
        toolResponse.getJSONArray("content").getJSONObject(0).getString("text")).getJSONArray("databases");

    final Set<String> fromTool = new HashSet<>();
    for (int i = 0; i < databases.length(); i++)
      fromTool.add(databases.getString(i));

    assertThat(fromResources).isEqualTo(fromTool);
  }

  @Test
  void resourcesReadMatchesGetSchemaTool() throws Exception {
    final JSONObject readResponse = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 403)
        .put("method", "resources/read")
        .put("params", new JSONObject().put("uri", "arcadedb://graph/schema")));

    final JSONArray contents = readResponse.getJSONObject("result").getJSONArray("contents");
    assertThat(contents.length()).isEqualTo(1);
    assertThat(contents.getJSONObject(0).getString("uri")).isEqualTo("arcadedb://graph/schema");
    assertThat(contents.getJSONObject(0).getString("mimeType")).isEqualTo("application/json");

    final JSONObject toolResponse = callTool("get_schema", new JSONObject().put("database", "graph"));
    final String toolText = toolResponse.getJSONArray("content").getJSONObject(0).getString("text");

    assertThat(contents.getJSONObject(0).getString("text")).isEqualTo(toolText);
  }

  @Test
  void resourcesReadUnknownDatabaseReturnsNotFound() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 404)
        .put("method", "resources/read")
        .put("params", new JSONObject().put("uri", "arcadedb://nosuchdb/schema")));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32002);
  }

  @Test
  void resourcesReadMalformedUriReturnsNotFound() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 405)
        .put("method", "resources/read")
        .put("params", new JSONObject().put("uri", "arcadedb://graph/tables")));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32002);
  }

  @Test
  void resourcesDeniedWhenReadsDisabled() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    try {
      final JSONObject listResponse = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 406)
          .put("method", "resources/list")
          .put("params", new JSONObject()));

      // A discovery call stays quiet: nothing is readable, so nothing is listed.
      assertThat(listResponse.getJSONObject("result").getJSONArray("resources").length()).isZero();

      final JSONObject readResponse = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 407)
          .put("method", "resources/read")
          .put("params", new JSONObject().put("uri", "arcadedb://graph/schema")));

      assertThat(readResponse.has("error")).isTrue();
      assertThat(readResponse.getJSONObject("error").getInt("code")).isEqualTo(-32600);
      assertThat(readResponse.getJSONObject("error").getString("message")).contains("not allowed");
    } finally {
      // Restore for the other tests in this class.
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));
    }
  }

  @Test
  void resourcesListOmitsUnauthorizedDatabases() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root").put("restricteduser")));

    // A user authorized only for a database that does not exist here, so "graph" must not appear in its resource list.
    if (!getServer(0).getSecurity().existsUser("restricteduser"))
      getServer(0).getSecurity().createUser(new JSONObject()
          .put("name", "restricteduser")
          .put("password", getServer(0).getSecurity().encodePassword("restrictedpass"))
          .put("databases", new JSONObject()
              .put("otherdb", new JSONArray().put("admin"))));

    final String restrictedAuth = "Basic " + Base64.getEncoder()
        .encodeToString("restricteduser:restrictedpass".getBytes(StandardCharsets.UTF_8));

    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", restrictedAuth);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 408)
        .put("method", "resources/list")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      final JSONArray resources = new JSONObject(body).getJSONObject("result").getJSONArray("resources");

      for (int i = 0; i < resources.length(); i++)
        assertThat(resources.getJSONObject(i).getString("uri")).isNotEqualTo("arcadedb://graph/schema");
    } finally {
      connection.disconnect();
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));
    }
  }
```

Add these two imports to the top of `MCPServerPluginTest.java` (the rest are already there):

```java
import java.util.HashSet;
import java.util.Set;
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `mvn -pl server test -Dtest=MCPServerPluginTest`
Expected: PASS, all tests including the eight new ones.

These are written after the implementation because Task 3 already shipped the HTTP resources arms. If any of them fail, the defect is in `MCPDispatcher` or `MCPResources`, not in the test.

- [ ] **Step 3: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java
git commit -m "test(mcp): cover resources/list and resources/read over HTTP"
```

---

### Task 6: Full verification sweep

**Files:**
- No source changes expected. This task is a gate.

**Interfaces:**
- Consumes: everything from Tasks 1-5.
- Produces: a verified branch, ready for the maintainer to review and push.

- [ ] **Step 1: Compile the server module cleanly**

Run: `mvn -pl server -am -DskipTests install`
Expected: BUILD SUCCESS. No warnings about unused imports in the MCP package (Task 4 deleted several).

- [ ] **Step 2: Run every MCP test class**

Run: `mvn -pl server test -Dtest='MCP*Test'`
Expected: PASS across `MCPConfigurationTest`, `MCPPermissionsTest`, `MCPServerPluginTest`, `MCPStdioServerTest`, `MCPResourcesTest`.

- [ ] **Step 3: Confirm no existing test was edited except the one intentional change**

Run:

```bash
git diff --stat main -- server/src/test/java/com/arcadedb/server/mcp/
```

Expected: `MCPServerPluginTest.java` and `MCPStdioServerTest.java` show **additions only** (new test methods and imports), and `MCPResourcesTest.java` is new. `MCPConfigurationTest.java` and `MCPPermissionsTest.java` are untouched.

If any *pre-existing* test method body changed, that is the signal from the Global Constraints: the dispatcher extraction altered behavior. Investigate rather than accept.

- [ ] **Step 4: Confirm no `System.out` or debug output was left behind**

Run:

```bash
git diff main -- server/src/main/java/com/arcadedb/server/mcp/ | grep -n "System.out" || echo "clean"
```

Expected: `clean`. (`MCPStdioServer.main` assigns `System.out` to a local `PrintStream` before redirecting it; that is pre-existing and is on an unchanged line, so it will not appear in the diff as an addition.)

- [ ] **Step 5: Run the wider server suite for collateral damage**

Run: `mvn -pl server test`
Expected: PASS, or the same set of failures that `main` already has. Compare against a baseline run on `main` if anything is red, because the server module has pre-existing flaky tests unrelated to MCP.

- [ ] **Step 6: Final commit if anything was touched**

```bash
git status --short
```

If clean, nothing to do. The branch is ready.

**Do not push and do not open a PR.** The maintainer reviews first.

**For the PR body, when the maintainer opens it**, include this note, because it is a behavior change that is not visible in the test diff:

> **Behavior change (stdio transport).** `MCPStdioServer` now honors `MCPConfiguration.isEnabled()` and `isUserAllowed()`, which it previously ignored, and its `initialize` response now includes the `instructions` block that the HTTP transport already returned. The default path is unaffected: `main()` sets `enabled=true` and authenticates as `root`, and `root` is the default sole entry in `allowedUsers`. A stdio deployment that had rewritten `allowedUsers` to omit `root` will now be refused.

---

## Sequencing note for the maintainer

This plan rewrites `MCPHttpHandler.java` and `MCPStdioServer.java`, the two files that #4860 (`vector_search`), #4863 (`sample_records`), and #4864 (`upsert_entity`/`upsert_relationship`) also edit. Per the spec, **#4865 should land before those three.**

The ordering is cheaper in both directions. Each of those issues adds a tool, which today means touching two `TOOLS_LIST` static blocks and two dispatch switches. After this refactor there is exactly one of each, in `MCPDispatcher`, so a sibling PR's diff halves. Landing #4865 last would instead force it to absorb every sibling's duplicated edits before deleting them.
