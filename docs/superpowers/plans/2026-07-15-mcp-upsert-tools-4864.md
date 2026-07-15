# MCP `upsert_entity` / `upsert_relationship` Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two MCP tools, `upsert_entity` and `upsert_relationship`, that perform injection-safe match-or-create-by-key via parameterized Cypher `MERGE`, so agents stop duplicating nodes/edges when building graph memory.

**Architecture:** Server layer only, no engine changes. Each tool is a static `getDefinition()`/`execute()` class in `com.arcadedb.server.mcp.tools` that builds a parameterized Cypher `MERGE` string plus a bound-parameter map, then delegates to a new shared `MCPToolUtils.executeParameterizedWrite(...)` helper. That helper reuses the existing `engine.analyze()` → `ExecuteCommandTool.checkPermission(Set,config)` gating and the same `JsonSerializer` output shape as `execute_command`/`query`. Identifiers (type/relType/property keys) are backtick-quoted and backtick-rejected; all values are bound parameters.

**Tech Stack:** Java 21, ArcadeDB engine (`Database.command(String, String, Map)`), OpenCypher engine, `com.arcadedb.serializer.json.JSONObject`/`JSONArray`, JUnit 5 + AssertJ.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-15-mcp-upsert-tools-4864-design.md`. Every task's requirements implicitly include it.
- **Java 21+**, Maven multi-module. Build the `server` module with `cd server && mvn -q -o test -Dtest=<Class>` (add `-o` only if the local repo is warm; drop it otherwise).
- **No new dependencies.** Use `com.arcadedb.serializer.json.JSONObject`/`JSONArray` only, with the default-valued getters (`getString(key, default)`, `getInt(key, default)`, `getJSONObject(key, default)`).
- **Code style:** import classes (no fully-qualified names in code), `final` on locals and params, single-statement `if` bodies need no braces, match surrounding style. Do **not** add Claude as author anywhere. No issue numbers in Javadoc/comments - state behavioral invariants only.
- **Safe-by-default is untouched:** introduce no new config flag; `enabled=false` and all write flags `false` out of the box stay as-is.
- **Field naming deviation from the issue:** the entity type field is `typeName`, not `type` (JSON-Schema keyword collision), matching the `full_text_search` precedent.
- **Git policy (project CLAUDE.md):** the maintainer performs commits after review. Treat every **Commit** step below as a checkpoint - stage the listed files (`git add ...`) and pause for maintainer review instead of committing autonomously.

## File Structure

- `server/src/main/java/com/arcadedb/server/mcp/tools/MCPToolUtils.java` — **modify**: add `quoteIdentifier(kind, raw)` (identifier validation + backtick-quoting) and `executeParameterizedWrite(database, cypher, params, config)` (analyze + permission-check + transactional execute + serialize).
- `server/src/main/java/com/arcadedb/server/mcp/tools/UpsertEntityTool.java` — **create**: `getDefinition()` + `execute(...)` building `MERGE (n:Type {matchKeys}) [SET ...] RETURN n`.
- `server/src/main/java/com/arcadedb/server/mcp/tools/UpsertRelationshipTool.java` — **create**: `getDefinition()` + `execute(...)` building `MERGE (a) MERGE (b) MERGE (a)-[r]->(b) [SET ...] RETURN r`.
- `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java` — **modify**: import + `TOOLS_LIST` entries + `tools/call` dispatch cases + `formatResult` arm.
- `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java` — **modify**: import + `TOOLS_LIST` entries + dispatch cases.
- `server/src/test/java/com/arcadedb/server/mcp/MCPToolUtilsTest.java` — **create**: unit tests for `quoteIdentifier`.
- `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java` — **modify**: HTTP integration tests + tools/list presence + count assertion `11 → 13`.
- `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java` — **modify**: stdio tools/list count assertion `11 → 13`.

---

### Task 1: Identifier safety helper in `MCPToolUtils`

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/MCPToolUtils.java`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPToolUtilsTest.java` (create)

**Interfaces:**
- Consumes: nothing new.
- Produces: `static String MCPToolUtils.quoteIdentifier(String kind, String raw)` — returns `` `raw` ``; throws `IllegalArgumentException` if `raw` is null, blank, or contains a backtick. `kind` is a human label used only in the error message (e.g. `"type name"`, `"match key"`).

- [ ] **Step 1: Write the failing test**

Create `server/src/test/java/com/arcadedb/server/mcp/MCPToolUtilsTest.java`:

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

import com.arcadedb.server.mcp.tools.MCPToolUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MCPToolUtilsTest {

  @Test
  void quotesPlainIdentifier() {
    assertThat(MCPToolUtils.quoteIdentifier("type name", "Person")).isEqualTo("`Person`");
  }

  @Test
  void quotesIdentifierWithSpacesAndDashes() {
    assertThat(MCPToolUtils.quoteIdentifier("property key", "first name")).isEqualTo("`first name`");
    assertThat(MCPToolUtils.quoteIdentifier("property key", "user-id")).isEqualTo("`user-id`");
  }

  @Test
  void rejectsNullOrBlank() {
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("type name", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("type name");
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("type name", "   "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsBacktickToBlockInjection() {
    assertThatThrownBy(() -> MCPToolUtils.quoteIdentifier("match key", "a` }) DETACH DELETE n //"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("backtick");
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && mvn -q test -Dtest=MCPToolUtilsTest`
Expected: FAIL to compile with "cannot find symbol: method quoteIdentifier".

- [ ] **Step 3: Add the helper method**

In `MCPToolUtils.java`, add this method inside the class (after `resolveDatabase`):

```java
  /**
   * Quotes an identifier (type name, relationship type, or property key) for safe inclusion in a Cypher
   * statement. Cypher cannot bind identifiers as parameters, so they are backtick-quoted here. An identifier
   * that is null, blank, or itself contains a backtick is rejected, which guarantees the quoting cannot be
   * escaped and no clause can be injected through an identifier. Values are never routed through this method;
   * they are always bound as parameters.
   *
   * @param kind human-readable label for the identifier, used only in the rejection message
   * @param raw  the identifier as supplied by the caller
   * @return the identifier wrapped in backticks
   */
  public static String quoteIdentifier(final String kind, final String raw) {
    if (raw == null || raw.isBlank())
      throw new IllegalArgumentException("The " + kind + " must not be null or blank");
    if (raw.indexOf('`') >= 0)
      throw new IllegalArgumentException(
          "The " + kind + " '" + raw + "' contains a backtick, which is not supported");
    return "`" + raw + "`";
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && mvn -q test -Dtest=MCPToolUtilsTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit (checkpoint - stage and pause)**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/MCPToolUtils.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPToolUtilsTest.java
# Pause here for maintainer review/commit per project git policy.
```

---

### Task 2: `UpsertEntityTool` + shared write helper + registration

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/MCPToolUtils.java` (add `executeParameterizedWrite`)
- Create: `server/src/main/java/com/arcadedb/server/mcp/tools/UpsertEntityTool.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`

**Interfaces:**
- Consumes: `MCPToolUtils.quoteIdentifier(kind, raw)` (Task 1); `MCPToolUtils.resolveDatabase(server, user, name)`; `ExecuteCommandTool.checkPermission(Set<OperationType>, MCPConfiguration)`.
- Produces:
  - `static JSONObject MCPToolUtils.executeParameterizedWrite(Database database, String cypher, Map<String,Object> params, MCPConfiguration config)` — analyzes `cypher` as Cypher, permission-checks the resulting operation types, runs the command with `params` inside a transaction, and returns `{"records":[...], "count":n}`.
  - `static JSONObject UpsertEntityTool.getDefinition()` and `static JSONObject UpsertEntityTool.execute(ArcadeDBServer, ServerSecurityUser, JSONObject, MCPConfiguration)`. Tool name: `upsert_entity`.

- [ ] **Step 1: Write the failing integration tests (HTTP)**

In `MCPServerPluginTest.java`, add these methods (they use the existing `callTool` and `saveMCPConfig` helpers and the default `"graph"` database):

```java
  @Test
  void upsertEntityIsIdempotent() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "Person")
        .put("matchKeys", new JSONObject().put("email", "ada@x.com"))
        .put("setProperties", new JSONObject().put("name", "Ada"));

    final JSONObject first = callTool("upsert_entity", args);
    assertThat(first.getBoolean("isError", true)).isFalse();

    // Second call with identical matchKeys must not create a second node.
    callTool("upsert_entity", new JSONObject(args.toString())
        .put("setProperties", new JSONObject().put("name", "Ada Lovelace")));

    final JSONObject countResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (p:Person {email: 'ada@x.com'}) RETURN count(p) AS c"));
    final JSONObject countPayload = new JSONObject(
        countResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(countPayload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityBindsValuesSoInjectionIsInert() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final String malicious = "x'}) DETACH DELETE n //";
    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "InjTest")
        .put("matchKeys", new JSONObject().put("k", malicious));

    callTool("upsert_entity", args);
    callTool("upsert_entity", new JSONObject(args.toString())); // repeat: still one node

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:InjTest) RETURN count(n) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityRequiresBothInsertAndUpdate() throws Exception {
    // allowUpdate off: a MERGE...SET needs UPDATE, so it must be denied.
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "Person")
        .put("matchKeys", new JSONObject().put("email", "denied@x.com"))
        .put("setProperties", new JSONObject().put("name", "Nope")));

    assertThat(resp.getBoolean("isError")).isTrue();
    final String text = resp.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void upsertEntityRejectsEmptyMatchKeys() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "Person")
        .put("matchKeys", new JSONObject()));

    assertThat(resp.getBoolean("isError")).isTrue();
    final String text = resp.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("matchKeys");
  }
```

Also update the tools/list test (`testToolsList` around line 124): change `isEqualTo(11)` to `isEqualTo(12)`, add `boolean hasUpsertEntity = false;` with the others, add `case "upsert_entity" -> hasUpsertEntity = true;` to the switch, and `assertThat(hasUpsertEntity).isTrue();` after.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && mvn -q test -Dtest=MCPServerPluginTest`
Expected: FAIL - the four new tests error (tool `upsert_entity` unknown → `isError` true with "Unknown tool"), and `testToolsList` fails on the count `11 != 12`.

- [ ] **Step 3: Add the shared write helper**

In `MCPToolUtils.java`, add imports and the method:

```java
import com.arcadedb.database.Database;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.mcp.MCPConfiguration;

import java.util.Map;
```

```java
  /**
   * Executes a parameterized Cypher write and returns its records. The statement is analyzed to determine its
   * operation types, which are gated through the same permission path as {@code execute_command}; a
   * {@code MERGE ... SET} yields {@code {CREATE, UPDATE}}, so both insert and update must be allowed. Values are
   * supplied through {@code params} as bound parameters; identifiers must already be quoted by the caller. The
   * command runs inside a transaction and each result row is serialized with the same configuration the other
   * write/read tools use.
   */
  public static JSONObject executeParameterizedWrite(final Database database, final String cypher,
      final Map<String, Object> params, final MCPConfiguration config) {
    final QueryEngine engine = database.getQueryEngine("cypher");
    final QueryEngine.AnalyzedQuery analyzed = engine.analyze(cypher);
    ExecuteCommandTool.checkPermission(analyzed.getOperationTypes(), config);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray records = new JSONArray();
    database.transaction(() -> {
      try (final ResultSet resultSet = database.command("cypher", cypher, params)) {
        while (resultSet.hasNext())
          records.put(serializer.serializeResult(database, resultSet.next()));
      }
    });

    return new JSONObject().put("records", records).put("count", records.length());
  }
```

- [ ] **Step 4: Create `UpsertEntityTool`**

Create `server/src/main/java/com/arcadedb/server/mcp/tools/UpsertEntityTool.java`:

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
package com.arcadedb.server.mcp.tools;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.HashMap;
import java.util.Map;

public class UpsertEntityTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "upsert_entity")
        .put("description",
            """
            Create or update a single vertex addressed by a match key, without duplicating it. The record is matched \
            (or created) by the 'matchKeys' property:value pairs, then any 'setProperties' are written. Repeated calls \
            with identical 'matchKeys' resolve to the same vertex. Values are bound as parameters, so they are safe to \
            pass verbatim. Requires both insert and update permission.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database"))
                .put("typeName", new JSONObject()
                    .put("type", "string")
                    .put("description", "The vertex type. Created automatically if it does not exist"))
                .put("matchKeys", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs used as the match/merge key; must be non-empty"))
                .put("setProperties", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs to write on the matched or created vertex")))
            .put("required", new JSONArray().put("database").put("typeName").put("matchKeys")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = args.getString("database");
    final String typeName = args.getString("typeName", null);
    if (typeName == null || typeName.isBlank())
      throw new IllegalArgumentException("'typeName' is required");

    final JSONObject matchKeys = args.getJSONObject("matchKeys", null);
    if (matchKeys == null || matchKeys.length() == 0)
      throw new IllegalArgumentException("'matchKeys' is required and must contain at least one property");

    final JSONObject setProperties = args.getJSONObject("setProperties", null);

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    final Map<String, Object> params = new HashMap<>();
    final StringBuilder cypher = new StringBuilder("MERGE (n:")
        .append(MCPToolUtils.quoteIdentifier("type name", typeName))
        .append(" {");

    int m = 0;
    for (final String key : matchKeys.keySet()) {
      if (m > 0)
        cypher.append(", ");
      final String p = "m" + m;
      cypher.append(MCPToolUtils.quoteIdentifier("match key", key)).append(": $").append(p);
      params.put(p, matchKeys.get(key));
      m++;
    }
    cypher.append("})");

    if (setProperties != null && setProperties.length() > 0) {
      cypher.append(" SET ");
      int s = 0;
      for (final String key : setProperties.keySet()) {
        if (s > 0)
          cypher.append(", ");
        final String p = "s" + s;
        cypher.append("n.").append(MCPToolUtils.quoteIdentifier("property key", key)).append(" = $").append(p);
        params.put(p, setProperties.get(key));
        s++;
      }
    }
    cypher.append(" RETURN n");

    return MCPToolUtils.executeParameterizedWrite(database, cypher.toString(), params, config);
  }
}
```

- [ ] **Step 5: Register in both transports**

In `MCPHttpHandler.java`: add `import com.arcadedb.server.mcp.tools.UpsertEntityTool;`; add `TOOLS_LIST.put(UpsertEntityTool.getDefinition());` after the `FullTextSearchTool` line in the static block; add `case "upsert_entity" -> UpsertEntityTool.execute(server, user, args, config);` to the `tools/call` dispatch switch; add `case "upsert_entity" -> result.getInt("count", 0) + " record(s)";` to the `formatResult` switch.

In `MCPStdioServer.java`: add `import com.arcadedb.server.mcp.tools.UpsertEntityTool;`; add `TOOLS_LIST.put(UpsertEntityTool.getDefinition());` after the `FullTextSearchTool` line; add `case "upsert_entity" -> UpsertEntityTool.execute(server, user, args, config);` to the dispatch switch.

- [ ] **Step 6: Update the stdio tool-count assertion**

In `MCPStdioServerTest.java` (line ~77): change `assertThat(tools.length()).isEqualTo(11);` to `isEqualTo(12);`.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cd server && mvn -q test -Dtest=MCPServerPluginTest,MCPStdioServerTest,MCPToolUtilsTest,MCPPermissionsTest`
Expected: PASS. The four new `upsertEntity*` tests pass, `testToolsList` sees 12 tools, stdio sees 12, and the pre-existing `MCPPermissionsTest.upsertRequiresBothCreateAndUpdate` stays green.

- [ ] **Step 8: Commit (checkpoint - stage and pause)**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/MCPToolUtils.java \
        server/src/main/java/com/arcadedb/server/mcp/tools/UpsertEntityTool.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java
# Pause here for maintainer review/commit per project git policy.
```

---

### Task 3: `UpsertRelationshipTool` + registration

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/mcp/tools/UpsertRelationshipTool.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`
- Test: `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`

**Interfaces:**
- Consumes: `MCPToolUtils.quoteIdentifier(...)`, `MCPToolUtils.resolveDatabase(...)`, `MCPToolUtils.executeParameterizedWrite(...)` (all from Tasks 1-2).
- Produces: `static JSONObject UpsertRelationshipTool.getDefinition()` and `static JSONObject UpsertRelationshipTool.execute(ArcadeDBServer, ServerSecurityUser, JSONObject, MCPConfiguration)`. Tool name: `upsert_relationship`.

- [ ] **Step 1: Write the failing integration tests (HTTP)**

In `MCPServerPluginTest.java`, add:

```java
  @Test
  void upsertRelationshipDoesNotDuplicateEdge() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("fromType", "Author")
        .put("fromMatchKeys", new JSONObject().put("name", "Ada"))
        .put("toType", "Book")
        .put("toMatchKeys", new JSONObject().put("isbn", "111"))
        .put("relType", "WROTE")
        .put("relProperties", new JSONObject().put("year", 1843));

    final JSONObject first = callTool("upsert_relationship", args);
    assertThat(first.getBoolean("isError", true)).isFalse();

    // Repeat with a different property value: the edge must be updated, not duplicated.
    callTool("upsert_relationship", new JSONObject(args.toString())
        .put("relProperties", new JSONObject().put("year", 1844)));

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (:Author {name:'Ada'})-[r:WROTE]->(:Book {isbn:'111'}) RETURN count(r) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertRelationshipAutoCreatesEndpoints() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    callTool("upsert_relationship", new JSONObject()
        .put("database", "graph")
        .put("fromType", "City")
        .put("fromMatchKeys", new JSONObject().put("name", "Turin"))
        .put("toType", "Country")
        .put("toMatchKeys", new JSONObject().put("name", "Italy"))
        .put("relType", "IN_COUNTRY"));

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (c:City {name:'Turin'})-[:IN_COUNTRY]->(n:Country {name:'Italy'}) RETURN count(*) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertRelationshipDeniedWithoutInsert() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", false)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_relationship", new JSONObject()
        .put("database", "graph")
        .put("fromType", "Author")
        .put("fromMatchKeys", new JSONObject().put("name", "X"))
        .put("toType", "Book")
        .put("toMatchKeys", new JSONObject().put("isbn", "999"))
        .put("relType", "WROTE"));

    assertThat(resp.getBoolean("isError")).isTrue();
    assertThat(resp.getJSONArray("content").getJSONObject(0).getString("text")).contains("not allowed");
  }
```

Also update `testToolsList`: change `isEqualTo(12)` to `isEqualTo(13)`, add `boolean hasUpsertRelationship = false;`, add `case "upsert_relationship" -> hasUpsertRelationship = true;`, and `assertThat(hasUpsertRelationship).isTrue();`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && mvn -q test -Dtest=MCPServerPluginTest`
Expected: FAIL - new `upsertRelationship*` tests error on unknown tool, and `testToolsList` fails on `12 != 13`.

- [ ] **Step 3: Create `UpsertRelationshipTool`**

Create `server/src/main/java/com/arcadedb/server/mcp/tools/UpsertRelationshipTool.java`:

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
package com.arcadedb.server.mcp.tools;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.HashMap;
import java.util.Map;

public class UpsertRelationshipTool {

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "upsert_relationship")
        .put("description",
            """
            Create or update a single directed edge between two vertices, without duplicating it. Each endpoint is \
            matched (or created) by its match keys, then the edge of type 'relType' between them is matched (or created) \
            and any 'relProperties' are written. Repeated calls between the same resolved endpoints with the same \
            'relType' resolve to the same edge. Values are bound as parameters. Requires both insert and update \
            permission.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database"))
                .put("fromType", new JSONObject()
                    .put("type", "string")
                    .put("description", "The source vertex type. Created automatically if it does not exist"))
                .put("fromMatchKeys", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs identifying the source vertex; must be non-empty"))
                .put("toType", new JSONObject()
                    .put("type", "string")
                    .put("description", "The destination vertex type. Created automatically if it does not exist"))
                .put("toMatchKeys", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs identifying the destination vertex; must be non-empty"))
                .put("relType", new JSONObject()
                    .put("type", "string")
                    .put("description", "The edge type. Created automatically if it does not exist"))
                .put("relProperties", new JSONObject()
                    .put("type", "object")
                    .put("description", "property:value pairs to write on the matched or created edge")))
            .put("required", new JSONArray()
                .put("database").put("fromType").put("fromMatchKeys")
                .put("toType").put("toMatchKeys").put("relType")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    final String databaseName = args.getString("database");
    final String fromType = require(args, "fromType");
    final String toType = require(args, "toType");
    final String relType = require(args, "relType");

    final JSONObject fromMatchKeys = requireKeys(args, "fromMatchKeys");
    final JSONObject toMatchKeys = requireKeys(args, "toMatchKeys");
    final JSONObject relProperties = args.getJSONObject("relProperties", null);

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    final Map<String, Object> params = new HashMap<>();
    final StringBuilder cypher = new StringBuilder();

    appendNodeMerge(cypher, params, "a", fromType, fromMatchKeys, "f");
    cypher.append('\n');
    appendNodeMerge(cypher, params, "b", toType, toMatchKeys, "t");
    cypher.append('\n')
        .append("MERGE (a)-[r:")
        .append(MCPToolUtils.quoteIdentifier("relationship type", relType))
        .append("]->(b)");

    if (relProperties != null && relProperties.length() > 0) {
      cypher.append(" SET ");
      int r = 0;
      for (final String key : relProperties.keySet()) {
        if (r > 0)
          cypher.append(", ");
        final String p = "r" + r;
        cypher.append("r.").append(MCPToolUtils.quoteIdentifier("relationship property key", key))
            .append(" = $").append(p);
        params.put(p, relProperties.get(key));
        r++;
      }
    }
    cypher.append(" RETURN r");

    return MCPToolUtils.executeParameterizedWrite(database, cypher.toString(), params, config);
  }

  private static void appendNodeMerge(final StringBuilder cypher, final Map<String, Object> params,
      final String variable, final String typeName, final JSONObject matchKeys, final String paramPrefix) {
    cypher.append("MERGE (").append(variable).append(':')
        .append(MCPToolUtils.quoteIdentifier("type name", typeName)).append(" {");
    int i = 0;
    for (final String key : matchKeys.keySet()) {
      if (i > 0)
        cypher.append(", ");
      final String p = paramPrefix + i;
      cypher.append(MCPToolUtils.quoteIdentifier("match key", key)).append(": $").append(p);
      params.put(p, matchKeys.get(key));
      i++;
    }
    cypher.append("})");
  }

  private static String require(final JSONObject args, final String field) {
    final String value = args.getString(field, null);
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("'" + field + "' is required");
    return value;
  }

  private static JSONObject requireKeys(final JSONObject args, final String field) {
    final JSONObject keys = args.getJSONObject(field, null);
    if (keys == null || keys.length() == 0)
      throw new IllegalArgumentException("'" + field + "' is required and must contain at least one property");
    return keys;
  }
}
```

- [ ] **Step 4: Register in both transports**

In `MCPHttpHandler.java`: add `import com.arcadedb.server.mcp.tools.UpsertRelationshipTool;`; add `TOOLS_LIST.put(UpsertRelationshipTool.getDefinition());` after the `UpsertEntityTool` line; add `case "upsert_relationship" -> UpsertRelationshipTool.execute(server, user, args, config);` to the dispatch switch; add `case "upsert_relationship" -> result.getInt("count", 0) + " record(s)";` to `formatResult` (or fold into the existing `upsert_entity` arm: `case "upsert_entity", "upsert_relationship" -> ...`).

In `MCPStdioServer.java`: add the import, the `TOOLS_LIST.put(...)` line, and the dispatch `case`.

- [ ] **Step 5: Update the stdio tool-count assertion**

In `MCPStdioServerTest.java` (line ~77): change `isEqualTo(12)` to `isEqualTo(13)`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd server && mvn -q test -Dtest=MCPServerPluginTest,MCPStdioServerTest,MCPPermissionsTest,MCPToolUtilsTest`
Expected: PASS. New relationship tests pass, `testToolsList` sees 13, stdio sees 13.

- [ ] **Step 7: Run the full MCP test set to confirm nothing regressed**

Run: `cd server && mvn -q test -Dtest='MCP*'`
Expected: PASS across `MCPConfigurationTest`, `MCPPermissionsTest`, `MCPServerPluginTest`, `MCPStdioServerTest`, `MCPToolUtilsTest`.

- [ ] **Step 8: Commit (checkpoint - stage and pause)**

```bash
git add server/src/main/java/com/arcadedb/server/mcp/tools/UpsertRelationshipTool.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPHttpHandler.java \
        server/src/main/java/com/arcadedb/server/mcp/MCPStdioServer.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java \
        server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java
# Pause here for maintainer review/commit per project git policy.
```

---

## Verification Checklist (whole feature)

- [ ] `cd server && mvn -q test -Dtest='MCP*'` is green.
- [ ] `upsert_entity` and `upsert_relationship` appear in both HTTP and stdio `tools/list`; both count assertions read 13.
- [ ] Idempotency: repeated `upsert_entity` with identical `matchKeys` yields one node; repeated `upsert_relationship` between the same endpoints yields one edge.
- [ ] Injection value in `matchKeys` is stored literally, deletes nothing, creates one node.
- [ ] With `allowUpdate=false` (entity) or `allowInsert=false` (relationship), the call is denied with "not allowed".
- [ ] Empty `matchKeys` / missing required field is rejected with a message naming the field.
- [ ] No `System.out` debug left behind; no Claude attribution anywhere.

## Notes for the implementer

- **Test isolation:** `MCPServerPluginTest` extends `BaseGraphServerTest`, whose default database is `"graph"`. The new tests create fresh types (`Person`, `InjTest`, `Author`, `Book`, `City`, `Country`) via `MERGE` auto-creation; they do not collide with the `Article` full-text seed. If a type name clashes with another test's data on a shared server instance, rename it - the assertions count only records matching their own keys, so cross-test bleed is unlikely but rename on any surprise.
- **Why `serializeResult` and not a manual element extract:** a Cypher `RETURN n` / `RETURN r` row is serialized flat (just `@rid`, `@type`, properties) by `JsonSerializer.serializeResult`, exactly as `query`/`execute_command` already emit. No special-casing is needed to get the record-shaped output the spec shows.
- **Permission precision:** an upsert with no `setProperties`/`relProperties` produces a `MERGE` with no `SET`, which may analyze to `{CREATE}` alone and then require only `allowInsert`. This is intended (the analyzer is the source of truth); the "requires both" tests deliberately include `setProperties`/`relProperties` so the `UPDATE` op is present.
- **Merge-conflict hotspots:** the `TOOLS_LIST` blocks, dispatch switches, and count assertions in `MCPHttpHandler`/`MCPStdioServer`/`MCPServerPluginTest`/`MCPStdioServerTest` are edited by every Wave-1 sibling. On rebase, reconcile the absolute count (always "+2 for these two tools") rather than assuming 11→13.
```
