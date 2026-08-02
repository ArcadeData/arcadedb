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
package com.arcadedb.mcp;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MCPServerPluginTest extends BaseGraphServerTest {

  private String getMcpUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/mcp";
  }

  private String getMcpConfigUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/mcp/config";
  }

  @BeforeEach
  void enableMCP() throws Exception {
    // MCP is disabled by default, enable it for tests
    final JSONObject config = new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("profile", "all")
        .put("allowedUsers", new JSONArray().put("root"));
    config.put("principalProfiles", (Object) null);
    saveMCPConfig(config);
    seedFullTextIndex();
    seedSampleRecords();
  }

  private void seedFullTextIndex() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("Article"))
      return;

    db.transaction(() -> {
      // Keep this MCP fixture on one bucket; the engine's explicit multi-bucket BM25 regression covers global score comparability.
      db.command("sql", "CREATE DOCUMENT TYPE Article BUCKETS 1");
      db.command("sql", "CREATE PROPERTY Article.title STRING");
      db.command("sql", "CREATE PROPERTY Article.content STRING");
      db.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");
      db.command("sql", "CREATE INDEX ON Article (title) UNIQUE");

      db.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      db.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'python scripting language'");
      // All three documents tokenize to exactly three terms, so BM25 length normalization is identical across them.
      // Doc3 therefore outranks Doc1 and Doc2 purely on term frequency for 'language' (3 occurrences against 1),
      // which gives the ranking test an unambiguous top hit. Keep the three lengths equal when editing this seed.
      db.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'language language language'");

      // A second full-text index on Article, so 'typeName: Article' alone is ambiguous between the two.
      db.command("sql", "CREATE INDEX ON Article (title, content) FULL_TEXT");

      // The schema strips spaces when deriving an index name, so a property named 'my prop' yields Spaced[myprop].
      db.command("sql", "CREATE DOCUMENT TYPE Spaced BUCKETS 1");
      db.command("sql", "CREATE PROPERTY Spaced.`my prop` STRING");
      db.command("sql", "CREATE INDEX ON Spaced (`my prop`) FULL_TEXT");
      db.command("sql", "INSERT INTO Spaced SET `my prop` = 'java tooling'");

      // A full-text index declared on a supertype is named for the supertype and still returns subtype records.
      db.command("sql", "CREATE DOCUMENT TYPE Searchable");
      db.command("sql", "CREATE PROPERTY Searchable.text STRING");
      db.command("sql", "CREATE INDEX ON Searchable (text) FULL_TEXT");
      db.command("sql", "CREATE DOCUMENT TYPE Decision EXTENDS Searchable");
      db.command("sql", "INSERT INTO Decision SET text = 'approved the java migration'");
    });
  }

  private void seedSampleRecords() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("McpSampleRecord"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE McpSampleRecord");
      db.command("sql", "CREATE PROPERTY McpSampleRecord.ordinal INTEGER");
      db.command("sql", "CREATE PROPERTY McpSampleRecord.label STRING");
      for (int i = 1; i <= 5; i++)
        db.command("sql", "INSERT INTO McpSampleRecord SET ordinal = ?, label = ?", i, "sample-" + i);

      db.command("sql", "CREATE DOCUMENT TYPE McpEmptySample");
      db.command("sql", "CREATE PROPERTY McpEmptySample.value STRING");

      db.command("sql", "CREATE EDGE TYPE McpSampleEdge");
      for (int i = 1; i <= 21; i++)
        db.command("sql", "CREATE DOCUMENT TYPE ZzMcpDefaultSample" + i);
    });
  }

  private void seedVectorIndexes() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("McpVectorRecord"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE McpVectorRecord BUCKETS 1");
      db.command("sql", "CREATE PROPERTY McpVectorRecord.name STRING");
      db.command("sql", "CREATE PROPERTY McpVectorRecord.category STRING");
      db.command("sql", "CREATE PROPERTY McpVectorRecord.embedding ARRAY_OF_FLOATS");
      db.command("sql", """
          CREATE INDEX ON McpVectorRecord (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE' }
          """);

      db.newDocument("McpVectorRecord")
          .set("name", "dense-a")
          .set("category", "keep")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f })
          .save();
      db.newDocument("McpVectorRecord")
          .set("name", "dense-b")
          .set("category", "drop")
          .set("embedding", new float[] { 0.9f, 0.1f, 0.0f })
          .save();
      db.newDocument("McpVectorRecord")
          .set("name", "dense-c")
          .set("category", "keep")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f })
          .save();

      db.command("sql", "CREATE DOCUMENT TYPE McpSparseVectorRecord BUCKETS 1");
      db.command("sql", "CREATE PROPERTY McpSparseVectorRecord.name STRING");
      db.command("sql", "CREATE PROPERTY McpSparseVectorRecord.tokens ARRAY_OF_INTEGERS");
      db.command("sql", "CREATE PROPERTY McpSparseVectorRecord.weights ARRAY_OF_FLOATS");
      db.command("sql", """
          CREATE INDEX ON McpSparseVectorRecord (tokens, weights) LSM_SPARSE_VECTOR
          METADATA { dimensions: 8, weightQuantization: 'FP32' }
          """);

      db.newDocument("McpSparseVectorRecord")
          .set("name", "sparse-low")
          .set("tokens", new int[] { 1, 5 })
          .set("weights", new float[] { 0.1f, 0.3f })
          .save();
      db.newDocument("McpSparseVectorRecord")
          .set("name", "sparse-high")
          .set("tokens", new int[] { 2, 5 })
          .set("weights", new float[] { 0.2f, 0.6f })
          .save();
    });
  }

  private void seedHybridGraph() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("McpHybridDoc"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE VERTEX TYPE McpHybridDoc BUCKETS 1");
      db.command("sql", "CREATE PROPERTY McpHybridDoc.title STRING");
      db.command("sql", "CREATE PROPERTY McpHybridDoc.content STRING");
      db.command("sql", "CREATE PROPERTY McpHybridDoc.embedding ARRAY_OF_FLOATS");
      db.command("sql", """
          CREATE INDEX ON McpHybridDoc (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE' }
          """);
      db.command("sql", "CREATE INDEX ON McpHybridDoc (content) FULL_TEXT");
      db.command("sql", "CREATE EDGE TYPE McpHybridCites");
      db.command("sql", "CREATE EDGE TYPE McpHybridMentions");

      // h0 is the nearest neighbor of the probe vector and the root of the citation chain.
      // h5 is the strongest full-text match for 'gearbox' and is not reachable from h0 at all,
      // so the full-text leg and the expansion leg contribute disjoint rows.
      final MutableVertex h0 = db.newVertex("McpHybridDoc").set("title", "h0")
          .set("content", "graph traversal over connected documents")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      final MutableVertex h1 = db.newVertex("McpHybridDoc").set("title", "h1")
          .set("content", "vector similarity ranking")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      final MutableVertex h2 = db.newVertex("McpHybridDoc").set("title", "h2")
          .set("content", "reciprocal rank fusion")
          .set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save();
      final MutableVertex h3 = db.newVertex("McpHybridDoc").set("title", "h3")
          .set("content", "breadth first expansion")
          .set("embedding", new float[] { 0.5f, 0.5f, 0.0f }).save();
      final MutableVertex h4 = db.newVertex("McpHybridDoc").set("title", "h4")
          .set("content", "unrelated mention target")
          .set("embedding", new float[] { 0.0f, 0.5f, 0.5f }).save();
      final MutableVertex h5 = db.newVertex("McpHybridDoc").set("title", "h5")
          .set("content", "gearbox gearbox gearbox")
          .set("embedding", new float[] { 0.1f, 0.1f, 0.9f }).save();

      // Chain h0 -> h1 -> h2 -> h3, plus a shortcut h0 -> h2 so h2 is reachable two ways,
      // plus h0 -> h4 on a different edge type so edge filtering is observable.
      h0.newEdge("McpHybridCites", h1).save();
      h1.newEdge("McpHybridCites", h2).save();
      h2.newEdge("McpHybridCites", h3).save();
      h0.newEdge("McpHybridCites", h2).save();
      h0.newEdge("McpHybridMentions", h4).save();
      // h5 is deliberately left unconnected.
      assertThat(h5.getIdentity()).isNotNull();
    });
  }

  private static JSONArray probeVector() {
    return new JSONArray().put(1.0).put(0.0).put(0.0);
  }

  private static JSONObject payloadOf(final JSONObject response) {
    assertThat(response.getBoolean("isError", true)).isFalse();
    return new JSONObject(response.getJSONArray("content").getJSONObject(0).getString("text"));
  }

  /**
   * Seeds a type holding more vectors than a filtered search inspects, so truncation reporting can be observed
   * on a candidate window that is genuinely smaller than the index. Exactly one record carries category 'solo'
   * and sits close to the probe vector, so a filtered search for it returns fewer hits than requested while
   * still having examined every match the filter can ever produce.
   */
  private void seedVectorBudgetRecords() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("McpVectorBudgetRecord"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE McpVectorBudgetRecord BUCKETS 1");
      db.command("sql", "CREATE PROPERTY McpVectorBudgetRecord.name STRING");
      db.command("sql", "CREATE PROPERTY McpVectorBudgetRecord.category STRING");
      db.command("sql", "CREATE PROPERTY McpVectorBudgetRecord.embedding ARRAY_OF_FLOATS");
      db.command("sql", """
          CREATE INDEX ON McpVectorBudgetRecord (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE' }
          """);

      db.newDocument("McpVectorBudgetRecord")
          .set("name", "budget-solo")
          .set("category", "solo")
          .set("embedding", new float[] { 0.95f, 0.05f, 0.0f })
          .save();

      // Comfortably more than the 16-candidate window a k=2 filtered search uses, so the index is larger than
      // the window regardless of which neighbors the graph returns.
      for (int i = 0; i < 24; i++)
        db.newDocument("McpVectorBudgetRecord")
            .set("name", "budget-bulk-" + i)
            .set("category", "bulk")
            .set("embedding", new float[] { 0.9f - i * 0.01f, 0.1f + i * 0.01f, 0.0f })
            .save();
    });
  }

  @Test
  void initialize() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", "initialize")
        .put("params", new JSONObject()));

    assertThat(response.has("result")).isTrue();
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getString("protocolVersion")).isNotEmpty();
    assertThat(result.getJSONObject("serverInfo").getString("name")).isEqualTo("arcadedb");
    assertThat(result.getJSONObject("capabilities").has("tools")).isTrue();
  }

  @Test
  void toolsList() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 2)
        .put("method", "tools/list")
        .put("params", new JSONObject()));

    assertThat(response.has("result")).isTrue();
    final JSONArray tools = response.getJSONObject("result").getJSONArray("tools");
    assertThat(tools.length()).isPositive();

    // Verify tool names
    boolean hasListDatabases = false;
    boolean hasGetSchema = false;
    boolean hasQuery = false;
    boolean hasExecuteCommand = false;
    boolean hasServerStatus = false;
    boolean hasProfilerStart = false;
    boolean hasProfilerStop = false;
    boolean hasProfilerStatus = false;
    boolean hasGetServerSettings = false;
    boolean hasSetServerSetting = false;
    boolean hasSampleRecords = false;
    boolean hasVectorSearch = false;
    boolean hasFullTextSearch = false;
    boolean hasUpsertEntity = false;
    boolean hasUpsertRelationship = false;

    for (int i = 0; i < tools.length(); i++) {
      final String name = tools.getJSONObject(i).getString("name");
      switch (name) {
      case "list_databases" -> hasListDatabases = true;
      case "get_schema" -> hasGetSchema = true;
      case "query" -> hasQuery = true;
      case "execute_command" -> hasExecuteCommand = true;
      case "server_status" -> hasServerStatus = true;
      case "profiler_start" -> hasProfilerStart = true;
      case "profiler_stop" -> hasProfilerStop = true;
      case "profiler_status" -> hasProfilerStatus = true;
      case "get_server_settings" -> hasGetServerSettings = true;
      case "set_server_setting" -> hasSetServerSetting = true;
      case "sample_records" -> hasSampleRecords = true;
      case "vector_search" -> hasVectorSearch = true;
      case "full_text_search" -> hasFullTextSearch = true;
      case "upsert_entity" -> hasUpsertEntity = true;
      case "upsert_relationship" -> hasUpsertRelationship = true;
      }
    }
    assertThat(hasListDatabases).isTrue();
    assertThat(hasGetSchema).isTrue();
    assertThat(hasQuery).isTrue();
    assertThat(hasExecuteCommand).isTrue();
    assertThat(hasServerStatus).isTrue();
    assertThat(hasProfilerStart).isTrue();
    assertThat(hasProfilerStop).isTrue();
    assertThat(hasProfilerStatus).isTrue();
    assertThat(hasGetServerSettings).isTrue();
    assertThat(hasSetServerSetting).isTrue();
    assertThat(hasSampleRecords).isTrue();
    assertThat(hasVectorSearch).isTrue();
    assertThat(hasFullTextSearch).isTrue();
    assertThat(hasUpsertEntity).isTrue();
    assertThat(hasUpsertRelationship).isTrue();
  }

  @Test
  void toolProfilesFilterDiscoveryAndExecution() throws Exception {
    saveMCPConfig(new JSONObject().put("profile", "rag"));

    JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 20)
        .put("method", "tools/list")
        .put("params", new JSONObject()));
    assertThat(toolNames(response))
        .contains("list_databases", "get_schema", "query", "sample_records", "vector_search", "full_text_search",
            "upsert_entity", "upsert_relationship")
        .doesNotContain("server_status", "execute_command");

    JSONObject denied = callTool("server_status", new JSONObject());
    assertThat(denied.getBoolean("isError", false)).isTrue();
    assertThat(denied.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("server_status").contains("rag");

    final JSONObject allowed = callTool("query", new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT FROM V1 LIMIT 1"));
    assertThat(allowed.getBoolean("isError", true)).isFalse();

    saveMCPConfig(new JSONObject().put("profile", "admin"));
    response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 21)
        .put("method", "tools/list")
        .put("params", new JSONObject()));
    assertThat(toolNames(response))
        .contains("list_databases", "get_schema", "query", "execute_command", "server_status",
            "profiler_start", "profiler_stop", "profiler_status", "get_server_settings", "set_server_setting")
        .doesNotContain("sample_records", "vector_search", "full_text_search");

    denied = callTool("full_text_search", new JSONObject());
    assertThat(denied.getBoolean("isError", false)).isTrue();
    assertThat(denied.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("full_text_search").contains("admin");

    final JSONObject adminAllowed = callTool("server_status", new JSONObject());
    assertThat(adminAllowed.getBoolean("isError", true)).isFalse();
  }

  @Test
  void principalProfilesDifferentiateNamedUsersOnOneHttpEndpoint() throws Exception {
    final String principalName = "mcp-profile-reader";
    final String password = "principalProfilePass1!";
    if (getServer(0).getSecurity().existsUser(principalName))
      getServer(0).getSecurity().dropUser(principalName);
    getServer(0).getSecurity().createUser(new JSONObject()
        .put("name", principalName)
        .put("password", getServer(0).getSecurity().encodePassword(password))
        .put("databases", new JSONObject().put("graph", new JSONArray().put("admin"))));

    try {
      saveMCPConfig(new JSONObject()
          .put("profile", "all")
          .put("allowedUsers", new JSONArray().put("root").put(principalName))
          .put("principalProfiles", new JSONObject().put(principalName, "rag")));

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 22)
          .put("method", "tools/list")
          .put("params", new JSONObject());
      assertThat(toolNames(mcpRequest(request))).contains("server_status", "execute_command");

      final String principalAuth = getBasicAuth(principalName, password);
      assertThat(toolNames(mcpRequest(request, principalAuth)))
          .contains("sample_records", "vector_search", "full_text_search")
          .doesNotContain("server_status", "execute_command");

      final JSONObject denied = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 23)
          .put("method", "tools/call")
          .put("params", new JSONObject()
              .put("name", "server_status")
              .put("arguments", new JSONObject())), principalAuth)
          .getJSONObject("result");
      assertThat(denied.getBoolean("isError", false)).isTrue();

      final JSONObject initialized = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 24)
          .put("method", "initialize")
          .put("params", new JSONObject()), principalAuth);
      assertThat(initialized.getJSONObject("result").getString("instructions"))
          .contains("retrieval and agent memory");
    } finally {
      getServer(0).getSecurity().dropUser(principalName);
    }
  }

  @Test
  void listDatabases() throws Exception {
    final JSONObject response = callTool("list_databases", new JSONObject());

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("databases")).isTrue();
    final JSONArray databases = result.getJSONArray("databases");
    // The test creates a "graph" database
    boolean foundGraph = false;
    for (int i = 0; i < databases.length(); i++)
      if ("graph".equals(databases.getString(i)))
        foundGraph = true;
    assertThat(foundGraph).isTrue();
  }

  @Test
  void getSchema() throws Exception {
    final JSONObject response = callTool("get_schema", new JSONObject().put("database", "graph"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getString("database")).isEqualTo("graph");
    assertThat(result.has("types")).isTrue();

    // Should have V1, V2, E1, E2, Person types from BaseGraphServerTest
    final JSONArray types = result.getJSONArray("types");
    assertThat(types.length()).isGreaterThanOrEqualTo(5);

    boolean foundV1 = false;
    for (int i = 0; i < types.length(); i++) {
      final JSONObject type = types.getJSONObject(i);
      if ("V1".equals(type.getString("name"))) {
        foundV1 = true;
        assertThat(type.getString("category")).isEqualTo("vertex");
        // V1 has an "id" property and an index
        assertThat(type.has("properties")).isTrue();
        assertThat(type.has("indexes")).isTrue();
      }
    }
    assertThat(foundV1).isTrue();
  }

  @Test
  void sampleRecordsUsesRequestedTypesAndPerTypeLimit() throws Exception {
    final JSONObject response = callTool("sample_records", new JSONObject()
        .put("database", getDatabaseName())
        .put("types", new JSONArray()
            .put("McpSampleRecord")
            .put("McpEmptySample")
            .put("McpSampleRecord"))
        .put("limit", 2));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));
    final JSONObject samples = payload.getJSONObject("samples");

    assertThat(samples.keySet()).containsExactlyInAnyOrder("McpSampleRecord", "McpEmptySample");
    assertThat(samples.getJSONArray("McpSampleRecord").length()).isEqualTo(2);
    assertThat(samples.getJSONArray("McpSampleRecord").getJSONObject(0).has("ordinal")).isTrue();
    assertThat(samples.getJSONArray("McpEmptySample").length()).isZero();
    assertThat(payload.getInt("sampledTypes")).isEqualTo(2);
    assertThat(payload.getInt("availableTypes")).isEqualTo(availableSampleTypeCount());
    assertThat(payload.getInt("recordsReturned")).isEqualTo(2);
    assertThat(payload.getBoolean("truncated")).isFalse();
  }

  @Test
  void sampleRecordsDefaultsToBoundedNonEdgeTypes() throws Exception {
    final JSONObject response = callTool("sample_records", new JSONObject()
        .put("database", getDatabaseName()));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));
    final JSONObject samples = payload.getJSONObject("samples");

    assertThat(samples.has("McpSampleRecord")).isTrue();
    assertThat(samples.getJSONArray("McpSampleRecord").length()).isEqualTo(3);
    assertThat(samples.has("McpEmptySample")).isTrue();
    assertThat(samples.getJSONArray("McpEmptySample").length()).isZero();
    assertThat(samples.has("McpSampleEdge")).isFalse();
    assertThat(payload.getInt("sampledTypes")).isEqualTo(20);
    assertThat(payload.getInt("availableTypes")).isEqualTo(availableSampleTypeCount());
    assertThat(payload.getInt("availableTypes")).isGreaterThan(20);
    assertThat(payload.getBoolean("truncated")).isTrue();
    assertThat(payload.getInt("recordsReturned")).isEqualTo(
        samples.keySet().stream().mapToInt(name -> samples.getJSONArray(name).length()).sum());
  }

  @Test
  void sampleRecordsAllowsExplicitEdgeTypes() throws Exception {
    final JSONObject response = callTool("sample_records", new JSONObject()
        .put("database", getDatabaseName())
        .put("types", new JSONArray().put("McpSampleEdge")));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONObject("samples").has("McpSampleEdge")).isTrue();
    assertThat(payload.getInt("sampledTypes")).isEqualTo(1);
    assertThat(payload.getInt("availableTypes")).isEqualTo(availableSampleTypeCount());
  }

  private int availableSampleTypeCount() {
    return Math.toIntExact(getServerDatabase(0, getDatabaseName()).getSchema().getTypes().stream()
        .filter(type -> !(type instanceof EdgeType))
        .count());
  }

  @Test
  void sampleRecordsRejectsMoreThanTwentyRequestedTypes() throws Exception {
    final JSONArray types = new JSONArray();
    for (int i = 0; i <= 20; i++)
      types.put("McpSampleRecord");

    final JSONObject response = callTool("sample_records", new JSONObject()
        .put("database", getDatabaseName())
        .put("types", types));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("types").contains("at most 20");
  }

  @Test
  void sampleRecordsRejectsOutOfRangeLimits() throws Exception {
    for (final int invalidLimit : new int[] { -1, 0, 21 }) {
      final JSONObject response = callTool("sample_records", new JSONObject()
          .put("database", getDatabaseName())
          .put("types", new JSONArray().put("McpSampleRecord"))
          .put("limit", invalidLimit));

      assertThat(response.getBoolean("isError", false)).isTrue();
      final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
      assertThat(errorText).contains("limit").contains("1").contains("20");
    }
  }

  @Test
  void sampleRecordsRejectsUnknownTypesBeforeQueryExecution() throws Exception {
    final JSONObject response = callTool("sample_records", new JSONObject()
        .put("database", getDatabaseName())
        .put("types", new JSONArray().put("McpSampleRecord` LIMIT 20")));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("does not exist").contains("McpSampleRecord");
  }

  @Test
  void sampleRecordsDeniedWhenReadsDisabled() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("sample_records", new JSONObject()
        .put("database", getDatabaseName())
        .put("types", new JSONArray().put("McpSampleRecord")));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("not allowed");
  }

  @Test
  void query() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("query", "SELECT FROM V1"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("records")).isTrue();
    assertThat(result.getInt("count")).isGreaterThan(0);
  }

  @Test
  void executeCommand() throws Exception {
    // Enable insert permission
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("execute_command", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("command", "INSERT INTO V1 SET id = 999, name = 'mcpTest'"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getInt("count")).isGreaterThan(0);
  }

  @Test
  void executeCommandDeniedByPermission() throws Exception {
    // Ensure insert is disabled
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("execute_command", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("command", "INSERT INTO V1 SET id = 998, name = 'shouldFail'"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void serverStatus() throws Exception {
    final JSONObject response = callTool("server_status", new JSONObject());

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("version")).isTrue();
    assertThat(result.has("serverName")).isTrue();
    assertThat(result.has("databases")).isTrue();
  }

  @Test
  void ping() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 99)
        .put("method", "ping")
        .put("params", new JSONObject()));

    assertThat(response.has("result")).isTrue();
  }

  @Test
  void methodNotFound() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 100)
        .put("method", "nonexistent/method")
        .put("params", new JSONObject()));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32601);
  }

  @Test
  void disabledMCP() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 101)
        .put("method", "initialize")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(503);
    } finally {
      connection.disconnect();
    }

    // Re-enable for other tests
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root")));
  }

  @Test
  void getConfig() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpConfigUrl()).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      final JSONObject config = new JSONObject(body);
      assertThat(config.has("enabled")).isTrue();
      assertThat(config.has("allowReads")).isTrue();
      assertThat(config.getString("profile")).isEqualTo("all");
      assertThat(config.has("allowedUsers")).isTrue();
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void invalidConfigTypeReturnsBadRequest() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpConfigUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final byte[] data = new JSONObject().put("enabled", "yes").toString().getBytes(StandardCharsets.UTF_8);
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }

    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      assertThat(FileUtils.readStreamAsString(connection.getErrorStream(), "utf8"))
          .contains("enabled").contains("boolean");
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void unknownTool() throws Exception {
    final JSONObject response = callTool("nonexistent_tool", new JSONObject());
    assertThat(response.getBoolean("isError")).isTrue();
  }

  @Test
  void notificationReturns202() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("method", "notifications/initialized")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      // MCP 2025-03-26 requires 202 Accepted (not 204) for a POST that carried only notifications.
      assertThat(connection.getResponseCode()).isEqualTo(202);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void queryToolRejectsWriteQuery() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("query", "INSERT INTO V1 SET id = 9999, name = 'shouldFail'"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("write operations");
  }

  @Test
  void unauthorizedUserDenied() throws Exception {
    // Configure only "root" as allowed user
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Create a non-root user
    if (!getServer(0).getSecurity().existsUser("mcpuser"))
      getServer(0).getSecurity().createUser("mcpuser", "mcppassword");

    final String nonRootAuth = "Basic " + Base64.getEncoder()
        .encodeToString("mcpuser:mcppassword".getBytes(StandardCharsets.UTF_8));

    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", nonRootAuth);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 200)
        .put("method", "tools/list")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(403);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void queryWithLimit() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "sql")
        .put("query", "SELECT FROM V1")
        .put("limit", 1));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getInt("count")).isEqualTo(1);
  }

  @Test
  void databaseAuthorizationDenied() throws Exception {
    // Configure MCP to allow "restricteduser"
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowedUsers", new JSONArray().put("root").put("restricteduser")));

    // Create a user with access only to a non-existent database "otherdb"
    if (!getServer(0).getSecurity().existsUser("restricteduser"))
      getServer(0).getSecurity().createUser(new JSONObject()
          .put("name", "restricteduser")
          .put("password", getServer(0).getSecurity().encodePassword("restrictedpass"))
          .put("databases", new JSONObject()
              .put("otherdb", new JSONArray().put("admin"))));

    final String restrictedAuth = "Basic " + Base64.getEncoder()
        .encodeToString("restricteduser:restrictedpass".getBytes(StandardCharsets.UTF_8));

    // Try to query "graph" database: the user should be denied
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", restrictedAuth);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 300)
        .put("method", "tools/call")
        .put("params", new JSONObject()
            .put("name", "query")
            .put("arguments", new JSONObject()
                .put("database", "graph")
                .put("language", "sql")
                .put("query", "SELECT FROM V1")));
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      final JSONObject response = new JSONObject(body);
      assertThat(response.has("result")).isTrue();
      final JSONObject result = response.getJSONObject("result");
      assertThat(result.getBoolean("isError")).isTrue();
      final String text = result.getJSONArray("content").getJSONObject(0).getString("text");
      assertThat(text).contains("not authorized");
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void profilerStartStopCycle() throws Exception {
    // Enable admin permission for profiler
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Start profiler
    final JSONObject startResponse = callTool("profiler_start", new JSONObject().put("timeoutSeconds", 30));
    assertThat(startResponse.getBoolean("isError", true)).isFalse();
    final JSONObject startResult = new JSONObject(startResponse.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(startResult.getString("status")).isEqualTo("started");
    assertThat(startResult.getInt("timeoutSeconds")).isEqualTo(30);

    // Run a query through the server database so the profiler captures it
    getServer(0).getDatabase("graph").query("sql", "SELECT FROM V1 LIMIT 1").close();

    // Check status while recording
    final JSONObject statusResponse = callTool("profiler_status", new JSONObject());
    assertThat(statusResponse.getBoolean("isError", true)).isFalse();
    final JSONObject statusResult = new JSONObject(statusResponse.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(statusResult.getBoolean("recording")).isTrue();
    assertThat(statusResult.getInt("totalQueries")).isGreaterThan(0);

    // Stop profiler
    final JSONObject stopResponse = callTool("profiler_stop", new JSONObject());
    assertThat(stopResponse.getBoolean("isError", true)).isFalse();
    final JSONObject stopResult = new JSONObject(stopResponse.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(stopResult.getBoolean("recording")).isFalse();
    assertThat(stopResult.has("queries")).isTrue();
    assertThat(stopResult.getInt("totalQueries")).isGreaterThan(0);
  }

  @Test
  void profilerStartAlreadyRecording() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Start profiler
    callTool("profiler_start", new JSONObject());

    // Try to start again
    final JSONObject response = callTool("profiler_start", new JSONObject());
    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject result = new JSONObject(response.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(result.getString("status")).isEqualTo("already_recording");

    // Cleanup: stop the profiler
    callTool("profiler_stop", new JSONObject());
  }

  @Test
  void profilerDeniedWithoutAdminPermission() throws Exception {
    // Ensure admin is disabled
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("profiler_start", new JSONObject());
    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void profilerStatusWhenNeverStarted() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    // Reset profiler to ensure clean state
    getServer(0).getQueryProfiler().reset();

    final JSONObject response = callTool("profiler_status", new JSONObject());
    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject result = new JSONObject(response.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(result.getBoolean("recording")).isFalse();
    assertThat(result.getInt("totalQueries")).isEqualTo(0);
  }

  @Test
  void getServerSettings() throws Exception {
    final JSONObject response = callTool("get_server_settings", new JSONObject());

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.has("settings")).isTrue();
    final JSONArray settings = result.getJSONArray("settings");
    assertThat(settings.length()).isGreaterThan(0);

    // Verify each setting has required fields
    final JSONObject first = settings.getJSONObject(0);
    assertThat(first.has("key")).isTrue();
    assertThat(first.has("value")).isTrue();
    assertThat(first.has("description")).isTrue();

    // Verify passwords are masked
    boolean foundPassword = false;
    for (int i = 0; i < settings.length(); i++) {
      final JSONObject setting = settings.getJSONObject(i);
      if (setting.getString("key").toLowerCase().contains("password")) {
        assertThat(setting.getString("value")).isEqualTo("*****");
        foundPassword = true;
      }
    }
    assertThat(foundPassword).isTrue();
  }

  @Test
  void getServerSettingsDeniedWithoutReadPermission() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("get_server_settings", new JSONObject());
    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void setServerSetting() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.sqlStatementCache")
        .put("value", "500"));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    final JSONObject result = new JSONObject(text);
    assertThat(result.getString("key")).isEqualTo("arcadedb.sqlStatementCache");
    assertThat(result.getString("newValue")).isEqualTo("500");

    // Restore default
    callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.sqlStatementCache")
        .put("value", "300"));
  }

  @Test
  void setServerSettingDeniedWithoutAdminPermission() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.sqlStatementCache")
        .put("value", "500"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("not allowed");
  }

  @Test
  void setServerSettingUnknownKey() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowAdmin", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("set_server_setting", new JSONObject()
        .put("key", "arcadedb.nonExistentSetting")
        .put("value", "foo"));

    assertThat(response.getBoolean("isError")).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("Unknown server setting");
  }

  @Test
  void apiTokenUserAllowedByTokenName() throws Exception {
    // Create an API token
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("mcptoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      // Configure MCP to allow "mcptoken" (the bare token name, not "apitoken:mcptoken")
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root").put("mcptoken")));

      // Use the API token to call MCP initialize
      final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 500)
          .put("method", "initialize")
          .put("params", new JSONObject());
      try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
        out.write(request.toString().getBytes(StandardCharsets.UTF_8));
      }
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
        final JSONObject response = new JSONObject(body);
        assertThat(response.has("result")).isTrue();
        assertThat(response.getJSONObject("result").has("protocolVersion")).isTrue();
      } finally {
        connection.disconnect();
      }
    } finally {
      // Cleanup: delete the token
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void apiTokenPrincipalProfileFiltersDiscoveryAndDirectCalls() throws Exception {
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("profiletoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      saveMCPConfig(new JSONObject()
          .put("profile", "all")
          .put("allowedUsers", new JSONArray().put("root").put("profiletoken"))
          .put("principalProfiles", new JSONObject().put("apitoken:profiletoken", "rag")));

      final String tokenAuth = "Bearer " + tokenValue;
      final JSONObject listed = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 503)
          .put("method", "tools/list")
          .put("params", new JSONObject()), tokenAuth);
      assertThat(toolNames(listed))
          .contains("sample_records", "vector_search")
          .doesNotContain("server_status", "execute_command");

      final JSONObject denied = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 504)
          .put("method", "tools/call")
          .put("params", new JSONObject()
              .put("name", "server_status")
              .put("arguments", new JSONObject())), tokenAuth)
          .getJSONObject("result");
      assertThat(denied.getBoolean("isError", false)).isTrue();
    } finally {
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void apiTokenPrincipalProfileAcceptsBareTokenName() throws Exception {
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("baretoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      // The bare token name is the spelling allowedUsers accepts, so a profile written the same way must apply.
      saveMCPConfig(new JSONObject()
          .put("profile", "all")
          .put("allowedUsers", new JSONArray().put("root").put("baretoken"))
          .put("principalProfiles", new JSONObject().put("baretoken", "rag")));

      final String tokenAuth = "Bearer " + tokenValue;
      final JSONObject listed = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 505)
          .put("method", "tools/list")
          .put("params", new JSONObject()), tokenAuth);
      assertThat(toolNames(listed))
          .contains("sample_records", "vector_search")
          .doesNotContain("server_status", "execute_command");

      final JSONObject denied = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 506)
          .put("method", "tools/call")
          .put("params", new JSONObject()
              .put("name", "server_status")
              .put("arguments", new JSONObject())), tokenAuth)
          .getJSONObject("result");
      assertThat(denied.getBoolean("isError", false)).isTrue();
    } finally {
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void apiTokenUserDeniedWhenNotInAllowedUsers() throws Exception {
    // Create an API token
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("deniedtoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      // Configure MCP with only "root": the token name "deniedtoken" is NOT in the list
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));

      final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 501)
          .put("method", "initialize")
          .put("params", new JSONObject());
      try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
        out.write(request.toString().getBytes(StandardCharsets.UTF_8));
      }
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(403);
      } finally {
        connection.disconnect();
      }
    } finally {
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void apiTokenUserAllowedByWildcard() throws Exception {
    // Create an API token
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject tokenResult = getServer(0).getSecurity().getApiTokenConfiguration()
        .createToken("wildcardtoken", "graph", 0, permissions);
    final String tokenValue = tokenResult.getString("token");

    try {
      // Configure MCP with wildcard "*"
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("*")));

      final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 502)
          .put("method", "initialize")
          .put("params", new JSONObject());
      try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
        out.write(request.toString().getBytes(StandardCharsets.UTF_8));
      }
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
        final JSONObject response = new JSONObject(body);
        assertThat(response.has("result")).isTrue();
      } finally {
        connection.disconnect();
      }
    } finally {
      getServer(0).getSecurity().getApiTokenConfiguration()
          .deleteToken(tokenResult.getString("tokenHash"));
    }
  }

  @Test
  void queryUnknownDatabaseReturnsAvailableList() throws Exception {
    final JSONObject response = callTool("query", new JSONObject()
        .put("database", "nonexistent_db")
        .put("language", "cypher")
        .put("query", "RETURN 1"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("nonexistent_db");
    assertThat(errorText).containsIgnoringCase("available databases");
    assertThat(errorText).contains("graph");
  }

  @Test
  void executeCommandUnknownDatabaseReturnsAvailableList() throws Exception {
    final JSONObject response = callTool("execute_command", new JSONObject()
        .put("database", "nonexistent_db")
        .put("language", "cypher")
        .put("command", "CREATE (n:Test) RETURN n"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("nonexistent_db");
    assertThat(errorText).containsIgnoringCase("available databases");
    assertThat(errorText).contains("graph");
  }

  @Test
  void getSchemaUnknownDatabaseReturnsAvailableList() throws Exception {
    final JSONObject response = callTool("get_schema", new JSONObject()
        .put("database", "nonexistent_db"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("nonexistent_db");
    assertThat(errorText).containsIgnoringCase("available databases");
    assertThat(errorText).contains("graph");
  }

  @Test
  void vectorSearchDenseReturnsDistanceAndProperties() throws Exception {
    seedVectorIndexes();

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("k", 2));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("McpVectorRecord[embedding]");
    assertThat(payload.getBoolean("sparse")).isFalse();
    assertThat(payload.getString("scoring")).startsWith("distance_lower_is_better");
    assertThat(payload.getInt("count")).isEqualTo(2);

    final JSONObject first = payload.getJSONArray("results").getJSONObject(0);
    assertThat(first.getString("rid")).startsWith("#");
    assertThat(first.has("score")).isFalse();
    assertThat(first.getDouble("distance")).isGreaterThanOrEqualTo(0.0);
    assertThat(first.getJSONObject("properties").getString("name")).isEqualTo("dense-a");
  }

  @Test
  void vectorSearchAppliesReadOnlyFilterToBoundedCandidates() throws Exception {
    seedVectorIndexes();

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("filter", "category = 'keep'")
        .put("k", 2));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getInt("count")).isEqualTo(2);
    assertThat(payload.getInt("candidateLimit")).isEqualTo(16);
    assertThat(payload.getBoolean("truncated")).isTrue();
    for (int i = 0; i < payload.getJSONArray("results").length(); i++)
      assertThat(payload.getJSONArray("results").getJSONObject(i)
          .getJSONObject("properties").getString("category")).isEqualTo("keep");
  }

  @Test
  void vectorSearchSparseAcceptsCompactIndicesAndWeights() throws Exception {
    seedVectorIndexes();

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryIndices", new JSONArray().put(5))
        .put("queryVector", new JSONArray().put(1.0))
        .put("sparse", true)
        .put("k", 2));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getBoolean("sparse")).isTrue();
    assertThat(payload.getString("scoring")).contains("score_higher_is_better");
    assertThat(payload.getInt("count")).isEqualTo(2);
    final JSONArray results = payload.getJSONArray("results");
    assertThat(results.getJSONObject(0).getJSONObject("properties").getString("name")).isEqualTo("sparse-high");
    assertThat(results.getJSONObject(0).has("distance")).isFalse();
    assertThat(results.getJSONObject(0).getDouble("score"))
        .isGreaterThan(results.getJSONObject(1).getDouble("score"));
  }

  @Test
  void vectorSearchUsesIndependentFilteredCandidateBudget() throws Exception {
    seedVectorIndexes();

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("filter", "category = 'missing'")
        .put("k", 1_000));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getInt("candidateLimit")).isEqualTo(8_000);
    assertThat(payload.getInt("count")).isZero();
    assertThat(payload.getBoolean("truncated")).isFalse();
  }

  /**
   * Truncation must describe the result window, not the size of the index. A filtered search that returns fewer
   * hits than requested has exhausted its matches, so reporting truncation there tells the caller to widen a
   * search that cannot yield more. The index deliberately holds more vectors than the candidate window.
   */
  @Test
  void vectorSearchDoesNotReportTruncationWhenResultWindowIsNotFilled() throws Exception {
    seedVectorBudgetRecords();

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorBudgetRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("filter", "category = 'solo'")
        .put("k", 2));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getInt("candidateLimit")).isEqualTo(16);
    assertThat(payload.getInt("count")).isEqualTo(1);
    assertThat(payload.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("name")).isEqualTo("budget-solo");
    assertThat(payload.getBoolean("truncated")).isFalse();
  }

  /**
   * A filled result window is the one case where more matches may exist beyond what was returned.
   */
  @Test
  void vectorSearchReportsTruncationWhenResultWindowIsFilled() throws Exception {
    seedVectorBudgetRecords();

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorBudgetRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("k", 2));

    assertThat(response.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getInt("count")).isEqualTo(2);
    assertThat(payload.getBoolean("truncated")).isTrue();
  }

  @Test
  void vectorSearchSupportsDenseOptionsAndFullSparseVectors() throws Exception {
    seedVectorIndexes();

    final JSONObject dense = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("efSearch", 20)
        .put("filter", "   ")
        .put("k", 1));
    assertThat(dense.getBoolean("isError", true)).isFalse();

    final JSONObject sparse = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryVector",
            new JSONArray().put(0.0).put(0.0).put(0.0).put(0.0).put(0.0).put(1.0).put(0.0).put(0.0))
        .put("sparse", true)
        .put("k", 2));
    assertThat(sparse.getBoolean("isError", true)).isFalse();
    final JSONObject payload = new JSONObject(
        sparse.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getInt("count")).isEqualTo(2);
    assertThat(payload.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("name")).isEqualTo("sparse-high");
  }

  @Test
  void vectorSearchValidatesIndexModeAndDimensions() throws Exception {
    seedVectorIndexes();

    final JSONObject wrongMode = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryVector", new JSONArray().put(1.0))
        .put("k", 1));
    assertThat(wrongMode.getBoolean("isError", false)).isTrue();
    assertThat(wrongMode.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("LSM_SPARSE_VECTOR").contains("sparse=true");

    final JSONObject wrongDimensions = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0))
        .put("k", 1));
    assertThat(wrongDimensions.getBoolean("isError", false)).isTrue();
    assertThat(wrongDimensions.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("2 dimensions").contains("requires 3");
  }

  @Test
  void vectorSearchRejectsInvalidOptionsAndIndexSelection() throws Exception {
    seedVectorIndexes();

    final JSONObject invalidEfSearch = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("efSearch", 0)
        .put("k", 1));
    assertThat(invalidEfSearch.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'efSearch' must be at least 1");

    final JSONObject sparseEfSearch = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryIndices", new JSONArray().put(5))
        .put("queryVector", new JSONArray().put(1.0))
        .put("efSearch", 20)
        .put("sparse", true)
        .put("k", 1));
    assertThat(sparseEfSearch.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'efSearch' applies only to dense");

    final JSONObject denseIndices = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryIndices", new JSONArray().put(0))
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("k", 1));
    assertThat(denseIndices.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'queryIndices' requires sparse=true");

    final JSONObject oversizedFilter = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("filter", "x".repeat(4_097))
        .put("k", 1));
    assertThat(oversizedFilter.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'filter' must not exceed 4096");

    final JSONObject unknownIndex = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Missing[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("k", 1));
    assertThat(unknownIndex.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("does not exist").contains("McpVectorRecord[embedding]");

    final JSONObject denseAsSparse = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryIndices", new JSONArray().put(0))
        .put("queryVector", new JSONArray().put(1.0))
        .put("sparse", true)
        .put("k", 1));
    assertThat(denseAsSparse.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("LSM_VECTOR").contains("sparse=false");

    final JSONObject nonVector = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[title]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("k", 1));
    assertThat(nonVector.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("LSM_TREE").contains("not LSM_VECTOR");
  }

  @Test
  void vectorSearchRejectsMalformedSparseVectors() throws Exception {
    seedVectorIndexes();

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryIndices", new JSONArray().put(1).put(5))
        .put("queryVector", new JSONArray().put(1.0))
        .put("sparse", true)
        .put("k", 2));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("same length");
  }

  @Test
  void vectorSearchRejectsInvalidVectorValuesAndSparseDimensions() throws Exception {
    seedVectorIndexes();

    final JSONObject empty = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray())
        .put("k", 1));
    assertThat(empty.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("must not be empty");

    final JSONObject nonNumeric = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put("bad").put(0.0))
        .put("k", 1));
    assertThat(nonNumeric.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("must contain only numbers");

    final JSONObject wrongFullDimensions = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryVector", new JSONArray().put(0.0).put(1.0))
        .put("sparse", true)
        .put("k", 1));
    assertThat(wrongFullDimensions.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("index requires 8").contains("Pass queryIndices");

    final JSONObject zeroFullVector = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryVector",
            new JSONArray().put(0.0).put(0.0).put(0.0).put(0.0).put(0.0).put(0.0).put(0.0).put(0.0))
        .put("sparse", true)
        .put("k", 1));
    assertThat(zeroFullVector.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("at least one non-zero weight");

    final JSONObject zeroDenseVector = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(0.0).put(0.0).put(0.0))
        .put("k", 1));
    assertThat(zeroDenseVector.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("at least one non-zero value");

    final JSONObject fractionalIndex = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryIndices", new JSONArray().put(1.5))
        .put("queryVector", new JSONArray().put(1.0))
        .put("sparse", true)
        .put("k", 1));
    assertThat(fractionalIndex.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("non-negative integers");

    final JSONObject duplicateIndex = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryIndices", new JSONArray().put(5).put(5))
        .put("queryVector", new JSONArray().put(1.0).put(0.5))
        .put("sparse", true)
        .put("k", 1));
    assertThat(duplicateIndex.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("duplicate dimension 5");

    final JSONObject outOfRange = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryIndices", new JSONArray().put(8))
        .put("queryVector", new JSONArray().put(1.0))
        .put("sparse", true)
        .put("k", 1));
    assertThat(outOfRange.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("outside index dimensions 0-7");

    final JSONObject zeroCompactVector = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpSparseVectorRecord[tokens,weights]")
        .put("queryIndices", new JSONArray().put(5))
        .put("queryVector", new JSONArray().put(0.0))
        .put("sparse", true)
        .put("k", 1));
    assertThat(zeroCompactVector.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("at least one non-zero weight");
  }

  @Test
  void vectorSearchRejectsMalformedFilterWithoutExecutingWrites() throws Exception {
    seedVectorIndexes();
    final Database db = getServerDatabase(0, getDatabaseName());
    final long before = db.countType("McpVectorRecord", false);

    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("filter", "category = 'keep'); DELETE FROM McpVectorRecord")
        .put("k", 2));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("Invalid vector search");
    assertThat(db.countType("McpVectorRecord", false)).isEqualTo(before);
  }

  @Test
  void vectorSearchEnforcesBoundsAndReadPermission() throws Exception {
    for (final int invalidK : new int[] { 0, 1_001 }) {
      final JSONObject response = callTool("vector_search", new JSONObject()
          .put("database", getDatabaseName())
          .put("indexName", "McpVectorRecord[embedding]")
          .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
          .put("k", invalidK));
      assertThat(response.getBoolean("isError", false)).isTrue();
      assertThat(response.getJSONArray("content").getJSONObject(0).getString("text")).contains("'k'");
    }

    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));
    final JSONObject denied = callTool("vector_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "McpVectorRecord[embedding]")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("k", 1));
    assertThat(denied.getBoolean("isError", false)).isTrue();
    assertThat(denied.getJSONArray("content").getJSONObject(0).getString("text")).contains("not allowed");
  }

  @Test
  void vectorSearchReportsArgumentFaultsBeforeDatabaseFaults() throws Exception {
    final JSONObject response = callTool("vector_search", new JSONObject()
        .put("database", "McpNoSuchDatabase")
        .put("queryVector", new JSONArray().put(1.0).put(0.0).put(0.0))
        .put("k", 2));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'indexName' is required");
  }

  @Test
  void hybridSearchIsRegisteredInHttpTransport() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 90)
        .put("method", "tools/list")
        .put("params", new JSONObject()));

    assertThat(toolNames(response)).contains("hybrid_search");
  }

  @Test
  void hybridSearchVectorOnlyReturnsTheVectorLegUnfused() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("k", 3)));

    assertThat(payload.getString("vectorIndexName")).isEqualTo("McpHybridDoc[embedding]");
    assertThat(payload.getBoolean("fused")).isFalse();
    assertThat(payload.getBoolean("sparse")).isFalse();
    assertThat(payload.getString("scoring")).startsWith("distance_lower_is_better");
    assertThat(payload.getInt("count")).isEqualTo(3);
    assertThat(payload.getJSONObject("legs").getJSONObject("vector").getInt("count")).isGreaterThanOrEqualTo(3);

    final JSONObject first = payload.getJSONArray("results").getJSONObject(0);
    assertThat(first.getJSONObject("properties").getString("title")).isEqualTo("h0");
    assertThat(first.getDouble("distance")).isGreaterThanOrEqualTo(0.0);
    assertThat(first.has("fusedScore")).isFalse();
    assertThat(first.getJSONArray("sources").getString(0)).isEqualTo("vector");
  }

  @Test
  void hybridSearchRejectsOutOfRangeK() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("k", 0));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'k' must be between 1 and 1000");
  }

  @Test
  void hybridSearchFusesVectorAndFullText() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        .put("k", 6)));

    assertThat(payload.getBoolean("fused")).isTrue();
    assertThat(payload.getString("fusionStrategy")).isEqualTo("RRF");
    assertThat(payload.getString("fulltextIndexName")).isEqualTo("McpHybridDoc[content]");
    assertThat(payload.getJSONObject("legs").getJSONObject("fulltext").getInt("count")).isEqualTo(1);

    final JSONArray results = payload.getJSONArray("results");
    boolean sawFullTextSource = false;
    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      assertThat(row.getDouble("fusedScore")).isGreaterThan(0.0);
      assertThat(row.has("distance")).isFalse();
      final JSONArray sources = row.getJSONArray("sources");
      for (int s = 0; s < sources.length(); s++)
        if ("fulltext".equals(sources.getString(s)))
          sawFullTextSource = true;
    }
    // h5 matches 'gearbox' and is far from the probe vector, so it can only arrive via the full-text leg.
    assertThat(sawFullTextSource).isTrue();
  }

  @Test
  void hybridSearchWeightsShiftTheFusedOrder() throws Exception {
    seedHybridGraph();

    final JSONObject fullTextHeavy = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        .put("weights", new JSONObject().put("vector", 0.01).put("fulltext", 100.0))
        .put("k", 6)));

    assertThat(fullTextHeavy.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("title")).isEqualTo("h5");
  }

  @Test
  void hybridSearchRejectsAnIncompleteFullTextLeg() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextQuery", "gearbox")
        .put("k", 3));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("fulltextIndexName").contains("fulltextQuery");
  }

  @Test
  void hybridSearchReportsTheFullTextIndexEvenWhenThatLegMatchesNothing() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "zzznosuchterm")
        .put("k", 3)));

    // A leg that matched nothing cannot become a fusion source, so the response is unfused. It must
    // still name the index that was searched, or the caller cannot tell an empty leg from an absent one.
    assertThat(payload.getBoolean("fused")).isFalse();
    assertThat(payload.getString("fulltextIndexName")).isEqualTo("McpHybridDoc[content]");
    assertThat(payload.getJSONObject("legs").getJSONObject("fulltext").getInt("count")).isEqualTo(0);
  }

  @Test
  void hybridSearchRejectsAWeightForALegTheRequestDoesNotUse() throws Exception {
    seedHybridGraph();

    final JSONObject noFullTextLeg = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("weights", new JSONObject().put("fulltext", 2.0))
        .put("k", 3));

    // A weight nothing will read is the same silent no-op as an unknown key, so it is rejected too.
    assertThat(noFullTextLeg.getBoolean("isError", false)).isTrue();
    assertThat(noFullTextLeg.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("weights.fulltext").contains("no full-text leg");

    final JSONObject noExpandLeg = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("weights", new JSONObject().put("expand", 2.0))
        .put("k", 3));

    assertThat(noExpandLeg.getBoolean("isError", false)).isTrue();
    assertThat(noExpandLeg.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("weights.expand").contains("no graph expansion leg");
  }

  @Test
  void hybridSearchAcceptsAWeightForALegTheRequestDoesUse() throws Exception {
    seedHybridGraph();

    // The guard must reject only weights with no leg behind them, never a legitimate override.
    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        .put("expand", new JSONObject().put("maxDepth", 1))
        .put("weights", new JSONObject().put("vector", 1.0).put("fulltext", 2.0).put("expand", 0.25))
        .put("k", 6)));

    assertThat(payload.getBoolean("fused")).isTrue();
  }

  @Test
  void hybridSearchRejectsANonNumericWeight() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("weights", new JSONObject().put("vector", "abc"))
        .put("k", 3));

    // A wrong-typed weight must be reported by the tool's own validation, not by the JSON layer's
    // parse failure, which names neither the argument nor the rule it broke.
    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("weights.vector").contains("finite number");
  }

  @Test
  void hybridSearchReportsTheSeedCountItActuallyUsed() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject().put("maxDepth", 1))
        .put("k", 10)));

    final JSONObject expand = payload.getJSONObject("legs").getJSONObject("expand");
    // The filter leaves exactly one seed, so the reported count is checkable rather than incidental.
    // seedsTruncated is derived from the same cap that HybridSearchSeedsTest pins directly; this
    // fixture is far too small to drive it true, so it is not asserted here.
    assertThat(expand.getInt("seedCount")).isEqualTo(1);
  }

  @Test
  void hybridSearchExpandsAlongTheGraphAndReportsPaths() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        // The fixture is small enough that an unfiltered vector leg retrieves every record, which would
        // make every record a seed and leave the expansion leg with nothing new to contribute. Narrowing
        // the vector leg to h0 is what makes the expanded rows observable.
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("direction", "out")
            .put("maxDepth", 2))
        .put("k", 10)));

    assertThat(payload.getBoolean("fused")).isTrue();
    final JSONObject expandLeg = payload.getJSONObject("legs").getJSONObject("expand");
    assertThat(expandLeg.getString("direction")).isEqualTo("out");
    assertThat(expandLeg.getInt("maxDepth")).isEqualTo(2);
    assertThat(expandLeg.getBoolean("truncated")).isFalse();
    assertThat(expandLeg.getInt("count")).isGreaterThan(0);

    JSONObject expanded = null;
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      final JSONArray sources = row.getJSONArray("sources");
      for (int s = 0; s < sources.length(); s++)
        if ("expand".equals(sources.getString(s)) && sources.length() == 1)
          expanded = row;
    }

    assertThat(expanded).isNotNull();
    assertThat(expanded.getInt("depth")).isBetween(1, 2);
    // The path starts at the seed and ends at the row itself, so it is one longer than the depth.
    assertThat(expanded.getJSONArray("path").length()).isEqualTo(expanded.getInt("depth") + 1);
    assertThat(expanded.getJSONArray("path").getString(expanded.getJSONArray("path").length() - 1))
        .isEqualTo(expanded.getString("rid"));
  }

  @Test
  void hybridSearchDedupsNodesReachableBySeveralPaths() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        // h2 must reach the result set through the expansion leg, not as a seed, or its depth is never set.
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 3))
        .put("k", 10)));

    // h2 is reachable from h0 both directly and through h1. It must appear once, at its shallowest depth.
    final Set<String> rids = new HashSet<>();
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++)
      assertThat(rids.add(results.getJSONObject(i).getString("rid"))).isTrue();

    JSONObject h2 = null;
    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      if ("h2".equals(row.getJSONObject("properties").getString("title")))
        h2 = row;
    }
    // h2 is reachable from h0 directly and through h1, so breadth-first discovery must place it at
    // depth 1 and emit it exactly once.
    assertThat(h2).isNotNull();
    assertThat(h2.getInt("depth")).isEqualTo(1);
    assertThat(h2.getJSONArray("path").length()).isEqualTo(2);
    assertThat(payload.getJSONObject("legs").getJSONObject("expand").getInt("count")).isGreaterThan(0);
  }

  @Test
  void hybridSearchRestrictsExpansionToTheRequestedEdgeTypes() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 1))
        .put("k", 10)));

    // h4 hangs off h0 by McpHybridMentions only, so restricting to McpHybridCites must not reach it.
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++)
      assertThat(results.getJSONObject(i).getJSONObject("properties").getString("title")).isNotEqualTo("h4");

    assertThat(payload.getJSONObject("legs").getJSONObject("expand").getInt("count")).isGreaterThan(0);

    final Set<String> titles = new HashSet<>();
    for (int i = 0; i < results.length(); i++)
      titles.add(results.getJSONObject(i).getJSONObject("properties").getString("title"));
    // h1 and h2 hang off h0 by McpHybridCites and must be reached; h4 hangs off it by
    // McpHybridMentions only and must not be.
    assertThat(titles).contains("h1", "h2");
    assertThat(titles).doesNotContain("h4");
  }

  @Test
  void hybridSearchWithoutEdgeTypesTraversesEveryEdgeType() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject().put("maxDepth", 1))
        .put("k", 10)));

    final Set<String> titles = new HashSet<>();
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++)
      titles.add(results.getJSONObject(i).getJSONObject("properties").getString("title"));

    // Omitting edgeTypes traverses every edge type, so h4 arrives over McpHybridMentions alongside
    // the McpHybridCites neighbors that the restricted traversal reaches.
    assertThat(payload.getJSONObject("legs").getJSONObject("expand").getInt("count")).isGreaterThan(0);
    assertThat(titles).contains("h1", "h2", "h4");
  }

  @Test
  void hybridSearchFusesAllThreeLegs() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        // Seeds become h0 (vector) and h5 (full-text). h5 is unconnected, so every expanded row comes
        // from h0's citation chain and none of them is already a seed.
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 2))
        .put("k", 10)));

    assertThat(payload.getBoolean("fused")).isTrue();
    assertThat(payload.getJSONObject("legs").has("vector")).isTrue();
    assertThat(payload.getJSONObject("legs").has("fulltext")).isTrue();
    assertThat(payload.getJSONObject("legs").has("expand")).isTrue();

    final Set<String> allSources = new HashSet<>();
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++) {
      final JSONArray sources = results.getJSONObject(i).getJSONArray("sources");
      for (int s = 0; s < sources.length(); s++)
        allSources.add(sources.getString(s));
    }
    assertThat(allSources).contains("vector", "fulltext", "expand");
  }

  @Test
  void hybridSearchRejectsScoreBasedFusionWithExpansion() throws Exception {
    seedHybridGraph();

    for (final String strategy : new String[] { "DBSF", "LINEAR" }) {
      final JSONObject response = callTool("hybrid_search", new JSONObject()
          .put("database", getDatabaseName())
          .put("vectorIndexName", "McpHybridDoc[embedding]")
          .put("queryVector", probeVector())
          .put("fusionStrategy", strategy)
          .put("expand", new JSONObject().put("maxDepth", 1))
          .put("k", 5));

      assertThat(response.getBoolean("isError", false)).isTrue();
      assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
          .contains(strategy).contains("RRF").contains("expand");
    }
  }

  @Test
  void hybridSearchAllowsScoreBasedFusionWithoutExpansion() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("fulltextIndexName", "McpHybridDoc[content]")
        .put("fulltextQuery", "gearbox")
        .put("fusionStrategy", "LINEAR")
        .put("k", 6)));

    assertThat(payload.getString("fusionStrategy")).isEqualTo("LINEAR");
    assertThat(payload.getBoolean("fused")).isTrue();

    final List<String> order = new ArrayList<>();
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++)
      order.add(results.getJSONObject(i).getJSONObject("properties").getString("title"));

    // h0's embedding is the probe vector exactly, so under a score-normalizing strategy it must not
    // sink below the records furthest from the probe. It does exactly that if a dense distance is
    // fused as though it were a similarity.
    assertThat(order).contains("h0");
    assertThat(order.indexOf("h0")).isLessThan(order.indexOf("h2"));
  }

  @Test
  void hybridSearchRejectsDepthAboveTheServerCap() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONObject().put("maxDepth", 4))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("maxDepth").contains("1 and 3");
  }

  @Test
  void hybridSearchHonorsTheDepthCapWhenTheGraphIsDeeper() throws Exception {
    seedHybridGraph();

    final JSONObject payload = payloadOf(callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("filter", "title = 'h0'")
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridCites"))
            .put("maxDepth", 1))
        .put("k", 10)));

    // From h0 at one hop only h1 and h2 are reachable; h3 sits two hops out and must not appear.
    final JSONArray results = payload.getJSONArray("results");
    for (int i = 0; i < results.length(); i++) {
      final JSONObject row = results.getJSONObject(i);
      assertThat(row.getJSONObject("properties").getString("title")).isNotEqualTo("h3");
      if (row.has("depth"))
        assertThat(row.getInt("depth")).isEqualTo(1);
    }

    assertThat(payload.getJSONObject("legs").getJSONObject("expand").getInt("count")).isGreaterThan(0);

    final Set<String> titles = new HashSet<>();
    for (int i = 0; i < results.length(); i++)
      titles.add(results.getJSONObject(i).getJSONObject("properties").getString("title"));
    // h1 and h2 sit one hop from h0 and must be reached; h3 sits two hops out and must not be.
    assertThat(titles).contains("h1", "h2");
    assertThat(titles).doesNotContain("h3");
  }

  @Test
  void hybridSearchRejectsAnUnknownEdgeType() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray().put("McpHybridNotAnEdge")))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("McpHybridNotAnEdge").contains("Available edge types");
  }

  @Test
  void hybridSearchRejectsExpansionOverADocumentType() throws Exception {
    seedVectorIndexes();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpVectorRecord[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONObject().put("maxDepth", 1))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("vertex type").contains("McpVectorRecord");
  }

  @Test
  void hybridSearchRejectsAnUnknownWeightsKey() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("weights", new JSONObject().put("vecter", 2.0))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("vecter").contains("vector").contains("fulltext").contains("expand");
  }

  @Test
  void hybridSearchRejectsANegativeWeight() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("weights", new JSONObject().put("expand", -1.0))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("weights.expand");
  }

  @Test
  void hybridSearchRejectsANonObjectExpand() throws Exception {
    seedHybridGraph();

    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONArray())
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(response.getJSONArray("content").getJSONObject(0).getString("text"))
        .contains("'expand' must be an object with optional edgeTypes, direction, and maxDepth");
  }

  @Test
  void hybridSearchRejectsInvalidExpandBeforeResolvingTheDatabase() throws Exception {
    final JSONObject response = callTool("hybrid_search", new JSONObject()
        .put("database", "McpNoSuchDatabase")
        .put("vectorIndexName", "McpHybridDoc[embedding]")
        .put("queryVector", probeVector())
        .put("expand", new JSONObject().put("maxDepth", 4))
        .put("k", 5));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String text = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("maxDepth");
    assertThat(text).doesNotContain("McpNoSuchDatabase");
  }

  @Test
  void fullTextSearchByIndexName() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
    assertThat(payload.getString("similarity")).isEqualTo("BM25");
    assertThat(payload.getInt("count")).isEqualTo(1);

    final JSONObject hit = payload.getJSONArray("results").getJSONObject(0);
    assertThat(hit.getString("rid")).startsWith("#");
    assertThat(hit.getFloat("score")).isGreaterThan(0f);
    assertThat(hit.getJSONObject("properties").getString("title")).isEqualTo("Doc1");
  }

  @Test
  void fullTextSearchRanksAndLimits() throws Exception {
    final JSONObject unlimitedResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "language"));

    final JSONObject unlimitedPayload = new JSONObject(
        unlimitedResponse.getJSONArray("content").getJSONObject(0).getString("text"));

    // Doc1, Doc2 and Doc3 all contain "language".
    assertThat(unlimitedPayload.getInt("count")).isEqualTo(3);

    final JSONArray results = unlimitedPayload.getJSONArray("results");
    final JSONObject first = results.getJSONObject(0);
    final JSONObject second = results.getJSONObject(1);
    assertThat(first.getFloat("score")).isGreaterThanOrEqualTo(second.getFloat("score"));
    // Doc3 repeats "language" three times where Doc1 and Doc2 mention it once, and all three are the same length,
    // so term frequency alone must put Doc3 on top. This assertion fails if the score-descending sort is dropped.
    assertThat(first.getJSONObject("properties").getString("title")).isEqualTo("Doc3");

    final JSONObject limitedResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "language")
        .put("limit", 1));

    final JSONObject limitedPayload = new JSONObject(
        limitedResponse.getJSONArray("content").getJSONObject(0).getString("text"));

    // The limit must cut the result set down to the single best-scoring hit, not an arbitrary one.
    assertThat(limitedPayload.getInt("count")).isEqualTo(1);
    final JSONObject limitedHit = limitedPayload.getJSONArray("results").getJSONObject(0);
    assertThat(limitedHit.getJSONObject("properties").getString("title")).isEqualTo("Doc3");
  }

  @Test
  void fullTextSearchRejectsNonPositiveLimit() throws Exception {
    final JSONObject zeroResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java")
        .put("limit", 0));

    assertThat(zeroResponse.getBoolean("isError", false)).isTrue();
    final String zeroErrorText = zeroResponse.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(zeroErrorText).containsIgnoringCase("limit");

    final JSONObject negativeResponse = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java")
        .put("limit", -1));

    assertThat(negativeResponse.getBoolean("isError", false)).isTrue();
    final String negativeErrorText = negativeResponse.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(negativeErrorText).containsIgnoringCase("limit");
  }

  @Test
  void fullTextSearchDerivesIndexNameWithSpacesStripped() throws Exception {
    // The schema registered this index as Spaced[myprop]; deriving 'Spaced[my prop]' verbatim would never match it.
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Spaced")
        .put("properties", new JSONArray().put("my prop"))
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Spaced[myprop]");
    assertThat(payload.getInt("count")).isEqualTo(1);
  }

  @Test
  void fullTextSearchRejectsBlankQueryText() throws Exception {
    // The Lucene parser turns a blank query into IndexException("Invalid search query: "), which names no cause. The
    // tool must reject it with a message that says which argument is wrong.
    for (final String blank : new String[] { "", "   " }) {
      final JSONObject response = callTool("full_text_search", new JSONObject()
          .put("database", getDatabaseName())
          .put("indexName", "Article[content]")
          .put("queryText", blank));

      assertThat(response.getBoolean("isError", false)).isTrue();
      final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
      assertThat(errorText).contains("queryText");
      assertThat(errorText).doesNotContain("Invalid search query");
    }
  }

  @Test
  void fullTextSearchDeniedWhenReadsDisabled() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("not allowed");
  }

  @Test
  void fullTextSearchIndexNameWinsOverTypeName() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[content]")
        .put("typeName", "Searchable")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    // 'typeName' addresses an unrelated index (Searchable[text]); 'indexName' must win and be used as-is.
    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
  }

  @Test
  void fullTextSearchByTypeNameAndProperties() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Article")
        .put("properties", new JSONArray().put("content"))
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    assertThat(payload.getString("indexName")).isEqualTo("Article[content]");
    assertThat(payload.getInt("count")).isEqualTo(1);
  }

  @Test
  void fullTextSearchByTypeNameAloneWhenUnambiguous() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Searchable")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isFalse();

    final JSONObject payload = new JSONObject(
        response.getJSONArray("content").getJSONObject(0).getString("text"));

    // The index lives on the supertype; the hit is a Decision, a subtype record.
    assertThat(payload.getString("indexName")).isEqualTo("Searchable[text]");
    assertThat(payload.getInt("count")).isEqualTo(1);
    assertThat(payload.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("@type")).isEqualTo("Decision");
  }

  @Test
  void fullTextSearchAmbiguousTypeNameListsCandidates() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Article")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Article[content]");
    assertThat(errorText).contains("Article[title,content]");
  }

  @Test
  void fullTextSearchOnSubtypeNameGuidesToSupertypeIndex() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "Decision")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Decision");
    assertThat(errorText).contains("Searchable[text]");
    assertThat(errorText).containsIgnoringCase("supertype");
  }

  @Test
  void fullTextSearchUnknownIndexListsAvailable() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Artcle[content]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Artcle[content]");
    assertThat(errorText).containsIgnoringCase("available full-text indexes");
    assertThat(errorText).contains("Article[content]");
  }

  @Test
  void fullTextSearchOnNonFullTextIndexIsRejected() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Article[title]")
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("Article[title]");
    assertThat(errorText).contains("is not a full-text index");
    assertThat(errorText).containsIgnoringCase("available full-text indexes");
    assertThat(errorText).contains("Article[content]");
  }

  @Test
  void fullTextSearchWithoutAddressingIsRejected() throws Exception {
    final JSONObject response = callTool("full_text_search", new JSONObject()
        .put("database", getDatabaseName())
        .put("queryText", "java"));

    assertThat(response.getBoolean("isError", false)).isTrue();
    final String errorText = response.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(errorText).contains("indexName");
    assertThat(errorText).contains("typeName");
  }

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

  @Test
  void disabledServerErrorEchoesRequestId() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", false)
        .put("allowedUsers", new JSONArray().put("root")));

    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 409)
        .put("method", "initialize")
        .put("params", new JSONObject());
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(503);
      // A 503 body arrives on the error stream; it must echo the request id per JSON-RPC 2.0.
      final String body = FileUtils.readStreamAsString(connection.getErrorStream(), "utf8");
      final JSONObject response = new JSONObject(body);
      assertThat(response.getInt("id")).isEqualTo(409);
      assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32600);
    } finally {
      connection.disconnect();
      saveMCPConfig(new JSONObject()
          .put("enabled", true)
          .put("allowReads", true)
          .put("allowedUsers", new JSONArray().put("root")));
    }
  }

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
        .put("typeName", "UpsertPerson")
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
        .put("query", "MATCH (p:UpsertPerson {email: 'ada@x.com'}) RETURN count(p) AS c"));
    final JSONObject countPayload = new JSONObject(
        countResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(countPayload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityWithoutSetPropertiesCreatesNode() throws Exception {
    // A bare MERGE (no SET) analyzes to {CREATE, UPDATE}, so both flags are required. This test covers the
    // no-SET execution path: the node is created and a repeated call matches rather than duplicating it.
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "NoSetPerson")
        .put("matchKeys", new JSONObject().put("name", "Solo"));

    final JSONObject first = callTool("upsert_entity", args);
    assertThat(first.getBoolean("isError", true)).isFalse();

    final JSONObject firstCountResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:NoSetPerson {name:'Solo'}) RETURN count(n) AS c"));
    final JSONObject firstCountPayload = new JSONObject(
        firstCountResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(firstCountPayload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);

    // Repeat with identical args (still no setProperties): the MERGE must match, not duplicate.
    callTool("upsert_entity", new JSONObject(args.toString()));

    final JSONObject secondCountResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:NoSetPerson {name:'Solo'}) RETURN count(n) AS c"));
    final JSONObject secondCountPayload = new JSONObject(
        secondCountResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(secondCountPayload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
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
        .put("typeName", "UpsertPerson")
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
        .put("typeName", "UpsertPerson")
        .put("matchKeys", new JSONObject()));

    assertThat(resp.getBoolean("isError")).isTrue();
    final String text = resp.getJSONArray("content").getJSONObject(0).getString("text");
    assertThat(text).contains("matchKeys");
  }

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

    // The second upsert_relationship call must have updated the existing edge's property.
    final JSONObject yearResp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (:Author {name:'Ada'})-[r:WROTE]->(:Book {isbn:'111'}) RETURN r.year AS y"));
    final JSONObject yearPayload = new JSONObject(yearResp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(yearPayload.getJSONArray("records").getJSONObject(0).getInt("y")).isEqualTo(1844);
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

  @Test
  void upsertRelationshipRejectsEmptyMatchKeys() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_relationship", new JSONObject()
        .put("database", "graph")
        .put("fromType", "Author")
        .put("fromMatchKeys", new JSONObject())
        .put("toType", "Book")
        .put("toMatchKeys", new JSONObject().put("isbn", "111"))
        .put("relType", "WROTE"));

    assertThat(resp.getBoolean("isError")).isTrue();
    assertThat(resp.getJSONArray("content").getJSONObject(0).getString("text")).contains("fromMatchKeys");
  }

  @Test
  void upsertEntityRejectsBacktickIdentifier() throws Exception {
    // The backtick guard lives in quoteIdentifier; this asserts it is actually wired into the tool path.
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject resp = callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "Bad`Type")
        .put("matchKeys", new JSONObject().put("id", "1")));

    assertThat(resp.getBoolean("isError")).isTrue();
    assertThat(resp.getJSONArray("content").getJSONObject(0).getString("text")).contains("backtick");
  }

  @Test
  void upsertEntityCompositeMatchKeysIsIdempotent() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final JSONObject args = new JSONObject()
        .put("database", "graph")
        .put("typeName", "CompositePerson")
        .put("matchKeys", new JSONObject().put("firstName", "Ada").put("lastName", "Lovelace"))
        .put("setProperties", new JSONObject().put("role", "mathematician"));

    callTool("upsert_entity", args);
    // Repeat with the same two-key match: must resolve to the same node, not create a second.
    callTool("upsert_entity", new JSONObject(args.toString())
        .put("setProperties", new JSONObject().put("role", "pioneer")));

    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (p:CompositePerson {firstName:'Ada', lastName:'Lovelace'}) RETURN count(p) AS c"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").getJSONObject(0).getInt("c")).isEqualTo(1);
  }

  @Test
  void upsertEntityBindsSetPropertyValuesSoInjectionIsInert() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("allowUpdate", true)
        .put("allowedUsers", new JSONArray().put("root")));

    final String malicious = "'}) DETACH DELETE n //";
    callTool("upsert_entity", new JSONObject()
        .put("database", "graph")
        .put("typeName", "SetInjTest")
        .put("matchKeys", new JSONObject().put("id", "1"))
        .put("setProperties", new JSONObject().put("note", malicious)));

    // The node still exists and stores the payload verbatim, proving the SET value was bound, not executed.
    final JSONObject resp = callTool("query", new JSONObject()
        .put("database", "graph")
        .put("language", "cypher")
        .put("query", "MATCH (n:SetInjTest {id:'1'}) RETURN n.note AS note"));
    final JSONObject payload = new JSONObject(resp.getJSONArray("content").getJSONObject(0).getString("text"));
    assertThat(payload.getJSONArray("records").length()).isEqualTo(1);
    assertThat(payload.getJSONArray("records").getJSONObject(0).getString("note")).isEqualTo(malicious);
  }

  @Test
  void initializeAdvertisesPromptsCapability() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 500)
        .put("method", "initialize")
        .put("params", new JSONObject()));

    final JSONObject capabilities = response.getJSONObject("result").getJSONObject("capabilities");
    assertThat(capabilities.has("prompts")).isTrue();
    assertThat(capabilities.getJSONObject("prompts").getBoolean("listChanged")).isFalse();
  }

  @Test
  void promptsListReturnsBothPromptsWhenWritesEnabled() throws Exception {
    saveMCPConfig(new JSONObject().put("allowInsert", true).put("allowUpdate", true));

    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 501)
        .put("method", "prompts/list")
        .put("params", new JSONObject()));

    final JSONArray prompts = response.getJSONObject("result").getJSONArray("prompts");
    final Set<String> names = new HashSet<>();
    for (int i = 0; i < prompts.length(); i++) {
      names.add(prompts.getJSONObject(i).getString("name"));
      assertThat(prompts.getJSONObject(i).getJSONArray("arguments").length()).isEqualTo(2);
    }

    assertThat(names).containsExactlyInAnyOrder("graphrag_query", "build_knowledge_graph");
  }

  @Test
  void promptsListHidesTheWritePromptWithoutWritePermissions() throws Exception {
    saveMCPConfig(new JSONObject().put("allowInsert", false).put("allowUpdate", false));

    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 502)
        .put("method", "prompts/list")
        .put("params", new JSONObject()));

    final JSONArray prompts = response.getJSONObject("result").getJSONArray("prompts");
    assertThat(prompts.length()).isEqualTo(1);
    assertThat(prompts.getJSONObject(0).getString("name")).isEqualTo("graphrag_query");
  }

  @Test
  void promptsListIsEmptyUnderTheAdminProfile() throws Exception {
    saveMCPConfig(new JSONObject().put("profile", "admin"));

    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 503)
        .put("method", "prompts/list")
        .put("params", new JSONObject()));

    assertThat(response.getJSONObject("result").getJSONArray("prompts").length()).isZero();
  }

  @Test
  void promptsGetRendersTheSubstitutedTemplate() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 504)
        .put("method", "prompts/get")
        .put("params", new JSONObject()
            .put("name", "graphrag_query")
            .put("arguments", new JSONObject()
                .put("database", "graph")
                .put("question", "Which papers cite Codd?"))));

    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getString("description")).isNotEmpty();

    final JSONArray messages = result.getJSONArray("messages");
    assertThat(messages.length()).isEqualTo(1);
    assertThat(messages.getJSONObject(0).getString("role")).isEqualTo("user");

    final String text = messages.getJSONObject(0).getJSONObject("content").getString("text");
    assertThat(text)
        .contains("'graph'")
        .contains("Which papers cite Codd?")
        .contains("vector_search")
        .doesNotContain("{database}", "{question}");
  }

  @Test
  void promptsGetRefusesAPromptHiddenByTheProfile() throws Exception {
    saveMCPConfig(new JSONObject().put("profile", "admin"));

    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 505)
        .put("method", "prompts/get")
        .put("params", new JSONObject()
            .put("name", "graphrag_query")
            .put("arguments", new JSONObject()
                .put("database", "graph")
                .put("question", "Which papers cite Codd?"))));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32600);
    assertThat(response.getJSONObject("error").getString("message")).contains("graphrag_query");
  }

  @Test
  void promptsGetRejectsAMissingArgument() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 506)
        .put("method", "prompts/get")
        .put("params", new JSONObject()
            .put("name", "graphrag_query")
            .put("arguments", new JSONObject().put("database", "graph"))));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32602);
    assertThat(response.getJSONObject("error").getString("message")).contains("question");
  }

  @Test
  void promptsGetRejectsAnUnknownPromptName() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 507)
        .put("method", "prompts/get")
        .put("params", new JSONObject()
            .put("name", "nope")
            .put("arguments", new JSONObject())));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32602);
    assertThat(response.getJSONObject("error").getString("message")).contains("Unknown prompt");
  }

  /**
   * A malformed 'arguments' member is a client mistake, so it must answer -32602 rather than -32603: the request
   * never reaches the prompt, so nothing internal went wrong. Reading it defaults only on an absent or null
   * member, so a JSON value of the wrong shape raises instead, which is why the read happens where the handler
   * can answer for it.
   */
  @Test
  void promptsGetRejectsNonObjectArguments() throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 508)
        .put("method", "prompts/get")
        .put("params", new JSONObject()
            .put("name", "graphrag_query")
            .put("arguments", new JSONArray().put("database").put("graph"))));

    assertThat(response.has("error")).isTrue();
    assertThat(response.getJSONObject("error").getInt("code")).isEqualTo(-32602);
  }

  /**
   * Mirrors {@link #principalProfilesDifferentiateNamedUsersOnOneHttpEndpoint()} for the Prompts surface: the
   * global profile alone is "all", under which both prompts name only tools that are reachable, so root sees
   * both. The principal's own profile is "admin", which registers neither the search tools graphrag_query names
   * nor the upsert tools build_knowledge_graph names. If prompts/list consulted only the global profile, the
   * principal would see the same two prompts as root; proving the principal sees none shows the effective
   * profile is the intersection, not the global profile alone.
   */
  @Test
  void principalProfilesFilterPromptsListByTheIntersectedProfile() throws Exception {
    final String principalName = "mcp-prompt-admin";
    final String password = "principalPromptPass1!";
    if (getServer(0).getSecurity().existsUser(principalName))
      getServer(0).getSecurity().dropUser(principalName);
    getServer(0).getSecurity().createUser(new JSONObject()
        .put("name", principalName)
        .put("password", getServer(0).getSecurity().encodePassword(password))
        .put("databases", new JSONObject().put("graph", new JSONArray().put("admin"))));

    try {
      saveMCPConfig(new JSONObject()
          .put("profile", "all")
          .put("allowReads", true)
          .put("allowInsert", true)
          .put("allowUpdate", true)
          .put("allowedUsers", new JSONArray().put("root").put(principalName))
          .put("principalProfiles", new JSONObject().put(principalName, "admin")));

      final JSONObject request = new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 508)
          .put("method", "prompts/list")
          .put("params", new JSONObject());

      final JSONArray rootPrompts = mcpRequest(request).getJSONObject("result").getJSONArray("prompts");
      final Set<String> rootNames = new HashSet<>();
      for (int i = 0; i < rootPrompts.length(); i++)
        rootNames.add(rootPrompts.getJSONObject(i).getString("name"));
      assertThat(rootNames).containsExactlyInAnyOrder("graphrag_query", "build_knowledge_graph");

      final String principalAuth = getBasicAuth(principalName, password);
      final JSONArray principalPrompts = mcpRequest(request, principalAuth).getJSONObject("result").getJSONArray("prompts");
      assertThat(principalPrompts.length()).isZero();

      final JSONObject denied = mcpRequest(new JSONObject()
          .put("jsonrpc", "2.0")
          .put("id", 509)
          .put("method", "prompts/get")
          .put("params", new JSONObject()
              .put("name", "graphrag_query")
              .put("arguments", new JSONObject()
                  .put("database", "graph")
                  .put("question", "Which papers cite Codd?"))), principalAuth);
      assertThat(denied.has("error")).isTrue();
      assertThat(denied.getJSONObject("error").getInt("code")).isEqualTo(-32600);
    } finally {
      getServer(0).getSecurity().dropUser(principalName);
    }
  }

  // ---- Helper methods ----

  private JSONObject mcpRequest(final JSONObject request) throws Exception {
    return mcpRequest(request, getBasicAuth());
  }

  private JSONObject mcpRequest(final JSONObject request, final String authorization) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", authorization);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final byte[] data = request.toString().getBytes(StandardCharsets.UTF_8);
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }

    connection.connect();

    try {
      final String body = FileUtils.readStreamAsString(connection.getInputStream(), "utf8");
      return new JSONObject(body);
    } finally {
      connection.disconnect();
    }
  }

  private JSONObject callTool(final String toolName, final JSONObject arguments) throws Exception {
    final JSONObject response = mcpRequest(new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 10)
        .put("method", "tools/call")
        .put("params", new JSONObject()
            .put("name", toolName)
            .put("arguments", arguments)));

    assertThat(response.has("result")).isTrue();
    return response.getJSONObject("result");
  }

  private static Set<String> toolNames(final JSONObject response) {
    final Set<String> names = new HashSet<>();
    final JSONArray tools = response.getJSONObject("result").getJSONArray("tools");
    for (int i = 0; i < tools.length(); i++)
      names.add(tools.getJSONObject(i).getString("name"));
    return names;
  }

  private void saveMCPConfig(final JSONObject config) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getMcpConfigUrl()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final byte[] data = config.toString().getBytes(StandardCharsets.UTF_8);
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }

    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private static String getBasicAuth() {
    return getBasicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private static String getBasicAuth(final String username, final String password) {
    return "Basic " + Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
