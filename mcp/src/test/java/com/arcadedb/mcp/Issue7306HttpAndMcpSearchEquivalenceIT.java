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
import com.arcadedb.mcp.tools.FullTextSearchTool;
import com.arcadedb.mcp.tools.HybridSearchTool;
import com.arcadedb.mcp.tools.VectorSearchTool;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.server.vector.FullTextQuery;
import com.arcadedb.server.vector.VectorLeg;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7306 asks for equivalence between the HTTP endpoint and the corresponding MCP tool for the same query,
 * and this is the only module that can see both.
 * <p>
 * The assertions are deliberately on the whole response document rather than on a field or two. Two surfaces
 * that agree on the ranking but disagree on {@code truncated}, {@code candidateLimit} or {@code scoring} are
 * still two surfaces a caller has to learn separately, which is the divergence the issue is about; and comparing
 * the documents is what makes this test notice a field added to one and not the other.
 */
public class Issue7306HttpAndMcpSearchEquivalenceIT extends BaseGraphServerTest {
  private static final String DENSE_TYPE  = "Eq7306";
  private static final String DENSE_INDEX = "Eq7306[embedding]";
  private static final String TEXT_INDEX  = "Eq7306[text]";

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @Override
  protected void populateDatabase() {
    super.populateDatabase();

    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + DENSE_TYPE + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".name STRING");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".text STRING");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".embedding ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + DENSE_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");
      db.command("sql", "CREATE INDEX ON " + DENSE_TYPE + " (text) FULL_TEXT");

      db.newDocument(DENSE_TYPE).set("name", "near").set("text", "alpha beta")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      db.newDocument(DENSE_TYPE).set("name", "mid").set("text", "beta gamma")
          .set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
      db.newDocument(DENSE_TYPE).set("name", "far").set("text", "gamma delta")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
    });
  }

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void vectorSearchAnswersTheSameDocumentOnHttpAndOnMcp() throws Exception {
    final JSONObject args = new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", 3);

    final JSONObject viaHttp = post("search", args);
    final JSONObject viaMcp = VectorSearchTool.execute(getServer(0), user, withDatabase(args), config);

    assertThat(viaHttp.toString()).isEqualTo(viaMcp.toString());
  }

  @Test
  void aFilteredVectorSearchAnswersTheSameDocumentOnHttpAndOnMcp() throws Exception {
    final JSONObject args = new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", 2)
        .put("efSearch", 64)
        .put("filter", "name <> 'mid'");

    assertThat(post("search", args).toString())
        .isEqualTo(VectorSearchTool.execute(getServer(0), user, withDatabase(args), config).toString());
  }

  @Test
  void fullTextSearchAnswersTheSameDocumentOnHttpAndOnMcp() throws Exception {
    final JSONObject args = new JSONObject()
        .put("indexName", TEXT_INDEX)
        .put("queryText", "beta")
        .put("limit", 5);

    assertThat(post("fulltext", args).toString())
        .isEqualTo(FullTextSearchTool.execute(getServer(0), user, withDatabase(args), config).toString());
  }

  @Test
  void hybridSearchAnswersTheSameDocumentOnHttpAndOnMcp() throws Exception {
    final JSONObject args = new JSONObject()
        .put("vectorIndexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("fulltextIndexName", TEXT_INDEX)
        .put("fulltextQuery", "gamma")
        .put("k", 3);

    assertThat(post("hybrid", args).toString())
        .isEqualTo(HybridSearchTool.execute(getServer(0), user, withDatabase(args), config).toString());
  }

  /**
   * The bounds are the half of the equivalence that is easy to lose: a surface that validated its own arguments
   * would drift the first time a limit changed. Both surfaces must refuse the same request with the same words.
   */
  @Test
  void bothSurfacesRefuseTheSameOutOfBoundsRequestWithTheSameWords() throws Exception {
    final JSONObject overK = new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", VectorLeg.MAX_K + 1);

    final HttpResponse<String> http = rawPost("search", overK);
    assertThat(http.statusCode()).isEqualTo(400);
    assertThat(http.body()).contains("'k' must be between 1 and " + VectorLeg.MAX_K);

    assertThatThrownBy(() -> VectorSearchTool.execute(getServer(0), user, withDatabase(overK), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'k' must be between 1 and " + VectorLeg.MAX_K);

    final JSONObject overLimit = new JSONObject()
        .put("indexName", TEXT_INDEX)
        .put("queryText", "beta")
        .put("limit", FullTextQuery.MAX_LIMIT + 1);

    final HttpResponse<String> httpLimit = rawPost("fulltext", overLimit);
    assertThat(httpLimit.statusCode()).isEqualTo(400);
    assertThat(httpLimit.body()).contains("'limit' must be between 1 and " + FullTextQuery.MAX_LIMIT);

    assertThatThrownBy(() -> FullTextSearchTool.execute(getServer(0), user, withDatabase(overLimit), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'limit' must be between 1 and " + FullTextQuery.MAX_LIMIT);
  }

  /**
   * The MCP tool schemas must advertise the bounds the search enforces. They are re-exported from the shared
   * implementation rather than written out, and this is what proves the re-export is wired.
   */
  @Test
  void theMcpToolSchemasAdvertiseTheSharedBounds() {
    final JSONObject vectorK = VectorSearchTool.getDefinition()
        .getJSONObject("inputSchema").getJSONObject("properties").getJSONObject("k");
    assertThat(vectorK.getInt("maximum")).isEqualTo(VectorLeg.MAX_K);
    assertThat(vectorK.getInt("default")).isEqualTo(VectorLeg.DEFAULT_K);

    assertThat(VectorSearchTool.getDefinition().getJSONObject("inputSchema").getJSONObject("properties")
        .getJSONObject("efSearch").getInt("maximum")).isEqualTo(VectorLeg.MAX_EF_SEARCH);

    assertThat(FullTextSearchTool.getDefinition().getJSONObject("inputSchema").getJSONObject("properties")
        .getJSONObject("limit").getInt("maximum")).isEqualTo(FullTextQuery.MAX_LIMIT);

    assertThat(HybridSearchTool.getDefinition().getJSONObject("inputSchema").getJSONObject("properties")
        .getJSONObject("k").getInt("maximum")).isEqualTo(VectorLeg.MAX_K);
  }

  // ───────────────────────────── plumbing ─────────────────────────────

  private JSONObject withDatabase(final JSONObject args) {
    return new JSONObject(args.toString()).put("database", getDatabaseName());
  }

  private HttpResponse<String> rawPost(final String operation, final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + getServer(0).getHttpServer().getPort()
            + "/api/v1/vector/" + getDatabaseName() + "/" + operation))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build()
        .send(request, HttpResponse.BodyHandlers.ofString());
  }

  private JSONObject post(final String operation, final JSONObject payload) throws Exception {
    final HttpResponse<String> response = rawPost(operation, payload);
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }
}
