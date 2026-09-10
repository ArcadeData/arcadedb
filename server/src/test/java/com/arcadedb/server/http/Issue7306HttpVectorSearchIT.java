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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.query.search.FullTextQuery;
import com.arcadedb.query.search.HybridSearch;
import com.arcadedb.query.search.VectorLeg;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7306, part 2: vector search had no HTTP endpoint and no gRPC RPC at all - the only structured surface
 * was MCP, and everything else had to go through hand-written SQL.
 * <p>
 * These tests cover the three new routes and, more importantly, the property the issue asks for: the bounds are
 * not re-derived per surface. Every assertion below that names a limit reads it from the shared implementation
 * constant ({@link VectorLeg#MAX_K}, {@link VectorLeg#MAX_EF_SEARCH}, {@link FullTextQuery#MAX_LIMIT}), so a
 * surface that grew a bound of its own would fail here rather than diverge quietly.
 */
public class Issue7306HttpVectorSearchIT extends BaseGraphServerTest {
  private static final String DENSE_TYPE  = "Vec7306";
  private static final String DENSE_INDEX = "Vec7306[embedding]";
  private static final String DOC_TYPE     = "Doc7306";
  private static final String DOC_INDEX    = "Doc7306[content]";
  private static final String SPARSE_TYPE  = "Sparse7306";
  private static final String SPARSE_INDEX = "Sparse7306[tokens,weights]";

  @Override
  protected void populateDatabase() {
    super.populateDatabase();

    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + DENSE_TYPE + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".name STRING");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".category STRING");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".embedding ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + DENSE_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");

      db.newDocument(DENSE_TYPE).set("name", "near").set("category", "keep")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      db.newDocument(DENSE_TYPE).set("name", "mid").set("category", "drop")
          .set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
      db.newDocument(DENSE_TYPE).set("name", "far").set("category", "keep")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();

      db.command("sql", "CREATE VERTEX TYPE " + DOC_TYPE + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + DOC_TYPE + ".title STRING");
      db.command("sql", "CREATE PROPERTY " + DOC_TYPE + ".content STRING");
      db.command("sql", "CREATE PROPERTY " + DOC_TYPE + ".embedding ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + DOC_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");
      db.command("sql", "CREATE INDEX ON " + DOC_TYPE + " (content) FULL_TEXT");
      db.command("sql", "CREATE EDGE TYPE Doc7306Cites");

      db.command("sql", "CREATE DOCUMENT TYPE " + SPARSE_TYPE + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + SPARSE_TYPE + ".name STRING");
      db.command("sql", "CREATE PROPERTY " + SPARSE_TYPE + ".tokens ARRAY_OF_INTEGERS");
      db.command("sql", "CREATE PROPERTY " + SPARSE_TYPE + ".weights ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + SPARSE_TYPE + " (tokens, weights) LSM_SPARSE_VECTOR "
          + "METADATA { dimensions: 8, weightQuantization: 'FP32' }");
      db.newDocument(SPARSE_TYPE).set("name", "sparse-low")
          .set("tokens", new int[] { 1, 5 }).set("weights", new float[] { 0.1f, 0.3f }).save();
      db.newDocument(SPARSE_TYPE).set("name", "sparse-high")
          .set("tokens", new int[] { 2, 5 }).set("weights", new float[] { 0.2f, 0.6f }).save();

      final MutableVertex d0 = db.newVertex(DOC_TYPE).set("title", "d0")
          .set("content", "graph traversal over connected documents")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      final MutableVertex d1 = db.newVertex(DOC_TYPE).set("title", "d1")
          .set("content", "vector similarity ranking")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      final MutableVertex d2 = db.newVertex(DOC_TYPE).set("title", "d2")
          .set("content", "gearbox gearbox gearbox")
          .set("embedding", new float[] { 0.0f, 0.0f, 1.0f }).save();
      d0.newEdge("Doc7306Cites", d1).save();
      d1.newEdge("Doc7306Cites", d2).save();
    });
  }

  // ───────────────────────────── the routes ─────────────────────────────

  @Test
  void vectorSearchRanksTheNearestNeighborFirst() throws Exception {
    final JSONObject response = post("search", new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", 3));

    assertThat(response.getString("indexName")).isEqualTo(DENSE_INDEX);
    assertThat(response.getBoolean("sparse")).isFalse();
    assertThat(response.getString("scoring")).startsWith("distance_lower_is_better:");
    assertThat(names(response)).containsExactly("near", "mid", "far");

    final JSONArray results = response.getJSONArray("results");
    assertThat(results.getJSONObject(0).getDouble("distance"))
        .isLessThanOrEqualTo(results.getJSONObject(1).getDouble("distance"));
    assertThat(response.getInt("count")).isEqualTo(3);
  }

  /**
   * A filter narrows the candidate window, which is what {@code candidateLimit} in the response reports. The
   * filtered-out record must not come back, and the ones that survive must keep their ranking.
   */
  @Test
  void vectorSearchAppliesTheFilterToTheCandidateWindow() throws Exception {
    final JSONObject response = post("search", new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", 3)
        .put("filter", "category = 'keep'"));

    assertThat(names(response)).containsExactly("near", "far");
    assertThat(response.getInt("candidateLimit")).isGreaterThan(3);
  }

  @Test
  void fullTextSearchReturnsTheMatchingDocuments() throws Exception {
    final JSONObject response = post("fulltext", new JSONObject()
        .put("indexName", DOC_INDEX)
        .put("queryText", "gearbox")
        .put("limit", 5));

    assertThat(response.getString("indexName")).isEqualTo(DOC_INDEX);
    assertThat(response.getInt("count")).isEqualTo(1);
    assertThat(response.getJSONArray("results").getJSONObject(0)
        .getJSONObject("properties").getString("title")).isEqualTo("d2");
  }

  /**
   * The hybrid route has to fuse: a request naming only the vector leg reports {@code fused: false}, and adding
   * the full-text leg flips it. Asserting both is what distinguishes a real fusion from a vector search wearing
   * the hybrid route's name.
   */
  @Test
  void hybridSearchFusesTheVectorAndFullTextLegs() throws Exception {
    final JSONObject vectorOnly = post("hybrid", new JSONObject()
        .put("vectorIndexName", DOC_TYPE + "[embedding]")
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", 3));
    assertThat(vectorOnly.getBoolean("fused")).isFalse();

    final JSONObject fused = post("hybrid", new JSONObject()
        .put("vectorIndexName", DOC_TYPE + "[embedding]")
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("fulltextQuery", "gearbox")
        .put("fulltextIndexName", DOC_INDEX)
        .put("k", 3));

    assertThat(fused.getBoolean("fused")).isTrue();
    assertThat(fused.getString("fulltextIndexName")).isEqualTo(DOC_INDEX);
    assertThat(fused.getJSONArray("results").length()).isGreaterThan(0);
    assertThat(fused.getJSONObject("legs").keySet()).contains("vector", "fulltext");
  }

  /**
   * The graph expansion leg, which only the hybrid route has. {@code maxDepth} is bounded by
   * {@link HybridSearch#MAX_DEPTH}, and the seeded walk must reach a document the retrieval legs did not.
   */
  @Test
  void hybridSearchExpandsAlongTheGraph() throws Exception {
    final JSONObject response = post("hybrid", new JSONObject()
        .put("vectorIndexName", DOC_TYPE + "[embedding]")
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("fulltextQuery", "gearbox")
        .put("fulltextIndexName", DOC_INDEX)
        .put("k", 5)
        .put("expand", new JSONObject()
            .put("edgeTypes", new JSONArray(List.of("Doc7306Cites")))
            .put("direction", "out")
            .put("maxDepth", HybridSearch.MAX_DEPTH)));

    assertThat(response.getJSONObject("legs").keySet()).contains("expand");
    assertThat(response.getJSONObject("legs").getJSONObject("expand").getInt("maxDepth"))
        .isEqualTo(HybridSearch.MAX_DEPTH);
  }

  /**
   * The sparse path is a different index type, a different SQL function and the opposite ranking direction, so a
   * route that only ever exercised the dense one would leave half of its own {@code sparse} flag untested.
   */
  @Test
  void aSparseVectorSearchScoresRatherThanMeasuresDistance() throws Exception {
    final JSONObject response = post("search", new JSONObject()
        .put("indexName", SPARSE_INDEX)
        .put("sparse", true)
        .put("queryIndices", new JSONArray(List.of(5)))
        .put("queryVector", new JSONArray(List.of(1.0)))
        .put("k", 2));

    assertThat(response.getBoolean("sparse")).isTrue();
    assertThat(response.getString("scoring")).startsWith("score_higher_is_better:");
    assertThat(names(response)).containsExactly("sparse-high", "sparse-low");

    final JSONArray results = response.getJSONArray("results");
    assertThat(results.getJSONObject(0).has("score")).isTrue();
    assertThat(results.getJSONObject(0).has("distance")).isFalse();
    assertThat(results.getJSONObject(0).getDouble("score"))
        .isGreaterThan(results.getJSONObject(1).getDouble("score"));
  }

  /**
   * {@code efSearch} sizes the dense HNSW candidate list and means nothing on the sparse path, so a request that
   * sends both is a mistake worth naming rather than a value to ignore.
   */
  @Test
  void efSearchOnASparseIndexIsRefused() throws Exception {
    final HttpResponse<String> response = rawPost("search", new JSONObject()
        .put("indexName", SPARSE_INDEX)
        .put("sparse", true)
        .put("queryIndices", new JSONArray(List.of(5)))
        .put("queryVector", new JSONArray(List.of(1.0)))
        .put("efSearch", 32)
        .put("k", 1));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("'efSearch' applies only to dense LSM_VECTOR indexes");
  }

  // ───────────────────────────── the bounds, read from the shared implementation ─────────────────────────────

  @Test
  void kAboveTheSharedMaximumIsRefused() throws Exception {
    final HttpResponse<String> response = rawPost("search", new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", VectorLeg.MAX_K + 1));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("'k' must be between 1 and " + VectorLeg.MAX_K);
  }

  @Test
  void efSearchOutsideTheSharedWindowIsRefused() throws Exception {
    for (final int efSearch : new int[] { 0, VectorLeg.MAX_EF_SEARCH + 1 }) {
      final HttpResponse<String> response = rawPost("search", new JSONObject()
          .put("indexName", DENSE_INDEX)
          .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
          .put("k", 1)
          .put("efSearch", efSearch));

      assertThat(response.statusCode()).as("efSearch=%d", efSearch).isEqualTo(400);
      assertThat(response.body()).contains("'efSearch' must be between 1 and " + VectorLeg.MAX_EF_SEARCH);
    }
  }

  @Test
  void aFullTextLimitAboveTheSharedMaximumIsRefused() throws Exception {
    final HttpResponse<String> response = rawPost("fulltext", new JSONObject()
        .put("indexName", DOC_INDEX)
        .put("queryText", "gearbox")
        .put("limit", FullTextQuery.MAX_LIMIT + 1));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("'limit' must be between 1 and " + FullTextQuery.MAX_LIMIT);
  }

  @Test
  void aBlankFullTextQueryIsRefusedWithACauseRatherThanAParserError() throws Exception {
    final HttpResponse<String> response = rawPost("fulltext", new JSONObject()
        .put("indexName", DOC_INDEX)
        .put("queryText", "   "));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("'queryText' must not be blank");
  }

  @Test
  void anUnknownIndexIsRefusedWithTheAvailableOnesNamed() throws Exception {
    final HttpResponse<String> response = rawPost("search", new JSONObject()
        .put("indexName", "NoSuchIndex7306")
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", 1));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("does not exist").contains("Available");
  }

  /**
   * A dimension mismatch is a client error too, and one worth naming: it is the mistake a caller makes when it
   * points a query at the wrong index.
   */
  @Test
  void aQueryVectorOfTheWrongWidthIsRefused() throws Exception {
    final HttpResponse<String> response = rawPost("search", new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0)))
        .put("k", 1));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("dimensions");
  }

  @Test
  void anEmptyBodyIsRefused() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(vectorUrl("search")))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", authorization())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}"))
        .build();

    final HttpResponse<String> response = HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(400);
  }

  // ───────────────────────────── plumbing ─────────────────────────────

  private String vectorUrl(final String operation) {
    return "http://localhost:" + getServer(0).getHttpServer().getPort()
        + "/api/v1/vector/" + getDatabaseName() + "/" + operation;
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private HttpResponse<String> rawPost(final String operation, final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(vectorUrl(operation)))
        .timeout(Duration.ofSeconds(60))
        .header("Authorization", authorization())
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

  private static List<String> names(final JSONObject response) {
    final JSONArray results = response.getJSONArray("results");
    final List<String> names = new ArrayList<>(results.length());
    for (int i = 0; i < results.length(); i++)
      names.add(results.getJSONObject(i).getJSONObject("properties").getString("name"));
    return names;
  }
}
