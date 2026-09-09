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
package com.arcadedb.server.vector;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.search.FullTextSearchOperation;
import com.arcadedb.query.search.HybridSearchOperation;
import com.arcadedb.query.search.VectorSearchLeg;
import com.arcadedb.query.search.VectorSearchOperation;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7306: vector search had no HTTP endpoint and no gRPC RPC - the proto matched "vector" zero times and
 * {@code HttpServer} registered no route - so the only structured access was MCP and everything else had to go
 * through hand-written {@code vector.neighbors(...)} SQL.
 * <p>
 * These tests drive the three new routes over real HTTP against a real dense index, a real sparse index and a real
 * full-text index. The equivalence tests are the ones that matter most: they assert that the HTTP route and the
 * shared {@code com.arcadedb.query.search} operation - the same code the MCP tool and the gRPC RPC call - return
 * the same answer for the same request, which is what the issue asked for when it said the surfaces must not be
 * able to disagree.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7306VectorSearchEndpointsIT extends BaseGraphServerTest {

  private final HttpClient client = HttpClient.newHttpClient();

  @BeforeEach
  void seedSearchIndexes() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (db.getSchema().existsType("V7306Doc"))
      return;

    db.transaction(() -> {
      db.command("sql", "CREATE VERTEX TYPE V7306Doc BUCKETS 1");
      db.command("sql", "CREATE PROPERTY V7306Doc.title STRING");
      db.command("sql", "CREATE PROPERTY V7306Doc.content STRING");
      db.command("sql", "CREATE PROPERTY V7306Doc.embedding ARRAY_OF_FLOATS");
      db.command("sql", """
          CREATE INDEX ON V7306Doc (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE' }
          """);
      db.command("sql", "CREATE INDEX ON V7306Doc (content) FULL_TEXT");
      db.command("sql", "CREATE EDGE TYPE V7306Cites");

      final MutableVertex d0 = db.newVertex("V7306Doc").set("title", "d0")
          .set("content", "gearbox maintenance schedule")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      final MutableVertex d1 = db.newVertex("V7306Doc").set("title", "d1")
          .set("content", "vector similarity ranking")
          .set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
      final MutableVertex d2 = db.newVertex("V7306Doc").set("title", "d2")
          .set("content", "reciprocal rank fusion")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      d0.newEdge("V7306Cites", d2).save();
      d1.newEdge("V7306Cites", d2).save();

      db.command("sql", "CREATE DOCUMENT TYPE V7306Sparse BUCKETS 1");
      db.command("sql", "CREATE PROPERTY V7306Sparse.name STRING");
      db.command("sql", "CREATE PROPERTY V7306Sparse.tokens ARRAY_OF_INTEGERS");
      db.command("sql", "CREATE PROPERTY V7306Sparse.weights ARRAY_OF_FLOATS");
      db.command("sql", """
          CREATE INDEX ON V7306Sparse (tokens, weights) LSM_SPARSE_VECTOR
          METADATA { dimensions: 8, weightQuantization: 'FP32' }
          """);
      db.newDocument("V7306Sparse").set("name", "sparse-low")
          .set("tokens", new int[] { 1, 5 }).set("weights", new float[] { 0.1f, 0.3f }).save();
      db.newDocument("V7306Sparse").set("name", "sparse-high")
          .set("tokens", new int[] { 2, 5 }).set("weights", new float[] { 0.2f, 0.6f }).save();
    });
  }

  // --------------------------------------------------------------------------------------------
  // POST /api/v1/vector/{database}/search
  // --------------------------------------------------------------------------------------------

  @Test
  void denseVectorSearchRanksByDistanceAndReportsItsScoring() throws Exception {
    final HttpResponse<String> response = post("search", new JSONObject()
        .put("indexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("k", 2));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getString("scoring")).startsWith("distance_lower_is_better:");
    assertThat(body.getBoolean("sparse")).isFalse();
    assertThat(body.getInt("count")).isEqualTo(2);

    final JSONArray results = body.getJSONArray("results");
    // d0 IS the probe vector, so it must rank first and no other row may tie it: an endpoint that returned the
    // right records in the wrong order would still pass a count-only assertion.
    assertThat(results.getJSONObject(0).getJSONObject("properties").getString("title")).isEqualTo("d0");
    assertThat(results.getJSONObject(0).getDouble("distance"))
        .isLessThan(results.getJSONObject(1).getDouble("distance"));
  }

  @Test
  void sparseVectorSearchRanksByScore() throws Exception {
    final HttpResponse<String> response = post("search", new JSONObject()
        .put("indexName", "V7306Sparse[tokens,weights]")
        .put("queryVector", new JSONArray().put(1.0f))
        .put("queryIndices", new JSONArray().put(5))
        .put("sparse", true)
        .put("k", 2));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getBoolean("sparse")).isTrue();
    assertThat(body.getString("scoring")).startsWith("score_higher_is_better:");

    final JSONArray results = body.getJSONArray("results");
    assertThat(results.length()).isEqualTo(2);
    // Higher is better on the sparse path, which is the opposite of the dense one: sparse-high carries weight 0.6
    // against sparse-low's 0.3 on the shared dimension 5.
    assertThat(results.getJSONObject(0).getJSONObject("properties").getString("name")).isEqualTo("sparse-high");
    assertThat(results.getJSONObject(0).getDouble("score"))
        .isGreaterThan(results.getJSONObject(1).getDouble("score"));
  }

  /**
   * The HTTP route and the shared operation are the same search. Asserting they agree is what keeps the route from
   * quietly growing its own copy of the ranking or of the bounds, which is the divergence the issue named.
   */
  @Test
  void theHttpRouteAnswersExactlyWhatTheSharedOperationAnswers() throws Exception {
    final JSONObject request = new JSONObject()
        .put("indexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(0.5f).put(0.5f).put(0.0f))
        .put("k", 3);

    final JSONObject overHttp = new JSONObject(post("search", request).body());
    final JSONObject direct = VectorSearchOperation.execute(getServerDatabase(0, getDatabaseName()),
        new JSONObject(request.toString()));

    assertThat(overHttp.toString()).isEqualTo(direct.toString());
  }

  @Test
  void aFilterNarrowsTheCandidateWindowAndIsReported() throws Exception {
    final HttpResponse<String> response = post("search", new JSONObject()
        .put("indexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("filter", "title = 'd2'")
        .put("k", 3));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    // The over-fetch is what makes a filtered search able to find anything at all, and reporting it is what lets
    // a caller tell "nothing matched" from "the window was too small".
    assertThat(body.getInt("candidateLimit")).isGreaterThan(3);
    assertThat(body.getJSONArray("results").length()).isEqualTo(1);
    assertThat(body.getJSONArray("results").getJSONObject(0).getJSONObject("properties").getString("title"))
        .isEqualTo("d2");
  }

  // --------------------------------------------------------------------------------------------
  // Bounds - the same numbers the MCP tools enforce, reached through the HTTP route
  // --------------------------------------------------------------------------------------------

  @Test
  void anEfSearchAboveTheSharedCeilingIsRefusedWithFourHundred() throws Exception {
    final HttpResponse<String> response = post("search", new JSONObject()
        .put("indexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("k", 1)
        .put("efSearch", VectorSearchLeg.MAX_EF_SEARCH + 1));

    assertThat(response.statusCode()).isEqualTo(400);
    // The message is the operation's own, not one this route re-derived: that is the whole point of sharing it.
    assertThat(response.body()).contains("'efSearch' must be between 1 and " + VectorSearchLeg.MAX_EF_SEARCH);
  }

  @Test
  void aKAboveTheSharedCeilingIsRefusedWithFourHundred() throws Exception {
    final HttpResponse<String> response = post("search", new JSONObject()
        .put("indexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("k", VectorSearchLeg.MAX_K + 1));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("'k' must be between 1 and " + VectorSearchLeg.MAX_K);
  }

  @Test
  void aLimitAboveTheSharedCeilingIsRefusedOnTheFullTextRoute() throws Exception {
    final HttpResponse<String> response = post("fulltext", new JSONObject()
        .put("indexName", "V7306Doc[content]")
        .put("queryText", "gearbox")
        .put("limit", FullTextSearchOperation.MAX_LIMIT + 1));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("'limit' must be between 1 and " + FullTextSearchOperation.MAX_LIMIT);
  }

  @Test
  void anUnknownIndexIsRefusedWithARecoveryHint() throws Exception {
    final HttpResponse<String> response = post("search", new JSONObject()
        .put("indexName", "V7306Doc[nosuchproperty]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("k", 1));

    assertThat(response.statusCode()).isEqualTo(400);
    // Naming the available indexes is what lets a caller self-correct without a second round trip.
    assertThat(response.body()).contains("V7306Doc[embedding]");
  }

  @Test
  void anEmptyBodyIsRefusedRatherThanTreatedAsAnEmptyRequest() throws Exception {
    final HttpRequest request = authenticated("vector/" + getDatabaseName() + "/search")
        .POST(HttpRequest.BodyPublishers.ofString(""))
        .header("Content-Type", "application/json")
        .build();
    final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(400);
  }

  // --------------------------------------------------------------------------------------------
  // POST /api/v1/vector/{database}/fulltext
  // --------------------------------------------------------------------------------------------

  @Test
  void fullTextSearchReturnsScoredMatches() throws Exception {
    final HttpResponse<String> response = post("fulltext", new JSONObject()
        .put("indexName", "V7306Doc[content]")
        .put("queryText", "gearbox"));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getString("indexName")).isEqualTo("V7306Doc[content]");
    assertThat(body.getString("similarity")).isNotBlank();
    assertThat(body.getInt("count")).isEqualTo(1);
    assertThat(body.getJSONArray("results").getJSONObject(0).getJSONObject("properties").getString("title"))
        .isEqualTo("d0");
    assertThat(body.getJSONArray("results").getJSONObject(0).getDouble("score")).isGreaterThan(0.0);
  }

  @Test
  void theFullTextRouteAnswersExactlyWhatTheSharedOperationAnswers() throws Exception {
    final JSONObject request = new JSONObject()
        .put("typeName", "V7306Doc")
        .put("properties", new JSONArray().put("content"))
        .put("queryText", "ranking fusion");

    final JSONObject overHttp = new JSONObject(post("fulltext", request).body());
    final JSONObject direct = FullTextSearchOperation.execute(getServerDatabase(0, getDatabaseName()),
        new JSONObject(request.toString()));

    assertThat(overHttp.toString()).isEqualTo(direct.toString());
  }

  // --------------------------------------------------------------------------------------------
  // POST /api/v1/vector/{database}/hybrid
  // --------------------------------------------------------------------------------------------

  @Test
  void hybridSearchFusesTheVectorAndFullTextLegs() throws Exception {
    final HttpResponse<String> response = post("hybrid", new JSONObject()
        .put("vectorIndexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(0.0f).put(1.0f).put(0.0f))
        .put("fulltextIndexName", "V7306Doc[content]")
        .put("fulltextQuery", "gearbox")
        .put("k", 3));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    assertThat(body.getBoolean("fused")).isTrue();
    assertThat(body.getString("fusionStrategy")).isEqualTo("RRF");
    assertThat(body.getString("fulltextIndexName")).isEqualTo("V7306Doc[content]");
    // Both legs must have contributed, otherwise "fused" would be true over a single source and the assertion
    // above would be passing for the wrong reason.
    assertThat(body.getJSONObject("legs").getJSONObject("vector").getInt("count")).isGreaterThan(0);
    assertThat(body.getJSONObject("legs").getJSONObject("fulltext").getInt("count")).isGreaterThan(0);
    assertThat(body.getJSONArray("results").getJSONObject(0).getJSONArray("sources").length()).isGreaterThan(0);
  }

  @Test
  void hybridSearchWithOnlyTheVectorLegReportsThatItDidNotFuse() throws Exception {
    final HttpResponse<String> response = post("hybrid", new JSONObject()
        .put("vectorIndexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("k", 2));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    // Fusion needs two sources. Reporting the leg's native distance instead of a fabricated fused score is what
    // keeps a single-leg answer honest.
    assertThat(body.getBoolean("fused")).isFalse();
    assertThat(body.getJSONArray("results").getJSONObject(0).has("distance")).isTrue();
    assertThat(body.getJSONArray("results").getJSONObject(0).has("fusedScore")).isFalse();
  }

  @Test
  void hybridSearchExpandsTheGraphAndReportsTheSeedBudget() throws Exception {
    final HttpResponse<String> response = post("hybrid", new JSONObject()
        .put("vectorIndexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("fulltextIndexName", "V7306Doc[content]")
        .put("fulltextQuery", "gearbox")
        .put("expand", new JSONObject().put("direction", "out").put("maxDepth", 1))
        .put("k", 5));

    assertThat(response.statusCode()).isEqualTo(200);
    final JSONObject body = new JSONObject(response.body());
    final JSONObject expandLeg = body.getJSONObject("legs").getJSONObject("expand");
    assertThat(expandLeg.getInt("seedCount")).isGreaterThan(0);
    assertThat(expandLeg.getString("direction")).isEqualTo("out");
    assertThat(expandLeg.getInt("maxDepth")).isEqualTo(1);
  }

  @Test
  void aScoreBasedFusionCombinedWithTheRankOnlyExpansionLegIsRefused() throws Exception {
    final HttpResponse<String> response = post("hybrid", new JSONObject()
        .put("vectorIndexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("expand", new JSONObject().put("maxDepth", 1))
        .put("fusionStrategy", "DBSF")
        .put("k", 3));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("needs a score on every row");
  }

  @Test
  void anExpansionDepthAboveTheServerCapIsRefused() throws Exception {
    final HttpResponse<String> response = post("hybrid", new JSONObject()
        .put("vectorIndexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("expand", new JSONObject().put("maxDepth", HybridSearchOperation.MAX_DEPTH + 1))
        .put("k", 3));

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body()).contains("expand.maxDepth must be between 1 and " + HybridSearchOperation.MAX_DEPTH);
  }

  @Test
  void theHybridRouteAnswersExactlyWhatTheSharedOperationAnswers() throws Exception {
    final JSONObject request = new JSONObject()
        .put("vectorIndexName", "V7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(0.0f).put(1.0f).put(0.0f))
        .put("fulltextIndexName", "V7306Doc[content]")
        .put("fulltextQuery", "gearbox")
        .put("k", 3);

    final JSONObject overHttp = new JSONObject(post("hybrid", request).body());
    final JSONObject direct = HybridSearchOperation.execute(getServerDatabase(0, getDatabaseName()),
        new JSONObject(request.toString()));

    assertThat(overHttp.toString()).isEqualTo(direct.toString());
  }

  // --------------------------------------------------------------------------------------------

  private HttpResponse<String> post(final String route, final JSONObject payload) throws Exception {
    final HttpRequest request = authenticated("vector/" + getDatabaseName() + "/" + route)
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .header("Content-Type", "application/json")
        .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  /**
   * The port is read from the running server rather than hard-coded, because
   * {@code arcadedb.server.httpIncomingPort} defaults to the RANGE 2480-2489: with anything already listening on
   * 2480 the test server binds 2481, and a request sent to 2480 anyway reaches the other process and comes back as
   * an authentication failure rather than as the port conflict it actually is.
   */
  private HttpRequest.Builder authenticated(final String path) throws IOException {
    return HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + getServer(0).getHttpServer().getPort() + "/api/v1/" + path))
        .setHeader("Authorization", "Basic "
            + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
  }
}
