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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.FullTextSearchRequest;
import com.arcadedb.server.grpc.FullTextSearchResponse;
import com.arcadedb.server.grpc.HybridSearchRequest;
import com.arcadedb.server.grpc.HybridSearchResponse;
import com.arcadedb.server.grpc.SearchHit;
import com.arcadedb.server.grpc.VectorSearchRequest;
import com.arcadedb.server.grpc.VectorSearchResponse;
import com.arcadedb.server.vector.FullTextQuery;
import com.arcadedb.server.vector.VectorLeg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7306: before this change the proto matched "vector" zero times, so vector search had no gRPC surface at
 * all. These tests exercise the three new RPCs and, more importantly, check that they answer the same thing the
 * HTTP routes do for the same query - the asymmetry the issue is about would simply have moved one protocol
 * down if the two had been implemented separately.
 */
public class Issue7306GrpcVectorSearchIT extends BaseGraphServerTest {
  private static final String DENSE_TYPE  = "GrpcVec7306";
  private static final String DENSE_INDEX = "GrpcVec7306[embedding]";
  private static final String TEXT_INDEX  = "GrpcVec7306[text]";

  private RemoteGrpcServer   server;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

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
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(server, "localhost", 50051, getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null)
      database.close();
    if (server != null)
      server.close();
    super.endTest();
  }

  @Test
  void vectorSearchRanksTheNearestNeighborFirstAndAgreesWithHttp() throws Exception {
    final VectorSearchResponse response = database.vectorSearch(VectorSearchRequest.newBuilder()
        .setIndexName(DENSE_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(3)
        .build());

    assertThat(response.getIndexName()).isEqualTo(DENSE_INDEX);
    assertThat(response.getScoring()).startsWith("distance_lower_is_better:");
    assertThat(response.getCount()).isEqualTo(3);
    assertThat(names(response.getResultsList())).containsExactly("near", "mid", "far");
    // A dense hit carries a distance and no score: the two rank in opposite directions, so a client must be
    // able to tell which one it received.
    assertThat(response.getResults(0).hasDistance()).isTrue();
    assertThat(response.getResults(0).hasScore()).isFalse();

    final JSONObject viaHttp = postHttp("search", new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", 3));

    assertThat(viaHttp.getString("scoring")).isEqualTo(response.getScoring());
    assertThat(viaHttp.getInt("count")).isEqualTo(response.getCount());
    assertThat(viaHttp.getInt("candidateLimit")).isEqualTo(response.getCandidateLimit());
    assertThat(viaHttp.getBoolean("truncated")).isEqualTo(response.getTruncated());
    assertThat(httpNames(viaHttp)).isEqualTo(names(response.getResultsList()));
    for (int i = 0; i < response.getResultsCount(); i++)
      assertThat(response.getResults(i).getRid())
          .isEqualTo(viaHttp.getJSONArray("results").getJSONObject(i).getString("rid"));
  }

  @Test
  void fullTextSearchAgreesWithHttp() throws Exception {
    final FullTextSearchResponse response = database.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setIndexName(TEXT_INDEX)
        .setQueryText("beta")
        .setLimit(5)
        .build());

    assertThat(response.getIndexName()).isEqualTo(TEXT_INDEX);
    assertThat(response.getCount()).isEqualTo(2);
    // A full-text hit is scored, higher is better - the mirror image of the dense case above.
    assertThat(response.getResults(0).hasScore()).isTrue();
    assertThat(response.getResults(0).hasDistance()).isFalse();

    final JSONObject viaHttp = postHttp("fulltext", new JSONObject()
        .put("indexName", TEXT_INDEX)
        .put("queryText", "beta")
        .put("limit", 5));

    assertThat(viaHttp.getString("similarity")).isEqualTo(response.getSimilarity());
    assertThat(httpNames(viaHttp)).isEqualTo(names(response.getResultsList()));
  }

  @Test
  void hybridSearchFusesAndAgreesWithHttp() throws Exception {
    final HybridSearchResponse response = database.hybridSearch(HybridSearchRequest.newBuilder()
        .setVectorIndexName(DENSE_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setFulltextIndexName(TEXT_INDEX)
        .setFulltextQuery("gamma")
        .setK(3)
        .build());

    assertThat(response.getFused()).isTrue();
    assertThat(response.getFulltextIndexName()).isEqualTo(TEXT_INDEX);
    assertThat(response.getLegsMap().keySet()).contains("vector", "fulltext");

    final JSONObject viaHttp = postHttp("hybrid", new JSONObject()
        .put("vectorIndexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("fulltextIndexName", TEXT_INDEX)
        .put("fulltextQuery", "gamma")
        .put("k", 3));

    assertThat(viaHttp.getBoolean("fused")).isEqualTo(response.getFused());
    assertThat(viaHttp.getString("fusionStrategy")).isEqualTo(response.getFusionStrategy());
    assertThat(httpNames(viaHttp)).isEqualTo(names(response.getResultsList()));
  }

  /**
   * The bounds must reach the gRPC caller as INVALID_ARGUMENT carrying the server's own words - the same words
   * the HTTP surface answers 400 with. A protocol that reported them as an opaque INTERNAL would leave a client
   * unable to tell its own mistake from a server fault.
   */
  @Test
  void aBoundCrossedOverGrpcCarriesTheSameMessageAsOverHttp() throws Exception {
    assertThatThrownBy(() -> database.vectorSearch(VectorSearchRequest.newBuilder()
        .setIndexName(DENSE_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(VectorLeg.MAX_K + 1)
        .build()))
        .hasMessageContaining("'k' must be between 1 and " + VectorLeg.MAX_K);

    assertThatThrownBy(() -> database.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setIndexName(TEXT_INDEX)
        .setQueryText("beta")
        .setLimit(FullTextQuery.MAX_LIMIT + 1)
        .build()))
        .hasMessageContaining("'limit' must be between 1 and " + FullTextQuery.MAX_LIMIT);

    // The same two requests over HTTP, so "the same message" is asserted against something rather than assumed.
    assertThat(rawPostHttp("search", new JSONObject()
        .put("indexName", DENSE_INDEX)
        .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
        .put("k", VectorLeg.MAX_K + 1)).body())
        .contains("'k' must be between 1 and " + VectorLeg.MAX_K);
  }

  /**
   * {@code 0} is the proto default for an unset int, so a request that simply does not mention {@code k} has to
   * mean "use the server default" rather than "return nothing" - which is what a naive mapping would produce.
   */
  @Test
  void anUnsetKFallsBackToTheServerDefaultInsteadOfZero() {
    final VectorSearchResponse response = database.vectorSearch(VectorSearchRequest.newBuilder()
        .setIndexName(DENSE_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .build());

    assertThat(response.getCount()).isEqualTo(3);
  }

  // ───────────────────────────── plumbing ─────────────────────────────

  private static List<String> names(final List<SearchHit> hits) {
    final List<String> names = new ArrayList<>(hits.size());
    for (final SearchHit hit : hits)
      names.add(hit.getRecord().getPropertiesMap().get("name").getStringValue());
    return names;
  }

  private static List<String> httpNames(final JSONObject response) {
    final JSONArray results = response.getJSONArray("results");
    final List<String> names = new ArrayList<>(results.length());
    for (int i = 0; i < results.length(); i++)
      names.add(results.getJSONObject(i).getJSONObject("properties").getString("name"));
    return names;
  }

  private HttpResponse<String> rawPostHttp(final String operation, final JSONObject payload) throws Exception {
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

  private JSONObject postHttp(final String operation, final JSONObject payload) throws Exception {
    final HttpResponse<String> response = rawPostHttp(operation, payload);
    assertThat(response.statusCode()).as("body: %s", response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }
}
