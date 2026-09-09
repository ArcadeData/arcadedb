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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.search.FullTextSearchOperation;
import com.arcadedb.query.search.HybridSearchOperation;
import com.arcadedb.query.search.VectorSearchLeg;
import com.arcadedb.query.search.VectorSearchOperation;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7306: vector search had no gRPC RPC at all - the proto matched "vector" zero times - so the feature was
 * unreachable on the protocol that already had every other capability HTTP was missing.
 * <p>
 * The equivalence tests are the load-bearing ones. They assert that the RPC and the shared
 * {@code com.arcadedb.query.search} operation - the same code the HTTP routes and the MCP tools call - agree on
 * the same request, which is the property the issue asked for: two protocols that cannot disagree about what a
 * legal request is or about what the answer says.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7306GrpcSearchIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private ManagedChannel                                       channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub       stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupFixtureAndChannel() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType("G7306Doc")) {
      db.transaction(() -> {
        db.command("sql", "CREATE VERTEX TYPE G7306Doc BUCKETS 1");
        db.command("sql", "CREATE PROPERTY G7306Doc.title STRING");
        db.command("sql", "CREATE PROPERTY G7306Doc.content STRING");
        db.command("sql", "CREATE PROPERTY G7306Doc.rank INTEGER");
        db.command("sql", "CREATE PROPERTY G7306Doc.embedding ARRAY_OF_FLOATS");
        db.command("sql", """
            CREATE INDEX ON G7306Doc (embedding) LSM_VECTOR
            METADATA { dimensions: 3, similarity: 'COSINE' }
            """);
        db.command("sql", "CREATE INDEX ON G7306Doc (content) FULL_TEXT");

        db.newVertex("G7306Doc").set("title", "near").set("rank", 1)
            .set("content", "flywheel bearing tolerance")
            .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
        db.newVertex("G7306Doc").set("title", "mid").set("rank", 2)
            .set("content", "vector similarity ranking")
            .set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
        db.newVertex("G7306Doc").set("title", "far").set("rank", 3)
            .set("content", "reciprocal rank fusion")
            .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      });
    }

    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    // GrpcAuthInterceptor authenticates from the call metadata, so the body credentials alone are not enough to
    // reach the handler: without these headers every call fails UNAUTHENTICATED before any argument is read.
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
  }

  @AfterEach
  void teardownChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // --------------------------------------------------------------------------------------------
  // VectorSearch
  // --------------------------------------------------------------------------------------------

  @Test
  void vectorSearchRanksByDistanceAndCarriesTypedRecords() {
    final VectorSearchResponse response = stub.vectorSearch(VectorSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .setK(2)
        .build());

    assertThat(response.getScoring()).startsWith("distance_lower_is_better:");
    assertThat(response.getSparse()).isFalse();
    assertThat(response.getCount()).isEqualTo(2);
    assertThat(response.getResultsList()).hasSize(2);

    final SearchHit first = response.getResults(0);
    assertThat(first.getRecord().getPropertiesMap().get("title").getStringValue()).isEqualTo("near");
    assertThat(first.getScore()).isLessThan(response.getResults(1).getScore());

    // A typed property must arrive typed, not flattened to the string the JSON form would carry: every other RPC
    // of this service answers with typed GrpcValues, and a search hit is not a different kind of record.
    assertThat(first.getRecord().getPropertiesMap().get("rank").getInt32Value()).isEqualTo(1);
  }

  /**
   * The RPC and the shared operation must describe the same ranking. Comparing rid-by-rid rather than only the
   * count is what would catch a translation that reordered or dropped a hit.
   */
  @Test
  void theRpcAnswersTheSameRankingTheSharedOperationAnswers() {
    final VectorSearchResponse overGrpc = stub.vectorSearch(VectorSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(0.5f, 0.5f, 0.0f))
        .setK(3)
        .build());

    final JSONObject direct = VectorSearchOperation.execute(getServerDatabase(0, getDatabaseName()),
        new JSONObject()
            .put("indexName", "G7306Doc[embedding]")
            .put("queryVector", new JSONArray().put(0.5f).put(0.5f).put(0.0f))
            .put("k", 3));

    assertThat(overGrpc.getIndexName()).isEqualTo(direct.getString("indexName"));
    assertThat(overGrpc.getScoring()).isEqualTo(direct.getString("scoring"));
    assertThat(overGrpc.getTruncated()).isEqualTo(direct.getBoolean("truncated"));
    assertThat(overGrpc.getCount()).isEqualTo(direct.getInt("count"));

    for (int i = 0; i < overGrpc.getResultsCount(); i++) {
      assertThat(overGrpc.getResults(i).getRid())
          .isEqualTo(direct.getJSONArray("results").getJSONObject(i).getString("rid"));
      assertThat(overGrpc.getResults(i).getScore())
          .isEqualTo(direct.getJSONArray("results").getJSONObject(i).getDouble("distance"));
    }
  }

  /**
   * proto3 cannot tell an unset int32 from a zero, and zero is out of range for both {@code k} and
   * {@code efSearch}. A request that sets neither must therefore fall through to the server's defaults rather than
   * be rejected as asking for zero results.
   */
  @Test
  void anUnsetKFallsThroughToTheServerDefaultRatherThanBeingReadAsZero() {
    final VectorSearchResponse response = stub.vectorSearch(VectorSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .build());

    assertThat(response.getCount()).isEqualTo(3);
  }

  // --------------------------------------------------------------------------------------------
  // Bounds - the same numbers HTTP and MCP enforce, mapped to INVALID_ARGUMENT
  // --------------------------------------------------------------------------------------------

  @Test
  void anEfSearchAboveTheSharedCeilingIsInvalidArgumentWithTheSharedMessage() {
    assertThatThrownBy(() -> stub.vectorSearch(VectorSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .setK(1)
        .setEfSearch(VectorSearchLeg.MAX_EF_SEARCH + 1)
        .build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("'efSearch' must be between 1 and " + VectorSearchLeg.MAX_EF_SEARCH);
  }

  @Test
  void aKAboveTheSharedCeilingIsInvalidArgument() {
    assertThatThrownBy(() -> stub.vectorSearch(VectorSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .setK(VectorSearchLeg.MAX_K + 1)
        .build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("'k' must be between 1 and " + VectorSearchLeg.MAX_K);
  }

  @Test
  void aFullTextLimitAboveTheSharedCeilingIsInvalidArgument() {
    assertThatThrownBy(() -> stub.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setIndexName("G7306Doc[content]")
        .setQueryText("flywheel")
        .setLimit(FullTextSearchOperation.MAX_LIMIT + 1)
        .build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("'limit' must be between 1 and " + FullTextSearchOperation.MAX_LIMIT);
  }

  @Test
  void anExpansionDepthAboveTheServerCapIsInvalidArgument() {
    assertThatThrownBy(() -> stub.hybridSearch(HybridSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setVectorIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .setK(2)
        .setExpand(GraphExpansion.newBuilder().setMaxDepth(HybridSearchOperation.MAX_DEPTH + 1).build())
        .build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("expand.maxDepth must be between 1 and " + HybridSearchOperation.MAX_DEPTH);
  }

  // --------------------------------------------------------------------------------------------
  // FullTextSearch
  // --------------------------------------------------------------------------------------------

  @Test
  void fullTextSearchReturnsScoredMatches() {
    final FullTextSearchResponse response = stub.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setIndexName("G7306Doc[content]")
        .setQueryText("flywheel")
        .build());

    assertThat(response.getIndexName()).isEqualTo("G7306Doc[content]");
    assertThat(response.getSimilarity()).isNotBlank();
    assertThat(response.getCount()).isEqualTo(1);
    assertThat(response.getResults(0).getRecord().getPropertiesMap().get("title").getStringValue())
        .isEqualTo("near");
    assertThat(response.getResults(0).getScore()).isGreaterThan(0.0);
  }

  @Test
  void fullTextSearchResolvesTheIndexByTypeAndProperties() {
    final FullTextSearchResponse response = stub.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setTypeName("G7306Doc")
        .addProperties("content")
        .setQueryText("fusion")
        .build());

    assertThat(response.getIndexName()).isEqualTo("G7306Doc[content]");
    assertThat(response.getCount()).isEqualTo(1);
  }

  // --------------------------------------------------------------------------------------------
  // HybridSearch
  // --------------------------------------------------------------------------------------------

  @Test
  void hybridSearchFusesBothLegsAndReportsThem() {
    final HybridSearchResponse response = stub.hybridSearch(HybridSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setVectorIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(0.0f, 1.0f, 0.0f))
        .setFulltextIndexName("G7306Doc[content]")
        .setFulltextQuery("flywheel")
        .setK(3)
        .build());

    assertThat(response.getFused()).isTrue();
    assertThat(response.getFusionStrategy()).isEqualTo("RRF");
    assertThat(response.getFulltextIndexName()).isEqualTo("G7306Doc[content]");
    assertThat(response.getLegs().getVectorCount()).isGreaterThan(0);
    assertThat(response.getLegs().getHasFulltext()).isTrue();
    assertThat(response.getLegs().getFulltextCount()).isGreaterThan(0);
    assertThat(response.getResults(0).getSourcesList()).isNotEmpty();
  }

  @Test
  void hybridSearchWithOnlyTheVectorLegReportsThatItDidNotFuse() {
    final HybridSearchResponse response = stub.hybridSearch(HybridSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setVectorIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .setK(2)
        .build());

    assertThat(response.getFused()).isFalse();
    assertThat(response.getLegs().getHasFulltext()).isFalse();
    assertThat(response.getLegs().getHasExpand()).isFalse();
    // Unfused, the score field carries the vector leg's own distance, which is why the response says so rather
    // than leaving a client to assume it was fused.
    assertThat(response.getResults(0).getScore()).isGreaterThanOrEqualTo(0.0);
  }

  @Test
  void aScoreBasedFusionCombinedWithTheRankOnlyExpansionLegIsRejected() {
    assertThatThrownBy(() -> stub.hybridSearch(HybridSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setVectorIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .setK(2)
        .setFusionStrategy("DBSF")
        .setExpand(GraphExpansion.newBuilder().setMaxDepth(1).build())
        .build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("needs a score on every row");
  }

  /**
   * Half a full-text leg is always a mistake, and proto3 renders an unset string as "". Sending only the index
   * name must therefore be rejected rather than read as "no full-text leg requested", which would silently return
   * a plausible result set that ignored what the caller asked for.
   */
  @Test
  void aFullTextIndexWithoutItsQueryIsRejectedRatherThanTreatedAsAbsent() {
    assertThatThrownBy(() -> stub.hybridSearch(HybridSearchRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setVectorIndexName("G7306Doc[embedding]")
        .addAllQueryVector(java.util.List.of(1.0f, 0.0f, 0.0f))
        .setK(2)
        .setFulltextIndexName("G7306Doc[content]")
        .build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("must be supplied together");
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }
}
