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
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.FullTextSearchRequest;
import com.arcadedb.server.grpc.FullTextSearchResponse;
import com.arcadedb.server.grpc.HybridSearchRequest;
import com.arcadedb.server.grpc.HybridSearchResponse;
import com.arcadedb.server.grpc.SearchHit;
import com.arcadedb.server.grpc.TransactionContext;
import com.arcadedb.server.grpc.VectorSearchRequest;
import com.arcadedb.server.grpc.VectorSearchResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7326: the three search RPCs #7306 added carried {@code database} and {@code credentials} but no
 * {@code TransactionContext}, so a search issued while the client held an open gRPC transaction ran on a gRPC
 * worker thread rather than on the transaction's own thread and could not observe that transaction's
 * uncommitted writes. The equivalent HTTP request, which carries {@code arcadedb-session-id} and therefore runs
 * inside the session's transaction through {@code DatabaseAbstractHandler}, could - and the last test here
 * asserts that the two protocols now agree, since that equivalence is what #7306 set out to establish.
 * <p>
 * What is asserted is the <i>read</i> the search performs, not the index scan that feeds it. A vector or
 * full-text index is populated at commit replay, so a record created inside the transaction is not yet a
 * candidate of the scan; every hit the scan does produce is then materialized with
 * {@code database.lookupByRID(rid, true)} (see {@code VectorSearch.appendResult} and
 * {@code FullTextQuery.search}), and the vector leg's {@code filter} is a SQL predicate evaluated over that
 * candidate window. Those are the reads that must be transactional, and they are what an uncommitted UPDATE
 * observably changes.
 */
public class Issue7326GrpcSearchInTransactionIT extends BaseGraphServerTest {
  private static final String TYPE        = "GrpcTx7326";
  private static final String DENSE_INDEX = "GrpcTx7326[embedding]";
  private static final String TEXT_INDEX  = "GrpcTx7326[text]";
  private static final int    GRPC_PORT   = 50051;

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
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + TYPE + ".name STRING");
      db.command("sql", "CREATE PROPERTY " + TYPE + ".text STRING");
      db.command("sql", "CREATE PROPERTY " + TYPE + ".embedding ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");
      db.command("sql", "CREATE INDEX ON " + TYPE + " (text) FULL_TEXT");

      db.newDocument(TYPE).set("name", "near").set("text", "alpha beta")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      db.newDocument(TYPE).set("name", "mid").set("text", "beta gamma")
          .set("embedding", new float[] { 0.9f, 0.1f, 0.0f }).save();
      db.newDocument(TYPE).set("name", "far").set("text", "gamma delta")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
    });
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(server, "localhost", GRPC_PORT, getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null) {
      try {
        if (database.isTransactionActive())
          database.rollback();
      } catch (final Throwable ignore) {
        // the test already failed; do not mask it
      }
      database.close();
    }
    if (server != null)
      server.close();
    super.endTest();
  }

  @Test
  void vectorSearchInsideATransactionSeesItsUncommittedUpdate() {
    database.begin();
    try {
      database.command("sql", "UPDATE " + TYPE + " SET name = 'renamed-in-tx' WHERE name = 'near'");

      final VectorSearchResponse response = database.vectorSearch(VectorSearchRequest.newBuilder()
          .setIndexName(DENSE_INDEX)
          .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
          .setK(3)
          .build());

      assertThat(names(response.getResultsList()))
          .as("the search must run inside the caller's transaction and see its uncommitted UPDATE")
          .containsExactly("renamed-in-tx", "mid", "far");
    } finally {
      database.rollback();
    }

    // Proof the update really was uncommitted: after the rollback the original value is back.
    final VectorSearchResponse afterRollback = database.vectorSearch(VectorSearchRequest.newBuilder()
        .setIndexName(DENSE_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(3)
        .build());
    assertThat(names(afterRollback.getResultsList())).containsExactly("near", "mid", "far");
  }

  /**
   * The vector leg's {@code filter} is a SQL predicate over the candidate window, so it is the second read the
   * search performs and it has to be evaluated inside the transaction too - a hit renamed by the transaction
   * must be selectable by its new name and not by its old one.
   */
  @Test
  void theVectorFilterIsEvaluatedInsideTheTransaction() {
    database.begin();
    try {
      database.command("sql", "UPDATE " + TYPE + " SET name = 'renamed-in-tx' WHERE name = 'near'");

      final VectorSearchResponse matched = database.vectorSearch(VectorSearchRequest.newBuilder()
          .setIndexName(DENSE_INDEX)
          .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
          .setFilter("name = 'renamed-in-tx'")
          .setK(3)
          .build());
      assertThat(names(matched.getResultsList())).containsExactly("renamed-in-tx");

      final VectorSearchResponse stale = database.vectorSearch(VectorSearchRequest.newBuilder()
          .setIndexName(DENSE_INDEX)
          .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
          .setFilter("name = 'near'")
          .setK(3)
          .build());
      assertThat(names(stale.getResultsList())).isEmpty();
    } finally {
      database.rollback();
    }
  }

  @Test
  void fullTextSearchInsideATransactionSeesItsUncommittedUpdate() {
    database.begin();
    try {
      database.command("sql", "UPDATE " + TYPE + " SET name = 'renamed-in-tx' WHERE name = 'near'");

      final FullTextSearchResponse response = database.fullTextSearch(FullTextSearchRequest.newBuilder()
          .setIndexName(TEXT_INDEX)
          .setQueryText("alpha")
          .setLimit(5)
          .build());

      assertThat(names(response.getResultsList())).containsExactly("renamed-in-tx");
    } finally {
      database.rollback();
    }
  }

  @Test
  void hybridSearchInsideATransactionSeesItsUncommittedUpdate() {
    database.begin();
    try {
      database.command("sql", "UPDATE " + TYPE + " SET name = 'renamed-in-tx' WHERE name = 'near'");

      final HybridSearchResponse response = database.hybridSearch(HybridSearchRequest.newBuilder()
          .setVectorIndexName(DENSE_INDEX)
          .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
          .setFulltextIndexName(TEXT_INDEX)
          .setFulltextQuery("alpha")
          .setK(3)
          .build());

      assertThat(names(response.getResultsList())).contains("renamed-in-tx").doesNotContain("near");
    } finally {
      database.rollback();
    }
  }

  /**
   * The verification the issue itself asks for: a record created inside the transaction, found by a search
   * issued inside that same transaction, before any commit. It works on the full-text path because the
   * full-text index resolves its posting lists through the transaction, so a row written in the transaction is
   * already a candidate of the scan - which makes this the strongest available form of the assertion, not just
   * a re-read of an already-indexed hit.
   * <p>
   * The dense vector index does not do this: its graph is rebuilt at commit replay, so the same row is not yet a
   * candidate there. That is an index-layer limit rather than a wire-protocol one - it is identical on HTTP with
   * a session id, as {@link #httpAndGrpcAgreeOnARecordCreatedInsideTheTransaction} asserts - and it is tracked
   * separately.
   */
  @Test
  void fullTextSearchInsideATransactionFindsARecordCreatedInIt() {
    database.begin();
    try {
      database.command("sql", "INSERT INTO " + TYPE
          + " SET name = 'created-in-tx', text = 'zulu', embedding = [1.0, 0.0, 0.0]");

      final FullTextSearchResponse response = database.fullTextSearch(FullTextSearchRequest.newBuilder()
          .setIndexName(TEXT_INDEX)
          .setQueryText("zulu")
          .setLimit(5)
          .build());

      assertThat(names(response.getResultsList()))
          .as("a record created inside the transaction must be findable inside it, before the commit")
          .containsExactly("created-in-tx");
    } finally {
      database.rollback();
    }

    // Rolled back, so it must be gone again - proof the hit above really was uncommitted.
    final FullTextSearchResponse afterRollback = database.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setIndexName(TEXT_INDEX)
        .setQueryText("zulu")
        .setLimit(5)
        .build());
    assertThat(afterRollback.getCount()).isZero();
  }

  /**
   * A non-blank transaction id the server no longer knows - reaped, committed or simply invented - must fail
   * loudly rather than silently reading outside the transaction the caller believes it is inside. This is the
   * same contract {@code updateRecord} and {@code lookupByRid} already carry.
   */
  @Test
  void aSearchNamingAnUnknownTransactionIsRejected() {
    final TransactionContext ghost = TransactionContext.newBuilder().setTransactionId("no-such-tx-7326").build();

    assertThatThrownBy(() -> database.vectorSearch(VectorSearchRequest.newBuilder()
        .setIndexName(DENSE_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(3)
        .setTransaction(ghost)
        .build()))
        .hasMessageContaining("no-such-tx-7326");

    assertThatThrownBy(() -> database.hybridSearch(HybridSearchRequest.newBuilder()
        .setVectorIndexName(DENSE_INDEX)
        .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
        .setK(3)
        .setTransaction(ghost)
        .build()))
        .hasMessageContaining("no-such-tx-7326");

    assertThatThrownBy(() -> database.fullTextSearch(FullTextSearchRequest.newBuilder()
        .setIndexName(TEXT_INDEX)
        .setQueryText("alpha")
        .setLimit(5)
        .setTransaction(ghost)
        .build()))
        .hasMessageContaining("no-such-tx-7326");
  }

  /**
   * The equivalence #7306 set out to establish: the same scenario over HTTP, where the session id already made
   * the search transactional, must produce the same answer as the gRPC run above.
   */
  @Test
  void httpAndGrpcAgreeOnWhatATransactionalSearchSees() {
    final List<String> viaGrpc;
    database.begin();
    try {
      database.command("sql", "UPDATE " + TYPE + " SET name = 'renamed-in-tx' WHERE name = 'near'");
      viaGrpc = names(database.vectorSearch(VectorSearchRequest.newBuilder()
          .setIndexName(DENSE_INDEX)
          .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
          .setK(3)
          .build()).getResultsList());
    } finally {
      database.rollback();
    }

    try (final RemoteDatabase http = new RemoteDatabase("localhost", getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      http.begin();
      try {
        http.command("sql", "UPDATE " + TYPE + " SET name = 'renamed-in-tx' WHERE name = 'near'");
        final JSONObject response = http.vectorSearch(new JSONObject()
            .put("indexName", DENSE_INDEX)
            .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
            .put("k", 3));
        assertThat(httpNames(response)).isEqualTo(viaGrpc);
      } finally {
        http.rollback();
      }
    }
  }

  /**
   * The other half of the parity claim, for the record the transaction created rather than the one it updated.
   * Neither protocol finds it through the dense vector index and both find it through the full-text index, so
   * what the two surfaces expose is the same index behaviour rather than a difference introduced by the wire.
   * If the vector index ever starts resolving uncommitted rows, this test fails on both halves at once and says
   * which claim to revisit.
   */
  @Test
  void httpAndGrpcAgreeOnARecordCreatedInsideTheTransaction() {
    final int grpcVectorHits;
    final List<String> grpcFullTextHits;
    database.begin();
    try {
      database.command("sql", "INSERT INTO " + TYPE
          + " SET name = 'created-in-tx', text = 'zulu', embedding = [1.0, 0.0, 0.0]");
      grpcVectorHits = names(database.vectorSearch(VectorSearchRequest.newBuilder()
          .setIndexName(DENSE_INDEX)
          .addAllQueryVector(List.of(1.0f, 0.0f, 0.0f))
          .setK(10)
          .build()).getResultsList()).size();
      grpcFullTextHits = names(database.fullTextSearch(FullTextSearchRequest.newBuilder()
          .setIndexName(TEXT_INDEX)
          .setQueryText("zulu")
          .setLimit(10)
          .build()).getResultsList());
    } finally {
      database.rollback();
    }

    assertThat(grpcFullTextHits).containsExactly("created-in-tx");

    try (final RemoteDatabase http = new RemoteDatabase("localhost", getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS)) {
      http.begin();
      try {
        http.command("sql", "INSERT INTO " + TYPE
            + " SET name = 'created-in-tx', text = 'zulu', embedding = [1.0, 0.0, 0.0]");
        assertThat(httpNames(http.vectorSearch(new JSONObject()
            .put("indexName", DENSE_INDEX)
            .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
            .put("k", 10))))
            .as("the dense index scan must miss the new row on HTTP exactly as it does on gRPC")
            .hasSize(grpcVectorHits)
            .doesNotContain("created-in-tx");
        assertThat(httpNames(http.fullTextSearch(new JSONObject()
            .put("indexName", TEXT_INDEX)
            .put("queryText", "zulu")
            .put("limit", 10))))
            .isEqualTo(grpcFullTextHits);
      } finally {
        http.rollback();
      }
    }
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
}
