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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.StreamQueryRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5047 (gRPC streaming & pagination correctness):
 * <ul>
 *   <li><b>PERF-2</b> - {@code queryStreamBatched} / {@code queryStreamBatchesIterator} must honor the
 *       requested query {@code language} instead of silently running as SQL.</li>
 *   <li><b>STR-4</b> - {@code is_last_batch} must be reported {@code true} on exact-multiple totals in both
 *       CURSOR and PAGED retrieval modes.</li>
 *   <li><b>STR-3</b> - PAGED mode must reject queries whose parameters collide with the reserved
 *       {@code _skip}/{@code _limit} names instead of silently overwriting them.</li>
 *   <li><b>CON-3</b> - closing a partially-read stream must release resources and leave the connection
 *       usable (the stream is cancelled, not drained).</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Issue5047StreamingCorrectnessIT extends BaseGraphServerTest {

  private static final String TYPE     = "Issue5047Stream";
  private static final int    GRPC_PORT = 50051;

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeAll
  void setupServer() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterAll
  void teardownServer() {
    if (grpcServer != null)
      grpcServer.close();
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    database = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS BUCKETS 8");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS LONG");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON `" + TYPE + "` (id) UNIQUE");
    database.command("sql", "DELETE FROM `" + TYPE + "`");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null) {
      try {
        database.rollback();
      } catch (Throwable ignore) {
      }
      database.close();
    }
    super.endTest();
  }

  private void insertRows(final int count) {
    database.begin();
    for (int i = 0; i < count; i++)
      database.command("sql", "INSERT INTO `" + TYPE + "` SET id = :id", Map.of("id", (long) i));
    database.commit();
  }

  @Test
  @DisplayName("PERF-2: queryStreamBatched honors the requested Cypher language")
  void queryStreamBatched_honorsCypherLanguage() {
    insertRows(20);

    // This is a Cypher query; if the stream request dropped the language it would run as SQL and fail to
    // parse (or return nothing meaningful). With the language preserved it returns all 20 rows.
    final List<Result> results = new ArrayList<>();
    try (ResultSet rs = database.queryStreamBatched("cypher", "MATCH (n:`" + TYPE + "`) RETURN n.id AS id",
        Map.of(), 10, StreamQueryRequest.RetrievalMode.CURSOR)) {
      while (rs.hasNext())
        results.add(rs.next());
    }

    assertThat(results).hasSize(20);
    assertThat(results).allSatisfy(r -> assertThat(r.<Long>getProperty("id")).isNotNull());
  }

  @Test
  @DisplayName("PERF-2: queryStreamBatchesIterator honors the requested Cypher language")
  void queryStreamBatchesIterator_honorsCypherLanguage() {
    insertRows(15);

    int total = 0;
    final var it = database.queryStreamBatchesIterator("cypher", "MATCH (n:`" + TYPE + "`) RETURN n.id AS id",
        Map.of(), 10, StreamQueryRequest.RetrievalMode.CURSOR);
    while (it.hasNext())
      total += it.next().results().size();

    assertThat(total).isEqualTo(15);
  }

  @Test
  @DisplayName("STR-4: CURSOR mode marks is_last_batch on an exact-multiple total")
  void cursorMode_isLastBatchOnExactMultiple() {
    insertRows(20); // exactly 2 x batchSize(10)

    int total = 0;
    try (ResultSet rs = database.queryStreamBatched("sql", "SELECT FROM `" + TYPE + "`", Map.of(), 10,
        StreamQueryRequest.RetrievalMode.CURSOR)) {
      final BatchedStreamingResultSet batched = (BatchedStreamingResultSet) rs;
      while (rs.hasNext()) {
        rs.next();
        total++;
      }
      assertThat(total).isEqualTo(20);
      // The terminal batch must carry is_last_batch=true even though the total is an exact multiple of the
      // batch size; before the fix the last full batch went out with is_last_batch=false.
      assertThat(batched.isLastBatch()).isTrue();
    }
  }

  @Test
  @DisplayName("STR-4: PAGED mode marks is_last_batch on an exact-multiple total")
  void pagedMode_isLastBatchOnExactMultiple() {
    insertRows(20); // exactly 2 x batchSize(10)

    int total = 0;
    try (ResultSet rs = database.queryStreamBatched("sql", "SELECT FROM `" + TYPE + "`", Map.of(), 10,
        StreamQueryRequest.RetrievalMode.PAGED)) {
      final BatchedStreamingResultSet batched = (BatchedStreamingResultSet) rs;
      while (rs.hasNext()) {
        rs.next();
        total++;
      }
      assertThat(total).isEqualTo(20);
      assertThat(batched.isLastBatch()).isTrue();
    }
  }

  @Test
  @DisplayName("STR-4: PAGED mode with a partial last page still streams all rows")
  void pagedMode_partialLastPage() {
    insertRows(25); // 2 full pages + a partial page of 5

    int total = 0;
    try (ResultSet rs = database.queryStreamBatched("sql", "SELECT FROM `" + TYPE + "`", Map.of(), 10,
        StreamQueryRequest.RetrievalMode.PAGED)) {
      while (rs.hasNext()) {
        rs.next();
        total++;
      }
    }
    assertThat(total).isEqualTo(25);
  }

  @Test
  @DisplayName("STR-3: PAGED mode rejects a query that reuses the reserved _skip/_limit parameter names")
  void pagedMode_rejectsReservedParameterName() {
    insertRows(5);

    // The caller binds a parameter literally named "_skip", which PAGED reserves for its SKIP wrapper. The
    // server must reject the request instead of silently overwriting the caller's value.
    assertThatThrownBy(() -> {
      try (ResultSet rs = database.queryStreamBatched("sql", "SELECT FROM `" + TYPE + "` WHERE id >= :_skip",
          Map.of("_skip", 0L), 10, StreamQueryRequest.RetrievalMode.PAGED)) {
        while (rs.hasNext())
          rs.next();
      }
    }).hasMessageContaining("_skip").hasMessageContaining("_limit");
  }

  @Test
  @DisplayName("CON-3: closing a partially-read stream releases resources and keeps the connection usable")
  void close_afterPartialRead_releasesAndStaysUsable() {
    insertRows(1000);

    int read = 0;
    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "`", 50)) {
      while (rs.hasNext() && read < 5) {
        rs.next();
        read++;
      }
      // close() runs here via try-with-resources: it must cancel the underlying call, not drain 995 rows.
    }
    assertThat(read).isEqualTo(5);

    // The connection must remain fully usable after the partial-read close.
    try (ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM `" + TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1000L);
    }
  }
}
