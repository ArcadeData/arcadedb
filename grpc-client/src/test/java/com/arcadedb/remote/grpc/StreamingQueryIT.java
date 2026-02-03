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
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for streaming query operations via gRPC.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamingQueryIT extends BaseGraphServerTest {

  private static final String TYPE = "StreamTest";
  private static final int LARGE_DATASET_SIZE = 10_000;

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeAll
  void setupServer() {
    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterAll
  void teardownServer() {
    if (grpcServer != null) {
      grpcServer.close();
    }
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    database = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS BUCKETS 8");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS LONG");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.name IF NOT EXISTS STRING");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON `" + TYPE + "` (id) UNIQUE");
    database.command("sql", "DELETE FROM `" + TYPE + "`");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null) {
      try { database.rollback(); } catch (Throwable ignore) {}
      database.close();
    }
    super.endTest();
  }

  private void insertTestData(int count) {
    database.begin();
    for (int i = 0; i < count; i++) {
      database.command("sql", "INSERT INTO `" + TYPE + "` SET id = :id, name = :name",
          Map.of("id", (long) i, "name", "record-" + i));
    }
    database.commit();
  }

  @Test
  @DisplayName("queryStream basic iteration returns all records lazily")
  void queryStream_basicIteration() {
    insertTestData(100);

    List<Result> results = new ArrayList<>();
    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "` ORDER BY id")) {
      while (rs.hasNext()) {
        results.add(rs.next());
      }
    }

    assertThat(results).hasSize(100);
    assertThat(results.get(0).<Long>getProperty("id")).isEqualTo(0L);
    assertThat(results.get(99).<Long>getProperty("id")).isEqualTo(99L);
  }

  @Test
  @DisplayName("queryStream with batch size respects configured batch size")
  void queryStream_withBatchSize() {
    insertTestData(50);

    int batchSize = 10;
    int count = 0;

    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "`", batchSize)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }

    assertThat(count).isEqualTo(50);
  }

  @Test
  @DisplayName("queryStream early termination releases resources")
  void queryStream_earlyTermination() {
    insertTestData(100);

    int readCount = 0;
    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "`")) {
      while (rs.hasNext() && readCount < 10) {
        rs.next();
        readCount++;
      }
    }

    assertThat(readCount).isEqualTo(10);
    // If resources weren't released, subsequent queries would fail
    try (ResultSet rs = database.query("sql", "SELECT count(*) as c FROM `" + TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  @Test
  @DisplayName("queryStream empty result returns empty iterator cleanly")
  void queryStream_emptyResult() {
    // No data inserted

    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "`")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  @DisplayName("queryStream large dataset streams without OOM")
  void queryStream_largeDataset() {
    insertTestData(LARGE_DATASET_SIZE);

    long count = 0;
    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "`", 500)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }

    assertThat(count).isEqualTo(LARGE_DATASET_SIZE);
  }

  @Test
  @DisplayName("queryStreamBatched exposes batch metadata")
  void queryStreamBatched_exposesMetadata() {
    insertTestData(25);

    try (ResultSet rs = database.queryStreamBatched("sql", "SELECT FROM `" + TYPE + "`", Map.of(), 10,
        StreamQueryRequest.RetrievalMode.CURSOR)) {
      assertThat((Object) rs).isInstanceOf(BatchedStreamingResultSet.class);
      BatchedStreamingResultSet batched = (BatchedStreamingResultSet) rs;

      int totalCount = 0;
      while (rs.hasNext()) {
        rs.next();
        totalCount++;
        // Batch metadata should be available
        assertThat(batched.getCurrentBatchSize()).isGreaterThanOrEqualTo(0);
      }

      assertThat(totalCount).isEqualTo(25);
      assertThat(batched.isLastBatch()).isTrue();
    }
  }

  @Test
  @DisplayName("queryStreamBatchesIterator iterates batches correctly")
  void queryStreamBatchesIterator_multipleBatches() {
    insertTestData(35);

    Iterator<QueryBatch> iterator = database.queryStreamBatchesIterator("sql", "SELECT FROM `" + TYPE + "`", Map.of(), 10,
        StreamQueryRequest.RetrievalMode.CURSOR);

    int batchCount = 0;
    int totalRecords = 0;

    while (iterator.hasNext()) {
      QueryBatch batch = iterator.next();
      batchCount++;
      totalRecords += batch.results().size();
    }

    assertThat(batchCount).isGreaterThanOrEqualTo(4); // 35 records / 10 per batch = at least 4 batches
    assertThat(totalRecords).isEqualTo(35);
  }

  @Test
  @DisplayName("queryStream with parameters works correctly")
  void queryStream_withParameters() {
    insertTestData(100);

    List<Result> results = new ArrayList<>();
    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "` WHERE id >= :minId AND id < :maxId",
        Map.of("minId", 10L, "maxId", 20L), 5)) {
      while (rs.hasNext()) {
        results.add(rs.next());
      }
    }

    assertThat(results).hasSize(10);
    for (Result r : results) {
      long id = r.getProperty("id");
      assertThat(id).isBetween(10L, 19L);
    }
  }
}
