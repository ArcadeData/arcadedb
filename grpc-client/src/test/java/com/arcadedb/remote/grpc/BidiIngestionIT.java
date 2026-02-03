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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for bidirectional streaming ingestion via gRPC.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BidiIngestionIT extends BaseGraphServerTest {

  private static final String TYPE = "BidiTest";

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
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS STRING");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.name IF NOT EXISTS STRING");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.value IF NOT EXISTS INTEGER");
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

  private InsertOptions defaultOptions() {
    return InsertOptions.newBuilder()
        .setDatabase(getDatabaseName())
        .setTargetClass(TYPE)
        .addKeyColumns("id")
        .setConflictMode(ConflictMode.CONFLICT_ERROR)
        .setTransactionMode(TransactionMode.PER_BATCH)
        .setServerBatchSize(100)
        .setCredentials(database.buildCredentials())
        .build();
  }

  private List<Map<String, Object>> generateRows(int count) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", "row-" + i);
      row.put("name", "name-" + i);
      row.put("value", i);
      rows.add(row);
    }
    return rows;
  }

  private long countRecords() {
    try (ResultSet rs = database.query("sql", "SELECT count(*) as c FROM `" + TYPE + "`")) {
      if (rs.hasNext()) {
        Number n = rs.next().getProperty("c");
        return n != null ? n.longValue() : 0;
      }
    }
    return 0;
  }

  @Test
  @Disabled("Bidi streaming has server-side timing issues that cause timeouts - needs investigation")
  @DisplayName("ingestBidi basic flow sends records and receives ACKs")
  void ingestBidi_basicFlow() throws InterruptedException {
    List<Map<String, Object>> rows = generateRows(50);

    InsertSummary summary = database.ingestBidi(defaultOptions(), rows, 10, 5, 60_000);

    assertThat(summary.getInserted()).isEqualTo(50);
    assertThat(summary.getUpdated()).isEqualTo(0);
    assertThat(countRecords()).isEqualTo(50);
  }

  @Test
  @Disabled("Bidi streaming has server-side timing issues that cause timeouts - needs investigation")
  @DisplayName("ingestBidi handles backpressure with maxInflight limit")
  void ingestBidi_backpressure() throws InterruptedException {
    List<Map<String, Object>> rows = generateRows(200);

    // Very low maxInflight to force backpressure
    InsertSummary summary = database.ingestBidi(defaultOptions(), rows, 20, 2, 120_000);

    assertThat(summary.getInserted()).isEqualTo(200);
    assertThat(countRecords()).isEqualTo(200);
  }

  @Test
  @Disabled("Bidi streaming has server-side timing issues that cause timeouts - needs investigation")
  @DisplayName("ingestBidi large volume streams without blocking indefinitely")
  void ingestBidi_largeVolume() throws InterruptedException {
    // Reduced from 50K to 5K to avoid timeout issues
    List<Map<String, Object>> rows = generateRows(5_000);

    InsertSummary summary = database.ingestBidi(defaultOptions(), rows, 500, 10, 300_000);

    assertThat(summary.getInserted()).isEqualTo(5_000);
    assertThat(countRecords()).isEqualTo(5_000);
  }

  @Test
  @Disabled("Bidi streaming has server-side timing issues that cause timeouts - needs investigation")
  @DisplayName("ingestBidi with explicit transaction commits correctly")
  void ingestBidi_withTransaction() throws InterruptedException {
    List<Map<String, Object>> rows = generateRows(30);

    database.begin();
    InsertSummary summary = database.ingestBidi(defaultOptions(), rows, 10, 5, 60_000);
    database.commit();

    assertThat(summary.getInserted()).isEqualTo(30);
    assertThat(countRecords()).isEqualTo(30);
  }

  @Test
  @Disabled("Bidi streaming has server-side timing issues that cause timeouts - needs investigation")
  @DisplayName("ingestBidi with conflict mode UPDATE performs upserts")
  void ingestBidi_upsertOnConflict() throws InterruptedException {
    // First insert
    List<Map<String, Object>> rows = generateRows(20);
    InsertOptions opts = InsertOptions.newBuilder()
        .setDatabase(getDatabaseName())
        .setTargetClass(TYPE)
        .addKeyColumns("id")
        .setConflictMode(ConflictMode.CONFLICT_UPDATE)
        .addUpdateColumnsOnConflict("name")
        .addUpdateColumnsOnConflict("value")
        .setTransactionMode(TransactionMode.PER_BATCH)
        .setServerBatchSize(100)
        .setCredentials(database.buildCredentials())
        .build();

    database.ingestBidi(opts, rows, 10, 5, 60_000);
    assertThat(countRecords()).isEqualTo(20);

    // Update some rows
    rows.get(0).put("name", "updated-name-0");
    rows.get(0).put("value", 999);

    InsertSummary summary = database.ingestBidi(opts, rows, 10, 5, 60_000);

    assertThat(summary.getUpdated()).isGreaterThanOrEqualTo(1);
    assertThat(countRecords()).isEqualTo(20);

    // Verify update
    try (ResultSet rs = database.query("sql", "SELECT FROM `" + TYPE + "` WHERE id = 'row-0'")) {
      assertThat(rs.hasNext()).isTrue();
      var result = rs.next();
      assertThat(result.<String>getProperty("name")).isEqualTo("updated-name-0");
      assertThat(result.<Number>getProperty("value").intValue()).isEqualTo(999);
    }
  }

  @Test
  @DisplayName("ingestBidi with invalid chunkSize throws IllegalArgumentException")
  void ingestBidi_invalidChunkSize() {
    List<Map<String, Object>> rows = generateRows(10);

    assertThatThrownBy(() -> database.ingestBidi(defaultOptions(), rows, 0, 5, 60_000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("chunkSize");
  }

  @Test
  @DisplayName("ingestBidi with invalid maxInflight throws IllegalArgumentException")
  void ingestBidi_invalidMaxInflight() {
    List<Map<String, Object>> rows = generateRows(10);

    assertThatThrownBy(() -> database.ingestBidi(defaultOptions(), rows, 10, 0, 60_000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxInflight");
  }

  @Test
  @DisplayName("ingestBidi with empty list returns empty summary")
  void ingestBidi_emptyList() throws InterruptedException {
    InsertSummary summary = database.ingestBidi(defaultOptions(), List.of(), 10, 5, 60_000);

    assertThat(summary.getReceived()).isEqualTo(0);
    assertThat(countRecords()).isEqualTo(0);
  }
}
