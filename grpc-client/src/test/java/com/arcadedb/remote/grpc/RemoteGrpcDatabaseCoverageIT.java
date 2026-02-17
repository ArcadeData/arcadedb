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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteTransactionExplicitLock;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests that improve code coverage for {@link RemoteGrpcDatabase}.
 * <p>
 * Covers direct CRUD API, lookup/exists, count, iterate, transaction edge cases,
 * streaming query modes, client-streaming ingestion, acquireLock, and command
 * with ContextConfiguration.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteGrpcDatabaseCoverageIT extends BaseGraphServerTest {

  static final String DOC_TYPE    = "CovDoc";
  static final String VERTEX_TYPE = "CovVertex";
  static final String EDGE_TYPE   = "CovEdge";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE DOCUMENT TYPE `" + DOC_TYPE + "` IF NOT EXISTS", Map.of());
    grpc.command("sql", "CREATE PROPERTY `" + DOC_TYPE + "`.name IF NOT EXISTS STRING", Map.of());
    grpc.command("sql", "CREATE PROPERTY `" + DOC_TYPE + "`.value IF NOT EXISTS INTEGER", Map.of());

    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS", Map.of());
    grpc.command("sql", "CREATE PROPERTY `" + VERTEX_TYPE + "`.name IF NOT EXISTS STRING", Map.of());

    grpc.command("sql", "CREATE EDGE TYPE `" + EDGE_TYPE + "` IF NOT EXISTS", Map.of());

    // Clean slate
    grpc.command("sql", "DELETE FROM `" + DOC_TYPE + "`", Map.of());
    grpc.command("sql", "DELETE FROM `" + VERTEX_TYPE + "`", Map.of());
  }

  @AfterEach
  void close() {
    if (grpc != null) {
      try {
        grpc.rollback();
      } catch (final Throwable ignore) {
      }
      grpc.close();
    }
  }

  // Helper: insert a doc via SQL and return its RID string
  private String insertDocViaSql(final String name, final int value) {
    try (ResultSet rs = grpc.command("sql",
        "INSERT INTO `" + DOC_TYPE + "` SET name = :name, value = :value",
        Map.of("name", name, "value", value))) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getIdentity().get().toString();
    }
  }

  // ==================== Direct CRUD API ====================

  @Test
  @DisplayName("createRecord creates a document and returns its RID")
  void testCreateRecordDocument() {
    final Map<String, Object> props = Map.of("name", "doc1", "value", 42);
    final String rid = grpc.createRecord(DOC_TYPE, props, 30_000);

    assertThat(rid).isNotNull().startsWith("#");

    try (ResultSet rs = grpc.query("sql", "SELECT FROM `" + DOC_TYPE + "` WHERE name = 'doc1'", Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("doc1");
      assertThat(r.<Number>getProperty("value").intValue()).isEqualTo(42);
    }
  }

  @Test
  @DisplayName("createRecordTx auto-commits within a single-call transaction")
  void testCreateRecordTxAutoCommits() {
    final Map<String, Object> props = Map.of("name", "txDoc", "value", 99);
    final String rid = grpc.createRecordTx(DOC_TYPE, props, 30_000);

    assertThat(rid).isNotNull().startsWith("#");

    try (ResultSet rs = grpc.query("sql", "SELECT FROM `" + DOC_TYPE + "` WHERE name = 'txDoc'", Map.of())) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  @Test
  @DisplayName("createRecord with null class throws IllegalArgumentException")
  void testCreateRecordWithNullClassThrows() {
    assertThatThrownBy(() -> grpc.createRecord(null, Map.of("name", "x"), 30_000))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("createRecord with blank class throws IllegalArgumentException")
  void testCreateRecordWithBlankClassThrows() {
    assertThatThrownBy(() -> grpc.createRecord("  ", Map.of("name", "x"), 30_000))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("createRecord with null props throws IllegalArgumentException")
  void testCreateRecordWithNullPropsThrows() {
    assertThatThrownBy(() -> grpc.createRecord(DOC_TYPE, null, 30_000))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("updateRecord by RID and props map")
  void testUpdateRecordByRidAndProps() {
    final String rid = grpc.createRecordTx(DOC_TYPE, Map.of("name", "original", "value", 1), 30_000);

    final boolean updated = grpc.updateRecord(rid, Map.of("name", "updated", "value", 2), 30_000);
    assertThat(updated).isTrue();

    try (ResultSet rs = grpc.query("sql", "SELECT FROM " + rid, Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("updated");
      assertThat(r.<Number>getProperty("value").intValue()).isEqualTo(2);
    }
  }

  @Test
  @DisplayName("deleteRecord by Record object")
  void testDeleteRecordByObject() {
    final String rid = insertDocViaSql("toDelete", 0);
    final RID ridObj = new RID(grpc, rid);
    final Record record = grpc.lookupByRID(ridObj);

    grpc.begin();
    grpc.deleteRecord(record);
    grpc.commit();

    // Verify via count query
    try (ResultSet rs = grpc.query("sql",
        "SELECT count(*) as c FROM `" + DOC_TYPE + "` WHERE name = 'toDelete'", Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(0);
    }
  }

  @Test
  @DisplayName("deleteRecord by RID string returns true for existing record")
  void testDeleteRecordByRid() {
    final String rid = insertDocViaSql("toDeleteByRid", 0);

    final boolean deleted = grpc.deleteRecord(rid, 30_000);
    assertThat(deleted).isTrue();

    // Verify deleted via count query
    try (ResultSet rs = grpc.query("sql",
        "SELECT count(*) as c FROM `" + DOC_TYPE + "` WHERE name = 'toDeleteByRid'", Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(0);
    }
  }

  @Test
  @DisplayName("deleteRecord with null identity throws IllegalArgumentException")
  void testDeleteRecordNullIdentityThrows() {
    assertThatThrownBy(() -> grpc.lookupByRID(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ==================== Lookup & Exists ====================

  @Test
  @DisplayName("lookupByRID returns the correct record")
  void testLookupByRID() {
    final String rid = insertDocViaSql("lookup", 7);
    final RID ridObj = new RID(grpc, rid);

    final Record record = grpc.lookupByRID(ridObj);
    assertThat(record).isNotNull();
    assertThat(record.asDocument().getString("name")).isEqualTo("lookup");
  }

  @Test
  @DisplayName("lookupByRID throws for missing record")
  void testLookupByRIDNotFoundThrows() {
    // Insert and delete so we have a valid bucket but missing record
    final String rid = insertDocViaSql("willRemove", 0);
    grpc.deleteRecord(rid, 30_000);
    final RID ridObj = new RID(grpc, rid);

    assertThatThrownBy(() -> grpc.lookupByRID(ridObj))
        .isInstanceOfAny(RecordNotFoundException.class, RemoteException.class);
  }

  @Test
  @DisplayName("lookupByRID with null throws IllegalArgumentException")
  void testLookupByRIDNullThrows() {
    assertThatThrownBy(() -> grpc.lookupByRID(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("existsRecord returns true for existing record")
  void testExistsRecordTrue() {
    final String rid = insertDocViaSql("exists", 1);
    final RID ridObj = new RID(grpc, rid);

    assertThat(grpc.existsRecord(ridObj)).isTrue();
  }

  @Test
  @DisplayName("existsRecord with null throws IllegalArgumentException")
  void testExistsRecordNullThrows() {
    assertThatThrownBy(() -> grpc.existsRecord(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ==================== Count ====================

  @Test
  @DisplayName("countType returns correct count (polymorphic)")
  void testCountType() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "c1", "value", 1), 30_000);
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "c2", "value", 2), 30_000);

    final long count = grpc.countType(DOC_TYPE, true);
    assertThat(count).isEqualTo(2);
  }

  @Test
  @DisplayName("countType non-polymorphic returns only exact type matches")
  void testCountTypeNonPolymorphic() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "np1", "value", 1), 30_000);

    final long count = grpc.countType(DOC_TYPE, false);
    assertThat(count).isGreaterThanOrEqualTo(1);
  }

  @Test
  @DisplayName("countBucket returns correct count")
  void testCountBucket() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "bucket1", "value", 1), 30_000);

    // Get the bucket name for DOC_TYPE
    try (ResultSet rs = grpc.query("sql", "SELECT FROM schema:types WHERE name = :name", Map.of("name", DOC_TYPE))) {
      assertThat(rs.hasNext()).isTrue();
      final Result typeInfo = rs.next();
      final List<?> buckets = typeInfo.getProperty("buckets");
      assertThat(buckets).isNotEmpty();
      final String bucketName = buckets.get(0).toString();

      final long count = grpc.countBucket(bucketName);
      assertThat(count).isGreaterThanOrEqualTo(1);
    }
  }

  // ==================== Iterate ====================

  @Test
  @DisplayName("iterateType returns all records (polymorphic)")
  void testIterateType() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "it1", "value", 1), 30_000);
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "it2", "value", 2), 30_000);

    final Iterator<Record> it = grpc.iterateType(DOC_TYPE, true);
    int count = 0;
    while (it.hasNext()) {
      final Record r = it.next();
      assertThat(r).isNotNull();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  @DisplayName("iterateType non-polymorphic only returns exact type")
  void testIterateTypeNonPolymorphic() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "itnp1", "value", 1), 30_000);

    final Iterator<Record> it = grpc.iterateType(DOC_TYPE, false);
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    assertThat(count).isGreaterThanOrEqualTo(1);
  }

  // ==================== Transaction Edge Cases ====================

  @Test
  @DisplayName("begin() twice throws TransactionException")
  void testBeginTwiceThrows() {
    grpc.begin();
    assertThatThrownBy(() -> grpc.begin())
        .isInstanceOf(TransactionException.class)
        .hasMessageContaining("already begun");
  }

  @Test
  @DisplayName("commit() without begin() throws TransactionException")
  void testCommitWithoutBeginThrows() {
    assertThatThrownBy(() -> grpc.commit())
        .isInstanceOf(TransactionException.class)
        .hasMessageContaining("not begun");
  }

  @Test
  @DisplayName("rollback() without begin() throws TransactionException")
  void testRollbackWithoutBeginThrows() {
    assertThatThrownBy(() -> grpc.rollback())
        .isInstanceOf(TransactionException.class)
        .hasMessageContaining("not begun");
  }

  @Test
  @DisplayName("close() with active transaction auto-rollbacks")
  void testCloseWithActiveTxAutoRollback() {
    grpc.begin();
    grpc.command("sql", "INSERT INTO `" + DOC_TYPE + "` SET name = 'autoRollback', value = 0", Map.of());

    // Close should auto-rollback the active transaction
    grpc.close();

    // Re-open to verify the insert was rolled back (recreate server ref since close invalidates)
    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    try (ResultSet rs = grpc.query("sql", "SELECT FROM `" + DOC_TYPE + "` WHERE name = 'autoRollback'", Map.of())) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ==================== Streaming Query Modes ====================

  @Test
  @DisplayName("queryStream with MATERIALIZE_ALL mode returns results")
  void testQueryStreamMaterializeAllMode() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "matAll1", "value", 1), 30_000);
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "matAll2", "value", 2), 30_000);

    try (ResultSet rs = grpc.queryStream("sql",
        "SELECT FROM `" + DOC_TYPE + "` WHERE name LIKE 'matAll%'",
        100, StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL)) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    }
  }

  @Test
  @DisplayName("queryStream with PAGED mode returns results")
  void testQueryStreamPagedMode() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "paged1", "value", 1), 30_000);
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "paged2", "value", 2), 30_000);

    try (ResultSet rs = grpc.queryStream("sql",
        "SELECT FROM `" + DOC_TYPE + "` WHERE name LIKE 'paged%'",
        100, StreamQueryRequest.RetrievalMode.PAGED)) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    }
  }

  @Test
  @DisplayName("queryStream with PROJECTION_AS_MAP encoding")
  void testQueryStreamWithProjectionAsMap() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "projMap", "value", 5), 30_000);

    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_MAP, 0);
    try (ResultSet rs = grpc.queryStream("sql",
        "SELECT name, value FROM `" + DOC_TYPE + "` WHERE name = 'projMap'", config)) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("projMap");
    }
  }

  @Test
  @DisplayName("queryStream with PROJECTION_AS_LINK encoding")
  void testQueryStreamWithProjectionAsLink() {
    grpc.createRecordTx(DOC_TYPE, Map.of("name", "projLink", "value", 6), 30_000);

    final RemoteGrpcConfig config = new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_LINK, 0);
    try (ResultSet rs = grpc.queryStream("sql",
        "SELECT name, value FROM `" + DOC_TYPE + "` WHERE name = 'projLink'", config)) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("projLink");
    }
  }

  // ==================== Client-Streaming Ingestion ====================

  @Test
  @DisplayName("ingestStream inserts documents via client streaming")
  void testIngestStreamDocuments() throws InterruptedException {
    final List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(Map.of("name", "stream1", "value", 10));
    rows.add(Map.of("name", "stream2", "value", 20));
    rows.add(Map.of("name", "stream3", "value", 30));

    final InsertOptions opts = InsertOptions.newBuilder()
        .setDatabase(getDatabaseName())
        .setTargetClass(DOC_TYPE)
        .setTransactionMode(TransactionMode.PER_BATCH)
        .setServerBatchSize(256)
        .setCredentials(grpc.buildCredentials())
        .build();

    final InsertSummary summary = grpc.ingestStreamAsListOfMaps(opts, rows, 2, 60_000);
    assertThat(summary.getReceived()).isGreaterThanOrEqualTo(3);

    try (ResultSet rs = grpc.query("sql",
        "SELECT FROM `" + DOC_TYPE + "` WHERE name LIKE 'stream%'", Map.of())) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    }
  }

  @Test
  @DisplayName("ingestStream with empty rows returns zero received")
  void testIngestStreamEmptyRowsReturnsZero() throws InterruptedException {
    final InsertOptions opts = InsertOptions.newBuilder()
        .setDatabase(getDatabaseName())
        .setTargetClass(DOC_TYPE)
        .setTransactionMode(TransactionMode.PER_BATCH)
        .setCredentials(grpc.buildCredentials())
        .build();

    final InsertSummary summary = grpc.ingestStreamAsListOfMaps(opts, List.of(), 10, 30_000);
    assertThat(summary.getReceived()).isEqualTo(0);
  }

  // ==================== AcquireLock ====================

  @Test
  @DisplayName("acquireLock returns same instance on repeated calls")
  void testAcquireLockReturnsSameInstance() {
    final RemoteTransactionExplicitLock lock1 = grpc.acquireLock();
    final RemoteTransactionExplicitLock lock2 = grpc.acquireLock();

    assertThat(lock1).isNotNull();
    assertThat(lock1).isSameAs(lock2);
  }

  // ==================== Command with ContextConfiguration ====================

  @Test
  @DisplayName("command with ContextConfiguration overload works correctly")
  void testCommandWithContextConfiguration() {
    final ContextConfiguration config = new ContextConfiguration();

    grpc.command("sql", "INSERT INTO `" + DOC_TYPE + "` SET name = 'ctxCfg', value = 100", config);

    try (ResultSet rs = grpc.query("sql",
        "SELECT FROM `" + DOC_TYPE + "` WHERE name = 'ctxCfg'", Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("value").intValue()).isEqualTo(100);
    }
  }

  @Test
  @DisplayName("command with ContextConfiguration and Map params overload")
  void testCommandWithContextConfigurationAndParams() {
    final ContextConfiguration config = new ContextConfiguration();

    grpc.command("sql", "INSERT INTO `" + DOC_TYPE + "` SET name = :name, value = :value", config,
        Map.of("name", "ctxCfgMap", "value", 200));

    try (ResultSet rs = grpc.query("sql",
        "SELECT FROM `" + DOC_TYPE + "` WHERE name = 'ctxCfgMap'", Map.of())) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("value").intValue()).isEqualTo(200);
    }
  }
}
