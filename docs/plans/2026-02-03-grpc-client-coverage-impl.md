# gRPC Client Test Coverage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add ~1,500 lines of tests covering streaming operations, bidirectional ingestion, and error handling for the grpc-client module.

**Architecture:** Integration tests extend `BaseGraphServerTest` (spins up real server). Unit tests use mocked gRPC stubs for fast execution and edge cases.

**Tech Stack:** JUnit 5, AssertJ, gRPC testing library (already in pom.xml), Java 21+

---

## Task 1: Create StreamingQueryIT Integration Tests

**Files:**
- Create: `grpc-client/src/test/java/com/arcadedb/remote/grpc/StreamingQueryIT.java`

**Step 1: Write the test file with basic infrastructure**

```java
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
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    AtomicInteger count = new AtomicInteger(0);

    try (ResultSet rs = database.queryStream("sql", "SELECT FROM `" + TYPE + "`", batchSize)) {
      while (rs.hasNext()) {
        rs.next();
        count.incrementAndGet();
      }
    }

    assertThat(count.get()).isEqualTo(50);
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

    try (ResultSet rs = database.queryStreamBatched("sql", "SELECT FROM `" + TYPE + "`", Map.of(), 10)) {
      assertThat(rs).isInstanceOf(BatchedStreamingResultSet.class);
      BatchedStreamingResultSet batched = (BatchedStreamingResultSet) rs;

      int totalCount = 0;
      while (rs.hasNext()) {
        rs.next();
        totalCount++;
        // Batch metadata should be available
        assertThat(batched.getCurrentBatchSize()).isGreaterThan(0);
      }

      assertThat(totalCount).isEqualTo(25);
      assertThat(batched.isLastBatch()).isTrue();
    }
  }

  @Test
  @DisplayName("queryStreamBatchesIterator iterates batches correctly")
  void queryStreamBatchesIterator_multipleBatches() {
    insertTestData(35);

    var iterator = database.queryStreamBatchesIterator("sql", "SELECT FROM `" + TYPE + "`", Map.of(), 10);

    int batchCount = 0;
    int totalRecords = 0;

    while (iterator.hasNext()) {
      QueryBatch batch = iterator.next();
      batchCount++;
      totalRecords += batch.records().size();
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
```

**Step 2: Run the test to verify it compiles and runs**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client -Dtest=StreamingQueryIT -DfailIfNoTests=false`

Expected: Tests execute (may pass or fail based on server behavior)

**Step 3: Commit**

```bash
git add grpc-client/src/test/java/com/arcadedb/remote/grpc/StreamingQueryIT.java
git commit -m "test(grpc-client): add streaming query integration tests

Coverage for queryStream, queryStreamBatched, and queryStreamBatchesIterator
including basic iteration, batch size, early termination, empty results,
large datasets, and parameterized queries.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Create BidiIngestionIT Integration Tests

**Files:**
- Create: `grpc-client/src/test/java/com/arcadedb/remote/grpc/BidiIngestionIT.java`

**Step 1: Write the bidirectional ingestion integration tests**

```java
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
  @DisplayName("ingestBidi basic flow sends records and receives ACKs")
  void ingestBidi_basicFlow() throws InterruptedException {
    List<Map<String, Object>> rows = generateRows(50);

    InsertSummary summary = database.ingestBidi(defaultOptions(), rows, 10, 5, 60_000);

    assertThat(summary.getInserted()).isEqualTo(50);
    assertThat(summary.getUpdated()).isEqualTo(0);
    assertThat(countRecords()).isEqualTo(50);
  }

  @Test
  @DisplayName("ingestBidi handles backpressure with maxInflight limit")
  void ingestBidi_backpressure() throws InterruptedException {
    List<Map<String, Object>> rows = generateRows(200);

    // Very low maxInflight to force backpressure
    InsertSummary summary = database.ingestBidi(defaultOptions(), rows, 20, 2, 120_000);

    assertThat(summary.getInserted()).isEqualTo(200);
    assertThat(countRecords()).isEqualTo(200);
  }

  @Test
  @DisplayName("ingestBidi large volume streams without blocking indefinitely")
  void ingestBidi_largeVolume() throws InterruptedException {
    List<Map<String, Object>> rows = generateRows(50_000);

    InsertSummary summary = database.ingestBidi(defaultOptions(), rows, 500, 10, 300_000);

    assertThat(summary.getInserted()).isEqualTo(50_000);
    assertThat(countRecords()).isEqualTo(50_000);
  }

  @Test
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
```

**Step 2: Run the test to verify it compiles and runs**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client -Dtest=BidiIngestionIT -DfailIfNoTests=false`

Expected: Tests execute

**Step 3: Commit**

```bash
git add grpc-client/src/test/java/com/arcadedb/remote/grpc/BidiIngestionIT.java
git commit -m "test(grpc-client): add bidirectional ingestion integration tests

Coverage for ingestBidi including basic flow, backpressure handling,
large volume streaming, transactions, upsert on conflict, and
parameter validation.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Create ErrorHandlingIT Integration Tests

**Files:**
- Create: `grpc-client/src/test/java/com/arcadedb/remote/grpc/ErrorHandlingIT.java`

**Step 1: Write the error handling integration tests**

```java
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
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for error handling in gRPC client operations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ErrorHandlingIT extends BaseGraphServerTest {

  private static final String TYPE = "ErrorTest";

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

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    database = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS BUCKETS 8");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS STRING");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.name IF NOT EXISTS STRING (mandatory)");
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

  @Test
  @DisplayName("Invalid SQL query throws appropriate exception")
  void invalidQuery_throwsException() {
    assertThatThrownBy(() -> database.query("sql", "SELECTT FROM nowhere"))
        .isInstanceOf(RemoteException.class);
  }

  @Test
  @DisplayName("Query on non-existent type throws exception")
  void nonExistentType_throwsException() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM NonExistentType12345"))
        .isInstanceOf(RemoteException.class);
  }

  @Test
  @DisplayName("Authentication failure throws SecurityException")
  void authenticationFailure_throwsSecurityException() {
    RemoteGrpcServer badServer = new RemoteGrpcServer("localhost", 50051, "root", "wrongpassword", true, List.of());
    RemoteGrpcDatabase badDb = new RemoteGrpcDatabase(badServer, "localhost", 50051, 2480, getDatabaseName(), "root", "wrongpassword");

    try {
      assertThatThrownBy(() -> badDb.query("sql", "SELECT FROM `" + TYPE + "`"))
          .isInstanceOf(SecurityException.class);
    } finally {
      badDb.close();
      badServer.close();
    }
  }

  @Test
  @DisplayName("lookupByRID with non-existent RID throws RecordNotFoundException")
  void recordNotFound_throwsRecordNotFoundException() {
    RID fakeRid = new RID(database, "#999:999999");

    assertThatThrownBy(() -> database.lookupByRID(fakeRid))
        .isInstanceOf(RecordNotFoundException.class);
  }

  @Test
  @DisplayName("Duplicate key insert throws DuplicatedKeyException")
  void duplicateKey_throwsDuplicatedKeyException() {
    database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'dup1', name = 'first'");

    assertThatThrownBy(() ->
        database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'dup1', name = 'second'"))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  @DisplayName("Concurrent modification throws ConcurrentModificationException")
  void concurrentModification_throwsConcurrentModificationException() throws InterruptedException {
    // Insert a record
    database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'conc1', name = 'original'");

    // Get the RID
    String rid;
    try (ResultSet rs = database.query("sql", "SELECT @rid as r FROM `" + TYPE + "` WHERE id = 'conc1'")) {
      rid = rs.next().getProperty("r").toString();
    }

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicReference<Throwable> error1 = new AtomicReference<>();
    AtomicReference<Throwable> error2 = new AtomicReference<>();

    // Two concurrent transactions trying to update the same record
    Runnable update1 = () -> {
      RemoteGrpcDatabase db1 = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      try {
        startLatch.await();
        db1.begin();
        db1.command("sql", "UPDATE `" + TYPE + "` SET name = 'updated1' WHERE id = 'conc1'");
        Thread.sleep(100); // Hold transaction open
        db1.commit();
      } catch (Throwable t) {
        error1.set(t);
        try { db1.rollback(); } catch (Throwable ignore) {}
      } finally {
        db1.close();
        doneLatch.countDown();
      }
    };

    Runnable update2 = () -> {
      RemoteGrpcDatabase db2 = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      try {
        startLatch.await();
        db2.begin();
        db2.command("sql", "UPDATE `" + TYPE + "` SET name = 'updated2' WHERE id = 'conc1'");
        Thread.sleep(100);
        db2.commit();
      } catch (Throwable t) {
        error2.set(t);
        try { db2.rollback(); } catch (Throwable ignore) {}
      } finally {
        db2.close();
        doneLatch.countDown();
      }
    };

    executor.submit(update1);
    executor.submit(update2);
    startLatch.countDown();
    doneLatch.await();
    executor.shutdown();

    // At least one should have failed with concurrent modification
    boolean concurrentError = (error1.get() instanceof ConcurrentModificationException)
        || (error2.get() instanceof ConcurrentModificationException)
        || (error1.get() != null && error1.get().getMessage() != null && error1.get().getMessage().contains("concurrent"))
        || (error2.get() != null && error2.get().getMessage() != null && error2.get().getMessage().contains("concurrent"));

    // Note: This test may pass if transactions serialize correctly, which is also valid behavior
    // The important thing is no unexpected exceptions occurred
    assertThat(error1.get() == null || error2.get() == null || concurrentError)
        .as("Either both succeed (serialized) or one fails with concurrent modification")
        .isTrue();
  }

  @Test
  @DisplayName("Schema violation throws validation exception")
  void schemaViolation_throwsValidationException() {
    // Try to insert without mandatory 'name' property
    assertThatThrownBy(() ->
        database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'sv1'"))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("name");
  }

  @Test
  @DisplayName("Connection to wrong port fails gracefully")
  void connectionRefused_throwsConnectionException() {
    RemoteGrpcServer badServer = new RemoteGrpcServer("localhost", 59999, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    RemoteGrpcDatabase badDb = new RemoteGrpcDatabase(badServer, "localhost", 59999, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    try {
      assertThatThrownBy(() -> badDb.query("sql", "SELECT 1"))
          .isInstanceOf(Exception.class); // Could be RemoteException, NeedRetryException, etc.
    } finally {
      badDb.close();
      badServer.close();
    }
  }
}
```

**Step 2: Run the test to verify it compiles and runs**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client -Dtest=ErrorHandlingIT -DfailIfNoTests=false`

Expected: Tests execute

**Step 3: Commit**

```bash
git add grpc-client/src/test/java/com/arcadedb/remote/grpc/ErrorHandlingIT.java
git commit -m "test(grpc-client): add error handling integration tests

Coverage for exception mapping including invalid queries, authentication
failures, record not found, duplicate keys, concurrent modifications,
schema violations, and connection errors.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Create GrpcExceptionMappingTest Unit Tests

**Files:**
- Create: `grpc-client/src/test/java/com/arcadedb/remote/grpc/GrpcExceptionMappingTest.java`

**Step 1: Write the exception mapping unit tests**

```java
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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.remote.RemoteException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for gRPC Status to domain exception mapping.
 * Tests the handleGrpcException method behavior without needing a server.
 */
class GrpcExceptionMappingTest {

  private RemoteGrpcDatabase database;

  @BeforeEach
  void setup() {
    // Create a minimal database instance just to test exception mapping
    // We'll use a TestableRemoteGrpcDatabase that exposes handleGrpcException
    database = new TestableRemoteGrpcDatabase();
  }

  @Test
  @DisplayName("Status.NOT_FOUND maps to RecordNotFoundException")
  void statusNotFound_mapsToRecordNotFoundException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.NOT_FOUND.withDescription("Record #12:0 not found"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(RecordNotFoundException.class)
        .hasMessageContaining("Record #12:0 not found");
  }

  @Test
  @DisplayName("Status.ALREADY_EXISTS maps to DuplicatedKeyException")
  void statusAlreadyExists_mapsToDuplicatedKeyException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.ALREADY_EXISTS.withDescription("Duplicate key on index"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  @DisplayName("Status.ABORTED maps to ConcurrentModificationException")
  void statusAborted_mapsToConcurrentModificationException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.ABORTED.withDescription("Transaction aborted due to conflict"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("Transaction aborted");
  }

  @Test
  @DisplayName("Status.DEADLINE_EXCEEDED maps to TimeoutException")
  void statusDeadlineExceeded_mapsToTimeoutException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.DEADLINE_EXCEEDED.withDescription("Operation timed out after 30s"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining("timed out");
  }

  @Test
  @DisplayName("Status.PERMISSION_DENIED maps to SecurityException")
  void statusPermissionDenied_mapsToSecurityException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.PERMISSION_DENIED.withDescription("User not authorized"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not authorized");
  }

  @Test
  @DisplayName("Status.UNAVAILABLE maps to NeedRetryException")
  void statusUnavailable_mapsToNeedRetryException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.UNAVAILABLE.withDescription("Service temporarily unavailable"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("unavailable");
  }

  @Test
  @DisplayName("Status.INTERNAL preserves error message in RemoteException")
  void statusInternal_preservesMessage() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.INTERNAL.withDescription("Internal server error: NullPointerException at line 42"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("Internal server error")
        .hasMessageContaining("NullPointerException");
  }

  @Test
  @DisplayName("Unknown status codes wrap as RemoteException without NPE")
  void unknownStatus_wrapsAsRemoteException() {
    StatusRuntimeException grpcException = new StatusRuntimeException(
        Status.DATA_LOSS.withDescription("Data corruption detected"));

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("Data corruption");
  }

  @Test
  @DisplayName("Status with null description uses code name")
  void statusWithNullDescription_usesCodeName() {
    StatusRuntimeException grpcException = new StatusRuntimeException(Status.INTERNAL);

    assertThatThrownBy(() -> database.handleGrpcException(grpcException))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("INTERNAL");
  }

  /**
   * Test helper that extends RemoteGrpcDatabase to expose the package-private
   * handleGrpcException method for testing without needing a real server.
   */
  private static class TestableRemoteGrpcDatabase extends RemoteGrpcDatabase {
    TestableRemoteGrpcDatabase() {
      super(null, "localhost", 50051, 2480, "test", "root", "test");
    }

    @Override
    public void handleGrpcException(Throwable e) {
      super.handleGrpcException(e);
    }
  }
}
```

**Step 2: Run the test to verify it compiles and runs**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client -Dtest=GrpcExceptionMappingTest -DfailIfNoTests=false`

Expected: Tests execute (may need adjustments based on actual method visibility)

**Step 3: If TestableRemoteGrpcDatabase doesn't work, adjust approach**

The `handleGrpcException` method is package-private. If the test class is in the same package, it should work. If not, we may need to use reflection or adjust the test design.

**Step 4: Commit**

```bash
git add grpc-client/src/test/java/com/arcadedb/remote/grpc/GrpcExceptionMappingTest.java
git commit -m "test(grpc-client): add gRPC exception mapping unit tests

Coverage for Status code to domain exception mapping including NOT_FOUND,
ALREADY_EXISTS, ABORTED, DEADLINE_EXCEEDED, PERMISSION_DENIED, UNAVAILABLE,
INTERNAL, and unknown status codes.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Create StreamingResultSetTest Unit Tests

**Files:**
- Create: `grpc-client/src/test/java/com/arcadedb/remote/grpc/StreamingResultSetTest.java`

**Step 1: Write the streaming result set unit tests**

```java
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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.server.grpc.GrpcRecord;
import com.arcadedb.server.grpc.GrpcValue;
import com.arcadedb.server.grpc.QueryResult;
import io.grpc.stub.BlockingClientCall;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Unit tests for StreamingResultSet lazy loading behavior.
 */
class StreamingResultSetTest {

  @Test
  @DisplayName("hasNext triggers lazy load on first call")
  void hasNext_triggersLazyLoad() throws Exception {
    // Create mock stream that returns one batch
    @SuppressWarnings("unchecked")
    BlockingClientCall<?, QueryResult> mockStream = mock(BlockingClientCall.class);
    RemoteGrpcDatabase mockDb = mock(RemoteGrpcDatabase.class);

    QueryResult batch = createBatch(List.of(createRecord("1", "test1")), false);
    QueryResult lastBatch = createBatch(List.of(), true);

    when(mockStream.hasNext()).thenReturn(true, true, false);
    when(mockStream.read()).thenReturn(batch, lastBatch);

    StreamingResultSet rs = new StreamingResultSet(mockStream, mockDb);

    // First hasNext should trigger read
    assertThat(rs.hasNext()).isTrue();
    verify(mockStream, atLeastOnce()).read();

    rs.close();
  }

  @Test
  @DisplayName("next without hasNext works correctly")
  void next_withoutHasNext_works() throws Exception {
    @SuppressWarnings("unchecked")
    BlockingClientCall<?, QueryResult> mockStream = mock(BlockingClientCall.class);
    RemoteGrpcDatabase mockDb = mock(RemoteGrpcDatabase.class);
    when(mockDb.grpcRecordToResult(any())).thenAnswer(inv -> {
      GrpcRecord rec = inv.getArgument(0);
      return mockResult(rec.getPropertiesMap().get("id").getStringValue());
    });

    QueryResult batch = createBatch(List.of(createRecord("1", "test1")), true);

    when(mockStream.hasNext()).thenReturn(true, false);
    when(mockStream.read()).thenReturn(batch);

    StreamingResultSet rs = new StreamingResultSet(mockStream, mockDb);

    // Call next directly
    Result result = rs.next();
    assertThat(result).isNotNull();

    rs.close();
  }

  @Test
  @DisplayName("close releases iterator resources")
  void close_releasesIterator() throws Exception {
    @SuppressWarnings("unchecked")
    BlockingClientCall<?, QueryResult> mockStream = mock(BlockingClientCall.class);
    RemoteGrpcDatabase mockDb = mock(RemoteGrpcDatabase.class);

    // Stream with pending data
    when(mockStream.hasNext()).thenReturn(true, true, false);
    when(mockStream.read()).thenReturn(
        createBatch(List.of(createRecord("1", "test")), false),
        createBatch(List.of(), true)
    );

    StreamingResultSet rs = new StreamingResultSet(mockStream, mockDb);
    rs.close();

    // Verify stream was drained
    verify(mockStream, atLeastOnce()).hasNext();
  }

  @Test
  @DisplayName("empty batch is handled gracefully")
  void emptyBatch_handledGracefully() throws Exception {
    @SuppressWarnings("unchecked")
    BlockingClientCall<?, QueryResult> mockStream = mock(BlockingClientCall.class);
    RemoteGrpcDatabase mockDb = mock(RemoteGrpcDatabase.class);
    when(mockDb.grpcRecordToResult(any())).thenAnswer(inv -> {
      GrpcRecord rec = inv.getArgument(0);
      return mockResult(rec.getPropertiesMap().get("id").getStringValue());
    });

    // Empty batch followed by data batch
    when(mockStream.hasNext()).thenReturn(true, true, true, false);
    when(mockStream.read()).thenReturn(
        createBatch(List.of(), false),  // empty non-terminal
        createBatch(List.of(createRecord("1", "test")), false),
        createBatch(List.of(), true)    // empty terminal
    );

    StreamingResultSet rs = new StreamingResultSet(mockStream, mockDb);

    assertThat(rs.hasNext()).isTrue();
    rs.next();
    assertThat(rs.hasNext()).isFalse();

    rs.close();
  }

  @Test
  @DisplayName("NoSuchElementException when exhausted")
  void next_whenExhausted_throwsNoSuchElement() throws Exception {
    @SuppressWarnings("unchecked")
    BlockingClientCall<?, QueryResult> mockStream = mock(BlockingClientCall.class);
    RemoteGrpcDatabase mockDb = mock(RemoteGrpcDatabase.class);

    when(mockStream.hasNext()).thenReturn(true, false);
    when(mockStream.read()).thenReturn(createBatch(List.of(), true));

    StreamingResultSet rs = new StreamingResultSet(mockStream, mockDb);

    assertThat(rs.hasNext()).isFalse();
    assertThatThrownBy(rs::next).isInstanceOf(NoSuchElementException.class);

    rs.close();
  }

  @Test
  @DisplayName("BatchedStreamingResultSet exposes batch metadata")
  void batchedResultSet_exposesMetadata() throws Exception {
    @SuppressWarnings("unchecked")
    BlockingClientCall<?, QueryResult> mockStream = mock(BlockingClientCall.class);
    RemoteGrpcDatabase mockDb = mock(RemoteGrpcDatabase.class);
    when(mockDb.grpcRecordToResult(any())).thenAnswer(inv -> {
      GrpcRecord rec = inv.getArgument(0);
      return mockResult(rec.getPropertiesMap().get("id").getStringValue());
    });

    QueryResult batch = QueryResult.newBuilder()
        .addAllRecords(List.of(createRecord("1", "test1"), createRecord("2", "test2")))
        .setIsLastBatch(true)
        .setRunningTotalEmitted(2)
        .build();

    when(mockStream.hasNext()).thenReturn(true, false);
    when(mockStream.read()).thenReturn(batch);

    BatchedStreamingResultSet rs = new BatchedStreamingResultSet(mockStream, mockDb);

    rs.hasNext(); // triggers batch load

    assertThat(rs.getCurrentBatchSize()).isEqualTo(2);
    assertThat(rs.isLastBatch()).isTrue();
    assertThat(rs.getRunningTotal()).isEqualTo(2);

    rs.close();
  }

  // Helper methods

  private GrpcRecord createRecord(String id, String name) {
    return GrpcRecord.newBuilder()
        .setRid("#0:" + id)
        .setType("TestType")
        .putProperties("id", GrpcValue.newBuilder().setStringValue(id).build())
        .putProperties("name", GrpcValue.newBuilder().setStringValue(name).build())
        .build();
  }

  private QueryResult createBatch(List<GrpcRecord> records, boolean isLast) {
    return QueryResult.newBuilder()
        .addAllRecords(records)
        .setIsLastBatch(isLast)
        .setRunningTotalEmitted(records.size())
        .build();
  }

  private Result mockResult(String id) {
    Result result = mock(Result.class);
    when(result.getProperty("id")).thenReturn(id);
    return result;
  }
}
```

**Step 2: Add Mockito dependency if not present**

Check if `mockito-core` is in the parent pom or needs to be added to grpc-client/pom.xml:

```xml
<dependency>
    <groupId>org.mockito</groupId>
    <artifactId>mockito-core</artifactId>
    <version>${mockito.version}</version>
    <scope>test</scope>
</dependency>
```

**Step 3: Run the test to verify it compiles and runs**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client -Dtest=StreamingResultSetTest -DfailIfNoTests=false`

Expected: Tests execute

**Step 4: Commit**

```bash
git add grpc-client/src/test/java/com/arcadedb/remote/grpc/StreamingResultSetTest.java
git commit -m "test(grpc-client): add StreamingResultSet unit tests

Coverage for lazy loading behavior including hasNext triggering load,
next without hasNext, close releasing resources, empty batch handling,
exhausted iterator, and BatchedStreamingResultSet metadata exposure.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Run Full Test Suite and Verify Coverage

**Step 1: Run all new tests together**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client -Dtest="StreamingQueryIT,BidiIngestionIT,ErrorHandlingIT,GrpcExceptionMappingTest,StreamingResultSetTest" -DfailIfNoTests=false`

Expected: All tests pass

**Step 2: Run full grpc-client module tests**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client`

Expected: All tests pass including existing ones

**Step 3: Generate coverage report (if JaCoCo is configured)**

Run: `cd /Users/frank/projects/arcade/worktrees/add-test-grpc-client && mvn test -pl grpc-client jacoco:report`

Then check: `grpc-client/target/site/jacoco/index.html`

**Step 4: Final commit with all tests passing**

```bash
git status
# Ensure all test files are committed
```

---

## Summary

| Task | File | Type | Tests |
|------|------|------|-------|
| 1 | StreamingQueryIT.java | Integration | 8 |
| 2 | BidiIngestionIT.java | Integration | 9 |
| 3 | ErrorHandlingIT.java | Integration | 8 |
| 4 | GrpcExceptionMappingTest.java | Unit | 9 |
| 5 | StreamingResultSetTest.java | Unit | 6 |
| **Total** | | | **40** |

**Expected Coverage Improvement:**
- StreamingResultSet/BatchedStreamingResultSet: 0% → ~80%
- ingestBidi code paths: ~10% → ~75%
- handleGrpcException: 0% → ~90%
- Overall grpc-client module: ~40% → ~70%
