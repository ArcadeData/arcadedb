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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Complements the synthetic replay-order tests with actual GraphBatch WAL, page replay and native index lookups.
 * Flushes are suspended before committing the added graph, so a successful reopen cannot pass just because its
 * pages happened to reach disk first. No WAL bytes are fabricated. Housekeeping runs at deterministic boundaries
 * before the first transaction read and after a real apply, on a different thread from recovery.
 */
@Tag("slow")
class RecoveryHousekeepingGraphTest extends TestHelper {
  private static final String PAYLOAD = "p".repeat(16 * 1024);
  private static final int LARGE_BATCH = 4_200;
  private static final int EDGE_COUNT = LARGE_BATCH + 4;
  private final List<RID> vertices = new ArrayList<>();
  private final Map<Integer, RID> edges = new LinkedHashMap<>();
  private Callable<Void> recoveryCallback;

  @Override
  protected void endTest() {
    // A failed factory.open() can leave a context for the rejected instance, while database still refers to the
    // killed fixture. Do not let TestHelper's teardown mistake those two instances for the same active owner.
    if (!database.isOpen())
      DatabaseContext.INSTANCE.removeCurrentThreadContexts();
  }

  @Test
  void realGraphSurvivesHousekeepingBeforeFirstReadAndBetweenReplays() throws Exception {
    final Map<Path, String> input = prepareCrashedGraph();
    final AtomicReference<BoundaryManager> observed = installOnNextRecovery(false, null);

    database = factory.open();
    verifyGraph();
    assertThat(observed.get().firstReadChecks).isPositive();
    assertThat(observed.get().appliedTransactions).isGreaterThan(1);
    for (final Path path : input.keySet())
      assertThat(path).as("fully replayed input can be retired").doesNotExist();

    // A subsequent ordinary commit and clean reopen still work, and retain the recovered graph.
    database.transaction(() -> database.newVertex("RecoveryVertex").set("id", 99).save());
    database.close();
    assertTimerStopped(observed.get());
    database = factory.open();
    verifyGraph();
    try (var cursor = database.lookupByKey("RecoveryVertex", "id", 99)) {
      assertThat(cursor.hasNext()).isTrue();
    }
  }

  @Test
  void repeatedFirstReadFailuresPreserveInputAndAllowARealRetry() throws Exception {
    final Map<Path, String> input = prepareCrashedGraph();
    final AtomicReference<BoundaryManager> observed = installOnNextRecovery(true, null);
    for (int attempt = 0; attempt < 2; ++attempt) {
      assertThatThrownBy(() -> database = factory.open()).hasStackTraceContaining("injected first WAL read failure");
      assertThat(observed.get().firstReadChecks).isPositive();
      assertThat(observed.get().appliedTransactions).isZero();
      assertInputPreserved(input);
      assertTimerStopped(observed.get());
    }
    // The callback copies these controls to each fresh manager; remove it for the final uninstrumented open.
    observed.get().owner.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, recoveryCallback);
    database = factory.open();
    verifyGraph();
  }

  @Test
  void failedReplayCleanupPreservesInputAndRetryIsIdempotent() throws Exception {
    final Map<Path, String> input = prepareCrashedGraph();
    final AtomicReference<BoundaryManager> observed = installOnNextRecovery(false, () -> {
      throw new IllegalStateException("injected failure after real WAL apply");
    });
    assertThatThrownBy(() -> database = factory.open()).hasStackTraceContaining("injected failure after real WAL apply");
    assertThat(observed.get().appliedTransactions).isOne();
    assertInputPreserved(input);
    assertTimerStopped(observed.get());
    observed.get().owner.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, recoveryCallback);
    database = factory.open();
    verifyGraph();
    database.close();
    database = factory.open();
    verifyGraph();
  }

  /**
   * An Error escaping the replay (the realistic one being an OutOfMemoryError on a large WAL) is not an Exception, and
   * the failed-open cleanup used to catch only those: the instance stayed marked open with database.lck locked and its
   * WAL timer running, so the path could not be opened again in this JVM.
   */
  @Test
  void anErrorEscapingReplayReleasesTheFailedOpenAndPreservesInput() throws Exception {
    final Map<Path, String> input = prepareCrashedGraph();
    final AtomicReference<BoundaryManager> observed = installOnNextRecovery(false, () -> {
      throw new OutOfMemoryError("injected error after real WAL apply");
    });
    assertThatThrownBy(() -> database = factory.open()).isInstanceOf(OutOfMemoryError.class)
        .hasMessage("injected error after real WAL apply");
    assertThat(observed.get().appliedTransactions).isOne();
    assertInputPreserved(input);

    observed.get().owner.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, recoveryCallback);
    database = factory.open();
    verifyGraph();
    assertTimerStopped(observed.get());
  }

  private AtomicReference<BoundaryManager> installOnNextRecovery(final boolean failRead, final Runnable applyFailure) {
    final AtomicReference<BoundaryManager> observed = new AtomicReference<>();
    recoveryCallback = () -> {
      final LocalDatabase opening = (LocalDatabase) DatabaseContext.INSTANCE.getActiveDatabase();
      // Retire the normal timer/owner before installing the instrumented one. Keep every pending WAL.
      opening.getTransactionManager().close(false, true);
      final BoundaryManager manager = new BoundaryManager(opening, failRead, applyFailure);
      final Field field = LocalDatabase.class.getDeclaredField("transactionManager");
      field.setAccessible(true);
      field.set(opening, manager);
      observed.set(manager);
      return null;
    };
    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, recoveryCallback);
    return observed;
  }

  private Map<Path, String> prepareCrashedGraph() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FILES, 2);
    database.getConfiguration().setValue(GlobalConfiguration.MAX_PAGE_RAM, 512L);
    final var vertexType = database.getSchema().createVertexType("RecoveryVertex", 1);
    vertexType.createProperty("id", Type.INTEGER);
    vertexType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    final var edgeType = database.getSchema().createEdgeType("RecoveryEdge", 1);
    edgeType.createProperty("id", Type.INTEGER);
    edgeType.createProperty("payload", Type.STRING);
    edgeType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    database.transaction(() -> {
      for (int i = 0; i < 8; ++i)
        vertices.add(database.newVertex("RecoveryVertex").set("id", i).save().getIdentity());
      vertices.getFirst().asVertex().newEdge("RecoveryEdge", vertices.get(1), "id", -1, "payload", "baseline");
    });
    // A clean baseline persists schema/dictionary and an existing edge before the crash-only changes.
    database.close();
    database = factory.open();
    final DatabaseInternal db = (DatabaseInternal) database;
    cancelTimer(db.getTransactionManager());
    assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true)).isTrue();
    addEdges(0, LARGE_BATCH);
    // Real commits cross the production threshold. Pending flush acknowledgements keep the retired WAL alive.
    assertThat(walPaths().stream().anyMatch(path -> path.toFile().length() > 64L * 1024 * 1024)).isTrue();
    db.getTransactionManager().checkWALFilesForTesting();
    addEdges(LARGE_BATCH, EDGE_COUNT);
    assertThat(walPaths().stream().filter(path -> path.toFile().length() > 0).count()).isGreaterThanOrEqualTo(2);
    for (int id = -1; id < EDGE_COUNT; ++id)
      try (var cursor = database.lookupByKey("RecoveryEdge", "id", id)) {
        assertThat(cursor.hasNext()).isTrue();
        edges.put(id, cursor.next().getIdentity());
        assertThat(cursor.hasNext()).isFalse();
      }
    db.kill();
    database.close();
    final Map<Path, String> input = new LinkedHashMap<>();
    for (final Path path : walPaths())
      input.put(path, digest(path));
    return input;
  }

  private void addEdges(final int start, final int end) {
    try (final GraphBatch batch = GraphBatch.builder(database).withWAL(true).withWALFlush(WALFile.FlushType.YES_FULL)
        .withParallelFlush(false).withLightEdges(false).withBatchSize(100).build()) {
      for (int id = start; id < end; ++id)
        batch.newEdge(vertices.get(id % 8), "RecoveryEdge", vertices.get((id + 1) % 8), "id", id, "payload", PAYLOAD);
    }
  }

  private void verifyGraph() {
    for (int id = 0; id < vertices.size(); ++id)
      try (var cursor = database.lookupByKey("RecoveryVertex", "id", id)) {
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next().getIdentity()).isEqualTo(vertices.get(id));
        assertThat(cursor.hasNext()).isFalse();
      }
    for (final var entry : edges.entrySet()) {
      final int id = entry.getKey();
      final RID expected = entry.getValue();
      try (var cursor = database.lookupByKey("RecoveryEdge", "id", id)) {
        assertThat(cursor.hasNext()).as("indexed edge %s", id).isTrue();
        assertThat(cursor.next().getIdentity()).isEqualTo(expected);
        assertThat(cursor.hasNext()).isFalse();
      }
      final Edge edge = database.lookupByRID(expected, true).asEdge();
      assertThat(edge.getInteger("id")).isEqualTo(id);
      assertThat(edge.getString("payload")).isEqualTo(id == -1 ? "baseline" : PAYLOAD);
      assertThat(edge.getOut()).isEqualTo(vertices.get(id == -1 ? 0 : id % 8));
      assertThat(edge.getIn()).isEqualTo(vertices.get(id == -1 ? 1 : (id + 1) % 8));
    }
    final List<RID> outgoing = new ArrayList<>();
    final List<RID> incoming = new ArrayList<>();
    for (final RID rid : vertices) {
      final Vertex vertex = database.lookupByRID(rid, true).asVertex();
      for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT, "RecoveryEdge"))
        outgoing.add(edge.getIdentity());
      for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN, "RecoveryEdge"))
        incoming.add(edge.getIdentity());
    }
    assertThat(outgoing).containsExactlyInAnyOrderElementsOf(edges.values());
    assertThat(incoming).containsExactlyInAnyOrderElementsOf(edges.values());
    try (var result = database.query("sql", "SELECT count() AS n FROM RecoveryEdge")) {
      assertThat(result.next().<Long>getProperty("n")).isEqualTo(edges.size());
    }
  }

  private List<Path> walPaths() throws Exception {
    try (var paths = Files.list(Path.of(getDatabasePath()))) {
      return paths.filter(path -> path.toString().endsWith(".wal")).sorted().toList();
    }
  }

  private static String digest(final Path path) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (var stream = Files.newInputStream(path)) {
      final byte[] buffer = new byte[64 * 1024];
      for (int size; (size = stream.read(buffer)) != -1; )
        digest.update(buffer, 0, size);
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static void assertInputPreserved(final Map<Path, String> input) throws Exception {
    for (final var entry : input.entrySet()) {
      assertThat(entry.getKey()).exists();
      assertThat(digest(entry.getKey())).isEqualTo(entry.getValue());
    }
  }

  private static void cancelTimer(final TransactionManager manager) throws Exception {
    final Field field = TransactionManager.class.getDeclaredField("task");
    field.setAccessible(true);
    ((Timer) field.get(manager)).cancel();
  }

  private static void assertTimerStopped(final BoundaryManager manager) throws Exception {
    for (final Thread thread : manager.timerThreads) {
      thread.join(60_000);
      assertThat(thread.isAlive()).as("retired manager's timer must terminate").isFalse();
    }
  }

  private static final class BoundaryManager extends TransactionManager {
    private final DatabaseInternal owner;
    private final List<Thread> timerThreads;
    private final boolean failFirstRead;
    private final Runnable applyFailure;
    private int firstReadChecks;
    private int appliedTransactions;

    BoundaryManager(final DatabaseInternal db, final boolean failRead, final Runnable applyFailure) {
      super(db);
      owner = db;
      failFirstRead = failRead;
      this.applyFailure = applyFailure;
      timerThreads = Thread.getAllStackTraces().keySet().stream()
          .filter(thread -> thread.getName().equals("ArcadeDB TransactionManager " + db.getName())).toList();
      assertThat(timerThreads).isNotEmpty();
    }

    @Override
    WALFile openWALFileForRecovery(final String path) throws FileNotFoundException {
      return new WALFile(path) {
        @Override
        public WALTransaction getFirstTransaction() {
          ++firstReadChecks;
          RecoveryHousekeepingTest.driveHousekeeping(BoundaryManager.this);
          if (failFirstRead)
            throw new IllegalStateException("injected first WAL read failure");
          return super.getFirstTransaction();
        }
      };
    }

    @Override
    public boolean applyChanges(final WALFile.WALTransaction tx, final Map<Integer, Integer> delta, final boolean ignoreErrors) {
      final boolean changed = super.applyChanges(tx, delta, ignoreErrors);
      ++appliedTransactions;
      RecoveryHousekeepingTest.driveHousekeeping(this);
      if (applyFailure != null)
        applyFailure.run();
      return changed;
    }

    @Override
    public void checkIntegrity() {
      try {
        super.checkIntegrity();
      } catch (final RuntimeException | Error failure) {
        // Run a tick in the gap between failed replay and LocalDatabase's real failed-open cleanup.
        RecoveryHousekeepingTest.driveHousekeeping(this);
        throw failure;
      }
    }
  }
}
