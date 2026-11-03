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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.index.CrashingChildJvm;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the posting logs of a sparse vector index give back after a process kill (issue #11239), beyond the plain committed
 * inserts {@link SparseVectorCommittedPostingsSurviveCrashTest} covers: nothing a rolled back or conflicted commit wrote,
 * tombstones of updates and deletes, the postings of both log generations around a flush, and a kill in the middle of a flush
 * that leaves both logs holding postings.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparsePostingLogCrashRecoveryTest {
  private static final int    DIMENSIONS = 1_000;
  private static final String TYPE_NAME  = "LogCrashDoc";
  private static final String IDX_NAME   = TYPE_NAME + "[tokens,weights]";

  @TempDir
  Path tempDirectory;

  /** A rolled back transaction leaves no posting in the log: its records are gone after the reopen, and so are its postings. */
  @Test
  void aRolledBackTransactionLeavesNoPostingBehind() throws Exception {
    final Path path = crash("rollback");
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database database = factory.open()) {
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(100L);
      assertThat(countEntries(database)).isEqualTo(200L);
      // the rolled back records had their own dimensions 500..549
      for (int dim = 500; dim < 550; dim++)
        assertThat(search(database, dim)).as("dim %d", dim).isEmpty();
      assertEveryRecordFoundOnItsDimension(database);
      database.drop();
    }
  }

  /** An update's and a delete's tombstones come back with the reopen: the old dimension and the deleted records find nothing. */
  @Test
  void tombstonesOfUpdatesAndDeletesSurviveACrash() throws Exception {
    final Path path = crash("update-delete");
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database database = factory.open()) {
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(90L);
      // ids 0..9 moved from their own dimension to id + 600; ids 10..19 were deleted
      for (int id = 0; id < 10; id++) {
        assertThat(search(database, id)).as("old dim of updated id %d", id).isEmpty();
        assertThat(search(database, id + 600)).as("new dim of updated id %d", id).hasSize(1);
      }
      for (int id = 10; id < 20; id++)
        assertThat(search(database, id)).as("deleted id %d", id).isEmpty();
      for (int id = 20; id < 100; id++)
        assertThat(search(database, id)).as("untouched id %d", id).hasSize(1);
      // 90 records, 2 postings each
      assertThat(countEntries(database)).isEqualTo(180L);
      database.drop();
    }
  }

  /** Postings sealed by a flush, then more committed to the other log: both come back, the second from the log. */
  @Test
  void postingsOfBothSidesOfAFlushSurviveACrash() throws Exception {
    final Path path = crash("flush-then-write");
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database database = factory.open()) {
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(300L);
      assertEveryRecordFoundOnItsDimension(database);
      assertThat(countEntries(database)).isEqualTo(600L);
      // the log the first flush sealed was reset: only the postings committed after it are replayed
      assertThat(engine(database).segmentCount()).isGreaterThanOrEqualTo(1);
      database.drop();
    }
  }

  /**
   * A kill in the middle of a flush, after a commit landed in the log the flush moved to: both logs hold postings at the reopen,
   * the next flush has nowhere to move the commits to and seals both, and a second kill after it still loses nothing.
   */
  @Test
  void aCrashInTheMiddleOfAFlushLeavesBothLogsReplayable() throws Exception {
    final Path path = crash("crash-mid-flush");

    // Reopened in a child, since a clean close here would seal and reset both logs: the flush after the reopen finds both logs
    // holding postings, seals them together and resets both; then a write and a kill
    final CrashingChildJvm.Result crashed = CrashingChildJvm.run(getClass(), 120, "flush-both-then-crash", path.toString());
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CrashingChildJvm.CRASH_EXIT_CODE);

    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database database = factory.open()) {
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(300L);
      assertEveryRecordFoundOnItsDimension(database);
      assertThat(countEntries(database)).isEqualTo(600L);
      database.drop();
    }
  }

  /**
   * An index created before the posting logs existed (simulated by deleting its log files from a cleanly closed database) opens
   * as before, and its first write creates the logs, so the postings committed from then on survive a kill as well.
   */
  @Test
  void anIndexCreatedBeforeThePostingLogsGetsThemOnItsFirstWrite() throws Exception {
    final Path path = tempDirectory.resolve("legacy");
    final CrashingChildJvm.Result prepared = CrashingChildJvm.run(getClass(), 60, "prepare", path.toString());
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();

    final File[] logs = path.toFile().listFiles((dir, name) -> name.endsWith("." + SparsePostingLog.FILE_EXT));
    assertThat(logs).hasSize(2);
    for (final File log : logs)
      assertThat(log.delete()).isTrue();

    final CrashingChildJvm.Result crashed = CrashingChildJvm.run(getClass(), 120, "legacy-write", path.toString());
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CrashingChildJvm.CRASH_EXIT_CODE);

    assertThat(path.toFile().listFiles((dir, name) -> name.endsWith("." + SparsePostingLog.FILE_EXT))).hasSize(2);
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database database = factory.open()) {
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(100L);
      assertEveryRecordFoundOnItsDimension(database);
      assertThat(countEntries(database)).isEqualTo(200L);
      database.drop();
    }
  }

  /** A commit that loses the page-version check rolls its log entries back with everything else. */
  @Test
  void aConflictedCommitLeavesNoPostingBehind() throws Exception {
    final Path path = crash("conflict");
    try (final DatabaseFactory factory = new DatabaseFactory(path.toString()); final Database database = factory.open()) {
      assertThat(database.countType(TYPE_NAME, false)).isEqualTo(100L);
      // id 0 moved to dim 700 by the commit that won; the one that lost wanted dim 800
      assertThat(search(database, 700)).hasSize(1);
      assertThat(search(database, 800)).isEmpty();
      assertThat(search(database, 0)).isEmpty();
      assertThat(countEntries(database)).isEqualTo(200L);
      database.drop();
    }
  }

  private Path crash(final String mode) throws Exception {
    final Path path = tempDirectory.resolve(mode);
    final CrashingChildJvm.Result prepared = CrashingChildJvm.run(getClass(), 60, "prepare", path.toString());
    assertThat(prepared.exitCode()).as(prepared.output()).isZero();
    final CrashingChildJvm.Result crashed = CrashingChildJvm.run(getClass(), 120, mode, path.toString());
    assertThat(crashed.exitCode()).as(crashed.output()).isEqualTo(CrashingChildJvm.CRASH_EXIT_CODE);
    return path;
  }

  private static void assertEveryRecordFoundOnItsDimension(final Database database) {
    database.iterateType(TYPE_NAME, false).forEachRemaining(record -> {
      final int id = record.asDocument().getInteger("id");
      assertThat(search(database, id)).as("id %d", id).containsExactly(record.getIdentity());
    });
  }

  private static long countEntries(final Database database) {
    return ((TypeIndex) database.getSchema().getIndexByName(IDX_NAME)).countEntries();
  }

  private static PaginatedSparseVectorEngine engine(final Database database) {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    return ((LSMSparseVectorIndex) index.getIndexesOnBuckets()[0]).getEngine();
  }

  private static Set<RID> search(final Database database, final int dim) {
    final Set<RID> found = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))", IDX_NAME,
        new int[] { dim }, new float[] { 1.0f }, 10)) {
      while (rs.hasNext())
        found.add(rs.next().getProperty("@rid"));
    }
    return found;
  }

  private static void insert(final Database database, final int from, final int to) {
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        database.newDocument(TYPE_NAME).set("id", i).set("tokens", new int[] { i, DIMENSIONS - 1 })
            .set("weights", new float[] { 1.0f, 0.5f }).save();
    });
  }

  private static MutableDocument byId(final Database database, final int id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE id = ?", id)) {
      return rs.next().toElement().modify();
    }
  }

  private static void flush(final Database database) {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    ((LSMSparseVectorIndex) index.getIndexesOnBuckets()[0]).flush();
  }

  public static void main(final String[] args) throws Exception {
    final String mode = args[0];
    if ("prepare".equals(mode)) {
      try (final DatabaseFactory factory = new DatabaseFactory(args[1]); final Database database = factory.create()) {
        database.transaction(() -> {
          final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
          type.createProperty("id", Type.INTEGER);
          type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
          type.createProperty("weights", Type.ARRAY_OF_FLOATS);
          database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
              .withUnique(true).create();
          database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" }).withSparseVectorType()
              .withDimensions(DIMENSIONS).create();
        });
      }
      return;
    }

    final DatabaseFactory factory = new DatabaseFactory(args[1]);
    final Database database = factory.open();
    switch (mode) {
    case "rollback" -> {
      insert(database, 0, 100);
      database.begin();
      for (int i = 500; i < 550; i++)
        database.newDocument(TYPE_NAME).set("id", i).set("tokens", new int[] { i, DIMENSIONS - 1 })
            .set("weights", new float[] { 1.0f, 0.5f }).save();
      database.rollback();
    }
    case "update-delete" -> {
      insert(database, 0, 100);
      database.transaction(() -> {
        for (int id = 0; id < 10; id++)
          byId(database, id).set("tokens", new int[] { id + 600, DIMENSIONS - 1 }).save();
      });
      database.transaction(() -> {
        for (int id = 10; id < 20; id++)
          byId(database, id).delete();
      });
    }
    case "flush-then-write" -> {
      insert(database, 0, 100);
      flush(database);
      insert(database, 100, 200);
      flush(database);
      insert(database, 200, 300);
    }
    case "crash-mid-flush" -> {
      insert(database, 0, 100);
      // inside the flush, after the swap and before the segment is built: a commit lands in the other log, then the process dies
      engine(database).setMutationHookForTest(() -> {
        final Thread writer = new Thread(() -> insert(database, 100, 200));
        writer.start();
        try {
          writer.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        Runtime.getRuntime().halt(CrashingChildJvm.CRASH_EXIT_CODE);
      });
      flush(database);
    }
    case "legacy-write" -> {
      if (!engine(database).getPostingLogFileIds().isEmpty())
        throw new IllegalStateException("The index was expected to open without posting logs");
      insert(database, 0, 100);
    }
    case "flush-both-then-crash" -> {
      if (engine(database).activePostingLogsForTest() != 3)
        throw new IllegalStateException("Both posting logs were expected to hold postings after the reopen");
      if (database.countType(TYPE_NAME, false) != 200L || countEntries(database) != 400L)
        throw new IllegalStateException("Both posting logs were expected to be replayed: " + countEntries(database));
      flush(database);
      if (engine(database).activePostingLogsForTest() != 0)
        throw new IllegalStateException("The flush was expected to reset both posting logs");
      insert(database, 200, 300);
      if (engine(database).activePostingLogsForTest() == 0)
        throw new IllegalStateException("The insert after the flush was expected to land in a posting log");
    }
    case "conflict" -> {
      insert(database, 0, 100);
      final CountDownLatch loaded = new CountDownLatch(1);
      final CountDownLatch winnerCommitted = new CountDownLatch(1);
      final AtomicReference<Throwable> loserOutcome = new AtomicReference<>();
      final Thread loser = new Thread(() -> {
        try {
          database.begin();
          byId(database, 0).set("tokens", new int[] { 800, DIMENSIONS - 1 }).save();
          loaded.countDown();
          winnerCommitted.await(30, TimeUnit.SECONDS);
          database.commit();
        } catch (final Throwable t) {
          loserOutcome.set(t);
          if (database.isTransactionActive())
            database.rollback();
        }
      });
      loser.start();
      loaded.await(30, TimeUnit.SECONDS);
      database.transaction(() -> byId(database, 0).set("tokens", new int[] { 700, DIMENSIONS - 1 }).save());
      winnerCommitted.countDown();
      loser.join();
      if (!(loserOutcome.get() instanceof ConcurrentModificationException))
        throw new IllegalStateException("The second commit was expected to conflict, got: " + loserOutcome.get());
    }
    default -> throw new IllegalArgumentException("Unknown mode " + mode);
    }
    Runtime.getRuntime().halt(CrashingChildJvm.CRASH_EXIT_CODE);
  }
}
