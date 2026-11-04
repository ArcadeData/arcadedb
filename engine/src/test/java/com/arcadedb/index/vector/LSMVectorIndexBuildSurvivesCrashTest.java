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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.index.CrashingChildJvm;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A vector index whose CREATE INDEX / REBUILD INDEX has returned must not lose entries to a process kill. The build
 * bulk-loads with the WAL off ({@code LSMVectorIndex.build}) and its pages reach disk through the asynchronous page
 * flush, so a kill right after the statement returns can reopen an index marked READY that misses vectors.
 * <p>
 * The "BeforeTheAsyncFlush" variants hold the page flush off while the statement runs, so the kill lands
 * deterministically before the asynchronous flush: a WAL-backed commit survives that (recovery replays it), a WAL-less
 * one cannot. The plain variants kill right after the statement returns and race the flush.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class LSMVectorIndexBuildSurvivesCrashTest {
  private static final int    DIMENSIONS      = 64;
  // 1 MB chunks at ~288 bytes a vector: about 3,600 vectors a chunk, so the bulk load commits several chunks
  private static final int    VECTORS         = 12_000;
  private static final String TYPE_NAME       = "CrashVecDoc";
  private static final String IDX_NAME        = TYPE_NAME + "[embedding]";
  private static final String AFTER_TYPE_NAME = "AfterBuildDoc";

  @TempDir
  Path tempDirectory;

  @Test
  void createIndexThenKill() throws Exception {
    final Path databasePath = tempDirectory.resolve("create-kill");
    runChild("prepare", databasePath, 0);
    runChild("create-halt", databasePath, CrashingChildJvm.CRASH_EXIT_CODE);
    assertIndexHoldsEveryVector(databasePath);
  }

  @Test
  void createIndexThenKillBeforeTheAsyncFlush() throws Exception {
    final Path databasePath = tempDirectory.resolve("create-kill-noflush");
    runChild("prepare", databasePath, 0);
    runChild("create-halt-noflush", databasePath, CrashingChildJvm.CRASH_EXIT_CODE);
    assertIndexHoldsEveryVector(databasePath);
  }

  @Test
  void rebuildIndexThenKill() throws Exception {
    final Path databasePath = tempDirectory.resolve("rebuild-kill");
    runChild("prepare-indexed", databasePath, 0);
    runChild("rebuild-halt", databasePath, CrashingChildJvm.CRASH_EXIT_CODE);
    assertIndexHoldsEveryVector(databasePath);
  }

  @Test
  void rebuildIndexThenKillBeforeTheAsyncFlush() throws Exception {
    final Path databasePath = tempDirectory.resolve("rebuild-kill-noflush");
    runChild("prepare-indexed", databasePath, 0);
    runChild("rebuild-halt-noflush", databasePath, CrashingChildJvm.CRASH_EXIT_CODE);
    assertIndexHoldsEveryVector(databasePath);
  }

  /**
   * The first insert of an indexed record after the build writes the index pages the build committed WITHOUT the WAL, and
   * writes them THROUGH the WAL: it has to come back from recovery too, and so does every vector the build loaded.
   */
  @Test
  void anIndexedInsertCommittedAfterTheBuildSurvivesAKillBeforeTheAsyncFlush() throws Exception {
    final Path databasePath = tempDirectory.resolve("create-indexed-write-kill-noflush");
    runChild("prepare", databasePath, 0);
    runChild("create-halt-noflush-indexed-write", databasePath, CrashingChildJvm.CRASH_EXIT_CODE);
    assertIndexHoldsEveryVector(databasePath, VECTORS + 1);
  }

  /**
   * The build's last commit is the WAL-backed commit of the transaction {@code TypeIndexBuilder} wraps it in, and it
   * modifies a page the WAL-less chunk commits created: if those never reached the disk, WAL recovery meets a version
   * gap on that page, aborts and replays NOTHING after it, so an unrelated transaction committed after the build is
   * lost as well.
   */
  @Test
  void aTransactionCommittedAfterTheBuildSurvivesAKillBeforeTheAsyncFlush() throws Exception {
    final Path databasePath = tempDirectory.resolve("create-write-kill-noflush");
    runChild("prepare", databasePath, 0);
    runChild("create-halt-noflush-write", databasePath, CrashingChildJvm.CRASH_EXIT_CODE);

    assertRecordCommittedAfterTheStatementSurvived(databasePath);
  }

  /**
   * Control for the variants above: the same flush suspension and kill with no index statement before the write. The
   * WAL-backed transaction comes back through recovery, so the harness itself loses nothing.
   */
  @Test
  void aTransactionCommittedWithoutAnIndexBuildSurvivesAKillBeforeTheAsyncFlush() throws Exception {
    final Path databasePath = tempDirectory.resolve("plain-write-kill-noflush");
    runChild("prepare", databasePath, 0);
    runChild("plain-halt-noflush-write", databasePath, CrashingChildJvm.CRASH_EXIT_CODE);
    assertRecordCommittedAfterTheStatementSurvived(databasePath);
  }

  private static void assertRecordCommittedAfterTheStatementSurvived(final Path databasePath) {
    final List<String> recoveryLog = new CopyOnWriteArrayList<>();
    final Runnable stopCapture = captureRecoveryLog(recoveryLog);
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      assertThat(database.countType(AFTER_TYPE_NAME, false))
          .as("record committed (WAL) after the statement; recovery log: %s", recoveryLog).isEqualTo(1L);
    } finally {
      stopCapture.run();
    }
  }

  private void runChild(final String mode, final Path databasePath, final int expectedExit) throws Exception {
    final CrashingChildJvm.Result result = CrashingChildJvm.run(getClass(), 300, mode, databasePath.toString());
    assertThat(result.exitCode()).as(result.output()).isEqualTo(expectedExit);
  }

  private static void assertIndexHoldsEveryVector(final Path databasePath) {
    assertIndexHoldsEveryVector(databasePath, VECTORS);
  }

  private static void assertIndexHoldsEveryVector(final Path databasePath, final int expectedVectors) {
    // what WAL recovery says while it replays: a gap it cannot bridge is part of the evidence
    final List<String> recoveryLog = new CopyOnWriteArrayList<>();
    final Runnable stopCapture = captureRecoveryLog(recoveryLog);
    try (final DatabaseFactory factory = new DatabaseFactory(databasePath.toString()); final Database database = factory.open()) {
      assertThat(database.countType(TYPE_NAME, false)).as("records after WAL recovery").isEqualTo(expectedVectors);

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
      assertThat(index).isNotNull();
      long entries = 0;
      LSMVectorIndex vectorIndex = null;
      final StringBuilder states = new StringBuilder();
      for (final IndexInternal sub : index.getIndexesOnBuckets()) {
        final LSMVectorIndex vector = (LSMVectorIndex) sub;
        vectorIndex = vector;
        states.append(vector.getName()).append(" fileIds=").append(vector.getFileIds()).append(" valid=").append(vector.isValid())
            .append(" pages=").append(vector.getTotalPages()).append("; ");
        if (vector.isValid())
          entries += vector.countEntries();
      }
      assertThat(entries).as("vector entries after the reopen (%s); recovery log: %s", states, recoveryLog)
          .isEqualTo(expectedVectors);

      // the index must still answer searches after the reopen: a graph read from pages the kill took away would find next
      // to none. The search is approximate (random 64-dimension vectors measured 18-20 of 20 here), so the bound tells a
      // broken graph from a working one, it is not a recall target
      final Iterator<Record> records = database.iterateType(TYPE_NAME, false);
      int found = 0;
      final int probes = 40;
      for (int probed = 0; probed < probes && records.hasNext(); probed++) {
        final Document doc = records.next().asDocument();
        for (final Pair<RID, Float> neighbour : vectorIndex.findNeighborsFromVector((float[]) doc.get("embedding"), 10))
          if (neighbour.getFirst().equals(doc.getIdentity())) {
            ++found;
            break;
          }
      }
      assertThat(found).as("records found among their own 10 nearest neighbours").isGreaterThanOrEqualTo(probes * 3 / 4);
    } finally {
      stopCapture.run();
    }
  }

  private static Runnable captureRecoveryLog(final List<String> recoveryLog) {
    // initializes the logging configuration first, which would otherwise reset the handler installed below
    LogManager.instance().log(LSMVectorIndexBuildSurvivesCrashTest.class, Level.FINE, "Reopening");
    final Logger txLogger = Logger.getLogger(TransactionManager.class.getName());
    final Level previousLevel = txLogger.getLevel();
    final Handler capture = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue())
          recoveryLog.add(record.getMessage());
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    txLogger.setLevel(Level.WARNING);
    txLogger.addHandler(capture);
    return () -> {
      txLogger.removeHandler(capture);
      txLogger.setLevel(previousLevel);
    };
  }

  public static void main(final String[] args) throws Exception {
    if (args.length != 2)
      throw new IllegalArgumentException("Expected mode and database path");
    final String mode = args[0];
    final String path = args[1];

    GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(1L);

    if ("prepare".equals(mode) || "prepare-indexed".equals(mode)) {
      try (final DatabaseFactory factory = new DatabaseFactory(path); final Database database = factory.create()) {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("id", Type.INTEGER);
        type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.getSchema().buildDocumentType().withName(AFTER_TYPE_NAME).withTotalBuckets(1).create();
        final Random rnd = new Random(42);
        database.transaction(() -> {
          for (int i = 0; i < VECTORS; i++) {
            final float[] v = new float[DIMENSIONS];
            for (int d = 0; d < DIMENSIONS; d++)
              v[d] = (float) rnd.nextGaussian();
            database.newDocument(TYPE_NAME).set("id", i).set("embedding", v).save();
          }
        });
        if ("prepare-indexed".equals(mode))
          createIndex(database);
      }
      return;
    }

    if (!mode.startsWith("create-halt") && !mode.startsWith("rebuild-halt") && !mode.startsWith("plain-halt"))
      throw new IllegalArgumentException("Unknown mode " + mode);
    final boolean holdFlush = mode.contains("-noflush");
    final boolean writeAfter = mode.endsWith("-write");
    final boolean indexedWriteAfter = mode.endsWith("-indexed-write");
    final boolean rebuild = mode.startsWith("rebuild");
    final boolean noStatement = mode.startsWith("plain");

    final DatabaseFactory factory = new DatabaseFactory(path);
    final Database database = factory.open();
    if (holdFlush)
      ((DatabaseInternal) database).getPageManager().suspendFlushAndExecute(database, () -> {
        if (!noStatement)
          runStatement(database, rebuild);
        if (indexedWriteAfter)
          database.transaction(() -> {
            final float[] v = new float[DIMENSIONS];
            Arrays.fill(v, 7.5f);
            database.newDocument(TYPE_NAME).set("id", VECTORS).set("embedding", v).save();
          });
        else if (writeAfter)
          database.transaction(() -> database.newDocument(AFTER_TYPE_NAME).set("committed", true).save());
        Runtime.getRuntime().halt(CrashingChildJvm.CRASH_EXIT_CODE);
      });
    else
      runStatement(database, rebuild);
    Runtime.getRuntime().halt(CrashingChildJvm.CRASH_EXIT_CODE);
  }

  private static void runStatement(final Database database, final boolean rebuild) {
    if (rebuild)
      database.command("sql", "REBUILD INDEX `" + IDX_NAME + "`");
    else
      createIndex(database);
  }

  private static void createIndex(final Database database) {
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\" }");
  }
}
