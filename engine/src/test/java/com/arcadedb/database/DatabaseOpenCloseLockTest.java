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

package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.Profiler;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndexMutable;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the database engine: DatabaseOpenCloseLock. Each nested class keeps the regression of one reported issue (see the
 * Javadoc of each).
 */
public class DatabaseOpenCloseLockTest {
  /**
   * Regression test for issue #10507: closing a database that was opened {@code READ_ONLY} must not write to its directory.
   * <p>
   * The close of a read-write handle persists {@code statistics.json} (the cached record counts) and {@code last-tx-id.bin}
   * (the recency marker of the HA bootstrap) from the handle's own memory. A {@code READ_ONLY} handle holds the state of its
   * open, and the engine lets a read-write process of the same directory commit while it is open, so writing that state back
   * at close rewound the writer's counters: {@code count(*)} under-reported and {@code CHECK DATABASE} reported an error.
   * <p>
   * The test plays the writer: while the {@code READ_ONLY} handle is open it replaces both files with content the handle never
   * loaded, with an old modification time, and expects the close to leave them byte for byte and timestamp for timestamp as
   * they were.
   */
  @Nested
  class ReadOnlyClose extends TestHelper {
    private static final String TYPE = "Issue10507Type";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE);
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.newDocument(TYPE).set("id", i).save();
      });
    }

    @Test
    void closingAReadOnlyHandleLeavesTheStatisticsAndTheLastTransactionIdAlone() throws IOException {
      database.close();

      final File statistics = new File(getDatabasePath(), "statistics.json");
      final File lastTxId = new File(getDatabasePath(), "last-tx-id.bin");
      assertThat(statistics).as("the read-write close writes the statistics").exists();
      assertThat(lastTxId).as("the read-write close writes the last transaction id").exists();

      final Database readOnly = factory.open(ComponentFile.MODE.READ_ONLY);
      try {
        assertThat(readOnly.countType(TYPE, true)).isEqualTo(100);

        // WHAT A READ-WRITE PROCESS THAT COMMITTED SINCE THE OPEN LEAVES BEHIND: THE HANDLE NEVER LOADED THIS CONTENT
        final String writerStatistics = new String(Files.readAllBytes(statistics.toPath()), StandardCharsets.UTF_8) + "\n";
        Files.write(statistics.toPath(), writerStatistics.getBytes(StandardCharsets.UTF_8));
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (final DataOutputStream out = new DataOutputStream(bytes)) {
          out.writeLong(123_456_789L);
        }
        Files.write(lastTxId.toPath(), bytes.toByteArray());

        // AN OLD TIMESTAMP, SO THAT ANY REWRITE MOVES IT WHATEVER THE RESOLUTION OF THE FILE SYSTEM
        final long oldTimestamp = System.currentTimeMillis() - 3_600_000L;
        assertThat(statistics.setLastModified(oldTimestamp)).isTrue();
        assertThat(lastTxId.setLastModified(oldTimestamp)).isTrue();

        final String statisticsBefore = new String(Files.readAllBytes(statistics.toPath()), StandardCharsets.UTF_8);
        final byte[] lastTxIdBefore = Files.readAllBytes(lastTxId.toPath());
        final long statisticsTimeBefore = Files.getLastModifiedTime(statistics.toPath()).toMillis();
        final long lastTxIdTimeBefore = Files.getLastModifiedTime(lastTxId.toPath()).toMillis();

        readOnly.close();

        assertThat(new String(Files.readAllBytes(statistics.toPath()), StandardCharsets.UTF_8)).as("statistics.json content")
            .isEqualTo(statisticsBefore);
        assertThat(Files.readAllBytes(lastTxId.toPath())).as("last-tx-id.bin content").isEqualTo(lastTxIdBefore);
        assertThat(Files.getLastModifiedTime(statistics.toPath()).toMillis()).as("statistics.json modification time")
            .isEqualTo(statisticsTimeBefore);
        assertThat(Files.getLastModifiedTime(lastTxId.toPath()).toMillis()).as("last-tx-id.bin modification time")
            .isEqualTo(lastTxIdTimeBefore);
      } finally {
        if (readOnly.isOpen())
          readOnly.close();
      }
    }

    @Test
    void closingAReadOnlyHandleLeavesAWriterWalFileAlone() throws IOException {
      database.close();

      // A WAL file nobody holds open, as a writer that crashed leaves behind: only a read-write open may replay or delete it
      final File wal = new File(getDatabasePath(), "txlog_0.wal");
      Files.write(wal.toPath(), new byte[0]);
      assertThat(wal).exists();

      final Database readOnly = factory.open(ComponentFile.MODE.READ_ONLY);
      try {
        assertThat(readOnly.countType(TYPE, true)).isEqualTo(100);
        readOnly.close();
        assertThat(wal).as("the WAL file of a writer").exists();
      } finally {
        if (readOnly.isOpen())
          readOnly.close();
      }
    }
  }

  /**
   * #10507, the general form of the rule: a database opened {@code READ_ONLY} does not own its directory (a read-write process may
   * have it open, and the engine lets it), so neither the open nor the close writes, rewrites or deletes any file in it.
   * <p>
   * {@link ReadOnlyClose} pins the files the report named. This one takes the whole directory: it is listed before and
   * after the read-only handle's life, with the modification time of every file moved to the past first, so a rewrite of the same
   * bytes is seen too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReadOnlyOpenCloseWritesNothing {
    private static final String DB_PATH    = "target/test-databases/ReadOnlyOpenCloseWritesNothing";
    private static final int    DIMENSIONS = 8;
    private static final int    VECTORS    = 50;
    /** The size from which a vector index defers its graph build on close (LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE). */
    private static final int    DEFERRED_GRAPH_VECTORS = 1000;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void aPlainDatabaseIsLeftAsItWas() throws IOException {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            db.getSchema().createDocumentType("Doc").createProperty("id", Type.INTEGER);
            db.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "id");
            for (int i = 0; i < 100; i++)
              db.newDocument("Doc").set("id", i).save();
          });
        } finally {
          db.close();
        }
      }

      assertThatReadOnlyLifeWritesNothing("Doc", 100);
    }

    @Test
    void aSchemaRecoveredFromThePreviousGenerationStaysInMemoryOnly() throws IOException {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          // ONE SCHEMA FILE WRITE PER CHANGE, SO THE PREVIOUS GENERATION ALREADY HOLDS THE TYPE
          db.getSchema().createDocumentType("Doc");
          db.getSchema().getType("Doc").createProperty("id", Type.INTEGER);
          db.getSchema().getType("Doc").createProperty("name", Type.STRING);
          db.transaction(() -> {
            for (int i = 0; i < 10; i++)
              db.newDocument("Doc").set("id", i).save();
          });
        } finally {
          db.close();
        }
      }

      // THE CURRENT SCHEMA FILE IS TORN, THE PREVIOUS GENERATION IS FINE: A READ-WRITE OPEN HEALS THE FILE, A READ-ONLY ONE HAS NO
      // RIGHT TO REPLACE A FILE A WRITER MAY BE REPLACING TOO
      final File schemaFile = new File(DB_PATH, "schema.json");
      assertThat(new File(DB_PATH, "schema.prev.json")).as("the previous generation the recovery reads").exists();
      final String torn = "{ \"torn\": ";
      Files.writeString(schemaFile.toPath(), torn);

      assertThatReadOnlyLifeWritesNothing("Doc", 10);
      assertThat(Files.readString(schemaFile.toPath())).isEqualTo(torn);
    }

    @Test
    void aVectorIndexWhoseGraphTheWritersCloseDeferredIsLeftAsItWas() throws IOException {
      // AT OR ABOVE THIS SIZE THE CLOSE OF A WRITER DOES NOT BUILD THE GRAPH: IT RECORDS THE DEFERRAL IN THE MANIFEST AND LEAVES THE
      // GRAPH FILE EMPTY. A READ-ONLY CLOSE FINDS THE SAME STATE, AND MUST NOT RECORD ANYTHING
      createVectorDatabase(DEFERRED_GRAPH_VECTORS);

      assertThatReadOnlyLifeWritesNothing("Doc", DEFERRED_GRAPH_VECTORS);
    }

    @Test
    void aVectorIndexBelowTheDeferralSizeWhoseGraphIsEmptyIsLeftAsItWas() throws IOException {
      // BELOW THAT SIZE THE CLOSE OF A WRITER BUILDS AND PERSISTS THE GRAPH, WHICH A READ-ONLY CLOSE MUST NOT DO, NOT EVEN FOR A COPY
      // OF THE DIRECTORY MADE BEFORE THE WRITER CLOSED (THE GRAPH FILE IS THERE, WITHOUT A GRAPH IN IT)
      createVectorDatabase(VECTORS);
      final File[] graphFiles = new File(DB_PATH).listFiles((dir, name) -> name.endsWith(".vecgraph"));
      assertThat(graphFiles).as("the writer's close persisted the graph, so the fixture can empty it").isNotEmpty();
      for (final File graphFile : graphFiles)
        try (final FileChannel channel = FileChannel.open(graphFile.toPath(), StandardOpenOption.WRITE)) {
          channel.truncate(0);
        }
      final File[] manifests = new File(DB_PATH).listFiles((dir, name) -> name.endsWith(".vecgraphfp"));
      for (final File manifest : manifests)
        assertThat(manifest.delete()).isTrue();

      assertThatReadOnlyLifeWritesNothing("Doc", VECTORS);
    }

    private static void createVectorDatabase(final int vectors) {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            db.getSchema().createDocumentType("Doc").createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\" }");
          });
          db.transaction(() -> {
            final Random random = new Random(10507);
            for (int i = 0; i < vectors; i++) {
              final float[] vector = new float[DIMENSIONS];
              for (int d = 0; d < DIMENSIONS; d++)
                vector[d] = random.nextFloat();
              db.newDocument("Doc").set("vector", vector).save();
            }
          });
        } finally {
          db.close();
        }
      }
    }

    private static void assertThatReadOnlyLifeWritesNothing(final String type, final long expectedCount) throws IOException {
      final long oldTimestamp = System.currentTimeMillis() - 3_600_000L;
      for (final File file : new File(DB_PATH).listFiles())
        if (file.isFile())
          assertThat(file.setLastModified(oldTimestamp)).isTrue();

      final Map<String, String> before = snapshot();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open(ComponentFile.MODE.READ_ONLY);
        try {
          assertThat(db.countType(type, true)).isEqualTo(expectedCount);
        } finally {
          db.close();
        }
      }

      assertThat(snapshot()).as("the files of the directory after a READ_ONLY open and close").isEqualTo(before);
    }

    /** File name to size, modification time and hash of the content, for every file of the directory. */
    private static Map<String, String> snapshot() throws IOException {
      final Map<String, String> files = new TreeMap<>();
      for (final File file : new File(DB_PATH).listFiles())
        if (file.isFile())
          files.put(file.getName(), file.length() + "/" + file.lastModified() + "/" + Arrays.hashCode(Files.readAllBytes(file.toPath())));
      return files;
    }
  }

  /**
   * Regression test for issue #4511: {@code LocalDatabase.openInternal} leaks the JVM {@link java.nio.channels.FileLock}
   * (and the underlying lock-file I/O channels) when recovery throws while reopening a database that was not closed
   * cleanly. Once the lock leaks, the database is permanently unopenable within the same JVM (and on Windows the lock
   * file cannot be removed by external tools either).
   *
   * <p>The scenario is reproduced by killing the database to leave the {@code database.lck} marker on disk (so the next
   * open enters the recovery path and acquires the file lock) and then forcing recovery to throw via a one-shot
   * {@code DB_NOT_CLOSED} callback. After that failed open, a subsequent open must succeed: that only happens if the
   * failed open released its file lock.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OpenFailureLockLeak extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Issue4511Type");
    }

    @Test
    void failedRecoveryReleasesFileLockSoDatabaseCanReopen() {
      // Produce some content and crash without a clean close so the next open runs recovery on a present lock file.
      database.transaction(() -> database.newDocument("Issue4511Type").set("k", 1).save());
      ((DatabaseInternal) database).kill();
      // Deregister the killed instance from the active-instances registry (the database.lck marker stays on disk).
      database.close();

      // Force the first recovery attempt to throw AFTER the file lock has been acquired.
      final AtomicBoolean failNextRecovery = new AtomicBoolean(true);
      factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, () -> {
        if (failNextRecovery.compareAndSet(true, false))
          throw new RuntimeException("Simulated recovery failure (issue #4511)");
        return null;
      });

      // The first open must fail because recovery threw.
      assertThatThrownBy(() -> factory.open()).isInstanceOf(Exception.class);

      // If the failed open leaked the file lock, this second open fails with a LockException
      // ("locked by another process"). With the fix the lock was released, so the open succeeds.
      database = factory.open();
      assertThat(database.isOpen()).isTrue();
      assertThat(database.getSchema().existsType("Issue4511Type")).isTrue();
    }
  }

  /**
   * Regression test for issue #4547: a {@code READ_ONLY} open of a not-cleanly-closed database (its
   * {@code database.lck} marker present, so recovery is required) must be rejected and must not leak
   * the OS file lock, so a subsequent recovery-capable {@code READ_WRITE} open still succeeds.
   */
  @Nested
  class ReadOnlyRecoveryLockLeak extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Issue4547Type");
    }

    @Test
    void readOnlyOpenOfRecoveringDatabaseIsRejectedAndDoesNotLeakLock() {
      // Produce some content and crash without a clean close so the next open runs recovery on a present lock file.
      database.transaction(() -> database.newDocument("Issue4547Type").set("k", 1).save());
      ((DatabaseInternal) database).kill();
      // Deregister the killed instance from the active-instances registry (the database.lck marker stays on disk).
      database.close();

      final File lockFile = new File(getDatabasePath() + "/database.lck");
      assertThat(lockFile.exists()).as("lock marker must be present so recovery is required").isTrue();

      // A READ_ONLY open of a database that needs recovery must be rejected. The deterministic rejection is a
      // DatabaseMetadataException raised by checkForRecovery() before any write lock is taken. In a heavily
      // loaded shared JVM an unrelated transient failure earlier in openInternal() can instead surface first as
      // a DatabaseOperationException ("Error on creating new database instance"). Both outcomes reject the open
      // and release the I/O resources acquired so far, which is exactly what issue #4547 guards - proven by the
      // recovery-capable READ_WRITE open below succeeding rather than failing on a leaked lock.
      assertThatThrownBy(() -> factory.open(ComponentFile.MODE.READ_ONLY))
          .isInstanceOfAny(DatabaseMetadataException.class, DatabaseOperationException.class);

      // If the rejected READ_ONLY open leaked the file lock, this READ_WRITE open fails with a
      // LockException ("locked by another process"). With the fix the lock was never leaked, so the
      // recovery-capable READ_WRITE open succeeds.
      database = factory.open();
      assertThat(database.isOpen()).isTrue();
      assertThat(database.getSchema().existsType("Issue4547Type")).isTrue();
    }
  }

  /**
   * Issue #11057, the embedded half (PR #11124 review): {@code BasicDatabase.rollback()} promises a no-op when no transaction is
   * active, but on a database opened {@code READ_ONLY} both {@code rollback()} and {@code commit()} asserted a WRITE intent before
   * looking for a transaction. An idle rollback threw {@code DatabaseIsReadOnlyException}, and the read transaction
   * {@code begin()} does open there could be neither committed nor rolled back, so it stayed open on the thread.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReadOnlyTransactionEnd extends TestHelper {
    private static final String TYPE = "Issue11057Type";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE);
      database.transaction(() -> database.newDocument(TYPE).set("id", 1).save());
    }

    @Test
    void aReadOnlyDatabaseEndsItsReadTransactionsAndAnIdleRollbackIsANoOp() {
      database.close();

      final Database readOnly = factory.open(ComponentFile.MODE.READ_ONLY);
      try {
        readOnly.rollback();
        assertThat(readOnly.isTransactionActive()).isFalse();

        readOnly.begin();
        assertThat(readOnly.countType(TYPE, true)).isEqualTo(1);
        readOnly.rollback();
        assertThat(readOnly.isTransactionActive()).as("the read transaction is rolled back").isFalse();
        readOnly.rollback();

        readOnly.begin();
        assertThat(readOnly.countType(TYPE, true)).isEqualTo(1);
        readOnly.commit();
        assertThat(readOnly.isTransactionActive()).as("the read transaction is committed").isFalse();

        // THE COUNTERWEIGHTS: A WRITE IS STILL REFUSED WHERE IT IS MADE, AND A COMMIT WITHOUT A TRANSACTION STILL THROWS
        readOnly.begin();
        assertThatThrownBy(() -> readOnly.newDocument(TYPE).set("id", 2).save()).isInstanceOf(DatabaseIsReadOnlyException.class);
        readOnly.rollback();
        assertThat(readOnly.isTransactionActive()).isFalse();
        assertThatThrownBy(readOnly::commit).isInstanceOf(TransactionException.class);

        assertThat(readOnly.countType(TYPE, true)).isEqualTo(1);
      } finally {
        if (readOnly.isOpen())
          readOnly.close();
      }
    }
  }

  /**
   * Issue #11068: a close FOR REOPEN (a server closing an idle database, an HA reinstall) keeps the instance open while it drains
   * its asynchronous work, before the write-locked teardown that rolls back every transaction still attached to it. A handle
   * resolved before the close could begin a transaction in that window and lose it to the teardown. Such a {@code begin()} is now
   * refused with the retryable {@link DatabaseIsReopeningException}, so the caller retries on the reopened database.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BeginDuringCloseForReopen {

    @TempDir
    private Path          tempDir;
    private LocalDatabase database;

    @AfterEach
    void tearDown() {
      LocalDatabase.TEST_CLOSE_HOOK = null;
      if (database != null && database.isOpen())
        database.close();
    }

    @Test
    void aTransactionBegunWhileTheInstanceClosesForReopenIsRefusedAsRetryable() throws Exception {
      database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("reopening").toString()).create();
      database.transaction(() -> database.getSchema().createDocumentType("Doc"));

      final CountDownLatch insideClose = new CountDownLatch(1);
      final CountDownLatch releaseClose = new CountDownLatch(1);
      LocalDatabase.TEST_CLOSE_HOOK = () -> {
        insideClose.countDown();
        try {
          releaseClose.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };

      final Thread closer = new Thread(database::closeForReopen, "closer-for-reopen");
      closer.start();
      try {
        assertThat(insideClose.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(database.isOpen()).as("still open: the teardown has not run yet").isTrue();

        final AtomicReference<Throwable> refused = new AtomicReference<>();
        CompletableFuture.runAsync(() -> {
          try {
            database.begin();
            database.rollback();
          } catch (final Throwable e) {
            refused.set(e);
          }
        }).get(30, TimeUnit.SECONDS);

        assertThat(refused.get()).isInstanceOf(DatabaseIsReopeningException.class);
      } finally {
        releaseClose.countDown();
        closer.join(30_000);
      }
      assertThat(database.isOpen()).isFalse();
    }
  }

  /**
   * Issue #11240: an HA leader keeps the file a compaction replaced until the compaction is published, so for a while the old
   * file is still on disk although the schema already maps it to its replacement. A transaction that resolved the old id
   * before the swap must lock the replacement, not the old file: the index entries it writes at commit land in the
   * replacement, whose lock it would otherwise never hold. The lock acquisition used to re-resolve a migrated id only once the
   * old file was gone.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MigratedFileStillOnDiskLock extends TestHelper {

    @Test
    void aLockOnAMigratedFileStillOnDiskLocksItsReplacement() throws InterruptedException {
      database.getSchema().createDocumentType("Retired", 1);
      database.getSchema().createDocumentType("Replacement", 1);
      final int retired = database.getSchema().getType("Retired").getBuckets(false).getFirst().getFileId();
      final int replacement = database.getSchema().getType("Replacement").getBuckets(false).getFirst().getFileId();

      // what a deferred compaction leaves for a while: both files on disk, the old one mapped to the new one
      ((LocalSchema) database.getSchema()).setMigratedFileId(retired, replacement, false);
      assertThat(((DatabaseInternal) database).getFileManager().existsFile(retired)).isTrue();

      database.begin();
      try {
        database.acquireLock().bucket(database.getSchema().getType("Retired").getBuckets(false).getFirst().getName()).lock();

        assertThat(lockableByAnotherThread(replacement)).as("the replacement is locked").isFalse();
        assertThat(lockableByAnotherThread(retired)).as("the retired file is not held").isTrue();
      } finally {
        database.rollback();
      }
    }

    private boolean lockableByAnotherThread(final int fileId) throws InterruptedException {
      final AtomicReference<LockManager.LOCK_STATUS> status = new AtomicReference<>();
      final Thread other = new Thread(() -> {
        final LockManager.LOCK_STATUS locked = ((DatabaseInternal) database).getTransactionManager()
            .tryLockFile(fileId, 1, Thread.currentThread());
        status.set(locked);
        if (locked == LockManager.LOCK_STATUS.YES)
          ((DatabaseInternal) database).getTransactionManager().unlockFile(fileId, Thread.currentThread());
      });
      other.start();
      other.join();
      return status.get() == LockManager.LOCK_STATUS.YES;
    }
  }

  /**
   * Issue #10156: the background warm-up of the indexes a schema reload re-created (#10021) was stopped by the schema close, the
   * tenth step of the write-locked teardown - after the page flush, the fsync, the "database is closed" mark and the purge of this
   * database's pages from the shared read cache. A warm-up read landing in between re-inserted a page keyed by the closed instance
   * into that cache, and on a drop it read files about to be deleted. #5418's rule is that index background work stops in
   * {@code closeDurableParts}, before any of that; the warm-up stops there now.
   * <p>
   * The invariant asserted is the rule's, not the thread's: at the moment the instance reports itself closed, no warm-up of this
   * database is alive or waiting.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Tag("vector")
  @Nested
  class WarmUpStoppedBeforeClose {
    private static final String DB_PATH    = "target/test-databases/WarmUpStoppedBeforeClose";
    private static final int    DIMENSIONS = 8;
    private static final int    VECTORS    = 200;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\" }");
          });
          db.transaction(() -> {
            final Random random = new Random(10156);
            for (int i = 0; i < VECTORS; i++) {
              final float[] vector = new float[DIMENSIONS];
              for (int d = 0; d < DIMENSIONS; d++)
                vector[d] = random.nextFloat();
              db.newDocument("Doc").set("vector", vector).save();
            }
          });
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          db.close();
        }
      }
    }

    @AfterEach
    void tearDown() {
      LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = null;
      GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void noWarmUpIsAliveOnceTheDatabaseReportsItselfClosed() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        final List<String> seenAtMarkedClosed = armWarmUpAndWatchTheClose(db);

        db.close();

        assertThat(seenAtMarkedClosed).as("warm-up threads alive when the database already reported itself closed").isEmpty();
      }
    }

    @Test
    void noWarmUpIsAliveOnceADroppedDatabaseReportsItselfClosed() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        final List<String> seenAtMarkedClosed = armWarmUpAndWatchTheClose(db);

        // the files the warm-up would read are deleted right after this close
        db.drop();

        assertThat(seenAtMarkedClosed).as("warm-up threads alive when the dropped database already reported itself closed").isEmpty();
      }
    }

    /**
     * Leaves a warm-up of the reloaded vector index waiting on a delay that does not elapse in the test - its thread alive - and
     * arms the hook that runs right after the close marks the instance closed.
     */
    private static List<String> armWarmUpAndWatchTheClose(final Database db) throws Exception {
      vectorIndex(db).warmUp();
      assertThat(vectorIndex(db).isWarm()).isTrue();

      GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(600_000);
      final LocalSchema schema = ((DatabaseInternal) db).getSchema().getEmbedded();
      schema.load(ComponentFile.MODE.READ_WRITE, true);

      final String threadName = "ArcadeDB-IndexWarmUp-" + db.getName();
      assertThat(warmUpThreads(threadName)).as("the reload queued a warm-up, so the fixture is not vacuous").isNotEmpty();
      assertThat(schema.awaitIndexWarmUp(0)).as("the warm-up is waiting").isFalse();

      final List<String> seen = new ArrayList<>();
      LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = () -> seen.addAll(warmUpThreads(threadName));
      return seen;
    }

    private static List<String> warmUpThreads(final String threadName) {
      final List<String> alive = new ArrayList<>();
      for (final Thread thread : Thread.getAllStackTraces().keySet())
        if (thread.isAlive() && thread.getName().equals(threadName))
          alive.add(thread.getName() + " (" + thread.getState() + ")");
      return alive;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression test for issue #5067 (item 1): {@code TransactionContext.kill()} released the explicit lock
   * set but nulled {@code lockedFiles} WITHOUT unlocking, so killing a transaction caught in
   * COMMIT_1ST_PHASE (its file locks acquired by {@code commit1stPhase}) leaked the {@code LockManager}
   * entries until {@code TransactionManager.kill()} closed the whole lock manager. Test-only API, but the
   * release must be symmetric with {@code reset()}.
   */
  @Nested
  class KillLockSymmetry extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("Issue5067Type");
    }

    @Test
    void killAfterCommit1stPhaseReleasesFileLocks() {
      database.begin();
      database.newDocument("Issue5067Type").set("k", 1).save();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

      // Bring the transaction into COMMIT_1ST_PHASE: this is where lockFilesInOrder populates lockedFiles.
      assertThat(tx.commit1stPhase(true)).isNotNull();

      tx.kill();
      // Leave the killed context inactive so the teardown close does not try to roll back its nulled state.
      tx.setStatus(TransactionContext.STATUS.INACTIVE);

      // The file locks acquired by commit1stPhase must have been released by kill(): a foreign requester
      // must be able to lock the same bucket file within the (short) timeout instead of timing out on a
      // lock still keyed to the killed transaction's requester.
      final int bucketFileId = database.getSchema().getType("Issue5067Type").getBuckets(false).getFirst().getFileId();
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      final Object foreignRequester = new Object();

      final LockManager.LOCK_STATUS status = transactionManager.tryLockFile(bucketFileId, 1_000L, foreignRequester);
      assertThat(status).as("kill() must release the COMMIT_1ST_PHASE file locks (symmetry with reset())")
          .isEqualTo(LockManager.LOCK_STATUS.YES);

      transactionManager.unlockFile(bucketFileId, foreignRequester);
    }
  }

  /**
   * Issue #7588, the audit pass that followed #7586: {@code LocalDatabase}'s constructor derived the database NAME
   * from the caller-supplied path with {@code path.lastIndexOf(File.separator)} - this JVM's own separator and no
   * other.
   * <p>
   * The path is caller-supplied through the embedded API, a configuration file or an environment variable, and is
   * routinely written with {@code '/'} even on Windows, where {@code File.separator} is {@code '\'}. A Windows user
   * passing {@code "C:/data/mydb"} therefore got the WHOLE PATH as the database name instead of {@code mydb} - the
   * name that then appears in every log line, every error message, the HTTP routes and the security ACL keys. A
   * trailing separator written the other way round was not stripped either, which made the name empty.
   * <p>
   * The tests mix both conventions in one path, as the real Windows defect did, so the gap is exercised on whichever
   * platform the suite runs on rather than only on Windows.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DatabaseNameFromMixedSeparatorPath {

    /**
     * The defect end to end: the database is really created, and it knows its own name.
     * <p>
     * Only the LAST separator is written the other way round, so the directory this creates is the same directory on
     * either platform and the test does not depend on how the OS reads the rest of the path.
     */
    @Test
    void aDatabaseNamesItselfAfterTheLastPathSegmentWhicheverSeparatorPrecedesIt() {
      final String path = "target/databases/issue7588" + otherSeparator() + "mydb";

      FileUtils.deleteRecursively(new File(path));
      final DatabaseFactory factory = new DatabaseFactory(path);
      try (final Database database = factory.create()) {
        assertThat(database.getName())
            .as("the last segment, not the whole path")
            .isEqualTo("mydb");
        assertThat(database.getDatabasePath()).isEqualTo(path);
      } finally {
        final DatabaseFactory reopen = new DatabaseFactory(path);
        if (reopen.exists())
          reopen.open().drop();
        FileUtils.deleteRecursively(new File(path));
      }
    }

    /**
     * A trailing separator written in the OTHER convention is stripped, so the path the name is read off does not end
     * in one. With the {@code File.separator}-only check the separator stayed, and the name came out EMPTY.
     * <p>
     * Asserted on the two helpers rather than by creating a database: a trailing {@code '\'} on a POSIX file system is
     * part of the directory NAME, so a database created at such a path and one created at the stripped path are two
     * different directories, and only Windows can tell the end-to-end story.
     */
    @Test
    void aTrailingSeparatorOfEitherConventionIsStrippedBeforeTheNameIsRead() {
      assertThat(FileUtils.getFileNameFromPath(FileUtils.stripTrailingSeparator("/data/mydb/")))
          .isEqualTo("mydb");
      assertThat(FileUtils.getFileNameFromPath(FileUtils.stripTrailingSeparator("C:\\data\\mydb\\")))
          .isEqualTo("mydb");
      assertThat(FileUtils.getFileNameFromPath(FileUtils.stripTrailingSeparator("C:/data/mydb")))
          .isEqualTo("mydb");
    }

    /**
     * A path with no separator at all is the name: an embedded caller that opens {@code "mydb"} relative to the
     * working directory has always got {@code mydb}, and still does.
     */
    @Test
    void aPathWithNoSeparatorIsTheNameItself() {
      final String path = "target-issue7588-bare";

      FileUtils.deleteRecursively(new File(path));
      final DatabaseFactory factory = new DatabaseFactory(path);
      try (final Database database = factory.create()) {
        assertThat(database.getName()).isEqualTo(path);
      } finally {
        final DatabaseFactory reopen = new DatabaseFactory(path);
        if (reopen.exists())
          reopen.open().drop();
        FileUtils.deleteRecursively(new File(path));
      }
    }

    /** The separator convention this JVM does NOT use, which is the one the lookup used to miss. */
    private static String otherSeparator() {
      return "/".equals(File.separator) ? "\\" : "/";
    }
  }

  /**
   * Issue #8316: a {@code close()} that marked the instance closed and then threw skipped the step that removes it from
   * {@link DatabaseFactory}'s active-instance registry. The closed instance stayed registered: the next open of the path
   * was refused as "already in use", and the server's lookup reused the dead instance instead of opening the files.
   * Every step after the instance is marked closed now runs whatever the one before it did - the files and the lock are
   * released - and the unregistration runs in a finally, keyed on the instance being closed. A query engine that fails to
   * close no longer aborts the rest of the teardown either.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FailedCloseUnregisters {
    private static final String DB_PATH = "./target/databases/test-issue-8316-failed-close";

    @AfterEach
    void cleanup() {
      LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = null;
      final Database leftover = DatabaseFactory.getActiveDatabaseInstance(DB_PATH);
      if (leftover != null && leftover.isOpen())
        leftover.drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void aCloseThatThrowsAfterMarkingTheInstanceClosedStillUnregistersIt() {
      final LocalDatabase database = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
      database.getSchema().createDocumentType("Doc");
      database.transaction(() -> database.newDocument("Doc").set("n", 1).save());
      assertThat(DatabaseFactory.getActiveDatabaseInstance(DB_PATH)).isSameAs(database);

      LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = () -> {
        throw new IllegalStateException("simulated failure after the instance was marked closed");
      };

      assertThatThrownBy(database::close).hasStackTraceContaining("simulated failure after the instance was marked closed");
      LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = null;

      assertThat(database.isOpen()).isFalse();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(DB_PATH))
          .as("a closed instance left in the registry is what the server's lookup reused in place of the files on disk")
          .isNull();
      assertThatCode(() -> Profiler.INSTANCE.toJSON())
          .as("nor may it stay in the JVM-wide profiler, whose every read would then fail on it")
          .doesNotThrowAnyException();
      // The rest of the teardown still ran: the files and the lock were released, so the path opens again, with its data.
      final Database reopened = new DatabaseFactory(DB_PATH).open();
      try {
        assertThat(reopened.countType("Doc", true)).isEqualTo(1L);
      } finally {
        reopened.drop();
      }
    }

    @Test
    void aQueryEngineThatFailsToCloseDoesNotAbortTheRestOfTheTeardown() {
      final LocalDatabase database = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
      database.getSchema().createDocumentType("Doc");
      database.transaction(() -> database.newDocument("Doc").set("n", 1).save());

      final AtomicBoolean closeAttempted = new AtomicBoolean();
      database.registerReusableQueryEngine((QueryEngine) Proxy.newProxyInstance(QueryEngine.class.getClassLoader(),
          new Class<?>[] { QueryEngine.class }, (proxy, method, args) -> switch (method.getName()) {
            case "getLanguage" -> "issue8316-failing";
            case "close" -> {
              closeAttempted.set(true);
              throw new IllegalStateException("simulated query engine close failure");
            }
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            case "toString" -> "issue8316-failing-engine";
            default -> null;
          }));

      database.close();

      assertThat(closeAttempted.get()).isTrue();
      assertThat(database.isOpen()).isFalse();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(DB_PATH)).isNull();

      // The files and the lock were released too: the same directory opens again, with its data.
      final Database reopened = new DatabaseFactory(DB_PATH).open();
      try {
        assertThat(reopened.countType("Doc", true)).isEqualTo(1L);
      } finally {
        reopened.drop();
      }
    }
  }

  /**
   * Regression test for issue #8356: two JVM shutdown hooks (DatabaseFactory's registry sweep and the
   * embedding ArcadeDBServer's own hook) can both call {@code close()} on the same {@link LocalDatabase}
   * instance at once. The {@code isOpen()} check each caller does beforehand is not atomic with the
   * {@code close()} call, so both can see the database open and both call in.
   * <p>
   * Before the fix, the only guard was the {@code open} boolean checked inside {@code closeDurableParts}'s
   * write-locked lambda - real teardown work (the async drain, the per-index flush and
   * {@code releaseBackgroundResources}, the graph-analytical-view shutdown) ran UNGUARDED ahead of it, so a
   * second caller silently repeated that work instead of a true no-op. This test drives the exact
   * interleaving the issue describes - a second {@code close()} call arriving while the first is confirmed
   * to already be in progress - and checks that the second call does not return, and does not observably run
   * any teardown of its own, until the first one has fully finished.
   */
  @Nested
  class ConcurrentCloseIsIdempotent {

    @TempDir
    private Path          tempDir;
    private LocalDatabase database;

    @AfterEach
    void tearDown() {
      LocalDatabase.TEST_CLOSE_HOOK = null;
      if (database != null && database.isOpen())
        database.close();
    }

    @Test
    void aSecondConcurrentCloseWaitsForTheFirstInsteadOfRepeatingTeardown() throws Exception {
      database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("mydb").toString()).create();
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc");
        database.newDocument("Doc").set("name", "first").save();
      });

      final CountDownLatch firstIsInsideClosing = new CountDownLatch(1);
      final CountDownLatch releaseFirst         = new CountDownLatch(1);
      final AtomicInteger  hookInvocations      = new AtomicInteger(0);

      LocalDatabase.TEST_CLOSE_HOOK = () -> {
        hookInvocations.incrementAndGet();
        firstIsInsideClosing.countDown();
        try {
          releaseFirst.await(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };

      final Thread firstCloser = new Thread(() -> database.close(), "first-closer");
      firstCloser.start();

      assertThat(firstIsInsideClosing.await(10, TimeUnit.SECONDS))
          .as("the first closer must reach the guarded section before the second one starts")
          .isTrue();

      final CountDownLatch secondReturned = new CountDownLatch(1);
      final Thread secondCloser = new Thread(() -> {
        database.close();
        secondReturned.countDown();
      }, "second-closer");
      secondCloser.start();

      // The second call must NOT return, and must NOT re-enter the guarded section (the hook fires once),
      // while the first is still parked inside it.
      assertThat(secondReturned.await(300, TimeUnit.MILLISECONDS))
          .as("a concurrent close() must wait for the in-progress one instead of returning early")
          .isFalse();
      assertThat(hookInvocations.get())
          .as("only the winning thread may run the guarded teardown section")
          .isEqualTo(1);

      releaseFirst.countDown();
      firstCloser.join(10_000);
      secondCloser.join(10_000);

      assertThat(secondReturned.await(0, TimeUnit.MILLISECONDS))
          .as("the second call must return once the first one completes")
          .isTrue();
      assertThat(hookInvocations.get())
          .as("the second call must not have run its own teardown pass")
          .isEqualTo(1);
      assertThat(database.isOpen()).isFalse();
    }
  }

  /**
   * Issue #8626: recovery writes the replayed pages straight to the data files, so before it deletes the WAL it replayed
   * it forces them, as a clean close and a WAL rotation do. When that fsync fails the WAL is the only durable copy of
   * the replayed pages and must survive the open (#4934 semantics), instead of being deleted as it always was.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RecoveryFsyncFailure {
    private static final String DB_PATH = "target/databases/Issue8626RecoveryFsyncFailureTest";

    @BeforeEach
    void fileSyncOn() {
      // the failure comes from a real fsync, which the unit-test lanes turn off (arcadedb.fileSync)
      GlobalConfiguration.FILE_SYNC.setValue(true);
    }

    @AfterEach
    void cleanup() {
      GlobalConfiguration.FILE_SYNC.reset();
      LocalDatabase.TEST_BEFORE_RECOVERY_REPLAY_HOOK = null;
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      try {
        if (factory.exists())
          factory.open().drop();
      } catch (final Exception e) {
        // THE TEST LEAVES A BROKEN FILE BEHIND: REMOVE THE DATABASE ON DISK BELOW
      }
      factory.close();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void recoveryWhoseFsyncFailsKeepsTheReplayedWal() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();

      try (final Database db = factory.create()) {
        db.getSchema().createDocumentType("Doc");
        db.getSchema().createDocumentType("Other");
        db.transaction(() -> db.newDocument("Other").set("v", 0).save());
      }

      // A crash after a committed transaction on Doc only: its WAL is what the next open replays.
      final Database crashed = factory.open();
      crashed.transaction(() -> crashed.newDocument("Doc").set("v", 1).save());
      ((LocalDatabase) crashed).kill();
      crashed.close();

      final File dbDir = new File(DB_PATH);
      final String[] crashWal = dbDir.list((d, n) -> n.endsWith(".wal"));
      assertThat(crashWal).isNotEmpty();

      // Break the fsync of a file the replay does not write to: recovery still has to force it (after a crash every
      // file is unsynced), and the failure must keep the WAL.
      final AtomicBoolean broken = new AtomicBoolean();
      LocalDatabase.TEST_BEFORE_RECOVERY_REPLAY_HOOK = db -> {
        try {
          final PaginatedComponent bucket = (PaginatedComponent) db.getSchema().getType("Other").getBuckets(false).getFirst();
          final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());
          final Field channelField = PaginatedComponentFile.class.getDeclaredField("channel");
          channelField.setAccessible(true);
          ((FileChannel) channelField.get(file)).close();
          broken.set(new File(file.getFilePath()).delete());
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      };

      final Database recovered = factory.open();
      try {
        assertThat(broken.get()).isTrue();
        for (final String wal : crashWal)
          assertThat(new File(dbDir, wal))
              .as("a WAL whose replayed pages could not be fsynced must survive the recovery").exists();

        assertThat(recovered.countType("Doc", false)).isEqualTo(1);
      } finally {
        LocalDatabase.TEST_BEFORE_RECOVERY_REPLAY_HOOK = null;
        recovered.close();
      }

      // The close cannot fsync the broken file either, so it stays crash-equivalent: WAL and lock file preserved.
      for (final String wal : crashWal)
        assertThat(new File(dbDir, wal)).exists();
      assertThat(new File(dbDir, "database.lck")).exists();
      factory.close();
    }
  }

  /**
   * Regression test for issue #8849: an index-compaction temporary ({@code *.temp_<ext>}) left in the database directory
   * - by a crash mid-compaction, by a snapshot shipped from a pre-#8019 leader, or by a restored backup - is invisible to
   * the open-time component scan (its extension is not in {@code SUPPORTED_FILE_EXT}), and nothing else removed it, so it
   * occupied disk for the life of the node. A read-write open now deletes it.
   */
  @Nested
  class OrphanCompactionTemporarySweep extends TestHelper {
    private static final String TYPE      = "Issue8849Doc";
    // A TYPE WHOSE NAME STARTS WITH THE TEMPORARY PREFIX: ITS FILES CARRY "temp_" IN THE NAME, NOT IN THE EXTENSION, AND
    // MUST SURVIVE THE SWEEP
    private static final String TEMP_TYPE = "temp_readings";

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE).createProperty("id", Integer.class);
      database.getSchema().getType(TYPE).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      database.getSchema().createDocumentType(TEMP_TYPE).createProperty("id", Integer.class);
      database.getSchema().getType(TEMP_TYPE).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");

      database.transaction(() -> {
        for (int i = 0; i < 100; i++) {
          database.newDocument(TYPE).set("id", i).save();
          database.newDocument(TEMP_TYPE).set("id", i).save();
        }
      });
    }

    @Test
    void orphanTemporariesAreDeletedOnOpen() throws IOException {
      final File[] orphans = plantOrphans();

      reopenDatabase();

      for (final File orphan : orphans)
        assertThat(orphan).doesNotExist();
      assertDataIntact();
    }

    @Test
    void orphanTemporaryLeftByACrashIsDeletedOnRecovery() throws IOException {
      ((DatabaseInternal) database).kill();
      // DEREGISTER THE KILLED INSTANCE: database.lck STAYS ON DISK, SO THE NEXT OPEN GOES THROUGH RECOVERY
      database.close();
      assertThat(new File(getDatabasePath(), "database.lck")).exists();

      final File[] orphans = plantOrphans();

      database = factory.open();

      for (final File orphan : orphans)
        assertThat(orphan).doesNotExist();
      assertDataIntact();
    }

    @Test
    void readOnlyOpenLeavesTemporariesInPlace() throws IOException {
      final File[] orphans = plantOrphans();

      reopenDatabaseInReadOnlyMode();

      // A READ-ONLY OPEN OWNS NOTHING (IT NEVER TAKES database.lck) AND MUST NOT WRITE TO THE DIRECTORY
      for (final File orphan : orphans)
        assertThat(orphan).exists();
      assertDataIntact();

      reopenDatabase();
      for (final File orphan : orphans)
        assertThat(orphan).doesNotExist();
    }

    @Test
    void filesOfATypeNamedLikeTheTemporaryPrefixAreNotSwept() {
      final File dir = new File(getDatabasePath());
      final String[] before = dir.list((d, name) -> name.startsWith(TEMP_TYPE));
      assertThat(before).isNotEmpty();

      reopenDatabase();

      assertThat(dir.list((d, name) -> name.startsWith(TEMP_TYPE))).containsExactlyInAnyOrder(before);
      assertDataIntact();
    }

    /**
     * Plants one orphan per producer of {@link PaginatedComponent#TEMP_EXT} files - the LSM-tree mutable index and the
     * LSM vector index - named as the producers name them, next to the real files of the database.
     */
    private File[] plantOrphans() throws IOException {
      database.close();

      final File dir = new File(getDatabasePath());
      final File[] orphans = new File[] {
          new File(dir, TYPE + "_0_99999.900.262144.v0." + PaginatedComponent.TEMP_EXT + LSMTreeIndexMutable.UNIQUE_INDEX_EXT),
          new File(dir, TEMP_TYPE + "_0_99999.901.262144.v0." + PaginatedComponent.TEMP_EXT + LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT),
          new File(dir, "vectors_99999.902.65536.v0." + PaginatedComponent.TEMP_EXT + LSMVectorIndexMutable.FILE_EXT) };
      for (final File orphan : orphans) {
        Files.write(orphan.toPath(), new byte[65_536]);
        assertThat(PaginatedComponent.isTemporaryFileName(orphan.getName())).isTrue();
      }
      return orphans;
    }

    private void assertDataIntact() {
      assertThat(database.countType(TYPE, false)).isEqualTo(100);
      assertThat(database.countType(TEMP_TYPE, false)).isEqualTo(100);
      assertThat(database.lookupByKey(TYPE, "id", 42).hasNext()).isTrue();
      assertThat(database.lookupByKey(TEMP_TYPE, "id", 42).hasNext()).isTrue();
    }
  }

  /**
   * Regression test for #8944: a ClosedChannelException surfacing from inside {@code executeInReadLock} must not make the
   * database close itself while the calling thread holds the read lock (the write lock close() waits for is never granted
   * to a reader), and a channel closed by an interrupt must not close it from {@code executeInWriteLock} either.
   * <p>
   * The lock contract is checked with a synthetic exception: the concurrent-cancel scenario of the issue is timing
   * dependent, and its retry half is covered deterministically by {@code PaginatedComponentFileReadRetryTest}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ClosedChannelInLock extends TestHelper {

    @Test
    void readLockClosedChannelDoesNotDeadlockClose() throws Exception {
      assertThatThrownBy(() -> database.executeInReadLock(() -> {
        throw new ClosedByInterruptException();
      })).isInstanceOf(DatabaseOperationException.class).hasCauseInstanceOf(ClosedChannelException.class);

      assertCloseReturns();
    }

    @Test
    void writeLockClosedByInterruptKeepsTheDatabaseOpen() {
      assertThatThrownBy(() -> database.executeInWriteLock(() -> {
        throw new ClosedByInterruptException();
      })).isInstanceOf(DatabaseOperationException.class).hasCauseInstanceOf(ClosedByInterruptException.class);

      assertThat(database.isOpen()).isTrue();
    }

    private void assertCloseReturns() throws Exception {
      final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        final Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
      });
      try {
        final Future<?> close = executor.submit(database::close);
        close.get(30, TimeUnit.SECONDS);
        assertThat(database.isOpen()).isFalse();
      } finally {
        executor.shutdownNow();
      }
    }
  }

  /**
   * Regression test for #9038: reading a record of a closed database must throw {@link DatabaseIsClosedException}, not answer empty.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReadAfterClose {
    private static final String PATH = "target/databases/Issue9038ReadAfterCloseTest";

    @Test
    void recordReadsAfterCloseThrow() {
      FileUtils.deleteRecursively(new File(PATH));
      final DatabaseFactory factory = new DatabaseFactory(PATH);
      try {
        try (final Database db = factory.create()) {
          db.command("sql", "CREATE DOCUMENT TYPE Person");
          db.transaction(() -> db.command("sql", "INSERT INTO Person SET name = 'Ann'"));
        }

        final Database db = factory.open();
        try {
          runAssertions(db);
        } finally {
          if (db.isOpen())
            db.close();
        }
      } finally {
        FileUtils.deleteRecursively(new File(PATH));
      }
    }

    private static void runAssertions(final Database db) {
      final Result sorted = db.query("sql", "SELECT FROM Person ORDER BY name").next();
      final Result projected = db.query("sql", "SELECT name FROM Person").next();
      final Document ann = db.query("sql", "SELECT FROM Person WHERE name = 'Ann'").next().getElement().get();
      assertThat(ann.get("name")).isEqualTo("Ann");
      db.close();

      assertThatThrownBy(() -> sorted.getProperty("name")).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(sorted::getPropertyNames).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(sorted::toJSON).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(() -> ann.get("name")).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(() -> ann.has("name")).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(ann::getPropertyNames).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(ann::toMap).isInstanceOf(DatabaseIsClosedException.class);
      assertThatThrownBy(ann::toJSON).isInstanceOf(DatabaseIsClosedException.class);
      // A PROJECTION COPIED ITS VALUES WHEN THE ROW WAS MADE
      assertThat((String) projected.getProperty("name")).isEqualTo("Ann");
    }
  }

  /**
   * Issue #9548: the JVM runs every shutdown hook concurrently, and the engine's hook closed every open database while the
   * server's own hook was still stopping the Raft HA service that writes into them. A leader committing across the
   * shutdown then failed to publish an entry the cluster had committed, and quarantined its own database. The engine's
   * hook now waits for the hooks registered as owning their databases before it closes anything.
   */
  @Nested
  class OwningShutdownHook {

    /** The core of the fix: a registered hook that is still running holds the engine's hook back until it is done. */
    @Test
    @Timeout(30)
    void theEngineHookWaitsForARunningOwningHook() throws Exception {
      final CountDownLatch ownerRunning = new CountDownLatch(1);
      final CountDownLatch ownerMayFinish = new CountDownLatch(1);
      final Thread owner = new Thread(() -> {
        ownerRunning.countDown();
        try {
          ownerMayFinish.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, "issue9548-owning-hook");
      DatabaseFactory.registerOwningShutdownHook(owner);
      try {
        owner.start();
        assertThat(ownerRunning.await(10, TimeUnit.SECONDS)).isTrue();

        final AtomicBoolean ownerDoneWhenReleased = new AtomicBoolean();
        final Thread engineHook = new Thread(() -> {
          DatabaseFactory.awaitOwningShutdownHooks(0);
          ownerDoneWhenReleased.set(owner.getState() == Thread.State.TERMINATED);
        }, "issue9548-engine-hook");
        engineHook.start();

        // A short wait expected to time out: a stall only makes it more true.
        engineHook.join(300);
        assertThat(engineHook.isAlive()).as("the engine hook must not proceed while the owning hook still runs").isTrue();

        ownerMayFinish.countDown();
        engineHook.join(10_000);
        assertThat(engineHook.isAlive()).isFalse();
        assertThat(ownerDoneWhenReleased.get()).as("released only once the owning hook had finished").isTrue();
      } finally {
        ownerMayFinish.countDown();
        DatabaseFactory.unregisterOwningShutdownHook(owner);
      }
    }

    /**
     * The JVM starts its hooks together and in no order, so the engine's hook can run before the owning one has started:
     * an unstarted hook is waited for too, but only for the grace, so a hook removed from the runtime cannot hold the exit.
     */
    @Test
    @Timeout(30)
    void anUnstartedOwningHookIsWaitedForOnlyForTheGrace() {
      final Thread neverStarted = new Thread(() -> { }, "issue9548-never-started");
      DatabaseFactory.registerOwningShutdownHook(neverStarted);
      try {
        DatabaseFactory.awaitOwningShutdownHooks(50);
        assertThat(neverStarted.getState()).isEqualTo(Thread.State.NEW);
      } finally {
        DatabaseFactory.unregisterOwningShutdownHook(neverStarted);
      }
    }

    /** An owning hook that starts within the grace is then waited for to the end. */
    @Test
    @Timeout(30)
    void anOwningHookStartedWithinTheGraceIsWaitedForToTheEnd() throws Exception {
      final CountDownLatch ownerMayFinish = new CountDownLatch(1);
      final Thread owner = new Thread(() -> {
        try {
          ownerMayFinish.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }, "issue9548-late-owning-hook");
      DatabaseFactory.registerOwningShutdownHook(owner);
      try {
        final AtomicBoolean ownerDoneWhenReleased = new AtomicBoolean();
        final Thread engineHook = new Thread(() -> {
          DatabaseFactory.awaitOwningShutdownHooks(5_000);
          ownerDoneWhenReleased.set(owner.getState() == Thread.State.TERMINATED);
        }, "issue9548-engine-hook-late");
        engineHook.start();
        engineHook.join(100);
        owner.start();
        engineHook.join(300);
        assertThat(engineHook.isAlive()).isTrue();

        ownerMayFinish.countDown();
        engineHook.join(10_000);
        assertThat(engineHook.isAlive()).isFalse();
        assertThat(ownerDoneWhenReleased.get()).isTrue();
      } finally {
        ownerMayFinish.countDown();
        DatabaseFactory.unregisterOwningShutdownHook(owner);
      }
    }

    /**
     * A hook that already finished, and the calling thread itself, are never waited for: joining itself, the caller would
     * wait forever, which the timeout turns into a failure.
     */
    @Test
    @Timeout(30)
    void aFinishedHookAndTheCallerItselfAreNotWaitedFor() throws Exception {
      final Thread finished = new Thread(() -> { }, "issue9548-finished");
      finished.start();
      finished.join();
      DatabaseFactory.registerOwningShutdownHook(finished);
      DatabaseFactory.registerOwningShutdownHook(Thread.currentThread());
      try {
        DatabaseFactory.awaitOwningShutdownHooks(0);
        assertThat(DatabaseFactory.isOwningShutdownHook(finished)).isTrue();
      } finally {
        DatabaseFactory.unregisterOwningShutdownHook(finished);
        DatabaseFactory.unregisterOwningShutdownHook(Thread.currentThread());
      }
      assertThat(DatabaseFactory.isOwningShutdownHook(finished)).isFalse();
    }
  }
}
