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
package com.arcadedb.integration.backup;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageManager;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of the full backup reading through the point-in-time page snapshot of issue #6075.
 * <p>
 * Two properties are defended. The first is correctness: a backup taken while transactions are committing restores to
 * a database that opens, passes the integrity check, and holds a record count between what was committed when the
 * backup started and what was committed when it finished - so the archive is a genuine point in time, neither torn
 * nor moving. The second is the reason the change exists: page flushing is no longer suspended for the DURATION of
 * the backup, so committing threads are not throttled by the {@code FLUSH_SUSPEND_MAX_DEFERRED_RAM} backlog.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6075SnapshotBackupIT {
  private static final String DATABASE_PATH = "target/databases/backup-snapshot";
  private static final String RESTORED_PATH = "target/databases/backup-snapshot-restored";
  private static final String BACKUP_FILE   = "target/backup-snapshot.zip";
  private static final String TYPE          = "Doc";
  private static final int    RECORDS       = 20_000;
  /** Throttled so the backup lasts long enough to sample the flush-suspension state a meaningful number of times. */
  private static final int    MAX_MB_PER_SECOND = 4;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.reset();
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The archive is a point in time even though transactions kept committing throughout, on the snapshot path and on
   * the suspend-and-freeze fallback alike.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void backupUnderConcurrentWritersRestoresToAPointInTime(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final AtomicBoolean running = new AtomicBoolean(true);
      final AtomicReference<Exception> writerFailure = new AtomicReference<>();
      final CountDownLatch warmedUp = new CountDownLatch(1);
      final AtomicInteger sequence = new AtomicInteger(RECORDS);

      final Thread writer = writerThread(database, running, writerFailure, warmedUp, sequence);
      try {
        assertThat(warmedUp.await(30, TimeUnit.SECONDS)).isTrue();

        final long countBefore = database.countType(TYPE, false);
        new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(MAX_MB_PER_SECOND).backupDatabase();
        final long countAfter = database.countType(TYPE, false);

        assertThat(countAfter).as("the writers must have kept committing during the backup").isGreaterThan(countBefore);

        running.set(false);
        writer.join(30_000);
        assertThat(writerFailure.get()).isNull();

        new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

        try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
          assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors")).isZero();
          assertThat(restored.countType(TYPE, false)).isBetween(countBefore, countAfter);
        }
      } finally {
        running.set(false);
        writer.join(30_000);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The headline win: with the snapshot the flush thread is parked only for the t0 barrier, not for the whole
   * backup. The suspend-and-freeze path is measured in the same run as the control - it is suspended essentially
   * from the first sample to the last - so the comparison does not depend on any absolute timing of this machine.
   */
  @Test
  void theBackupNoLongerSuspendsPageFlushingForItsWholeDuration() throws Exception {
    final double withSnapshot = measureSuspendedFraction(true);
    final double withoutSnapshot = measureSuspendedFraction(false);

    assertThat(withoutSnapshot).as("the fallback path must still suspend flushing for the whole backup").isGreaterThan(0.9);
    assertThat(withSnapshot).as("a snapshot backup must not hold the flush suspension for its duration").isLessThan(0.5);
  }

  /**
   * The reliability story of the snapshot path is that a window which cannot hold its point in time does NOT produce
   * a torn archive: the whole backup restarts on the suspend-and-freeze path, which throttles writers but always
   * completes. A streamed archive cannot be repaired in place, so this is a restart of `writeArchive` against the
   * same file, and it has to be exercised end to end rather than only at the `PageSnapshot` level.
   * <p>
   * The shadow cap is set to 1 MB - a handful of 64 KB pages - while a writer rewrites the whole 10 MB dataset in a
   * loop, so the window is certain to overflow while the (throttled) backup is still reading.
   */
  @Test
  void aShadowOverflowMidBackupFallsBackAndStillProducesARestorableArchive() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.setValue(1);
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.setValue(1);

    final PrintStream originalOut = System.out;
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();

    try (final Database database = createDatabase()) {
      final AtomicBoolean running = new AtomicBoolean(true);
      final AtomicReference<Exception> writerFailure = new AtomicReference<>();
      final CountDownLatch warmedUp = new CountDownLatch(1);

      final Thread writer = new Thread(() -> {
        int round = 0;
        while (running.get()) {
          final int current = round++;
          try {
            // A FULL REWRITE PASS DIRTIES EVERY PAGE OF THE DATASET, SO THE 1 MB SHADOW IS BLOWN THROUGH AT ONCE
            database.transaction(() -> database.iterateType(TYPE, false).forEachRemaining(
                record -> record.asDocument().modify().set("payload", "overflow-" + current + "-" + "y".repeat(500)).save()));
            warmedUp.countDown();
          } catch (final Exception e) {
            writerFailure.compareAndSet(null, e);
            return;
          }
        }
      }, "backup-overflow-writer");
      writer.setDaemon(true);
      writer.start();

      try {
        assertThat(warmedUp.await(60, TimeUnit.SECONDS)).isTrue();

        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
          new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(2).backupDatabase();
        } finally {
          System.setOut(originalOut);
        }

        running.set(false);
        writer.join(60_000);
        assertThat(writerFailure.get()).isNull();

        assertThat(captured.toString(StandardCharsets.UTF_8))
            .as("the overflow must have driven the documented fallback, not merely been survived")
            .contains("Point-in-time snapshot unusable");

        assertThat(new File(BACKUP_FILE)).exists();
        new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

        try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
          assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors")).isZero();
          assertThat(restored.countType(TYPE, false)).isEqualTo(RECORDS);
        }
      } finally {
        System.setOut(originalOut);
        running.set(false);
        writer.join(60_000);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  private double measureSuspendedFraction(final boolean snapshot) throws Exception {
    clean();
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      final AtomicBoolean sampling = new AtomicBoolean(true);
      final AtomicLong samples = new AtomicLong();
      final AtomicLong suspendedSamples = new AtomicLong();

      final Thread sampler = new Thread(() -> {
        while (sampling.get()) {
          samples.incrementAndGet();
          if (pageManager.isPageFlushingSuspended(db))
            suspendedSamples.incrementAndGet();
          try {
            Thread.sleep(1);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "backup-suspension-sampler");
      sampler.setDaemon(true);
      sampler.start();

      try {
        new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(MAX_MB_PER_SECOND).backupDatabase();
      } finally {
        sampling.set(false);
        sampler.join(10_000);
      }

      assertThat(samples.get()).as("the throttled backup must last long enough to sample").isGreaterThan(50);
      return (double) suspendedSamples.get() / samples.get();
    }
  }

  private Thread writerThread(final Database database, final AtomicBoolean running, final AtomicReference<Exception> failure,
      final CountDownLatch warmedUp, final AtomicInteger sequence) {
    final Thread writer = new Thread(() -> {
      while (running.get()) {
        try {
          final int id = sequence.incrementAndGet();
          database.transaction(
              () -> database.newDocument(TYPE).set("id", id).set("payload", "concurrent-" + id).save());
          warmedUp.countDown();
        } catch (final Exception e) {
          failure.compareAndSet(null, e);
          return;
        }
      }
    }, "backup-snapshot-writer");
    writer.setDaemon(true);
    writer.start();
    return writer;
  }

  private Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).set("payload", "x".repeat(500)).save();
    });
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    return database;
  }
}
