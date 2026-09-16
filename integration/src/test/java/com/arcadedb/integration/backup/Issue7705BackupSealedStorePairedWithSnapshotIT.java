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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7705: a full backup's {@code .ts.sealed} set must be the SAME OBSERVATION as the {@code schema.json} the
 * point-in-time window captured, and a store it listed and could not read must fail the backup rather than be
 * skipped.
 * <p>
 * Two gaps on one path, both inherited from the reasoning issue #7671 corrected on the HA snapshot ship:
 * <ol>
 *   <li>{@code backupFromSnapshot} opened its window under NO database lock and then listed the sealed stores off
 *       the filesystem afterwards, so a {@code DROP TYPE} of a TIMESERIES type landing between the two produced an
 *       archive whose {@code schema.json} - captured at t0 - declares the type while its sealed segments are
 *       absent: the #6356 / #6839 "type whose sealed store fails to load" state, in a backup;</li>
 *   <li>a store that went away between the listing and the read was swallowed with a log line, so a
 *       {@code FULL BACKUP} could report success over an archive that contradicts its own schema, with nothing in
 *       the archive recording that it does. The swallow was in fact SILENT: {@code compressFile}'s
 *       {@code exists()} pre-check returned 0 before the {@code FileNotFoundException} the catch was written
 *       for could ever be raised.</li>
 * </ol>
 * The fix is the ship's: list inside the same read-locked frame that opens the window, and refuse. The lock is
 * held for the barrier plus one {@code File.listFiles}, not for the backup, so the availability #6114 bought is
 * untouched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7705BackupSealedStorePairedWithSnapshotIT {
  private static final String DATABASE_PATH = "target/databases/backup-7705";
  private static final String BACKUP_FILE   = "target/backup-7705.zip";
  private static final String TYPE          = "Reading";
  private static final int    SHARDS        = 2;
  private static final int    SAMPLES       = 5_000;
  private static final long   BASE_TS       = 1_700_000_000_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * Gap 1, observed through the lock itself: with the capture inside {@code executeInReadLock}, a backup cannot
   * even start capturing while a writer holds the database write lock - which is exactly what a {@code DROP TYPE}
   * holds while it rewrites {@code schema.json}. Before the fix the window path took no database lock at all, so
   * the backup sailed straight past a concurrent schema change and paired a t0 {@code schema.json} with a listing
   * taken after it.
   * <p>
   * Asserted as "blocked while held, completes once released" rather than by racing a real {@code DROP TYPE}: the
   * span between {@code openSnapshot} and {@code File.listFiles} is microseconds wide, so a racing test would be
   * asserting a coin flip. This one is decidable.
   */
  @Test
  void theCaptureWaitsForAConcurrentSchemaWriter() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final TimeSeriesEngine engine = engineOf(database);
      ingest(engine, 0, SAMPLES);
      engine.compactAll();
      assertThat(sealedFiles()).as("the test is about pairing sealed stores, so there must be some").hasSize(SHARDS);

      final CountDownLatch writerHoldsTheLock = new CountDownLatch(1);
      final CountDownLatch releaseTheWriter = new CountDownLatch(1);
      final CountDownLatch backupFinished = new CountDownLatch(1);
      final AtomicReference<Throwable> backupFailure = new AtomicReference<>();

      final Thread writer = new Thread(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        try {
          ((DatabaseInternal) database).executeInWriteLock(() -> {
            writerHoldsTheLock.countDown();
            assertThat(releaseTheWriter.await(60, TimeUnit.SECONDS)).isTrue();
            return null;
          });
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        }
      }, "issue7705-schema-writer");
      writer.setDaemon(true);
      writer.start();

      final Thread backup = new Thread(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        try {
          new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
        } catch (final Throwable e) {
          backupFailure.set(e);
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
          backupFinished.countDown();
        }
      }, "issue7705-backup");
      backup.setDaemon(true);

      try {
        assertThat(writerHoldsTheLock.await(60, TimeUnit.SECONDS)).isTrue();
        backup.start();

        assertThat(backupFinished.await(3, TimeUnit.SECONDS))
            .as("#7705: the capture is inside the database read lock, so it cannot complete while a schema writer "
                + "holds the write lock - before the fix the window path took no database lock at all")
            .isFalse();
      } finally {
        releaseTheWriter.countDown();
      }

      assertThat(backupFinished.await(120, TimeUnit.SECONDS))
          .as("and it must complete once the writer lets go, not deadlock on it")
          .isTrue();
      assertThat(backupFailure.get()).isNull();
      writer.join(60_000);
      assertThat(new File(BACKUP_FILE)).exists();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * Gap 2, on both backup paths: a name the listing produced that cannot be opened when its turn comes fails the
   * backup, naming the store, and leaves no archive behind for an operator to restore from.
   * <p>
   * The unopenable store is a DIRECTORY carrying the sealed-store name. That is the same {@code
   * FileNotFoundException} a store deleted between the listing and the read raises, produced deterministically
   * rather than by winning a race, and it exercises the whole chain: the listing names it, the {@code exists()}
   * pre-check no longer hides it, and the refusal turns it into a failed backup.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aSealedStoreThatCannotBeReadFailsTheBackup(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final TimeSeriesEngine engine = engineOf(database);
      ingest(engine, 0, SAMPLES);
      engine.compactAll();

      final File unreadable = new File(DATABASE_PATH, "Vanished_shard_0.ts.sealed");
      assertThat(unreadable.mkdir()).isTrue();

      assertThatThrownBy(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase())
          .as("#7705: an archive that silently omits a store its schema declares reports success and is "
              + "discovered at restore time")
          .isInstanceOf(BackupException.class)
          // Backup.backupDatabase rewraps with its own generic message, so the refusal is asserted along the whole
          // chain rather than on the outermost frame.
          .hasStackTraceContaining("Vanished_shard_0.ts.sealed")
          .hasStackTraceContaining("would declare its type without its data");

      assertThat(new File(BACKUP_FILE))
          .as("a failed backup must not leave a partial archive behind")
          .doesNotExist();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The compaction scratch file is still not a sealed store and is still not archived, so the refusal above did
   * not turn an unrelated leftover into a failed backup.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aCompactionScratchFileStillDoesNotFailTheBackup(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final TimeSeriesEngine engine = engineOf(database);
      ingest(engine, 0, SAMPLES);
      engine.compactAll();

      assertThat(new File(DATABASE_PATH, TYPE + "_shard_0.ts.sealed.tmp").createNewFile()).isTrue();

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
      assertThat(new File(BACKUP_FILE)).exists();
    }
    TestHelper.checkActiveDatabases();
  }

  // --- helpers ---

  private Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);
    return database;
  }

  private static TimeSeriesEngine engineOf(final Database database) {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
  }

  private static File[] sealedFiles() {
    final File[] files = new File(DATABASE_PATH).listFiles((d, name) -> name.endsWith(".ts.sealed"));
    return files != null ? files : new File[0];
  }

  private static void ingest(final TimeSeriesEngine engine, final int from, final int count) throws Exception {
    final long[] timestamps = new long[count];
    final Object[] hosts = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = BASE_TS + (long) (from + i) * 1_000L;
      hosts[i] = "host_" + ((from + i) % 4);
      values[i] = (double) (from + i);
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }
}
