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
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7280 - a full backup archives the compacted TimeSeries segments, and restores them consistently.
 * <p>
 * A {@code .ts.sealed} sealed store is raw {@code FileChannel} I/O and is registered with neither the
 * {@code FileManager} nor the page snapshot, so both backup paths used to walk straight past it: the archive
 * completed, the restore completed, and every compacted sample was gone. Two properties are defended here, on
 * both paths ({@code PAGE_SNAPSHOT_ENABLED} true and false):
 * <ol>
 *   <li>the sealed segments are in the archive and come back out of a restore, sample for sample;</li>
 *   <li>a compaction running DURING the backup window neither loses nor duplicates samples - the sealed image
 *       and the page image in one archive are a pair, not two unrelated instants.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7280SealedTimeSeriesBackupIT {
  private static final String DATABASE_PATH = "target/databases/backup-sealed-ts";
  private static final String RESTORED_PATH = "target/databases/backup-sealed-ts-restored";
  private static final String BACKUP_FILE   = "target/backup-sealed-ts.zip";
  private static final String TYPE          = "Reading";
  private static final int    SHARDS        = 2;
  private static final int    SAMPLES       = 60_000;
  private static final long   BASE_TS       = 1_700_000_000_000L;
  private static final long   STEP_MS       = 1_000L;
  /** Throttled so the racing compaction has a window to land inside. */
  private static final int    MAX_MB_PER_SECOND = 1;
  /** Small enough that a full ingest+compact cycle fits several times inside the throttled backup window. */
  private static final int    RACE_CHUNK        = 2_000;
  private static final String DOC_TYPE          = "Sensor";
  /** Bulk that makes the throttled backup last seconds, so compactions are certain to land inside its window. */
  private static final int    DOCUMENTS         = 8_000;
  private static final String PAYLOAD           = "x".repeat(500);

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The reported defect: everything that had been compacted into a sealed segment was missing from the restore,
   * and only the mutable {@code .tstb} tail came back.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void sealedSegmentsSurviveABackupRoundTrip(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    final List<Object[]> expected;
    try (final Database database = createDatabase()) {
      populateDocuments(database, 100);

      final TimeSeriesEngine engine = engineOf(database);
      ingest(engine, 0, SAMPLES);
      engine.compactAll();

      // The test is only about sealed data if there IS sealed data: without this the fix could be a no-op and
      // the assertions below would still pass on the mutable tail alone.
      assertThat(sealedBlocks(engine)).as("the fixture must have sealed at least one segment").isPositive();
      assertThat(sealedFiles()).as("the sealed stores must exist on disk before the backup").hasSize(SHARDS);

      // Samples written after the compaction stay in the mutable bucket, so the restore has to reassemble both
      // halves rather than either one alone.
      ingest(engine, SAMPLES, SAMPLES / 10);

      expected = readAll(engine);
      assertThat(expected).hasSize(SAMPLES + SAMPLES / 10);

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    }

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors")).isZero();
      assertThat(restored.countType(DOC_TYPE, false)).as("the graph half must be unaffected").isEqualTo(100);

      final List<Object[]> actual = readAll(engineOf(restored));
      assertThat(actual).as("every sample, sealed and mutable alike, must come back").hasSameSizeAs(expected);
      for (int i = 0; i < expected.size(); i++)
        assertThat(actual.get(i)).as("sample %d", i).containsExactly(expected.get(i));
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * Guards the entry NAMES: a sealed store written under a path-qualified or otherwise wrong name would extract
   * somewhere the reopened database never looks, and the round-trip above would fail with no clue why. The
   * compaction scratch file must NOT be in there.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void theArchiveCarriesOneEntryPerSealedStoreAndNoScratchFile(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final TimeSeriesEngine engine = engineOf(database);
      ingest(engine, 0, SAMPLES);
      engine.compactAll();
      assertThat(sealedBlocks(engine)).isPositive();

      // A leftover scratch file from an interrupted compaction: it is not a sealed store and must be skipped.
      assertThat(new File(DATABASE_PATH, TYPE + "_shard_0.ts.sealed.tmp").createNewFile()).isTrue();

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    }

    final List<String> entries = new ArrayList<>();
    try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements())
        entries.add(it.nextElement().getName());
    }

    assertThat(entries).contains(TYPE + "_shard_0.ts.sealed", TYPE + "_shard_1.ts.sealed");
    assertThat(entries).doesNotContain(TYPE + "_shard_0.ts.sealed.tmp");
    TestHelper.checkActiveDatabases();
  }

  /**
   * The consistency half. A {@code .ts.sealed} is swapped as a whole file, outside the page snapshot's window
   * and outside the flush suspension, so a compaction that both STARTS and FINISHES inside the backup window
   * would otherwise pair a post-swap sealed image with a pre-clear page image - the same samples twice.
   * <p>
   * Compactions are driven in a loop for the duration of a throttled backup, rather than left to the 60-second
   * maintenance scheduler, so at least one of them is certainly contained in the window. Two things are then
   * asserted about the restore, and each fails for its own reason: the sample count must sit inside the
   * [before, after] band the backup's point in time can legitimately fall in (a smaller count is loss), and no
   * timestamp may appear twice (a repeat is a torn sealed/page pair). Timestamps are unique by construction,
   * so both are decidable.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aCompactionRacingTheBackupNeitherLosesNorDuplicatesSamples(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    final int countBefore;
    final int countAfter;
    try (final Database database = createDatabase()) {
      // The bulk lives in the document type: it is what makes the throttled backup last long enough for a
      // compaction to both start and finish inside its window, which is the interleaving under test.
      populateDocuments(database, DOCUMENTS);

      final TimeSeriesEngine engine = engineOf(database);
      ingest(engine, 0, SAMPLES);
      engine.compactAll();
      assertThat(sealedBlocks(engine)).as("the fixture must start with a sealed segment").isPositive();

      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean running = new AtomicBoolean(true);
      final AtomicInteger written = new AtomicInteger(SAMPLES);
      final AtomicInteger compactions = new AtomicInteger();
      final CountDownLatch warmedUp = new CountDownLatch(1);

      final Thread compactor = new Thread(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database);
        try {
          while (running.get()) {
            ingest(engine, written.get(), RACE_CHUNK);
            written.addAndGet(RACE_CHUNK);
            engine.compactAll();
            compactions.incrementAndGet();
            warmedUp.countDown();
          }
        } catch (final Throwable e) {
          failure.set(e);
          warmedUp.countDown();
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        }
      }, "issue7280-racing-compactor");
      compactor.setDaemon(true);
      compactor.start();

      try {
        assertThat(warmedUp.await(120, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull();

        final int compactionsBefore = compactions.get();
        // Every one of these samples is fully committed before the backup starts, so the archive owes all of
        // them whatever point in time it settles on.
        countBefore = written.get();
        new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(MAX_MB_PER_SECOND).backupDatabase();
        final int compactionsInsideWindow = compactions.get() - compactionsBefore;

        // The two paths hold the pause for different spans, so what "raced the backup" means differs, and the
        // assertion says which one it is rather than papering over the difference:
        //
        // - the snapshot path releases the pause as soon as the sealed stores have been read, so compactions
        //   run freely for the whole (long, throttled) page-streaming phase. A compaction completing there IS
        //   the interleaving under test, and the restore below has to survive it;
        // - the frozen path holds the pause for its whole callback, so a compaction is expected to be BLOCKED
        //   rather than to complete. Asserting zero would be racy - a compaction already in its lock-free phase
        //   when the pause was taken can still finish - so what is asserted for that path is only that the
        //   blocked compaction is released and completes, below.
        if (snapshot)
          assertThat(compactionsInsideWindow)
              .as("with the pause released after the sealed copy, a compaction must have completed inside the "
                  + "page-streaming phase").isPositive();
      } finally {
        running.set(false);
        compactor.join(120_000);
      }
      assertThat(failure.get()).isNull();
      assertThat(compactor.isAlive())
          .as("the compactor must have finished, not still be blocked on a pause that was never released").isFalse();
      // Sampled only once the writer has stopped, so no chunk is half-committed behind the counter and the
      // upper bound is a real ceiling rather than a racing read.
      countAfter = written.get();
    }

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      final List<Long> timestamps = readTimestamps(engineOf(restored));

      assertThat(timestamps.size()).as("the archive must be a point in time between the two samplings")
          .isBetween(countBefore, countAfter);
      assertThat(timestamps).as("a repeated timestamp means a post-compaction sealed image was paired with a "
          + "pre-compaction page image").doesNotHaveDuplicates();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * A TimeSeries type alongside a plain document type. The document type is not decoration: the user who
   * reported this saw "the backups appear to only be capturing the graph", so the test has to show the graph
   * half still restoring, and {@code CHECK DATABASE} reports {@code totalErrors} from record buckets, of which a
   * TimeSeries type owns none.
   */
  private Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);
    database.command("sql", "CREATE DOCUMENT TYPE " + DOC_TYPE);
    return database;
  }

  private static void populateDocuments(final Database database, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument(DOC_TYPE).set("id", i).set("payload", PAYLOAD).save();
    });
  }

  private static TimeSeriesEngine engineOf(final Database database) {
    return ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
  }

  private static int sealedBlocks(final TimeSeriesEngine engine) {
    int total = 0;
    for (int i = 0; i < engine.getShardCount(); i++)
      total += engine.getShard(i).getSealedStore().getBlockCount();
    return total;
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
      timestamps[i] = BASE_TS + (long) (from + i) * STEP_MS;
      hosts[i] = "host_" + ((from + i) % 4);
      values[i] = (double) (from + i);
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  /** Every sample in timestamp order, as one row per sample, so the assertions can compare element-wise. */
  private static List<Object[]> readAll(final TimeSeriesEngine engine) throws Exception {
    final List<Object[]> rows = new ArrayList<>();
    final Iterator<Object[]> it = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    while (it.hasNext())
      rows.add(it.next());
    return rows;
  }

  /** The timestamp column alone, in order. */
  private static List<Long> readTimestamps(final TimeSeriesEngine engine) throws Exception {
    final List<Long> timestamps = new ArrayList<>();
    for (final Object[] row : readAll(engine))
      timestamps.add((Long) row[0]);
    return timestamps;
  }
}
