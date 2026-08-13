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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The backup-side measurements issue #6075 asked for and PR #6100 did not produce, listed in #6116: what a running
 * backup does to the writers beside it on the point-in-time snapshot path against the suspend-and-freeze path, how
 * much deferred RAM each accumulates, and how big the copy-on-write shadow gets as a fraction of the database - the
 * sizing evidence behind the 1 GB {@code arcadedb.pageSnapshotMaxSize} default, which was chosen without any.
 * <p>
 * Deliberately shaped like {@link BackupCompressionBenchmark#concurrentWriterImpact()}, the #6072 harness that
 * produced the 4.3% / 77% figures, so the two sets of numbers can be read side by side: the same window-scoped
 * measurement (only commits that fall INSIDE the backup count, otherwise a long backup and a short one dilute to
 * the same average), the same percentiles, the same deferred-RAM sampler.
 * <p>
 * The shadow and deferred-RAM readings are taken through {@code PageManager.getStats()} - the gauges #6116 adds -
 * rather than through the window object, because the backup owns its window internally and never exposes it. That
 * is the same path an operator's dashboard reads, so this benchmark also exercises the wiring it depends on.
 * <p>
 * Excluded from the normal build ({@code @Tag("benchmark")}). Run it explicitly:
 * <pre>
 *   mvn -o -pl integration test -Dtest=PageSnapshotBackupBenchmark -Dgroups=benchmark -DexcludedGroups=
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class PageSnapshotBackupBenchmark {
  private static final String DATABASE_PATH = "target/databases/page-snapshot-backup-benchmark";
  private static final String BACKUP_FILE   = "target/page-snapshot-backup-benchmark.zip";
  private static final String TYPE          = "Doc";
  private static final int    TARGET_MB     = Integer.parseInt(
      System.getProperty("arcadedb.snapshot.benchmark.sizeMB", "512"));
  private static final int    PAYLOAD_SIZE  = 512;
  /** Records per transaction, and the unit every latency percentile below is measured over. */
  private static final int    BATCH         = 50;
  /** Throttled so the backup lasts long enough to sample, and so the write rates below are what differs, not the I/O. */
  private static final int    MAX_MB_PER_SECOND = 32;

  /**
   * Update targets, kept as raw (bucket, position) pairs rather than as {@link RID}s: a RID carries the
   * {@code Database} instance it was created against, and every measurement below opens its own instance - so a
   * stored RID would resolve against the closed builder instance and every update would throw.
   */
  private static final List<int[]> MUTABLE_RECORDS = new ArrayList<>();
  private static final String      PAYLOAD         = payload();

  private static long databaseSize;

  /**
   * Rebuilt from scratch before EVERY measurement, not once for the class. The load inserts as well as updates, so
   * a shared database grows from one run to the next: the later runs would be backing up more data than the earlier
   * ones (a longer window, a different throughput) and, worse, the "shadow as a fraction of the database" column -
   * the one number this benchmark exists to produce - would be dividing by a size that no longer exists.
   */
  static void buildDatabase() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    new File(BACKUP_FILE).delete();
    MUTABLE_RECORDS.clear();

    final long begin = System.currentTimeMillis();
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.getSchema().createDocumentType(TYPE);

      final long records = (long) TARGET_MB * 1024 * 1024 / (PAYLOAD_SIZE + 64);
      long id = 0;
      while (id < records) {
        database.transaction(() -> {
          for (int i = 0; i < 5_000; i++) {
            final MutableDocument document = database.newDocument(TYPE).set("payload", PAYLOAD);
            document.save();
            // ONE IN A HUNDRED IS ENOUGH TO DRIVE UPDATES THAT LAND ALL OVER THE FILE WITHOUT HOLDING ONE PER RECORD
            final RID rid = document.getIdentity();
            if (MUTABLE_RECORDS.size() < 50_000 && rid.getPosition() % 100 == 0)
              MUTABLE_RECORDS.add(new int[] { rid.getBucketId(), (int) rid.getPosition() });
          }
        });
        id += 5_000;
      }
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    }

    databaseSize = directorySize(new File(DATABASE_PATH));
    System.out.printf("[snapshot-backup-benchmark] built a %s database in %,d ms%n",
        FileUtils.getSizeAsString(databaseSize), System.currentTimeMillis() - begin);
  }

  @AfterAll
  static void dropDatabase() {
    clean();
  }

  static void clean() {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).open()) {
      database.drop();
    } catch (final Exception e) {
      // ALREADY GONE OR NEVER BUILT: THE RECURSIVE DELETE BELOW IS THE BACKSTOP
    }
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    new File(BACKUP_FILE).delete();
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.reset();
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.reset();
  }

  /**
   * The headline comparison: sustained write throughput and commit latency during a backup, on the snapshot path
   * against the suspend-and-freeze one, with a no-backup baseline measured in the same run. The deferred-RAM column
   * is what the suspension costs in RAM before it starts throttling committers outright at
   * {@code arcadedb.flushSuspendMaxDeferredRAM}; on the snapshot path it should never leave zero.
   */
  @Test
  void writerImpactOfABackupOnBothPaths() throws Exception {
    System.out.println("\n[snapshot-backup-benchmark] writers running beside a backup (measured only INSIDE the "
        + "backup window):");
    System.out.println("  " + runWriterLoad("no backup running", null));
    System.out.println("  " + runWriterLoad("backup, snapshot path", Boolean.TRUE));
    System.out.println("  " + runWriterLoad("backup, suspend-and-freeze", Boolean.FALSE));
  }

  /**
   * How large the copy-on-write shadow grows during a real backup, at several write rates, as a fraction of the
   * database. This is the number the 1 GB {@code arcadedb.pageSnapshotMaxSize} default has to be judged against: the
   * shadow holds the pre-image of every page dirtied while the window is open, so its ceiling is the working set of
   * the backup's duration, and on a small, very hot database it approaches the database size itself.
   */
  @Test
  void shadowPeakSizeAcrossWriteRates() throws Exception {
    System.out.println("\n[snapshot-backup-benchmark] copy-on-write shadow peak during a backup, by write rate:");
    for (final int pauseMs : new int[] { 100, 20, 5, 0 })
      System.out.println("  " + runWriterLoad("pause %3d ms between transactions".formatted(pauseMs), Boolean.TRUE,
          pauseMs));
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static String runWriterLoad(final String label, final Boolean snapshotPath) throws Exception {
    return runWriterLoad(label, snapshotPath, 0);
  }

  /**
   * Runs a sustained mixed insert/update load and reports what happened only inside the measurement window - the
   * backup, or a fixed sleep for the baseline.
   *
   * @param snapshotPath {@code true} for the page-snapshot path, {@code false} for suspend-and-freeze, {@code null}
   *                     to run no backup at all.
   */
  private static String runWriterLoad(final String label, final Boolean snapshotPath, final int pauseMs)
      throws Exception {
    if (snapshotPath != null)
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshotPath);

    buildDatabase();

    try (final Database database = new DatabaseFactory(DATABASE_PATH).open()) {
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

      final AtomicBoolean stop = new AtomicBoolean(false);
      final AtomicLong peakDeferredRAM = new AtomicLong();
      final AtomicLong peakShadowBytes = new AtomicLong();
      final AtomicLong peakShadowPages = new AtomicLong();
      final List<long[]> samples = new ArrayList<>();

      final AtomicReference<Exception> writerFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        while (!stop.get()) {
          final long begin = System.nanoTime();
          try {
            database.transaction(() -> {
              for (int i = 0; i < BATCH / 2; i++)
                database.newDocument(TYPE).set("payload", PAYLOAD).save();
              for (int i = 0; i < BATCH / 2; i++) {
                final int[] target = MUTABLE_RECORDS.get(ThreadLocalRandom.current().nextInt(MUTABLE_RECORDS.size()));
                database.newRID(target[0], target[1]).asDocument(true).modify().set("payload", PAYLOAD).save();
              }
            });
          } catch (final Exception e) {
            // A DEAD WRITER WOULD OTHERWISE BE REPORTED AS A ROW OF PERFECT ZEROS
            writerFailure.compareAndSet(null, e);
            return;
          }
          final long end = System.nanoTime();
          synchronized (samples) {
            samples.add(new long[] { end, (end - begin) / 1000 });
          }
          if (pauseMs > 0)
            try {
              Thread.sleep(pauseMs);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
        }
      }, "snapshot-backup-benchmark-writer");

      final Thread sampler = new Thread(() -> {
        while (!stop.get()) {
          final PageManager.PPageManagerStats stats = pageManager.getStats();
          peakDeferredRAM.accumulateAndGet(stats.deferredRAMBytes, Math::max);
          peakShadowBytes.accumulateAndGet(stats.snapshotShadowBytes, Math::max);
          peakShadowPages.accumulateAndGet(stats.snapshotShadowedPages, Math::max);
          try {
            Thread.sleep(20);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "snapshot-backup-benchmark-sampler");
      sampler.setDaemon(true);

      final long windowBegin;
      final long windowEnd;
      writer.start();
      sampler.start();
      try {
        // LET THE LOAD REACH STEADY STATE BEFORE THE WINDOW OPENS
        Thread.sleep(2_000);
        windowBegin = System.nanoTime();
        if (snapshotPath != null)
          new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(MAX_MB_PER_SECOND).backupDatabase();
        else
          Thread.sleep(10_000);
        windowEnd = System.nanoTime();
      } finally {
        stop.set(true);
        writer.join(60_000);
        sampler.join(10_000);
        new File(BACKUP_FILE).delete();
      }

      if (writerFailure.get() != null)
        throw writerFailure.get();

      final List<Long> inWindow = new ArrayList<>();
      synchronized (samples) {
        for (final long[] sample : samples)
          if (sample[0] >= windowBegin && sample[0] <= windowEnd)
            inWindow.add(sample[1]);
      }
      final long[] sorted = new long[inWindow.size()];
      for (int i = 0; i < sorted.length; i++)
        sorted[i] = inWindow.get(i);
      Arrays.sort(sorted);

      final double windowSeconds = (windowEnd - windowBegin) / 1e9;
      return ("%-34s window=%6.1fs commits=%,6d (%,8.0f rec/s) p50=%,7dus p95=%,8dus p99=%,8dus "
          + "peakDeferredRAM=%9s shadowPeak=%9s (%,6d pages, %5.1f%% of the database)").formatted(label, windowSeconds,
          sorted.length, sorted.length * (double) BATCH / windowSeconds, percentile(sorted, 50), percentile(sorted, 95),
          percentile(sorted, 99), FileUtils.getSizeAsString(peakDeferredRAM.get()),
          FileUtils.getSizeAsString(peakShadowBytes.get()), peakShadowPages.get(),
          100.0 * peakShadowBytes.get() / databaseSize);
    }
  }

  private static long percentile(final long[] sorted, final int percentile) {
    if (sorted.length == 0)
      return 0;
    return sorted[Math.min(sorted.length - 1, (int) ((long) sorted.length * percentile / 100))];
  }

  private static String payload() {
    final StringBuilder builder = new StringBuilder(PAYLOAD_SIZE);
    while (builder.length() < PAYLOAD_SIZE)
      builder.append("field").append(builder.length()).append("=value;");
    return builder.substring(0, PAYLOAD_SIZE);
  }

  private static long directorySize(final File directory) {
    long size = 0;
    final File[] files = directory.listFiles();
    if (files != null)
      for (final File file : files)
        size += file.isDirectory() ? directorySize(file) : file.length();
    return size;
  }
}
