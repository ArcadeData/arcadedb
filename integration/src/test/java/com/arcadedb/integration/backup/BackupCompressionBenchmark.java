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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Before/after measurements required by issue #6072: a change that claims a several-times-faster backup has to show
 * the numbers, including the ones that are easy to forget - the archive size it traded away for the speed, and what a
 * running backup does to the writers it is running alongside.
 * <p>
 * Excluded from the normal build ({@code @Tag("benchmark")}). Run it explicitly:
 * <pre>
 *   mvn -o -pl integration test -Dtest=BackupCompressionBenchmark -Darcadedb.backup.benchmark.sizeMB=1024
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class BackupCompressionBenchmark {
  private static final String DATABASE_PATH = "target/databases/backup-benchmark";
  private static final String BACKUP_FILE   = "target/backup-benchmark.zip";
  private static final String RESTORED_PATH = "target/databases/backup-benchmark-restored";
  private static final int    TARGET_MB     = Integer.parseInt(System.getProperty("arcadedb.backup.benchmark.sizeMB", "1024"));
  private static final int    PAYLOAD_SIZE  = 512;

  private record Measurement(String label, long elapsedMs, long archiveBytes, long peakHeapBytes, long cpuNanos) {
  }

  @BeforeAll
  static void buildDatabase() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));

    final long begin = System.currentTimeMillis();
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.transaction(() -> {
        final VertexType type = database.getSchema().createVertexType("Doc");
        type.createProperty("id", Type.LONG);
        type.createProperty("payload", Type.STRING);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      });

      final long records = (long) TARGET_MB * 1024 * 1024 / (PAYLOAD_SIZE + 64);
      final Random random = new Random(17);
      long id = 0;
      while (id < records) {
        final long from = id;
        database.transaction(() -> {
          for (int i = 0; i < 5_000; i++)
            database.newVertex("Doc").set("id", from + i).set("payload", payload(random)).save();
        });
        id += 5_000;
      }
    }
    System.out.printf("[backup-benchmark] built a %s database in %,d ms%n",
        FileUtils.getSizeAsString(directorySize(new File(DATABASE_PATH))), System.currentTimeMillis() - begin);
  }

  @AfterAll
  static void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * Wall-clock duration, archive size, CPU and peak heap for the old configuration (single-threaded level 9) against
   * the new default and against the thread-count sweep the default sizing has to be chosen from.
   */
  @Test
  void compressionLevelAndThreadSweep() throws Exception {
    final long databaseSize = directorySize(new File(DATABASE_PATH));
    final List<Measurement> measurements = new ArrayList<>();

    try (final Database database = new DatabaseFactory(DATABASE_PATH).open()) {
      // WARM UP THE PAGE CACHE AND THE JIT SO THE FIRST CONFIGURATION IS NOT PENALISED
      measure(database, "warmup", 1, 4);

      measurements.add(measure(database, "legacy writer, level 9 (previous default)", 9, 0));
      measurements.add(measure(database, "legacy writer, level 6", 6, 0));
      measurements.add(measure(database, "legacy writer, level 1", 1, 0));

      for (final int threads : new int[] { 1, 2, 4, 8 })
        for (final int level : new int[] { 1, 6, 9 })
          measurements.add(measure(database, "parallel writer, level %d, %d threads".formatted(level, threads), level, threads));
    }

    report(databaseSize, measurements);

    // RESTORE MUST NOT REGRESS. THE RESTORE PATH IS UNCHANGED, BUT A LEVEL-1 ARCHIVE IS BIGGER AND INFLATES
    // DIFFERENTLY, SO BOTH ENDS OF THE TRADE ARE TIMED
    try (final Database database = new DatabaseFactory(DATABASE_PATH).open()) {
      System.out.printf("[backup-benchmark] restore from a legacy level-9 archive: %,d ms%n",
          timeRestore(database, 9, 0));
      System.out.printf("[backup-benchmark] restore from a parallel level-1 archive: %,d ms%n",
          timeRestore(database, 1, 8));
    }
  }

  private static long timeRestore(final Database database, final int level, final int threads) throws Exception {
    new File(BACKUP_FILE).delete();
    new Backup(database, BACKUP_FILE).setVerboseLevel(0).setCompressionLevel(level).setCompressionThreads(threads)
        .backupDatabase();

    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    final long begin = System.currentTimeMillis();
    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    return System.currentTimeMillis() - begin;
  }

  /**
   * The number that matters most to users: what a running backup does to sustained insert throughput and to commit
   * latency, measured with and without a backup in flight, plus the deferred-RAM high-water mark that says how close
   * the writers came to being throttled outright.
   */
  @Test
  void concurrentWriterImpact() throws Exception {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).open()) {
      System.out.println("[backup-benchmark] " + runWriterLoad(database, "no backup running", null, 0));
      System.out.println("[backup-benchmark] " + runWriterLoad(database, "legacy writer, level 9", 9, 0));
      System.out.println("[backup-benchmark] " + runWriterLoad(database, "parallel writer, level 9, 8 threads", 9, 8));
      System.out.println("[backup-benchmark] " + runWriterLoad(database, "parallel writer, level 1, auto threads", 1, -1));
    }
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static Measurement measure(final Database database, final String label, final int level, final int threads)
      throws Exception {
    new File(BACKUP_FILE).delete();
    System.gc();

    final HeapSampler sampler = new HeapSampler();
    sampler.start();
    final long cpuBegin = processCpuNanos();
    final long begin = System.currentTimeMillis();

    new Backup(database, BACKUP_FILE).setVerboseLevel(0).setCompressionLevel(level).setCompressionThreads(threads)
        .backupDatabase();

    final long elapsed = System.currentTimeMillis() - begin;
    final long cpu = processCpuNanos() - cpuBegin;
    sampler.stop();

    return new Measurement(label, elapsed, new File(BACKUP_FILE).length(), sampler.peak(), cpu);
  }

  private static void report(final long databaseSize, final List<Measurement> measurements) {
    final Measurement reference = measurements.get(0);
    System.out.printf("%n[backup-benchmark] database on disk: %s%n", FileUtils.getSizeAsString(databaseSize));
    System.out.printf("%-46s %10s %10s %10s %10s %10s %10s%n", "configuration", "seconds", "MB/s", "archive", "ratio",
        "vs base", "peak heap");
    for (final Measurement m : measurements) {
      final double seconds = m.elapsedMs() / 1000.0;
      System.out.printf("%-46s %10.2f %10.1f %10s %9.1f%% %9.2fx %10s%n", m.label(), seconds,
          seconds > 0 ? databaseSize / 1024.0 / 1024.0 / seconds : 0,
          FileUtils.getSizeAsString(m.archiveBytes()), 100.0 * m.archiveBytes() / databaseSize,
          m.elapsedMs() > 0 ? reference.elapsedMs() / (double) m.elapsedMs() : 0,
          FileUtils.getSizeAsString(m.peakHeapBytes()));
    }
    System.out.println("[backup-benchmark] CPU seconds consumed by each configuration:");
    for (final Measurement m : measurements)
      System.out.printf("  %-46s %8.2f cpu-s (%.1f cores)%n", m.label(), m.cpuNanos() / 1e9,
          m.elapsedMs() > 0 ? m.cpuNanos() / 1e6 / m.elapsedMs() : 0);
  }

  /**
   * Runs a sustained insert load and reports what happened <b>only inside the measurement window</b>. Measuring the
   * whole run instead would dilute the impact away: the level-9 backup takes far longer than the level-1 one, so a
   * fixed-duration window would compare a load that spent 60% of its time next to a backup against one that spent
   * 5% of its time next to it, and both would look the same.
   *
   * @param level   deflate level of the backup to run inside the window, or {@code null} for the no-backup baseline.
   * @param threads compression threads of that backup.
   */
  private static String runWriterLoad(final Database database, final String label, final Integer level,
      final int threads) throws Exception {
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong peakDeferredRAM = new AtomicLong();
    final List<long[]> samples = new ArrayList<>();
    final AtomicLong nextId = new AtomicLong(System.nanoTime());

    final Thread writer = new Thread(() -> {
      final Random random = new Random(31);
      while (!stop.get()) {
        final long begin = System.nanoTime();
        final long from = nextId.getAndAdd(200);
        database.transaction(() -> {
          for (int i = 0; i < 200; i++)
            database.newVertex("Doc").set("id", from + i).set("payload", payload(random)).save();
        });
        final long end = System.nanoTime();
        synchronized (samples) {
          samples.add(new long[] { end, (end - begin) / 1000 });
        }
      }
    }, "backup-benchmark-writer");

    final Thread sampler = new Thread(() -> {
      final DatabaseInternal internal = (DatabaseInternal) database;
      while (!stop.get()) {
        peakDeferredRAM.accumulateAndGet(internal.getPageManager().getDeferredRAMBytes(), Math::max);
        try {
          Thread.sleep(20);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }, "backup-benchmark-sampler");
    sampler.setDaemon(true);

    final long windowBegin;
    final long windowEnd;
    writer.start();
    sampler.start();
    try {
      // LET THE LOAD REACH STEADY STATE BEFORE THE WINDOW OPENS
      Thread.sleep(2_000);
      windowBegin = System.nanoTime();
      if (level != null)
        new Backup(database, BACKUP_FILE).setVerboseLevel(0).setCompressionLevel(level).setCompressionThreads(threads)
            .backupDatabase();
      else
        Thread.sleep(10_000);
      windowEnd = System.nanoTime();
    } finally {
      stop.set(true);
      writer.join();
      sampler.join();
      new File(BACKUP_FILE).delete();
    }

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
    return "%-42s window=%5.1fs commits=%,6d (%,7.0f rec/s) p50=%,7dus p95=%,8dus p99=%,8dus peakDeferredRAM=%s".formatted(
        label, windowSeconds, sorted.length, sorted.length * 200 / windowSeconds, percentile(sorted, 50),
        percentile(sorted, 95), percentile(sorted, 99), FileUtils.getSizeAsString(peakDeferredRAM.get()));
  }

  private static long percentile(final long[] sorted, final int percentile) {
    if (sorted.length == 0)
      return 0;
    return sorted[Math.min(sorted.length - 1, (int) ((long) sorted.length * percentile / 100))];
  }

  private static final class HeapSampler {
    private final AtomicLong    peak    = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private       Thread        thread;

    void start() {
      thread = new Thread(() -> {
        while (running.get()) {
          peak.accumulateAndGet(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
          try {
            Thread.sleep(25);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }, "backup-benchmark-heap-sampler");
      thread.setDaemon(true);
      thread.start();
    }

    void stop() throws InterruptedException {
      running.set(false);
      thread.join();
    }

    long peak() {
      return peak.get();
    }
  }

  private static long processCpuNanos() {
    final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
    // THE PROCESS CPU TIME IS ONLY ON THE HOTSPOT-SPECIFIC SUBINTERFACE, SO A NON-HOTSPOT JVM REPORTS 0 RATHER THAN
    // FAILING THE BENCHMARK - THE WALL-CLOCK AND ARCHIVE-SIZE COLUMNS ARE THE ONES THE DECISION RESTS ON
    if (bean instanceof com.sun.management.OperatingSystemMXBean hotspot)
      return hotspot.getProcessCpuTime();
    return 0;
  }

  private static String payload(final Random random) {
    final StringBuilder builder = new StringBuilder(PAYLOAD_SIZE);
    // HALF STRUCTURED, HALF RANDOM: PAGES THAT COMPRESS LIKE REAL DATA RATHER THAN LIKE A BEST OR WORST CASE
    while (builder.length() < PAYLOAD_SIZE / 2)
      builder.append("field").append(builder.length()).append("=value;");
    while (builder.length() < PAYLOAD_SIZE)
      builder.append((char) ('a' + random.nextInt(26)));
    return builder.toString();
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
