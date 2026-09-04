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
package com.arcadedb.integration.restore;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.backup.Backup;
import com.arcadedb.integration.restore.format.ParallelZipExtractor;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * Before/after measurements required by issue #6086, in the same shape as the #6072 backup table so the two can be
 * read together. Two questions have to be answered with numbers rather than intuition:
 * <ol>
 *   <li><b>Is the restore inflate-bound or write-bound?</b> {@link #whereTheRestoreSpendsItsTime()} answers it by
 *       timing the two halves separately - inflating every entry and throwing the bytes away, then writing the same
 *       number of bytes with no inflation - against the whole restore.</li>
 *   <li><b>What does the parallelism buy, and at what heap?</b> {@link #restoreThreadSweep()} times the pre-#6086
 *       path (a single thread and an 8 KB buffer, reimplemented here so "before" is measured rather than
 *       remembered), the same walk with the larger buffer, and the parallel path from 1 to 8 threads.</li>
 * </ol>
 * Excluded from the normal build ({@code @Tag("benchmark")}). Run it explicitly:
 * <pre>
 *   mvn -o -pl integration -am test -Dtest=RestoreParallelBenchmark -Darcadedb.restore.benchmark.sizesMB=256,1024
 * </pre>
 */
@Tag("benchmark")
class RestoreParallelBenchmark {
  private static final String DATABASE_PATH = "target/databases/restore-benchmark";
  private static final String RESTORED_PATH = "target/databases/restore-benchmark-restored";
  private static final String BACKUP_FILE   = "target/restore-benchmark.zip";
  private static final String LEGACY_FILE   = "target/restore-benchmark-legacy.zip";
  private static final int    PAYLOAD_SIZE  = 512;
  private static final int    LEGACY_BUFFER = 8192;

  private record Measurement(String label, long elapsedMs, long peakHeapBytes) {
  }

  @Test
  void restoreThreadSweep() throws Exception {
    for (final int types : types())
      for (final int sizeMB : sizes()) {
        final long databaseSize = buildFixture(sizeMB, types);
        final List<Measurement> measurements = new ArrayList<>();

        // WARM THE PAGE CACHE AND THE JIT SO THE FIRST CONFIGURATION IS NOT PENALISED
        measure("warmup", () -> sequentialRestore(new File(BACKUP_FILE), LEGACY_BUFFER, false));

        measurements.add(measure("sequential, 8KB buffer, unbuffered read (before #6086)",
            () -> sequentialRestore(new File(BACKUP_FILE), LEGACY_BUFFER, false)));
        measurements.add(measure("sequential, 8KB buffer, buffered read",
            () -> sequentialRestore(new File(BACKUP_FILE), LEGACY_BUFFER, true)));
        measurements.add(measure("sequential, 256KB buffer, buffered read",
            () -> sequentialRestore(new File(BACKUP_FILE), ParallelZipExtractor.BUFFER_SIZE, true)));
        measurements.add(measure("restore, threads=0 (sequential path)", () -> restore(BACKUP_FILE, 0)));

        for (final int threads : new int[] { 1, 2, 4, 8 })
          measurements.add(measure("restore, threads=" + threads, () -> restore(BACKUP_FILE, threads)));

        measurements.add(measure("legacy level-9 archive, threads=0", () -> restore(LEGACY_FILE, 0)));
        measurements.add(measure("legacy level-9 archive, threads=8", () -> restore(LEGACY_FILE, 8)));

        report(sizeMB, types, databaseSize, measurements);
      }
  }

  /**
   * The measurement the issue asks for first, because the two answers point at different fixes: if the restore is
   * write-bound the buffer size is most of the win, and if it is inflate-bound only real parallelism helps.
   */
  @Test
  void whereTheRestoreSpendsItsTime() throws Exception {
    for (final int sizeMB : sizes()) {
      final long databaseSize = buildFixture(sizeMB, 1);

      final long inflateOnly = time(() -> inflateEverything(new File(BACKUP_FILE)));
      final long writeOnly = time(() -> writeEverything(new File(BACKUP_FILE)));
      final long whole = time(() -> sequentialRestore(new File(BACKUP_FILE), LEGACY_BUFFER, false));

      System.out.printf("%n[restore-benchmark] %,d MB target, %s on disk: inflate-only %,d ms, write-only %,d ms, "
              + "whole sequential restore %,d ms (sum of the halves %,d ms)%n", sizeMB,
          FileUtils.getSizeAsString(databaseSize), inflateOnly, writeOnly, whole, inflateOnly + writeOnly);
    }
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static int[] sizes() {
    return intList("arcadedb.restore.benchmark.sizesMB", "256,1024");
  }

  /**
   * How many vertex types the fixture is spread over, which is what decides how much the per-entry parallelism can
   * possibly buy: one type is one dominant archive entry and therefore the worst case for it, several types are
   * several entries of comparable size and therefore the case it was built for. Both are real database shapes and the
   * numbers are only meaningful when both are reported.
   */
  private static int[] types() {
    return intList("arcadedb.restore.benchmark.types", "1,8");
  }

  private static int[] intList(final String property, final String fallback) {
    final String[] parts = System.getProperty(property, fallback).split(",");
    final int[] values = new int[parts.length];
    for (int i = 0; i < parts.length; i++)
      values[i] = Integer.parseInt(parts[i].trim());
    return values;
  }

  /**
   * Builds a database of roughly {@code sizeMB} spread over {@code types} vertex types, its default archive and a
   * pre-#6072 one, and returns its size on disk.
   */
  private static long buildFixture(final int sizeMB, final int types) throws Exception {
    clean();

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.transaction(() -> {
        for (int t = 0; t < types; t++) {
          final VertexType type = database.getSchema().createVertexType("Doc" + t);
          type.createProperty("id", Type.LONG);
          type.createProperty("payload", Type.STRING);
          type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
        }
      });

      final long records = (long) sizeMB * 1024 * 1024 / (PAYLOAD_SIZE + 64);
      final Random random = new Random(17);
      long id = 0;
      while (id < records) {
        final long from = id;
        database.transaction(() -> {
          for (int i = 0; i < 5_000; i++)
            database.newVertex("Doc" + ((from + i) % types)).set("id", from + i).set("payload", payload(random)).save();
        });
        id += 5_000;
      }

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
      new Backup(database, LEGACY_FILE).setVerboseLevel(0).setCompressionThreads(0).setCompressionLevel(9)
          .backupDatabase();
    }

    return directorySize(new File(DATABASE_PATH));
  }

  private static void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
    new File(LEGACY_FILE).delete();
  }

  private interface Work {
    void run() throws Exception;
  }

  private static void restore(final String archive, final int threads) {
    new Restore(archive, RESTORED_PATH).setVerboseLevel(0).setRestoreThreads(threads).restoreDatabase();
  }

  private static Measurement measure(final String label, final Work work) throws Exception {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    System.gc();

    final HeapSampler sampler = new HeapSampler();
    sampler.start();
    final long elapsed = time(work);
    sampler.stop();

    return new Measurement(label, elapsed, sampler.peak());
  }

  private static long time(final Work work) throws Exception {
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    final long begin = System.currentTimeMillis();
    work.run();
    return System.currentTimeMillis() - begin;
  }

  /**
   * The pre-#6086 restore, reimplemented so the "before" column is a measurement and not a memory: one thread
   * alternating between inflating into a fixed buffer and writing it out. {@code bufferedRead} controls the other
   * half of the story - {@code ZipInputStream} fills its inflater 512 bytes at a time, so whether there is a buffer
   * under it matters more than how big the copy buffer above it is.
   */
  private static void sequentialRestore(final File archive, final int bufferSize, final boolean bufferedRead)
      throws IOException {
    final File directory = new File(RESTORED_PATH);
    directory.mkdirs();

    final InputStream source = bufferedRead ?
        new BufferedInputStream(new FileInputStream(archive), ParallelZipExtractor.BUFFER_SIZE) :
        new FileInputStream(archive);

    final byte[] buffer = new byte[bufferSize];
    try (final ZipInputStream zip = new ZipInputStream(source)) {
      ZipEntry entry = zip.getNextEntry();
      while (entry != null) {
        try (final OutputStream out = new FileOutputStream(new File(directory, entry.getName()))) {
          int len;
          while ((len = zip.read(buffer)) > 0)
            out.write(buffer, 0, len);
        }
        entry = zip.getNextEntry();
      }
    }
  }

  /** Inflates every entry and throws the bytes away: the inflate half of the restore, with no writing at all. */
  private static long inflateEverything(final File archive) throws IOException {
    final byte[] buffer = new byte[LEGACY_BUFFER];
    long total = 0;
    try (final ZipInputStream zip = new ZipInputStream(new FileInputStream(archive))) {
      ZipEntry entry = zip.getNextEntry();
      while (entry != null) {
        int len;
        while ((len = zip.read(buffer)) > 0)
          total += len;
        entry = zip.getNextEntry();
      }
    }
    return total;
  }

  /**
   * The write half of the restore with no inflation at all: the same files, of the same sizes, written in the same
   * 8 KB units, but fed from one already-resident buffer instead of from a {@code ZipInputStream}. Holding a whole
   * entry in heap to write its real bytes would be the wrong measurement anyway - what is being timed here is the
   * filesystem, and the filesystem does not care what the bytes are. Entry sizes come from the central directory,
   * which is why this one uses {@link ZipFile}: reading them is free there.
   */
  private static void writeEverything(final File archive) throws IOException {
    final File directory = new File(RESTORED_PATH);
    directory.mkdirs();

    try (final ZipFile zip = new ZipFile(archive)) {
      final byte[] buffer = new byte[LEGACY_BUFFER];
      final Enumeration<? extends ZipEntry> entries = zip.entries();
      while (entries.hasMoreElements()) {
        final ZipEntry entry = entries.nextElement();
        long remaining = entry.getSize();
        if (remaining < 0)
          try (final InputStream in = zip.getInputStream(entry)) {
            remaining = in.readAllBytes().length;
          }
        try (final OutputStream out = new FileOutputStream(new File(directory, entry.getName()))) {
          while (remaining > 0) {
            final int chunk = (int) Math.min(buffer.length, remaining);
            out.write(buffer, 0, chunk);
            remaining -= chunk;
          }
        }
      }
    }
  }

  private static void report(final int sizeMB, final int types, final long databaseSize,
      final List<Measurement> measurements) {
    final Measurement reference = measurements.get(0);
    System.out.printf("%n[restore-benchmark] %,d MB target over %d type(s), database on disk: %s%n", sizeMB, types,
        FileUtils.getSizeAsString(databaseSize));
    System.out.printf("%-46s %10s %10s %10s %10s%n", "configuration", "seconds", "MB/s", "vs before", "peak heap");
    for (final Measurement m : measurements) {
      final double seconds = m.elapsedMs() / 1000.0;
      System.out.printf("%-46s %10.2f %10.1f %9.2fx %10s%n", m.label(), seconds,
          seconds > 0 ? databaseSize / 1024.0 / 1024.0 / seconds : 0,
          m.elapsedMs() > 0 ? reference.elapsedMs() / (double) m.elapsedMs() : 0,
          FileUtils.getSizeAsString(m.peakHeapBytes()));
    }
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
      }, "restore-benchmark-heap-sampler");
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
