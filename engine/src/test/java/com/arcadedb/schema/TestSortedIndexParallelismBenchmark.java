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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Sorted-only stage benchmark used to select and validate bounded parallel build work. */
@Tag("benchmark")
class TestSortedIndexParallelismBenchmark {
  private static final String ENTRY_COUNT_PROPERTY = "arcadedb.sortedParallelBenchmark.entries";
  private static final String BUCKET_COUNT_PROPERTY = "arcadedb.sortedParallelBenchmark.buckets";
  private static final String MEMORY_MIB_PROPERTY = "arcadedb.sortedParallelBenchmark.memoryMiB";
  private static final String MERGE_FAN_IN_PROPERTY = "arcadedb.sortedParallelBenchmark.mergeFanIn";

  private static final int DEFAULT_ENTRY_COUNT = 1_000_000;
  private static final int DEFAULT_BUCKET_COUNT = 8;
  private static final int DEFAULT_MEMORY_MIB = 64;
  private static final int DEFAULT_MERGE_FAN_IN = 8;
  private static final int INSERT_BATCH_SIZE = 10_000;
  private static final String TYPE_NAME = "ParallelBenchmarkRecord";
  private static final String PROPERTY_NAME = "lookupKey";
  private static final Path BENCHMARK_ROOT = Path.of("target", "databases", "SortedIndexParallelismBenchmark");

  @Test
  void measureSerialSortedBuildStages() throws Exception {
    final int entries = Integer.getInteger(ENTRY_COUNT_PROPERTY, DEFAULT_ENTRY_COUNT);
    final int buckets = Integer.getInteger(BUCKET_COUNT_PROPERTY, DEFAULT_BUCKET_COUNT);
    final int memoryMiB = Integer.getInteger(MEMORY_MIB_PROPERTY, DEFAULT_MEMORY_MIB);
    final int mergeFanIn = Integer.getInteger(MERGE_FAN_IN_PROPERTY, DEFAULT_MERGE_FAN_IN);
    if (entries < 1 || buckets < 1 || memoryMiB < 1 || mergeFanIn < 2)
      throw new IllegalArgumentException("Benchmark entries, buckets, and memory must be positive; fan-in must be at least 2");

    FileUtils.deleteRecursively(BENCHMARK_ROOT.toFile());
    try {
      final Path sourcePath = BENCHMARK_ROOT.resolve("source");
      createSourceDatabase(sourcePath, entries, buckets);
      final Path buildPath = BENCHMARK_ROOT.resolve("serial");
      FileUtils.copyDirectory(sourcePath.toFile(), buildPath.toFile());

      final BuildResult result = buildAndValidate(buildPath, entries, memoryMiB, mergeFanIn);
      System.out.printf(Locale.ROOT,
          "%nSorted LSM parallelism serial baseline%n" +
              "entries: %,d%n" +
              "buckets: %,d%n" +
              "memory budget: %,d MiB%n" +
              "merge fan-in: %,d%n" +
              "index phase: %.3f s%n" +
              "ascending digest: %s%n" +
              "descending digest: %s%n" +
              "metrics: %s%n%n",
          entries, buckets, memoryMiB, mergeFanIn, result.buildNanos() / 1_000_000_000D,
          result.ascendingDigest(), result.descendingDigest(), result.metrics().toJSON());
    } finally {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
      FileUtils.deleteRecursively(BENCHMARK_ROOT.toFile());
    }
  }

  private void createSourceDatabase(final Path path, final int entries, final int buckets) {
    try (DatabaseFactory factory = new DatabaseFactory(path.toString()); Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME)
          .withTotalBuckets(buckets).create();
      type.createProperty(PROPERTY_NAME, Type.STRING);
      final int stride = permutationStride(entries);

      for (int from = 0; from < entries; from += INSERT_BATCH_SIZE) {
        final int batchStart = from;
        final int batchEnd = Math.min(entries, from + INSERT_BATCH_SIZE);
        database.transaction(() -> {
          for (int i = batchStart; i < batchEnd; i++) {
            final int permuted = (int) Math.floorMod((long) i * stride, entries);
            database.newDocument(TYPE_NAME).set(PROPERTY_NAME, key(permuted)).save();
          }
        });
      }
    }
  }

  private BuildResult buildAndValidate(final Path path, final int expectedEntries, final int memoryMiB,
      final int mergeFanIn) throws Exception {
    final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();
    final long buildNanos;
    final String ascendingDigest;
    final String descendingDigest;
    try (DatabaseFactory factory = new DatabaseFactory(path.toString()); Database database = factory.open()) {
      TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
      final TypeIndex index;
      try {
        final long started = System.nanoTime();
        index = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { PROPERTY_NAME })
            .withType(Schema.INDEX_TYPE.LSM_TREE)
            .withBuildMode(IndexBuildMode.SORTED)
            .withBuildMemoryBudget((long) memoryMiB << 20)
            .withBuildMergeFanIn(mergeFanIn)
            .withUnique(true)
            .create();
        buildNanos = System.nanoTime() - started;
      } finally {
        TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
      }

      assertThat(captured.get()).isNotNull();
      assertThat(captured.get().scannedRecords()).isEqualTo(expectedEntries);
      assertThat(count(index.get(new Object[] { key(expectedEntries / 2) }))).isEqualTo(1);
      ascendingDigest = digest(index.iterator(true), expectedEntries);
      descendingDigest = digest(index.iterator(false), expectedEntries);
    }

    try (DatabaseFactory factory = new DatabaseFactory(path.toString()); Database database = factory.open()) {
      final TypeIndex index = database.getSchema().getType(TYPE_NAME).getIndexByProperties(PROPERTY_NAME);
      assertThat(digest(index.iterator(true), expectedEntries)).isEqualTo(ascendingDigest);
      assertThat(digest(index.iterator(false), expectedEntries)).isEqualTo(descendingDigest);
    }
    return new BuildResult(buildNanos, ascendingDigest, descendingDigest, captured.get());
  }

  private static String digest(final IndexCursor cursor, final int expectedEntries) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    int count = 0;
    try {
      while (cursor.hasNext()) {
        final Identifiable record = cursor.next();
        digest.update(cursor.getKeys()[0].toString().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(record.getIdentity().toString().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
        count++;
      }
    } finally {
      cursor.close();
    }
    assertThat(count).isEqualTo(expectedEntries);
    return HexFormat.of().formatHex(digest.digest());
  }

  private static long count(final IndexCursor cursor) {
    long count = 0L;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      return count;
    } finally {
      cursor.close();
    }
  }

  private static String key(final int value) {
    return "key-%09d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".formatted(value);
  }

  private static int permutationStride(final int entries) {
    if (entries == 1)
      return 1;

    int candidate = Math.max(1, (int) (entries * 0.6180339887498949));
    while (greatestCommonDivisor(candidate, entries) != 1)
      --candidate;
    return candidate;
  }

  private static int greatestCommonDivisor(int left, int right) {
    while (right != 0) {
      final int remainder = left % right;
      left = right;
      right = remainder;
    }
    return left;
  }

  private record BuildResult(long buildNanos, String ascendingDigest, String descendingDigest,
                             SortedIndexBuildMetrics metrics) {
  }
}
