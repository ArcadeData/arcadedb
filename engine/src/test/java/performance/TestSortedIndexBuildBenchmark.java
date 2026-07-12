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
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexBuildMode;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compares the normal record-at-a-time LSM index build with the bounded sorted build over identical physical input.
 * Both the raw normal-build time and its subsequent compaction are reported because the sorted build already returns
 * compacted output. The sorted build runs first so the normal path receives any JVM warmup advantage. Dataset creation,
 * copying, and validation are excluded from the reported index timings.
 * <p>
 * Run explicitly with:
 * {@code JAVA_TOOL_OPTIONS=-Xmx4g ./mvnw -pl engine -Dtest=TestSortedIndexBuildBenchmark -Dgroups=benchmark test}.
 * Override the deterministic ten-million-record default with
 * {@code -Darcadedb.sortedBuildBenchmark.entries=5000000}.
 */
@Tag("benchmark")
class TestSortedIndexBuildBenchmark {
  private static final String ENTRY_COUNT_PROPERTY    = "arcadedb.sortedBuildBenchmark.entries";
  private static final String VALUES_PER_KEY_PROPERTY = "arcadedb.sortedBuildBenchmark.valuesPerKey";
  private static final int    DEFAULT_ENTRY_COUNT     = 10_000_000;
  private static final int    DEFAULT_VALUES_PER_KEY  = 1;
  private static final int    INSERT_BATCH_SIZE       = 10_000;
  private static final long   SORTED_MEMORY_BUDGET    = 1L << 30;
  private static final int    SORTED_MERGE_FAN_IN     = 8;
  private static final String TYPE_NAME               = "BenchmarkRecord";
  private static final String PROPERTY_NAME           = "lookupKey";
  private static final Path   BENCHMARK_ROOT          = Path.of("target", "databases", "SortedIndexBuildBenchmark");

  @Test
  void compareDefaultAndSortedBuilds() throws Exception {
    final int entries = Integer.getInteger(ENTRY_COUNT_PROPERTY, DEFAULT_ENTRY_COUNT);
    final int valuesPerKey = Integer.getInteger(VALUES_PER_KEY_PROPERTY, DEFAULT_VALUES_PER_KEY);
    if (entries < 1)
      throw new IllegalArgumentException(ENTRY_COUNT_PROPERTY + " must be positive");
    if (valuesPerKey < 1 || valuesPerKey > entries)
      throw new IllegalArgumentException(VALUES_PER_KEY_PROPERTY + " must be between 1 and the entry count");

    FileUtils.deleteRecursively(BENCHMARK_ROOT.toFile());
    try {
      final Path sourcePath = BENCHMARK_ROOT.resolve("source");
      createSourceDatabase(sourcePath, entries, valuesPerKey);

      // Run SORTED first so DEFAULT, the slower control, receives any shared JVM warmup advantage.
      final Path sortedPath = BENCHMARK_ROOT.resolve("sorted");
      FileUtils.copyDirectory(sourcePath.toFile(), sortedPath.toFile());
      final BuildResult sorted = buildAndMeasure(sortedPath, IndexBuildMode.SORTED, entries, valuesPerKey);

      final Path defaultPath = BENCHMARK_ROOT.resolve("default");
      FileUtils.copyDirectory(sourcePath.toFile(), defaultPath.toFile());
      final BuildResult normal = buildAndMeasure(defaultPath, IndexBuildMode.DEFAULT, entries, valuesPerKey);

      final double rawBuildRatio = normal.buildNanos() / (double) sorted.buildNanos();
      final double compactedSpeedup = normal.totalToCompactedNanos() / (double) sorted.totalToCompactedNanos();
      System.out.printf(Locale.ROOT,
          "%nSorted LSM index build benchmark%n" +
              "entries: %,d%n" +
              "distinct keys: %,d%n" +
              "values per key: %,d%n" +
              "permutation stride: %,d%n" +
              "max heap: %.2f GiB%n" +
              "DEFAULT build: %.3f s%n" +
              "DEFAULT compaction: %.3f s%n" +
              "DEFAULT total to compacted output: %.3f s%n" +
              "SORTED build to compacted output: %.3f s%n" +
              "raw DEFAULT/SORTED build ratio: %.2fx%n" +
              "compacted-output speedup: %.2fx%n%n",
          entries, (entries + valuesPerKey - 1L) / valuesPerKey, valuesPerKey, permutationStride(entries),
          Runtime.getRuntime().maxMemory() / (double) (1L << 30), seconds(normal.buildNanos()),
          seconds(normal.compactionNanos()), seconds(normal.totalToCompactedNanos()),
          seconds(sorted.totalToCompactedNanos()), rawBuildRatio, compactedSpeedup);

      assertThat(sorted.entries()).isEqualTo(entries);
      assertThat(normal.entries()).isEqualTo(entries);
      if (entries >= DEFAULT_ENTRY_COUNT)
        assertThat(sorted.totalToCompactedNanos())
            .as("SORTED should reach equivalent compacted output before DEFAULT at %,d entries", entries)
            .isLessThan(normal.totalToCompactedNanos());
    } finally {
      FileUtils.deleteRecursively(BENCHMARK_ROOT.toFile());
    }
  }

  private void createSourceDatabase(final Path path, final int entries, final int valuesPerKey) {
    try (DatabaseFactory factory = new DatabaseFactory(path.toString()); Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty(PROPERTY_NAME, Type.STRING);
      final int stride = permutationStride(entries);

      for (int from = 0; from < entries; from += INSERT_BATCH_SIZE) {
        final int batchStart = from;
        final int batchEnd = Math.min(entries, from + INSERT_BATCH_SIZE);
        database.transaction(() -> {
          for (int i = batchStart; i < batchEnd; i++) {
            final int permuted = (int) Math.floorMod((long) i * stride, entries);
            database.newDocument(TYPE_NAME).set(PROPERTY_NAME, key(permuted / valuesPerKey)).save();
          }
        });
      }
    }
  }

  private BuildResult buildAndMeasure(final Path path, final IndexBuildMode mode, final int expectedEntries,
      final int valuesPerKey) throws Exception {
    try (DatabaseFactory factory = new DatabaseFactory(path.toString()); Database database = factory.open()) {
      final AtomicLong scannedEntries = new AtomicLong();
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { PROPERTY_NAME });
      builder.withType(Schema.INDEX_TYPE.LSM_TREE);
      builder.withBuildMode(mode);
      builder.withUnique(false);
      builder.withCallback((document, total) -> scannedEntries.set(total));
      if (mode == IndexBuildMode.SORTED)
        builder.withBuildMemoryBudget(SORTED_MEMORY_BUDGET).withBuildMergeFanIn(SORTED_MERGE_FAN_IN);

      final long started = System.nanoTime();
      final TypeIndex index = builder.create();
      final long buildNanos = System.nanoTime() - started;

      assertThat(scannedEntries.get()).as("%s scanned entry count", mode).isEqualTo(expectedEntries);

      long compactionNanos = 0L;
      if (mode == IndexBuildMode.DEFAULT) {
        database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
        database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1_024);
        final long compactionStarted = System.nanoTime();
        assertThat(index.scheduleCompaction()).as("DEFAULT index is eligible for compaction").isTrue();
        assertThat(index.compact()).as("DEFAULT index compaction completed").isTrue();
        compactionNanos = System.nanoTime() - compactionStarted;
      }

      final LSMTreeIndex bucketIndex = (LSMTreeIndex) index.getIndexesOnBuckets()[0];
      assertThat(bucketIndex.getMutableIndex().getSubIndex()).as("%s compacted output", mode).isNotNull();
      assertThat(count(index.get(new Object[] { key(0) }))).as("%s point lookup", mode).isEqualTo(valuesPerKey);
      return new BuildResult(buildNanos, compactionNanos, scannedEntries.get());
    }
  }

  private static long count(final IndexCursor cursor) {
    long count = 0L;
    while (cursor.hasNext()) {
      cursor.next();
      count++;
    }
    return count;
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

  private static double seconds(final long nanos) {
    return nanos / 1_000_000_000.0;
  }

  private record BuildResult(long buildNanos, long compactionNanos, long entries) {
    long totalToCompactedNanos() {
      return buildNanos + compactionNanos;
    }
  }
}
