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
import com.arcadedb.engine.PageManager;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the per-series bloom filters of issue #5517 are actually worth on a point lookup, measured against the same
 * database files with the filters consulted and ignored.
 * <p>
 * <b>The measurement has to be run in two cache regimes, or it lies.</b> The filters do not save instructions worth
 * counting - they save PAGE READS, and a page read only costs anything when the page is not resident. With a cache
 * large enough to hold the whole index the filters replace a cached read with a cached read and the speedup collapses
 * to almost nothing; with a cache smaller than the index - which is every bulk load worth optimising - they replace a
 * disk read with a read of a structure more than an order of magnitude smaller, which is the whole point. Reporting
 * only one of the two would be a number chosen to flatter.
 * <p>
 * The workload is the one that motivated the issue: a UNIQUE index during a bulk load, where the duplicate check for
 * every incoming record misses in every compacted series by definition. Lookups of keys that ARE present are measured
 * too, because that is the case a filter cannot help and must not hurt.
 * <p>
 * Run with:
 * {@code ./mvnw -pl engine -Dtest=LSMTreeBloomFilterBenchmark -Dgroups=benchmark test}
 * <p>
 * Knobs (all optional):
 * {@code -Darcadedb.bloomBenchmark.entries=1000000}
 * {@code -Darcadedb.bloomBenchmark.lookups=200000}
 * {@code -Darcadedb.bloomBenchmark.smallCacheMB=32}
 * {@code -Darcadedb.bloomBenchmark.largeCacheMB=4096}
 * {@code -Darcadedb.bloomBenchmark.compactionRamMB=8}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class LSMTreeBloomFilterBenchmark {
  private static final String ENTRIES_PROPERTY         = "arcadedb.bloomBenchmark.entries";
  private static final String LOOKUPS_PROPERTY         = "arcadedb.bloomBenchmark.lookups";
  private static final String SMALL_CACHE_PROPERTY     = "arcadedb.bloomBenchmark.smallCacheMB";
  private static final String LARGE_CACHE_PROPERTY     = "arcadedb.bloomBenchmark.largeCacheMB";
  private static final String COMPACTION_RAM_PROPERTY  = "arcadedb.bloomBenchmark.compactionRamMB";

  private static final int DEFAULT_ENTRIES        = 2_000_000;
  private static final int DEFAULT_LOOKUPS        = 200_000;
  private static final int DEFAULT_SMALL_CACHE_MB = 16;
  private static final int DEFAULT_LARGE_CACHE_MB = 4_096;
  private static final int DEFAULT_COMPACTION_RAM = 8;

  private static final int    INSERT_BATCH  = 20_000;
  private static final int    WARMUP        = 20_000;
  private static final String TYPE_NAME     = "BloomBenchmarkRecord";
  private static final String PROPERTY_NAME = "email";
  private static final Path   ROOT          = Path.of("target", "databases", "LSMTreeBloomFilterBenchmark");

  /**
   * Present keys take the EVEN slots of the key space and absent keys the odd ones, so an absent key always falls
   * BETWEEN two stored keys.
   * <p>
   * This is the whole experiment. A key outside a series' range is already ruled out by its root page, for free and
   * without the filters, so absent keys drawn from their own prefix - the obvious way to write this - would measure
   * nothing at all. A bulk load's duplicate checks land among the keys already stored, and that is the case where a
   * series must otherwise be read to discover it does not hold the key.
   */
  private static String key(final int i) {
    return "user-%09d@example.com".formatted(i * 2L);
  }

  private static String absentKey(final int i) {
    return "user-%09d@example.com".formatted(i * 2L + 1);
  }

  private record Measurement(long absentNanos, long presentNanos, long absentPagesRead, long absentBytesRead,
                             long presentPagesRead, long seriesSkipped, long seriesProbed, long effectiveCacheRAM,
                             long observedCacheRAM) {
    double absentLookupsPerSecond(final int lookups) {
      return lookups / (absentNanos / 1_000_000_000d);
    }

    double presentLookupsPerSecond(final int lookups) {
      return lookups / (presentNanos / 1_000_000_000d);
    }
  }

  private record Layout(int series, long compactedBytes, int compactedPages, int compactedPageSize,
                        long filterBytes, int filterPages, int filterPageSize, long keys) {
  }

  @Test
  void filtersAgainstNoFiltersInBothCacheRegimes() throws Exception {
    final int entries = Integer.getInteger(ENTRIES_PROPERTY, DEFAULT_ENTRIES);
    final int lookups = Integer.getInteger(LOOKUPS_PROPERTY, DEFAULT_LOOKUPS);
    final int smallCacheMB = Integer.getInteger(SMALL_CACHE_PROPERTY, DEFAULT_SMALL_CACHE_MB);
    final int largeCacheMB = Integer.getInteger(LARGE_CACHE_PROPERTY, DEFAULT_LARGE_CACHE_MB);
    final int compactionRamMB = Integer.getInteger(COMPACTION_RAM_PROPERTY, DEFAULT_COMPACTION_RAM);

    FileUtils.deleteRecursively(ROOT.toFile());
    final long buildStarted = System.nanoTime();
    try {
      final Layout layout = build(entries, compactionRamMB);
      final double buildSeconds = seconds(System.nanoTime() - buildStarted);

      // The control (filters ignored) runs first in each regime, so the treatment never inherits its warmup.
      final Measurement smallOff = measure(smallCacheMB, false, entries, lookups);
      final Measurement smallOn = measure(smallCacheMB, true, entries, lookups);
      final Measurement largeOff = measure(largeCacheMB, false, entries, lookups);
      final Measurement largeOn = measure(largeCacheMB, true, entries, lookups);

      report(entries, lookups, smallCacheMB, largeCacheMB, buildSeconds, layout, smallOff, smallOn, largeOff, largeOn);

      // Not an assertion about speed - hardware decides that - but about the filters being exercised at all. A run
      // where nothing was skipped would print numbers that mean nothing.
      assertThat(smallOn.seriesSkipped()).as("the filters must have skipped series in the constrained-cache run")
          .isGreaterThan(0);
      assertThat(smallOff.seriesSkipped()).as("no series may be skipped with the filters off").isZero();
      assertThat(layout.series()).as("the benchmark needs several compacted series to be meaningful").isGreaterThan(1);

    } finally {
      FileUtils.deleteRecursively(ROOT.toFile());
    }
  }

  /** Builds the database once, with the filters on, and compacts it into several series. */
  private Layout build(final int entries, final int compactionRamMB) throws Exception {
    restartPageManager(DEFAULT_LARGE_CACHE_MB);
    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.setValue(0.01f);
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue((long) compactionRamMB);

    try (final DatabaseFactory factory = new DatabaseFactory(ROOT.toString()); final Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty(PROPERTY_NAME, Type.STRING);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { PROPERTY_NAME })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

      // Insert in a PERMUTED key order, not ascending.
      //
      // A compaction round slices the mutable pages by RAM, and mutable pages fill in insertion order - so if keys
      // arrive ascending, every series ends up owning a disjoint slice of the key space and a series' root page alone
      // rules out almost every lookup, for free. That is the shape of a load keyed by a counter or a timestamp, and
      // the one where these filters have least to offer. A load keyed by an email, a UUID or any business id arrives
      // in no key order at all, every series spans the whole range, and the root page rules out nothing - which is the
      // workload #5470 reported and the one this measures.
      final int stride = permutationStride(entries);
      for (int from = 0; from < entries; from += INSERT_BATCH) {
        final int batchStart = from;
        final int batchEnd = Math.min(entries, from + INSERT_BATCH);
        database.transaction(() -> {
          for (int i = batchStart; i < batchEnd; i++)
            database.newDocument(TYPE_NAME).set(PROPERTY_NAME, key((int) ((long) i * stride % entries))).save();
        });
      }

      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties(PROPERTY_NAME).getFirst();
      assertThat(((IndexInternal) typeIndex).scheduleCompaction()).isTrue();
      assertThat(((IndexInternal) typeIndex).compact()).isTrue();

      final LSMTreeIndexCompacted compacted = compactedOf(typeIndex);
      final LSMTreeIndexBloomFilter filter = compacted.getBloomFilter();
      assertThat(filter).as("the build must have produced a bloom filter file").isNotNull();

      return new Layout(compacted.getSeriesCount(), compacted.getOSFile().length(), compacted.getTotalPages(),
          compacted.getPageSize(), filter.getOSFile().length(), filter.getTotalPages(), filter.getPageSize(), entries);
    }
  }

  /**
   * One timed pass over a freshly started page manager, so the cache begins empty and the regime under test is the
   * only thing that decides what stays resident.
   */
  private Measurement measure(final int cacheMB, final boolean filtersEnabled, final int entries, final int lookups)
      throws Exception {
    restartPageManager(cacheMB);
    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.setValue(filtersEnabled ? 0.01f : 0f);

    try (final DatabaseFactory factory = new DatabaseFactory(ROOT.toString()); final Database database = factory.open()) {
      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties(PROPERTY_NAME).getFirst();
      final LSMTreeIndexCompacted compacted = compactedOf(typeIndex);

      assertThat(compacted.isBloomFilterEnabled()).as("filters consulted when enabled=%s", filtersEnabled)
          .isEqualTo(filtersEnabled);

      // Warm the JIT and let the cache reach the steady state this regime allows.
      final int[] warmupOrder = probeOrder(entries, WARMUP);
      for (final int position : warmupOrder)
        drain(typeIndex.get(new Object[] { absentKey(position) }));

      final long skippedBefore = compacted.getBloomSkippedSeries();
      final long probedBefore = compacted.getBloomProbedSeries();

      final int[] order = probeOrder(entries, lookups);

      // Each phase gets its own page counters: the present-key phase HAS to read data pages, so folding both into one
      // number would hide whatever the absent-key phase - the one the filters exist for - actually saved.
      long pagesBefore = PageManager.INSTANCE.getStats().pagesRead;
      long bytesBefore = PageManager.INSTANCE.getStats().pagesReadSize;
      final long absentStarted = System.nanoTime();
      for (final int position : order)
        drain(typeIndex.get(new Object[] { absentKey(position) }));
      final long absentNanos = System.nanoTime() - absentStarted;
      final long absentPagesRead = PageManager.INSTANCE.getStats().pagesRead - pagesBefore;
      final long absentBytesRead = PageManager.INSTANCE.getStats().pagesReadSize - bytesBefore;
      // What was RESIDENT when the phase ended, not what was configured. PageManager.checkForPageDisposal() runs at
      // most once every 100 ms, so on a read-only workload this fast the cache overshoots its maximum freely between
      // sweeps - reporting the configured number alone would describe a regime the run never had.
      final long observedCacheRAM = PageManager.INSTANCE.getStats().readCacheRAM;

      pagesBefore = PageManager.INSTANCE.getStats().pagesRead;
      final long presentStarted = System.nanoTime();
      for (final int position : order)
        drain(typeIndex.get(new Object[] { key(position) }));
      final long presentNanos = System.nanoTime() - presentStarted;
      final long presentPagesRead = PageManager.INSTANCE.getStats().pagesRead - pagesBefore;

      return new Measurement(absentNanos, presentNanos, absentPagesRead, absentBytesRead, presentPagesRead,
          compacted.getBloomSkippedSeries() - skippedBefore, compacted.getBloomProbedSeries() - probedBefore,
          PageManager.INSTANCE.getStats().maxRAM, observedCacheRAM);
    }
  }

  private void report(final int entries, final int lookups, final int smallCacheMB, final int largeCacheMB,
      final double buildSeconds, final Layout layout, final Measurement smallOff, final Measurement smallOn,
      final Measurement largeOff, final Measurement largeOn) {
    System.out.printf(Locale.ROOT, """

            LSM index bloom filter benchmark (#5517)
            ----------------------------------------
            entries              : %,d (unique index, 1 bucket)
            compacted series     : %d
            compacted index      : %s in %,d pages of %,d B
            bloom filter file    : %s in %,d pages of %,d B  (%.2f bytes/key, %.1f%% of the index)
            build + compaction   : %.1f s
            lookups per timing   : %,d

            CONSTRAINED CACHE - asked for %d MB (limit %s, actually resident %s) against a %s index
              %s
              absent  lookups  off %,12.0f/s  on %,12.0f/s   %.2fx
              absent  pages    off %,12d    on %,12d    %.2fx fewer
              absent  bytes    off %12s    on %12s
              present lookups  off %,12.0f/s  on %,12.0f/s   %.2fx
              present pages    off %,12d    on %,12d
              series           %,d skipped of %,d probed

            RESIDENT CACHE - %d MB, the whole index fits
              absent  lookups  off %,12.0f/s  on %,12.0f/s   %.2fx
              absent  pages    off %,12d    on %,12d
              present lookups  off %,12.0f/s  on %,12.0f/s   %.2fx
              series           %,d skipped of %,d probed

            How to read this. ABSENT is a bulk load's duplicate check, which by definition misses in every series.
            PRESENT still gains, because a key lives in ONE series and the filters spare the reader the others -
            the gain is smaller only because that one series must be read whatever happens.
            Compare the two cache regimes to separate the two effects: whatever speedup survives with the whole
            index RESIDENT is CPU (root and leaf binary searches not performed); whatever the CONSTRAINED regime
            adds on top of that is avoided I/O, and it grows with the ratio of index size to cache.
            Both depend on series' key ranges OVERLAPPING, which is what a business-keyed load produces. Keys that
            arrive already ascending give each series a disjoint slice that its root page alone rules out, and
            there the filters have little left to save - see the permuted insertion order above.

            """,
        entries, layout.series(),
        FileUtils.getSizeAsString(layout.compactedBytes()), layout.compactedPages(), layout.compactedPageSize(),
        FileUtils.getSizeAsString(layout.filterBytes()), layout.filterPages(), layout.filterPageSize(),
        layout.filterBytes() / (double) layout.keys(),
        100d * layout.filterBytes() / layout.compactedBytes(), buildSeconds, lookups,

        smallCacheMB, FileUtils.getSizeAsString(smallOn.effectiveCacheRAM()),
        FileUtils.getSizeAsString(smallOn.observedCacheRAM()), FileUtils.getSizeAsString(layout.compactedBytes()),
        smallOn.observedCacheRAM() < layout.compactedBytes() ?
            "the index does NOT fit, as in a bulk load" :
            "NOTE: the index ended up fully resident anyway (the page cache is swept at most every 100ms and "
                + "overshoots its limit in between), so these rows measure mostly CPU - raise -D" + ENTRIES_PROPERTY,
        smallOff.absentLookupsPerSecond(lookups), smallOn.absentLookupsPerSecond(lookups),
        smallOn.absentLookupsPerSecond(lookups) / smallOff.absentLookupsPerSecond(lookups),
        smallOff.absentPagesRead(), smallOn.absentPagesRead(),
        smallOn.absentPagesRead() == 0 ? 0d : smallOff.absentPagesRead() / (double) smallOn.absentPagesRead(),
        FileUtils.getSizeAsString(smallOff.absentBytesRead()), FileUtils.getSizeAsString(smallOn.absentBytesRead()),
        smallOff.presentLookupsPerSecond(lookups), smallOn.presentLookupsPerSecond(lookups),
        smallOn.presentLookupsPerSecond(lookups) / smallOff.presentLookupsPerSecond(lookups),
        smallOff.presentPagesRead(), smallOn.presentPagesRead(),
        smallOn.seriesSkipped(), smallOn.seriesSkipped() + smallOn.seriesProbed(),

        largeCacheMB,
        largeOff.absentLookupsPerSecond(lookups), largeOn.absentLookupsPerSecond(lookups),
        largeOn.absentLookupsPerSecond(lookups) / largeOff.absentLookupsPerSecond(lookups),
        largeOff.absentPagesRead(), largeOn.absentPagesRead(),
        largeOff.presentLookupsPerSecond(lookups), largeOn.presentLookupsPerSecond(lookups),
        largeOn.presentLookupsPerSecond(lookups) / largeOff.presentLookupsPerSecond(lookups),
        largeOn.seriesSkipped(), largeOn.seriesSkipped() + largeOn.seriesProbed());
  }

  private static LSMTreeIndexCompacted compactedOf(final TypeIndex typeIndex) {
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    return bucketIndex.getMutableIndex().getSubIndex();
  }

  /**
   * Tears the page manager down and back up so the next open starts with an empty cache of {@code cacheMB}. The size
   * is read once at startup, so without this every regime after the first would silently run at the first one's size.
   */
  private static void restartPageManager(final int cacheMB) {
    PageManager.INSTANCE.close();
    GlobalConfiguration.MAX_PAGE_RAM.setValue((long) cacheMB);
  }

  /**
   * {@code count} positions spread over the key space in a fixed pseudo-random order, identical for every run.
   * <p>
   * Order matters as much as the keys do. Ascending probes hit the same index page thousands of times in a row, so
   * everything stays cached and the measurement quietly turns into a CPU benchmark whatever the cache size says. The
   * duplicate checks of a load keyed by anything but a counter - an email, a UUID, a business id - arrive in no useful
   * order, and that is when a page has to be fetched to answer a question the filter answers from RAM.
   */
  private static int[] probeOrder(final int entries, final int count) {
    final Random random = new Random(0x5117);
    final int[] order = new int[count];
    for (int i = 0; i < count; i++)
      order[i] = random.nextInt(entries);
    return order;
  }

  private static void drain(final IndexCursor cursor) {
    try {
      while (cursor.hasNext())
        cursor.next();
    } finally {
      cursor.close();
    }
  }

  /** A stride coprime with {@code entries}, so {@code i * stride % entries} walks every key exactly once. */
  private static int permutationStride(final int entries) {
    int stride = Math.max(1, entries / 3 + 1);
    while (gcd(stride, entries) != 1)
      ++stride;
    return stride;
  }

  private static int gcd(final int a, final int b) {
    return b == 0 ? a : gcd(b, a % b);
  }

  private static double seconds(final long nanos) {
    return nanos / 1_000_000_000d;
  }
}
