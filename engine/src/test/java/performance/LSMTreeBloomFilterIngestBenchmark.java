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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the per-series bloom filters of issue #5517 are worth on the thing a user actually waits for: the
 * <b>wall-clock of a bulk load</b>.
 * <p>
 * {@code LSMTreeBloomFilterBenchmark} measures lookups against an index that is already built, which isolates the
 * effect but answers a question nobody asked. The question asked on issue #5470 was how much faster the LOAD gets,
 * and that is a different measurement: the filters only help one part of the work (the duplicate check a unique index
 * performs for every incoming record), while the load also pays for parsing, record writes, WAL and the compactions
 * themselves - and, with the filters on, for building them. Whatever survives that dilution is the honest number.
 * <p>
 * Three things decide whether this measures anything at all, all learned the hard way on #5517:
 * <ul>
 *   <li><b>Keys must arrive in no key order.</b> A compaction slices the mutable pages by RAM and mutable pages fill
 *   in INSERTION order, so ascending keys give every series a disjoint slice of the key space that its root page rules
 *   out for free - the filters then have nothing left to skip. Business keys (an email, a UUID, an account id) arrive
 *   unordered, every series spans the whole range, and the root page rules out nothing. That is the reported
 *   workload, so the keys here are permuted.</li>
 *   <li><b>The index must not fit in the page cache.</b> The filters do not save instructions worth counting, they
 *   save page READS, and a page read is free when the page is resident. The cache is therefore held well below the
 *   index size, which is the regime of every load big enough to be worth optimising.</li>
 *   <li><b>Several compacted series must exist WHILE the load runs.</b> A duplicate check costs one page read per
 *   series, so with one series there is almost nothing to skip. The compaction RAM budget is lowered to make series
 *   accumulate over a dataset that fits in a benchmark, rather than over the 16M records of the original report -
 *   this simulates the series structure of a much larger load, and the report says so.</li>
 * </ul>
 * Each arm runs on a freshly created database, the control (filters off) first so the treatment never inherits its
 * warmup, and the pair is repeated so run-to-run spread is visible rather than hidden behind a single number.
 * <p>
 * Run with:
 * {@code ./mvnw -pl engine -Dtest=LSMTreeBloomFilterIngestBenchmark -Dgroups=benchmark test}
 * <p>
 * Knobs (all optional):
 * {@code -Darcadedb.bloomIngest.entries=2000000}
 * {@code -Darcadedb.bloomIngest.cacheMB=64}
 * {@code -Darcadedb.bloomIngest.compactionRamMB=8}
 * {@code -Darcadedb.bloomIngest.rounds=2}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class LSMTreeBloomFilterIngestBenchmark {

  private static final String ENTRIES_PROPERTY        = "arcadedb.bloomIngest.entries";
  private static final String CACHE_PROPERTY          = "arcadedb.bloomIngest.cacheMB";
  private static final String COMPACTION_RAM_PROPERTY = "arcadedb.bloomIngest.compactionRamMB";
  private static final String ROUNDS_PROPERTY         = "arcadedb.bloomIngest.rounds";

  private static final int DEFAULT_ENTRIES        = 2_000_000;
  private static final int DEFAULT_CACHE_MB       = 64;
  private static final int DEFAULT_COMPACTION_RAM = 8;
  private static final int DEFAULT_ROUNDS         = 2;

  private static final int    INSERT_BATCH  = 20_000;
  private static final String TYPE_NAME     = "BloomIngestRecord";
  private static final String PROPERTY_NAME = "email";
  private static final Path   ROOT          = Path.of("target", "databases", "LSMTreeBloomFilterIngestBenchmark");

  @Test
  void bulkLoadWithAndWithoutFilters() throws Exception {
    final int entries = Integer.getInteger(ENTRIES_PROPERTY, DEFAULT_ENTRIES);
    final int cacheMB = Integer.getInteger(CACHE_PROPERTY, DEFAULT_CACHE_MB);
    final int compactionRamMB = Integer.getInteger(COMPACTION_RAM_PROPERTY, DEFAULT_COMPACTION_RAM);
    final int rounds = Integer.getInteger(ROUNDS_PROPERTY, DEFAULT_ROUNDS);

    final Run[] off = new Run[rounds];
    final Run[] on = new Run[rounds];

    try {
      for (int round = 0; round < rounds; round++) {
        // Control first in every round: whatever the machine is doing to itself, both arms meet it in the same order.
        off[round] = ingest(entries, cacheMB, compactionRamMB, false);
        on[round] = ingest(entries, cacheMB, compactionRamMB, true);
      }

      report(entries, cacheMB, compactionRamMB, rounds, off, on);

      // Not assertions about speed - the hardware decides that - but about the run having measured the feature at
      // all. A load where no series was ever skipped would print numbers that mean nothing.
      assertThat(on[0].seriesSkipped()).as("the filters must have skipped series during the load").isGreaterThan(0);
      assertThat(off[0].seriesSkipped()).as("no series may be skipped with the filters off").isZero();
      assertThat(on[0].series()).as("the load needs several compacted series for a duplicate check to cost anything")
          .isGreaterThan(1);
    } finally {
      FileUtils.deleteRecursively(ROOT.toFile());
    }
  }

  /**
   * One full load into a database created for it, timed end to end. The page manager is restarted first so the cache
   * starts empty at the size under test: it is read once at startup, so without this every arm after the first would
   * silently run at the first one's size.
   */
  private Run ingest(final int entries, final int cacheMB, final int compactionRamMB, final boolean filtersEnabled)
      throws Exception {

    FileUtils.deleteRecursively(ROOT.toFile());

    PageManager.INSTANCE.close();
    GlobalConfiguration.MAX_PAGE_RAM.setValue((long) cacheMB);
    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.setValue(filtersEnabled ? 0.01f : 0f);
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue((long) compactionRamMB);

    try (final DatabaseFactory factory = new DatabaseFactory(ROOT.toString()); final Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty(PROPERTY_NAME, Type.STRING);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { PROPERTY_NAME })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

      final long pagesBefore = PageManager.INSTANCE.getStats().pagesRead;
      final long bytesBefore = PageManager.INSTANCE.getStats().pagesReadSize;

      // Permuted, not ascending: see the class comment. This is what makes the series overlap in key range, which is
      // the only shape in which a filter has anything to answer.
      final int stride = permutationStride(entries);
      final long started = System.nanoTime();
      for (int from = 0; from < entries; from += INSERT_BATCH) {
        final int batchStart = from;
        final int batchEnd = Math.min(entries, from + INSERT_BATCH);
        database.transaction(() -> {
          for (int i = batchStart; i < batchEnd; i++)
            database.newDocument(TYPE_NAME).set(PROPERTY_NAME, key((int) ((long) i * stride % entries))).save();
        });
      }
      final long nanos = System.nanoTime() - started;

      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties(PROPERTY_NAME).getFirst();
      final LSMTreeIndexCompacted compacted = compactedOf(typeIndex);
      final LSMTreeIndexBloomFilter filter = compacted.getBloomFilter();

      assertThat(compacted.isBloomFilterEnabled()).as("filters consulted when enabled=%s", filtersEnabled)
          .isEqualTo(filtersEnabled);

      return new Run(nanos, PageManager.INSTANCE.getStats().pagesRead - pagesBefore,
          PageManager.INSTANCE.getStats().pagesReadSize - bytesBefore, compacted.getBloomSkippedSeries(),
          compacted.getBloomProbedSeries(), compacted.getSeriesCount(), compacted.getOSFile().length(),
          filter != null ? filter.getOSFile().length() : 0L);
    }
  }

  private void report(final int entries, final int cacheMB, final int compactionRamMB, final int rounds,
      final Run[] off, final Run[] on) {

    final StringBuilder perRound = new StringBuilder();
    for (int round = 0; round < rounds; round++)
      perRound.append("            round %d              : %.1f s off, %.1f s on  (%.2fx)%n"
          .formatted(round + 1, off[round].seconds(), on[round].seconds(),
              off[round].seconds() / on[round].seconds()));

    final double offBest = best(off);
    final double onBest = best(on);

    System.out.printf(Locale.ROOT, """

            LSM index bloom filter - BULK LOAD wall-clock (#5517, asked on #5470)
            --------------------------------------------------------------------
            records loaded       : %,d into a unique index (1 bucket), keys in permuted order
            page cache           : %d MB against a %s index -> %s
            compaction RAM       : %d MB, lowered so several series accumulate DURING the load: this stands in
                                   for the series structure of a much larger load, not for its size
            compacted series     : %d without filters, %d with
            bloom filter file    : %s (%.2f bytes/key, %.1f%% of the index)

            %s
            best of %d           : %.1f s off, %.1f s on  -> %.2fx
            throughput           : %,.0f records/s off, %,.0f records/s on

            series skipped       : %,d of %,d duplicate-check probes answered from the filters alone

            pages read (best run): %,d off, %,d on   |   bytes read: %s off, %s on
              NOT a like-for-like comparison, and not the saving: these counters cover the WHOLE load, so most of
              them are the compactions' own I/O, and the two arms did not compact identically (%d series against
              %d - a faster load reaches a different point by the time each automatic compaction fires). Reading a
              filter page is itself a page read, so the arm that skips data pages can still show more of them. The
              wall-clock above is the number that compares.

            Read this as the END-TO-END load time, not the speedup of the duplicate check: the load also pays for
            record writes, WAL and the compactions, and with the filters on for building them. Run-to-run spread on
            a laptop is easily +/-20%%, which is why every round is printed.
            """, entries, cacheMB, FileUtils.getSizeAsString(on[0].indexBytes()),
        on[0].indexBytes() > cacheMB * 1024L * 1024L
            ? "the index does NOT fit, which is the regime worth optimising"
            : "the index FITS, so what is measured here is the search work saved, not I/O",
        compactionRamMB,
        off[0].series(), on[0].series(), FileUtils.getSizeAsString(on[0].filterBytes()),
        on[0].filterBytes() / (double) entries, 100.0 * on[0].filterBytes() / on[0].indexBytes(),
        perRound.toString().stripTrailing(), rounds, offBest, onBest, offBest / onBest,
        entries / offBest, entries / onBest,
        on[0].seriesSkipped(), on[0].seriesSkipped() + on[0].seriesProbed(),
        bestRun(off).pagesRead(), bestRun(on).pagesRead(),
        FileUtils.getSizeAsString(bestRun(off).bytesRead()), FileUtils.getSizeAsString(bestRun(on).bytesRead()),
        off[0].series(), on[0].series());
  }

  private static double best(final Run[] runs) {
    double bestSeconds = Double.MAX_VALUE;
    for (final Run run : runs)
      bestSeconds = Math.min(bestSeconds, run.seconds());
    return bestSeconds;
  }

  /** The fastest round, so the page/byte counters quoted belong to the timing quoted next to them. */
  private static Run bestRun(final Run[] runs) {
    Run winner = runs[0];
    for (final Run run : runs)
      if (run.seconds() < winner.seconds())
        winner = run;
    return winner;
  }

  private record Run(long nanos, long pagesRead, long bytesRead, long seriesSkipped, long seriesProbed, int series,
                     long indexBytes, long filterBytes) {
    double seconds() {
      return nanos / 1_000_000_000d;
    }
  }

  private static String key(final int i) {
    return "user-%09d@example.com".formatted(i * 2L);
  }

  private static LSMTreeIndexCompacted compactedOf(final TypeIndex typeIndex) {
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    return bucketIndex.getMutableIndex().getSubIndex();
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
}
