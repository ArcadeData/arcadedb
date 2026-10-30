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
package com.arcadedb.index.columnar.spike;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.ParallelScanProducerPool;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spike benchmark v2: projection (native match + count) versus SQL GROUP BY on the same WHERE, for one facet (the gate), for a
 * five facet page and for the off-heap block variant. Timings are reported, never asserted; correctness is.
 * <p>
 * Segments are built once before the measured rounds: they model the persisted projection, so their build is not query cost.
 */
@Tag("benchmark")
class ColumnarSpikeBenchmark extends TestHelper {
  private static final int    RECORDS       = Integer.getInteger("columnarSpike.records", 1_000_000);
  private static final int    WARMUP        = 2;
  private static final int    RUNS          = 6;
  private static final int    TOP           = 10;
  private static final long   SEED          = 42L;
  private static final double MISSING_RATIO = 0.01;
  private static final int    REREAD_PAGES  = 200;
  private static final long   SELECTIVE_TO  = 10_000L;
  private static final int    TASKS         = 16;
  private static final int    QUERIES       = 50;
  private static final int    THREADS       = Integer.getInteger("columnarSpike.threads", 8);
  private static final int    ITERATIONS    = 20;
  /**
   * The SQL front-end floor: the GROUP BY shape of the gate on an index-served predicate that matches nothing, so its time is
   * parse/plan (or statement-cache lookup), query context, security and result set, with no rows to fetch or group.
   */
  private static final String FLOOR_WHERE   = SpikeDataset.FILE_SIZE + " < 0";
  private static final String FLOOR_QUERY   = SpikeTermsCounter.groupByQuery(FLOOR_WHERE, SpikeDataset.BRAND);
  private static final String BROAD_CAVEAT  = "The broad gate assumes `status` is a projected column; an unindexed predicate on a property that is not "
      + "projected falls back to the record scan (GROUP BY cost).";

  /** The facets of the K = 5 page, in the order both sides run them. The first four are single-valued, tags is multi-valued. */
  private static final String[] FACETS        = { SpikeDataset.BRAND, SpikeDataset.STATUS, SpikeDataset.MIME_TYPE, SpikeDataset.AUTHOR,
      SpikeDataset.TAGS };
  private static final int      K             = FACETS.length;
  private static final int      BRAND_F       = 0;
  private static final int      STATUS_F      = 1;
  private static final int      AUTHOR_F      = 3;
  private static final int      TAGS_F        = 4;
  /** The native match sources report uncovered positions themselves, so the count gets no position list. */
  private static final long[]   NO_POSITIONS  = new long[0];

  private final SpikeReport       report       = new SpikeReport();
  private final SpikeSegment[]    segments     = new SpikeSegment[TAGS_F];
  private final SpikeDictionary[] dictionaries = new SpikeDictionary[K];
  private final long[]            uncovered    = new long[1];
  private       SpikeMultiSegment tagSegment;
  private       SpikeBlockSegment brandBlocks;
  private       SpikeBlockSegment authorBlocks;
  /** JVM stall observed inside the measured calls of the current predicate (context only, already discounted). */
  private       long              stall;
  /** Every timed call folds a cheap value of its result in here, so none of them is dead code. */
  private       long              sink;

  /** A measured predicate: its SQL text and the native match source (takes the uncovered counter, returns the match bitmap). */
  private record Predicate(String label, String where, Function<long[], long[]> match) {
  }

  private record Phase(double qps, double p50, double p95, long wallNanos, long stallNanos) {
  }

  private record Timed(long nanos, long stallNanos, List<List<SpikeTermsCounter.Bucket>> result) {
  }

  private record Outcome(double k1Projection, double k1GroupBy, double k5Projection, double k5GroupBy, double heapK1Count,
                         double blockK1Count, double heapK5, double blockK5, double sqlFloor,
                         double k1WithFloor) {
    boolean gate() {
      return k1Projection < k1GroupBy;
    }

    /** Reported, not decisive: the projection side charged with the SQL front end it skips (median of the per-round sums). */
    boolean gateWithFloor() {
      return k1WithFloor < k1GroupBy;
    }
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void run() throws IOException {
    report.text("## Columnar faceting spike results v2 (records=" + RECORDS + ")");
    report.text("");

    final StallAwareStopwatch sw = StallAwareStopwatch.start();
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, RECORDS, SEED, MISSING_RATIO);
    report.row("ingest", sw.effectiveMs() + " ms", "stall " + sw.stallMs() + " ms (context only)");

    final LocalBucket bucket = SpikeDataset.bucket(database);
    buildSegments(bucket);
    sizes(bucket);

    final Outcome selective = measure("selective", SpikeDataset.FILE_SIZE + " < " + SELECTIVE_TO,
        "index range on fileSize [0, " + SELECTIVE_TO + ")",
        () -> SpikeMatchSource.indexRange(database, segments[BRAND_F], SpikeDataset.FILE_SIZE, 0L, SELECTIVE_TO, uncovered), true);
    final Outcome broad = measure("broad", "status = 'published'", "sequential scan of the status column",
        () -> SpikeMatchSource.columnEquals(segments[STATUS_F], dictionaries[STATUS_F], "published"), false);

    pageReread(bucket);
    memory();

    final List<Predicate> predicates = List.of(
        new Predicate("selective", SpikeDataset.FILE_SIZE + " < " + SELECTIVE_TO,
            u -> SpikeMatchSource.indexRange(database, segments[BRAND_F], SpikeDataset.FILE_SIZE, 0L, SELECTIVE_TO, u)),
        new Predicate("broad", "status = 'published'", u -> SpikeMatchSource.columnEquals(segments[STATUS_F], dictionaries[STATUS_F], "published")));
    // concurrent first: the cold section reopens the database and leaves the engine cache cold
    concurrent(predicates);
    cold(predicates);

    report.text("");
    report.row("sink (dead-code guard)", String.valueOf(sink), "folded results of every timed call");

    final String verdict;
    if (selective.gate() && broad.gate())
      verdict = "GO";
    else
      verdict = "NO-GO: projection (native match + count) does not beat GROUP BY for " + (!selective.gate() ? "selective" : "")
          + (!selective.gate() && !broad.gate() ? " and " : "") + (!broad.gate() ? "broad" : "");
    report.text("");
    report.text("**Verdict: " + verdict + "** (gate, K = 1: selective " + SpikeClock.ms(selective.k1Projection()) + " ms vs "
        + SpikeClock.ms(selective.k1GroupBy()) + " ms, broad " + SpikeClock.ms(broad.k1Projection()) + " ms vs "
        + SpikeClock.ms(broad.k1GroupBy()) + " ms)");
    report.text("");
    report.text("The gate's projection side is a direct Java call and excludes the SQL front end a real faceted query pays; the SQL floor ("
        + "`" + FLOOR_QUERY + "`, zero rows) has a median of " + SpikeClock.ms(selective.sqlFloor()) + " ms (selective rounds) and "
        + SpikeClock.ms(broad.sqlFloor()) + " ms (broad rounds).");
    report.text("");
    report.text("gate including SQL floor: " + (selective.gateWithFloor() && broad.gateWithFloor() ? "GO" : "NO-GO") + " (reported, not decisive: selective "
        + SpikeClock.ms(selective.k1WithFloor()) + " ms vs " + SpikeClock.ms(selective.k1GroupBy()) + " ms, broad "
        + SpikeClock.ms(broad.k1WithFloor()) + " ms vs " + SpikeClock.ms(broad.k1GroupBy()) + " ms)");
    report.text("");
    report.text(BROAD_CAVEAT);
    report.text("");
    report.text(parallelismNote());
    report.text("");
    report.text("K = 5 page: projection " + SpikeClock.ms(selective.k5Projection()) + " ms vs GROUP BY " + SpikeClock.ms(selective.k5GroupBy())
        + " ms (selective), projection " + SpikeClock.ms(broad.k5Projection()) + " ms vs GROUP BY " + SpikeClock.ms(broad.k5GroupBy())
        + " ms (broad)");
    report.text("");
    report.text("block variant overhead vs heap: " + percent(selective.blockK1Count(), selective.heapK1Count()) + " (selective, K = 1 brand count), "
        + percent(broad.blockK1Count(), broad.heapK1Count()) + " (broad, K = 1 brand count); K = 5 page " + percent(selective.blockK5(),
        selective.heapK5()) + " (selective), " + percent(broad.blockK5(), broad.heapK5()) + " (broad)");
    final Path file = report.write();
    assertThat(file).exists();
  }

  /** The projection side of a page: native match once, then count + top-N of the first {@code facets} facets. Allocates its own scratch. */
  private List<List<SpikeTermsCounter.Bucket>> projectionPage(final Function<long[], long[]> match, final int facets) {
    final long[] bitmap = match.apply(new long[1]);
    final List<List<SpikeTermsCounter.Bucket>> page = new ArrayList<>(facets);
    for (int f = 0; f < facets; f++)
      page.add(SpikeTermsCounter.topN(countHeap(f, bitmap), dictionaries[f], TOP));
    return page;
  }

  /** The GROUP BY side of a page: one query per facet, back to back. */
  private List<List<SpikeTermsCounter.Bucket>> groupByPage(final String where, final int facets) {
    final List<List<SpikeTermsCounter.Bucket>> page = new ArrayList<>(facets);
    for (int f = 0; f < facets; f++)
      page.add(f == TAGS_F ?
          SpikeTermsCounter.groupByUnwind(database, where, SpikeDataset.TAGS, TOP) :
          SpikeTermsCounter.groupBy(database, where, FACETS[f], TOP, new long[1]));
    return page;
  }

  /** The machine and the JVM-wide producer pool the broad GROUP BY scan runs on, and how to read the numbers next to a single-threaded projection. */
  private static String parallelismNote() {
    return "Parallelism context: " + Runtime.getRuntime().availableProcessors() + " available processors, ParallelScanProducerPool max parallelism "
        + ParallelScanProducerPool.getInstance().getMaxParallelism() + ". The broad GROUP BY scans in parallel internally on that JVM-wide pool "
        + "(the caller runs units itself when the pool is saturated) while each projection query runs on one thread; under " + THREADS
        + " concurrent clients the queries/s ratio therefore reflects total CPU cost, not per-query parallel latency. The selective GROUP BY is "
        + "index-served and sequential.";
  }

  private void concurrent(final List<Predicate> predicates) {
    report.text("");
    report.text("### Concurrent K = 5 page, " + THREADS + " threads x " + ITERATIONS + " iterations (dedicated fixed pool)");
    report.text("");
    report.text("Each thread runs the K = 5 page " + ITERATIONS + " times for one predicate, projection and GROUP BY in separate phases; the phase "
        + "order alternates across the predicates. Latency per page query (nanosecond clock, stall-discounted), throughput = total queries over the "
        + "stall-discounted window. Count arrays and bitmaps are allocated per call, the segments, dictionaries and the off-heap buffer are read-only "
        + "(absolute gets), so no scratch is shared.");
    report.text("");
    report.text(parallelismNote());
    final List<String[]> rows = new ArrayList<>();
    for (int i = 0; i < predicates.size(); i++) {
      final Predicate predicate = predicates.get(i);
      final List<List<SpikeTermsCounter.Bucket>> oracle = groupByPage(predicate.where(), K);
      final boolean projectionFirst = i % 2 == 0;
      for (int step = 0; step < 2; step++) {
        final boolean projection = (step == 0) == projectionFirst;
        final Phase phase = concurrentPhase(predicate, projection, oracle);
        rows.add(new String[] { predicate.label(), projection ? "projection" : "GROUP BY", step == 0 ? "first" : "second",
            String.format("%.1f", phase.qps()), SpikeClock.ms(phase.p50()), SpikeClock.ms(phase.p95()), SpikeClock.ms(phase.wallNanos()),
            SpikeClock.ms(phase.stallNanos()) });
      }
    }
    report.table("Concurrent K = 5 page (" + THREADS + " threads, " + THREADS * ITERATIONS + " queries per phase)",
        new String[] { "predicate", "method", "phase order", "queries/s", "p50 ms", "p95 ms", "window ms", "stall ms" }, rows);
  }

  private Phase concurrentPhase(final Predicate predicate, final boolean projection, final List<List<SpikeTermsCounter.Bucket>> oracle) {
    final long[] latencies = new long[THREADS * ITERATIONS];
    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    try {
      final CountDownLatch ready = new CountDownLatch(THREADS);
      final CountDownLatch go = new CountDownLatch(1);
      final List<Future<List<List<SpikeTermsCounter.Bucket>>>> futures = new ArrayList<>(THREADS);
      for (int t = 0; t < THREADS; t++) {
        final int thread = t;
        futures.add(pool.submit(() -> {
          ready.countDown();
          go.await();
          List<List<SpikeTermsCounter.Bucket>> last = null;
          for (int it = 0; it < ITERATIONS; it++) {
            final SpikeClock clock = SpikeClock.start();
            last = projection ? projectionPage(predicate.match(), K) : groupByPage(predicate.where(), K);
            latencies[thread * ITERATIONS + it] = clock.stop();
          }
          return last;
        }));
      }
      ready.await();
      final SpikeClock window = SpikeClock.start();
      go.countDown();
      final List<List<List<SpikeTermsCounter.Bucket>>> results = new ArrayList<>(THREADS);
      for (final Future<List<List<SpikeTermsCounter.Bucket>>> future : futures)
        results.add(future.get());
      final long wall = window.stop();
      for (int t = 0; t < THREADS; t++)
        for (int f = 0; f < K; f++)
          assertThat(results.get(t).get(f)).as(predicate.label() + " " + (projection ? "projection" : "GROUP BY") + " thread " + t + " " + FACETS[f])
              .containsExactlyElementsOf(oracle.get(f));
      final long[] sorted = latencies.clone();
      Arrays.sort(sorted);
      sink += sorted[0];
      return new Phase(wall == 0 ? 0 : latencies.length * 1e9 / wall, percentile(sorted, 0.50), percentile(sorted, 0.95), wall, window.stallNanos());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException(e.getCause());
    } finally {
      pool.shutdown();
      try {
        if (!pool.awaitTermination(30, TimeUnit.SECONDS))
          pool.shutdownNow();
      } catch (final InterruptedException e) {
        pool.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private static double percentile(final long[] sorted, final double p) {
    return sorted[Math.max(0, (int) Math.ceil(p * sorted.length) - 1)];
  }

  /**
   * One round of K = 1 and K = 5 on a freshly reopened database (engine page cache empty, OS file cache NOT), per predicate and per
   * order. The segments are reused across the reopen: they hold only RID positions and values, never a page or bucket handle, and model
   * the projection's own cache, which the design keeps warm separately. Match sources look the index up from the new handle.
   */
  private void cold(final List<Predicate> predicates) {
    report.text("");
    report.text("### Cold (engine cache only)");
    report.text("");
    report.text("The database is closed and reopened before every timed cell (predicate, method, K), so each row is engine-cold: ArcadeDB's page "
        + "cache is empty, the OS file cache is NOT emptied, and the JIT stays warm. The first GROUP BY after a reopen also pays an empty "
        + "statement/plan cache on the new database instance. Segments stay in heap / off-heap across the reopen (they hold only RID positions "
        + "and values, and model the projection's own cache, which the design keeps warm separately). The broad projection does no engine I/O at "
        + "all (columnEquals reads the heap segment only) and the selective projection reads only index pages, so the projection's cold rows say "
        + "nothing about a cold projection store. Cell order alternates: projection first for the first predicate, GROUP BY first for the second.");
    final List<String[]> rows = new ArrayList<>();
    for (int i = 0; i < predicates.size(); i++) {
      final Predicate predicate = predicates.get(i);
      final List<List<SpikeTermsCounter.Bucket>> oracle = groupByPage(predicate.where(), K);
      final boolean projectionFirst = i % 2 == 0;
      for (int step = 0; step < 2; step++) {
        final boolean projection = (step == 0) == projectionFirst;
        final String[] cells = new String[2];
        long stall = 0;
        for (int c = 0; c < 2; c++) {
          final int facets = c == 0 ? 1 : K;
          reopenDatabase();
          final Timed timed = timed(projection ? () -> projectionPage(predicate.match(), facets) : () -> groupByPage(predicate.where(), facets));
          for (int f = 0; f < facets; f++)
            assertThat(timed.result().get(f)).as(predicate.label() + " cold K = " + facets + " " + FACETS[f]).containsExactlyElementsOf(oracle.get(f));
          sink += timed.result().size();
          cells[c] = SpikeClock.ms(timed.nanos());
          stall += timed.stallNanos();
        }
        rows.add(new String[] { predicate.label(), projection ? "projection" : "GROUP BY", step == 0 ? "first" : "second", cells[0], cells[1],
            SpikeClock.ms(stall) });
      }
    }
    report.table("Cold (engine cache only, reopen before every cell)",
        new String[] { "predicate", "method", "order", "K = 1 ms", "K = 5 ms", "stall ms" }, rows);
  }

  private static Timed timed(final Supplier<List<List<SpikeTermsCounter.Bucket>>> call) {
    final SpikeClock clock = SpikeClock.start();
    final List<List<SpikeTermsCounter.Bucket>> result = call.get();
    final long nanos = clock.stop();
    return new Timed(nanos, clock.stallNanos(), result);
  }

  private void buildSegments(final LocalBucket bucket) {
    final List<String[]> rows = new ArrayList<>();
    for (int f = 0; f < K; f++) {
      dictionaries[f] = new SpikeDictionary();
      final SpikeClock clock = SpikeClock.start();
      final long builtRows;
      if (f == TAGS_F) {
        tagSegment = SpikeMultiSegment.build(bucket, FACETS[f], dictionaries[f]);
        builtRows = tagSegment.rows();
      } else {
        segments[f] = SpikeSegment.build(bucket, FACETS[f], dictionaries[f]);
        builtRows = segments[f].rows();
      }
      final long nanos = clock.stop();
      rows.add(buildRow(FACETS[f] + (f == TAGS_F ? " (multi-valued)" : ""), nanos, clock.stallNanos(), builtRows));
    }
    SpikeClock clock = SpikeClock.start();
    brandBlocks = SpikeBlockSegment.build(segments[BRAND_F]);
    long nanos = clock.stop();
    rows.add(buildRow("brand blocks (off-heap, from the heap segment)", nanos, clock.stallNanos(), segments[BRAND_F].rows()));
    clock = SpikeClock.start();
    authorBlocks = SpikeBlockSegment.build(segments[AUTHOR_F]);
    nanos = clock.stop();
    rows.add(buildRow("author blocks (off-heap, from the heap segment)", nanos, clock.stallNanos(), segments[AUTHOR_F].rows()));
    report.table("Segment builds (outside the measured rounds, they model the persisted projection)",
        new String[] { "segment", "build ms", "stall ms", "records/s" }, rows);

    // one match bitmap must serve every segment: same bucket, same stripes
    for (int f = 1; f < TAGS_F; f++)
      assertThat(segments[f].presence()).containsExactly(segments[BRAND_F].presence());
    assertThat(tagSegment.presence()).containsExactly(segments[BRAND_F].presence());
  }

  private static String[] buildRow(final String name, final long nanos, final long stallNanos, final long rows) {
    return new String[] { name, SpikeClock.ms(nanos), SpikeClock.ms(stallNanos), nanos == 0 ? "n/a" : String.format("%.0f", rows * 1e9 / nanos) };
  }

  private void sizes(final LocalBucket bucket) throws IOException {
    final long bucketBytes = (long) bucket.getTotalPages() * bucket.getPageSize();
    report.row("bucket bytes", String.valueOf(bucketBytes), bucket.getTotalPages() + " pages x " + bucket.getPageSize());

    final byte[] brandSerialized = segments[BRAND_F].serialize();
    Files.createDirectories(SpikeReport.DIRECTORY);
    final Path segmentFile = SpikeReport.DIRECTORY.resolve("segment.bin");
    Files.write(segmentFile, brandSerialized);
    report.row("brand segment file bytes", String.valueOf(Files.size(segmentFile)), segmentFile.toString());

    final List<String[]> rows = new ArrayList<>();
    for (int f = 0; f < K; f++) {
      final long bytes;
      final long rowCount;
      final long presenceBytes;
      final int bitWidth;
      if (f == TAGS_F) {
        bytes = tagSegment.serializedBytes() + dictionaryBytes(dictionaries[f]);
        rowCount = tagSegment.rows();
        presenceBytes = tagSegment.presence().length * 8L + tagSegment.pageRowStart().length * 4L;
        bitWidth = tagSegment.bitWidth();
      } else {
        bytes = f == BRAND_F ? brandSerialized.length : segments[f].serialize().length;
        rowCount = segments[f].rows();
        presenceBytes = segments[f].presence().length * 8L + segments[f].pageRowStart().length * 4L;
        bitWidth = segments[f].bitWidth();
      }
      rows.add(new String[] { FACETS[f], String.valueOf(bytes), String.format("%.3f", (double) bytes / rowCount),
          String.format("%.2f%%", 100.0 * presenceBytes / bytes), String.format("%.2f%%", 100.0 * bytes / bucketBytes),
          String.valueOf(bitWidth), String.valueOf(dictionaries[f].size()) });
    }
    report.table("Segment sizes (serialized, dictionary included; presence = presence bitmap + page row starts, spec 3.3.1 sparsity)",
        new String[] { "segment", "bytes", "bytes per record", "presence share", "of bucket", "bit width", "dictionary size" }, rows);

    report.row("brand blocks off-heap bytes", String.valueOf(brandBlocks.offHeapBytes()),
        brandBlocks.blockCount() + " blocks of " + SpikeBlockSegment.BLOCK_ROWS + " rows, heap side " + brandBlocks.heapBytes() + " bytes");
    report.row("author blocks off-heap bytes", String.valueOf(authorBlocks.offHeapBytes()),
        authorBlocks.blockCount() + " blocks of " + SpikeBlockSegment.BLOCK_ROWS + " rows, heap side " + authorBlocks.heapBytes() + " bytes");
    report.row("brand heap segment bytes", String.valueOf(segments[BRAND_F].heapBytes()), "heap variant, for comparison");
    report.row("author heap segment bytes", String.valueOf(segments[AUTHOR_F].heapBytes()), "heap variant, for comparison");
  }

  private static long dictionaryBytes(final SpikeDictionary dictionary) {
    long bytes = 4;
    for (int i = 0; i < dictionary.size(); i++)
      bytes += 4 + dictionary.valueOf(i).getBytes(StandardCharsets.UTF_8).length;
    return bytes;
  }

  private Outcome measure(final String label, final String where, final String nativeName, final Supplier<long[]> nativeMatch,
      final boolean planChecksForAllFacets) {
    report.text("");
    report.text("### Predicate " + label + ": " + where + " (native match: " + nativeName + ")");
    report.text("");
    if (label.equals("broad")) {
      report.text(BROAD_CAVEAT);
      report.text("");
    }
    planChecks(label, "match", "SELECT @rid FROM " + SpikeDataset.TYPE + " WHERE " + where, false);
    for (int f = 0; f < (planChecksForAllFacets ? K : 1); f++)
      planChecks(label, "GROUP BY " + FACETS[f], groupByQuery(where, f), true);
    planChecks("SQL floor", "SQL floor", FLOOR_QUERY, true);

    stall = 0;
    uncovered[0] = 0;
    final long[] k1Match = new long[RUNS];
    final long[] k1Count = new long[RUNS];
    final long[] k1Total = new long[RUNS];
    final long[] k1BlockMatch = new long[RUNS];
    final long[] k1BlockCount = new long[RUNS];
    final long[] k1BlockTotal = new long[RUNS];
    final long[] k5Match = new long[RUNS];
    final long[][] k5Count = new long[K][RUNS];
    final long[] k5Total = new long[RUNS];
    final long[] k5BlockMatch = new long[RUNS];
    final long[][] k5BlockCount = new long[K][RUNS];
    final long[] k5BlockTotal = new long[RUNS];
    final long[] sqlMatch = new long[RUNS];
    final long[] sqlCount = new long[RUNS];
    final long[] sqlTotal = new long[RUNS];
    final long[] groupByK1 = new long[RUNS];
    final long[] sqlFloor = new long[RUNS];
    final long[][] groupByK5 = new long[K][RUNS];
    final long[] groupByK5Total = new long[RUNS];

    // results of the last measured round, checked after the loop
    long[] lastBitmap = null;
    SpikeTermsCounter.Counts k1Heap = null;
    SpikeTermsCounter.Counts k1Block = null;
    SpikeTermsCounter.Counts sqlCounts = null;
    List<SpikeTermsCounter.Bucket> k1Top = null;
    List<SpikeTermsCounter.Bucket> k1GroupByTop = null;
    long k1GroupByMissing = 0;
    final SpikeTermsCounter.Counts[] k5Heap = new SpikeTermsCounter.Counts[K];
    final SpikeTermsCounter.Counts[] k5Block = new SpikeTermsCounter.Counts[K];
    final List<List<SpikeTermsCounter.Bucket>> k5Top = new ArrayList<>(Collections.nCopies(K, List.of()));
    final List<List<SpikeTermsCounter.Bucket>> k5BlockTop = new ArrayList<>(Collections.nCopies(K, List.of()));
    final List<List<SpikeTermsCounter.Bucket>> groupByTop = new ArrayList<>(Collections.nCopies(K, List.of()));
    final long[] groupByMissing = new long[K];

    for (int round = 0; round < WARMUP + RUNS; round++) {
      final boolean measured = round >= WARMUP;
      final int slot = round - WARMUP;
      // order keyed on the measured-run index (RUNS is even): each order runs RUNS / 2 times
      final boolean projectionFirst = (measured ? slot : round) % 2 == 0;
      for (int step = 0; step < 2; step++) {
        if ((step == 0) == projectionFirst) {
          // K = 1, heap: the gate
          SpikeClock clock = SpikeClock.start();
          final long[] bitmap = nativeMatch.get();
          final long k1m = stop(clock, measured);
          sink += bitmap.length;
          clock = SpikeClock.start();
          final SpikeTermsCounter.Counts c1 = countHeap(BRAND_F, bitmap);
          final List<SpikeTermsCounter.Bucket> t1 = top(c1, BRAND_F);
          final long k1c = stop(clock, measured);

          // K = 1, off-heap blocks
          clock = SpikeClock.start();
          final long[] blockBitmap = nativeMatch.get();
          final long k1bm = stop(clock, measured);
          sink += blockBitmap.length;
          clock = SpikeClock.start();
          final SpikeTermsCounter.Counts b1 = countBlock(BRAND_F, blockBitmap);
          top(b1, BRAND_F);
          final long k1bc = stop(clock, measured);

          // K = 5 page, heap: match once + 5 counts + 5 top-N
          clock = SpikeClock.start();
          final long[] pageBitmap = nativeMatch.get();
          final long k5m = stop(clock, measured);
          sink += pageBitmap.length;
          long k5t = k5m;
          for (int f = 0; f < K; f++) {
            clock = SpikeClock.start();
            final SpikeTermsCounter.Counts c = countHeap(f, pageBitmap);
            final List<SpikeTermsCounter.Bucket> t = top(c, f);
            final long n = stop(clock, measured);
            k5t += n;
            if (measured) {
              k5Count[f][slot] = n;
              k5Heap[f] = c;
              k5Top.set(f, t);
            }
          }

          // K = 5 page, brand and author on off-heap blocks, the others on the heap
          clock = SpikeClock.start();
          final long[] pageBlockBitmap = nativeMatch.get();
          final long k5bm = stop(clock, measured);
          sink += pageBlockBitmap.length;
          long k5bt = k5bm;
          for (int f = 0; f < K; f++) {
            clock = SpikeClock.start();
            final SpikeTermsCounter.Counts c = countBlock(f, pageBlockBitmap);
            final List<SpikeTermsCounter.Bucket> t = top(c, f);
            final long n = stop(clock, measured);
            k5bt += n;
            if (measured) {
              k5BlockCount[f][slot] = n;
              k5Block[f] = c;
              k5BlockTop.set(f, t);
            }
          }

          // SQL match source (run 1 method)
          clock = SpikeClock.start();
          final long[] positions = SpikeTermsCounter.positions(database, where);
          final long sm = stop(clock, measured);
          sink += positions.length;
          clock = SpikeClock.start();
          final long[] sqlBitmap = SpikeTermsCounter.matchBitmap(segments[BRAND_F], positions);
          final SpikeTermsCounter.Counts sc = SpikeTermsCounter.count(segments[BRAND_F], sqlBitmap, positions, dictionaries[BRAND_F].size());
          top(sc, BRAND_F);
          final long scn = stop(clock, measured);

          if (measured) {
            k1Match[slot] = k1m;
            k1Count[slot] = k1c;
            k1Total[slot] = k1m + k1c;
            k1BlockMatch[slot] = k1bm;
            k1BlockCount[slot] = k1bc;
            k1BlockTotal[slot] = k1bm + k1bc;
            k5Match[slot] = k5m;
            k5Total[slot] = k5t;
            k5BlockMatch[slot] = k5bm;
            k5BlockTotal[slot] = k5bt;
            sqlMatch[slot] = sm;
            sqlCount[slot] = scn;
            sqlTotal[slot] = sm + scn;
            lastBitmap = bitmap;
            k1Heap = c1;
            k1Top = t1;
            k1Block = b1;
            sqlCounts = sc;
          }
        } else {
          // SQL front-end floor: same statement shape as the gate, index-served, zero rows
          final long[] floorMissing = new long[1];
          SpikeClock clock = SpikeClock.start();
          final List<SpikeTermsCounter.Bucket> floor = SpikeTermsCounter.groupBy(database, FLOOR_WHERE, SpikeDataset.BRAND, TOP, floorMissing);
          final long floorNs = stop(clock, measured);
          sink += floor.size() + floorMissing[0];

          // K = 1 GROUP BY
          final long[] missing = new long[1];
          clock = SpikeClock.start();
          final List<SpikeTermsCounter.Bucket> g1 = SpikeTermsCounter.groupBy(database, where, SpikeDataset.BRAND, TOP, missing);
          final long g1n = stop(clock, measured);
          sink += g1.size() + missing[0];

          // K = 5 page: the five GROUP BY queries back to back, as an application renders the page
          final List<List<SpikeTermsCounter.Bucket>> page = new ArrayList<>(K);
          final long[] pageMissing = new long[K];
          final long[] pageNs = new long[K];
          final SpikeClock pageClock = SpikeClock.start();
          for (int f = 0; f < K; f++) {
            final long[] m = new long[1];
            clock = SpikeClock.start();
            final List<SpikeTermsCounter.Bucket> r = f == TAGS_F ?
                SpikeTermsCounter.groupByUnwind(database, where, SpikeDataset.TAGS, TOP) :
                SpikeTermsCounter.groupBy(database, where, FACETS[f], TOP, m);
            pageNs[f] = clock.stop();
            page.add(r);
            pageMissing[f] = m[0];
          }
          final long pageTotal = stop(pageClock, measured);
          for (int f = 0; f < K; f++)
            sink += page.get(f).size() + pageMissing[f];

          if (measured) {
            groupByK1[slot] = g1n;
            sqlFloor[slot] = floorNs;
            k1GroupByTop = g1;
            k1GroupByMissing = missing[0];
            groupByK5Total[slot] = pageTotal;
            for (int f = 0; f < K; f++) {
              groupByK5[f][slot] = pageNs[f];
              groupByTop.set(f, page.get(f));
              groupByMissing[f] = pageMissing[f];
            }
          }
        }
      }
    }

    // correctness on the last measured round: native match, every facet, both count paths, both sides of the timed round
    final long[] sqlPositions = SpikeTermsCounter.positions(database, where);
    assertThat(lastBitmap).as(label + " native match equals the SQL filter").containsExactly(
        SpikeTermsCounter.matchBitmap(segments[BRAND_F], sqlPositions));
    assertThat(uncovered[0]).as(label + " positions beyond the segment").isZero();
    final long matches = SpikeMatchSource.cardinality(lastBitmap);
    assertThat(matches).isEqualTo(sqlPositions.length);
    for (int f = 0; f < K; f++) {
      final long[] missing = new long[1];
      final List<SpikeTermsCounter.Bucket> oracle;
      if (f == TAGS_F) {
        oracle = SpikeTermsCounter.groupByUnwind(database, where, SpikeDataset.TAGS, Integer.MAX_VALUE);
        missing[0] = matches - countWithTags(where);
      } else {
        oracle = SpikeTermsCounter.groupBy(database, where, FACETS[f], Integer.MAX_VALUE, missing);
        assertThat(groupByMissing[f]).as(label + " " + FACETS[f] + " timed GROUP BY missing").isEqualTo(missing[0]);
      }
      assertCounts(label + " " + FACETS[f] + " K = 5 heap", k5Heap[f], f, oracle, missing[0]);
      assertCounts(label + " " + FACETS[f] + " K = 5 block", k5Block[f], f, oracle, missing[0]);
      assertThat(k5Top.get(f)).as(label + " " + FACETS[f] + " K = 5 heap top-N vs timed GROUP BY").containsExactlyElementsOf(groupByTop.get(f));
      assertThat(k5BlockTop.get(f)).as(label + " " + FACETS[f] + " K = 5 block top-N vs timed GROUP BY")
          .containsExactlyElementsOf(groupByTop.get(f));
      if (f == BRAND_F) {
        assertCounts(label + " brand K = 1 heap", k1Heap, f, oracle, missing[0]);
        assertCounts(label + " brand K = 1 block", k1Block, f, oracle, missing[0]);
        assertCounts(label + " brand SQL match source", sqlCounts, f, oracle, missing[0]);
        assertThat(sqlCounts.uncovered()).isZero();
        assertThat(k1Top).as(label + " brand K = 1 top-N vs timed GROUP BY").containsExactlyElementsOf(k1GroupByTop);
        assertThat(k1GroupByMissing).isEqualTo(missing[0]);
      }
    }

    final String header = RUNS + " runs after " + WARMUP + " warmup, orders balanced " + RUNS / 2 + "/" + RUNS / 2
        + ", median of even count = mean of middle two";
    final List<String[]> gate = new ArrayList<>();
    gate.add(row("nativeMatchMs (" + nativeName + ", bitmap)", k1Match));
    gate.add(row("countMs (brand, heap: count + top " + TOP + ")", k1Count));
    gate.add(row("native match + count", k1Total));
    gate.add(row("groupByMs (SQL GROUP BY brand)", groupByK1));
    gate.add(row("SQL floor (" + FLOOR_QUERY + ", zero rows)", sqlFloor));
    final long[] withFloor = new long[RUNS];
    for (int i = 0; i < RUNS; i++)
      withFloor[i] = k1Total[i] + sqlFloor[i];
    gate.add(row("native match + count + SQL floor (same round)", withFloor));
    gate.add(row("SQL match source (run 1 method): matchMs (SELECT @rid, positions)", sqlMatch));
    gate.add(row("SQL match source (run 1 method): countMs (bitmap + count + top " + TOP + ")", sqlCount));
    gate.add(row("SQL match source (run 1 method): match + count", sqlTotal));
    report.table("K = 1, the gate, " + label + " (ms, " + header + ")", new String[] { "phase", "median", "min", "all runs" }, gate);

    final List<String[]> pageRows = new ArrayList<>();
    pageRows.add(row("nativeMatchMs (once for the page)", k5Match));
    for (int f = 0; f < K; f++)
      pageRows.add(row("count + top " + TOP + " " + FACETS[f] + " (heap)", k5Count[f]));
    pageRows.add(row("projection page (match + 5 counts + 5 top-N)", k5Total));
    for (int f = 0; f < K; f++)
      pageRows.add(row("GROUP BY " + FACETS[f] + (f == TAGS_F ? " (UNWIND)" : ""), groupByK5[f]));
    pageRows.add(row("GROUP BY page (5 queries back to back)", groupByK5Total));
    report.table("K = 5 facet page, " + label + " (ms, " + header + ")", new String[] { "phase", "median", "min", "all runs" }, pageRows);

    final List<String[]> blockRows = new ArrayList<>();
    blockRows.add(row("K = 1 nativeMatchMs", k1BlockMatch));
    blockRows.add(row("K = 1 countBlocks brand + top " + TOP + " (off-heap)", k1BlockCount));
    blockRows.add(row("K = 1 native match + countBlocks", k1BlockTotal));
    blockRows.add(row("K = 5 nativeMatchMs", k5BlockMatch));
    for (int f = 0; f < K; f++)
      blockRows.add(row("K = 5 count + top " + TOP + " " + FACETS[f] + (f == BRAND_F || f == AUTHOR_F ? " (off-heap blocks)" : " (heap)"),
          k5BlockCount[f]));
    blockRows.add(row("K = 5 page with blocks", k5BlockTotal));
    report.table("Block variant, " + label + " (ms, " + header + ")", new String[] { "phase", "median", "min", "all runs" }, blockRows);

    final Outcome outcome = new Outcome(median(k1Total), median(groupByK1), median(k5Total), median(groupByK5Total), median(k1Count),
        median(k1BlockCount), median(k5Total), median(k5BlockTotal), median(sqlFloor),
        median(withFloor));
    report.row("matches (" + label + ")", String.valueOf(matches), String.format("%.2f%% of %d", 100.0 * matches / RECORDS, RECORDS));
    report.row("JVM stall during measured calls (" + label + ")", SpikeClock.ms(stall) + " ms", "already discounted, context only");
    report.row("countMs / groupByMs (" + label + ")", outcome.k1GroupBy() == 0 ? "n/a" : String.format("%.4f", outcome.heapK1Count() / outcome.k1GroupBy()),
        "projection own share, on top of the match");
    report.row("gate K = 1 (" + label + ")", SpikeClock.ms(outcome.k1Projection()) + " ms vs " + SpikeClock.ms(outcome.k1GroupBy()) + " ms",
        outcome.gate() ? "projection wins" : "projection loses");
    report.row("gate K = 1 including SQL floor (" + label + ")", SpikeClock.ms(outcome.k1WithFloor()) + " ms vs "
        + SpikeClock.ms(outcome.k1GroupBy()) + " ms", (outcome.gateWithFloor() ? "projection wins" : "projection loses") + ", reported, not decisive");
    report.row("SQL match source K = 1 (" + label + ")", SpikeClock.ms(median(sqlTotal)) + " ms vs " + SpikeClock.ms(outcome.k1GroupBy()) + " ms",
        "run 1 method, for reference");
    report.row("K = 5 page (" + label + ")", SpikeClock.ms(outcome.k5Projection()) + " ms vs " + SpikeClock.ms(outcome.k5GroupBy()) + " ms",
        outcome.k5Projection() < outcome.k5GroupBy() ? "projection wins" : "projection loses");
    report.row("block overhead (" + label + ")", percent(outcome.blockK1Count(), outcome.heapK1Count()) + " K = 1 count, "
        + percent(outcome.blockK5(), outcome.heapK5()) + " K = 5 page", "off-heap blocks vs heap segment");
    return outcome;
  }

  private void planChecks(final String label, final String name, final String sql, final boolean withProfile) {
    final String plan = explain(sql);
    report.text("EXPLAIN of the " + name + " query: `" + sql + "`");
    report.text("```");
    report.text(plan);
    report.text("```");
    indexCheck(label, name + " EXPLAIN", plan);
    if (withProfile) {
      final String profile = profile(sql);
      report.text("PROFILE of the " + name + " query (run once, outside the measured rounds, actual runtime plan):");
      report.text("```");
      report.text(profile);
      report.text("```");
      indexCheck(label, name + " PROFILE", profile);
    }
    report.text("");
  }

  private static String groupByQuery(final String where, final int facet) {
    return facet == TAGS_F ? SpikeTermsCounter.groupByUnwindQuery(where, SpikeDataset.TAGS) : SpikeTermsCounter.groupByQuery(where, FACETS[facet]);
  }

  private SpikeTermsCounter.Counts countHeap(final int facet, final long[] bitmap) {
    if (facet == TAGS_F)
      return SpikeTermsCounter.countMulti(tagSegment, bitmap, dictionaries[facet].size());
    return SpikeTermsCounter.count(segments[facet], bitmap, NO_POSITIONS, dictionaries[facet].size());
  }

  private SpikeTermsCounter.Counts countBlock(final int facet, final long[] bitmap) {
    if (facet == BRAND_F)
      return SpikeTermsCounter.countBlocks(brandBlocks, bitmap, dictionaries[facet].size());
    if (facet == AUTHOR_F)
      return SpikeTermsCounter.countBlocks(authorBlocks, bitmap, dictionaries[facet].size());
    return countHeap(facet, bitmap);
  }

  /** Top-N with string resolution of the returned values only, folded into the sink. */
  private List<SpikeTermsCounter.Bucket> top(final SpikeTermsCounter.Counts counts, final int facet) {
    final List<SpikeTermsCounter.Bucket> top = SpikeTermsCounter.topN(counts, dictionaries[facet], TOP);
    sink += counts.missing() + (top.isEmpty() ? 0 : top.get(0).count());
    return top;
  }

  private long stop(final SpikeClock clock, final boolean measured) {
    final long nanos = clock.stop();
    if (measured)
      stall += clock.stallNanos();
    return nanos;
  }

  /** Compares every ordinal (not only the top N, which would be quadratic at high cardinality) and the missing count. */
  private void assertCounts(final String what, final SpikeTermsCounter.Counts counts, final int facet,
      final List<SpikeTermsCounter.Bucket> oracle, final long missing) {
    final SpikeDictionary dictionary = dictionaries[facet];
    final int[] expected = new int[dictionary.size()];
    for (final SpikeTermsCounter.Bucket bucket : oracle) {
      final int ordinal = dictionary.ordinalOf(bucket.value());
      assertThat(ordinal).as(what + " value " + bucket.value() + " in the dictionary").isGreaterThanOrEqualTo(0);
      expected[ordinal] = (int) bucket.count();
    }
    assertThat(counts.byOrdinal()).as(what).containsExactly(expected);
    assertThat(counts.missing()).as(what + " missing").isEqualTo(missing);
  }

  private long countWithTags(final String where) {
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS cnt FROM " + SpikeDataset.TYPE + " WHERE (" + where + ") AND tags IS NOT NULL AND tags.size() > 0")) {
      return ((Number) rs.next().getProperty("cnt")).longValue();
    }
  }

  private void pageReread(final LocalBucket bucket) {
    final int totalPages = bucket.getTotalPages();
    final int pages = Math.min(REREAD_PAGES, totalPages);
    long records = 0;
    final SpikeClock clock = SpikeClock.start();
    for (int i = 0; i < pages; i++) {
      final int page = (int) ((long) i * totalPages / pages);
      final Iterator<Record> it = bucket.iterator(page, page + 1);
      while (it.hasNext()) {
        final String brand = ((Document) it.next()).getString(SpikeDataset.BRAND);
        sink += brand == null ? 0 : brand.length();
        records++;
      }
    }
    final long nanos = clock.stop();
    report.text("");
    report.text("### Page re-read cost (spec 6.3)");
    report.text("");
    report.text("Note: the pages are warm (the benchmark just scanned them), so this is a lower bound of a cold re-read.");
    report.text("");
    report.row("pages re-read", String.valueOf(pages), "evenly spread over " + totalPages);
    report.row("avg records per page", String.format("%.1f", (double) records / pages), "");
    report.row("total", SpikeClock.ms(nanos) + " ms", "stall " + SpikeClock.ms(clock.stallNanos()) + " ms");
    report.row("per page", String.format("%.2f us", nanos / 1000.0 / pages), "");
    report.row("per record", String.format("%.4f us", records == 0 ? 0.0 : nanos / 1000.0 / records), "");
  }

  private void memory() {
    final SpikeSegment segment = segments[BRAND_F];
    final long brandArray = dictionaries[BRAND_F].size() * 4L;
    final long authorArray = dictionaries[AUTHOR_F].size() * 4L;
    long pageArrays = 0;
    for (int f = 0; f < K; f++)
      pageArrays += dictionaries[f].size() * 4L;
    final long bitmap = segment.presence().length * 8L;
    report.text("");
    report.text("### Memory of count arrays (spec 5.7)");
    report.text("");
    report.row("brand int[cardinality]", brandArray + " bytes", "cardinality " + dictionaries[BRAND_F].size());
    report.row("author int[AUTHOR_CARDINALITY]", SpikeDataset.AUTHOR_CARDINALITY * 4L + " bytes",
        "high-cardinality facet, per query at the nominal cardinality " + SpikeDataset.AUTHOR_CARDINALITY);
    report.row("author int[measured cardinality]", authorArray + " bytes", "measured dictionary size " + dictionaries[AUTHOR_F].size());
    report.row("match bitmap", bitmap + " bytes", segment.pageCount() + " pages x " + segment.maxRecordsInPage() / 64 + " words x 8");
    report.row("measured per K = 1 query", (brandArray + bitmap) + " bytes", "brand count array + match bitmap");
    report.row("measured per K = 5 page", (pageArrays + bitmap) + " bytes", "five count arrays + one match bitmap");
    report.row("spec extrapolation (author, measured)", (authorArray * TASKS * QUERIES) + " bytes",
        "4 bytes x " + dictionaries[AUTHOR_F].size() + " ordinals x " + TASKS + " tasks x " + QUERIES + " queries");
    report.row("spec extrapolation (spec figure)", (4L * 1_000_000 * TASKS * QUERIES) + " bytes",
        "4 bytes x 1,000,000 ordinals x " + TASKS + " tasks x " + QUERIES + " queries");
  }

  /** The index step itself, not the filter text: every plan prints "fileSize < 10000", even a full scan. */
  private void indexCheck(final String label, final String planName, final String plan) {
    final boolean usesIndex = plan.contains("FETCH FROM INDEX") && plan.contains("[" + SpikeDataset.FILE_SIZE + "]");
    if (usesIndex)
      report.text("Index check (" + label + ", " + planName + "): uses the fileSize index.");
    else
      report.text("WARNING: index check (" + label + ", " + planName + "): does NOT use the fileSize index"
          + (label.equals("broad") ? " (expected, the broad predicate has no index)" : " (unexpected, " + label + " is index-served)"));
  }

  private String profile(final String sql) {
    try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
      return rs.hasNext() ? rs.next().<String>getProperty("executionPlanAsString") : "(no plan)";
    }
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.hasNext() ? rs.next().<String>getProperty("executionPlanAsString") : "(no plan)";
    }
  }

  private static String percent(final double value, final double base) {
    return base == 0 ? "n/a" : String.format("%+.1f%%", 100.0 * (value - base) / base);
  }

  private static String[] row(final String name, final long[] values) {
    final long[] sorted = values.clone();
    Arrays.sort(sorted);
    final String[] all = new String[values.length];
    for (int i = 0; i < values.length; i++)
      all[i] = SpikeClock.ms(values[i]);
    return new String[] { name, SpikeClock.ms(median(values)), SpikeClock.ms(sorted[0]), String.join(", ", all) };
  }

  private static double median(final long[] values) {
    final long[] sorted = values.clone();
    Arrays.sort(sorted);
    final int n = sorted.length;
    return n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
  }
}
