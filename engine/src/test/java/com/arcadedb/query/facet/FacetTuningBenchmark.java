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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.index.columnar.BenchmarkReport;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntUnaryOperator;
import java.util.logging.Level;

/**
 * Report-only benchmarks of the query-side tuning levers (plan 7, #10662): one method per lever, each timing the shipped path in
 * on/off pairs of the same code over the same data inside one JVM ({@link BenchmarkReport#pairs}: the pair is the noise of the run),
 * never asserting a time. The levers' switches are gone since the tuning was measured (verdicts: the roadmap's tuning outcome). This
 * class holds the shared fixture. Run one with {@code columnar-bench.sh <label> <rounds> tuning:<method>}.
 * <p>
 * Fixture: {@code -DfacetBench.records} (default 200,000) {@code Asset} records of {@link ColumnarTestData} (seed 42, tag vocabulary
 * 800, as {@link FacetsVsGroupByBenchmark}) in the requested bucket count, with an LSM index on {@code fileSize}, caught up. The
 * report of a method is {@code FacetTuningBenchmark.<method>}, written after the method when {@code -Dcolumnar.bench.label} is set.
 */
@Tag("benchmark")
class FacetTuningBenchmark extends TestHelper {
  protected static final String TYPE   = ColumnarTestData.TYPE;
  protected static final int    WARMUP = 2;
  private static final long     TRICKLE_MS      = 40_000;
  private static final String[] TRICKLE_FIGURES = { "flushCount", "mergeCount", "flushMs", "mergeMs", "maintenanceCpuMs", "segments",
      "catchUpMs", "commitMs" };
  private static final String[] TRICKLE_UNITS   = { "count", "count", "ms", "ms", "ms", "count", "ms", "ms" };

  protected final int       rounds = BenchmarkReport.rounds();
  protected BenchmarkReport report;

  @BeforeEach
  void createReport(final TestInfo info) {
    report = new BenchmarkReport("FacetTuningBenchmark." + info.getTestMethod().map(m -> m.getName()).orElse("unknown"));
  }

  @Override
  protected void endTest() {
    final Path written = report == null ? null : report.write();
    if (written != null)
      LogManager.instance().log(this, Level.INFO, "FACET-TUNING report written to %s", written);
  }

  /** Loads the shared data set into the database and waits for the projections to catch up; returns the record count. */
  protected int loadAssets(final int buckets) {
    final int records = Integer.getInteger("facetBench.records", 200_000);
    final ColumnarTestData data = new ColumnarTestData(42);
    data.buckets = buckets;
    data.tagVocabulary = 800;
    data.createSchema(database);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) NOTUNIQUE");
    data.insertAssets(database, records);
    FacetOracle.awaitCaughtUp(database);
    return records;
  }

  /**
   * Runs the query {@value #WARMUP} warm-up rounds then {@link #rounds} measured ones, each round twice (variants on and off, the same
   * code) in rotating order, and logs the median of each variant in stall-discounted ms.
   */
  protected void measure(final String metric, final Runnable query) {
    report.pairs(WARMUP, rounds, metric, () -> BenchmarkReport.effectiveNanos(query));
    logMedians(metric);
  }

  /**
   * Task 9 Step 6, rank-view cadence: pages through the 60,000 values of a terms facet, TOP 100, the first page {@code ORDER BY VALUE}
   * and every next one {@code AFTER :c} with the cursor the previous page returned (600 pages, one walk per sample), over values whose
   * rank view covers the first 50,000 only ({@code Tail}: 10,000 appended values interleaved with them, the unranked tail a rebuild every
   * {@code RANK_VIEW_MIN_INTERVAL_MS} leaves), against the same values after a forced rebuild ({@code Ranked}). The variant picks the
   * type. {@code tailVsRanked.totalMs} pages {@code Tail} as "on" and {@code Ranked} as "off"; {@code
   * rankedVsRanked.totalMs} pages {@code Ranked} in both variants, the A-vs-A noise of the same run. The apply passes (and with them
   * the maintenance that would rebuild the tail) are suspended while the walks run, everything caught up; the fixture of {@code Tail}
   * runs on a frozen maintenance clock (see the body). {@code rankRebuild60k.ms} (variant "on", one sample, for information) is the
   * cost of the rebuild the cadence pays: the {@link GlobalDictionary#rebuildRankView()} of the 60,000 values of {@code Tail}, after the
   * walks.
   */
  @Test
  void afterPagingRankCadence() throws Exception {
    final String ranked = createValueType("Ranked");
    insertValues(ranked, 50_000, i -> i * 2);
    insertValues(ranked, 10_000, i -> i * 10 + 1);
    FacetOracle.awaitCaughtUp(database);
    dictionary(ranked).rebuildRankView();

    // a frozen maintenance clock: the first rank-view rebuild the maintenance stamps throttles every later one, so the 10,000 appended
    // after the forced rebuild of the 50,000 stay an unranked tail
    final long frozenMs = System.currentTimeMillis();
    ColumnarTestAccess.setClock(manager(), () -> frozenMs);
    try {
      final String tail = createValueType("Tail");
      insertValues(tail, 50_000, i -> i * 2);
      FacetOracle.awaitCaughtUp(database);
      final GlobalDictionary tailDictionary = dictionary(tail);
      tailDictionary.rebuildRankView();
      insertValues(tail, 10_000, i -> i * 10 + 1);
      FacetOracle.awaitCaughtUp(database);
      try (final AutoCloseable held = manager().suspendApply()) {
        checkRankView(tailDictionary, 50_000, 60_000);
        final GlobalDictionary rankedDictionary = dictionary(ranked);
        checkRankView(rankedDictionary, 60_000, 60_000);
        report.pairs(WARMUP, rounds, recorder -> {
          final boolean on = BenchmarkReport.ON.equals(recorder.variant());
          recorder.nanos("tailVsRanked.totalMs", BenchmarkReport.effectiveNanos(() -> pageThrough(on ? tail : ranked, 60_000)));
          recorder.nanos("rankedVsRanked.totalMs", BenchmarkReport.effectiveNanos(() -> pageThrough(ranked, 60_000)));
        });
        checkRankView(tailDictionary, 50_000, 60_000);
        checkRankView(rankedDictionary, 60_000, 60_000);
        // one sample, for information: the rebuild is a no-op once a view covers every value, so only the tail can be timed, once
        report.add("rankRebuild60k.ms", "ms", BenchmarkReport.ON, BenchmarkReport.effectiveNanos(tailDictionary::rebuildRankView) / 1_000_000d);
        checkRankView(tailDictionary, 60_000, 60_000);
      }
    } finally {
      ColumnarTestAccess.setClock(manager(), System::currentTimeMillis);
    }
    logMedians("tailVsRanked.totalMs");
    logMedians("rankedVsRanked.totalMs");
    LogManager.instance().log(this, Level.INFO, String.format(Locale.ROOT, "FACET-TUNING rankRebuild60k.ms %.1f ms",
        report.samples("rankRebuild60k.ms", BenchmarkReport.ON)[0]));
  }

  /**
   * Task 10, {@code facet.searchValues} direct count path: 20,000 values of one COLUMNAR string property
   * ({@code <letter><4 digits>}, one record each, 769 or 770 per letter, one bucket, caught up), searched by each of the 26 one-letter
   * prefixes with limit 20 (one batch of 40 candidates per call, every one counted 1, so 20 rows). One sample is 100 passes over the 26
   * prefixes (2,600 calls, long enough that the JIT state of the two variants settles in the warm-up rounds); {@code
   * searchValues.msPerCall} is its time divided by 2,600. The run throws unless every call returned 20 rows
   * starting with its prefix.
   */
  @Test
  void searchValues() {
    final String type = createValueType("Suggest");
    for (int from = 0; from < 20_000; from += 5_000) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < start + 5_000; i++)
          database.newDocument(type).set("v", String.format(Locale.ROOT, "%c%04d", (char) ('a' + i % 26), i / 26)).save();
      });
    }
    FacetOracle.awaitCaughtUp(database);
    final int passes = 100;
    report.pairs(WARMUP, rounds, recorder -> recorder.nanos("searchValues.msPerCall",
        BenchmarkReport.effectiveNanos(() -> {
          for (int pass = 0; pass < passes; pass++)
            for (char letter = 'a'; letter <= 'z'; letter++)
              suggest(type, String.valueOf(letter));
        }) / (passes * 26L)));
    logMedians("searchValues.msPerCall");
  }

  /**
   * Task 12, one hierarchy view per query, the query variant of Task 11's {@code hierarchyBuild}: two
   * HIERARCHY facets of one property ({@code TOP 10} and {@code TOP 20}) over {@code -DfacetBench.records} (default 200,000) assets in 3
   * buckets, up to 4 tags each drawn from a 4 x 4 tag forest (340 paths), caught up and with a fresh view (no build is timed). The query
   * takes the view once instead of once per collector; {@code hierarchyQuery.ms} is the time of one query, averaged over
   * the 10 queries of a sample. The run throws unless every query answers both facets from the index with the counts of the first.
   */
  @Test
  void hierarchyQuery() {
    final int records = Integer.getInteger("facetBench.records", 200_000);
    final ColumnarTestData data = new ColumnarTestData(42);
    data.buckets = 3;
    data.maxTags = 4;
    data.tagHierarchy = new JSONObject().put("vertexType", ColumnarTestData.TAG_TYPE).put("key", "path")
        .put("parentEdge", ColumnarTestData.PARENT_EDGE);
    data.createSchema(database);
    data.tagValues = ColumnarTestData.createTagTree(database, 42, 4, 4);
    data.insertAssets(database, records);
    FacetOracle.awaitCaughtUp(database);
    final String sql = "SELECT FROM " + TYPE + " FACETS t: tags HIERARCHY TOP 10, u: tags HIERARCHY TOP 20";
    final List<String> first = hierarchyQuery(sql, null);
    final int queries = 10;
    report.pairs(WARMUP, rounds, recorder -> recorder.nanos("hierarchyQuery.ms", BenchmarkReport.effectiveNanos(() -> {
      for (int q = 0; q < queries; q++)
        hierarchyQuery(sql, first);
    }) / queries));
    logMedians("hierarchyQuery.ms");
  }

  /**
   * Task 13, read events fire once per matched record: {@code -DfacetBench.records} (default 200,000)
   * assets in 3 buckets with an AFTER READ listener on the type that leaves each record as it is, queried with a broad WHERE ({@code
   * fileSize > 409600}, about 90% of the records) and three facets (brand and author TERMS, fileSize STATS), all scanned (reason
   * {@code read_events}). The facets are counted from the rows the WHERE read (before Task 13, every match was read a second time by
   * the fallback scan, firing the listener again). {@code readEventType.ms} is the time of one query, averaged over the 3 queries of a
   * sample. The run throws unless every facet is scanned with the counts of the first query, and the listener fires once per match.
   */
  @Test
  void readEventType() {
    loadAssets(3);
    final String where = "fileSize > 409600";
    final String sql = "SELECT FROM " + TYPE + " WHERE " + where + " FACETS b: brand TERMS TOP 10, a: author TERMS TOP 10, s: fileSize STATS";
    final long matches;
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM " + TYPE + " WHERE " + where)) {
      matches = rs.next().<Number>getProperty("n").longValue();
    }
    final Thread caller = Thread.currentThread();
    final long[] reads = new long[1];
    final AfterRecordReadListener listener = record -> {
      if (Thread.currentThread() == caller)
        reads[0]++;
      return record;
    };
    database.getSchema().getType(TYPE).getEvents().registerListener(listener);
    try {
      final List<String> first = readEventQuery(sql, null, reads, matches);
      final int queries = 3;
      report.pairs(WARMUP, rounds, recorder -> recorder.nanos("readEventType.ms", BenchmarkReport.effectiveNanos(() -> {
        for (int q = 0; q < queries; q++)
          readEventQuery(sql, first, reads, matches);
      }) / queries));
    } finally {
      database.getSchema().getType(TYPE).getEvents().unregisterListener(listener);
    }
    logMedians("readEventType.ms");
  }

  /**
   * Runs the query of {@link #readEventType()}; throws unless every facet is scanned (with the counts of {@code expected}) and the
   * listener fired once per match.
   */
  private List<String> readEventQuery(final String sql, final List<String> expected, final long[] reads, final long matches) {
    reads[0] = 0;
    final FacetResults results;
    try (final ResultSet rs = database.query("sql", sql)) {
      results = rs.getFacets();
    }
    final List<String> counts = new ArrayList<>();
    for (final String alias : List.of("b", "a", "s")) {
      final FacetResult result = results.get(alias);
      if (!"scan".equals(result.source()))
        throw new IllegalStateException("facet " + alias + " " + result.source() + ": " + result.reason());
      if (result.stats() != null)
        counts.add(alias + "=" + result.stats());
      for (final FacetBucket bucket : result.buckets())
        counts.add(alias + ":" + bucket.value() + "=" + bucket.count());
    }
    if (reads[0] != matches)
      throw new IllegalStateException("the listener fired " + reads[0] + " times for " + matches + " matches, expected " + matches);
    if (expected != null && !expected.equals(counts))
      throw new IllegalStateException("facet counts changed: " + counts + " instead of " + expected);
    return counts;
  }

  /** Runs the query of {@link #hierarchyQuery()}; throws unless both facets are answered from the index (as {@code expected}). */
  private List<String> hierarchyQuery(final String sql, final List<String> expected) {
    final FacetResults results;
    try (final ResultSet rs = database.query("sql", sql)) {
      results = rs.getFacets();
    }
    final FacetResult t = results.get("t");
    final FacetResult u = results.get("u");
    if (!"index".equals(t.source()) || !"index".equals(u.source()) || t.buckets().size() != 10 || u.buckets().size() != 20)
      throw new IllegalStateException("hierarchy facets " + t.source() + "/" + u.source() + ": " + t.reason() + " " + u.reason());
    final List<String> counts = new ArrayList<>(u.buckets().size());
    for (final FacetBucket bucket : u.buckets())
      counts.add(bucket.value() + "=" + bucket.count());
    if (expected != null && !expected.equals(counts))
      throw new IllegalStateException("hierarchy counts changed: " + counts + " instead of " + expected);
    return counts;
  }

  /**
   * Task 3 of #10662, flush coalescing at a low ingest rate, the shape of the plan 6 load tests (a few dozen overlay rows per flush): per
   * round a fresh database, 20,000 records loaded, caught up and flushed, then {@value #TRICKLE_MS} ms of commits of 5 updates and 5
   * inserts every 20 ms (500 rows per second), then the catch-up. The window is longer than a 30 s flushMaxAge, so an age-triggered
   * flush lands inside it. The flush settings are compared by whole runs: those given as system
   * properties ({@code arcadedb.columnar.flush*}) apply to every round (TestHelper resets the configuration after each test). Variant
   * {@code run}, per round: {@code trickle.flushCount}, {@code mergeCount}, {@code flushMs}, {@code mergeMs}, {@code maintenanceCpuMs}
   * (the {@code ArcadeDB-ColumnarMaintenance-*} threads), all over the trickle and its catch-up, {@code segments} published at the end,
   * {@code catchUpMs} (the apply lag left at the end of the trickle) and {@code commitMs} (the median commit of the trickle).
   */
  @Test
  void flushTrickle() throws Exception {
    final GlobalConfiguration[] settings = { GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS,
        GlobalConfiguration.COLUMNAR_FLUSH_MIN_ROWS, GlobalConfiguration.COLUMNAR_FLUSH_MAX_AGE };
    for (final GlobalConfiguration setting : settings) {
      final String value = System.getProperty(setting.getKey());
      if (value != null)
        setting.setValue(value);
    }
    for (int round = 0; round < WARMUP + rounds; round++) {
      if (round > 0) {
        database.drop();
        database = factory.create();
      }
      final long[] figures = trickle();
      if (round < WARMUP)
        continue;
      for (int i = 0; i < TRICKLE_FIGURES.length; i++)
        report.add("trickle." + TRICKLE_FIGURES[i], TRICKLE_UNITS[i], "run", figures[i]);
    }
    for (final String figure : TRICKLE_FIGURES)
      LogManager.instance().log(this, Level.INFO, String.format(Locale.ROOT, "FACET-TUNING trickle.%s: median %.1f", figure,
          BenchmarkReport.median(report.samples("trickle." + figure, "run"))));
  }

  /** One round of {@link #flushTrickle} on the current (fresh) database; the figures in the order of {@link #TRICKLE_FIGURES}. */
  private long[] trickle() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 20_000);
    final ColumnarIndexManager manager = manager();
    FacetOracle.awaitCaughtUp(database);
    manager.flushNow();
    final ColumnarStats stats = manager.getStats();
    final long flushes = stats.flushCount(TYPE);
    final long merges = stats.mergeCount(TYPE);
    final long flushNanos = stats.flushDurationTotalNanos(TYPE);
    final long mergeNanos = stats.mergeDurationTotalNanos(TYPE);
    final long maintenanceNanos = maintenanceCpuNanos();

    final Random random = new Random(42);
    final long[] commits = new long[(int) (TRICKLE_MS / 20)];
    final long start = System.nanoTime();
    for (int commit = 0; commit < commits.length; commit++) {
      LockSupport.parkNanos(start + commit * 20_000_000L - System.nanoTime());
      final long begin = System.nanoTime();
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          rids.get(random.nextInt(rids.size())).asDocument(true).modify().set("brand", "brand-" + random.nextInt(200)).save();
      });
      rids.addAll(data.insertAssets(database, 5));
      commits[commit] = System.nanoTime() - begin;
    }
    final StallAwareStopwatch catchUp = StallAwareStopwatch.start();
    if (!manager.awaitCaughtUp(600_000))
      throw new IllegalStateException("The columnar projections did not catch up");
    final long catchUpMs = catchUp.effectiveMs();
    Arrays.sort(commits);
    return new long[] { stats.flushCount(TYPE) - flushes, stats.mergeCount(TYPE) - merges,
        (stats.flushDurationTotalNanos(TYPE) - flushNanos) / 1_000_000L, (stats.mergeDurationTotalNanos(TYPE) - mergeNanos) / 1_000_000L,
        (maintenanceCpuNanos() - maintenanceNanos) / 1_000_000L, stats.segments(TYPE), catchUpMs, commits[commits.length / 2] / 1_000_000L };
  }

  /** CPU nanoseconds so far of the live {@code ArcadeDB-ColumnarMaintenance-*} threads (the pool keeps its threads). */
  private static long maintenanceCpuNanos() {
    final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    long total = 0;
    for (final ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds(), 0))
      if (info != null && info.getThreadName().startsWith("ArcadeDB-ColumnarMaintenance-"))
        total += Math.max(0, threads.getThreadCpuTime(info.getThreadId()));
    return total;
  }

  /** One {@code facet.searchValues} call, limit 20; throws unless it returned 20 values starting with the prefix. */
  private void suggest(final String type, final String prefix) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT expand(facet.searchValues(:t, 'v', :p, 20))", Map.of("t", type, "p", prefix))) {
      while (rs.hasNext()) {
        final String value = rs.next().getProperty("value");
        if (!value.startsWith(prefix))
          throw new IllegalStateException("Value " + value + " for prefix " + prefix);
        rows++;
      }
    }
    if (rows != 20)
      throw new IllegalStateException(rows + " values for prefix " + prefix + ", expected 20");
  }

  private String createValueType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".v STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (v) COLUMNAR");
    return type;
  }

  /** {@code count} records, record {@code i} holding the value {@code v<value.applyAsInt(i)>} (six digits), 5,000 per transaction. */
  private void insertValues(final String type, final int count, final IntUnaryOperator value) {
    for (int from = 0; from < count; from += 5_000) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(count, start + 5_000); i++)
          database.newDocument(type).set("v", String.format(Locale.ROOT, "v%06d", value.applyAsInt(i))).save();
      });
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private GlobalDictionary dictionary(final String type) {
    return manager().projection(database.getSchema().getType(type).getBuckets(false).getFirst().getFileId()).dictionary("v");
  }

  private static void checkRankView(final GlobalDictionary dictionary, final int ranked, final int size) {
    if (dictionary.rankViewSize() != ranked || dictionary.size() != size)
      throw new IllegalStateException(
          "Rank view covers " + dictionary.rankViewSize() + " of " + dictionary.size() + " values, the fixture needs " + ranked + " of " + size);
  }

  /** Pages the facet of {@code v} TOP 100 until the cursor is null; throws unless every value was returned once. */
  private void pageThrough(final String type, final int values) {
    Object cursor = null;
    int seen = 0;
    Object last = null;
    do {
      final String sql = "SELECT FROM " + type + " FACETS f: v TERMS TOP 100" + (cursor == null ? " ORDER BY VALUE" : " AFTER :c");
      try (final ResultSet rs = database.query("sql", sql, cursor == null ? Map.of() : Map.of("c", cursor))) {
        final FacetResult result = rs.getFacets().get("f");
        for (final FacetBucket bucket : result.buckets()) {
          if (bucket.isMissing())
            continue;
          if (last != null && ((String) bucket.value()).compareTo((String) last) <= 0)
            throw new IllegalStateException("Value " + bucket.value() + " after " + last);
          last = bucket.value();
          seen++;
        }
        cursor = result.after();
      }
    } while (cursor != null);
    if (seen != values)
      throw new IllegalStateException("Paged " + seen + " values of " + values);
  }

  /** Logs the median of each variant of the metric, in stall-discounted ms. */
  private void logMedians(final String metric) {
    LogManager.instance().log(this, Level.INFO, String.format(Locale.ROOT, "FACET-TUNING %s: median on %.1f ms, off %.1f ms",
        metric, BenchmarkReport.median(report.samples(metric, BenchmarkReport.ON)),
        BenchmarkReport.median(report.samples(metric, BenchmarkReport.OFF))));
  }
}
