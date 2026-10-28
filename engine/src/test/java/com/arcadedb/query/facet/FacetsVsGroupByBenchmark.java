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
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Report-only benchmark: the shipped FACETS path against the three equivalent GROUP BY queries, for a selective WHERE (LSM index on
 * fileSize), a broad one and none, with one bucket and with three, at lag 0 and while the projection lags (apply suspended, 5,000
 * pending mutations). Timings are logged, never asserted; correctness (facets equal GROUP BY) is asserted on the last round only.
 * <p>
 * Run: {@code mvn -o -pl engine -am test -Dtest=FacetsVsGroupByBenchmark -DexcludedGroups=none -DfacetBench.records=200000
 * -Dsurefire.failIfNoSpecifiedTests=false}
 */
@Tag("benchmark")
class FacetsVsGroupByBenchmark extends TestHelper {
  private static final String TYPE         = ColumnarTestData.TYPE;
  private static final int    WARMUP       = 2;
  private static final int    ROUNDS       = 5;
  private static final int    LAG_UPDATES  = 5_000;
  private static final int    TOP          = 1_000;
  private static final long   MIB          = 1_048_576L;
  private static final String FACETS_CLAUSE = "FACETS brand: brand TERMS TOP " + TOP + ", tags: tags TERMS TOP " + TOP + ", size: fileSize RANGES ([0, "
      + MIB + "), [" + MIB + ", " + 2 * MIB + "), [" + 2 * MIB + ", " + 3 * MIB + "), [" + 3 * MIB + ", *))";

  private record Case(String name, String where) {
  }

  private record Timing(double median, long min) {
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  @Test
  @Timeout(3_600)
  void oneBucket() throws Exception {
    run(1);
  }

  @Test
  @Timeout(3_600)
  void threeBuckets() throws Exception {
    run(3);
  }

  private void run(final int buckets) throws Exception {
    final int records = Integer.getInteger("facetBench.records", 1_000_000);
    final ColumnarTestData data = new ColumnarTestData(42);
    data.buckets = buckets;
    data.tagVocabulary = 800;
    data.createSchema(database);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) NOTUNIQUE");
    data.insertAssets(database, records);
    FacetOracle.awaitCaughtUp(database);

    final List<Case> cases = List.of(new Case("selective", "fileSize < 10000"), new Case("broad", "brand <> 'brand-1'"),
        new Case("none", null));
    for (final Case c : cases)
      assertThat(count(c.where)).as("records matching " + c.name).isPositive();
    log("INFO records=" + records + " buckets=" + buckets + " selective matches=" + count("fileSize < 10000") + " broad matches="
        + count("brand <> 'brand-1'"));

    for (final Case c : cases)
      measure(buckets, "lag0", c);

    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, LAG_UPDATES);
      // default settings: a page-granular pending set covers most matches of a broad WHERE (about 60% at 200k records), and
      // patchBudgetRatio 1 lets a query patch up to all of its matches, so every case is patched and exact
      for (final Case c : cases)
        measure(buckets, "lagging", c, true);
      // the absolute budget alone (ratio 0): a broad patch set exceeds it and the answer is counted from the projection, inexact
      final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
      try {
        for (final Case c : cases)
          measure(buckets, "lag-abs", c, false);
      } finally {
        GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
      }
    }
    FacetOracle.awaitCaughtUp(database);
  }

  private void measure(final int buckets, final String lag, final Case c) {
    measure(buckets, lag, c, false);
  }

  /** @param mustBeExact whether the settings of the round promise an exact answer: then an inexact one fails the run */
  private void measure(final int buckets, final String lag, final Case c, final boolean mustBeExact) {
    final String filter = c.where == null ? "" : " WHERE " + c.where;
    final String facetSql = "SELECT @rid FROM " + TYPE + filter + " " + FACETS_CLAUSE + " LIMIT 20";
    final FacetResults[] last = new FacetResults[1];
    final Timing facets = time(() -> {
      try (final ResultSet rs = database.query("sql", facetSql)) {
        while (rs.hasNext())
          rs.next();
        last[0] = rs.getFacets();
      }
    });

    final Map<Object, Long>[] brandCounts = new Map[1];
    final Map<Object, Long>[] tagCounts = new Map[1];
    final Map<Long, Long>[] sizeCounts = new Map[1];
    final Timing groupBy = time(() -> {
      brandCounts[0] = groupBy("SELECT brand AS v, count(*) AS c FROM " + TYPE + filter + " GROUP BY brand");
      // each tag once per record, as the facet counts documents
      tagCounts[0] = groupBy("SELECT t AS v, count(*) AS c FROM (SELECT tags.asSet() AS t FROM " + TYPE + filter + " UNWIND t) GROUP BY t");
      final Map<Object, Long> sizes = groupBy("SELECT fileSize / " + MIB + " AS v, count(*) AS c FROM " + TYPE + filter + " GROUP BY fileSize / "
          + MIB);
      // the division may be exact (a fraction per key): the key is cut to its range, the counts of one range are added
      sizeCounts[0] = new HashMap<>();
      sizes.forEach((k, v) -> sizeCounts[0].merge(((Number) k).longValue(), v, Long::sum));
    });

    // correctness, on the last round only
    final FacetResults results = last[0];
    assertThat(results).isNotNull();
    assertThat(results.exact() || !mustBeExact).as("exact " + c.name + " " + lag).isTrue();
    if (!results.exact()) {
      logResult(buckets, lag, c, facets, groupBy, results, "INEXACT (patch budget exceeded, correctness not compared)");
      return;
    }
    assertThat(counts(results.get("brand"))).as("brand " + c.name + " " + lag).isEqualTo(
        withoutMissing(FacetOracle.expectedCounts(database, TYPE, "brand", c.where)));
    assertThat(counts(results.get("tags"))).as("tags " + c.name + " " + lag).isEqualTo(
        withoutMissing(FacetOracle.expectedCounts(database, TYPE, "tags", c.where)));
    assertThat(brandCounts[0]).as("timed brand GROUP BY " + c.name + " " + lag).isEqualTo(counts(results.get("brand")));
    assertThat(tagCounts[0]).as("timed tags GROUP BY " + c.name + " " + lag).isEqualTo(counts(results.get("tags")));
    final List<FacetBucket> ranges = results.get("size").buckets();
    assertThat(ranges).hasSize(4);
    for (int i = 0; i < 4; i++)
      assertThat(ranges.get(i).count()).as("size range " + i + " " + c.name + " " + lag).isEqualTo(sizeCounts[0].getOrDefault((long) i, 0L));

    logResult(buckets, lag, c, facets, groupBy, results, "exact");
  }

  private void logResult(final int buckets, final String lag, final Case c, final Timing facets, final Timing groupBy,
      final FacetResults results, final String verdict) {
    final StringBuilder modes = new StringBuilder();
    for (final String alias : results.aliases()) {
      final FacetExecutionStats stats = results.executionStats().get(alias);
      modes.append(alias).append('=').append(results.get(alias).source()).append('/')
          .append(stats == null ? "n/a" : stats.countingMode()).append(' ');
    }
    log(String.format(Locale.ROOT, "RESULT buckets=%d lag=%-7s where=%-9s facets median %8.1f ms min %5d ms | group-by(3 queries) median %8.1f ms min %5d ms"
            + " | ratio facets/groupby %.3f | meta.patched=%d meta.exact=%b (%s) | %s", buckets, lag, c.name, facets.median, facets.min,
        groupBy.median, groupBy.min, facets.median / Math.max(1d, groupBy.median), results.patched(), results.exact(), verdict,
        modes.toString().trim()));
  }

  private static Map<Object, Long> withoutMissing(final Map<Object, Long> expected) {
    final Map<Object, Long> counts = new HashMap<>(expected);
    counts.remove(FacetOracle.MISSING);
    return counts;
  }

  private static Map<Object, Long> counts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        counts.put(bucket.value(), bucket.count());
    return counts;
  }

  private Map<Object, Long> groupBy(final String sql) {
    final Map<Object, Long> counts = new HashMap<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final var row = rs.next();
        final Object key = row.getProperty("v");
        if (key != null)
          counts.put(key, row.<Number>getProperty("c").longValue());
      }
    }
    return counts;
  }

  private long count(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE + (where == null ? "" : " WHERE " + where))) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static Timing time(final Runnable body) {
    final List<Long> times = new ArrayList<>(ROUNDS);
    for (int i = 0; i < WARMUP + ROUNDS; i++) {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      body.run();
      if (i >= WARMUP)
        times.add(stopwatch.effectiveMs());
    }
    final long[] sorted = times.stream().mapToLong(Long::longValue).sorted().toArray();
    return new Timing(sorted[sorted.length / 2], sorted[0]);
  }

  private void log(final String message) {
    LogManager.instance().log(this, Level.INFO, "FACET-BENCH " + message);
  }
}
