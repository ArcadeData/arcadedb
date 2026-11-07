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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ORDER BY ts ASC/DESC with LIMIT push-down: ascending limit, latest-bounded reads, last point and descending tail.
 */
class TimeSeriesAscendingDescendingLimitTest extends TestHelper {

  // #7663
  /**
   * Issue #7663: {@code FetchFromTimeSeriesStep} carried a {@code descendingLimit} only (issue #5414), so
   * {@code ORDER BY ts ASC LIMIT n} - and a bare {@code LIMIT n} - took the {@code iterateQuery} arm and the surplus
   * rows were decompressed, boxed and held before {@code LimitExecutionStep} dropped them.
   * <p>
   * {@code iterateQuery} is not a lazy escape from that: its own javadoc says
   * {@code TimeSeriesSealedStore#iterateRange} materialises every matching row of the sealed layer before the
   * iterator is returned, so its residency is O(matching rows). The ascending fetch now takes the same bounded path
   * the descending one has taken since #5414, {@link TimeSeriesEngine#queryAscending}.
   * <p>
   * The residency saving itself is asserted at engine level by {@code TimeSeriesAscendingDescendingLimitTest.AscendingLimit}, which counts the
   * blocks decompressed. What this class pins is the planner decision the SQL path depends on - the cap reaches the
   * fetch, visible as {@code TOP n} in the plan - and, for every shape where a row could still be discarded
   * downstream, that it does NOT. A cap pushed past a residual filter returns too few rows, which is the failure
   * mode the guards exist for.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7663">issue #7663</a>
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class AscendingLimitPushDown {

    private static final int  TAGS    = 4;

    private static final int  PER_TAG = 2_000;

    private static final int  SHARDS  = 2;

    private static final long BASE_TS = 1_700_000_000_000L;

    private static final long STEP_MS = 1_000L;

    @BeforeEach
    void populate() throws IOException {
      database.command("sql",
          "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);

      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

      final int total = TAGS * PER_TAG;
      final long[] timestamps = new long[total];
      final Object[] hosts = new Object[total];
      final Object[] values = new Object[total];

      int i = 0;
      for (int t = 0; t < PER_TAG; t++)
        for (int h = 0; h < TAGS; h++) {
          timestamps[i] = BASE_TS + t * STEP_MS;
          hosts[i] = "host_" + h;
          values[i] = (double) (t * TAGS + h);
          i++;
        }

      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();
    }

    /**
     * The defect itself: {@code ORDER BY ts ASC LIMIT n} must reach the engine as a bounded fetch.
     */
    @Test
    void sqlOrderByAscWithLimitPushesTheRowCapIntoTheFetch() {
      final String sql = "SELECT ts, value FROM Point WHERE host = 'host_1' ORDER BY ts ASC LIMIT 10";

      final String explain = explain(sql);
      assertThat(explain).contains("FETCH FROM TIMESERIES Point");
      assertThat(explain).doesNotContain("FETCH FROM TIMESERIES DESC");
      assertThat(explain).contains("TOP 10");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(10);
      for (int i = 0; i < rows.size(); i++)
        assertThat(epochMillis(rows.get(i))).isEqualTo(BASE_TS + i * STEP_MS);
    }

    /**
     * A bare {@code LIMIT n} is the other half of the defect: no ORDER BY at all, so the first n rows of the fetch
     * ARE the answer and the cap belongs in the fetch just the same.
     */
    @Test
    void sqlBareLimitPushesTheRowCapIntoTheFetch() {
      final String sql = "SELECT ts, value FROM Point WHERE host = 'host_2' LIMIT 5";

      assertThat(explain(sql)).contains("TOP 5");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(5);
      for (int i = 0; i < rows.size(); i++)
        assertThat(epochMillis(rows.get(i))).isEqualTo(BASE_TS + i * STEP_MS);
    }

    /**
     * The bounded fetch has to answer exactly what the unbounded one answered, truncated: same rows, same order,
     * same values. This is the assertion that would catch a cap applied to the wrong end of the series.
     */
    @Test
    void theBoundedFetchAnswersThePrefixOfTheUnboundedOne() {
      final List<Result> bounded = query("SELECT ts, value FROM Point WHERE host = 'host_3' ORDER BY ts ASC LIMIT 25");
      final List<Result> unbounded = query("SELECT ts, value FROM Point WHERE host = 'host_3' ORDER BY ts ASC");

      assertThat(bounded).hasSize(25);
      assertThat(unbounded).hasSize(PER_TAG);
      for (int i = 0; i < bounded.size(); i++) {
        assertThat(epochMillis(bounded.get(i))).isEqualTo(epochMillis(unbounded.get(i)));
        assertThat(((Number) bounded.get(i).getProperty("value")).doubleValue())
            .isEqualTo(((Number) unbounded.get(i).getProperty("value")).doubleValue());
      }
    }

    /**
     * SKIP is served out of the SAME fetch, so the cap has to be {@code skip + limit} and not {@code limit}: a cap
     * of {@code limit} alone would let SKIP consume the whole answer and return nothing.
     */
    @Test
    void skipIsChargedAgainstTheSameCap() {
      final String sql = "SELECT ts FROM Point WHERE host = 'host_0' ORDER BY ts ASC SKIP 7 LIMIT 3";

      assertThat(explain(sql)).contains("TOP 10");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(3);
      for (int i = 0; i < rows.size(); i++)
        assertThat(epochMillis(rows.get(i))).isEqualTo(BASE_TS + (7 + i) * STEP_MS);
    }

    /**
     * The bound must not survive a predicate the engine never saw: {@code value < 100} is a residual filter, so the
     * rows the engine returns can still be discarded and a cap would answer short.
     */
    @Test
    void aResidualPredicateStopsTheRowCap() {
      final String sql = "SELECT ts, value FROM Point WHERE host = 'host_1' AND value < 1000 ORDER BY ts ASC LIMIT 10";

      assertThat(explain(sql)).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(10);
      for (final Result row : rows)
        assertThat(((Number) row.getProperty("value")).doubleValue()).isLessThan(1000.0);
    }

    /**
     * An OR is pushed down as the UNION of the per-block tag values, a superset the residual filter narrows, so the
     * cap must not go with it - exactly as on the descending path (issue #5414).
     */
    @Test
    void anOrPredicateStopsTheRowCap() {
      final String sql = "SELECT ts, host FROM Point WHERE host = 'host_1' OR host = 'host_2' ORDER BY ts ASC LIMIT 2";

      assertThat(explain(sql)).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(2);
      assertThat(rows.stream().map(r -> (String) r.getProperty("host")))
          .containsExactlyInAnyOrder("host_1", "host_2");
    }

    /**
     * DISTINCT collapses rows AFTER the fetch, so {@code LIMIT n} over it needs more than n fetched rows.
     */
    @Test
    void distinctStopsTheRowCap() {
      final String sql = "SELECT DISTINCT host FROM Point WHERE host = 'host_1' LIMIT 1";

      assertThat(explain(sql)).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<String>getProperty("host")).isEqualTo("host_1");
    }

    /**
     * An ORDER BY on anything but the timestamp re-orders the WHOLE result, so the oldest n rows of the fetch are
     * not the first n of the answer. Ordering by {@code value} descending selects the NEWEST samples, which a
     * pushed-down ascending cap would have thrown away.
     */
    @Test
    void anOrderByOnAnotherColumnStopsTheRowCap() {
      final String sql = "SELECT ts, value FROM Point WHERE host = 'host_0' ORDER BY value DESC LIMIT 3";

      assertThat(explain(sql)).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(3);
      // host_0 carries the samples 0, TAGS, 2*TAGS ... so its largest value is at the newest timestamp.
      assertThat(epochMillis(rows.getFirst())).isEqualTo(BASE_TS + (PER_TAG - 1) * STEP_MS);
    }

    /**
     * An aggregate consumes every row of the series to produce one, so the cap applies to the OUTPUT and never to
     * the fetch.
     */
    @Test
    void anAggregateStopsTheRowCap() {
      final String sql = "SELECT count(*) AS c FROM Point WHERE host = 'host_1' LIMIT 1";

      assertThat(explain(sql)).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.getFirst().getProperty("c")).intValue()).isEqualTo(PER_TAG);
    }

    /**
     * No LIMIT at all means no cap, and the ascending fetch keeps the unbounded arm it has always had.
     */
    @Test
    void withoutALimitTheFetchStaysUnbounded() {
      final String sql = "SELECT ts FROM Point WHERE host = 'host_2' ORDER BY ts ASC";

      assertThat(explain(sql)).doesNotContain("TOP ");
      assertThat(query(sql)).hasSize(PER_TAG);
    }

    /**
     * A cap larger than the range holds returns the range, not a padded answer, and a time bound still applies
     * under the cap.
     */
    @Test
    void theCapNeverInventsRowsAndTheTimeBoundStillApplies() {
      final long cutoff = BASE_TS + 4 * STEP_MS;
      final String sql = "SELECT ts FROM Point WHERE host = 'host_1' AND ts <= " + cutoff + " ORDER BY ts ASC LIMIT 100";

      assertThat(explain(sql)).contains("TOP 100");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(5);
      assertThat(epochMillis(rows.getFirst())).isEqualTo(BASE_TS);
      assertThat(epochMillis(rows.getLast())).isEqualTo(cutoff);
    }

    /**
     * A series that has never been compacted lives entirely in the mutable layer: the bounded ascending fetch has
     * to read it there too, including rows that arrived out of order.
     */
    @Test
    void theCapWorksWithoutAnySealedBlock() {
      database.command("sql", "CREATE TIMESERIES TYPE Fresh TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Fresh SET ts = 3000, host = 'a', value = 3.0");
        database.command("sql", "INSERT INTO Fresh SET ts = 1000, host = 'a', value = 1.0");
        database.command("sql", "INSERT INTO Fresh SET ts = 2000, host = 'a', value = 2.0");
      });

      final List<Result> rows = query("SELECT ts, value FROM Fresh WHERE host = 'a' ORDER BY ts ASC LIMIT 2");
      assertThat(rows).hasSize(2);
      assertThat(((Number) rows.getFirst().getProperty("value")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) rows.get(1).getProperty("value")).doubleValue()).isEqualTo(2.0);
    }

    /**
     * An empty selection under a cap is still empty, and a cap of zero returns nothing rather than everything - the
     * encoding where {@code <= 0} means "unlimited" inside the engine must not leak out to {@code LIMIT 0}.
     */
    @Test
    void anEmptySelectionAndAZeroLimitBothReturnNothing() {
      assertThat(query("SELECT ts FROM Point WHERE host = 'absent' ORDER BY ts ASC LIMIT 10")).isEmpty();
      assertThat(query("SELECT ts FROM Point WHERE host = 'host_1' ORDER BY ts ASC LIMIT 0")).isEmpty();
    }

    /**
     * The cap is an OPTIMISATION, so deciding it must never be the reason a query fails. The ascending path is
     * reached by ANY time-series query carrying a LIMIT, where the descending one needed an explicit
     * {@code ORDER BY ts DESC}, so the planner-time evaluation behind the push-down test now meets expression
     * shapes it never used to - here the right-hand side of a tag equality, evaluated against a null record.
     * Whatever it does, the query still has to answer, uncapped.
     * <p>
     * The {@code catch (RuntimeException)} guarding that evaluation is defence in depth, and these two shapes do
     * not prove it fires: both resolve to null rather than throwing, so the push-down test simply returns false.
     * An expression that genuinely throws where it is evaluated - {@code host = 1 / 0} - cannot pin it either,
     * because the residual filter evaluates the same expression per row and the query fails identically with or
     * without the guard. What the guard buys is that a cap, which is only ever an optimisation, can never be the
     * reason a query that used to run now fails.
     */
    @Test
    void anUnevaluableTagPredicateFallsBackToAnUncappedScanRatherThanFailing() {
      for (final String predicate : new String[] {
          "host = value.asString()",     // a method call on a column, with no record to read it from
          "host = $undefinedVariable" }) { // an undefined context variable
        final String sql = "SELECT ts FROM Point WHERE " + predicate + " LIMIT 5";

        // Whether the predicate matches anything is not the point - answering at all is.
        assertThat(query(sql)).as("'%s' must still answer", predicate).hasSizeLessThanOrEqualTo(5);
        assertThat(explain(sql)).as("'%s' must not be capped: it was never proved pushed down", predicate)
            .doesNotContain("TOP ");
      }
    }

    /**
     * A parameterised LIMIT is resolved at execution time, not at planning time, so the cap cannot be decided and
     * the query must still return exactly what it asked for.
     */
    @Test
    void aParameterisedLimitStillAnswersCorrectly() {
      final List<Result> results = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT ts FROM Point WHERE host = 'host_1' LIMIT :max",
          Map.of("max", 4))) {
        while (rs.hasNext())
          results.add(rs.next());
      }
      assertThat(results).hasSize(4);
      assertThat(epochMillis(results.getFirst())).isEqualTo(BASE_TS);
    }

    private List<Result> query(final String sql) {
      final List<Result> results = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          results.add(rs.next());
      }
      return results;
    }

    private String explain(final String sql) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
        final StringBuilder sb = new StringBuilder();
        while (rs.hasNext())
          sb.append((String) rs.next().getProperty("executionPlanAsString"));
        return sb.toString();
      }
    }

    private static long epochMillis(final Result row) {
      final Object ts = row.getProperty("ts");
      if (ts instanceof LocalDateTime ldt)
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
      return ((Number) ts).longValue();
    }
  }

  // #7336
  /**
   * Issue #7336: {@code POST /ts/{database}/query} read a caller-supplied {@code limit} and then called
   * {@link TimeSeriesEngine#query}, which merges every shard's full range into one {@code ArrayList} and sorts
   * it, so ten rows over a wide range cost O(N) heap and O(N log N) time. {@link TimeSeriesEngine#iterateQuery}
   * is not a substitute - its own javadoc says the sealed layer materialises every matching row before the
   * iterator is returned - so the bound had to become a bound on the FETCH.
   * <p>
   * {@link TimeSeriesEngine#queryAscending} is the ascending mirror of {@code queryDescending} (issue #5414):
   * each shard stops walking blocks as soon as its own limit is satisfied.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class AscendingLimit {

    private static final int  TAGS    = 8;

    private static final int  PER_TAG = 20_000;

    private static final int  SHARDS  = 2;

    private static final long BASE_TS = 1_700_000_000_000L;

    private static final long STEP_MS = 1_000L;

    private TimeSeriesEngine engine;

    @BeforeEach
    void populate() throws IOException {
      database.command("sql",
          "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);

      engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

      final int total = TAGS * PER_TAG;
      final long[] timestamps = new long[total];
      final Object[] hosts = new Object[total];
      final Object[] values = new Object[total];

      int i = 0;
      for (int t = 0; t < PER_TAG; t++)
        for (int h = 0; h < TAGS; h++) {
          timestamps[i] = BASE_TS + t * STEP_MS;
          hosts[i] = "host_" + h;
          values[i] = (double) (t * TAGS + h);
          i++;
        }

      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();
    }

    /**
     * The defect itself: a small limit over the widest possible range must not touch the whole series.
     */
    @Test
    void ascendingScanTouchesOnlyTheOldestBlocks() throws Exception {
      int totalBlocks = 0;
      for (int i = 0; i < engine.getShardCount(); i++)
        totalBlocks += engine.getShard(i).getSealedStore().getBlockCount();
      assertThat(totalBlocks).isGreaterThan(engine.getShardCount());

      final TagFilter filter = TagFilter.eq(0, "host_3");

      // What the unbounded walk costs, measured rather than assumed: this is what engine.query() paid to answer
      // the same ten rows, and every bound below is stated against it so none of them can pass trivially.
      final AggregationMetrics unbounded = new AggregationMetrics();
      engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, unbounded, row -> true);
      final int unboundedBlocks = unbounded.getFastPathBlocks() + unbounded.getSlowPathBlocks();
      assertThat(unboundedBlocks).isGreaterThan(1);
      assertThat(unbounded.getMaterializedRows()).isEqualTo(PER_TAG);

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> oldest = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 10, metrics);

      assertThat(oldest).hasSize(10);
      assertThat((long) oldest.getFirst()[0]).isEqualTo(BASE_TS);

      // The whole point of the bound: at most the oldest block of each shard is decompressed.
      final int touched = metrics.getFastPathBlocks() + metrics.getSlowPathBlocks();
      assertThat(touched).isLessThanOrEqualTo(engine.getShardCount());
      assertThat(touched).isLessThan(totalBlocks);
      assertThat(touched).isLessThan(unboundedBlocks);

      // And the rows it boxed are bounded by the limit and the shard count, not by the series.
      assertThat(metrics.getMaterializedRows()).isLessThan(unbounded.getMaterializedRows());
      assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo((long) 10 * engine.getShardCount());
    }

    /**
     * The bounded fetch must answer exactly what {@code query()} answered, truncated: same rows, same order.
     */
    @Test
    void ascendingScanReturnsTheOldestRowsInOrder() throws Exception {
      final TagFilter filter = TagFilter.eq(0, "host_5");
      final List<Object[]> rows = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 100, null);

      assertThat(rows).hasSize(100);
      for (int i = 1; i < rows.size(); i++)
        assertThat((long) rows.get(i)[0]).isGreaterThan((long) rows.get(i - 1)[0]);

      final List<Object[]> reference = ascending(Long.MIN_VALUE, Long.MAX_VALUE, filter);
      for (int i = 0; i < rows.size(); i++) {
        assertThat((long) rows.get(i)[0]).isEqualTo((long) reference.get(i)[0]);
        assertThat(((Number) rows.get(i)[2]).doubleValue()).isEqualTo(((Number) reference.get(i)[2]).doubleValue());
      }
    }

    /**
     * A limit of zero or less is unlimited, exactly as on {@code queryDescending}, and the time bounds still apply.
     */
    @Test
    void ascendingScanHonoursTimeBoundsAndUnlimitedMode() throws Exception {
      final TagFilter filter = TagFilter.eq(0, "host_1");
      final long from = BASE_TS + 50 * STEP_MS;
      final long to = BASE_TS + 150 * STEP_MS;

      final List<Object[]> reference = ascending(from, to, filter);
      assertThat(reference).hasSize(101);

      for (final int unlimited : new int[] { 0, -1 }) {
        final List<Object[]> rows = engine.queryAscending(from, to, null, filter, unlimited, null);
        assertThat(rows).hasSameSizeAs(reference);
        for (int i = 0; i < rows.size(); i++)
          assertThat((long) rows.get(i)[0]).isEqualTo((long) reference.get(i)[0]);
      }

      // A limit larger than the range holds returns the range, not a padded answer.
      final List<Object[]> overshoot = engine.queryAscending(from, to, null, filter, 1_000, null);
      assertThat(overshoot).hasSameSizeAs(reference);

      // And a limit of exactly the range's size is not reported any differently.
      final List<Object[]> exact = engine.queryAscending(from, to, null, filter, reference.size(), null);
      assertThat(exact).hasSameSizeAs(reference);
      assertThat((long) exact.getLast()[0]).isEqualTo(to);
    }

    /**
     * The mutable layer has to be asked for the WHOLE limit, not for the remainder the sealed layer left
     * unfilled. Late arrivals carry OLD timestamps, so an answer can be made entirely of mutable rows even when
     * the sealed layer already produced rows of its own - which is why {@code TimeSeriesShard.scanRangeAscending}
     * passes {@code limit} down rather than {@code need - sealedRows.size()}.
     * <p>
     * One shard on purpose: the parameter under test lives in the shard, and round-robin routing would otherwise
     * split the late arrivals across shards and let the engine's merge paper over a wrong per-shard bound.
     */
    @Test
    void ascendingScanAsksTheMutableLayerForTheWholeLimitNotTheRemainder() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE Late TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      final TimeSeriesEngine late = ((LocalTimeSeriesType) database.getSchema().getType("Late")).getEngine();

      final int sealed = 10;
      final long[] sealedTs = new long[sealed];
      final Object[] sealedHosts = new Object[sealed];
      final Object[] sealedValues = new Object[sealed];
      for (int i = 0; i < sealed; i++) {
        sealedTs[i] = BASE_TS + i * STEP_MS;
        sealedHosts[i] = "host_0";
        sealedValues[i] = (double) i;
      }
      late.appendBatch(sealedTs, new Object[][] { sealedHosts, sealedValues });
      late.compactAll();

      // Six late arrivals, all OLDER than anything sealed, still in the mutable layer.
      final int lateCount = 6;
      final long[] lateTs = new long[lateCount];
      final Object[] lateHosts = new Object[lateCount];
      final Object[] lateValues = new Object[lateCount];
      for (int i = 0; i < lateCount; i++) {
        lateTs[i] = BASE_TS - (lateCount - i) * STEP_MS;
        lateHosts[i] = "host_0";
        lateValues[i] = (double) -(lateCount - i);
      }
      late.appendBatch(lateTs, new Object[][] { lateHosts, lateValues });

      // The range is narrowed so the sealed layer contributes exactly two rows, leaving a remainder of three.
      // Asking the mutable layer for that remainder returns only the three oldest late arrivals and then pads the
      // answer with the two sealed rows - the wrong set, because the five oldest are all late arrivals.
      final List<Object[]> rows = late.queryAscending(Long.MIN_VALUE, BASE_TS + STEP_MS, null, null, 5, null);

      assertThat(rows).hasSize(5);
      for (int i = 0; i < 5; i++) {
        assertThat((long) rows.get(i)[0]).isEqualTo(BASE_TS - (lateCount - i) * STEP_MS);
        assertThat(((Number) rows.get(i)[2]).doubleValue()).isEqualTo(-(double) (lateCount - i));
      }
    }

    /**
     * The simpler shape of the same hazard, through the sharded fixture: a late arrival older than everything
     * sealed must still lead the ascending answer.
     */
    @Test
    void ascendingScanSeesAnOlderRowStillInTheMutableLayer() throws Exception {
      final long olderTs = BASE_TS - 10 * STEP_MS;
      engine.appendSamples(new long[] { olderTs }, new Object[] { "host_3" }, new Object[] { -1.0 });

      final TagFilter filter = TagFilter.eq(0, "host_3");
      final List<Object[]> rows = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 2, null);

      assertThat(rows).hasSize(2);
      assertThat((long) rows.getFirst()[0]).isEqualTo(olderTs);
      assertThat(((Number) rows.getFirst()[2]).doubleValue()).isEqualTo(-1.0);
      assertThat((long) rows.get(1)[0]).isEqualTo(BASE_TS);
    }

    /**
     * The projection and the tag filter must survive the bounded path: a subset of columns still resolves the
     * filter against the right column, which is the trap {@code matchesMapped} exists for.
     */
    @Test
    void ascendingScanAppliesTheTagFilterUnderAColumnProjection() throws Exception {
      final TagFilter filter = TagFilter.eq(0, "host_7");
      final int[] projection = new int[] { 0 };

      final List<Object[]> rows = engine.queryAscending(BASE_TS, BASE_TS + 5 * STEP_MS, projection, filter, 3, null);

      assertThat(rows).hasSize(3);
      for (final Object[] row : rows) {
        assertThat(row).hasSize(2);
        assertThat(row[1]).isEqualTo("host_7");
      }
      assertThat((long) rows.getFirst()[0]).isEqualTo(BASE_TS);
      assertThat((long) rows.getLast()[0]).isEqualTo(BASE_TS + 2 * STEP_MS);
    }

    /**
     * Rows are routed to shards round-robin, so a filter matching nothing must still terminate, and a query
     * whose range holds fewer rows than the limit must not invent any.
     */
    @Test
    void ascendingScanOnAnEmptySelectionReturnsNothing() throws Exception {
      assertThat(engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "absent"), 10, null))
          .isEmpty();
      assertThat(engine.queryAscending(BASE_TS - 100_000L, BASE_TS - 1L, null, null, 10, null)).isEmpty();
    }

    /**
     * Across shards the merge has to be a real merge: the oldest N rows of the type span every shard, since a
     * timestamp is shared by all tags and the tags are spread round-robin.
     */
    @Test
    void ascendingScanMergesAcrossShards() throws Exception {
      final List<Object[]> rows = engine.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, null, null, TAGS, null);

      assertThat(rows).hasSize(TAGS);
      final List<Object> hosts = new ArrayList<>();
      for (final Object[] row : rows) {
        assertThat((long) row[0]).isEqualTo(BASE_TS);
        hosts.add(row[1]);
      }
      // Every tag shares the oldest timestamp, so the merge must have visited both shards to find them all.
      assertThat(hosts).hasSize(TAGS).doesNotHaveDuplicates();
    }

    private List<Object[]> ascending(final long fromTs, final long toTs, final TagFilter filter) throws IOException {
      final List<Object[]> all = new ArrayList<>();
      final Iterator<Object[]> it = engine.iterateQuery(fromTs, toTs, null, filter);
      while (it.hasNext())
        all.add(it.next());
      return all;
    }
  }

  // ---- #7322 ----
  // Issue #7322: {@code TimeSeriesGateway.latest} answered "the newest sample" by merging every shard's whole
  // range into one list, sorting it and keeping the last row - O(series) heap and O(series log series) time for
  // a question about one row, on the endpoint a Grafana single-stat panel polls. It now asks
  // {@link TimeSeriesEngine#queryDescending} for a single row, which stops walking blocks as soon as its own
  // limit is satisfied.
  // <p>
  // The two forms disagree on exactly one thing, and only on that: which row wins when several samples share the
  // newest timestamp. The whole-series scan sorted ascending (stably) and took the last of the tied rows; the
  // newest-first scan sorts descending (stably) and keeps the first. That change is deliberate and is pinned
  // below - it is the reason the swap was tracked as its own issue rather than folded into #7305.
  //
  // @author Roberto Franchini (r.franchini@arcadedata.com)

  private static final long BASE_TS = 1_700_000_000_000L;

  private static final long STEP_MS = 1_000L;

  private TimeSeriesEngine createType(final String name, final int shards) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + shards);
    return ((LocalTimeSeriesType) database.getSchema().getType(name)).getEngine();
  }

  /**
   * Appends {@code ticks} samples for each of {@code hosts}, one tick per {@link #STEP_MS}.
   */
  private void append(final TimeSeriesEngine engine, final int ticks, final String... hosts) throws IOException {
    final int rows = ticks * hosts.length;
    final long[] timestamps = new long[rows];
    final Object[] hostValues = new Object[rows];
    final Object[] values = new Object[rows];

    int i = 0;
    for (int t = 0; t < ticks; t++)
      for (final String host : hosts) {
        timestamps[i] = BASE_TS + (long) t * STEP_MS;
        hostValues[i] = host;
        values[i] = (double) i;
        i++;
      }
    engine.appendBatch(timestamps, new Object[][] { hostValues, values });
  }

  /**
   * {@code Object[].equals} is identity, so rows are compared by content.
   */
  private static List<Object> content(final Object[] row) {
    return row == null ? null : Arrays.asList(row);
  }

  /**
   * An INDEPENDENT oracle: the exhaustive ascending scan, which is the implementation {@code latest} used
   * before #7322. Deliberately not {@code queryDescending(..., 1, ...)} - that is the expression under test,
   * and comparing the two would assert that A equals A (code review on PR #7376). The two implementations
   * agree on every selection whose newest timestamp is unique, which is what the callers below construct, so
   * this is a real cross-check between two different ways of finding the same row.
   * <p>
   * Uniqueness is asserted rather than assumed: a fixture edited into a tie would otherwise start comparing
   * against the wrong row in silence, since the two implementations part company on exactly that case.
   */
  private Object[] exhaustiveNewest(final TimeSeriesEngine engine, final TagFilter tagFilter) throws IOException {
    final List<Object[]> all = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, tagFilter);
    if (all.isEmpty())
      return null;

    final Object[] newest = all.getLast();
    assertThat(all).as("this oracle is only valid where the newest timestamp is unique")
        .filteredOn(row -> (long) row[0] == (long) newest[0]).hasSize(1);
    return newest;
  }

  // #7322
  @Test
  void latestIsTheNewestRowOnASingleShard() throws Exception {
    final TimeSeriesEngine engine = createType("Single", 1);
    append(engine, 500, "host_0");

    final Object[] latest = TimeSeriesGateway.latest(engine, null);

    assertThat(latest).isNotNull();
    assertThat((long) latest[0]).isEqualTo(BASE_TS + 499 * STEP_MS);
    assertThat(content(latest)).isEqualTo(content(exhaustiveNewest(engine, null)));
  }

  // #7322
  /**
   * One host only, so every timestamp appears exactly once and the answer is a single row rather than a tie -
   * this test is about merging the two storage layers of four shards, not about the tie-break.
   */
  @Test
  void latestIsTheNewestRowAcrossShardsAndAcrossBothStorageLayers() throws Exception {
    final TimeSeriesEngine engine = createType("Layers", 4);
    append(engine, 400, "host_0");
    // Everything appended so far moves to the sealed layer; the tail that follows stays mutable, so the
    // newest-first walk has to merge both layers of every shard - and the newest row is in the SEALED one,
    // which is the direction a walk that stops at the first mutable row it finds would get wrong.
    engine.compactAll();
    append(engine, 40, "host_0");

    final Object[] latest = TimeSeriesGateway.latest(engine, null);

    assertThat(latest).isNotNull();
    // Ticks restart at 0 on the second append, so the newest timestamp is the older, now-sealed run's last tick.
    assertThat((long) latest[0]).isEqualTo(BASE_TS + 399 * STEP_MS);
    assertThat(content(latest)).isEqualTo(content(exhaustiveNewest(engine, null)));
  }

  // #7322
  @Test
  void latestHonoursTheTagFilter() throws Exception {
    final TimeSeriesEngine engine = createType("Tagged", 2);
    append(engine, 300, "host_0", "host_1");
    // host_2 stops earlier, so a filter on it must not answer with the other hosts' newer samples.
    append(engine, 100, "host_2");

    final TagFilter onlyHost2 = TagFilter.eq(0, "host_2");
    final Object[] latest = TimeSeriesGateway.latest(engine, onlyHost2);

    assertThat(latest).isNotNull();
    assertThat(latest[1]).isEqualTo("host_2");
    assertThat((long) latest[0]).isEqualTo(BASE_TS + 99 * STEP_MS);
    assertThat(content(latest)).isEqualTo(content(exhaustiveNewest(engine, onlyHost2)));
  }

  // #7322
  @Test
  void latestIsNullWhenTheSelectionHoldsNoRow() throws Exception {
    final TimeSeriesEngine engine = createType("Empty", 2);

    assertThat(TimeSeriesGateway.latest(engine, null)).isNull();

    append(engine, 10, "host_0");
    assertThat(TimeSeriesGateway.latest(engine, TagFilter.eq(0, "absent"))).isNull();
  }

  // #7322
  /**
   * The documented tie-break, and the one behavioural difference this issue introduces.
   * <p>
   * The tied pair is appended FIRST, while the type's round-robin append counter is still 0, so sample
   * {@code i} lands in shard {@code i} - the placement the assertions below depend on, and asserted rather
   * than assumed. Both shards then hold a row at the newest timestamp.
   * <p>
   * {@code TimeSeriesEngine.query} sorts the shard-ordered list ascending and the old {@code latest} took the
   * LAST of the tied rows - shard 1's. {@code queryDescending} sorts it descending, and both sorts are stable,
   * so the survivor at limit 1 is the FIRST in shard order - shard 0's. Asserting both halves is what makes
   * this test fail against the old implementation rather than pass against either.
   */
  @Test
  void aTieAtTheNewestTimestampGoesToTheRowTheNewestFirstScanYieldsFirst() throws Exception {
    final TimeSeriesEngine engine = createType("Tied", 2);

    final long tiedTs = BASE_TS + 100 * STEP_MS;
    engine.appendBatch(new long[] { tiedTs, tiedTs },
        new Object[][] { new Object[] { "in_shard_0", "in_shard_1" }, new Object[] { 1.0d, 2.0d } });
    // Older filler, so the answer is not simply "the only row there is".
    append(engine, 5, "older");

    // The placement the rest of this test reads as "shard order", checked rather than inferred.
    assertThat(engine.getShard(0).scanRange(tiedTs, tiedTs, null, null)).singleElement()
        .satisfies(row -> assertThat(row[1]).isEqualTo("in_shard_0"));
    assertThat(engine.getShard(1).scanRange(tiedTs, tiedTs, null, null)).singleElement()
        .satisfies(row -> assertThat(row[1]).isEqualTo("in_shard_1"));

    final Object[] latest = TimeSeriesGateway.latest(engine, null);

    assertThat(latest).isNotNull();
    assertThat((long) latest[0]).isEqualTo(tiedTs);

    // No oracle here: the tie is the one case where the two implementations disagree by design, so the
    // expected row is spelled out instead.
    assertThat(latest[1]).isEqualTo("in_shard_0");

    // And it is NOT what the whole-series ascending scan used to answer. This assertion is the regression
    // guard: restoring engine.query(...).get(size - 1) makes it fail.
    final List<Object[]> everything = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(everything.getLast()[1]).isEqualTo("in_shard_1");
    assertThat(content(latest)).isNotEqualTo(content(everything.getLast()));
  }

  // #7322
  /**
   * A tie-break nobody can rely on is not a tie-break: repeated calls against an unchanged layout must answer
   * the same row, and sealing the tied rows must not move the answer either.
   */
  @Test
  void theTieBreakIsStableAcrossCallsAndAcrossCompaction() throws Exception {
    final TimeSeriesEngine engine = createType("StableTie", 2);
    final long tiedTs = BASE_TS + 7 * STEP_MS;
    engine.appendBatch(new long[] { tiedTs, tiedTs },
        new Object[][] { new Object[] { "shard0", "shard1" }, new Object[] { 1.0d, 2.0d } });

    final Object[] first = TimeSeriesGateway.latest(engine, null);
    final Object[] second = TimeSeriesGateway.latest(engine, null);
    assertThat(content(first)).isEqualTo(content(second));

    engine.compactAll();

    final Object[] afterCompaction = TimeSeriesGateway.latest(engine, null);
    assertThat(content(afterCompaction)).isEqualTo(content(first));
  }

  // #5414
  /**
   * Issue #5414: an unbounded last-point-per-tag query used to scan the tag's whole series, because the
   * only scan direction available was ascending and the sealed layer materialises every matching row
   * before the caller sees the first one. The descending scan walks sealed blocks newest-first and
   * stops as soon as the requested number of rows has been produced, so the unbounded form costs
   * O(blocks touched) instead of O(series).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LastPoint {

    private static final int  TAGS    = 8;

    private static final int  PER_TAG = 20_000;

    private static final int  SHARDS  = 2;

    private static final long BASE_TS = 1_700_000_000_000L;

    private static final long STEP_MS = 1_000L;

    private TimeSeriesEngine engine;

    @BeforeEach
    void populate() throws IOException {
      database.command("sql",
          "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);

      engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

      final int total = TAGS * PER_TAG;
      final long[] timestamps = new long[total];
      final Object[] hosts = new Object[total];
      final Object[] values = new Object[total];

      int i = 0;
      for (int t = 0; t < PER_TAG; t++)
        for (int h = 0; h < TAGS; h++) {
          timestamps[i] = BASE_TS + t * STEP_MS;
          hosts[i] = "host_" + h;
          values[i] = (double) (t * TAGS + h);
          i++;
        }

      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();
    }

    @Test
    void descendingScanTouchesOnlyTheNewestBlocks() throws Exception {
      int totalBlocks = 0;
      for (int i = 0; i < engine.getShardCount(); i++)
        totalBlocks += engine.getShard(i).getSealedStore().getBlockCount();
      assertThat(totalBlocks).isGreaterThan(engine.getShardCount());

      final TagFilter filter = TagFilter.eq(0, "host_3");

      final AggregationMetrics metrics = new AggregationMetrics();
      final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 1, metrics);

      assertThat(newest).hasSize(1);
      assertThat((long) newest.getFirst()[0]).isEqualTo(newestTimestamp(filter));

      // The whole point of the shortcut: at most the newest block of each shard is decompressed.
      final int touched = metrics.getFastPathBlocks() + metrics.getSlowPathBlocks();
      assertThat(touched).isLessThanOrEqualTo(engine.getShardCount());
      assertThat(touched).isLessThan(totalBlocks);
    }

    @Test
    void descendingScanReturnsTheNewestRowsInOrder() throws Exception {
      final TagFilter filter = TagFilter.eq(0, "host_5");
      final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 100, null);

      assertThat(rows).hasSize(100);
      for (int i = 1; i < rows.size(); i++)
        assertThat((long) rows.get(i)[0]).isLessThan((long) rows.get(i - 1)[0]);

      // Cross-check against the tail of the ascending scan.
      final List<Object[]> all = ascending(Long.MIN_VALUE, Long.MAX_VALUE, filter);
      for (int i = 0; i < rows.size(); i++)
        assertThat((long) rows.get(i)[0]).isEqualTo((long) all.get(all.size() - 1 - i)[0]);
    }

    @Test
    void descendingScanHonoursTimeBoundsAndUnlimitedMode() throws Exception {
      final TagFilter filter = TagFilter.eq(0, "host_1");
      final long from = BASE_TS + 50 * STEP_MS;
      final long to = BASE_TS + 150 * STEP_MS;

      final List<Object[]> bounded = engine.queryDescending(from, to, null, filter, 0, null);
      final List<Object[]> reference = ascending(from, to, filter);

      assertThat(bounded).hasSameSizeAs(reference);
      for (int i = 0; i < bounded.size(); i++)
        assertThat((long) bounded.get(i)[0]).isEqualTo((long) reference.get(reference.size() - 1 - i)[0]);
    }

    @Test
    void descendingScanSeesRowsStillInTheMutableLayer() throws Exception {
      final long newerTs = BASE_TS + (PER_TAG + 100) * STEP_MS;
      engine.appendSamples(new long[] { newerTs }, new Object[] { "host_3" }, new Object[] { 999.0 });

      final TagFilter filter = TagFilter.eq(0, "host_3");
      final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, 2, null);

      assertThat(rows).hasSize(2);
      assertThat((long) rows.getFirst()[0]).isEqualTo(newerTs);
      assertThat(((Number) rows.getFirst()[2]).doubleValue()).isEqualTo(999.0);
      assertThat((long) rows.get(1)[0]).isEqualTo(BASE_TS + (PER_TAG - 1) * STEP_MS);
    }

    @Test
    void sqlOrderByDescLimitUsesTheDescendingFetch() {
      assertThat(explain("SELECT ts, value FROM Point WHERE host = 'host_2' ORDER BY ts DESC LIMIT 1"))
          .contains("FETCH FROM TIMESERIES DESC").contains("TOP 1");

      final List<Result> rows = query("SELECT ts, value FROM Point WHERE host = 'host_2' ORDER BY ts DESC LIMIT 3");
      assertThat(rows).hasSize(3);
      assertThat(epochMillis(rows.get(1))).isLessThan(epochMillis(rows.get(0)));
      assertThat(epochMillis(rows.get(2))).isLessThan(epochMillis(rows.get(1)));

      // The newest timestamp must match the plain unbounded ascending scan's tail.
      final List<Result> asc = query("SELECT ts FROM Point WHERE host = 'host_2'");
      assertThat(epochMillis(rows.getFirst())).isEqualTo(epochMillis(asc.getLast()));
    }

    @Test
    void sqlOrderByDescWithSkipPushesSkipPlusLimit() {
      assertThat(explain("SELECT ts FROM Point WHERE host = 'host_2' ORDER BY ts DESC SKIP 2 LIMIT 3"))
          .contains("FETCH FROM TIMESERIES DESC").contains("TOP 5");

      final List<Result> rows = query("SELECT ts FROM Point WHERE host = 'host_2' ORDER BY ts DESC SKIP 2 LIMIT 3");
      final List<Result> all = query("SELECT ts FROM Point WHERE host = 'host_2' ORDER BY ts DESC LIMIT 5");
      assertThat(rows).hasSize(3);
      for (int i = 0; i < 3; i++)
        assertThat(epochMillis(rows.get(i))).isEqualTo(epochMillis(all.get(i + 2)));
    }

    @Test
    void sqlTsLastIsAnsweredFromTheNewestRow() {
      assertThat(explain("SELECT ts.last(value, ts) AS v FROM Point WHERE host = 'host_4'"))
          .contains("FETCH FROM TIMESERIES DESC").contains("TOP 1");

      final List<Result> rows = query("SELECT ts.last(value, ts) AS v FROM Point WHERE host = 'host_4'");
      assertThat(rows).hasSize(1);

      final List<Result> asc = query("SELECT value FROM Point WHERE host = 'host_4'");
      assertThat(((Number) rows.getFirst().getProperty("v")).doubleValue())
          .isEqualTo(((Number) asc.getLast().getProperty("value")).doubleValue());
    }

    @Test
    void sqlTsLastWithoutAnyFilterReturnsTheGlobalNewest() {
      final List<Result> rows = query("SELECT ts.last(value, ts) AS v FROM Point");
      assertThat(rows).hasSize(1);

      // The newest timestamp is shared by all tags, so ts.last() may legitimately return any of them.
      final long lastTs = BASE_TS + (PER_TAG - 1) * STEP_MS;
      final List<Result> newest = query("SELECT value FROM Point WHERE ts = " + lastTs);
      assertThat(newest).hasSize(TAGS);
      assertThat(newest.stream().map(r -> ((Number) r.getProperty("value")).doubleValue()))
          .contains(((Number) rows.getFirst().getProperty("v")).doubleValue());
    }

    @Test
    void sqlOrderByDescStillHonoursTimeBounds() {
      final long cutoff = BASE_TS + 100 * STEP_MS;
      final List<Result> rows = query(
          "SELECT ts FROM Point WHERE host = 'host_1' AND ts <= " + cutoff + " ORDER BY ts DESC LIMIT 1");
      assertThat(rows).hasSize(1);
      assertThat(epochMillis(rows.getFirst())).isEqualTo(cutoff);
    }

    @Test
    void sqlOrderByDescWithoutLimitStillReturnsEverything() {
      final String explain = explain("SELECT ts FROM Point WHERE host = 'host_0' ORDER BY ts DESC");
      assertThat(explain).contains("FETCH FROM TIMESERIES DESC");
      assertThat(explain).doesNotContain("TOP ");

      final List<Result> rows = query("SELECT ts FROM Point WHERE host = 'host_0' ORDER BY ts DESC");
      assertThat(rows).hasSize(PER_TAG);
      for (int i = 1; i < 50; i++)
        assertThat(epochMillis(rows.get(i))).isLessThan(epochMillis(rows.get(i - 1)));
    }

    /**
     * A predicate the engine cannot evaluate must not let the row cap through: the residual filter
     * could otherwise drop the only row the engine returned.
     */
    @Test
    void sqlOrderByDescWithResidualPredicateDoesNotPushTheRowCap() {
      final String sql = "SELECT ts, value FROM Point WHERE host = 'host_6' AND value < 100 ORDER BY ts DESC LIMIT 1";

      final String explain = explain(sql);
      assertThat(explain).contains("FETCH FROM TIMESERIES DESC");
      assertThat(explain).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.getFirst().getProperty("value")).doubleValue()).isLessThan(100.0);

      final List<Result> reference = query("SELECT ts, value FROM Point WHERE host = 'host_6' AND value < 100");
      assertThat(epochMillis(rows.getFirst())).isEqualTo(epochMillis(reference.getLast()));
    }

    /**
     * {@code ts <> x} mentions the timestamp column but produces no bound, so it stays a residual
     * predicate and the row cap must not be pushed down.
     */
    @Test
    void sqlOrderByDescWithNonRangeTimestampPredicateDoesNotPushTheRowCap() {
      final long lastTs = BASE_TS + (PER_TAG - 1) * STEP_MS;
      final String sql = "SELECT ts FROM Point WHERE host = 'host_7' AND ts <> " + lastTs + " ORDER BY ts DESC LIMIT 1";

      assertThat(explain(sql)).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(1);
      assertThat(epochMillis(rows.getFirst())).isEqualTo(BASE_TS + (PER_TAG - 2) * STEP_MS);
    }

    /**
     * An OR pushes the union of the tag values, so the engine may return rows the residual filter
     * removes: the row cap must not be pushed down, but the answer must stay correct.
     */
    @Test
    void sqlOrderByDescWithOrDoesNotPushTheRowCap() {
      final String sql = "SELECT ts, host FROM Point WHERE host = 'host_1' OR host = 'host_2' ORDER BY ts DESC LIMIT 2";

      assertThat(explain(sql)).doesNotContain("TOP ");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(2);
      final long lastTs = BASE_TS + (PER_TAG - 1) * STEP_MS;
      assertThat(epochMillis(rows.getFirst())).isEqualTo(lastTs);
      assertThat(epochMillis(rows.get(1))).isEqualTo(lastTs);
      assertThat(rows.stream().map(r -> (String) r.getProperty("host"))).containsExactlyInAnyOrder("host_1", "host_2");
    }

    /**
     * ORDER BY on the ascending direction, on another column, or mixed with a second key must keep the
     * regular ascending fetch plus the sort step.
     */
    @Test
    void sqlOtherOrderingsKeepTheAscendingFetch() {
      assertThat(explain("SELECT ts FROM Point WHERE host = 'host_0' ORDER BY ts LIMIT 1"))
          .doesNotContain("FETCH FROM TIMESERIES DESC");
      assertThat(explain("SELECT ts, value FROM Point WHERE host = 'host_0' ORDER BY value DESC LIMIT 1"))
          .doesNotContain("FETCH FROM TIMESERIES DESC");
      assertThat(explain("SELECT ts, value FROM Point WHERE host = 'host_0' ORDER BY ts DESC, value ASC LIMIT 1"))
          .doesNotContain("FETCH FROM TIMESERIES DESC");
    }

    @Test
    void sqlSelectStarOrderByDescLimitUsesTheDescendingFetch() {
      assertThat(explain("SELECT * FROM Point WHERE host = 'host_0' ORDER BY ts DESC LIMIT 1"))
          .contains("FETCH FROM TIMESERIES DESC").contains("TOP 1");

      final List<Result> rows = query("SELECT * FROM Point WHERE host = 'host_0' ORDER BY ts DESC LIMIT 1");
      assertThat(rows).hasSize(1);
      assertThat(epochMillis(rows.getFirst())).isEqualTo(BASE_TS + (PER_TAG - 1) * STEP_MS);
      assertThat((String) rows.getFirst().getProperty("host")).isEqualTo("host_0");
    }

    /**
     * The ORDER BY of a projecting query is applied after the projection, so an alias that rebinds the
     * timestamp column's name must not be mistaken for the timestamp itself.
     */
    @Test
    void sqlOrderByDescOnAnAliasShadowingTheTimestampKeepsTheAscendingFetch() {
      final String sql = "SELECT value AS ts FROM Point WHERE host = 'host_0' ORDER BY ts DESC LIMIT 1";
      assertThat(explain(sql)).doesNotContain("FETCH FROM TIMESERIES DESC");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(1);
      // The largest `value` of host_0, not its newest timestamp.
      assertThat(((Number) rows.getFirst().getProperty("ts")).doubleValue()).isEqualTo((PER_TAG - 1) * (double) TAGS);
    }

    /**
     * When the timestamp is not projected the planner adds it for the sort and strips it afterwards,
     * and that trailing projection is chained together with the sort step we would be removing.
     */
    @Test
    void sqlOrderByDescWithoutTheTimestampProjectedKeepsTheAscendingFetch() {
      final String sql = "SELECT value FROM Point WHERE host = 'host_0' ORDER BY ts DESC LIMIT 1";
      assertThat(explain(sql)).doesNotContain("FETCH FROM TIMESERIES DESC");

      final List<Result> rows = query(sql);
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().getPropertyNames()).containsExactly("value");
      assertThat(((Number) rows.getFirst().getProperty("value")).doubleValue()).isEqualTo((PER_TAG - 1) * (double) TAGS);
    }

    /**
     * ts.last() mixed with another aggregate still needs the whole series.
     */
    @Test
    void sqlTsLastMixedWithAnotherAggregateKeepsTheFullScan() {
      assertThat(explain("SELECT ts.last(value, ts) AS v, avg(value) AS a FROM Point WHERE host = 'host_0'"))
          .doesNotContain("FETCH FROM TIMESERIES DESC");

      final List<Result> rows = query("SELECT ts.last(value, ts) AS v, avg(value) AS a FROM Point WHERE host = 'host_0'");
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.getFirst().getProperty("v")).doubleValue()).isEqualTo((PER_TAG - 1) * (double) TAGS);
    }

    /**
     * ts.last() over a column that is not the type's timestamp is a plain aggregate over every row.
     */
    @Test
    void sqlTsLastOverAnotherColumnKeepsTheFullScan() {
      assertThat(explain("SELECT ts.last(ts, value) AS v FROM Point WHERE host = 'host_0'"))
          .doesNotContain("FETCH FROM TIMESERIES DESC");
    }

    /**
     * A series that has never been compacted lives entirely in the mutable layer.
     */
    @Test
    void descendingScanWorksWithoutAnySealedBlock() {
      database.command("sql", "CREATE TIMESERIES TYPE Fresh TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Fresh SET ts = 3000, host = 'a', value = 3.0");
        database.command("sql", "INSERT INTO Fresh SET ts = 1000, host = 'a', value = 1.0");
        database.command("sql", "INSERT INTO Fresh SET ts = 2000, host = 'b', value = 2.0");
      });

      final List<Result> rows = query("SELECT ts, value FROM Fresh WHERE host = 'a' ORDER BY ts DESC LIMIT 1");
      assertThat(rows).hasSize(1);
      assertThat(((Number) rows.getFirst().getProperty("value")).doubleValue()).isEqualTo(3.0);

      final List<Result> last = query("SELECT ts.last(value, ts) AS v FROM Fresh WHERE host = 'a'");
      assertThat(((Number) last.getFirst().getProperty("v")).doubleValue()).isEqualTo(3.0);
    }

    @Test
    void descendingScanOnAnEmptySeriesReturnsNoOrNullRows() {
      database.command("sql", "CREATE TIMESERIES TYPE Empty TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");

      assertThat(query("SELECT ts FROM Empty WHERE host = 'a' ORDER BY ts DESC LIMIT 1")).isEmpty();

      // A no-GROUP-BY aggregate over an empty input always returns exactly one row with a null value
      // (issue #6680), same as SELECT avg(x) FROM AnyEmptyType. ts.last() is itself an aggregate
      // function (SQLFunctionTsLast extends SQLAggregatedFunction), so it follows the same rule.
      final List<Result> avg = query("SELECT avg(value) AS v FROM Empty WHERE host = 'a'");
      assertThat(avg).hasSize(1);
      assertThat(avg.getFirst().<Object>getProperty("v")).isNull();

      final List<Result> last = query("SELECT ts.last(value, ts) AS v FROM Empty WHERE host = 'a'");
      assertThat(last).hasSize(1);
      assertThat(last.getFirst().<Object>getProperty("v")).isNull();
    }

    @Test
    void descendingScanHandlesNegativeTimestamps() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Historic TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      final TimeSeriesEngine historic = ((LocalTimeSeriesType) database.getSchema().getType("Historic")).getEngine();

      historic.appendBatch(new long[] { -30_000L, -20_000L, -10_000L, 10_000L },
          new Object[][] { { "a", "b", "a", "b" }, { 1.0, 2.0, 3.0, 4.0 } });
      historic.compactAll();

      final List<Object[]> rows = historic.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "a"), 1,
          null);
      assertThat(rows).hasSize(1);
      assertThat((long) rows.getFirst()[0]).isEqualTo(-10_000L);

      final List<Object[]> bounded = historic.queryDescending(Long.MIN_VALUE, -20_000L, null, TagFilter.eq(0, "a"), 1,
          null);
      assertThat(bounded).hasSize(1);
      assertThat((long) bounded.getFirst()[0]).isEqualTo(-30_000L);
    }

    // ---------------------------------------------------------------------------------------------

    private long newestTimestamp(final TagFilter filter) throws IOException {
      final List<Object[]> all = ascending(Long.MIN_VALUE, Long.MAX_VALUE, filter);
      return (long) all.getLast()[0];
    }

    private List<Object[]> ascending(final long fromTs, final long toTs, final TagFilter filter) throws IOException {
      final List<Object[]> all = new ArrayList<>();
      final Iterator<Object[]> it = engine.iterateQuery(fromTs, toTs, null, filter);
      while (it.hasNext())
        all.add(it.next());
      return all;
    }

    private List<Result> query(final String sql) {
      final List<Result> results = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          results.add(rs.next());
      }
      return results;
    }

    private String explain(final String sql) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
        final StringBuilder sb = new StringBuilder();
        while (rs.hasNext())
          sb.append((String) rs.next().getProperty("executionPlanAsString"));
        return sb.toString();
      }
    }

    private static long epochMillis(final Result row) {
      final Object ts = row.getProperty("ts");
      if (ts instanceof LocalDateTime ldt)
        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
      return ((Number) ts).longValue();
    }
  }

  // ---- #5416 ----
  // Issue #5416: the newest-first scan added by #5414 only covered the sealed layer, so an unbounded
  // last-point query still read the whole unsealed tail: {@code TimeSeriesBucket.scanRange()} walks
  // data pages front to back and materialises every matching row, ignoring the limit. The mutable
  // bucket now has its own descending walk with page pruning and early exit, the sealed layer boxes
  // only the rows it returns, and EXPLAIN shows the pushed-down tag filter.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final int  TAGS    = 4;

  /**
   * Creates the type and appends {@code ticks} samples per tag, all of them left unsealed.
   */
  private TimeSeriesEngine createUnsealed(final int ticks, final int shards) throws IOException {
    database.command("sql",
        "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + shards);
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();
    append5416(engine, 0, ticks);
    return engine;
  }

  private void append5416(final TimeSeriesEngine engine, final int fromTick, final int toTick) throws IOException {
    final int rows = (toTick - fromTick) * TAGS;
    final long[] timestamps = new long[rows];
    final Object[] hosts = new Object[rows];
    final Object[] values = new Object[rows];

    int i = 0;
    for (int t = fromTick; t < toTick; t++)
      for (int h = 0; h < TAGS; h++) {
        timestamps[i] = BASE_TS + (long) t * STEP_MS;
        hosts[i] = "host_" + h;
        values[i] = (double) (t * TAGS + h);
        i++;
      }
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
  }

  /**
   * Rows are {@code Object[]}, whose {@code equals} is identity: compare their contents instead.
   */
  private static List<List<Object>> deep(final List<Object[]> rows) {
    final List<List<Object>> out = new ArrayList<>(rows.size());
    for (final Object[] row : rows)
      out.add(Arrays.asList(row));
    return out;
  }

  private static List<Long> timestamps(final List<Object[]> rows) {
    final List<Long> out = new ArrayList<>(rows.size());
    for (final Object[] row : rows)
      out.add((Long) row[0]);
    return out;
  }

  /**
   * Reference implementation: full ascending scan, filtered and sorted in memory.
   */
  private List<Object[]> expectedNewest(final TimeSeriesEngine engine, final long fromTs, final long toTs,
      final String host, final int limit) throws IOException {
    final List<Object[]> all = new ArrayList<>(
        engine.query(fromTs, toTs, null, host == null ? null : TagFilter.eq(0, host)));
    all.sort((a, b) -> Long.compare((long) b[0], (long) a[0]));
    return limit > 0 && all.size() > limit ? new ArrayList<>(all.subList(0, limit)) : all;
  }

  // #5416
  @Test
  void unsealedTailAnswersTheNewestPointWithoutMaterialisingIt() throws Exception {
    // A single shard so the tag is guaranteed to be present in it: with several shards, a shard
    // holding no matching row has nothing to build a cut-off from and has to look at every page.
    final TimeSeriesEngine engine = createUnsealed(20_000, 1);

    // Everything is still in the mutable layer: no block can be involved.
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isZero();

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_2"), 1, metrics);

    assertThat(deep(newest)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_2", 1)));

    // The point of the fix: the 80k unsealed rows are no longer turned into Object[]. Only the newest
    // page is examined, and every older page is dropped on its header alone.
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(4);
    assertThat(metrics.getScannedPages()).isLessThanOrEqualTo(2);

    // Every page the walk reached was either scanned or skipped: nothing was left unaccounted for,
    // which is what "dropped on its header alone" means. Derived from the stride rather than
    // hard-coded, because how many pages 80k rows occupy is a property of the row format - dictionary
    // -encoding the host tag took this schema from a 274-byte stride to 20 (issue #5519).
    final int rowsPerPage = engine.getShard(0).getMutableBucket().getMaxSamplesPerPage();
    final int dataPages = (20_000 * TAGS + rowsPerPage - 1) / rowsPerPage;
    assertThat(metrics.getScannedPages() + metrics.getSkippedPages()).isEqualTo(dataPages);
  }

  // #5416
  /**
   * Samples are routed round-robin, so a tag can be missing from a shard entirely. Such a shard has
   * no matching row to build a cut-off from, and only the bound carried over from the shards already
   * visited stops it from reading everything it owns.
   */
  @Test
  void aShardWithoutTheTagIsPrunedByTheRunningBound() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(20_000, 4);

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_1"), 1, metrics);

    assertThat(deep(newest)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", 1)));

    // Only the shards visited before the first one holding the tag can be forced to walk their pages.
    final int totalPages = metrics.getScannedPages() + metrics.getSkippedPages();
    assertThat(metrics.getScannedPages()).isLessThan(totalPages * 3 / 4);
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(4);
  }

  // #5416
  @Test
  void unsealedTailTopNMatchesAFullScan() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(5_000, 2);

    for (final int limit : new int[] { 1, 2, 7, 100, 5_000, 10_000 })
      assertThat(deep(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_1"), limit,
          null)))
          .as("limit " + limit)
          .isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", limit)));
  }

  // #5416
  @Test
  void unsealedTailWithoutTagFilterMatchesAFullScan() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(2_000, 2);

    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, null, 10, null);

    // All tags share the same timestamps here, so which row wins a tie is arbitrary in both
    // directions: compare the timestamp sequence, which is what the order is defined on.
    assertThat(timestamps(rows)).isEqualTo(timestamps(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, null, 10)));
    assertThat(deep(rows)).isSubsetOf(deep(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null)));
  }

  // #5416
  @Test
  void unsealedTailHonoursTimeBounds() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(3_000, 2);

    final long fromTs = BASE_TS + 1_000 * STEP_MS;
    final long toTs = BASE_TS + 1_500 * STEP_MS;
    final List<Object[]> rows = engine.queryDescending(fromTs, toTs, null, TagFilter.eq(0, "host_0"), 5, null);

    assertThat(deep(rows)).isEqualTo(deep(expectedNewest(engine, fromTs, toTs, "host_0", 5)));
    assertThat((long) rows.getFirst()[0]).isEqualTo(toTs);
  }

  // #5416
  /**
   * Late arrivals are appended to the last page, so pages are not globally ordered by timestamp. The
   * descending walk must skip a page it cannot use rather than stop at it.
   */
  @Test
  void outOfOrderArrivalsAreStillReturnedNewestFirst() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(4_000, 1);

    // A batch older than everything already stored, landing on the newest pages.
    final long[] timestamps = new long[] { BASE_TS - 5_000, BASE_TS - 4_000 };
    engine.appendBatch(timestamps,
        new Object[][] { new Object[] { "host_0", "host_0" }, new Object[] { -1.0, -2.0 } });

    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_0"),
        10, null);
    assertThat(deep(rows)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_0", 10)));

    // And the old rows are reachable when the range asks for them.
    final List<Object[]> old = engine.queryDescending(Long.MIN_VALUE, BASE_TS - 1, null, TagFilter.eq(0, "host_0"), 10,
        null);
    assertThat(old).hasSize(2);
    assertThat((long) old.getFirst()[0]).isEqualTo(BASE_TS - 4_000);
  }

  // #5416
  @Test
  void sealedAndUnsealedLayersAreMergedNewestFirst() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(5_000, 2);
    engine.compactAll();
    append5416(engine, 5_000, 5_400);

    long mutableRows = 0;
    int blocks = 0;
    for (int s = 0; s < engine.getShardCount(); s++) {
      mutableRows += engine.getShard(s).getMutableBucket().getSampleCount();
      blocks += engine.getShard(s).getSealedStore().getBlockCount();
    }
    assertThat(mutableRows).isPositive();
    assertThat(blocks).isPositive();

    // A limit that straddles the boundary has to pull from both layers.
    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_3"),
        600, null);
    assertThat(deep(rows)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_3", 600)));
  }

  // #5416
  /**
   * The page-level filter compares raw UTF-8 bytes for STRING columns; any other type still has to
   * go through the decoded value, otherwise the leading bytes would be read as a length prefix.
   */
  @Test
  void aFilterOnANonStringColumnStillMatches() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(500, 1);

    // Column 1 is the DOUBLE value, not the tag.
    final double value = 4.0 * 100 + 2;
    final List<Object[]> rows = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(1, value), 5,
        null);

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[2]).isEqualTo(value);
    assertThat(rows.getFirst()[1]).isEqualTo("host_2");
  }

  // #5416
  @Test
  void columnSubsetKeepsTheTagFilterSemantics() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(500, 1);
    final TimeSeriesShard shard = engine.getShard(0);

    // Column 0 is the tag: asking for it keeps the filter usable.
    final List<Object[]> withTag = shard.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 0 },
        TagFilter.eq(0, "host_1"), 3, null);
    assertThat(withTag).hasSize(3);
    for (final Object[] row : withTag)
      assertThat(row[1]).isEqualTo("host_1");

    // Asking only for the value column drops the tag from the ROW, and the filter is applied all the same: it is
    // evaluated before the projection, against the page and against the decompressed block columns, so a
    // condition on a column the caller did not ask for is an ordinary condition (issue #7733). It used to answer
    // nothing here while a sealed fast-path block answered the rows, so the same query gave different answers as
    // compaction ran. Same contract on both scan directions.
    final List<Object[]> withoutTag = shard.scanRangeDescending(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 1 },
        TagFilter.eq(0, "host_1"), 3, null);
    final List<Object[]> ascendingWithoutTag = shard.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 1 },
        TagFilter.eq(0, "host_1"));
    assertThat(withoutTag).hasSize(3);
    for (final Object[] row : withoutTag)
      assertThat(row).as("and the row still carries only the projection: {timestamp, value}").hasSize(2);
    assertThat(ascendingWithoutTag).isNotEmpty();
  }

  // #5416
  @Test
  void emptyBucketReturnsNothing() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

    assertThat(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, TagFilter.eq(0, "host_0"), 1, null))
        .isEmpty();
  }

  // #5416
  /**
   * The sealed layer must return exactly what the boxed implementation used to return, including the
   * INTEGER / LONG / STRING value types.
   */
  @Test
  void sealedRowsKeepTheirValueTypes() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Mixed TIMESTAMP ts TAGS (host STRING) "
        + "FIELDS (d DOUBLE, i INTEGER, l LONG)");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Mixed")).getEngine();

    final int rows = 1_000;
    final long[] timestamps = new long[rows];
    final Object[] hosts = new Object[rows];
    final Object[] doubles = new Object[rows];
    final Object[] ints = new Object[rows];
    final Object[] longs = new Object[rows];
    for (int i = 0; i < rows; i++) {
      timestamps[i] = BASE_TS + (long) i * STEP_MS;
      hosts[i] = "host_" + (i % 2);
      doubles[i] = i + 0.5d;
      ints[i] = i;
      longs[i] = (long) i * 1_000_000L;
    }
    engine.appendBatch(timestamps, new Object[][] { hosts, doubles, ints, longs });
    engine.compactAll();

    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_1"), 1, null);

    assertThat(newest).hasSize(1);
    final Object[] row = newest.getFirst();
    assertThat(row[0]).isEqualTo(BASE_TS + 999L * STEP_MS);
    assertThat(row[1]).isEqualTo("host_1");
    assertThat(row[2]).isEqualTo(999.5d);
    assertThat(row[3]).isEqualTo(999);
    assertThat(row[4]).isEqualTo(999_000_000L);

    // And it agrees with the ascending scan, which still uses the boxed path.
    assertThat(deep(newest)).isEqualTo(deep(expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", 1)));
  }

  // #5416
  @Test
  void sealedTopOneBoxesOnlyTheRowsItReturns() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(20_000, 2);
    engine.compactAll();

    final AggregationMetrics metrics = new AggregationMetrics();
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null,
        TagFilter.eq(0, "host_2"), 1, metrics);

    assertThat(newest).hasSize(1);
    // One row per shard at most, instead of one boxed value per row stored in the block.
    assertThat(metrics.getMaterializedRows()).isLessThanOrEqualTo(engine.getShardCount());
  }

  // #5416
  @Test
  void explainShowsThePushedDownTagFilter() throws Exception {
    createUnsealed(100, 1);

    final String plan = explain("SELECT ts, value FROM Point WHERE host = 'host_1' ORDER BY ts DESC LIMIT 1");
    assertThat(plan).contains("FETCH FROM TIMESERIES DESC Point");
    assertThat(plan).contains("TAGS host = 'host_1'");
    assertThat(plan).contains("TOP 1");
  }

  // #5416
  @Test
  void explainShowsTheTagFilterOnTheAscendingFetchToo() throws Exception {
    createUnsealed(100, 1);

    final String plan = explain("SELECT ts, value FROM Point WHERE host = 'host_1'");
    assertThat(plan).contains("FETCH FROM TIMESERIES Point");
    assertThat(plan).contains("TAGS host = 'host_1'");
    assertThat(plan).doesNotContain("TOP ");
  }

  // #5416
  @Test
  void explainShowsNoTagsWhenThereIsNoTagPredicate() throws Exception {
    createUnsealed(100, 1);

    assertThat(explain("SELECT ts, value FROM Point ORDER BY ts DESC LIMIT 1")).doesNotContain("TAGS");
  }

  // #5416
  /**
   * The planner only turns tag equalities into a {@link TagFilter} today, but the renderer has to
   * cover the multi-value and multi-condition forms the engine API can build.
   */
  @Test
  void tagFilterIsRenderedForEveryConditionShape() {
    final String[] names = new String[] { "host", "rack" };

    assertThat(TagFilter.eq(0, "web_1").describe(names)).isEqualTo("host = 'web_1'");
    assertThat(TagFilter.eq(0, "web_1").and(1, 7).describe(names)).isEqualTo("host = 'web_1' AND rack = 7");
    assertThat(TagFilter.in(0, new LinkedHashSet<>(List.of("b", "a"))).describe(names))
        .isEqualTo("host IN ['a', 'b']");

    // Unknown or missing names fall back to the positional form.
    assertThat(TagFilter.eq(3, "x").describe(names)).isEqualTo("col3 = 'x'");
    assertThat(TagFilter.eq(0, "x").describe(null)).isEqualTo("col0 = 'x'");
  }

  // #5416
  @Test
  void sqlLastPointOnAnUnsealedTailReturnsTheNewestRow() throws Exception {
    final TimeSeriesEngine engine = createUnsealed(10_000, 2);

    final long expectedTs = (long) expectedNewest(engine, Long.MIN_VALUE, Long.MAX_VALUE, "host_1", 1).getFirst()[0];

    try (final ResultSet rs = database.query("sql",
        "SELECT ts, value FROM Point WHERE host = 'host_1' ORDER BY ts DESC LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((LocalDateTime) row.getProperty("ts")).toInstant(ZoneOffset.UTC).toEpochMilli())
          .isEqualTo(expectedTs);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return (String) rs.next().getProperty("executionPlanAsString");
    }
  }
}
