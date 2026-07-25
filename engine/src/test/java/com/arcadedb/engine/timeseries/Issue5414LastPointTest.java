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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5414: an unbounded last-point-per-tag query used to scan the tag's whole series, because the
 * only scan direction available was ascending and the sealed layer materialises every matching row
 * before the caller sees the first one. The descending scan walks sealed blocks newest-first and
 * stops as soon as the requested number of rows has been produced, so the unbounded form costs
 * O(blocks touched) instead of O(series).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5414LastPointTest extends TestHelper {

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
  void descendingScanTouchesOnlyTheNewestBlocks() throws IOException {
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
  void descendingScanReturnsTheNewestRowsInOrder() throws IOException {
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
  void descendingScanHonoursTimeBoundsAndUnlimitedMode() throws IOException {
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
  void descendingScanSeesRowsStillInTheMutableLayer() throws IOException {
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
  void descendingScanOnAnEmptySeriesReturnsNothing() {
    database.command("sql", "CREATE TIMESERIES TYPE Empty TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");

    assertThat(query("SELECT ts FROM Empty WHERE host = 'a' ORDER BY ts DESC LIMIT 1")).isEmpty();

    // Same as any other aggregate over an empty time series: no row at all.
    assertThat(query("SELECT avg(value) AS v FROM Empty WHERE host = 'a'")).isEmpty();
    assertThat(query("SELECT ts.last(value, ts) AS v FROM Empty WHERE host = 'a'")).isEmpty();
  }

  @Test
  void descendingScanHandlesNegativeTimestamps() throws IOException {
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
