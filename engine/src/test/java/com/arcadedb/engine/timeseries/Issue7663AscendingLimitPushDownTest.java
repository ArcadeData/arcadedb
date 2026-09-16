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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
 * The residency saving itself is asserted at engine level by {@code Issue7336AscendingLimitTest}, which counts the
 * blocks decompressed. What this class pins is the planner decision the SQL path depends on - the cap reaches the
 * fetch, visible as {@code TOP n} in the plan - and, for every shape where a row could still be discarded
 * downstream, that it does NOT. A cap pushed past a residual filter returns too few rows, which is the failure
 * mode the guards exist for.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7663">issue #7663</a>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7663AscendingLimitPushDownTest extends TestHelper {

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
