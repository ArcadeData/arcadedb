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
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10324: a TIMESERIES row scan whose WHERE the fetch step already reproduces exactly - a time range, tag equalities and
 * range predicates on numeric FIELD columns - evaluated the whole WHERE a second time in a FILTER step over every row it returned.
 * The residual filter is now planned only for the predicates the engine does not reproduce. Every query is checked against a
 * document twin holding the same rows, in every storage state (mutable, sealed, sealed plus a fresh tail).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10324TimeSeriesRowScanPushDownTest extends TestHelper {
  private static final long T0    = 1_767_225_600_000L;
  private static final long HOUR  = 3_600_000L;
  private static final int  HOSTS = 4;

  private static final String RANGE = "ts >= " + (T0 + 20 * 60_000L) + " AND ts < " + (T0 + 2 * HOUR + 7 * 60_000L);

  private void forEachState(final int shards, final Runnable check) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (uu DOUBLE, ui LONG, uf FLOAT) SHARDS "
        + shards + " COMPACTION_INTERVAL 1 HOURS");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    load(0, 120);
    check.run(); // everything in the mutable buffer
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    check.run(); // everything sealed
    load(120, 170);
    check.run(); // sealed blocks plus a fresh tail
    database.command("sql", "DROP TYPE T");
    database.command("sql", "DROP TYPE D");
  }

  private void load(final int fromSample, final int toSample) {
    database.transaction(() -> {
      for (int s = fromSample; s < toSample; s++)
        for (int h = 0; h < HOSTS; h++) {
          final long ts = T0 + s * 60_000L + h * 1_000L;
          // ABSENT VALUES ON BOTH A FLOATING-POINT AND AN INTEGRAL COLUMN: A PUSHED PREDICATE MUST DROP THEM AS SQL DOES
          final Double uu = (s + h) % 9 == 0 ? null : (double) ((s * 31 + h * 17) % 101);
          final Long ui = (s + h) % 7 == 0 ? null : s * 3L + h;
          final float uf = ((s + h) % 10) / 10f;
          final String host = "host_" + h;
          database.command("sql", "INSERT INTO T SET ts = ?, host = ?, uu = ?, ui = ?, uf = ?", ts, host, uu, ui, uf);
          database.command("sql", "INSERT INTO D SET ts = ?, host = ?, uu = ?, ui = ?, uf = ?", ts, host, uu, ui, uf);
        }
    });
  }

  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final TreeMap<String, String> sorted = new TreeMap<>();
        for (final String name : r.getPropertyNames()) {
          final Object value = r.getProperty(name);
          final String text;
          // the twin stores the timestamp as the number it was given, the TIMESERIES type hands it back as a date-time
          if (value instanceof LocalDateTime d)
            text = String.format(Locale.ROOT, "%.6f", (double) d.toInstant(ZoneOffset.UTC).toEpochMilli());
          else if (value instanceof Date d)
            text = String.format(Locale.ROOT, "%.6f", (double) d.getTime());
          else if (value instanceof Number n)
            text = String.format(Locale.ROOT, "%.6f", n.doubleValue());
          else
            text = String.valueOf(value);
          sorted.put(name, text);
        }
        out.add(sorted.toString());
      }
    }
    out.sort(null);
    return out;
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** {@code sql} names the type {@code X}; it is run against the TIMESERIES type and against its document twin. */
  private void assertSameAsTheTwin(final String sql) {
    final List<String> expected = rows(sql.replace(" X", " D"));
    assertThat(expected).as("the twin answers something: " + sql).isNotEmpty();
    assertThat(rows(sql.replace(" X", " T"))).as(sql).isEqualTo(expected);
  }

  private void assertNoResidualFilter(final String sql) {
    assertThat(plan(sql.replace(" X", " T"))).as(sql).contains("FETCH FROM TIMESERIES").doesNotContain("FILTER ITEMS");
    assertSameAsTheTwin(sql);
  }

  private void assertResidualFilter(final String sql) {
    assertThat(plan(sql.replace(" X", " T"))).as(sql).contains("FETCH FROM TIMESERIES").contains("FILTER ITEMS");
    assertSameAsTheTwin(sql);
  }

  @Test
  void aWhereTheFetchReproducesIsNotEvaluatedAgain() {
    for (final int shards : new int[] { 1, 3 })
      forEachState(shards, () -> {
        // THE REPORTED SHAPE: A TIME RANGE AND A FIELD PREDICATE
        assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE " + RANGE + " AND uu > 90.0");
        assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE " + RANGE);
        assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE uu > 90.0");
        assertNoResidualFilter("SELECT uu FROM X WHERE " + RANGE);
        assertNoResidualFilter("SELECT FROM X WHERE host = 'host_2' AND " + RANGE + " AND ui BETWEEN 30 AND 200");
        assertNoResidualFilter("SELECT ts, ui FROM X WHERE ui >= 0");
        assertNoResidualFilter("SELECT ts, uu FROM X WHERE uu = 50");
        assertNoResidualFilter("SELECT host, ts FROM X WHERE host = 'host_1' ORDER BY ts DESC LIMIT 5");
        assertNoResidualFilter("SELECT host, ts FROM X WHERE " + RANGE + " LIMIT 7");
      });
  }

  @Test
  void aPredicateTheFetchDoesNotReproduceIsStillFiltered() {
    forEachState(2, () -> {
      // A FLOAT column, a negation and an OR of two different tags are left to the SQL filter
      assertResidualFilter("SELECT host, ts, uf FROM X WHERE " + RANGE + " AND uf > 0.5");
      assertResidualFilter("SELECT host, ts, uu FROM X WHERE " + RANGE + " AND host <> 'host_1'");
      assertResidualFilter("SELECT host, ts, uu FROM X WHERE uu > 90.0 AND (host = 'host_1' OR ui > 300)");
      // a field predicate takes no row cap into the fetch, so it goes to the unbounded read, which reproduces it, and the LIMIT
      // runs above
      assertNoResidualFilter("SELECT host, ts, uu FROM X WHERE uu > 90.0 LIMIT 3");
      // the same tag twice is an intersection the tag filter does not model exactly
      assertResidualFilter("SELECT host, ts FROM X WHERE host = 'host_1' AND host = 'host_1' AND " + RANGE);
    });
  }

  @Test
  void aProjectionOfColumnsIsBuiltByTheFetch() {
    forEachState(2, () -> {
      // THE REPORTED SHAPE: THE ROW IS BUILT ONCE, IN THE FETCH, AND NO PROJECTION STEP COPIES IT
      final String reported = "SELECT host, ts, uu FROM X WHERE " + RANGE + " AND uu > 90.0";
      assertThat(plan(reported.replace(" X", " T"))).contains("PROJECTED host, ts, uu").doesNotContain("CALCULATE PROJECTIONS");
      assertSameAsTheTwin(reported);

      // ALIASES, IN PROJECTION ORDER
      assertThat(plan("SELECT uu AS value, host AS h FROM T WHERE " + RANGE)).contains("PROJECTED uu AS value, host AS h");
      assertSameAsTheTwin("SELECT uu AS value, host AS h FROM X WHERE " + RANGE);
      assertThat(columnNames("SELECT uu AS value, host AS h, ts FROM T WHERE " + RANGE)).containsExactly("value", "h", "ts");

      // AN ORDER BY ON A COLUMN THE PROJECTION LEAVES OUT: THE COLUMN IS PROJECTED FOR THE SORT AND DROPPED AFTER IT
      assertSameAsTheTwin("SELECT host, uu FROM X WHERE " + RANGE + " ORDER BY ui DESC, ts LIMIT 20");
      assertThat(columnNames("SELECT host, uu FROM T WHERE " + RANGE + " ORDER BY ui DESC, ts LIMIT 20")).containsExactly("host", "uu");
      assertSameAsTheTwin("SELECT host FROM X WHERE host = 'host_1' ORDER BY ts DESC LIMIT 5");
      assertSameAsTheTwin("SELECT DISTINCT host FROM X WHERE " + RANGE);
      assertSameAsTheTwin("SELECT uu FROM X WHERE " + RANGE + " SKIP 10 LIMIT 15");

      // NOT A PLAIN COLUMN, OR A WHERE THE FETCH DOES NOT REPRODUCE: THE PROJECTION STEP STAYS
      assertThat(plan("SELECT host, uu * 2 AS d FROM T WHERE " + RANGE)).contains("CALCULATE PROJECTIONS").doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT host, uu * 2 AS d FROM X WHERE " + RANGE);
      assertThat(plan("SELECT host, uf FROM T WHERE uf > 0.5")).contains("CALCULATE PROJECTIONS").doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT host, uf FROM X WHERE uf > 0.5");
      assertThat(plan("SELECT FROM T WHERE " + RANGE)).doesNotContain("PROJECTED");
      assertThat(plan("SELECT *, uu AS v FROM T WHERE " + RANGE)).doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT count(*) AS c, host FROM X WHERE " + RANGE + " GROUP BY host");
    });
  }

  @Test
  void duplicatedColumnsCaseAndLiteralsTheTagFilterDoesNotModel() {
    forEachState(1, () -> {
      // THE SAME COLUMN TWICE, UNDER ITS NAME AND AN ALIAS
      assertThat(plan("SELECT uu, uu AS b, host, uu FROM T WHERE " + RANGE)).contains("PROJECTED");
      assertSameAsTheTwin("SELECT uu, uu AS b, host, uu FROM X WHERE " + RANGE);
      assertThat(columnNames("SELECT uu, uu AS b, host FROM T WHERE " + RANGE)).containsExactly("uu", "b", "host");
      // A NAME THAT IS NOT A COLUMN AS WRITTEN (NAMES ARE CASE-SENSITIVE) IS LEFT TO THE PROJECTION STEP, WHICH ANSWERS NULL
      assertThat(plan("SELECT HOST, uu FROM T WHERE " + RANGE)).doesNotContain("PROJECTED");
      assertSameAsTheTwin("SELECT HOST, uu FROM X WHERE " + RANGE);
      // A NULL TAG LITERAL IS NO EXACT FILTER, A LITERAL OF ANOTHER CASE IS ONE THAT MATCHES NOTHING: BOTH ANSWER AS THE TWIN
      assertThat(plan("SELECT host FROM T WHERE host = null AND " + RANGE)).contains("FILTER ITEMS");
      assertThat(rows("SELECT host FROM T WHERE host = null AND " + RANGE)).isEqualTo(rows("SELECT host FROM D WHERE host = null AND " + RANGE));
      assertThat(plan("SELECT host FROM T WHERE host = 'HOST_1'")).doesNotContain("FILTER ITEMS");
      assertThat(rows("SELECT host FROM T WHERE host = 'HOST_1'")).isEmpty();
      assertThat(rows("SELECT host FROM D WHERE host = 'HOST_1'")).isEmpty();
    });
  }

  @Test
  void aTimestampReadsBackAsTheSameDateTimeAcrossDayBoundariesAndBefore1970() {
    database.command("sql", "CREATE TIMESERIES TYPE B TIMESTAMP ts FIELDS (v LONG) SHARDS 1");
    final long day = 86_400_000L;
    final long[] stamps = { -day - 1, -day, -1, 0, 1, day - 1, day, 3 * day + 12_345, T0 };
    for (final long ts : stamps)
      database.command("sql", "INSERT INTO B SET ts = ?, v = ?", ts, ts);
    for (int pass = 0; pass < 2; pass++) {
      try (final ResultSet rs = database.query("sql", "SELECT ts, v FROM B ORDER BY ts")) {
        int n = 0;
        while (rs.hasNext()) {
          final Result r = rs.next();
          final long ts = r.<Number>getProperty("v").longValue();
          assertThat(r.<LocalDateTime>getProperty("ts")).as("ts " + ts)
              .isEqualTo(LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(ts), ZoneOffset.UTC));
          n++;
        }
        assertThat(n).isEqualTo(stamps.length);
      }
      database.command("sql", "COMPACT TIMESERIES TYPE B");
    }
  }

  private List<String> columnNames(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return new ArrayList<>(rs.next().getPropertyNames());
    }
  }

  @Test
  void aTemporalTagLiteralTheColumnCannotReadIsLeftToTheFilter() {
    database.command("sql", "CREATE TIMESERIES TYPE TD TIMESTAMP ts TAGS (day DATE) FIELDS (v LONG) SHARDS 1");
    database.command("sql", "INSERT INTO TD SET ts = 1000, day = '2026-01-01', v = 1");
    database.command("sql", "INSERT INTO TD SET ts = 2000, day = '2026-01-02', v = 2");
    // the literal is not pushed into the tag filter, so the fetch returns every row and only the filter can drop them
    assertThat(plan("SELECT FROM TD WHERE day = 'not a date'")).contains("FILTER ITEMS");
    assertThat(plan("SELECT FROM TD WHERE day = '2026-01-02'")).doesNotContain("FILTER ITEMS");
    // a literal finer than the tag's precision is truncated into the tag filter, which then keeps the whole day: only the filter
    // gives the SQL answer, no row
    try (final ResultSet rs = database.query("sql", "SELECT FROM TD WHERE day = ?", LocalDateTime.of(2026, 1, 2, 10, 30))) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT v FROM TD WHERE day = '2026-01-02'")) {
      assertThat(rs.next().<Number>getProperty("v").longValue()).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
