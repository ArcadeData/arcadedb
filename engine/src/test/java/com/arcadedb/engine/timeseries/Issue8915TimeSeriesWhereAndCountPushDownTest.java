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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #8915, #8916 and #8917: the TIMESERIES planner answered differently from the same SQL over a document type.
 * <ul>
 *   <li>#8915: the aggregation push-down counted every row for {@code count(field)}, nulls included, as a Double;</li>
 *   <li>#8916: a time range on one OR branch was applied to the whole WHERE;</li>
 *   <li>#8917: two equalities on the same tag in one AND were unioned instead of intersected.</li>
 * </ul>
 * Each query runs against a TIMESERIES type and a document twin holding the same rows, before and after compaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8915TimeSeriesWhereAndCountPushDownTest extends TestHelper {

  private static final long T0 = 1_790_812_800_000L;

  private void load() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE, n LONG)");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> {
      for (int i = 1; i <= 4; i++) {
        final Double v = i % 2 == 0 ? null : (double) i; // the even ones are null
        database.command("sql", "INSERT INTO T SET ts = ?, host = ?, v = ?, n = 1", i * 1000L, i % 2 == 1 ? "a" : "b", v);
        database.command("sql", "INSERT INTO D SET ts = ?, host = ?, v = ?, n = 1", i * 1000L, i % 2 == 1 ? "a" : "b", v);
      }
    });
  }

  private void forEachState(final Runnable check) {
    load();
    check.run();
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    check.run();
  }

  private List<Object> ns(final String sql) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("n"));
    }
    return out;
  }

  @Test
  void countOfFieldSkipsNullsOnEveryPathAndIsALong() {
    forEachState(() -> {
      for (final String where : new String[] { "", "WHERE n > 0 " }) {
        try (final ResultSet rs = database.query("sql",
            "SELECT ts.timeBucket('1h', ts) AS b, count(v) AS cv, count(*) AS c FROM T " + where + "GROUP BY b")) {
          assertThat(rs.hasNext()).isTrue();
          final Result r = rs.next();
          assertThat((Object) r.getProperty("cv")).as(where).isEqualTo(2L);
          assertThat((Object) r.getProperty("c")).as(where).isEqualTo(4L);
          assertThat(rs.hasNext()).isFalse();
        }
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(v) AS cv, count(*) AS c FROM T")) {
        final Result r = rs.next();
        assertThat((Object) r.getProperty("cv")).isEqualTo(2L);
        assertThat((Object) r.getProperty("c")).isEqualTo(4L);
      }
    });
  }

  @Test
  void orWithATimeRangeOnOneBranchKeepsTheOtherBranchRows() {
    forEachState(() -> {
      for (final String w : new String[] { "ts >= 3000 OR host = 'a'", "ts < 2000 OR ts >= 4000", "ts < 2000 OR ts >= 3000",
          "(ts >= 3000 AND host = 'b') OR ts <= 1000", "ts BETWEEN 1000 AND 1000 OR ts = 4000", "ts > 4000 OR ts < 1000" }) {
        final List<Object> expected = ns("SELECT v AS n FROM D WHERE " + w + " ORDER BY ts");
        assertThat(ns("SELECT v AS n FROM T WHERE " + w + " ORDER BY ts")).as(w).isEqualTo(expected);
        try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE " + w)) {
          assertThat(((Number) rs.next().getProperty("c")).longValue()).as(w).isEqualTo(
              ((Number) database.query("sql", "SELECT count(*) AS c FROM D WHERE " + w).next().getProperty("c")).longValue());
        }
        // grouped, whether or not it is pushed down
        try (final ResultSet rs = database.query("sql",
            "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T WHERE " + w + " GROUP BY b");
            final ResultSet ref = database.query("sql",
                "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM D WHERE " + w + " GROUP BY b")) {
          final long got = rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0L;
          final long want = ref.hasNext() ? ((Number) ref.next().getProperty("c")).longValue() : 0L;
          assertThat(got).as("grouped " + w).isEqualTo(want);
        }
      }
    });
  }

  @Test
  void twoEqualitiesOnTheSameTagInOneAndMatchNothing() {
    forEachState(() -> {
      for (final String w : new String[] { "host = 'a' AND host = 'b'", "host = 'a' AND host = 'b' AND v > 0",
          "(host = 'a' AND host = 'b') OR host = 'a'", "host = 'a' AND host = 'a'" }) {
        final String agg = "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c, sum(n) AS s FROM %s WHERE " + w + " GROUP BY b";
        try (final ResultSet rs = database.query("sql", String.format(agg, "T"));
            final ResultSet ref = database.query("sql", String.format(agg, "D"))) {
          final long got = rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0L;
          final long want = ref.hasNext() ? ((Number) ref.next().getProperty("c")).longValue() : 0L;
          assertThat(got).as(w).isEqualTo(want);
        }
      }
      try (final ResultSet rs = database.query("sql",
          "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T WHERE host = 'a' AND host = 'b' GROUP BY b")) {
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void orOfTagsAcrossBlocksStaysExact() {
    forEachState(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c FROM T WHERE host = 'a' OR host = 'b' GROUP BY b")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(4L);
      }
    });
  }
}
