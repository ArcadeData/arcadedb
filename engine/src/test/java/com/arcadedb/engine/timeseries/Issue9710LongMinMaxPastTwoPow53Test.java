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
 * Issue #9710: since the ungrouped TimeSeries aggregate push-down (#9612) the engine answers {@code min}/{@code max} through a
 * double, so a LONG sample needing more than 53 bits came back rounded - as a {@code Long} for some shapes and a {@code Double}
 * for others - and under a field filter even as a value the WHERE clause excludes. They must be the stored longs, which is what
 * a document twin holding the same rows answers.
 */
class Issue9710LongMinMaxPastTwoPow53Test extends TestHelper {
  private static final long T0   = 1_767_225_600_000L;
  private static final long BASE = 9_007_199_254_740_990L; // 2^53 - 2

  private static final String[] WHERE = { "", //
      " WHERE bg BETWEEN 9007199254740991 AND 9007199254740993", //
      " WHERE bg >= 9007199254740993", //
      " WHERE bg > 9007199254740992 AND bg < 9007199254740997", //
      " WHERE host = 'h1'", //
      " WHERE host = 'h2' AND bg >= 9007199254740993", //
      " WHERE ts >= " + (T0 + 10_000L) + " AND ts < " + (T0 + 50_000L), //
      " WHERE ts > " + (T0 + 10_000L) + " AND ts <= " + (T0 + 50_000L) + " AND bg <= 9007199254740997" };

  private void createAndLoad(final int shards, final long base, final int from, final int to, final boolean createTypes) {
    if (createTypes) {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, d DOUBLE) SHARDS " + shards);
      database.command("sql", "CREATE DOCUMENT TYPE D");
    }
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, bg = ?, d = ?", T0 + i * 1_000L, "h" + (i % 3),
              base + i % 9, (double) i);
    });
  }

  /** The only row of an ungrouped answer, each value spelled with its Java class. */
  private String row(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Result r = rs.next();
      assertThat(rs.hasNext()).isFalse();
      final List<String> out = new ArrayList<>();
      for (final String name : r.getPropertyNames().stream().sorted().toList()) {
        final Object value = r.getProperty(name);
        out.add(name + "=" + value + (value == null ? "" : "(" + value.getClass().getSimpleName() + ")"));
      }
      return out.toString();
    }
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private void assertEveryShapeAnswersTheStoredLongs() {
    for (final String where : WHERE) {
      final String sql = "SELECT min(bg) AS mn, max(bg) AS mx, count(*) AS c FROM X" + where;
      // the case this issue is about is the push-down, so a plan that stops using it would not prove anything
      assertThat(plan(sql.replace("FROM X", "FROM T"))).as(sql).contains("AGGREGATE FROM TIMESERIES").contains("ungrouped");
      assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
    }
  }

  @Test
  void ungroupedMinMaxOfALongPastTwoPow53AreTheStoredLongs() {
    for (final int shards : new int[] { 1, 3 }) {
      createAndLoad(shards, BASE, 0, 90, true);
      assertEveryShapeAnswersTheStoredLongs(); // mutable buffer
      database.command("sql", "COMPACT TIMESERIES TYPE T");
      assertEveryShapeAnswersTheStoredLongs(); // sealed blocks
      createAndLoad(shards, BASE, 90, 120, false);
      assertEveryShapeAnswersTheStoredLongs(); // sealed blocks plus a mutable tail
      database.command("sql", "DROP TYPE T");
      database.command("sql", "DROP TYPE D");
    }
  }

  @Test
  void theReportedAnswersAreExact() {
    createAndLoad(1, BASE, 0, 90, true);
    for (int pass = 0; pass < 2; pass++) {
      try (final ResultSet rs = database.query("sql", "SELECT min(bg) AS mn, max(bg) AS mx FROM T WHERE bg >= 9007199254740993")) {
        final Result r = rs.next();
        // 9007199254740993 rounds to 2^53 as a double, a value the WHERE clause excludes
        assertThat(r.<Object>getProperty("mn")).isEqualTo(9_007_199_254_740_993L);
        assertThat(r.<Object>getProperty("mx")).isEqualTo(9_007_199_254_740_998L);
      }
      try (final ResultSet rs = database.query("sql",
          "SELECT min(bg) AS mn, max(bg) AS mx FROM T WHERE bg BETWEEN 9007199254740991 AND 9007199254740993")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("mn")).isEqualTo(9_007_199_254_740_991L);
        assertThat(r.<Object>getProperty("mx")).isEqualTo(9_007_199_254_740_993L);
      }
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void negativeLongsPastMinusTwoPow53AreExactToo() {
    createAndLoad(2, -BASE - 8, 0, 90, true); // -9007199254740998 .. -9007199254740990
    assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(row("SELECT min(bg) AS mn, max(bg) AS mx FROM D"));
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(row("SELECT min(bg) AS mn, max(bg) AS mx FROM D"));
    assertThat(row("SELECT min(bg) AS mn FROM T")).isEqualTo("[mn=-9007199254740998(Long)]");
  }

  @Test
  void otherAggregatesOfTheSameRowAreUntouchedByTheExactRecount() {
    createAndLoad(2, BASE, 0, 90, true);
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    final String sql = "SELECT min(bg) AS mn, max(bg) AS mx, count(*) AS c, min(d) AS md, max(d) AS xd, avg(d) AS ad FROM X WHERE bg >= 9007199254740993";
    assertThat(row(sql.replace("FROM X", "FROM T"))).isEqualTo(row(sql.replace("FROM X", "FROM D")));
  }

  @Test
  void extremesAtTheThresholdAndAtTheLimitsOfALong() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, sm LONG) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    // bg: exactly 2^53 and its neighbours, plus both limits of a long; sm: small values, which no recount touches
    final long[] bg = { (1L << 53) - 1, 1L << 53, (1L << 53) + 1, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1 };
    database.transaction(() -> {
      for (int i = 0; i < 70; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, bg = ?, sm = ?", T0 + i * 1_000L, "h" + (i % 2),
              bg[i % bg.length], (long) i);
    });
    final String[] queries = { "SELECT min(bg) AS mn, max(bg) AS mx, min(sm) AS ms, max(sm) AS xs FROM X",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE bg BETWEEN 9007199254740991 AND 9007199254740993",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE bg >= 9007199254740992 AND bg < 9223372036854775807",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE bg > -9223372036854775808 AND bg <= 9007199254740992",
        // min under 2^53, max over it: only the max is recounted
        "SELECT min(bg) AS mn, max(bg) AS mx, max(sm) AS xs FROM X WHERE bg >= 9007199254740991 AND bg <= 9007199254740993",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE host = 'h1'" };
    for (int pass = 0; pass < 2; pass++) {
      for (final String sql : queries)
        assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
    assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx FROM T")).isEqualTo(
        "[mn=" + Long.MIN_VALUE + "(Long), mx=" + Long.MAX_VALUE + "(Long)]");
    assertThat(row("SELECT max(bg) AS mx FROM T WHERE bg <= 9007199254740992")).isEqualTo("[mx=9007199254740992(Long)]");
  }

  @Test
  void twoLongColumnsRecountedTogetherUnderAFilterOnAThird() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (a LONG, b LONG, f LONG) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> {
      for (int i = 0; i < 80; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, a = ?, b = ?, f = ?", T0 + i * 1_000L, "h" + (i % 2),
              BASE + i % 9, -BASE - i % 7, (long) i);
    });
    final String[] queries = { "SELECT min(a) AS na, max(a) AS xa, min(b) AS nb, max(b) AS xb FROM X WHERE f >= 20 AND f < 60",
        "SELECT max(a) AS xa, min(a) AS na, max(a) AS xa2, min(b) AS nb, count(*) AS c FROM X WHERE f BETWEEN 3 AND 41 AND host = 'h0'",
        "SELECT min(a) AS na, max(a) AS xa, min(b) AS nb, max(b) AS xb FROM X" };
    for (int pass = 0; pass < 2; pass++) {
      for (final String sql : queries) {
        assertThat(plan(sql.replace("FROM X", "FROM T"))).as(sql).contains("ungrouped");
        assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
      }
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void samplesWithoutAMeasurementDoNotDisturbTheRecount() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, f LONG) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, bg = ?, f = ?", T0 + i * 1_000L, "h" + (i % 2),
              i % 3 == 0 ? null : BASE + i % 9, (long) i);
    });
    // a sample stored without bg is a sample with no value since issue #9721, which min skips like the twin does
    final String[] queries = { "SELECT min(bg) AS mn, max(bg) AS mx, count(bg) AS c FROM X",
        "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE f >= 10", "SELECT min(bg) AS mn, max(bg) AS mx FROM X WHERE host = 'h1'" };
    for (int pass = 0; pass < 2; pass++) {
      for (final String sql : queries)
        assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(row(sql.replace("FROM X", "FROM D")));
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void anEmptyRangeStillAnswersNull() {
    createAndLoad(1, BASE, 0, 90, true);
    final String sql = "SELECT min(bg) AS mn, max(bg) AS mx, count(*) AS c FROM X WHERE bg > 9007199254741000";
    assertThat(row(sql.replace("FROM X", "FROM T"))).isEqualTo(row(sql.replace("FROM X", "FROM D")));
  }
}
