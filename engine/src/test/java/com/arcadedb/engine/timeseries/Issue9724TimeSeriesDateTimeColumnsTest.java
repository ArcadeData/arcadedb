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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #9724 and #9725: a TimeSeries DATE/DATETIME column (not the TIMESTAMP one) answered SQL with the raw epoch count in its
 * own unit, while INSERT reads a number as epoch MILLIS like a document property does. So a value read back from a row could not
 * be written back as it was (a DATETIME_NANOS count read as millis saturated to Long.MAX_VALUE nanos), and filtering by the very
 * literal INSERT accepted matched nothing, on a tag (push-down) and on a field. Such a column now reads back as the date/time
 * object a document property of the same type holds, a filter literal is converted exactly as INSERT converts it, and an instant
 * the precision cannot represent is refused instead of saturated - on a document property too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9724TimeSeriesDateTimeColumnsTest extends TestHelper {
  private static final String[] TYPES = { "DATE", "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS" };

  @Test
  void theReportedScenario() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME) FIELDS (v DATETIME) SHARDS 1");
    database.command("sql", "INSERT INTO T SET ts = 1700000000000, k = '2023-11-14 22:15:23', v = '2023-11-14 22:15:23'");
    for (int pass = 0; pass < 2; pass++) {
      assertThat(count("SELECT FROM T")).isEqualTo(1);
      final String byTag = "SELECT FROM T WHERE k = '2023-11-14 22:15:23'";
      assertThat(plan(byTag)).contains("TAGS");
      assertThat(count(byTag)).as("tag push-down, pass " + pass).isEqualTo(1);
      assertThat(count("SELECT FROM T WHERE v = '2023-11-14 22:15:23'")).as("field filter, pass " + pass).isEqualTo(1);
      assertThat(count("SELECT FROM T WHERE k = '2023-11-14 22:15:24'")).isZero();
      assertThat(count("SELECT FROM T WHERE v = ?", LocalDateTime.of(2023, 11, 14, 22, 15, 23))).isEqualTo(1);
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void everyPrecisionReadsBackLikeADocumentTwinAndFiltersByItsOwnLiteral() {
    final StringBuilder tags = new StringBuilder();
    final StringBuilder fields = new StringBuilder();
    database.command("sql", "CREATE DOCUMENT TYPE D");
    for (int t = 0; t < TYPES.length; t++) {
      tags.append(t > 0 ? ", " : "").append("k").append(t).append(' ').append(TYPES[t]);
      fields.append(t > 0 ? ", " : "").append("v").append(t).append(' ').append(TYPES[t]);
      database.command("sql", "CREATE PROPERTY D.k" + t + " " + TYPES[t]);
      database.command("sql", "CREATE PROPERTY D.v" + t + " " + TYPES[t]);
    }
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (" + tags + ") FIELDS (" + fields + ") SHARDS 2");

    final Object[] literals = { "2023-11-14 22:15:23.123456789", LocalDateTime.of(2023, 11, 14, 22, 15, 23, 987_654_321), 1_700_000_123_456L };
    database.transaction(() -> {
      for (int r = 0; r < literals.length; r++)
        for (final String type : new String[] { "T", "D" }) {
          final StringBuilder sql = new StringBuilder("INSERT INTO " + type + " SET ts = " + (1_700_000_000_000L + r * 1_000L));
          final List<Object> args = new ArrayList<>();
          for (int t = 0; t < TYPES.length; t++) {
            sql.append(", k").append(t).append(" = ?, v").append(t).append(" = ?");
            args.add(literalFor(literals[r], t));
            args.add(literalFor(literals[r], t));
          }
          database.command("sql", sql.toString(), args.toArray());
        }
    });

    for (int pass = 0; pass < 2; pass++) {
      final List<String> onT = new ArrayList<>();
      final List<String> onD = new ArrayList<>();
      for (int t = 0; t < TYPES.length; t++) {
        onT.addAll(rows("SELECT k" + t + ", v" + t + " FROM T ORDER BY ts"));
        onD.addAll(rows("SELECT k" + t + ", v" + t + " FROM D ORDER BY ts"));
        for (final Object any : literals) {
          final Object literal = literalFor(any, t);
          // a literal finer than the column's precision does not match on the twin either: the stored value lost its fraction
          final long expected = count("SELECT FROM D WHERE k" + t + " = ?", literal);
          if (TYPES[t].equals("DATETIME_NANOS"))
            assertThat(expected).as(TYPES[t] + " " + literal).isPositive();
          assertThat(count("SELECT FROM T WHERE k" + t + " = ?", literal)).as("tag " + TYPES[t] + " = " + literal).isEqualTo(expected);
          assertThat(count("SELECT FROM T WHERE v" + t + " = ?", literal)).as("field " + TYPES[t] + " = " + literal).isEqualTo(expected);
        }
      }
      assertThat(onT).as("pass " + pass).isEqualTo(onD);
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void aValueReadFromARowWritesBackAsItIs() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (n DATETIME_NANOS, s DATETIME_SECOND, d DATE)");
    database.command("sql", "INSERT INTO T SET ts = 1, host = 'a', n = '2023-11-14 22:15:23.123456789', s = '2023-11-14 22:15:23', d = '2023-11-14'");
    final Result first = database.query("sql", "SELECT FROM T").next();
    final Object[] readBack = { first.getProperty("n"), first.getProperty("s"), first.getProperty("d") };
    database.command("sql", "INSERT INTO T SET ts = 2, host = 'b', n = ?, s = ?, d = ?", readBack);
    final List<String> rows = rows("SELECT n, s, d FROM T ORDER BY ts");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(1)).isEqualTo(rows.get(0));
    assertThat(first.<Object>getProperty("n")).isEqualTo(LocalDateTime.of(2023, 11, 14, 22, 15, 23, 123_456_789));
  }

  @Test
  void aPreEpochInstantRoundTripsInEveryPrecision() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME_NANOS) FIELDS (n DATETIME_NANOS, u DATETIME_MICROS, "
        + "s DATETIME_SECOND, d DATE)");
    // before 1970, where a floored division and a truncated one land on different units
    final LocalDateTime instant = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999);
    database.command("sql", "INSERT INTO T SET ts = 1, k = ?, n = ?, u = ?, s = ?, d = ?", instant, instant, instant, instant, instant);
    for (int pass = 0; pass < 2; pass++) {
      assertThat(rows("SELECT k, n, u, s, d FROM T")).containsExactly(
          "[d=1969-12-31(LocalDate), k=1969-12-31T23:59:59.999999999(LocalDateTime), n=1969-12-31T23:59:59.999999999(LocalDateTime), "
              + "s=1969-12-31T23:59:59(LocalDateTime), u=1969-12-31T23:59:59.999999(LocalDateTime)]");
      assertThat(count("SELECT FROM T WHERE k = ?", instant)).isEqualTo(1);
      assertThat(count("SELECT FROM T WHERE n = ?", instant)).isEqualTo(1);
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  @Test
  void minAndMaxOfADateTimeFieldAndAGroupingDateTimeTagReadAsDateTimes() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME_MICROS) FIELDS (v DATETIME_NANOS)");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.k DATETIME_MICROS");
    database.command("sql", "CREATE PROPERTY D.v DATETIME_NANOS");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, k = ?, v = ?", 1_700_000_000_000L + i * 1_000L,
              LocalDateTime.of(2024, 1, 1 + i % 3, 0, 0, 0, 1_000), LocalDateTime.of(2262, 1, 1, 0, 0, 0, i * 7 + 1));
    });
    final String[] queries = { "SELECT min(v) AS mn, max(v) AS mx FROM X", "SELECT k, min(v) AS mn, max(v) AS mx FROM X GROUP BY k ORDER BY k" };
    for (int pass = 0; pass < 2; pass++) {
      for (final String sql : queries) {
        assertThat(plan(sql.replace("FROM X", "FROM T"))).as(sql).contains("AGGREGATE FROM TIMESERIES");
        assertThat(rows(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(rows(sql.replace("FROM X", "FROM D")));
      }
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
    assertThat(rows("SELECT max(v) AS mx FROM T")).containsExactly("[mx=2262-01-01T00:00:00.000000204(LocalDateTime)]");
  }

  @Test
  void anInstantThePrecisionCannotHoldIsRefusedNotSaturated() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (n DATETIME_NANOS)");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.n DATETIME_NANOS");
    // the reporter's row: a nanosecond count, read as millis like every number into a DATETIME, is ~53 million years away
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T SET ts = 1, host = 'a', n = 1700000123456789000"))
        .isInstanceOf(CommandExecutionException.class).hasStackTraceContaining("outside the range");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO D SET n = 1700000123456789000")))
        .hasStackTraceContaining("outside the range");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO D SET n = '2500-01-01 00:00:00'")))
        .hasStackTraceContaining("outside the range");
    assertThat(count("SELECT FROM T")).isZero();
    assertThat(count("SELECT FROM D")).isZero();

    // a FILTER with such an instant is not an error: no row can hold it, so it matches nothing, on a tag and on a field
    database.command("sql", "CREATE TIMESERIES TYPE F TIMESTAMP ts TAGS (k DATETIME_NANOS) FIELDS (n DATETIME_NANOS)");
    database.command("sql", "INSERT INTO F SET ts = 1, k = '2262-04-11 23:47:16', n = '2262-04-11 23:47:16'");
    assertThat(count("SELECT FROM F WHERE k = '2500-01-01 00:00:00'")).isZero();
    assertThat(count("SELECT FROM F WHERE n = '2500-01-01 00:00:00'")).isZero();
    assertThat(count("SELECT FROM F WHERE k = '2262-04-11 23:47:16'")).isEqualTo(1);

    // the limits themselves are representable and stored as they are
    database.command("sql", "INSERT INTO T SET ts = 1, host = 'a', n = '2262-04-11 23:47:16.854775807'");
    assertThat(rows("SELECT n FROM T")).containsExactly("[n=2262-04-11T23:47:16.854775807(LocalDateTime)]");
  }

  @Test
  void theEngineSurfacesKeepTheirEpochCountsAndReadATemporalTagFilter() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (k DATETIME_SECOND) FIELDS (x LONG)");
    database.command("sql", "INSERT INTO T SET ts = 1, k = '2023-11-14 22:15:23', x = 1");
    final ColumnDefinition tag = new ColumnDefinition("k", Type.DATETIME_SECOND, ColumnDefinition.ColumnRole.TAG);
    // a number is a count in the column's unit at the engine API, as the line protocol and the HTTP surfaces speak it
    assertThat(tag.coerceValue(1_700_000_123L)).isEqualTo(1_700_000_123L);
    assertThat(tag.coerceValue("1700000123")).isEqualTo(1_700_000_123L);
    // a date or a datetime in any form is the count it stands for
    assertThat(tag.coerceValue("2023-11-14 22:15:23")).isEqualTo(1_700_000_123L);
    assertThat(tag.coerceValue(LocalDateTime.of(2023, 11, 14, 22, 15, 23))).isEqualTo(1_700_000_123L);
    // only ASCII digits make a count; anything else that is no date is handed back for the filter to match nothing
    assertThat(tag.coerceValue("-1")).isEqualTo(-1L);
    assertThat(tag.coerceValue("\u0661\u0662")).isEqualTo("\u0661\u0662");
    assertThat(tag.coerceValue("-")).isEqualTo("-");
  }

  /** A number is epoch DAYS into a DATE and epoch MILLIS into every DATETIME, as on a document property (issue #9324). */
  private static Object literalFor(final Object literal, final int type) {
    return literal instanceof Long && TYPES[type].equals("DATE") ? (Object) 19_675L : literal;
  }

  @Test
  void theStrictConversionAnswersWhatTheSaturatingOneDoesWhereverItFits() {
    final Object[] values = { new Date(-1_500L), new Date(1_700_000_123_456L), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999),
        LocalDateTime.of(2023, 11, 14, 22, 15, 23, 123_456_789), LocalDate.of(1900, 1, 1),
        Instant.ofEpochSecond(-5, 1), ZonedDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_807, ZoneOffset.UTC),
        1_700_000_123_456L, "1700000123456", "2023-11-14 22:15:23.123456789" };
    for (final ChronoUnit unit : new ChronoUnit[] { ChronoUnit.SECONDS,
        ChronoUnit.MILLIS, ChronoUnit.MICROS, ChronoUnit.NANOS })
      for (final Object value : values)
        assertThat(DateUtils.dateTimeToTimestampExact(database, value, unit)).as(value + " in " + unit)
            .isEqualTo(DateUtils.dateTimeToTimestamp(database, value, unit));
    // past the range: refused rather than the saturated value
    assertThatThrownBy(() -> DateUtils.dateTimeToTimestampExact(database, LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_808),
        ChronoUnit.NANOS)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> DateUtils.dateTimeToTimestampExact(database, LocalDate.of(1600, 1, 1),
        ChronoUnit.NANOS)).isInstanceOf(IllegalArgumentException.class);
  }

  private long count(final String sql, final Object... args) {
    long count = 0;
    try (final ResultSet rs = database.query("sql", sql, args)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final List<String> values = new ArrayList<>();
        for (final String name : r.getPropertyNames().stream().sorted().toList()) {
          final Object value = r.getProperty(name);
          values.add(name + "=" + value + (value == null ? "" : "(" + value.getClass().getSimpleName() + ")"));
        }
        out.add(values.toString());
      }
    }
    return out;
  }
}
