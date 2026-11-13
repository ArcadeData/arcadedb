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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.function.date.DateFields;
import com.arcadedb.function.date.DateParse;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlTemporalComparisonTest {
  /**
   * Regression for #10206.
   * <p>
   * Issue #10206: SQL date handling against DuckDB - invalid dates were moved to a valid day instead of being refused,
   * a DATE compared equal to a DATETIME of the same day, a time pattern could not format a DATE, a DATE minus a
   * DATETIME_NANOS overflowed, and year 0000 parsed without a pattern but not with {@code yyyy}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlDateStrictness extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE E");
      database.command("sql", "CREATE PROPERTY E.id INTEGER");
      database.command("sql", "CREATE PROPERTY E.d1 DATE");
      database.command("sql", "CREATE PROPERTY E.t1 DATETIME");
      database.command("sql", "CREATE PROPERTY E.t2 DATETIME_MICROS");
      database.command("sql", "CREATE PROPERTY E.t3 DATETIME_NANOS");
      database.command("sql", "CREATE PROPERTY E.s STRING");
      database.transaction(() -> {
        database.newDocument("E").set("id", 1).set("d1", LocalDate.of(2021, 2, 3)).set("t1", LocalDateTime.of(2021, 2, 3, 12, 0))
            .set("t2", LocalDateTime.of(2021, 2, 3, 12, 0)).set("s", "2021-02-03").save();
        database.newDocument("E").set("id", 2).set("d1", LocalDate.of(2021, 2, 28)).set("s", "2021-02-30").save();
        database.newDocument("E").set("id", 3).set("s", "2021-04-31").save();
        database.newDocument("E").set("id", 4).set("s", "2021-02-03 24:00:00").save();
        database.newDocument("E").set("id", 5).set("s", "0000-01-01").save();
        database.newDocument("E").set("id", 6).set("d1", LocalDate.of(2021, 2, 3)).set("t2", LocalDateTime.of(1700, 3, 1, 0, 0))
            .set("t3", LocalDateTime.of(1700, 3, 1, 0, 0, 0, 1)).save();
      });
    }

    @Test
    void aDateComparesAsMidnightAgainstADatetime() {
      assertThat(ids("SELECT id FROM E WHERE d1 >= t1")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE d1 = t1")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE d1 <> t1")).containsExactly(1);
      assertThat(ids("SELECT id FROM E WHERE d1 < t1")).containsExactly(1);
      assertThat(ids("SELECT id FROM E WHERE t1 > d1")).containsExactly(1);
      assertThat(ids("SELECT id FROM E WHERE d1 >= t2")).containsExactly(6);
      assertThat(ids("SELECT id FROM E WHERE d1 BETWEEN t1 AND t1")).isEmpty();
      // The same day at midnight IS equal
      assertThat(ids("SELECT id FROM E WHERE d1 = ?", LocalDateTime.of(2021, 2, 3, 0, 0))).containsExactly(1, 6);
      assertThat(ids("SELECT id FROM E WHERE d1 = ?", LocalDateTime.of(2021, 2, 3, 12, 0))).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE d1 < ?", LocalDateTime.of(2021, 2, 3, 12, 0))).containsExactly(1, 6);
      assertThat(ids("SELECT id FROM E WHERE d1 BETWEEN ? AND ?", LocalDateTime.of(2021, 2, 3, 0, 0),
          LocalDateTime.of(2021, 2, 3, 12, 0))).containsExactly(1, 6);
    }

    @Test
    void aDateComparesAsMidnightAgainstADatetimeThroughAnIndex() {
      database.command("sql", "CREATE INDEX ON E (d1) NOTUNIQUE");
      assertThat(ids("SELECT id FROM E WHERE d1 = ?", LocalDateTime.of(2021, 2, 3, 0, 0))).containsExactly(1, 6);
      assertThat(ids("SELECT id FROM E WHERE d1 = ?", LocalDateTime.of(2021, 2, 3, 12, 0))).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE d1 >= ?", LocalDateTime.of(2021, 2, 3, 12, 0))).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE d1 <= ?", LocalDateTime.of(2021, 2, 3, 12, 0))).containsExactly(1, 6);
      assertThat(ids("SELECT id FROM E WHERE d1 > ?", LocalDateTime.of(2021, 2, 3, 0, 0))).containsExactly(2);
    }

    @Test
    void anInvalidDateIsRefusedRatherThanMovedToAValidDay() {
      assertThat(value("SELECT s.asDate('yyyy-MM-dd') AS v FROM E WHERE id = 2")).isNull();
      assertThat(value("SELECT s.asDate('yyyy-MM-dd') AS v FROM E WHERE id = 3")).isNull();
      assertThat(value("SELECT s.asDate() AS v FROM E WHERE id = 2")).isNull();
      assertThat(value("SELECT s.asDate() AS v FROM E WHERE id = 3")).isNull();
      assertThat(value("SELECT s.asDatetime() AS v FROM E WHERE id = 2")).isNull();
      assertThat(value("SELECT s.asDateTime('yyyy-MM-dd HH:mm:ss') AS v FROM E WHERE id = 4")).isNull();
      assertThat(value("SELECT s.asDateTime() AS v FROM E WHERE id = 4")).isNull();
      assertThat(value("SELECT date(s, 'yyyy-MM-dd') AS v FROM E WHERE id = 2")).isNull();

      // The valid ones still parse
      assertThat(value("SELECT s.asDate('yyyy-MM-dd') AS v FROM E WHERE id = 1")).isEqualTo(LocalDate.of(2021, 2, 3));
      assertThat(value("SELECT s.asDate() AS v FROM E WHERE id = 1")).isEqualTo(LocalDate.of(2021, 2, 3));
      assertThat(value("SELECT '2024-02-29'.asDate('yyyy-MM-dd') AS v")).isEqualTo(LocalDate.of(2024, 2, 29));
      assertThat(value("SELECT '2021-02-03 23:59:59'.asDateTime('yyyy-MM-dd HH:mm:ss') AS v")).isEqualTo(
          LocalDateTime.of(2021, 2, 3, 23, 59, 59));
      assertThat(value("SELECT '03/02/21 11:30 PM'.asDateTime('dd/MM/yy hh:mm a') AS v")).isEqualTo(
          LocalDateTime.of(2021, 2, 3, 23, 30));
    }

    /** A datetime with an offset is the instant it names, read at UTC, with an index on the DATE or without one. */
    @Test
    void aDateComparesAsMidnightAgainstAZonedDatetimeWithOrWithoutAnIndex() {
      // 2021-02-03T23:30-05:00 is 2021-02-04T04:30Z
      final OffsetDateTime zoned = OffsetDateTime.of(2021, 2, 3, 23, 30, 0, 0, ZoneOffset.ofHours(-5));
      for (int pass = 0; pass < 2; pass++) {
        assertThat(ids("SELECT id FROM E WHERE d1 < ?", zoned)).as("pass %d", pass).containsExactly(1, 6);
        assertThat(ids("SELECT id FROM E WHERE d1 = ?", zoned)).as("pass %d", pass).isEmpty();
        assertThat(ids("SELECT id FROM E WHERE d1 >= ?", zoned)).as("pass %d", pass).containsExactly(2);
        assertThat(ids("SELECT id FROM E WHERE d1 <= ?", zoned)).as("pass %d", pass).containsExactly(1, 6);
        assertThat(ids("SELECT id FROM E WHERE d1 <= ?", LocalDateTime.of(2021, 2, 4, 0, 0))).as("pass %d", pass)
            .containsExactly(1, 6);
        if (pass == 0)
          database.command("sql", "CREATE INDEX ON E (d1) NOTUNIQUE");
      }
    }

    @Test
    void patternsWithLiteralsQuotesAndWeekdaysAreReadStrictly() {
      assertThat(value("SELECT '2021-02-03T10:15'.asDateTime(\"yyyy-MM-dd'T'HH:mm\") AS v")).isEqualTo(
          LocalDateTime.of(2021, 2, 3, 10, 15));
      assertThat(value("SELECT ?.asDate(?) AS v", "3 o'clock 2021-02-03", "d 'o''clock' yyyy-MM-dd")).isEqualTo(LocalDate.of(2021, 2, 3));
      assertThat(value("SELECT 'Wed, 03 Feb 2021 10:00'.asDateTime('EEE, dd MMM yyyy HH:mm') AS v")).isEqualTo(
          LocalDateTime.of(2021, 2, 3, 10, 0));
      // the day of the week has to agree with the date
      assertThat(value("SELECT 'Thu, 03 Feb 2021 10:00'.asDateTime('EEE, dd MMM yyyy HH:mm') AS v")).isNull();
    }

    @Test
    void anInvalidDateLiteralMatchesNoDate() {
      assertThat(ids("SELECT id FROM E WHERE d1 = '2021-02-30'")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE d1 = '2021-02-28'")).containsExactly(2);
    }

    @Test
    void anInvalidDateIsRefusedOnWrite() {
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("E").set("id", 7).set("d1", "2021-02-30").save()))//
          .hasStackTraceContaining("2021-02-30");
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO E SET id = 8, t1 = '2021-02-03 24:00:00'"))//
          .hasStackTraceContaining("24:00:00");
      assertThat(ids("SELECT id FROM E WHERE id >= 7")).isEmpty();

      database.transaction(() -> database.newDocument("E").set("id", 9).set("d1", "2024-02-29").save());
      assertThat(ids("SELECT id FROM E WHERE d1 = '2024-02-29'")).containsExactly(9);
    }

    @Test
    void yearZeroParsesWithAndWithoutAPattern() {
      assertThat(value("SELECT s.asDate() AS v FROM E WHERE id = 5")).isEqualTo(LocalDate.of(0, 1, 1));
      assertThat(value("SELECT s.asDate('yyyy-MM-dd') AS v FROM E WHERE id = 5")).isEqualTo(LocalDate.of(0, 1, 1));
      // ...and formats back to the string it was read from
      assertThat(value("SELECT s.asDate('yyyy-MM-dd').format('yyyy-MM-dd') AS v FROM E WHERE id = 5")).isEqualTo("0000-01-01");
    }

    @Test
    void aTimePatternFormatsADateAsMidnight() {
      assertThat(value("SELECT d1.format('HH') AS v FROM E WHERE id = 1")).isEqualTo("00");
      assertThat(value("SELECT d1.format('yyyy-MM-dd HH:mm:ss') AS v FROM E WHERE id = 1")).isEqualTo("2021-02-03 00:00:00");
      assertThat(value("SELECT d1.format('yyyy-MM-dd') AS v FROM E WHERE id = 1")).isEqualTo("2021-02-03");
    }

    @Test
    void aDateMinusADatetimeNanosDoesNotOverflow() {
      assertThat(value("SELECT (d1 - t2) AS v FROM E WHERE id = 6")).isEqualTo(Duration.ofHours(2813208));
      assertThat(value("SELECT (d1 - t3) AS v FROM E WHERE id = 6")).isEqualTo(Duration.ofHours(2813208).minusNanos(1));
    }

    private List<Integer> ids(final String sql, final Object... args) {
      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, args)) {
        while (rs.hasNext())
          ids.add(rs.next().<Integer>getProperty("id"));
      }
      ids.sort(null);
      return ids;
    }

    private Object value(final String sql, final Object... args) {
      try (final ResultSet rs = database.query("sql", sql, args)) {
        final Result r = rs.next();
        return r.getProperty("v");
      }
    }
  }

  /**
   * Regression for #10560.
   * <p>
   * Issue #10560: a number against a datetime is compared as epoch milliseconds, exactly, whichever side the number stands on. The
   * comparison operators converted the operand on the right to the class of the one on the left: a datetime cannot be turned into a
   * number, so a number on the left made {@code 2000 = t} and {@code 2000 <> t} judge every datetime "not equal", a fractional
   * number was read as the millisecond it falls in ({@code t = 2000.5} held for the datetime at 2000), and a number outside the range of
   * a long matched no datetime at all ({@code t <= 1e19}). The time series planner and its residual WHERE depend on it (see
   * {@code TimeSeriesTemporalColumnsTest}); this is the same rule on a plain document type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DateTimeAgainstNumber extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE E");
      database.command("sql", "CREATE PROPERTY E.id INTEGER");
      database.command("sql", "CREATE PROPERTY E.t1 DATETIME");
      database.command("sql", "CREATE PROPERTY E.t3 DATETIME_NANOS");
      // the same instants as t1, in a property with an index: the index has to answer what the scan of t1 does
      database.command("sql", "CREATE PROPERTY E.t4 DATETIME");
      database.command("sql", "CREATE INDEX ON E (t4) NOTUNIQUE");
      // the other precisions count seconds and microseconds, not milliseconds: the same, with and without an index
      database.command("sql", "CREATE PROPERTY E.s1 DATETIME_SECOND");
      database.command("sql", "CREATE PROPERTY E.s2 DATETIME_SECOND");
      database.command("sql", "CREATE INDEX ON E (s2) NOTUNIQUE");
      database.command("sql", "CREATE PROPERTY E.u1 DATETIME_MICROS");
      database.command("sql", "CREATE PROPERTY E.u2 DATETIME_MICROS");
      database.command("sql", "CREATE INDEX ON E (u2) NOTUNIQUE");
      database.transaction(() -> {
        // 1000 ms, 2000 ms and 3000 ms; the nanosecond column holds 1000 ms, 2000.5 ms (500000 ns below the millisecond is half of it)
        final LocalDateTime one = LocalDateTime.of(1970, 1, 1, 0, 0, 1);
        final LocalDateTime two = LocalDateTime.of(1970, 1, 1, 0, 0, 2);
        final LocalDateTime three = LocalDateTime.of(1970, 1, 1, 0, 0, 3);
        final LocalDateTime twoAndAHalfMillisecond = LocalDateTime.of(1970, 1, 1, 0, 0, 2, 500_000);
        database.newDocument("E").set("id", 1).set("t1", one).set("t4", one).set("t3", one).set("s1", one).set("s2", one).set("u1", one)
            .set("u2", one).save();
        database.newDocument("E").set("id", 2).set("t1", two).set("t4", two).set("t3", twoAndAHalfMillisecond).set("s1", two).set("s2", two)
            .set("u1", twoAndAHalfMillisecond).set("u2", twoAndAHalfMillisecond).save();
        database.newDocument("E").set("id", 3).set("t1", three).set("t4", three).set("s1", three).set("s2", three).set("u1", three)
            .set("u2", three).save();
      });
    }

    /**
     * The other datetime precisions count seconds, microseconds or nanoseconds, and an index converts a number of milliseconds that
     * falls between two of its keys to the key it falls in. A bound like that stays on the scan, so the indexed property answers what
     * the one without an index does.
     */
    @Test
    void anIndexOnAnotherPrecisionAnswersLikeTheScan() {
      final String[] operators = { "=", "<", "<=", ">", ">=" };
      final String[] bounds = { "2000", "2000.5", "2500", "1999.9", "2999.5", "1e19", "-1e19", "3000", "2000.0", "0.5", "1500" };
      for (final String[] pair : new String[][] { { "s2", "s1" }, { "u2", "u1" } })
        for (final String operator : operators)
          for (final String bound : bounds)
            assertThat(ids("SELECT id FROM E WHERE " + pair[0] + " " + operator + " " + bound)).as(pair[0] + " " + operator + " " + bound)
                .isEqualTo(ids("SELECT id FROM E WHERE " + pair[1] + " " + operator + " " + bound));
      // the microsecond column holds 2000.5 ms in the second row
      assertThat(ids("SELECT id FROM E WHERE u2 = 2000.5")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE u2 > 2000")).containsExactly(2, 3);
    }

    /**
     * An index on a DATETIME converted a numeric bound to the datetime it falls in, as the scan did before: {@code t >= 2000.5} read as
     * {@code t >= 2000} through the index and {@code t <= 1e19} found nothing. Now the scan is exact, and the index maps the bound onto
     * the keys that bound the same set (the smallest key above a lower bound, the largest below an upper one), as it does for an
     * integral key. The rows of {@code t4}, which has an index, are those of {@code t1}, which has not.
     */
    @Test
    void anIndexOnTheDatetimeAnswersLikeTheScan() {
      final String[] operators = { "=", "<", "<=", ">", ">=" };
      final Object[] bounds = { 2000L, 2000.5d, 1999.5d, 2999.5d, 999.5d, 3000.5d, -0.5d, 1e19d, -1e19d, 9.3e18d, Double.NaN, Float.NaN,
          Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, new BigDecimal("2000.5"), new BigDecimal("9223372036854775808"),
          new BigDecimal("-9223372036854775809"), new BigDecimal("9223372036854775807.5"), new BigInteger("9223372036854775808"),
          new BigDecimal("1E+999999999"), new BigDecimal("-1E+999999999"), new BigDecimal("1E-999999999"), new BigDecimal("-1E-999999999") };
      for (final String operator : operators)
        for (final Object bound : bounds) {
          final String name = "t " + operator + " " + bound.getClass().getSimpleName() + " " + bound;
          assertThat(ids("SELECT id FROM E WHERE t4 " + operator + " ?", bound)).as(name).isEqualTo(ids("SELECT id FROM E WHERE t1 " + operator + " ?", bound));
        }

      for (final Object[] range : new Object[][] { { 1500.5d, 2500.5d }, { 2000.5d, 2999.5d }, { 2000L, 3000L }, { -1e19d, 1e19d }, { 2000.5d, 1e19d },
          { 2000.2d, 2000.7d }, { new BigDecimal("1E+999999999"), new BigDecimal("1E+999999999") }, { Double.NaN, Double.NaN } })
        assertThat(ids("SELECT id FROM E WHERE t4 BETWEEN ? AND ?", range)).as(range[0] + ".." + range[1])
            .isEqualTo(ids("SELECT id FROM E WHERE t1 BETWEEN ? AND ?", range));

      // the rows of the report
      assertThat(ids("SELECT id FROM E WHERE t4 >= 2000.5")).containsExactly(3);
      assertThat(ids("SELECT id FROM E WHERE t4 = 2000.5")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t4 < 2000.5")).containsExactly(1, 2);
      assertThat(ids("SELECT id FROM E WHERE t4 <= 1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t4 > -1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t4 >= 1e19")).isEmpty();
    }

    @Test
    void aWholeNumberOnEitherSide() {
      assertThat(ids("SELECT id FROM E WHERE t1 = 2000")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE 2000 = t1")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE t1 <> 2000")).containsExactly(1, 3);
      assertThat(ids("SELECT id FROM E WHERE 2000 <> t1")).containsExactly(1, 3);
      assertThat(ids("SELECT id FROM E WHERE 2000 > t1")).containsExactly(1);
      assertThat(ids("SELECT id FROM E WHERE 2000 >= t1")).containsExactly(1, 2);
      assertThat(ids("SELECT id FROM E WHERE 2000 < t1")).containsExactly(3);
      assertThat(ids("SELECT id FROM E WHERE 2000 <= t1")).containsExactly(2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 > 2000")).containsExactly(3);
      assertThat(ids("SELECT id FROM E WHERE t1 <= 2000")).containsExactly(1, 2);
    }

    @Test
    void aFractionalNumberIsNotTheMillisecondItFallsIn() {
      assertThat(ids("SELECT id FROM E WHERE t1 = 2000.5")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE 2000.5 = t1")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 <> 2000.5")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE 2000.5 <> t1")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 < 2000.5")).containsExactly(1, 2);
      assertThat(ids("SELECT id FROM E WHERE 2000.5 > t1")).containsExactly(1, 2);
      assertThat(ids("SELECT id FROM E WHERE t1 <= 2000.5")).containsExactly(1, 2);
      assertThat(ids("SELECT id FROM E WHERE t1 > 2000.5")).containsExactly(3);
      assertThat(ids("SELECT id FROM E WHERE t1 >= 2000.5")).containsExactly(3);
      assertThat(ids("SELECT id FROM E WHERE 2000.5 < t1")).containsExactly(3);
      assertThat(ids("SELECT id FROM E WHERE 1999.5 <= t1")).containsExactly(2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 BETWEEN 1500.5 AND 2500.5")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE t1 BETWEEN 2000.5 AND 2999.5")).isEmpty();
    }

    @Test
    void aNumberOutsideTheRangeOfALongIsBelowOrAboveEveryDatetime() {
      assertThat(ids("SELECT id FROM E WHERE t1 < 1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 <= 1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 > -1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 >= -1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 > 1e19")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 < -1e19")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 = 1e19")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 <> 1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE 1e19 > t1")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE -1e19 < t1")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE 1e19 < t1")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 BETWEEN -1e19 AND 1e19")).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 BETWEEN 2000.5 AND 1e19")).containsExactly(3);
    }

    @Test
    void theMillisecondsBelowTheOnesAreNotDropped() {
      // the nanosecond column of id 2 holds 2000.5 ms
      assertThat(ids("SELECT id FROM E WHERE t3 = 2000.5")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE 2000.5 = t3")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE t3 = 2000")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t3 > 2000")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE t3 >= 2000.5")).containsExactly(2);
      assertThat(ids("SELECT id FROM E WHERE t3 > 2000.5")).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t3 < 2000.5")).containsExactly(1);
      assertThat(ids("SELECT id FROM E WHERE t3 <= 2000")).containsExactly(1);
      assertThat(ids("SELECT id FROM E WHERE 2000 < t3")).containsExactly(2);
    }

    @Test
    void everyNumberClassReadsTheSame() {
      final Object[] two = { 2000L, 2000, (short) 2000, 2000.0d, 2000.0f, new BigDecimal("2000"), new BigDecimal("2000.00"),
          BigInteger.valueOf(2000) };
      for (final Object value : two) {
        assertThat(ids("SELECT id FROM E WHERE t1 = ?", value)).as(value.getClass().getSimpleName()).containsExactly(2);
        assertThat(ids("SELECT id FROM E WHERE ? = t1", value)).as(value.getClass().getSimpleName()).containsExactly(2);
        assertThat(ids("SELECT id FROM E WHERE ? <> t1", value)).as(value.getClass().getSimpleName()).containsExactly(1, 3);
        assertThat(ids("SELECT id FROM E WHERE t1 < ?", value)).as(value.getClass().getSimpleName()).containsExactly(1);
        assertThat(ids("SELECT id FROM E WHERE ? <= t1", value)).as(value.getClass().getSimpleName()).containsExactly(2, 3);
      }

      final Object[] beyondTheLong = { Double.MAX_VALUE, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, new BigDecimal("9223372036854775808"),
          new BigInteger("9223372036854775808"), new BigDecimal("9223372036854775807.5") };
      for (final Object value : beyondTheLong) {
        assertThat(ids("SELECT id FROM E WHERE t1 < ?", value)).as(value.toString()).containsExactly(1, 2, 3);
        assertThat(ids("SELECT id FROM E WHERE t1 <= ?", value)).as(value.toString()).containsExactly(1, 2, 3);
        assertThat(ids("SELECT id FROM E WHERE ? > t1", value)).as(value.toString()).containsExactly(1, 2, 3);
        assertThat(ids("SELECT id FROM E WHERE t1 > ?", value)).as(value.toString()).isEmpty();
        assertThat(ids("SELECT id FROM E WHERE t1 = ?", value)).as(value.toString()).isEmpty();
      }

      final Object[] beforeTheLong = { -Double.MAX_VALUE, Float.NEGATIVE_INFINITY, new BigDecimal("-9223372036854775809"),
          new BigInteger("-9223372036854775809") };
      for (final Object value : beforeTheLong) {
        assertThat(ids("SELECT id FROM E WHERE t1 > ?", value)).as(value.toString()).containsExactly(1, 2, 3);
        assertThat(ids("SELECT id FROM E WHERE t1 >= ?", value)).as(value.toString()).containsExactly(1, 2, 3);
        assertThat(ids("SELECT id FROM E WHERE ? < t1", value)).as(value.toString()).containsExactly(1, 2, 3);
        assertThat(ids("SELECT id FROM E WHERE t1 < ?", value)).as(value.toString()).isEmpty();
        assertThat(ids("SELECT id FROM E WHERE t1 <> ?", value)).as(value.toString()).containsExactly(1, 2, 3);
      }
    }

    @Test
    void aNaNIsAboveEveryDatetimeAsItIsAboveEveryNumber() {
      // the order of Double.compare, the one a plain long column is compared in
      assertThat(ids("SELECT id FROM E WHERE t1 < ?", Double.NaN)).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 <= ?", Float.NaN)).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE ? > t1", Double.NaN)).containsExactly(1, 2, 3);
      assertThat(ids("SELECT id FROM E WHERE t1 > ?", Double.NaN)).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 >= ?", Double.NaN)).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 = ?", Double.NaN)).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE ? = t1", Double.NaN)).isEmpty();
      assertThat(ids("SELECT id FROM E WHERE t1 <> ?", Double.NaN)).containsExactly(1, 2, 3);
    }

    private List<Integer> ids(final String sql, final Object... parameters) {
      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet resultSet = database.query("sql", sql, parameters)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          ids.add(row.<Integer>getProperty("id"));
        }
      }
      ids.sort(Integer::compare);
      return ids;
    }
  }

  /**
   * Regression for #10596.
   * <p>
   * Issue #10596: {@code asDateTime(format)} and {@code date(value, format)} kept the wall clock of an offset-bearing string, so the
   * same instant with a {@code +02:00} offset was stored as 03:04:05 from them and as 01:04:05 from the schema's own pattern, a bound
   * parameter or an {@code OffsetDateTime} (issues #10489, #10498). The rule is one: the instant a text names is stored as the UTC wall
   * clock. A caller that named a pattern did not reach the chain that applies it - {@code DateUtils.parse(text, format)} was a
   * one-line {@code LocalDateTime.parse}, which takes the LOCAL fields of the parse and discards the offset it just read - and the
   * {@code timezone} argument of {@code date(value, format, timezone)} was inert there for the same reason.
   * <p>
   * {@code asDate(format)} keeps the wall clock: its target has no zone to keep, and the day written is the day meant. The census
   * {@code DatetimeParseWithAPatternCensusTest} (census module) makes a text-to-datetime parse with a pattern unreachable outside
   * {@code DateUtils}, so that every conversion has to answer what becomes of the offset.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExplicitFormatOffset extends TestHelper {

    private static final String         ISO_PATTERN    = "yyyy-MM-dd'T'HH:mm:ssXXX";
    private static final String         SCHEMA_PATTERN = "yyyy/MM/dd HH:mm:ss XXX";
    private static final LocalDateTime  UTC_WALL_CLOCK = LocalDateTime.of(2020, 1, 2, 1, 4, 5);
    private static final List<ZoneId>   DATABASE_ZONES = List.of(ZoneId.of("UTC"), ZoneId.of("America/New_York"));

    private Object scalar(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().getProperty("r");
      }
    }

    @Test
    void anExplicitPatternStoresTheInstantTheTextNames() {
      for (final ZoneId zone : DATABASE_ZONES) {
        database.getSchema().setZoneId(zone);

        assertThat(scalar("select '2020-01-02T03:04:05+02:00'.asDateTime() as r")).as("no format, zone " + zone).isEqualTo(UTC_WALL_CLOCK);
        assertThat(scalar("select '2020-01-02T03:04:05+02:00'.asDateTime(\"" + ISO_PATTERN + "\") as r")).as("asDateTime(format), zone " + zone)
            .isEqualTo(UTC_WALL_CLOCK);
        assertThat(scalar("select date('2020-01-02T03:04:05+02:00') as r")).as("date(value), zone " + zone).isEqualTo(UTC_WALL_CLOCK);
        assertThat(scalar("select date('2020-01-02T03:04:05+02:00', \"" + ISO_PATTERN + "\") as r")).as("date(value, format), zone " + zone)
            .isEqualTo(UTC_WALL_CLOCK);
      }
    }

    @Test
    void theSamePatternGivesTheSameInstantWhetherTheSchemaAppliesItOrTheCallerNamesIt() {
      database.command("sql", "alter database dateTimeFormat \"" + SCHEMA_PATTERN + "\"").close();
      for (final ZoneId zone : DATABASE_ZONES) {
        database.getSchema().setZoneId(zone);
        final String text = "2020/01/02 03:04:05 +02:00";

        assertThat(scalar("select '" + text + "'.asDateTime() as r")).as("the schema's pattern, zone " + zone).isEqualTo(UTC_WALL_CLOCK);
        assertThat(scalar("select '" + text + "'.asDateTime('" + SCHEMA_PATTERN + "') as r")).as("the same pattern by name, zone " + zone)
            .isEqualTo(UTC_WALL_CLOCK);
        assertThat(scalar("select date('" + text + "') as r")).isEqualTo(UTC_WALL_CLOCK);
        assertThat(scalar("select date('" + text + "', '" + SCHEMA_PATTERN + "') as r")).isEqualTo(UTC_WALL_CLOCK);
      }
    }

    @Test
    void aWriteStoresTheSameValueWhetherThePatternIsNamedOrNot() {
      database.command("sql", "alter database dateTimeFormat \"" + SCHEMA_PATTERN + "\"").close();
      database.command("sql", "CREATE DOCUMENT TYPE T").close();
      database.command("sql", "CREATE PROPERTY T.ts DATETIME").close();
      database.transaction(() -> {
        database.command("sql", "insert into T set k = 'plain', ts = '2020/01/02 03:04:05 +02:00'").close();
        database.command("sql", "insert into T set k = 'named', ts = '2020/01/02 03:04:05 +02:00'.asDateTime('" + SCHEMA_PATTERN + "')").close();
      });

      for (final String key : List.of("plain", "named"))
        assertThat(scalar("select ts as r from T where k = '" + key + "'")).as(key).isEqualTo(UTC_WALL_CLOCK);
    }

    @Test
    void theTimezoneArgumentDoesNotChangeTheInstantATextNames() {
      final String text = "2020-01-02T03:04:05+02:00";
      for (final String zone : List.of("UTC", "Asia/Tokyo", "America/New_York"))
        assertThat(scalar("select date('" + text + "', \"" + ISO_PATTERN + "\", '" + zone + "') as r")).as(zone).isEqualTo(UTC_WALL_CLOCK);
      assertThat(scalar("select date('" + text + "', { format: \"" + ISO_PATTERN + "\", timezone: 'Asia/Tokyo' }) as r")).isEqualTo(UTC_WALL_CLOCK);
    }

    @Test
    void theTimezoneArgumentIsTheZoneATextThatNamesNoneIsReadIn() {
      final String pattern = "yyyy-MM-dd HH:mm:ss";
      // no zone named: the wall clock is kept, as the chain without a pattern keeps it - the database's zone plays no part in it
      for (final ZoneId zone : DATABASE_ZONES) {
        database.getSchema().setZoneId(zone);
        assertThat(scalar("select date('2020-01-02 03:04:05', '" + pattern + "') as r")).as("zone " + zone)
            .isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
      }
      // the zone named is where the text was written: 03:04:05 in Tokyo (+09:00) is 18:04:05 of the day before in UTC
      assertThat(scalar("select date('2020-01-02 03:04:05', '" + pattern + "', 'Asia/Tokyo') as r")).isEqualTo(LocalDateTime.of(2020, 1, 1, 18, 4, 5));
      assertThat(scalar("select date('2020-01-02 03:04:05', { format: '" + pattern + "', timezone: 'Asia/Tokyo' }) as r"))
          .isEqualTo(LocalDateTime.of(2020, 1, 1, 18, 4, 5));
      assertThat(scalar("select date('2020-01-02 03:04:05', '" + pattern + "', 'UTC') as r")).isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
    }

    @Test
    void aTextWithoutAnOffsetKeepsItsWallClock() {
      assertThat(scalar("select '2020-01-02 03:04:05'.asDateTime('yyyy-MM-dd HH:mm:ss') as r")).isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
      assertThat(scalar("select '2020-01-02'.asDateTime('yyyy-MM-dd') as r")).isEqualTo(LocalDateTime.of(2020, 1, 2, 0, 0, 0));
      assertThat(scalar("select '2020-01-02T03:04:05Z'.asDateTime(\"yyyy-MM-dd'T'HH:mm:ssX\") as r")).as("Z is UTC: the same wall clock")
          .isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
    }

    @Test
    void aPatternWithAZoneRegionConvertsToo() {
      assertThat(scalar("select '2020-01-02 03:04:05 Europe/Rome'.asDateTime('yyyy-MM-dd HH:mm:ss VV') as r")).isEqualTo(
          LocalDateTime.of(2020, 1, 2, 2, 4, 5));
      assertThat(scalar("select '2020-07-02 03:04:05 Europe/Rome'.asDateTime('yyyy-MM-dd HH:mm:ss VV') as r")).as("summer time, +02:00")
          .isEqualTo(LocalDateTime.of(2020, 7, 2, 1, 4, 5));
    }

    @Test
    void aTextThatDoesNotMatchThePatternAnswersNull() {
      assertThat(scalar("select 'not a date'.asDateTime(\"" + ISO_PATTERN + "\") as r")).isNull();
      assertThat(scalar("select date('not a date', \"" + ISO_PATTERN + "\") as r")).isNull();
      // a pattern with no date at all cannot make a datetime: the same refusal as before, not an exception
      assertThat(scalar("select '12'.asDateTime('MM') as r")).isNull();
    }

    @Test
    void asDateKeepsTheWallClockSoTheDayWrittenIsTheDayMeant() {
      // 2020-01-02T01:04:05+02:00 is 2020-01-01T23:04:05Z: converting to UTC would answer the day before the one the text names
      final Object day = scalar("select '2020-01-02T01:04:05+02:00'.asDate(\"" + ISO_PATTERN + "\") as r");
      assertThat(day).isEqualTo(LocalDate.of(2020, 1, 2));
      assertThat(scalar("select '2020-01-02T01:04:05+02:00'.asDate() as r")).as("the same without a pattern").isEqualTo(day);
    }

    @Test
    void theUtilityNamesWhatBecomesOfTheOffset() {
      final String text = "2020-01-02T03:04:05+02:00";
      assertThat(DateUtils.parseDateTimeWithFormat(text, ISO_PATTERN, null)).isEqualTo(UTC_WALL_CLOCK);
      assertThat(DateUtils.parseDateTimeKeepingWallClockWithFormat(text, ISO_PATTERN)).isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
      assertThat(DateUtils.parseDateTimeWithFormat("2020-01-02 03:04:05", "yyyy-MM-dd HH:mm:ss", ZoneId.of("Asia/Tokyo")))
          .isEqualTo(LocalDateTime.of(2020, 1, 1, 18, 4, 5));
      assertThatThrownBy(() -> DateUtils.parseDateTimeWithFormat("nope", ISO_PATTERN, null)).isInstanceOf(DateTimeParseException.class);
    }

    /** The function that answers an epoch for a text read with a pattern read the wall clock in the JVM's zone, and dropped the offset. */
    @Test
    void dateParseAnswersTheInstantTheTextNames() {
      final TimeZone previous = TimeZone.getDefault();
      try {
        for (final String jvmZone : List.of("UTC", "Asia/Tokyo", "America/New_York")) {
          TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
          final Object epoch = new DateParse().execute(new Object[] { "2020-01-02T03:04:05+02:00", "ms", ISO_PATTERN }, null);
          assertThat(epoch).as("JVM zone " + jvmZone).isEqualTo(Instant.parse("2020-01-02T01:04:05Z").toEpochMilli());
        }
        // a text without an offset is read in the JVM's zone, as it always was
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
        assertThat(new DateParse().execute(new Object[] { "2020-01-02 03:04:05", "ms", "yyyy-MM-dd HH:mm:ss" }, null)).isEqualTo(
            Instant.parse("2020-01-01T18:04:05Z").toEpochMilli());
      } finally {
        TimeZone.setDefault(previous);
      }
    }

    @Test
    @SuppressWarnings("unchecked")
    void dateFieldsAreTheFieldsOfTheInstantTheTextNames() {
      final Map<String, Object> ownOffset = (Map<String, Object>) new DateFields().execute(
          new Object[] { "2020-01-02T03:04:05+02:00", ISO_PATTERN }, null);
      assertThat(ownOffset.get("hour")).as("the text's own offset when no zone is asked for").isEqualTo(3L);
      assertThat(ownOffset.get("timezone")).isEqualTo("+02:00");

      final Map<String, Object> inUtc = (Map<String, Object>) new DateFields().execute(
          new Object[] { "2020-01-02T03:04:05+02:00", ISO_PATTERN, "UTC" }, null);
      assertThat(inUtc.get("hour")).as("the same instant expressed in the zone asked for").isEqualTo(1L);
      assertThat(inUtc.get("timezone")).isEqualTo("UTC");

      final Map<String, Object> zoneLess = (Map<String, Object>) new DateFields().execute(
          new Object[] { "2020-01-02 03:04:05", "yyyy-MM-dd HH:mm:ss", "Asia/Tokyo" }, null);
      assertThat(zoneLess.get("hour")).as("a text with no zone is read in the zone asked for").isEqualTo(3L);
      assertThat(zoneLess.get("timezone")).isEqualTo("Asia/Tokyo");
    }
  }
}
