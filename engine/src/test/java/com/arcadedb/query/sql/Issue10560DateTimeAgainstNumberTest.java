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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10560: a number against a datetime is compared as epoch milliseconds, exactly, whichever side the number stands on. The
 * comparison operators converted the operand on the right to the class of the one on the left: a datetime cannot be turned into a
 * number, so a number on the left made {@code 2000 = t} and {@code 2000 <> t} judge every datetime "not equal", a fractional
 * number was read as the millisecond it falls in ({@code t = 2000.5} held for the datetime at 2000), and a number outside the range of
 * a long matched no datetime at all ({@code t <= 1e19}). The time series planner and its residual WHERE depend on it (see
 * {@code Issue10560TimeSeriesBoundNotALongTest}); this is the same rule on a plain document type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10560DateTimeAgainstNumberTest extends TestHelper {
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
