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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10206: SQL date handling against DuckDB - invalid dates were moved to a valid day instead of being refused,
 * a DATE compared equal to a DATETIME of the same day, a time pattern could not format a DATE, a DATE minus a
 * DATETIME_NANOS overflowed, and year 0000 parsed without a pattern but not with {@code yyyy}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10206SqlDateStrictnessTest extends TestHelper {

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
