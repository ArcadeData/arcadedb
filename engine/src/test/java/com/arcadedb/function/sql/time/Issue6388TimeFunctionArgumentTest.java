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
package com.arcadedb.function.sql.time;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Several SQL time/date functions threw a raw JDK exception - an HTTP 500 - for ordinary valid input, and
 * {@code sysdate()} silently ignored the argument it documents (issue #6388). Each of these is now either the right
 * answer or a typed argument error, which the HTTP layer maps to 400.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6388TimeFunctionArgumentTest extends TestHelper {

  @Test
  void sysdateAppliesTheZoneGivenAsItsOnlyArgument() throws Exception {
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(ZonedDateTime.class);
    try (final ResultSet rs = database.query("sql",
        "SELECT sysdate('America/New_York') AS ny, sysdate('Europe/Rome') AS rome")) {
      final var row = rs.next();
      final ZonedDateTime ny = row.getProperty("ny");
      final ZonedDateTime rome = row.getProperty("rome");
      // The argument used to be read as params[1] and therefore dropped: both answers were the same server-local
      // LocalDateTime with no zone at all.
      assertThat(ny.getZone().getId()).isEqualTo("America/New_York");
      assertThat(rome.getZone().getId()).isEqualTo("Europe/Rome");
    }
  }

  @Test
  void sysdateRejectsAnUnknownZone() {
    assertThatThrownBy(() -> database.query("sql", "SELECT sysdate('Bogus/Zone') AS d").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bogus/Zone");
  }

  @Test
  void durationSupportsWeeksAsAnExactAmountOfTime() {
    try (final ResultSet rs = database.query("sql", "SELECT duration(2, 'week') AS d")) {
      assertThat(rs.next().<Duration>getProperty("d")).isEqualTo(Duration.ofDays(14));
    }
    try (final ResultSet rs = database.query("sql", "SELECT duration(3, 'days') AS d")) {
      assertThat(rs.next().<Duration>getProperty("d")).isEqualTo(Duration.ofDays(3));
    }
  }

  @Test
  void durationRejectsCalendarUnitsWithAReadableMessage() {
    for (final String unit : new String[] { "year", "month", "years", "months" })
      assertThatThrownBy(() -> database.query("sql", "SELECT duration(1, '" + unit + "') AS d").next())
          .as(unit)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("no fixed length");
  }

  /**
   * The weeks conversion is the one place an amount can overflow, and it must answer the same typed error as every
   * other bad argument in this family rather than a bare ArithmeticException.
   */
  @Test
  void durationRejectsAnAmountThatOverflows() {
    assertThatThrownBy(() -> database.query("sql", "SELECT duration(1000000000000000000, 'week') AS d").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("overflows");
  }

  /**
   * An offset outside the int range must saturate rather than wrap: Number.intValue() on a huge long returns an
   * unrelated - possibly small and positive - number that would sail past the non-negative check.
   */
  @Test
  void lagAndLeadSaturateAnOutOfRangeOffset() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Huge6388");
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO Huge6388 SET v = ?, ts = ?", i, i);
    });

    database.transaction(() -> {
      // 2^32 wraps to 0 under intValue(); saturated it stays past the end of the rows, so every row takes the default.
      try (final ResultSet rs = database.query("sql", "SELECT ts.lag(v, 4294967296, ts) AS r FROM Huge6388")) {
        assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, null, null);
      }
      try (final ResultSet rs = database.query("sql", "SELECT ts.lead(v, 4294967296, ts) AS r FROM Huge6388")) {
        assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, null, null);
      }
    });
  }

  @Test
  void timeBucketRejectsANonPositiveInterval() {
    assertThatThrownBy(() -> database.query("sql", "SELECT ts.timeBucket('0s', 1700000000000) AS b").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
  }

  @Test
  void timeBucketWithANullArgumentIsNullNotAnNpe() {
    try (final ResultSet rs = database.query("sql", "SELECT ts.timeBucket(null, 1700000000000) AS b")) {
      assertThat(rs.next().<Object>getProperty("b")).isNull();
    }
  }

  @Test
  void timeBucketStillTruncatesToTheBucketStart() {
    try (final ResultSet rs = database.query("sql", "SELECT ts.timeBucket('1h', 3900000) AS b")) {
      assertThat(rs.next().<Date>getProperty("b")).isEqualTo(new Date(3_600_000L));
    }
  }

  @Test
  void lagAndLeadRejectANegativeOffset() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Sample6388");
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO Sample6388 SET v = ?, ts = ?", i, i);
    });

    database.transaction(() -> {
      for (final String fn : new String[] { "ts.lag", "ts.lead" })
        assertThatThrownBy(() -> database.query("sql", "SELECT " + fn + "(v, -1, ts) AS r FROM Sample6388").next())
            .as(fn)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("non-negative");
    });
  }

  @Test
  void lagWithoutATimestampKeepsArrivalOrderInsteadOfThrowing() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Arrival6388");
      for (final String v : new String[] { "a", "b", "c" })
        database.command("sql", "INSERT INTO Arrival6388 SET v = ?", v);
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT ts.lag(v) AS r FROM Arrival6388")) {
        assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, "a", "b");
      }
      try (final ResultSet rs = database.query("sql", "SELECT ts.lead(v) AS r FROM Arrival6388")) {
        assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly("b", "c", null);
      }
    });
  }

  @Test
  void lagStillOrdersByTheTimestampWhenGivenOne() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Ordered6388");
      database.command("sql", "INSERT INTO Ordered6388 SET v = 'c', ts = 3");
      database.command("sql", "INSERT INTO Ordered6388 SET v = 'a', ts = 1");
      database.command("sql", "INSERT INTO Ordered6388 SET v = 'b', ts = 2");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT ts.lag(v, 1, ts) AS r FROM Ordered6388")) {
        assertThat(rs.next().<List<Object>>getProperty("r")).containsExactly(null, "a", "b");
      }
    });
  }

  @Test
  void dateRejectsAMalformedPatternInsteadOfLeakingTheJdkException() {
    // An unterminated literal quote: DateTimeFormatterBuilder.appendPattern() rejects it with an
    // IllegalArgumentException, which is NOT a DateTimeParseException and so escaped the catch untouched.
    assertThatThrownBy(() -> database.query("sql", "SELECT date('2020-01-01', \"yyyy-'\") AS d").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid date format");
  }

  @Test
  void dateRejectsAnUnknownZoneInsteadOfLeakingTheJdkException() {
    assertThatThrownBy(() -> database.query("sql", "SELECT date('2020-01-01', 'yyyy-MM-dd', 'Bogus/Zone') AS d").next())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bogus/Zone");
  }

  @Test
  void dateStillAnswersNullWhenTheValueDoesNotMatchTheFormat() {
    try (final ResultSet rs = database.query("sql", "SELECT date('not-a-date', 'yyyy-MM-dd') AS d")) {
      assertThat(rs.next().<Object>getProperty("d")).isNull();
    }
  }

  @Test
  void theFormatterCacheIsBounded() {
    final int before = DateUtils.getCachedFormatterCount();
    // Distinct caller-supplied patterns used to be remembered forever, one entry each.
    for (int i = 0; i < 5_000; i++)
      DateUtils.getFormatter("yyyy-MM-dd'" + ("x".repeat(i % 40 + 1)) + i + "'");

    assertThat(DateUtils.getCachedFormatterCount()).isGreaterThanOrEqualTo(before).isLessThanOrEqualTo(1_000 + 64);
  }
}
