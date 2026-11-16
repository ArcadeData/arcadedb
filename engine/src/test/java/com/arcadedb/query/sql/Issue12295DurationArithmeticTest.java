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
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * Regression tests for #12295: a date difference (a {@link Duration}) compared greater than every number and less than none,
 * {@code duration + 1} was the string {@code 'PT24H1'}, {@code duration * 2} was null, {@code datetime + number} was a duration from
 * the epoch and {@code datetime - duration(1, 'day')} was a NullPointerException.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12295DurationArithmeticTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE DD");
    database.command("sql", "CREATE PROPERTY DD.dt DATETIME");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO DD SET k = 1, dt = '2021-02-28 10:00:00'");
      database.command("sql", "INSERT INTO DD SET k = 2, dt = sysdate()");
    });
  }

  @Test
  void aDifferenceComparesWithANumberOfSeconds() {
    assertThat(keys("SELECT k FROM DD WHERE sysdate() - dt < 3600 ORDER BY k")).containsExactly(2);
    assertThat(keys("SELECT k FROM DD WHERE sysdate() - dt > 3600 ORDER BY k")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE sysdate() - dt > 9999999999 ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM DD WHERE 3600 > sysdate() - dt ORDER BY k")).containsExactly(2);
    assertThat(keys("SELECT k FROM DD WHERE sysdate() - dt > -5 ORDER BY k")).containsExactly(1, 2);
    assertThat(keys("SELECT k FROM DD WHERE sysdate() - dt < -5 ORDER BY k")).isEmpty();
  }

  @Test
  void anExactDayEqualsItsSeconds() {
    final String diff = "(dt - '2021-02-27 10:00:00'.asDateTime())";
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " = 86400")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " = 86400.0")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " <> 86400")).isEmpty();
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " = 86400.5")).isEmpty();
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " < 86401")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " <= 86400")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " >= 86400")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " > 99999999999")).isEmpty();
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND 86401 > " + diff)).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " IN [86400]")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " BETWEEN 86400 AND 86400")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND " + diff + " = duration(1, 'day')")).containsExactly(1);
  }

  @Test
  void durationArithmeticWithNumbersAndDurations() {
    final Result row = single("SELECT d + 1 AS plus, d + d AS twice, d / 3600 AS hours, d * 2 AS doubled, d - 1 AS minus, "
        + "d % 7 AS rem, 1 + d AS plusLeft, 2 * d AS doubledLeft, d / duration(1, 'hour') AS ratio, d / duration(7, 'hour') AS ratio7, "
        + "d - 0.5 AS half, d % duration(7, 'hour') AS remDuration "
        + "FROM (SELECT dt - '2021-02-27 10:00:00'.asDateTime() AS d FROM DD WHERE k = 1)");
    assertThat(row.<Duration>getProperty("plus")).isEqualTo(Duration.ofSeconds(86401));
    assertThat(row.<Duration>getProperty("twice")).isEqualTo(Duration.ofSeconds(172800));
    assertThat(row.<Duration>getProperty("hours")).isEqualTo(Duration.ofSeconds(24));
    assertThat(row.<Duration>getProperty("doubled")).isEqualTo(Duration.ofSeconds(172800));
    assertThat(row.<Duration>getProperty("minus")).isEqualTo(Duration.ofSeconds(86399));
    assertThat(row.<Duration>getProperty("rem")).isEqualTo(Duration.ofSeconds(86400 % 7));
    assertThat(row.<Duration>getProperty("plusLeft")).isEqualTo(Duration.ofSeconds(86401));
    assertThat(row.<Duration>getProperty("doubledLeft")).isEqualTo(Duration.ofSeconds(172800));
    assertThat(row.<Long>getProperty("ratio")).isEqualTo(24L);
    assertThat(row.<Double>getProperty("ratio7")).isCloseTo(24.0 / 7, offset(1e-12));
    assertThat(row.<Duration>getProperty("half")).isEqualTo(Duration.ofMillis(86399500));
    assertThat(row.<Duration>getProperty("remDuration")).isEqualTo(Duration.ofHours(3));

    // the arithmetic result keeps comparing with numbers of seconds
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND (dt - '2021-02-27 10:00:00'.asDateTime()) / 3600 = 24")).containsExactly(1);
  }

  @Test
  void aDatetimeMovesByADurationOrANumberOfMilliseconds() {
    final Result row = single("SELECT dt - duration(1, 'day') AS minusDay, duration(1, 'day') + dt AS plusDayLeft, "
        + "dt + duration(90, 'minute') AS plus90, dt + 1000 AS plusMillis, dt - 1000 AS minusMillis, 1000 + dt AS plusMillisLeft "
        + "FROM DD WHERE k = 1");
    final Object dt = single("SELECT dt FROM DD WHERE k = 1").getProperty("dt");
    assertThat(row.<Object>getProperty("minusDay")).isInstanceOf(dt.getClass());
    assertThat(epochMillis(row.getProperty("minusDay"))).isEqualTo(epochMillis(dt) - 86_400_000L);
    assertThat(epochMillis(row.getProperty("plusDayLeft"))).isEqualTo(epochMillis(dt) + 86_400_000L);
    assertThat(epochMillis(row.getProperty("plus90"))).isEqualTo(epochMillis(dt) + 90 * 60_000L);
    assertThat(epochMillis(row.getProperty("plusMillis"))).isEqualTo(epochMillis(dt) + 1000L);
    assertThat(epochMillis(row.getProperty("minusMillis"))).isEqualTo(epochMillis(dt) - 1000L);
    assertThat(epochMillis(row.getProperty("plusMillisLeft"))).isEqualTo(epochMillis(dt) + 1000L);

    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND dt + 0 = dt")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND dt + 1000 > dt")).containsExactly(1);
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND dt + 1000 < dt")).isEmpty();
    assertThat(keys("SELECT k FROM DD WHERE k = 1 AND dt - duration(1, 'day') = '2021-02-27 10:00:00'.asDateTime()")).containsExactly(1);
  }

  @Test
  void javaTimeDatetimesAndDatesKeepTheirClass() {
    final Map<String, Object> params = Map.of("ldt", LocalDateTime.of(2021, 2, 28, 10, 0, 0, 123_456_789), "ld", LocalDate.of(2021, 2, 28));
    try (final ResultSet rs = database.query("sql",
        "SELECT :ldt + duration(1, 'nanosecond') AS a, :ld + duration(2, 'day') AS b, :ld + duration(36, 'hour') AS c, "
            + ":ldt - :ld AS d, :ldt + 0.5 AS e", params)) {
      final Result row = rs.next();
      assertThat(row.<LocalDateTime>getProperty("a")).isEqualTo(LocalDateTime.of(2021, 2, 28, 10, 0, 0, 123_456_790));
      assertThat(row.<LocalDate>getProperty("b")).isEqualTo(LocalDate.of(2021, 3, 2));
      assertThat(row.<LocalDateTime>getProperty("c")).isEqualTo(LocalDateTime.of(2021, 3, 1, 12, 0));
      assertThat(row.<Duration>getProperty("d")).isEqualTo(Duration.ofHours(10).plusNanos(123_456_789));
      assertThat(row.<LocalDateTime>getProperty("e")).isEqualTo(LocalDateTime.of(2021, 2, 28, 10, 0, 0, 123_956_789));
    }
  }

  @Test
  void meaninglessPairsAreRefused() {
    assertThatThrownBy(() -> single("SELECT dt + dt AS v FROM DD WHERE k = 1")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> single("SELECT duration(1, 'day') - dt AS v FROM DD WHERE k = 1"))
        .isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> single("SELECT 1000 - dt AS v FROM DD WHERE k = 1")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> single("SELECT duration(1, 'day') / 0 AS v")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> single("SELECT duration(1, 'day') * duration(1, 'day') AS v")).isInstanceOf(ArithmeticErrorException.class);
  }

  @Test
  void stringsStillConcatenate() {
    final Result row = single("SELECT 'age: ' + duration(1, 'day') AS a, duration(1, 'day') + ' later' AS b");
    assertThat(row.<String>getProperty("a")).isEqualTo("age: PT24H");
    assertThat(row.<String>getProperty("b")).isEqualTo("PT24H later");
  }

  @Test
  void jsonKeepsEveryDigitOfALongDuration() {
    final Result row = single("SELECT '2021-05-15 13:14:15.123456789'.asDateTime('yyyy-MM-dd HH:mm:ss.SSSSSSSSS') - "
        + "'2018-01-01 00:00:00'.asDateTime() AS d");
    final Duration duration = row.getProperty("d");
    assertThat(duration).isEqualTo(Duration.parse("PT29533H14M15.123456789S"));
    final JSONObject json = new JSONObject().put("d", duration);
    assertThat(new BigDecimal(json.toString().replaceAll("[^0-9.]", ""))).isEqualByComparingTo("106319655.123456789");
  }

  private Result single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next();
    }
  }

  private List<Integer> keys(final String sql) {
    final List<Integer> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        result.add(rs.next().<Number>getProperty("k").intValue());
    }
    return result;
  }

  private static long epochMillis(final Object value) {
    if (value instanceof Date date)
      return date.getTime();
    if (value instanceof LocalDateTime ldt)
      return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    throw new IllegalArgumentException("unexpected " + value.getClass());
  }
}
