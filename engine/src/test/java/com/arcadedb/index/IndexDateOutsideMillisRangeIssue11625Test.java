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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #11625: a DATE index compared its keys through epoch milliseconds, so a date the millisecond range
 * cannot hold (before year -292275054 or after 292278993, the extremes Neo4j's ExtremeValuesLibrary tests) failed every
 * insert and range lookup on the index with "long overflow" once it was a key, and could not be inserted next to another key.
 */
class IndexDateOutsideMillisRangeIssue11625Test extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE", "UNIQUE" })
  void anIndexedDateOutsideTheMillisRangeIsOrderedAndComparedAsUnindexed(final String kind) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE PROPERTY T.d DATE");
      database.command("sql", "CREATE INDEX ON T (d) " + kind);
    });
    // the extremes first, so they are keys before the ordinary dates arrive, then in the other order
    final List<String> dates = List.of("-999999999-01-01", "+999999999-12-31", "2024-01-01", "-292275055-01-01", "1970-01-01",
        "+292278994-01-01", "-0001-12-31");
    for (final String date : dates)
      database.transaction(() -> database.command("sql", "INSERT INTO T SET d = ?", LocalDate.parse(date)));

    final List<LocalDate> sorted = dates.stream().map(LocalDate::parse).sorted().toList();
    assertThat(dates("SELECT d FROM T ORDER BY d")).containsExactlyElementsOf(sorted);
    assertThat(dates("SELECT d FROM T WHERE d > '1900-01-01' ORDER BY d")).containsExactly(LocalDate.parse("1970-01-01"),
        LocalDate.parse("2024-01-01"), LocalDate.parse("+292278994-01-01"), LocalDate.parse("+999999999-12-31"));
    assertThat(dates("SELECT d FROM T WHERE d < ? ORDER BY d", LocalDate.parse("-292275055-01-01"))).containsExactly(
        LocalDate.parse("-999999999-01-01"));
    assertThat(dates("SELECT d FROM T WHERE d = ?", LocalDate.parse("-999999999-01-01"))).containsExactly(
        LocalDate.parse("-999999999-01-01"));
    assertThat(dates("SELECT d FROM T WHERE d = ?", LocalDate.parse("+292278994-01-01"))).containsExactly(
        LocalDate.parse("+292278994-01-01"));
  }

  /**
   * A DATE and a DATETIME past the millisecond range: both saturated to Long.MAX_VALUE milliseconds, so two different
   * instants compared equal. They compare by the instant itself.
   */
  @Test
  void anExtremeDateAndAnExtremeDateTimeCompareByTheirInstant() {
    final BinaryComparator comparator = new BinaryComparator();
    final LocalDate date = LocalDate.parse("+300000000-01-01");
    final LocalDateTime later = LocalDateTime.parse("+400000000-01-01T00:00");
    final LocalDateTime sameInstant = date.atStartOfDay();
    final LocalDateTime earlyBefore = LocalDateTime.parse("-400000000-01-01T00:00");
    final LocalDate earlyDate = LocalDate.parse("-300000000-01-01");

    assertThat(comparator.compare(date, BinaryTypes.TYPE_DATE, later, BinaryTypes.TYPE_DATETIME)).isNegative();
    assertThat(comparator.compare(later, BinaryTypes.TYPE_DATETIME, date, BinaryTypes.TYPE_DATE)).isPositive();
    assertThat(comparator.compare(date, BinaryTypes.TYPE_DATE, sameInstant, BinaryTypes.TYPE_DATETIME)).isZero();
    assertThat(comparator.compare(date, BinaryTypes.TYPE_DATE, sameInstant.plusNanos(1), BinaryTypes.TYPE_DATETIME_NANOS))
        .isNegative();
    assertThat(comparator.compare(earlyDate, BinaryTypes.TYPE_DATE, earlyBefore, BinaryTypes.TYPE_DATETIME)).isPositive();
    assertThat(comparator.compare(earlyBefore, BinaryTypes.TYPE_DATETIME_MICROS, earlyDate, BinaryTypes.TYPE_DATE)).isNegative();
    assertThat(comparator.compare(LocalDate.MAX, BinaryTypes.TYPE_DATE, LocalDateTime.MAX, BinaryTypes.TYPE_DATETIME)).isNegative();
    assertThat(comparator.compare(LocalDate.MIN, BinaryTypes.TYPE_DATE, LocalDateTime.MIN, BinaryTypes.TYPE_DATETIME)).isZero();
  }

  @Test
  void anExtremeDateComparedToADateTimeOrAStringSaturatesInsteadOfThrowing() {
    final BinaryComparator comparator = new BinaryComparator();
    final LocalDate min = LocalDate.MIN;
    final LocalDate max = LocalDate.MAX;
    final LocalDateTime now = LocalDateTime.of(2024, 1, 1, 0, 0);

    assertThat(comparator.compare(min, BinaryTypes.TYPE_DATE, now, BinaryTypes.TYPE_DATETIME)).isNegative();
    assertThat(comparator.compare(max, BinaryTypes.TYPE_DATE, now, BinaryTypes.TYPE_DATETIME)).isPositive();
    assertThat(comparator.compare(now, BinaryTypes.TYPE_DATETIME, min, BinaryTypes.TYPE_DATE)).isPositive();
    assertThat(comparator.compare(now, BinaryTypes.TYPE_DATETIME, max, BinaryTypes.TYPE_DATE)).isNegative();
    assertThat(comparator.compare(min, BinaryTypes.TYPE_DATE, "2024-01-01", BinaryTypes.TYPE_STRING)).isNegative();
    assertThat(comparator.compare(max, BinaryTypes.TYPE_DATE, "2024-01-01", BinaryTypes.TYPE_STRING)).isPositive();
    assertThat(comparator.compare("2024-01-01", BinaryTypes.TYPE_STRING, max, BinaryTypes.TYPE_DATE)).isNegative();
  }

  /** A DATE converted to another date implementation keeps its instant or fails; it never takes the saturated milliseconds. */
  @Test
  void anExtremeDateConvertedToADateTimeKeepsItsDayAndToADateFails() {
    assertThat(DateUtils.getDate(LocalDate.MAX, LocalDateTime.class)).isEqualTo(LocalDate.MAX.atStartOfDay());
    assertThat(DateUtils.getDate(LocalDate.MIN, LocalDateTime.class)).isEqualTo(LocalDate.MIN.atStartOfDay());
    assertThatThrownBy(() -> DateUtils.getDate(LocalDate.MAX, Date.class)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> DateUtils.getDate(LocalDate.MIN, Calendar.class)).isInstanceOf(ArithmeticException.class);
    assertThat(((Date) DateUtils.getDate(LocalDate.of(2024, 1, 1), Date.class)).getTime()).isEqualTo(1_704_067_200_000L);
  }

  @Test
  void dateToTimestampSaturatesAtEveryPrecision() {
    for (final ChronoUnit unit : new ChronoUnit[] { ChronoUnit.MILLIS, ChronoUnit.MICROS, ChronoUnit.NANOS }) {
      assertThat(DateUtils.dateTimeToTimestamp(LocalDate.MIN, unit)).isEqualTo(Long.MIN_VALUE);
      assertThat(DateUtils.dateTimeToTimestamp(LocalDate.MAX, unit)).isEqualTo(Long.MAX_VALUE);
    }
    assertThat(DateUtils.dateTimeToTimestamp(LocalDate.of(1970, 1, 2), ChronoUnit.SECONDS)).isEqualTo(86_400L);
  }

  /** The untyped compareTo takes the same exact-instant arm as the typed compare(), so it does not saturate past year 2262 either. */
  @Test
  void theUntypedCompareOfAnExtremeDateAndDateTimeIsByTheirInstant() {
    final LocalDate date = LocalDate.parse("+300000000-01-01");
    final LocalDateTime later = LocalDateTime.parse("+400000000-01-01T00:00");

    assertThat(BinaryComparator.compareTo(date, later)).isNegative();
    assertThat(BinaryComparator.compareTo(later, date)).isPositive();
    assertThat(BinaryComparator.compareTo(date, date.atStartOfDay())).isZero();
    assertThat(BinaryComparator.compareTo(date, date.atStartOfDay().plusNanos(1))).isNegative();
    assertThat(BinaryComparator.compareTo(LocalDate.parse("-300000000-01-01"), LocalDateTime.parse("-400000000-01-01T00:00")))
        .isPositive();
    assertThat(BinaryComparator.compareTo(LocalDate.MAX, LocalDateTime.MAX)).isNegative();
  }

  private List<LocalDate> dates(final String query, final Object... args) {
    try (final ResultSet rs = database.query("sql", query, args)) {
      return rs.stream().map(r -> r.<LocalDate>getProperty("d")).toList();
    }
  }
}
