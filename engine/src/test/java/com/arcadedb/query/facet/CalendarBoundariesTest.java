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
package com.arcadedb.query.facet;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CalendarBoundariesTest {
  private static final int MAX = 100_000;

  private static long ms(final String instant) {
    return Instant.parse(instant).toEpochMilli();
  }

  private static CalendarBoundaries calendar(final TemporalUnit unit, final String zone, final String min, final String max) {
    return CalendarBoundaries.calendar("h", unit, ZoneId.of(zone), null, ms(min), ms(max), MAX);
  }

  /** The length of the bucket whose key starts with the prefix. */
  private static long len(final CalendarBoundaries b, final String keyPrefix) {
    for (int i = 0; i < b.size(); i++)
      if (b.key(i).startsWith(keyPrefix))
        return b.to(i) - b.from(i);
    throw new AssertionError("no bucket starts with " + keyPrefix);
  }

  private static long dayLen(final String zone, final String date) {
    final CalendarBoundaries b = calendar(ChronoUnit.DAYS, zone, date + "T12:00:00Z", date + "T12:00:00Z");
    assertThat(b.size()).isEqualTo(1);
    assertThat(b.key(0)).startsWith(date);
    return b.to(0) - b.from(0);
  }

  @Test
  void romeMonthsAcrossDst2026() {
    final CalendarBoundaries b = calendar(ChronoUnit.MONTHS, "Europe/Rome", "2026-01-15T00:00:00Z", "2026-12-15T00:00:00Z");
    assertThat(b.size()).isEqualTo(12);
    assertThat(len(b, "2026-03")).isEqualTo(31 * 86_400_000L - 3_600_000L);
    assertThat(len(b, "2026-10")).isEqualTo(31 * 86_400_000L + 3_600_000L);
    assertThat(b.from(0)).isEqualTo(ms("2025-12-31T23:00:00Z"));
    assertThat(b.key(0)).isEqualTo("2026-01-01T00:00+01:00");
  }

  @Test
  void newYorkDaysAcrossDst2026() {
    assertThat(dayLen("America/New_York", "2026-03-08")).isEqualTo(82_800_000L);
    assertThat(dayLen("America/New_York", "2026-11-01")).isEqualTo(90_000_000L);
  }

  @Test
  void lordHoweHalfHourShift() {
    assertThat(dayLen("Australia/Lord_Howe", "2026-04-05")).isEqualTo(88_200_000L);
  }

  @Test
  void saoPauloDayWithoutMidnight() {
    final CalendarBoundaries b = calendar(ChronoUnit.DAYS, "America/Sao_Paulo", "2018-11-04T12:00:00Z", "2018-11-04T12:00:00Z");
    assertThat(b.from(0)).isEqualTo(ms("2018-11-04T03:00:00Z"));
    assertThat(b.to(0) - b.from(0)).isEqualTo(82_800_000L);
  }

  @Test
  void kolkataHalfHourOffsetNoDst() {
    final CalendarBoundaries b = calendar(ChronoUnit.DAYS, "Asia/Kolkata", "2026-06-01T12:00:00Z", "2026-06-01T12:00:00Z");
    assertThat(b.from(0)).isEqualTo(ms("2026-05-31T18:30:00Z"));
    assertThat(b.key(0)).isEqualTo("2026-06-01T00:00+05:30");
  }

  @Test
  void hoursOnFallBackNightAreTwentyFive() {
    assertThat(calendar(ChronoUnit.HOURS, "Europe/Rome", "2026-10-24T22:00:00Z", "2026-10-25T22:59:59Z").size()).isEqualTo(25);
  }

  @Test
  void weeksStartMonday() {
    final CalendarBoundaries b = calendar(ChronoUnit.WEEKS, "UTC", "2026-03-04T10:00:00Z", "2026-03-04T10:00:00Z");
    assertThat(b.from(0)).isEqualTo(ms("2026-03-02T00:00:00Z"));
    assertThat(b.to(0)).isEqualTo(ms("2026-03-09T00:00:00Z"));
  }

  @Test
  void quartersAndYears() {
    final CalendarBoundaries q = calendar(IsoFields.QUARTER_YEARS, "UTC", "2026-02-10T00:00:00Z", "2026-02-10T00:00:00Z");
    assertThat(q.from(0)).isEqualTo(ms("2026-01-01T00:00:00Z"));
    assertThat(q.to(0)).isEqualTo(ms("2026-04-01T00:00:00Z"));

    final CalendarBoundaries q3 = calendar(IsoFields.QUARTER_YEARS, "UTC", "2026-08-31T23:59:59Z", "2026-11-01T00:00:00Z");
    assertThat(q3.size()).isEqualTo(2);
    assertThat(q3.from(0)).isEqualTo(ms("2026-07-01T00:00:00Z"));
    assertThat(q3.to(1)).isEqualTo(ms("2027-01-01T00:00:00Z"));

    final CalendarBoundaries y = calendar(ChronoUnit.YEARS, "UTC", "2026-06-15T00:00:00Z", "2026-06-15T00:00:00Z");
    assertThat(y.from(0)).isEqualTo(ms("2026-01-01T00:00:00Z"));
    assertThat(y.to(0)).isEqualTo(ms("2027-01-01T00:00:00Z"));
  }

  @Test
  void offsetShiftsStarts() {
    // the offset is added to the zone-local boundary: DAYS in UTC with +6h start at 06:00Z
    final CalendarBoundaries b = CalendarBoundaries.calendar("h", ChronoUnit.DAYS, ZoneOffset.UTC, Duration.ofHours(6),
        ms("2026-03-04T10:00:00Z"), ms("2026-03-05T10:00:00Z"), MAX);
    assertThat(b.size()).isEqualTo(2);
    assertThat(b.from(0)).isEqualTo(ms("2026-03-04T06:00:00Z"));
    assertThat(b.from(1)).isEqualTo(ms("2026-03-05T06:00:00Z"));

    // a value before the shifted boundary of its own day belongs to the previous bucket
    final CalendarBoundaries early = CalendarBoundaries.calendar("h", ChronoUnit.DAYS, ZoneOffset.UTC, Duration.ofHours(6),
        ms("2026-03-04T03:00:00Z"), ms("2026-03-04T03:00:00Z"), MAX);
    assertThat(early.from(0)).isEqualTo(ms("2026-03-03T06:00:00Z"));
    assertThat(early.to(0)).isEqualTo(ms("2026-03-04T06:00:00Z"));

    // a negative offset larger than a bucket still covers the minimum
    final CalendarBoundaries negative = CalendarBoundaries.calendar("h", ChronoUnit.DAYS, ZoneOffset.UTC, Duration.ofHours(-30),
        ms("2026-03-04T10:00:00Z"), ms("2026-03-04T10:00:00Z"), MAX);
    assertThat(negative.bucketOf(ms("2026-03-04T10:00:00Z"))).isEqualTo(0);
  }

  @Test
  void bucketOfIsHalfOpen() {
    final CalendarBoundaries b = calendar(ChronoUnit.DAYS, "UTC", "2026-03-04T10:00:00Z", "2026-03-06T10:00:00Z");
    assertThat(b.size()).isEqualTo(3);
    assertThat(b.bucketOf(b.from(0))).isEqualTo(0);
    assertThat(b.bucketOf(b.to(0) - 1)).isEqualTo(0);
    assertThat(b.bucketOf(b.to(0))).isEqualTo(1);
    assertThat(b.bucketOf(b.to(2) - 1)).isEqualTo(2);
    assertThat(b.bucketOf(b.to(2))).isEqualTo(-1);
    assertThat(b.bucketOf(b.from(0) - 1)).isEqualTo(-1);
  }

  @Test
  void fixedMatchesTimeBucket() {
    final Random random = new Random(20261019L);
    final SQLFunctionTimeBucket function = new SQLFunctionTimeBucket();
    for (final String interval : new String[] { "1h", "1d", "1w" }) {
      final long intervalMs = SQLFunctionTimeBucket.parseInterval(interval);
      final long offsetMs = SQLFunctionTimeBucket.resolveOffset(Map.of("timezone", "+08:00"), interval, intervalMs);
      for (int i = 0; i < 1000; i++) {
        final long t = ms("2000-01-01T00:00:00Z") + (long) (random.nextDouble() * 1_000_000_000_000L);
        final CalendarBoundaries b = CalendarBoundaries.fixed("h", ZoneOffset.UTC, intervalMs, offsetMs, t, t, MAX);
        final LocalDateTime expected = (LocalDateTime) function.execute(null, null, null,
            new Object[] { interval, Instant.ofEpochMilli(t), Map.of("timezone", "+08:00") }, null);
        assertThat(b.size()).isEqualTo(1);
        assertThat(b.from(0)).isEqualTo(expected.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertThat(b.to(0) - b.from(0)).isEqualTo(intervalMs);
        assertThat(b.bucketOf(t)).isEqualTo(0);
      }
    }
  }

  @Test
  void fixedCoversMinAndMax() {
    final CalendarBoundaries b = CalendarBoundaries.fixed("h", ZoneOffset.UTC, 3_600_000L, 0L, ms("2026-03-04T10:30:00Z"), ms("2026-03-04T12:00:00Z"), MAX);
    assertThat(b.size()).isEqualTo(3);
    assertThat(b.from(0)).isEqualTo(ms("2026-03-04T10:00:00Z"));
    assertThat(b.to(2)).isEqualTo(ms("2026-03-04T13:00:00Z"));
    assertThat(b.key(0)).isEqualTo("2026-03-04T10:00Z");
  }

  @Test
  void fixedKeysUseKeyZone() {
    final ZoneId plus8 = ZoneId.of("+08:00");
    final CalendarBoundaries b = CalendarBoundaries.fixed("h", plus8, 86_400_000L, -8 * 3_600_000L + 86_400_000L, ms("2026-03-04T10:00:00Z"),
        ms("2026-03-04T10:00:00Z"), MAX);
    assertThat(b.key(0)).isEqualTo("2026-03-04T00:00+08:00");
  }

  @Test
  void tooManyBucketsRefused() {
    final String message = "more than 100000 buckets";
    assertThatThrownBy(() -> CalendarBoundaries.calendar("h", ChronoUnit.MINUTES, ZoneOffset.UTC, null, ms("2016-01-01T00:00:00Z"),
        ms("2026-01-01T00:00:00Z"), MAX)).isInstanceOf(CommandExecutionException.class).hasMessageContaining(message)
        .hasMessageContaining("HISTOGRAM h would").hasMessageContaining("use a coarser interval or BOUNDS");
    assertThatThrownBy(() -> CalendarBoundaries.fixed("h", ZoneOffset.UTC, 60_000L, 0L, ms("2016-01-01T00:00:00Z"), ms("2026-01-01T00:00:00Z"), MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining(message);
    // exactly at the cap is accepted
    assertThat(CalendarBoundaries.fixed("h", ZoneOffset.UTC, 1000L, 0L, 0L, 99_999_999L, MAX).size()).isEqualTo(MAX);
  }

  @Test
  void fixedWideRangeIsRefusedByTheCapNotByOverflow() {
    assertThatThrownBy(() -> CalendarBoundaries.fixed("h", ZoneOffset.UTC, 3_600_000L, 0L, -5_000_000_000_000_000_000L,
        5_000_000_000_000_000_000L, MAX)).isInstanceOf(CommandExecutionException.class).hasMessageContaining("HISTOGRAM h would produce more than 100000 buckets");
    assertThatThrownBy(() -> CalendarBoundaries.fixed("h", ZoneOffset.UTC, 1L, 0L, Long.MIN_VALUE, Long.MAX_VALUE, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("more than 100000 buckets");
  }

  @Test
  void fixedAtTheLongLimitsIsRefusedNeverInverted() {
    assertThatThrownBy(() -> CalendarBoundaries.fixed("h", ZoneOffset.UTC, 3_600_000L, 0L, Long.MAX_VALUE - 10, Long.MAX_VALUE, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("representable");
    assertThatThrownBy(() -> CalendarBoundaries.fixed("h", ZoneOffset.UTC, 3_600_000L, 0L, Long.MIN_VALUE, Long.MIN_VALUE + 10, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("representable");
    // a single bucket as wide as the whole range is fine while its end is representable
    final CalendarBoundaries b = CalendarBoundaries.fixed("h", ZoneOffset.UTC, 1L << 61, 0L, -5L, 5L, MAX);
    assertThat(b.size()).isEqualTo(2);
    assertThat(b.to(b.size() - 1)).isGreaterThan(b.from(b.size() - 1));
  }

  @Test
  void calendarAtTheLongLimitsIsRefused() {
    assertThatThrownBy(() -> CalendarBoundaries.calendar("h", ChronoUnit.YEARS, ZoneOffset.UTC, null, Long.MAX_VALUE - 5, Long.MAX_VALUE, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("representable");
    assertThatThrownBy(() -> CalendarBoundaries.calendar("h", ChronoUnit.YEARS, ZoneOffset.UTC, null, Long.MIN_VALUE, Long.MIN_VALUE + 5, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("representable");
    assertThatThrownBy(() -> CalendarBoundaries.calendar("h", ChronoUnit.DAYS, ZoneOffset.UTC, Duration.ofSeconds(Long.MAX_VALUE), 0L, 0L, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("representable");
  }

  @Test
  void minGreaterThanMaxIsRefused() {
    assertThatThrownBy(() -> CalendarBoundaries.fixed("h", ZoneOffset.UTC, 1000L, 0L, 1_000_000_000L, 0L, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("greater than");
    assertThatThrownBy(() -> CalendarBoundaries.calendar("h", ChronoUnit.DAYS, ZoneOffset.UTC, null, 1_000_000_000L, 0L, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("greater than");
  }

  @Test
  void zeroIntervalIsRefused() {
    assertThatThrownBy(() -> CalendarBoundaries.fixed("h", ZoneOffset.UTC, 0L, 0L, 0L, 10L, MAX)).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("must be positive");
  }

  @Test
  void hugeOffsetStillSeeksDirectly() {
    // 50 years of OFFSET on hourly buckets would be ~438000 steps if walked bucket by bucket
    final long min = ms("2026-03-04T10:00:00Z");
    final CalendarBoundaries b = CalendarBoundaries.calendar("h", ChronoUnit.HOURS, ZoneOffset.UTC, Duration.ofDays(365 * 50L), min, min, MAX);
    assertThat(b.size()).isEqualTo(1);
    assertThat(b.from(0)).isLessThanOrEqualTo(min);
    assertThat(b.to(0)).isGreaterThan(min);
    assertThat(b.to(0) - b.from(0)).isEqualTo(3_600_000L);
  }

  @Test
  void preEpochInputs() {
    final CalendarBoundaries month = calendar(ChronoUnit.MONTHS, "UTC", "1968-11-20T00:00:00Z", "1968-11-20T00:00:00Z");
    assertThat(month.from(0)).isEqualTo(ms("1968-11-01T00:00:00Z"));
    assertThat(month.to(0)).isEqualTo(ms("1968-12-01T00:00:00Z"));
    assertThat(month.bucketOf(ms("1968-11-20T00:00:00Z"))).isEqualTo(0);

    final CalendarBoundaries fixed = CalendarBoundaries.fixed("h", ZoneOffset.UTC, 86_400_000L, 0L, -1L, -1L, MAX);
    assertThat(fixed.from(0)).isEqualTo(-86_400_000L);
    assertThat(fixed.to(0)).isEqualTo(0L);
  }

  @Test
  void calendarCapBoundary() {
    // 100000 minutes: min on a boundary, max inside the 100000th bucket
    final long min = ms("2026-01-01T00:00:00Z");
    final CalendarBoundaries ok = CalendarBoundaries.calendar("h", ChronoUnit.MINUTES, ZoneOffset.UTC, null, min, min + 99_999 * 60_000L, MAX);
    assertThat(ok.size()).isEqualTo(MAX);
    assertThatThrownBy(() -> CalendarBoundaries.calendar("h", ChronoUnit.MINUTES, ZoneOffset.UTC, null, min, min + 100_000 * 60_000L, MAX))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("more than 100000 buckets");
  }
}
