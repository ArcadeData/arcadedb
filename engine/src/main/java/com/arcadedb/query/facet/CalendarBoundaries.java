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

import com.arcadedb.engine.timeseries.TimeBucketGrid;
import com.arcadedb.exception.CommandExecutionException;

import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;

/**
 * The bucket boundaries of a date histogram, built once per query as a sorted {@code long[]} of epoch-millisecond starts plus the
 * end of the last bucket in a trailing slot. A value is located with {@link Arrays#binarySearch}: DST transitions inside a month or
 * a day are in the array, never rounded per value.
 * <p>
 * Calendar units follow the zone: minutes and hours advance on the instant line (a fall-back night has 25 hourly buckets), days,
 * weeks (Monday based), months, quarters and years advance on the local line. A day starts at {@code toLocalDate().atStartOfDay(zone)},
 * so a zone whose midnight does not exist starts the day at the first valid instant. An {@code OFFSET} is added to every
 * zone-local boundary (it is applied after the zone-local truncation, not before). The boundaries cover {@code [floor(min), ceil(max)]}.
 */
public final class CalendarBoundaries {
  private final long[] starts;  // size() starts, then the end of the last bucket
  private final int    size;
  private final ZoneId zone;

  private CalendarBoundaries(final long[] starts, final int size, final ZoneId zone) {
    this.starts = starts;
    this.size = size;
    this.zone = zone;
  }

  /**
   * Calendar boundaries.
   *
   * @param alias      the facet alias, named in the bucket cap message
   * @param unit       {@link ChronoUnit#MINUTES}, {@code HOURS}, {@code DAYS}, {@code WEEKS}, {@code MONTHS}, {@code YEARS} or
   *                   {@link IsoFields#QUARTER_YEARS}
   * @param zone       the zone whose local calendar is followed
   * @param offset     added to every zone-local boundary, null for none
   * @param minMs      the smallest value to cover
   * @param maxMs      the largest value to cover
   * @param maxBuckets the bucket cap, exceeded raises {@link CommandExecutionException}
   */
  public static CalendarBoundaries calendar(final String alias, final TemporalUnit unit, final ZoneId zone, final Duration offset,
      final long minMs, final long maxMs, final int maxBuckets) {
    checkRange(alias, minMs, maxMs);
    try {
      final long offsetMs = offset == null ? 0L : offset.toMillis();
      final Cursor cursor = new Cursor(unit, zone);
      cursor.seek(minMs, offsetMs);

      long[] starts = new long[16];
      int n = 0;
      starts[0] = cursor.current(offsetMs);
      while (true) {
        cursor.advance();
        final long end = cursor.current(offsetMs);
        n++;
        if (n > maxBuckets)
          throw tooMany(alias);
        if (n + 1 >= starts.length)
          starts = Arrays.copyOf(starts, starts.length * 2);
        starts[n] = end;
        if (end > maxMs)
          break;
      }
      return new CalendarBoundaries(starts, n, zone);
    } catch (final ArithmeticException | DateTimeException e) {
      throw outOfRange(alias);
    }
  }

  /**
   * Fixed-width boundaries aligned like {@code ts.timeBucket}: {@code start = floor((min - offset) / interval) * interval + offset}.
   * Keys are rendered in {@code keyZone}. The bucket count is computed without overflow, so a range too wide for the cap is refused
   * before anything is allocated.
   *
   * @param alias    the facet alias, named in the error messages
   * @param keyZone  the zone of the rendered keys (the TIMEZONE of the histogram, else UTC)
   * @param offsetMs the grid offset as {@code SQLFunctionTimeBucket.resolveOffset} returns it (any value, reduced modulo the interval)
   */
  public static CalendarBoundaries fixed(final String alias, final ZoneId keyZone, final long intervalMs, final long offsetMs,
      final long minMs, final long maxMs, final int maxBuckets) {
    if (intervalMs <= 0)
      throw new CommandExecutionException("HISTOGRAM " + alias + " interval must be positive: " + intervalMs + "ms");
    checkRange(alias, minMs, maxMs);
    final long first = TimeBucketGrid.bucketStart(minMs, intervalMs, offsetMs);
    if (first > minMs)  // the floor wrapped below Long.MIN_VALUE
      throw outOfRange(alias);
    // first <= max, so the span is exact as an unsigned 64-bit number even when it exceeds Long.MAX_VALUE
    final long buckets = Long.divideUnsigned(maxMs - first, intervalMs);  // count - 1
    if (Long.compareUnsigned(buckets, maxBuckets) >= 0)
      throw tooMany(alias);
    final int n = (int) buckets + 1;
    final long[] starts = new long[n + 1];
    starts[0] = first;
    try {
      for (int i = 1; i <= n; i++)
        starts[i] = Math.addExact(starts[i - 1], intervalMs);
    } catch (final ArithmeticException e) {
      throw outOfRange(alias);
    }
    return new CalendarBoundaries(starts, n, keyZone);
  }

  private static void checkRange(final String alias, final long minMs, final long maxMs) {
    if (minMs > maxMs)
      throw new CommandExecutionException("HISTOGRAM " + alias + " minimum " + minMs + " is greater than its maximum " + maxMs);
  }

  private static CommandExecutionException outOfRange(final String alias) {
    return new CommandExecutionException("HISTOGRAM " + alias + " range is out of the representable time range: use BOUNDS");
  }

  /** The index of the bucket holding the value (half-open {@code [from, to)}), -1 when it is outside every bucket. */
  public int bucketOf(final long epochMs) {
    final int index = Arrays.binarySearch(starts, 0, size + 1, epochMs);
    if (index >= 0)
      return index == size ? -1 : index;
    final int insertion = -index - 1;
    return insertion == 0 || insertion == size + 1 ? -1 : insertion - 1;
  }

  public int size() {
    return size;
  }

  public long from(final int i) {
    return starts[i];
  }

  public long to(final int i) {
    return starts[i + 1];
  }

  /** The start of bucket i as an ISO-8601 local date-time with the zone offset in force at that instant, e.g. {@code 2026-03-01T00:00+01:00}. */
  public String key(final int i) {
    final Instant instant = Instant.ofEpochMilli(starts[i]);
    return OffsetDateTime.ofInstant(instant, zone).toString();
  }

  /** The refusal of a histogram above the bucket cap (D18), shared by the numeric histogram. */
  static CommandExecutionException tooMany(final String alias) {
    return new CommandExecutionException("HISTOGRAM " + alias + " would produce more than 100000 buckets: use a coarser interval or BOUNDS");
  }

  /** Walks the boundaries: the instant line for minutes and hours, the local calendar for the rest. */
  private static final class Cursor {
    private final TemporalUnit unit;
    private final ZoneId       zone;
    private final boolean      instantLine;
    private ZonedDateTime      instantBase;  // instant-line units
    private LocalDate          dateBase;     // local-line units

    Cursor(final TemporalUnit unit, final ZoneId zone) {
      this.unit = unit;
      this.zone = zone;
      this.instantLine = unit == ChronoUnit.MINUTES || unit == ChronoUnit.HOURS;
      if (!instantLine && unit != ChronoUnit.DAYS && unit != ChronoUnit.WEEKS && unit != ChronoUnit.MONTHS && unit != ChronoUnit.YEARS
          && unit != IsoFields.QUARTER_YEARS)
        throw new IllegalArgumentException("Unsupported calendar unit: " + unit);
    }

    /** Positions the cursor on the last boundary (offset included) that is not after {@code minMs}. */
    void seek(final long minMs, final long offsetMs) {
      // start from the period holding min - offset: a huge offset costs no per-bucket walk, only the DST drift is corrected below
      final ZonedDateTime at = Instant.ofEpochMilli(Math.subtractExact(minMs, offsetMs)).atZone(zone);
      if (instantLine)
        instantBase = at.truncatedTo(unit);
      else
        dateBase = truncate(at.toLocalDate());
      while (current(offsetMs) > minMs)
        step(-1);
      while (true) {
        step(1);
        if (current(offsetMs) > minMs) {
          step(-1);
          return;
        }
      }
    }

    void advance() {
      step(1);
    }

    long current(final long offsetMs) {
      return Math.addExact(instantLine ? instantBase.toInstant().toEpochMilli() : dateBase.atStartOfDay(zone).toInstant().toEpochMilli(), offsetMs);
    }

    private void step(final int n) {
      if (instantLine)
        instantBase = instantBase.plus(n, unit);
      else if (unit == ChronoUnit.DAYS)
        dateBase = dateBase.plusDays(n);
      else if (unit == ChronoUnit.WEEKS)
        dateBase = dateBase.plusWeeks(n);
      else if (unit == ChronoUnit.MONTHS)
        dateBase = dateBase.plusMonths(n);
      else if (unit == IsoFields.QUARTER_YEARS)
        dateBase = dateBase.plusMonths(3L * n);
      else
        dateBase = dateBase.plusYears(n);
    }

    private LocalDate truncate(final LocalDate date) {
      if (unit == ChronoUnit.WEEKS)
        return date.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
      if (unit == ChronoUnit.MONTHS)
        return date.withDayOfMonth(1);
      if (unit == IsoFields.QUARTER_YEARS)
        return date.withDayOfMonth(1).withMonth((date.getMonthValue() - 1) / 3 * 3 + 1);
      if (unit == ChronoUnit.YEARS)
        return date.withDayOfYear(1);
      return date;
    }
  }
}
