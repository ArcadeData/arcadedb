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

import com.arcadedb.database.Identifiable;
import com.arcadedb.engine.timeseries.TimeBucketGrid;
import com.arcadedb.function.sql.SQLFunctionConfigurableAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.DateUtils;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * SQL function: ts.timeBucket(interval_string, timestamp [, options])
 * Returns the start of the time bucket containing the given timestamp.
 * <p>
 * Intervals: '1s', '5s', '1m', '5m', '1h', '1d', '1w'
 * <p>
 * Buckets are multiples of the interval counted from an origin. The default origin is the Unix epoch, which was a
 * Thursday at 00:00 UTC: a '1w' bucket starts on Thursday and a '1d' bucket starts at 08:00 in UTC+8 (issue #8798).
 * The optional third parameter moves the grid, as one of:
 * <ul>
 *   <li>{@code {origin: <instant>}}: any instant a bucket starts at. {@code {origin: '2024-01-01T00:00:00Z'}} makes
 *   weeks start on Monday. A bare instant as the third parameter means the same.</li>
 *   <li>{@code {offset: '<duration>'}}: the grid is shifted by a signed duration from the epoch, e.g.
 *   {@code {offset: '-8h'}} makes '1d' buckets start at local midnight in UTC+8.</li>
 *   <li>{@code {timezone: '+08:00'}}: local midnight (and local Monday for weeks) of a zone with a fixed offset. A
 *   zone with daylight saving, or one whose standard time ever changed, has buckets of 23 or 25 hours, which a
 *   fixed-width grid cannot express: it is refused rather than approximated. A '1w' (any interval written in weeks) is anchored to the local Monday;
 *   the same width written as '7d' is not.</li>
 * </ul>
 * <p>
 * Example: SELECT ts.timeBucket('1h', ts) AS hour, avg(temperature) FROM SensorData GROUP BY hour
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionTimeBucket extends SQLFunctionConfigurableAbstract {
  public static final String NAME = "ts.timeBucket";

  /** The last options resolved: the third parameter is almost always a constant, so it is parsed once, not per row. */
  private record ResolvedOffset(Object options, String interval, long offsetMs) {
  }

  private volatile ResolvedOffset lastOffset;

  public SQLFunctionTimeBucket() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length < 2)
      throw new IllegalArgumentException("time_bucket() requires 2 parameters: interval and timestamp");

    if (params[0] == null || params[1] == null)
      return null;

    final String interval = params[0].toString();
    final long intervalMs = parseInterval(interval);

    // A zero-width (or negative) bucket has no boundary to truncate to: '0s' used to reach the division below and
    // throw ArithmeticException: / by zero (issue #6388).
    if (intervalMs <= 0)
      throw new IllegalArgumentException(
          "time_bucket() interval '" + interval + "' must be a positive amount of time, but resolves to " + intervalMs
              + "ms");

    final long timestampMs = toEpochMs(params[1]);
    final long offsetMs = params.length > 2 ? cachedOffset(params[2], interval, intervalMs) : 0L;

    // Floor to the bucket boundary. Math.floorDiv, not '/': Java integer division truncates TOWARD ZERO, so for a
    // pre-epoch (negative) timestamp the plain division returned a boundary LATER than its own input - '1h' over
    // -1800000 (1969-12-31T23:30:00Z) answered the epoch itself - and collapsed the last pre-epoch bucket into the
    // first post-epoch one, silently merging two intervals under one GROUP BY key (issue #6824). The engine's own
    // bucket anchor was floor-aligned for the same reason in #4595; this is the SQL function that fix did not
    // reach. intervalMs is positive here (guarded above), so floorDiv differs from '/' only on negative inputs.
    // The arithmetic lives in TimeBucketGrid, shared with the aggregation push-down, so an origin cannot be honoured
    // by one plan and ignored by the other (issue #8798).
    final long bucketStart = TimeBucketGrid.bucketStart(timestampMs, intervalMs, offsetMs);

    // Issue #7610: expose the bucket as a LocalDateTime, not a java.util.Date. This is the same
    // representation AggregateFromTimeSeriesStep's own bucket pushdown already uses for a single
    // grouping key (issue #4385), and it keeps the value out of JSONObject's ambiguous
    // Date-vs-DATE-column dispatch - a java.util.Date here silently lost its time of day over HTTP
    // query results, because the JSON serializer cannot tell a computed instant from a genuine DATE
    // column by Java class alone.
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(bucketStart), ZoneOffset.UTC);
  }

  private long cachedOffset(final Object options, final String interval, final long intervalMs) {
    final ResolvedOffset last = lastOffset;
    if (last != null && last.interval.equals(interval) && Objects.equals(last.options, options))
      return last.offsetMs;
    final long offsetMs = resolveOffset(options, interval, intervalMs);
    lastOffset = new ResolvedOffset(options, interval, offsetMs);
    return offsetMs;
  }

  /**
   * The bucket grid offset an options value asks for, reduced modulo the interval so that it is what
   * {@link TimeBucketGrid#bucketStart} takes (issue #8798). {@code null} is the epoch-aligned grid.
   * <p>
   * Shared with the planner's aggregation push-down, which has to resolve the same options from the same value or
   * the pushed-down and the generic plans of one query would bucket differently.
   *
   * @param options    a map (or a record) with {@code origin}, {@code offset} or {@code timezone}, or a bare instant
   *                   meaning {@code origin}; only one of the three may be given
   * @param interval   the interval as written, because a timezone aligns a week to the local Monday and the milliseconds
   *                   alone no longer say it was a week
   * @param intervalMs the interval in milliseconds, which must be positive
   * @throws IllegalArgumentException if the options are malformed
   */
  public static long resolveOffset(final Object options, final String interval, final long intervalMs) {
    if (options == null)
      return 0L;

    final Map<String, Object> map = optionsMap(options);
    if (map == null)
      return TimeBucketGrid.normalizeOffset(originToEpochMs(options), intervalMs);

    Object origin = null;
    Object offset = null;
    Object timezone = null;
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      switch (entry.getKey().toLowerCase(Locale.ROOT)) {
      case "origin" -> origin = entry.getValue();
      case "offset" -> offset = entry.getValue();
      case "timezone" -> timezone = entry.getValue();
      default -> throw new IllegalArgumentException(
          "Unknown ts.timeBucket option '" + entry.getKey() + "'. Supported: origin, offset, timezone");
      }
    }

    final int given = (origin != null ? 1 : 0) + (offset != null ? 1 : 0) + (timezone != null ? 1 : 0);
    if (given > 1)
      throw new IllegalArgumentException("ts.timeBucket accepts only one of origin, offset or timezone");
    if (given == 0)
      return 0L;

    if (origin != null)
      return TimeBucketGrid.normalizeOffset(originToEpochMs(origin), intervalMs);
    if (offset != null)
      return TimeBucketGrid.normalizeOffset(offsetToMs(offset), intervalMs);

    return TimeBucketGrid.normalizeOffset(timezoneOriginMs(timezone.toString(), interval), intervalMs);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> optionsMap(final Object options) {
    if (options instanceof Map<?, ?> map) {
      final Map<String, Object> copy = new HashMap<>(map.size() * 2);
      for (final Map.Entry<?, ?> entry : map.entrySet())
        copy.put(String.valueOf(entry.getKey()), entry.getValue());
      return copy;
    }
    if (options instanceof Result result && !result.isElement()) {
      final Map<String, Object> copy = new HashMap<>();
      for (final String name : result.getPropertyNames())
        copy.put(name, result.getProperty(name));
      return copy;
    }
    return null;
  }

  /** An instant a bucket starts at: a date/time, an ISO-8601 string, or a number of epoch milliseconds. */
  private static long originToEpochMs(final Object origin) {
    if (origin instanceof Number number)
      return number.longValue();
    try {
      return DateUtils.toEpochMillis(origin);
    } catch (final IllegalArgumentException | DateTimeParseException e) {
      throw new IllegalArgumentException("Unsupported ts.timeBucket origin: '" + origin + "'", e);
    }
  }

  /** A signed duration: {@code '-8h'}, {@code '+30m'}, or a number of milliseconds. */
  private static long offsetToMs(final Object offset) {
    if (offset instanceof Number number)
      return number.longValue();
    final String text = offset.toString().trim();
    if (!text.isEmpty() && (text.charAt(0) == '-' || text.charAt(0) == '+')) {
      final long magnitude = parseInterval(text.substring(1));
      return text.charAt(0) == '-' ? -magnitude : magnitude;
    }
    return parseInterval(text);
  }

  /**
   * The origin that makes buckets start at the local midnight of a fixed-offset zone - the local Monday for a week,
   * which is how a weekly report counts.
   */
  private static long timezoneOriginMs(final String timezone, final String interval) {
    final ZoneId zone;
    try {
      zone = ZoneId.of(timezone.trim());
    } catch (final DateTimeException e) {
      throw new IllegalArgumentException("Unknown ts.timeBucket timezone: '" + timezone + "'", e);
    }
    if (!zone.getRules().isFixedOffset())
      throw new IllegalArgumentException("ts.timeBucket timezone '" + timezone + "' has more than one UTC offset over"
          + " time (daylight saving, or a past change of its standard time), so its days are not all the same length and"
          + " cannot be a fixed-width bucket. Use a fixed offset (for example '+08:00' or 'UTC+8'), or an explicit"
          + " {offset: '<duration>'}");

    final long zoneOffsetMs = zone.getRules().getOffset(Instant.EPOCH).getTotalSeconds() * 1000L;
    // 1970-01-05 is the first Monday after the epoch
    final long mondayMs = interval != null && interval.trim().toLowerCase(Locale.ROOT).endsWith("w") ? 4 * 86_400_000L : 0L;
    return mondayMs - zoneOffsetMs;
  }

  public static long parseInterval(final String interval) {
    if (interval == null || interval.isEmpty())
      throw new IllegalArgumentException("Invalid time_bucket interval: empty");

    // Parse numeric part and unit suffix
    int unitStart = 0;
    for (int i = 0; i < interval.length(); i++) {
      if (!Character.isDigit(interval.charAt(i))) {
        unitStart = i;
        break;
      }
    }

    if (unitStart == 0)
      throw new IllegalArgumentException("Invalid time_bucket interval: '" + interval + "'");

    final long value = Long.parseLong(interval.substring(0, unitStart));
    final String unit = interval.substring(unitStart).trim().toLowerCase(Locale.ROOT);

    return switch (unit) {
      case "s" -> value * 1000L;
      case "m" -> value * 60_000L;
      case "h" -> value * 3_600_000L;
      case "d" -> value * 86_400_000L;
      case "w" -> value * 7 * 86_400_000L;
      default -> throw new IllegalArgumentException("Unknown time_bucket unit: '" + unit + "'. Supported: s, m, h, d, w");
    };
  }

  /**
   * #8152: was a private near-copy of the same conversion that lives in three other places. It is now one converter
   * in {@link DateUtils#toEpochMillis}, which this function's own contract is the reason for: the day it started
   * answering a {@link LocalDateTime} instead of a {@link Date} (#7610), the copies that did not know about
   * {@code LocalDateTime} started reading every bucket as the epoch.
   */
  private static long toEpochMs(final Object value) {
    try {
      return DateUtils.toEpochMillis(value);
    } catch (final IllegalArgumentException | DateTimeParseException e) {
      throw new IllegalArgumentException("Unsupported timestamp for time_bucket: '" + value + "'"
          + (value != null ? " (" + value.getClass().getName() + ")" : ""), e);
    }
  }

  @Override
  public String getSyntax() {
    return "ts.timeBucket(<interval_string>, <timestamp> [, <options>])";
  }
}
