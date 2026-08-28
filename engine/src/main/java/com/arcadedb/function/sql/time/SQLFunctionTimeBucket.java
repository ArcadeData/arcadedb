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
import com.arcadedb.function.sql.SQLFunctionConfigurableAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Locale;

/**
 * SQL function: time_bucket(interval_string, timestamp)
 * Returns the start of the time bucket containing the given timestamp.
 * <p>
 * Intervals: '1s', '5s', '1m', '5m', '1h', '1d', '1w'
 * <p>
 * Example: SELECT time_bucket('1h', ts) AS hour, avg(temperature) FROM SensorData GROUP BY hour
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionTimeBucket extends SQLFunctionConfigurableAbstract {
  public static final String NAME = "ts.timeBucket";

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

    // Floor to the bucket boundary. Math.floorDiv, not '/': Java integer division truncates TOWARD ZERO, so for a
    // pre-epoch (negative) timestamp the plain division returned a boundary LATER than its own input - '1h' over
    // -1800000 (1969-12-31T23:30:00Z) answered the epoch itself - and collapsed the last pre-epoch bucket into the
    // first post-epoch one, silently merging two intervals under one GROUP BY key (issue #6824). The engine's own
    // bucket anchor was floor-aligned for the same reason in #4595; this is the SQL function that fix did not
    // reach. intervalMs is positive here (guarded above), so floorDiv differs from '/' only on negative inputs.
    final long bucketStart = Math.floorDiv(timestampMs, intervalMs) * intervalMs;

    return new Date(bucketStart);
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

  private static long toEpochMs(final Object value) {
    if (value instanceof Long l)
      return l;
    if (value instanceof Date d)
      return d.getTime();
    if (value instanceof Instant i)
      return i.toEpochMilli();
    if (value instanceof LocalDateTime ldt)
      return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    if (value instanceof Number n)
      return n.longValue();
    if (value instanceof String s) {
      try {
        return Instant.parse(s).toEpochMilli();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Cannot parse timestamp for time_bucket: '" + s + "'", e);
      }
    }
    throw new IllegalArgumentException("Unsupported timestamp type for time_bucket: " + value.getClass().getName());
  }

  @Override
  public String getSyntax() {
    return "time_bucket(<interval_string>, <timestamp>)";
  }
}
