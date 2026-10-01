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
package com.arcadedb.engine.timeseries;

/**
 * The ONE definition of where a fixed-width time bucket starts (issue #8798).
 * <p>
 * Buckets are multiples of the interval counted from an origin, and the origin defaults to the Unix epoch. The epoch
 * was a Thursday at 00:00 UTC, so with no origin a {@code 1w} bucket starts on Thursday and a {@code 1d} bucket starts
 * at 08:00 in UTC+8. An origin moves the grid:
 * <pre>
 *   bucketStart = floorDiv(ts - origin, interval) * interval + origin
 * </pre>
 * Only the origin modulo the interval matters, so every caller carries that normalised <i>offset</i> (see
 * {@link #normalizeOffset}) instead of the origin: a value in {@code [0, interval)}, zero for the default grid, which
 * is also what lets the epoch-aligned fast path stay exactly the arithmetic it always was.
 * <p>
 * Every path that buckets - the SQL function, the aggregation push-down, the native endpoint, continuous aggregates -
 * goes through here, because an origin that one path honoured and another ignored would put the same sample in two
 * different buckets depending on how the query happened to be planned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeBucketGrid {

  private TimeBucketGrid() {
  }

  /**
   * The origin reduced modulo the interval, in {@code [0, intervalMs)}.
   *
   * @param intervalMs the bucket width, which must be positive
   */
  public static long normalizeOffset(final long originMs, final long intervalMs) {
    if (intervalMs <= 0)
      throw new IllegalArgumentException("A bucket interval must be positive, got " + intervalMs);
    return Math.floorMod(originMs, intervalMs);
  }

  /**
   * The start of the bucket holding {@code timestampMs}.
   *
   * @param intervalMs the bucket width, which must be positive
   * @param offsetMs   the grid offset as {@link #normalizeOffset} returns it; {@code 0} for the epoch-aligned grid
   */
  public static long bucketStart(final long timestampMs, final long intervalMs, final long offsetMs) {
    if (offsetMs == 0)
      return Math.floorDiv(timestampMs, intervalMs) * intervalMs;
    return Math.floorDiv(timestampMs - offsetMs, intervalMs) * intervalMs + offsetMs;
  }
}
