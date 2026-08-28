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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6824: {@code ts.timeBucket()} computed its boundary with plain Java integer division, which truncates
 * toward zero rather than flooring. For any negative epoch value that puts the returned "bucket start" AFTER the
 * timestamp it is supposed to contain, and it collapses the last pre-epoch bucket into the first post-epoch one.
 * <p>
 * A bucket function has exactly one contract - {@code bucket(t) <= t}, and every {@code t} in one interval maps to
 * one boundary - so the two arms below are that contract, not a spot check of two constants: the containment arm
 * would fail for EVERY negative input under the old expression, and the partition arm is what a {@code GROUP BY}
 * key actually relies on. Truncation toward zero merges {@code [-1h, 0)} into the bucket for {@code [0, 1h)}, so a
 * historical series crossing 1970 silently aggregated two hours of samples under one boundary.
 * <p>
 * The engine's own bucket anchor was already floor-aligned for #4595
 * ({@code TimeSeriesNegativeTimestampBucketTest}); this is the same defect one layer up, in the SQL function, which
 * that fix did not reach. The {@code intervalMs <= 0} guard added for #6388 only ever guarded the divisor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6824TimeBucketPreEpochTest extends TestHelper {

  private static final long HOUR_MS = 3_600_000L;

  private long bucket(final String interval, final long timestampMs) {
    try (final ResultSet resultSet = database.query("sql",
        "SELECT ts.timeBucket('" + interval + "', " + timestampMs + ") AS b")) {
      return resultSet.next().<Date>getProperty("b").getTime();
    }
  }

  /**
   * The whole of the contract: the bucket start can never be later than the timestamp it buckets. Every negative
   * input below returned a LATER instant before the fix - {@code -1_800_000} (1969-12-31T23:30:00Z) came back as
   * the epoch itself, half an hour into its own future.
   */
  @Test
  void aBucketStartIsNeverLaterThanTheTimestampItBuckets() {
    for (final long timestampMs : new long[] { -1L, -1_800_000L, -HOUR_MS, -HOUR_MS - 1, -86_400_000L, -1_000_000_000_000L,
        0L, 1L, 1_700_000_000_000L })
      assertThat(bucket("1h", timestampMs)).as("bucket('1h', %d)", timestampMs).isLessThanOrEqualTo(timestampMs);
  }

  /**
   * The two documented repros, pinned as the exact boundaries they must be rather than only as "not in the future":
   * 1969-12-31T23:30:00Z belongs to the 23:00 hour, and -1ms belongs to 1969-12-31, not to 1970-01-01.
   */
  @Test
  void aPreEpochTimestampBucketsToItsOwnIntervalStart() {
    assertThat(bucket("1h", -1_800_000L)).isEqualTo(-HOUR_MS);
    assertThat(bucket("1d", -1L)).isEqualTo(-86_400_000L);
    // Exactly on a boundary is its own bucket start, on both sides of the epoch.
    assertThat(bucket("1h", -HOUR_MS)).isEqualTo(-HOUR_MS);
    assertThat(bucket("1h", 0L)).isZero();
  }

  /**
   * The reason this matters at all: used as a {@code GROUP BY} key, truncation toward zero merged the last
   * pre-epoch bucket into the first post-epoch one, so the two hours either side of 1970 aggregated as one.
   */
  @Test
  void thePreEpochAndPostEpochBucketsDoNotCollapseIntoEachOther() {
    final Map<Long, Integer> countByBucket = new HashMap<>();
    // Two samples in [-1h, 0) and three in [0, 1h): five samples, two buckets - never one bucket of five.
    for (final long timestampMs : new long[] { -HOUR_MS, -1L, 0L, 1L, HOUR_MS - 1 })
      countByBucket.merge(bucket("1h", timestampMs), 1, Integer::sum);

    assertThat(countByBucket).containsExactlyInAnyOrderEntriesOf(Map.of(-HOUR_MS, 2, 0L, 3));
  }

  /** The positive side is unchanged, including the case {@code Issue6388TimeFunctionArgumentTest} already pins. */
  @Test
  void positiveTimestampsBucketExactlyAsBefore() {
    assertThat(bucket("1h", 3_900_000L)).isEqualTo(3_600_000L);
    assertThat(bucket("1d", 1_700_000_000_000L)).isEqualTo(1_699_920_000_000L);
  }

  /**
   * The typed overloads reach the same expression through {@code toEpochMs()}, which accepts a negative
   * {@code Date} and a pre-epoch {@code Instant} without complaint - so the fix has to hold for them too.
   */
  @Test
  void theTypedPreEpochOverloadsFloorAsWell() {
    try (final ResultSet resultSet = database.query("sql", "SELECT ts.timeBucket('1h', ?) AS b",
        new Date(-1_800_000L))) {
      assertThat(resultSet.next().<Date>getProperty("b").getTime()).isEqualTo(-HOUR_MS);
    }
    try (final ResultSet resultSet = database.query("sql", "SELECT ts.timeBucket('1h', ?) AS b",
        Instant.ofEpochMilli(-1_800_000L))) {
      assertThat(resultSet.next().<Date>getProperty("b").getTime()).isEqualTo(-HOUR_MS);
    }
  }
}
