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

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TimeBucketGrid#bucketStart} against an arbitrary-precision reference, over the whole {@code long} range and in particular
 * at both of its ends (code review on PR #10004). The start of a bucket is {@code ts - floorMod(ts - offset, interval)} computed
 * without overflow; when that start lies below {@link Long#MIN_VALUE} it cannot be represented, and the grid answers it modulo
 * 2^64, which is still ONE value for every timestamp of the bucket. The formula it used to compute,
 * {@code floorDiv(ts - offset, interval) * interval + offset}, wrapped on the subtraction for a timestamp within {@code offset} of
 * {@link Long#MIN_VALUE}, and then put two timestamps of one bucket into two different buckets.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeBucketGridTest {
  private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);

  /** The exact start of the bucket of {@code ts}, reduced modulo 2^64 into a long the way the grid's arithmetic does. */
  private static long reference(final long ts, final long interval, final long offset) {
    final BigInteger t = BigInteger.valueOf(ts);
    final BigInteger n = BigInteger.valueOf(interval);
    final BigInteger start = t.subtract(t.subtract(BigInteger.valueOf(offset)).mod(n));
    return start.mod(TWO_64).longValue();
  }

  private static List<Long> timestamps() {
    final List<Long> out = new ArrayList<>();
    for (long d = 0; d < 40; d++) {
      out.add(Long.MIN_VALUE + d);
      out.add(Long.MAX_VALUE - d);
      out.add(d - 20);
    }
    // the region within an hour, a day and a week of either end, where the offset subtraction wraps
    for (final long span : new long[] { 3_600_000L, 86_400_000L, 604_800_000L })
      for (long d = 0; d < span; d += span / 97 + 1) {
        out.add(Long.MIN_VALUE + d);
        out.add(Long.MAX_VALUE - d);
      }
    final Random random = new Random(10004);
    for (int i = 0; i < 2_000; i++)
      out.add(random.nextLong());
    return out;
  }

  @Test
  void matchesTheExactStartOverTheWholeRange() {
    final long[] intervals = { 1, 7, 10, 1_000, 900_000, 3_600_000, 86_400_000, 604_800_000, Long.MAX_VALUE / 3, Long.MAX_VALUE };
    final List<Long> timestamps = timestamps();
    for (final long interval : intervals)
      for (final long offset : new long[] { 0, 1, interval / 2, interval - 1, 9 }) {
        if (offset >= interval)
          continue;
        for (final long ts : timestamps)
          assertThat(TimeBucketGrid.bucketStart(ts, interval, offset)).as("ts=%d interval=%d offset=%d", ts, interval, offset)
              .isEqualTo(reference(ts, interval, offset));
      }
  }

  /** The case the review reported: with interval 10 and offset 9, MIN+8 and MIN+9 share the bucket that starts at MIN+7. */
  @Test
  void twoTimestampsOfOneBucketNearLongMinValueShareItsStart() {
    assertThat(TimeBucketGrid.bucketStart(Long.MIN_VALUE + 8, 10, 9)).isEqualTo(Long.MIN_VALUE + 7);
    assertThat(TimeBucketGrid.bucketStart(Long.MIN_VALUE + 9, 10, 9)).isEqualTo(Long.MIN_VALUE + 7);
    assertThat(TimeBucketGrid.bucketStart(Long.MIN_VALUE + 16, 10, 9)).isEqualTo(Long.MIN_VALUE + 7);
    assertThat(TimeBucketGrid.bucketStart(Long.MIN_VALUE + 17, 10, 9)).isEqualTo(Long.MIN_VALUE + 17);
  }

  /** A bucket that begins below Long.MIN_VALUE has no representable start, but all of its timestamps still agree on one value. */
  @Test
  void theBucketThatBeginsBelowLongMinValueIsStillOneBucket() {
    // MIN is 2 past a multiple of 10, so with offset 9 the bucket holding MIN..MIN+6 starts at MIN-3
    final long start = TimeBucketGrid.bucketStart(Long.MIN_VALUE, 10, 9);
    for (long d = 1; d <= 6; d++)
      assertThat(TimeBucketGrid.bucketStart(Long.MIN_VALUE + d, 10, 9)).isEqualTo(start);
    assertThat(start).isEqualTo(Long.MAX_VALUE - 2);
  }

  /** An offset outside [0, interval) means the same grid as the offset normalizeOffset reduces it to. */
  @Test
  void anUnnormalizedOffsetMeansTheSameGrid() {
    final long hour = 3_600_000L;
    for (final long ts : new long[] { Long.MIN_VALUE + 5, -1, 0, 1, 1_700_000_123_456L, Long.MAX_VALUE }) {
      assertThat(TimeBucketGrid.bucketStart(ts, hour, hour + 900_000L)).isEqualTo(TimeBucketGrid.bucketStart(ts, hour, 900_000L));
      assertThat(TimeBucketGrid.bucketStart(ts, hour, -2_700_000L)).isEqualTo(TimeBucketGrid.bucketStart(ts, hour, 900_000L));
    }
  }
}
