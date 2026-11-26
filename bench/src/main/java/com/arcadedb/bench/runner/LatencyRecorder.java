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
package com.arcadedb.bench.runner;

import org.HdrHistogram.Histogram;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.DataFormatException;

/**
 * Records latency samples in nanoseconds in a growable primitive array, sorted once on the first read. Every sample counts as
 * {@link Progress} for the stall watchdog.
 */
public final class LatencyRecorder {
  private static final long HIGHEST_TRACKABLE_MICROS = 3_600_000_000L;
  private static final int  SIGNIFICANT_DIGITS       = 3;

  private long[]  samples;
  private int     count;
  private boolean sorted = true;

  public LatencyRecorder(final int capacity) {
    samples = new long[Math.max(capacity, 16)];
  }

  public void record(final long nanos) {
    if (count == samples.length)
      samples = Arrays.copyOf(samples, count + (count >> 1) + 1);
    samples[count++] = nanos;
    sorted = false;
    Progress.advance();
  }

  /**
   * Appends every sample of {@code other}, which is left as it was. The samples are not counted as {@link Progress} again: they
   * were when {@code other} recorded them.
   */
  public void addAll(final LatencyRecorder other) {
    if (other.count == 0)
      return;
    if (count + other.count > samples.length)
      samples = Arrays.copyOf(samples, count + other.count);
    System.arraycopy(other.samples, 0, samples, count, other.count);
    count += other.count;
    sorted = false;
  }

  /**
   * Nearest-rank percentile, p in (0, 100]. Returns 0 when nothing was recorded.
   */
  public long percentile(final double p) {
    if (count == 0)
      return 0L;
    if (!sorted) {
      Arrays.sort(samples, 0, count);
      sorted = true;
    }
    final int rank = (int) Math.ceil(p / 100.0 * count);
    return samples[Math.min(Math.max(rank, 1), count) - 1];
  }

  /**
   * The sum of every recorded sample in nanoseconds.
   */
  public long totalNanos() {
    long sum = 0;
    for (int i = 0; i < count; i++)
      sum += samples[i];
    return sum;
  }

  public int count() {
    return count;
  }

  /**
   * The samples as an HdrHistogram in microseconds (3 significant digits, up to 1 hour, larger samples clamped), compressed and
   * base64 encoded.
   */
  public String encodedHistogram() {
    final Histogram histogram = new Histogram(HIGHEST_TRACKABLE_MICROS, SIGNIFICANT_DIGITS);
    for (int i = 0; i < count; i++)
      histogram.recordValue(Math.min(samples[i] / 1000L, HIGHEST_TRACKABLE_MICROS));
    final ByteBuffer buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
    final int length = histogram.encodeIntoCompressedByteBuffer(buffer);
    final byte[] bytes = new byte[length];
    buffer.rewind();
    buffer.get(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }

  public static Histogram decode(final String encoded) {
    try {
      return Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(Base64.getDecoder().decode(encoded)), 0L);
    } catch (final DataFormatException e) {
      throw new IllegalArgumentException("not a latency histogram: " + e.getMessage(), e);
    }
  }
}
