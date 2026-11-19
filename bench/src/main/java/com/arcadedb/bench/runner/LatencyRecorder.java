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

import java.util.Arrays;

/**
 * Records latency samples in nanoseconds in a growable primitive array, sorted once on the first read.
 */
public final class LatencyRecorder {
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
}
