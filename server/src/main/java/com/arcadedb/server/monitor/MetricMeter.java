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
package com.arcadedb.server.monitor;

/**
 * Stores the metrics in RAM.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MetricMeter implements ServerMetrics.Meter {
  private static final int SLOTS = 60;

  private       long   totalCounter             = 0L;
  private final long[] lastMinuteCounters       = new long[SLOTS];
  private       int    lastMinuteCountersIndex  = 0;
  private       long   lastHitTimestampInSecs   = 0L;
  private       long   lastAskedTimestampInSecs;

  public MetricMeter() {
    this(System.currentTimeMillis() / 1000);
  }

  /**
   * Visible for tests: lets a test place the "last asked" instant in the past to exercise the ring cap without waiting.
   */
  MetricMeter(final long lastAskedTimestampInSecs) {
    this.lastAskedTimestampInSecs = lastAskedTimestampInSecs;
  }

  @Override
  public synchronized void hit() {
    ++totalCounter;
    updateCountersFromLastHit();
    ++lastMinuteCounters[lastMinuteCountersIndex];
  }

  @Override
  public synchronized void hits(final long count) {
    totalCounter += count;
    updateCountersFromLastHit();
    lastMinuteCounters[lastMinuteCountersIndex] += count;
  }

  @Override
  public synchronized float getRequestsPerSecondInLastMinute() {
    return getTotalRequestsInLastMinute() / (float) SLOTS;
  }

  @Override
  public synchronized float getRequestsPerSecondSinceLastAsked() {
    final long nowInSecs = updateCountersFromLastHit();
    final long diffInSecs = nowInSecs - lastAskedTimestampInSecs;

    if (diffInSecs < 1)
      return 0F;

    // THE RING HOLDS 60 SLOTS AND THE CURRENT ONE IS STILL FILLING: A LONGER GAP WOULD JUST READ THE SAME SLOTS AGAIN
    final int slots = (int) Math.min(diffInSecs, SLOTS - 1);

    long total = 0L;

    int index = lastMinuteCountersIndex;
    for (int i = 0; i < slots; i++) {
      if (index == 0)
        index = SLOTS - 1;
      else
        --index;
      total += lastMinuteCounters[index];
    }

    lastAskedTimestampInSecs = nowInSecs;
    return (float) total / slots;
  }

  @Override
  public synchronized long getTotalRequestsInLastMinute() {
    updateCountersFromLastHit();
    long total = 0L;
    for (int i = 0; i < SLOTS; i++)
      total += lastMinuteCounters[i];
    return total;
  }

  @Override
  public synchronized long getTotalCounter() {
    return totalCounter;
  }

  /**
   * Called under synchronized, no need to synchronize here.
   *
   * @return
   */
  private long updateCountersFromLastHit() {
    final long nowInSecs = System.currentTimeMillis() / 1000;

    if (lastHitTimestampInSecs == 0) {
      // FIRST TIME
      lastHitTimestampInSecs = nowInSecs;
      return nowInSecs;
    }

    final long diffInSecsFromLastHit = nowInSecs - lastHitTimestampInSecs;

    if (diffInSecsFromLastHit > 0) {
      // AFTER A FULL TURN OF THE RING EVERY SLOT IS ALREADY ZEROED
      final long steps = Math.min(diffInSecsFromLastHit, SLOTS);
      for (long i = 0; i < steps; i++) {
        if (lastMinuteCountersIndex >= SLOTS - 1)
          lastMinuteCountersIndex = 0;
        else
          ++lastMinuteCountersIndex;
        lastMinuteCounters[lastMinuteCountersIndex] = 0L;
      }
      lastHitTimestampInSecs = nowInSecs;
    }

    return nowInSecs;
  }
}
