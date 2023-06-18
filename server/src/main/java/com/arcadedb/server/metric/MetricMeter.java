/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.metric;

/**
 * Stores the metrics in RAM.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MetricMeter implements ServerMetrics.Meter {
  private       long   totalCounter             = 0L;
  private final long[] lastMinuteCounters       = new long[60];
  private       int    lastMinuteCountersIndex  = 0;
  private       long   lastHitTimestampInSecs   = 0L;
  private       long   lastAskedTimestampInSecs = 0L;

  @Override
  public synchronized void hit() {
    ++totalCounter;
    updateCountersFromLastHit();
    ++lastMinuteCounters[lastMinuteCountersIndex];
  }

  @Override
  public synchronized float getRequestsPerSecondInLastMinute() {
    return getTotalRequestsInLastMinute() / 60F;
  }

  @Override
  public synchronized float getRequestsPerSecondSinceLastAsked() {
    final long nowInSecs = updateCountersFromLastHit();
    final long diffInSecs = nowInSecs - lastAskedTimestampInSecs;

    if (diffInSecs < 1)
      return 0F;

    long total = 0L;

    int index = lastMinuteCountersIndex;
    for (int i = 0; i < diffInSecs; i++) {
      if (index == 0)
        index = 59;
      else
        --index;
      total += lastMinuteCounters[index];
    }

    lastAskedTimestampInSecs = nowInSecs;
    return total / diffInSecs;
  }

  @Override
  public synchronized long getTotalRequestsInLastMinute() {
    updateCountersFromLastHit();
    long total = 0L;
    for (int i = 0; i < 60; i++)
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
      for (int i = 0; i < diffInSecsFromLastHit; i++) {
        if (lastMinuteCountersIndex >= 59)
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
