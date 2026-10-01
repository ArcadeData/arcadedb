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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The heap the in-memory buffers of all the queries running in the JVM may hold at once, across every database:
 * {@link GlobalConfiguration#QUERY_MAX_HEAP_RAM} (issue #8591).
 * <p>
 * {@link GlobalConfiguration#QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP} caps what one operation of one query holds, which
 * protects against a single runaway query but not against many large ones at once: the incident behind #8583 had 55
 * concurrent requests, each well under that cap, holding 64 GB together. Here every query reserves the estimated size
 * of its buffers, through its {@link QueryHeapTracker}, and a reservation past the budget fails the query that asked.
 * <p>
 * The trackers reserve in chunks, so this counter is touched a few dozen times by a query that buffers gigabytes and
 * never by one that buffers less than {@link QueryHeapTracker#UNRESERVED_BYTES}. The limit is read from the setting on
 * every reservation, so a change applies to the next one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class QueryHeapBudget {
  private static final AtomicLong RESERVED = new AtomicLong();
  private static final AtomicLong PEAK     = new AtomicLong();
  private static final LongAdder  REFUSALS = new LongAdder();

  private QueryHeapBudget() {
  }

  /** The budget in bytes, or 0 when it is disabled. */
  public static long getLimitBytes() {
    final long megabytes = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    if (megabytes <= 0)
      return 0L;
    // A VALUE TOO LARGE FOR BYTES IN A LONG IS A BUDGET NOTHING REACHES
    return megabytes < Long.MAX_VALUE / (1024 * 1024) ? megabytes * 1024 * 1024 : Long.MAX_VALUE;
  }

  public static boolean isEnabled() {
    return GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong() > 0;
  }

  /** The bytes the running queries hold reserved right now. */
  public static long getReservedBytes() {
    return RESERVED.get();
  }

  /** The most bytes the queries held reserved at once since the JVM started. */
  public static long getPeakReservedBytes() {
    return PEAK.get();
  }

  /** How many times a query was refused heap since the JVM started. */
  public static long getRefusals() {
    return REFUSALS.sum();
  }

  /**
   * Reserves {@code bytes} unless that takes the reservations past {@code limit}.
   *
   * @param limit the budget in bytes, as {@link #getLimitBytes()} answered it; 0 reserves without a bound
   */
  static boolean tryReserve(final long bytes, final long limit) {
    long current;
    do {
      current = RESERVED.get();
      if (limit > 0 && current + bytes > limit)
        return false;
    } while (!RESERVED.compareAndSet(current, current + bytes));

    final long now = current + bytes;
    long peak;
    while (now > (peak = PEAK.get()) && !PEAK.compareAndSet(peak, now)) {
      // RETRY: ANOTHER RESERVATION MOVED THE PEAK MEANWHILE
    }
    return true;
  }

  static void release(final long bytes) {
    RESERVED.addAndGet(-bytes);
  }

  static void refused() {
    REFUSALS.increment();
  }
}
