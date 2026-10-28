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
package com.arcadedb.query.facet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The {@link FacetMemoryBudget} reservations of one collector (D13): every byte a counter holds is reserved here before it is
 * allocated. All the waiting reservations of the collector share one wait window: the first one starts the
 * {@code arcadedb.columnar.queryBudgetWait} deadline, every later one (a top-up) waits only for its remainder, so the facet's latency
 * is bounded by one wait however many times it grows.
 * <p>
 * Not thread safe: a collector is driven by one query thread.
 */
final class CollectorReservations {
  private final long                                budgetWaitMs;
  private final List<FacetMemoryBudget.Reservation> reservations = new ArrayList<>(2);
  /** {@link System#nanoTime()} of the first waiting reservation. */
  private       long                                waitStartNanos;
  private       boolean                             waitStarted;
  private       long                                bytes;
  private       int                                 waits;

  /** @param budgetWaitMs the longest the collector waits for the budget, all its reservations together */
  CollectorReservations(final long budgetWaitMs) {
    this.budgetWaitMs = budgetWaitMs;
  }

  /**
   * Reserves {@code bytes} more.
   *
   * @param wait false to try without waiting (a dense counter, refused, falls back to a smaller hash map)
   *
   * @return the granted reservation, null when the budget refused
   */
  FacetMemoryBudget.Reservation reserve(final long bytes, final boolean wait) {
    final FacetMemoryBudget.Reservation granted = FacetMemoryBudget.instance().reserve(bytes, wait ? remainingWaitMs() : 0);
    if (granted != null) {
      reservations.add(granted);
      this.bytes += granted.bytes();
      if (granted.waited())
        waits++;
    }
    return granted;
  }

  /** The bytes reserved and not released. */
  long bytes() {
    return bytes;
  }

  /** The grants that were not immediate. */
  int waits() {
    return waits;
  }

  /** Releases every reservation; idempotent. */
  void release() {
    for (final FacetMemoryBudget.Reservation reservation : reservations)
      reservation.close();
    reservations.clear();
    bytes = 0;
  }

  private long remainingWaitMs() {
    final long now = System.nanoTime();
    if (!waitStarted) {
      waitStarted = true;
      waitStartNanos = now;
    }
    // elapsed, not a deadline: a huge setting cannot overflow the nanosecond clock
    return Math.max(0, budgetWaitMs - TimeUnit.NANOSECONDS.toMillis(now - waitStartNanos));
  }
}
