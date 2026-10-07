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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The JVM-wide pool of scan read-ahead (issue #9404): what a scan reserves, what it gives back, and what happens to a scan nobody
 * finished.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ScanReadAheadBudgetTest {
  private final long previousLimit = GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.getValueAsLong();

  @AfterEach
  void restore() {
    GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(previousLimit);
  }

  @Test
  void aReservationIsHeldThenGivenBackAndTheAvailableBytesFollow() {
    GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(64L);
    final long baseline = ScanReadAheadBudget.getReservedBytes();
    final long availableBefore = ScanReadAheadBudget.getAvailableBytes();
    assertThat(ScanReadAheadBudget.getLimitBytes()).isEqualTo(64L * 1024 * 1024);

    final ScanReadAheadBudget.Reservation reservation = ScanReadAheadBudget.newReservation(new Object());
    reservation.reserve(1_000_000L);
    reservation.reserve(500_000L);
    assertThat(reservation.getHeldBytes()).isEqualTo(1_500_000L);
    assertThat(ScanReadAheadBudget.getReservedBytes() - baseline).isEqualTo(1_500_000L);
    assertThat(ScanReadAheadBudget.getAvailableBytes()).isEqualTo(availableBefore - 1_500_000L);
    assertThat(ScanReadAheadBudget.getPeakReservedBytes()).isGreaterThanOrEqualTo(baseline + 1_500_000L);

    reservation.release();
    reservation.release();
    assertThat(reservation.getHeldBytes()).isZero();
    assertThat(ScanReadAheadBudget.getReservedBytes()).isEqualTo(baseline);
  }

  @Test
  void manyThreadsReservingAndReleasingLeaveThePoolWhereItWas() throws Exception {
    GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(1024L);
    final long baseline = ScanReadAheadBudget.getReservedBytes();
    final int threads = 16;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++)
      futures.add(pool.submit(() -> {
        start.await();
        final ScanReadAheadBudget.Reservation reservation = ScanReadAheadBudget.newReservation(new Object());
        for (int i = 0; i < 5_000; i++) {
          reservation.reserve(1_000L + i % 7);
          if (i % 3 == 0)
            reservation.release();
        }
        reservation.release();
        return null;
      }));
    start.countDown();
    for (final Future<?> future : futures)
      future.get();
    pool.shutdown();

    assertThat(ScanReadAheadBudget.getReservedBytes()).isEqualTo(baseline);
  }

  @Test
  void aPoolPastItsLimitHasNothingLeftButStaysUsable() {
    GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(1L);
    final ScanReadAheadBudget.Reservation reservation = ScanReadAheadBudget.newReservation(new Object());
    try {
      reservation.reserve(8L * 1024 * 1024);
      assertThat(ScanReadAheadBudget.getAvailableBytes()).isZero();
    } finally {
      reservation.release();
    }
  }

  @Test
  void zeroAndNegativeLimitsDisableThePool() {
    for (final long limit : new long[] { 0L, -1L, Long.MIN_VALUE }) {
      GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(limit);
      assertThat(ScanReadAheadBudget.isEnabled()).isFalse();
      assertThat(ScanReadAheadBudget.getLimitBytes()).isZero();
      assertThat(ScanReadAheadBudget.getAvailableBytes()).isEqualTo(Long.MAX_VALUE);
    }
    GlobalConfiguration.QUERY_SCAN_READ_AHEAD_MAX_RAM.setValue(Long.MAX_VALUE);
    assertThat(ScanReadAheadBudget.getLimitBytes()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void aReservationNobodyReleasedGoesBackWhenItsOwnerIsCollected() {
    final long baseline = ScanReadAheadBudget.getReservedBytes();
    reserveForAnOwnerThatIsGoneRightAfter(2_000_000L);
    assertThat(ScanReadAheadBudget.getReservedBytes()).isGreaterThanOrEqualTo(baseline);

    await().atMost(Duration.ofSeconds(20)).until(() -> {
      System.gc();
      return ScanReadAheadBudget.getReservedBytes() <= baseline;
    });
  }

  private static void reserveForAnOwnerThatIsGoneRightAfter(final long bytes) {
    ScanReadAheadBudget.newReservation(new Object()).reserve(bytes);
  }
}
