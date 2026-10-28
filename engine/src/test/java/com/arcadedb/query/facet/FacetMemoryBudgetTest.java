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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FacetMemoryBudgetTest {
  @Test
  @Timeout(30)
  void grantsAndReleases() {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    try (final FacetMemoryBudget.Reservation r = b.reserve(512 * 1024, 0)) {
      assertThat(r).isNotNull();
      assertThat(r.bytes()).isEqualTo(512 * 1024);
      assertThat(r.waited()).isFalse();
      assertThat(b.available()).isEqualTo(512 * 1024);
    }
    assertThat(b.available()).isEqualTo(1 << 20);
  }

  @Test
  @Timeout(30)
  void closeIsIdempotent() {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    final FacetMemoryBudget.Reservation r = b.reserve(1024, 0);
    r.close();
    r.close();
    assertThat(b.available()).isEqualTo(1 << 20);
  }

  @Test
  @Timeout(30)
  void roundsUpToKibiBytes() {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    try (final FacetMemoryBudget.Reservation r = b.reserve(1, 0)) {
      assertThat(r).isNotNull();
      assertThat(b.available()).isEqualTo((1 << 20) - 1024);
    }
  }

  @Test
  @Timeout(30)
  void refusesWhenNothingIsLeftWithoutWaiting() {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    try (final FacetMemoryBudget.Reservation all = b.reserve(1 << 20, 0)) {
      assertThat(all).isNotNull();
      assertThat(b.reserve(1024, 0)).isNull();
    }
    assertThat(b.available()).isEqualTo(1 << 20);
  }

  @Test
  @Timeout(30)
  void refusesAboveCapacityWithoutWaiting() {
    // returns at once: a wait of a minute would hit the @Timeout otherwise
    assertThat(new FacetMemoryBudget(1024).reserve(4096, 60_000)).isNull();
  }

  @Test
  @Timeout(30)
  void waitsThenGrantsInOrder() throws Exception {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    final FacetMemoryBudget.Reservation holder = b.reserve(1 << 20, 0);
    assertThat(holder).isNotNull();
    final AtomicReference<FacetMemoryBudget.Reservation> granted = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    final Thread waiter = new Thread(() -> {
      granted.set(b.reserve(1 << 19, 20_000));
      done.countDown();
    });
    waiter.start();
    awaitQueued(b, waiter);
    assertThat(granted.get()).isNull();
    holder.close();
    assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
    final FacetMemoryBudget.Reservation r = granted.get();
    assertThat(r).isNotNull();
    assertThat(r.waited()).isTrue();
    r.close();
    assertThat(b.available()).isEqualTo(1 << 20);
    waiter.join();
  }

  @Test
  @Timeout(30)
  void waitTimesOutWhenNothingIsReleased() {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    try (final FacetMemoryBudget.Reservation all = b.reserve(1 << 20, 0)) {
      assertThat(all).isNotNull();
      assertThat(b.reserve(1024, 10)).isNull();
    }
  }

  @Test
  @Timeout(30)
  void resizesOnSettingChange() {
    final Object original = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    try {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(2L << 20);
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(2L << 20);
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(1L << 20);
      final FacetMemoryBudget shrunk = FacetMemoryBudget.instance();
      assertThat(shrunk.available()).isEqualTo(1L << 20);
      assertThat(shrunk.reserve(2L << 20, 0)).isNull();
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(original);
      FacetMemoryBudget.instance();
    }
  }

  @Test
  @Timeout(30)
  void shrinkingBelowWhatIsHeldDoesNotLosePermits() {
    final Object original = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    try {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(4L << 20);
      final FacetMemoryBudget budget = FacetMemoryBudget.instance();
      final FacetMemoryBudget.Reservation held = budget.reserve(3L << 20, 0);
      assertThat(held).isNotNull();
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(1L << 20);
      assertThat(FacetMemoryBudget.instance().available()).isZero();
      held.close();
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(1L << 20);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(original);
      FacetMemoryBudget.instance();
    }
  }

  /** Spins until the thread is parked in the budget's queue; fails instead of hanging when the thread died first. */
  private static void awaitQueued(final FacetMemoryBudget b, final Thread waiter) {
    while (!b.hasWaiters()) {
      assertThat(waiter.isAlive()).as("the waiter ended before it queued").isTrue();
      Thread.onSpinWait();
    }
  }

  @Test
  @Timeout(30)
  void smallRequestDoesNotBargePastAQueuedWaiter() throws Exception {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    final FacetMemoryBudget.Reservation first = b.reserve(512 * 1024, 0);
    final FacetMemoryBudget.Reservation second = b.reserve(512 * 1024, 0);
    assertThat(first).isNotNull();
    assertThat(second).isNotNull();
    final AtomicReference<FacetMemoryBudget.Reservation> granted = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    final Thread waiter = new Thread(() -> {
      granted.set(b.reserve(768 * 1024, 20_000));
      done.countDown();
    });
    waiter.start();
    awaitQueued(b, waiter);
    first.close();
    // 512 KiB are free but the 768 KiB waiter is first in line: a small immediate request is refused
    assertThat(b.reserve(1024, 0)).isNull();
    second.close();
    assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(granted.get()).isNotNull();
    granted.get().close();
    waiter.join();
  }

  @Test
  @Timeout(30)
  void interruptedWaitReturnsNullKeepsTheFlagAndLeaksNothing() throws Exception {
    final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20);
    final FacetMemoryBudget.Reservation holder = b.reserve(1 << 20, 0);
    assertThat(holder).isNotNull();
    final AtomicReference<Object> result = new AtomicReference<>("unset");
    final AtomicReference<Boolean> flag = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    final Thread waiter = new Thread(() -> {
      result.set(b.reserve(1024, 20_000));
      flag.set(Thread.currentThread().isInterrupted());
      done.countDown();
    });
    waiter.start();
    awaitQueued(b, waiter);
    waiter.interrupt();
    assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(result.get()).isNull();
    assertThat(flag.get()).isTrue();
    assertThat(b.available()).isZero();
    holder.close();
    assertThat(b.available()).isEqualTo(1 << 20);
    waiter.join();
  }
}
