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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Task 9: the two dedicated columnar pools, the apply ticker behind {@link ColumnarApplyPool#register(Runnable)} and the
 * configuration keys they read.
 */
class ColumnarPoolsTest {

  private static final LongSupplier FAST = () -> 1L;

  @Test
  void poolsAreSingletonsWithConfiguredSize() throws Exception {
    assertThat(ColumnarApplyPool.getInstance()).isSameAs(ColumnarApplyPool.getInstance());
    assertThat(ColumnarMaintenancePool.getInstance()).isSameAs(ColumnarMaintenancePool.getInstance());

    final CountDownLatch release = new CountDownLatch(1);
    try {
      // Saturate with more tasks than threads: the pool must never grow past its configured size
      for (int i = 0; i < 50; i++) {
        ColumnarApplyPool.getInstance().getExecutorService().execute(() -> await(release));
        ColumnarMaintenancePool.getInstance().getExecutorService().execute(() -> await(release));
      }
      final PoolStats apply = ColumnarApplyPool.getInstance().getPoolStats();
      final PoolStats maintenance = ColumnarMaintenancePool.getInstance().getPoolStats();
      assertThat(apply.poolSize()).isBetween(1, GlobalConfiguration.COLUMNAR_APPLY_THREADS.getValueAsInteger());
      assertThat(maintenance.poolSize()).isBetween(1, GlobalConfiguration.COLUMNAR_MAINTENANCE_THREADS.getValueAsInteger());
      assertThat(apply.queueDepth()).as("unbounded queue: excess tasks wait").isGreaterThan(0);
      assertThat(apply.queueCapacityRemaining()).as("unbounded queue reports -1").isEqualTo(-1);
      assertThat(apply.callerRunFallbacks()).isZero();
      assertThat(maintenance.callerRunFallbacks()).isZero();
    } finally {
      release.countDown();
    }
  }

  @Test
  void tickerRunsRegisteredTicksUntilUnregistered() throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    final CountDownLatch twoCalls = new CountDownLatch(2);
    final Runnable tick = () -> {
      calls.incrementAndGet();
      twoCalls.countDown();
    };
    final ColumnarApplyPool pool = ColumnarApplyPool.getInstance();
    pool.register(tick, FAST);
    try {
      assertThat(twoCalls.await(30, TimeUnit.SECONDS)).as("the ticker calls the tick repeatedly").isTrue();
    } finally {
      pool.unregister(tick);
    }
    pool.unregister(tick); // idempotent

    final int afterUnregister = calls.get();
    // Let a few intervals elapse through a second, live tick: proves the ticker is still running, so the first stayed put
    // because it was removed and not because the ticker stopped.
    final CountDownLatch otherTicks = new CountDownLatch(3);
    final Runnable other = otherTicks::countDown;
    pool.register(other, FAST);
    try {
      assertThat(otherTicks.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.unregister(other);
    }
    assertThat(calls.get()).as("an unregistered tick is never called again").isEqualTo(afterUnregister);
  }

  @Test
  void aFailingTickDoesNotStopTheOthers() throws Exception {
    final CountDownLatch survivor = new CountDownLatch(2);
    final Runnable failing = () -> {
      throw new IllegalStateException("tick failure injected by the test");
    };
    final Runnable good = survivor::countDown;
    final ColumnarApplyPool pool = ColumnarApplyPool.getInstance();
    pool.register(failing, FAST);
    pool.register(good, FAST);
    try {
      assertThat(survivor.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.unregister(failing);
      pool.unregister(good);
    }
  }

  @Test
  void unregisterWaitsForAnInFlightTick() throws Exception {
    final CountDownLatch inTick = new CountDownLatch(1);
    final CountDownLatch leaveTick = new CountDownLatch(1);
    final AtomicInteger finished = new AtomicInteger();
    final Runnable tick = () -> {
      inTick.countDown();
      await(leaveTick);
      finished.incrementAndGet();
    };
    final ColumnarApplyPool pool = ColumnarApplyPool.getInstance();
    pool.register(tick, FAST);
    assertThat(inTick.await(30, TimeUnit.SECONDS)).isTrue();

    final AtomicReference<Boolean> unregisterReturnedEarly = new AtomicReference<>();
    final Thread unregistering = new Thread(() -> {
      pool.unregister(tick);
      unregisterReturnedEarly.set(finished.get() == 0);
    });
    unregistering.start();
    unregistering.join(300); // still parked behind the running tick
    assertThat(unregistering.isAlive()).as("unregister blocks while the tick is running").isTrue();
    leaveTick.countDown();
    unregistering.join(30_000);
    assertThat(unregistering.isAlive()).isFalse();
    assertThat(unregisterReturnedEarly.get()).as("the tick had finished when unregister returned").isFalse();
  }

  @Test
  void neverRunsOnTheSubmittingThread() throws Exception {
    for (final ExecutorService service : new ExecutorService[] { ColumnarApplyPool.getInstance().getExecutorService(),
        ColumnarMaintenancePool.getInstance().getExecutorService() }) {
      final CountDownLatch release = new CountDownLatch(1);
      try {
        for (int i = 0; i < 1_000; i++)
          service.execute(() -> await(release));

        final AtomicReference<Thread> ranOn = new AtomicReference<>();
        final CountDownLatch ran = new CountDownLatch(1);
        service.execute(() -> {
          ranOn.set(Thread.currentThread());
          ran.countDown();
        });
        assertThat(ran.getCount()).as("queued behind the blocked tasks, not run inline on the submitter").isEqualTo(1);
        release.countDown();
        assertThat(ran.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(ranOn.get()).isNotSameAs(Thread.currentThread());
        assertThat(ranOn.get().isDaemon()).isTrue();
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void eachTickRunsAtItsOwnInterval() throws Exception {
    final AtomicInteger fast = new AtomicInteger();
    final AtomicInteger slow = new AtomicInteger();
    final Runnable fastTick = fast::incrementAndGet;
    final Runnable slowTick = slow::incrementAndGet;
    final ColumnarApplyPool pool = ColumnarApplyPool.getInstance();
    pool.register(fastTick, () -> 10L);
    pool.register(slowTick, () -> 200L);
    try {
      awaitCount(fast, 40);
    } finally {
      pool.unregister(fastTick);
      pool.unregister(slowTick);
    }
    // The fast tick ran 40 times (>= 400 ms at its interval); the slow one can have run only a few times in that window
    assertThat(slow.get()).as("a slow-interval tick is not called at the fast rate").isLessThan(fast.get() / 4);
  }

  @Test
  void aChangedIntervalTakesEffect() throws Exception {
    final AtomicLong interval = new AtomicLong(3_600_000L);
    final AtomicInteger calls = new AtomicInteger();
    final Runnable tick = calls::incrementAndGet;
    final ColumnarApplyPool pool = ColumnarApplyPool.getInstance();
    pool.register(tick, interval::get);
    try {
      final AtomicInteger probe = new AtomicInteger();
      final Runnable probeTick = probe::incrementAndGet;
      pool.register(probeTick, FAST);
      try {
        awaitCount(probe, 10);
      } finally {
        pool.unregister(probeTick);
      }
      assertThat(calls.get()).as("an hour-long interval: not called yet").isZero();
      interval.set(1L);
      awaitCount(calls, 2);
    } finally {
      pool.unregister(tick);
    }
  }

  @Test
  void aTickUnregisteredWhileAnEarlierOneRunsNeverRuns() throws Exception {
    final CountDownLatch aRunning = new CountDownLatch(1);
    final CountDownLatch releaseA = new CountDownLatch(1);
    final AtomicInteger bRuns = new AtomicInteger();
    final AtomicInteger aRuns = new AtomicInteger();
    final Runnable a = () -> {
      if (aRuns.incrementAndGet() == 1) {
        aRunning.countDown();
        await(releaseA);
      }
    };
    final Runnable b = bRuns::incrementAndGet;
    final ColumnarApplyPool pool = ColumnarApplyPool.getInstance();
    final AtomicBoolean armed = new AtomicBoolean();
    // a cannot fire before both are registered, so b is certainly in the snapshot the ticker iterates behind a
    pool.register(a, () -> armed.get() ? 1L : 3_600_000L);
    pool.register(b, () -> 1L);
    armed.set(true);
    try {
      assertThat(aRunning.await(30, TimeUnit.SECONDS)).isTrue();
      // b is in the snapshot the ticker is iterating, behind a. Retire it from another thread: its monitor is free, so
      // this returns while a still runs, and only the live re-check can keep b from running after a returns.
      final Thread helper = new Thread(() -> pool.unregister(b));
      helper.start();
      helper.join(30_000);
      assertThat(helper.isAlive()).isFalse();
      releaseA.countDown();
      awaitCount(aRuns, 3);
      assertThat(bRuns.get()).as("b was retired before the ticker reached it").isZero();
    } finally {
      releaseA.countDown();
      pool.unregister(a);
      pool.unregister(b);
    }
  }

  @Test
  void aThrowingIntervalSupplierDoesNotStopTheTicker() throws Exception {
    final AtomicInteger good = new AtomicInteger();
    final Runnable neverRuns = () -> {
      throw new AssertionError("a tick whose interval cannot be read is not due");
    };
    final Runnable goodTick = good::incrementAndGet;
    final ColumnarApplyPool pool = ColumnarApplyPool.getInstance();
    pool.register(neverRuns, () -> {
      throw new IllegalStateException("supplier failure injected by the test");
    });
    pool.register(goodTick, FAST);
    try {
      awaitCount(good, 5);
    } finally {
      pool.unregister(neverRuns);
      pool.unregister(goodTick);
    }
  }

  @Test
  void registerAfterCloseIsRejected() throws Exception {
    final Constructor<ColumnarApplyPool> constructor = ColumnarApplyPool.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    final ColumnarApplyPool own = constructor.newInstance();
    own.close();
    assertThatThrownBy(() -> own.register(() -> {
    }, FAST)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void configurationDefaults() {
    assertThat(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.getDefValue()).isEqualTo(1000L);
    assertThat(GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS.getDefValue()).isEqualTo(65536);
    assertThat(GlobalConfiguration.COLUMNAR_MERGE_DEAD_RATIO.getDefValue()).isEqualTo(0.3f);
    assertThat(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS.getDefValue()).isEqualTo(10);
    assertThat(GlobalConfiguration.COLUMNAR_MAX_CARDINALITY.getDefValue()).isEqualTo(1_000_000);
    assertThat(GlobalConfiguration.COLUMNAR_SEGMENT_BLOCK_ROWS.getDefValue()).isEqualTo(16384);
    assertThat(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL.getDefValue()).isEqualTo(100L);
    assertThat(GlobalConfiguration.COLUMNAR_APPLY_THREADS.getDefValue()).isEqualTo(2);
    assertThat(GlobalConfiguration.COLUMNAR_MAINTENANCE_THREADS.getDefValue()).isEqualTo(2);
    assertThat(GlobalConfiguration.COLUMNAR_REBUILD_CONCURRENCY.getDefValue()).isEqualTo(1);
    assertThat(GlobalConfiguration.COLUMNAR_DICTIONARY_COMPACT_RATIO.getDefValue()).isEqualTo(0.5f);

    assertThat(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.getKey()).isEqualTo("arcadedb.columnar.flushInterval");
    assertThat(GlobalConfiguration.COLUMNAR_APPLY_THREADS.getKey()).isEqualTo("arcadedb.columnar.applyThreads");
    assertThat(GlobalConfiguration.COLUMNAR_DICTIONARY_COMPACT_RATIO.getKey()).isEqualTo("arcadedb.columnar.dictionaryCompactRatio");

    for (final GlobalConfiguration jvm : new GlobalConfiguration[] { GlobalConfiguration.COLUMNAR_APPLY_THREADS,
        GlobalConfiguration.COLUMNAR_MAINTENANCE_THREADS, GlobalConfiguration.COLUMNAR_REBUILD_CONCURRENCY })
      assertThat(jvm.getScope()).isEqualTo(GlobalConfiguration.SCOPE.JVM);
    for (final GlobalConfiguration database : new GlobalConfiguration[] { GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL,
        GlobalConfiguration.COLUMNAR_FLUSH_MAX_ROWS, GlobalConfiguration.COLUMNAR_MERGE_DEAD_RATIO,
        GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, GlobalConfiguration.COLUMNAR_MAX_CARDINALITY,
        GlobalConfiguration.COLUMNAR_SEGMENT_BLOCK_ROWS, GlobalConfiguration.COLUMNAR_APPLY_INTERVAL,
        GlobalConfiguration.COLUMNAR_DICTIONARY_COMPACT_RATIO })
      assertThat(database.getScope()).isEqualTo(GlobalConfiguration.SCOPE.DATABASE);
  }

  private static void awaitCount(final AtomicInteger counter, final int target) throws InterruptedException {
    final CountDownLatch never = new CountDownLatch(1);
    for (int i = 0; i < 3_000 && counter.get() < target; i++)
      never.await(10, TimeUnit.MILLISECONDS);
    assertThat(counter.get()).as("ticks observed").isGreaterThanOrEqualTo(target);
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
