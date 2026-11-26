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
package com.arcadedb.bench.workload.ramp;

import com.arcadedb.bench.workload.ramp.OpenLoop.StepResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenLoopTest {
  private static final long MS = 1_000_000L;

  @Test
  void arrivalsFollowTheSchedule() {
    final FakeClock clock = new FakeClock();
    final List<Long> startedAt = new ArrayList<>();
    final OpenLoop loop = new OpenLoop(clock, 4, client -> startedAt.add(clock.nanos()), Runnable::run);

    final StepResult result = loop.runStep(100, Duration.ZERO, Duration.ofSeconds(1));

    assertThat(result.arrivals()).isEqualTo(100);
    assertThat(result.ratePerS()).isEqualTo(100.0);
    assertThat(result.dropped()).isZero();
    assertThat(result.errors()).isZero();
    assertThat(result.lateStarts()).isZero();
    assertThat(startedAt).hasSize(100);
    for (int i = 0; i < 100; i++)
      assertThat(startedAt.get(i)).isEqualTo(i * 10 * MS);
  }

  /**
   * Coordinated omission pin: the first operation blocks the only client (and, inline, the scheduler) for 25 ms, so the
   * arrivals due at 10 ms and 20 ms start late. Their latency counts from when they were due, not from when they were sent: a
   * send-based measure would report 1 ms for both.
   */
  @Test
  void latencyIsMeasuredFromTheScheduledStart() {
    final FakeClock clock = new FakeClock();
    final AtomicInteger calls = new AtomicInteger();
    final OpenLoop loop = new OpenLoop(clock, 1, client -> clock.advance(calls.getAndIncrement() == 0 ? 25 * MS : MS),
        Runnable::run);

    final StepResult result = loop.runStep(100, Duration.ZERO, Duration.ofMillis(30));

    // latencies: 25 ms (due 0, done 25), 16 ms (due 10, done 26), 7 ms (due 20, done 27)
    assertThat(result.arrivals()).isEqualTo(3);
    assertThat(result.dropped()).isZero();
    assertThat(result.p50Ms()).isEqualTo(16.0);
    assertThat(result.p99Ms()).isEqualTo(25.0);
    assertThat(result.lateStarts()).isEqualTo(2);
  }

  @Test
  void aLateSchedulerCatchesUpWithoutDrifting() {
    final FakeClock clock = new FakeClock();
    final OpenLoop loop = new OpenLoop(clock, 1, client -> clock.advance(25 * MS), Runnable::run);

    final StepResult result = loop.runStep(100, Duration.ZERO, Duration.ofMillis(100));

    // every arrival is still issued (the count comes from the schedule, not from the pace), arrival i starts at 25i and ends
    // at 25(i+1), so its latency is 25 + 15i ms
    assertThat(result.arrivals()).isEqualTo(10);
    assertThat(result.dropped()).isZero();
    assertThat(result.lateStarts()).isEqualTo(9);
    assertThat(result.p50Ms()).isEqualTo(85.0);
    assertThat(result.p99Ms()).isEqualTo(160.0);
  }

  @Test
  void aBusyPoolDrops() {
    final FakeClock clock = new FakeClock();
    final AtomicInteger completed = new AtomicInteger();
    final OpenLoop loop = new OpenLoop(clock, 2, client -> completed.incrementAndGet(), clock.delayingExecutor(n -> 25 * MS));

    final StepResult result = loop.runStep(100, Duration.ZERO, Duration.ofMillis(100));

    // arrivals every 10 ms, each holding a client for 25 ms: the ones due at 20, 50 and 80 ms find both clients busy
    assertThat(result.arrivals()).isEqualTo(10);
    assertThat(result.dropped()).isEqualTo(3);
    assertThat(completed.get()).isEqualTo(7);
    assertThat(result.errors()).isZero();
    assertThat(result.p50Ms()).isEqualTo(25.0);
    assertThat(result.p99Ms()).isEqualTo(25.0);
    assertThat(result.broke()).isTrue();
    assertThat(result.why()).contains("dropped");
  }

  @Test
  void errorsAreCountedPerStep() {
    final FakeClock clock = new FakeClock();
    final AtomicBoolean failing = new AtomicBoolean(true);
    final AtomicInteger calls = new AtomicInteger();
    final OpenLoop loop = new OpenLoop(clock, 1, client -> {
      if (failing.get() && calls.getAndIncrement() % 10 == 0)
        throw new IllegalStateException("boom");
    }, Runnable::run);

    final StepResult first = loop.runStep(100, Duration.ZERO, Duration.ofSeconds(1));
    assertThat(first.arrivals()).isEqualTo(100);
    assertThat(first.errors()).isEqualTo(10);
    assertThat(first.dropped()).as("a failing operation still gives its client back").isZero();
    assertThat(first.broke()).isTrue();
    assertThat(first.why()).contains("errors");

    failing.set(false);
    final StepResult second = loop.runStep(100, Duration.ZERO, Duration.ofSeconds(1));
    assertThat(second.arrivals()).isEqualTo(100);
    assertThat(second.errors()).isZero();
    assertThat(second.dropped()).isZero();
    assertThat(second.broke()).isFalse();
  }

  @Test
  void anErrorThrownByTheOperationIsAnError() {
    // an Error (AssertionError, NoClassDefFoundError, OutOfMemoryError) must not vanish: a step whose every request threw one
    // would otherwise count no error, no drop and no latency, never break and climb to the ceiling
    final FakeClock clock = new FakeClock();
    final OpenLoop loop = new OpenLoop(clock, 1, client -> {
      throw new AssertionError("boom");
    }, Runnable::run);
    final StepResult step = loop.runStep(100, Duration.ZERO, Duration.ofSeconds(1));
    assertThat(step.arrivals()).isEqualTo(100);
    assertThat(step.errors()).isEqualTo(100);
    assertThat(step.dropped()).as("a request that threw an Error still gives its client back").isZero();
    assertThat(loop.busyClients()).isZero();
    assertThat(step.broke()).isTrue();
    assertThat(step.why()).contains("errors 100 of 100");
  }

  @Test
  void breakRule() {
    assertThat(new StepResult(100, 1000, 0, 0, 0, 49.9, 1).broke()).isFalse();
    assertThat(new StepResult(100, 1000, 0, 0, 0, 49.9, 1).why()).isEmpty();
    assertThat(new StepResult(100, 1000, 0, 0, 0, 50.0, 1).broke()).isFalse();
    assertThat(new StepResult(100, 1000, 0, 0, 0, 50.1, 1).broke()).isTrue();
    assertThat(new StepResult(100, 1000, 0, 0, 0, 50.1, 1).why()).contains("p99");

    assertThat(new StepResult(100, 1000, 1, 0, 0, 1, 1).broke()).isFalse();
    assertThat(new StepResult(100, 1000, 2, 0, 0, 1, 1).broke()).isTrue();
    assertThat(new StepResult(100, 1000, 2, 0, 0, 1, 1).why()).contains("errors");

    assertThat(new StepResult(100, 1000, 0, 1, 0, 1, 1).broke()).isTrue();
    assertThat(new StepResult(100, 1000, 0, 1, 0, 1, 1).why()).contains("dropped");

    // late starts are a client-capacity signal for the caller, not a server break
    assertThat(new StepResult(100, 1000, 0, 0, 500, 1, 1).broke()).isFalse();
  }

  @Test
  void warmTrafficIsNotCounted() {
    // every warm arrival fails, the last one (due 490 ms, 30 ms long) only at 520 ms, inside the counted window; counted
    // arrivals complete at 505, 515, 525 ms ... and never fail: none of the warm errors reach the step
    final FakeClock clock = new FakeClock();
    final AtomicInteger warmFailures = new AtomicInteger();
    final OpenLoop failingWarm = new OpenLoop(clock, 4, client -> {
      if (clock.nanos() < 500 * MS || clock.nanos() == 520 * MS) {
        warmFailures.incrementAndGet();
        throw new IllegalStateException("warm");
      }
    }, clock.delayingExecutor(n -> n == 49 ? 30 * MS : 5 * MS));

    final StepResult errors = failingWarm.runStep(100, Duration.ofMillis(500), Duration.ofSeconds(1));
    assertThat(warmFailures.get()).isEqualTo(50);
    assertThat(errors.arrivals()).isEqualTo(100);
    assertThat(errors.errors()).isZero();
    assertThat(errors.dropped()).isZero();

    // warm arrivals are slow (40 ms), counted ones fast (5 ms): none of the slow latencies reach the step's percentiles
    final FakeClock clock2 = new FakeClock();
    final OpenLoop slowWarm = new OpenLoop(clock2, 16, client -> {
    }, clock2.delayingExecutor(n -> n < 50 ? 40 * MS : 5 * MS));

    final StepResult latency = slowWarm.runStep(100, Duration.ofMillis(500), Duration.ofSeconds(1));
    assertThat(latency.arrivals()).isEqualTo(100);
    assertThat(latency.dropped()).isZero();
    assertThat(latency.p50Ms()).isEqualTo(5.0);
    assertThat(latency.p99Ms()).isEqualTo(5.0);
  }

  @Test
  void aRequestHungPastTheDrainDeadlineBreaksTheStepAndHoldsNoClientIntoTheNext() {
    final FakeClock clock = new FakeClock();
    final OpenLoop loop = new OpenLoop(clock, 4, client -> {
    }, clock.delayingExecutor(n -> n == 0 ? 40_000 * MS : 5 * MS));

    final StepResult hung = loop.runStep(100, Duration.ZERO, Duration.ofSeconds(1));
    assertThat(hung.unfinished()).isEqualTo(1);
    assertThat(hung.errors()).isZero();
    assertThat(hung.dropped()).isZero();
    assertThat(hung.broke()).isTrue();
    assertThat(hung.why()).contains("1 arrivals unfinished after 30 s");
    assertThat(loop.busyClients()).as("the hung request still holds its client").isEqualTo(1);

    // the next step waits for the hung request to give its client back (at 40 s) and starts with the full pool
    final StepResult next = loop.runStep(100, Duration.ZERO, Duration.ofSeconds(1));
    assertThat(clock.nanos()).as("step 2 ran after the hung request ended at 40 s").isGreaterThanOrEqualTo(40_990 * MS);
    assertThat(next.arrivals()).isEqualTo(100);
    assertThat(next.dropped()).isZero();
    assertThat(next.unfinished()).isZero();
    assertThat(next.broke()).isFalse();
    assertThat(loop.busyClients()).isZero();
  }

  @Test
  void aHungWarmRequestBreaksTheStep() {
    final FakeClock clock = new FakeClock();
    final OpenLoop loop = new OpenLoop(clock, 4, client -> {
    }, clock.delayingExecutor(n -> n == 0 ? 40_000 * MS : 5 * MS));

    final StepResult hung = loop.runStep(100, Duration.ofMillis(500), Duration.ofSeconds(1));
    assertThat(hung.unfinished()).isEqualTo(1);
    assertThat(hung.errors()).isZero();
    assertThat(hung.broke()).isTrue();
  }

  @Test
  void aRejectedSubmissionIsADropAndFreesItsClient() {
    final FakeClock clock = new FakeClock();
    final AtomicInteger submissions = new AtomicInteger();
    final AtomicInteger completed = new AtomicInteger();
    final Executor rejectingFourth = task -> {
      if (submissions.getAndIncrement() == 3)
        throw new RejectedExecutionException("full");
      task.run();
    };
    final OpenLoop loop = new OpenLoop(clock, 1, client -> completed.incrementAndGet(), rejectingFourth);

    final StepResult result = loop.runStep(100, Duration.ZERO, Duration.ofMillis(100));
    assertThat(result.arrivals()).isEqualTo(10);
    assertThat(result.dropped()).isEqualTo(1);
    assertThat(completed.get()).as("the only client serves every arrival after the rejected one").isEqualTo(9);
    assertThat(result.errors()).isZero();
    assertThat(loop.busyClients()).isZero();
  }

  @Test
  void aFailingExecutorPropagatesAndReleasesItsClient() {
    final FakeClock clock = new FakeClock();
    final AtomicBoolean broken = new AtomicBoolean(true);
    final Executor executor = task -> {
      if (broken.get())
        throw new IllegalStateException("executor down");
      task.run();
    };
    final OpenLoop loop = new OpenLoop(clock, 1, client -> {
    }, executor);

    assertThatThrownBy(() -> loop.runStep(100, Duration.ZERO, Duration.ofMillis(100))).isInstanceOf(IllegalStateException.class)
        .hasMessage("executor down");
    assertThat(loop.busyClients()).isZero();

    broken.set(false);
    final long before = clock.nanos();
    final StepResult result = loop.runStep(100, Duration.ZERO, Duration.ofMillis(100));
    assertThat(clock.nanos() - before).as("no wait for a phantom in-flight request").isLessThan(100 * MS);
    assertThat(result.dropped()).isZero();
    assertThat(result.broke()).isFalse();
  }

  @Test
  void anInterruptWhileDrainingCancelsTheStep() {
    final FakeClock clock = new FakeClock();
    final AtomicInteger calls = new AtomicInteger();
    // arrival 0 (due 0, 15 ms) completes during the drain and interrupts; arrival 1 (due 10 ms, 1 s) is still in flight
    final OpenLoop loop = new OpenLoop(clock, 2, client -> {
      if (calls.getAndIncrement() == 0)
        Thread.currentThread().interrupt();
    }, clock.delayingExecutor(n -> n == 0 ? 15 * MS : 1_000 * MS));

    try {
      assertThatThrownBy(() -> loop.runStep(100, Duration.ZERO, Duration.ofMillis(20))).isInstanceOf(CancellationException.class);
      assertThat(clock.nanos()).as("cancelled at once, not after the drain deadline").isLessThan(1_000 * MS);
    } finally {
      Thread.interrupted();
    }
  }

  /**
   * Real threads and the system clock: asserts counts only, never latency or time.
   */
  @Test
  void realThreadsSmoke() {
    final ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      final AtomicInteger completed = new AtomicInteger();
      final OpenLoop loop = new OpenLoop(OpenLoop.SYSTEM, 4, client -> completed.incrementAndGet(), pool);

      final StepResult result = loop.runStep(200, Duration.ZERO, Duration.ofMillis(250));
      assertThat(result.arrivals()).isEqualTo(50);
      assertThat(result.errors()).isZero();
      assertThat(result.unfinished()).isZero();
      assertThat(completed.get() + result.dropped() + result.errors()).isEqualTo(result.arrivals());
      assertThat(loop.busyClients()).isZero();
    } finally {
      pool.shutdownNow();
    }
  }
}
