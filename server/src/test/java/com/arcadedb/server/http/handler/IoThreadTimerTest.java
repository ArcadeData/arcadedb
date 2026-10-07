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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9439: a delayed task added to an XNIO I/O thread from another thread with a plain
 * {@code executeAfter} is lost when it lands while the I/O thread sits between computing its next wait and starting it:
 * XNIO wakes the selector only when it is already polling, and re-checks only the immediate queue once it is. The write
 * watchdog of every streamed response and the NDJSON keep-alive were armed that way from worker threads, so a watchdog
 * could never fire and leave a worker blocked on a client that stopped reading. {@link IoThreadTimer} hands the timer to
 * the I/O thread instead.
 * <p>
 * Runs against a real XNIO worker with ONE I/O thread and nothing else on it, which is the case where a lost timer is
 * lost for good: no other activity ever wakes the selector again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class IoThreadTimerTest {
  /**
   * How long a 1 ms timer may take before it counts as lost. A hang detector, not a latency bound: a timer that is seen
   * fires within milliseconds, one that is lost never does.
   */
  private static final long LOST_AFTER_MS = 5_000;

  private XnioWorker   worker;
  private XnioIoThread ioThread;

  @BeforeEach
  void startWorker() throws Exception {
    worker = Xnio.getInstance().createWorker(OptionMap.builder().set(Options.WORKER_IO_THREADS, 1)
        .set(Options.WORKER_TASK_CORE_THREADS, 1).set(Options.WORKER_TASK_MAX_THREADS, 1).getMap());
    ioThread = worker.getIoThread(0);
  }

  @AfterEach
  void stopWorker() throws InterruptedException {
    worker.shutdownNow();
    worker.awaitTermination(10, TimeUnit.SECONDS);
  }

  /**
   * Schedules each timer the instant the I/O thread finishes a task: that is when it computes its next wait and then
   * parks in the selector, the window a direct {@code executeAfter} from this thread falls into. With the direct call
   * this loses timers within the first few hundred rounds on a laptop; through {@link IoThreadTimer} none is lost.
   */
  @Test
  void aTimerScheduledFromAnotherThreadIsNeverLostToTheSelectorWakeupRace() throws InterruptedException {
    for (int round = 0; round < 2_000; round++) {
      final AtomicBoolean taskDone = new AtomicBoolean();
      ioThread.execute(() -> taskDone.set(true));
      while (!taskDone.get())
        Thread.onSpinWait();

      final CountDownLatch fired = new CountDownLatch(1);
      IoThreadTimer.schedule(ioThread, fired::countDown, 1);
      assertThat(fired.await(LOST_AFTER_MS, TimeUnit.MILLISECONDS)).as("timer of round %d was lost", round).isTrue();
    }
  }

  @Test
  void aCancelThatRacesTheHandOverStillCancels() throws InterruptedException {
    // THE I/O THREAD IS BUSY, SO THE HAND-OVER IS STILL QUEUED WHEN THE CANCEL RUNS
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch busy = new CountDownLatch(1);
    ioThread.execute(() -> {
      busy.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    assertThat(busy.await(LOST_AFTER_MS, TimeUnit.MILLISECONDS)).isTrue();

    final AtomicInteger cancelledRuns = new AtomicInteger();
    final Runnable cancel = IoThreadTimer.schedule(ioThread, cancelledRuns::incrementAndGet, 1);
    cancel.run();
    release.countDown();

    assertNeverFires(cancelledRuns, 1);
  }

  @Test
  void aCancelAfterTheHandOverRemovesTheTimer() throws InterruptedException {
    final AtomicInteger cancelledRuns = new AtomicInteger();
    final Runnable cancel = IoThreadTimer.schedule(ioThread, cancelledRuns::incrementAndGet, 200);
    // WAIT FOR THE HAND-OVER: ANY TASK QUEUED AFTER IT RUNS AFTER IT
    final CountDownLatch handedOver = new CountDownLatch(1);
    ioThread.execute(handedOver::countDown);
    assertThat(handedOver.await(LOST_AFTER_MS, TimeUnit.MILLISECONDS)).isTrue();

    cancel.run();
    // CANCELLING AGAIN, OR AFTER THE FACT, IS HARMLESS
    cancel.run();

    assertNeverFires(cancelledRuns, 200);
  }

  @Test
  void aTimerScheduledOnTheIoThreadItselfFires() throws InterruptedException {
    final CountDownLatch fired = new CountDownLatch(1);
    ioThread.execute(() -> IoThreadTimer.schedule(ioThread, fired::countDown, 1));
    assertThat(fired.await(LOST_AFTER_MS, TimeUnit.MILLISECONDS)).isTrue();

    // AND IT CAN BE CANCELLED FROM THERE TOO
    final AtomicInteger cancelledRuns = new AtomicInteger();
    final CountDownLatch scheduled = new CountDownLatch(1);
    ioThread.execute(() -> {
      IoThreadTimer.schedule(ioThread, cancelledRuns::incrementAndGet, 1).run();
      scheduled.countDown();
    });
    assertThat(scheduled.await(LOST_AFTER_MS, TimeUnit.MILLISECONDS)).isTrue();
    assertNeverFires(cancelledRuns, 1);
  }

  /**
   * Proves a cancelled timer of {@code delayMs} never ran without sleeping on a guess: a sentinel timer due later is
   * scheduled the same way and awaited, and the I/O thread runs its delayed tasks in deadline order, so once the
   * sentinel fired the cancelled timer would have fired before it.
   */
  private void assertNeverFires(final AtomicInteger cancelledRuns, final long delayMs) throws InterruptedException {
    final CountDownLatch sentinel = new CountDownLatch(1);
    IoThreadTimer.schedule(ioThread, sentinel::countDown, delayMs + 50);
    assertThat(sentinel.await(LOST_AFTER_MS, TimeUnit.MILLISECONDS)).isTrue();
    assertThat(cancelledRuns.get()).isZero();
  }
}
