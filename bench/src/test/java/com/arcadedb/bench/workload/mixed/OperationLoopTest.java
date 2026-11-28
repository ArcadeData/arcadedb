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
package com.arcadedb.bench.workload.mixed;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.remote.RemoteException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class OperationLoopTest {
  @Test
  void aConflictIsRetriedAndCountsOnce() {
    final AtomicInteger calls = new AtomicInteger();
    final AtomicBoolean stop = new AtomicBoolean();
    final OperationLoop loop = new OperationLoop("w", () -> {
      if (calls.incrementAndGet() <= 2)
        throw new ConcurrentModificationException("conflict");
      stop.set(true);
    }, 0, stop, new CommitGaps(0), false, 16);
    loop.run();
    assertThat(loop.completed()).isEqualTo(1);
    assertThat(loop.retries()).isEqualTo(2);
    assertThat(loop.failure()).isNull();
    assertThat(loop.latencies().count()).isEqualTo(1);
  }

  @Test
  void aRemoteConflictIsRetried() {
    final AtomicInteger calls = new AtomicInteger();
    final AtomicBoolean stop = new AtomicBoolean();
    final OperationLoop loop = new OperationLoop("w", () -> {
      if (calls.incrementAndGet() == 1)
        throw new RemoteException("com.arcadedb.exception.ConcurrentModificationException: boom");
      stop.set(true);
    }, 0, stop, new CommitGaps(0), false, 16);
    loop.run();
    assertThat(loop.completed()).isEqualTo(1);
    assertThat(loop.retries()).isEqualTo(1);
    assertThat(loop.failure()).isNull();
  }

  @Test
  void aFourthConflictInARowIsAFailure() {
    final AtomicInteger calls = new AtomicInteger();
    final NeedRetryException error = new NeedRetryException("always");
    final OperationLoop loop = new OperationLoop("w", () -> {
      calls.incrementAndGet();
      throw error;
    }, 0, new AtomicBoolean(), new CommitGaps(0), false, 16);
    loop.run();
    assertThat(loop.failure()).isSameAs(error);
    assertThat(calls.get()).isEqualTo(4);
    assertThat(loop.retries()).isEqualTo(3);
    assertThat(loop.completed()).isZero();
  }

  @Test
  void aNonRetryableErrorEndsTheLoopAtOnce() {
    final IllegalStateException error = new IllegalStateException("bad");
    final OperationLoop loop = new OperationLoop("w", () -> {
      throw error;
    }, 0, new AtomicBoolean(), new CommitGaps(0), false, 16);
    loop.run();
    assertThat(loop.failure()).isSameAs(error);
    assertThat(loop.completed()).isZero();
    assertThat(loop.retries()).isZero();
  }

  @Test
  void aCommittingOperationReportsEachSuccessToTheGaps() {
    final AtomicInteger calls = new AtomicInteger();
    final AtomicBoolean stop = new AtomicBoolean();
    final long start = System.nanoTime();
    final CommitGaps gaps = new CommitGaps(start);
    final OperationLoop loop = new OperationLoop("w", () -> {
      if (calls.incrementAndGet() == 3)
        stop.set(true);
    }, 0, stop, gaps, true, 16);
    loop.run();
    assertThat(loop.completed()).isEqualTo(3);
    // each commit is "now", so the trailing gap from the last commit to a far end is the whole remainder
    final long end = System.nanoTime() + 1_000_000_000L;
    assertThat(gaps.longestGapNanos(end)).isGreaterThan(900_000_000L);
    assertThat(gaps.longestGapNanos(System.nanoTime())).isLessThan(1_000_000_000L);
  }

  @Test
  void aNonCommittingOperationLeavesTheGapsAlone() {
    final AtomicBoolean stop = new AtomicBoolean();
    final CommitGaps gaps = new CommitGaps(0);
    final OperationLoop loop = new OperationLoop("w", () -> stop.set(true), 0, stop, gaps, false, 16);
    loop.run();
    assertThat(loop.completed()).isEqualTo(1);
    assertThat(gaps.longestGapNanos(500)).isEqualTo(500);
  }

  @Test
  void anInterruptDuringThePauseEndsTheLoopCleanly() throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    final OperationLoop loop = new OperationLoop("w", calls::incrementAndGet, 60_000, new AtomicBoolean(), new CommitGaps(0), false, 16);
    final Thread thread = new Thread(loop);
    thread.start();
    while (loop.completed() == 0)
      Thread.sleep(5);
    thread.interrupt();
    thread.join(10_000);
    assertThat(thread.isAlive()).isFalse();
    assertThat(loop.failure()).isNull();
    assertThat(loop.completed()).isEqualTo(1);
  }

  @Test
  void anInterruptInsideTheOperationAfterStopIsNotAFailure() {
    final AtomicBoolean stop = new AtomicBoolean();
    final OperationLoop loop = new OperationLoop("w", () -> {
      stop.set(true);
      throw new RemoteException("wrapped", new InterruptedException("hung"));
    }, 0, stop, new CommitGaps(0), false, 16);
    loop.run();
    assertThat(Thread.interrupted()).isTrue();
    assertThat(loop.failure()).isNull();
    assertThat(loop.completed()).isZero();
  }

  @Test
  void anInterruptInsideTheOperationWithoutStopIsAFailure() {
    final InterruptedException error = new InterruptedException("unexpected");
    final OperationLoop loop = new OperationLoop("w", () -> {
      throw error;
    }, 0, new AtomicBoolean(), new CommitGaps(0), false, 16);
    loop.run();
    assertThat(loop.failure()).isSameAs(error);
  }

  // the pause is a minute: a loop that slept it out would still be alive when the (generous) join gives up
  @Test
  void aStopDuringThePauseEndsTheLoopWithoutSleepingItOut() throws InterruptedException {
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicInteger calls = new AtomicInteger();
    final OperationLoop loop = new OperationLoop("w", calls::incrementAndGet, 60_000, stop, new CommitGaps(0), false, 16);
    final Thread thread = new Thread(loop);
    thread.setDaemon(true);
    thread.start();
    while (calls.get() == 0)
      Thread.sleep(5);
    stop.set(true);
    thread.join(10_000);
    assertThat(thread.isAlive()).isFalse();
    assertThat(loop.completed()).isEqualTo(1);
    assertThat(loop.failure()).isNull();
  }
}
