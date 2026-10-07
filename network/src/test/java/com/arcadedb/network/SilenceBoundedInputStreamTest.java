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
package com.arcadedb.network;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SilenceBoundedInputStream} driven by a hand-cranked timer, so each firing happens exactly when the test says.
 */
class SilenceBoundedInputStreamTest {

  private static final long BUDGET_MS = 1_000L;

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void aSilentReadIsReleasedAsATimeout() throws Exception {
    final ManualTimer timer = new ManualTimer();
    final BlockingStream source = new BlockingStream();
    final SilenceBoundedInputStream stream = new SilenceBoundedInputStream(source, BUDGET_MS, timer);

    final CompletableFuture<Integer> read = CompletableFuture.supplyAsync(() -> readOrRethrow(stream));
    source.awaitReading();
    timer.fireNext();

    assertThatThrownBy(read::join).cause().cause().isInstanceOf(HttpTimeoutException.class);
    assertThat(stream.hasExpired()).isTrue();
  }

  /** Activity while the read waits re-arms the timer; the read is released only once the activity stands still too. */
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void activityKeepsAWaitingReadAliveUntilItStops() throws Exception {
    final ManualTimer timer = new ManualTimer();
    final BlockingStream source = new BlockingStream();
    final AtomicLong activity = new AtomicLong();
    final SilenceBoundedInputStream stream = new SilenceBoundedInputStream(source, BUDGET_MS, timer, activity::get);

    final CompletableFuture<Integer> read = CompletableFuture.supplyAsync(() -> readOrRethrow(stream));
    source.awaitReading();

    activity.incrementAndGet();
    timer.fireNext();
    assertThat(source.closed).as("the exchange moved: re-armed, not fired").isFalse();
    assertThat(timer.pending()).isEqualTo(1);

    timer.fireNext();
    assertThatThrownBy(read::join).cause().cause().isInstanceOf(HttpTimeoutException.class);
    assertThat(source.closed).isTrue();
  }

  /**
   * A read that returns while its timer is re-arming: the new task, scheduled after the read cancelled the old one, is
   * cancelled too, not left queued for a whole budget.
   */
  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void aReArmRacingTheReadsReturnLeavesNoTaskQueued() throws Exception {
    final ManualTimer timer = new ManualTimer();
    final BlockingStream source = new BlockingStream();
    final CountDownLatch readReturned = new CountDownLatch(1);
    final AtomicInteger samples = new AtomicInteger();
    // The first sample is taken when the read arms, the second by the firing timer once it has seen the read still
    // armed: that is where the read is let return, and the firing waits for it before it re-arms.
    final SilenceBoundedInputStream stream = new SilenceBoundedInputStream(source, BUDGET_MS, timer, () -> {
      if (samples.incrementAndGet() == 2) {
        source.release('x');
        try {
          readReturned.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return samples.get();
    });

    final CompletableFuture<Integer> read = CompletableFuture.supplyAsync(() -> {
      final int b = readOrRethrow(stream);
      readReturned.countDown();
      return b;
    });
    source.awaitReading();
    timer.fireNext();

    assertThat(read.join()).isEqualTo('x');
    assertThat(stream.hasExpired()).isFalse();
    assertThat(timer.pending()).as("the re-armed task is cancelled, not orphaned").isZero();
  }

  private static int readOrRethrow(final InputStream stream) {
    try {
      return stream.read();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Tasks run only when the test fires them; a cancelled task is dropped. */
  private static final class ManualTimer implements SilenceBoundedInputStream.Timer {
    private final List<Runnable> tasks = new ArrayList<>();

    @Override
    public synchronized Runnable schedule(final Runnable task, final long delayMs) {
      tasks.add(task);
      return () -> {
        synchronized (this) {
          tasks.remove(task);
        }
      };
    }

    void fireNext() {
      final Runnable task;
      synchronized (this) {
        assertThat(tasks).as("a task to fire").isNotEmpty();
        task = tasks.removeFirst();
      }
      task.run();
    }

    synchronized int pending() {
      return tasks.size();
    }
  }

  /** Blocks every read until a byte is released or the stream is closed. */
  private static final class BlockingStream extends InputStream {
    private final    CountDownLatch reading  = new CountDownLatch(1);
    private final    CountDownLatch released = new CountDownLatch(1);
    private volatile int            next     = -1;
    private volatile boolean        closed;

    void awaitReading() throws InterruptedException {
      reading.await();
    }

    void release(final int b) {
      next = b;
      released.countDown();
    }

    @Override
    public int read() throws IOException {
      reading.countDown();
      try {
        released.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      if (closed)
        throw new IOException("closed");
      return next;
    }

    @Override
    public void close() {
      closed = true;
      released.countDown();
    }
  }
}
