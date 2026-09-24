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

import com.arcadedb.server.http.handler.PostBatchHandler.ReadBoundedInputStream;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7738: the follower's relay of a streamed {@code /api/v1/batch} answer read the leader's body through
 * {@code BodyHandlers.ofInputStream}, which has no per-read timeout, so a leader that sent its headers and then
 * stalled parked the follower's worker thread indefinitely. {@link ReadBoundedInputStream} bounds every read.
 * <p>
 * The end-to-end reachability of this class from the relay is {@code Issue7738StreamingBatchRelayReadDeadlineTest}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class ReadBoundedInputStreamTest {

  private static final long BUDGET_MS        = 250;
  /** How long the blocked read gives up on its own when nothing releases it: the unbounded case, observable. */
  private static final long PEER_GIVES_UP_MS = 20_000;
  /** Separates "the timer released the read" from "nothing did". */
  private static final long SEPARATION_MS    = 5_000;

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  @AfterEach
  void stopScheduler() {
    scheduler.shutdownNow();
  }

  @Test
  void aReadThatNeverReturnsIsReleasedAsATimeout() {
    for (final ThrowingRead call : new ThrowingRead[] { InputStream::read, in -> in.read(new byte[64]),
        InputStream::readAllBytes }) {
      final ReadBoundedInputStream in = new ReadBoundedInputStream(new SilentLeaderBody(false), BUDGET_MS,
          timer(new AtomicInteger()));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> call.on(in))
          .as("a stalled leader is reported as a timeout, distinguishable from a dropped connection")
          .isInstanceOf(HttpTimeoutException.class)
          .hasMessageContaining(BUDGET_MS + "ms");
      watch.assertGaveUpWithin(SEPARATION_MS, "a read bounded by the timer from one held until the leader goes away");
      assertThat(in.hasExpired()).isTrue();
    }
  }

  /** Some JDKs answer a read on a closed response stream with an end of stream rather than an exception. */
  @Test
  void aReleasedReadThatSeesEndOfStreamIsStillATimeout() {
    final ReadBoundedInputStream in = new ReadBoundedInputStream(new SilentLeaderBody(true), BUDGET_MS,
        timer(new AtomicInteger()));

    assertThatThrownBy(in::read)
        .as("an end of stream caused by the timer must not pass for the leader finishing its answer")
        .isInstanceOf(HttpTimeoutException.class);
  }

  /** The bound is on silence, not on the length of the answer: reads that return leave no timer behind. */
  @Test
  void readsThatReturnDisarmTheirTimer() throws Exception {
    final AtomicInteger fired = new AtomicInteger();
    final ReadBoundedInputStream in = new ReadBoundedInputStream(
        new ByteArrayInputStream("{\"type\":\"progress\"}\n".getBytes(StandardCharsets.UTF_8)), BUDGET_MS, timer(fired));

    assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("{\"type\":\"progress\"}\n");
    assertThat(in.read()).isEqualTo(-1);

    Thread.sleep(BUDGET_MS * 4);
    assertThat(fired.get()).as("a read that completed must leave no timer behind").isZero();
    assertThat(in.hasExpired()).isFalse();
  }

  /** A failure that is not the timer's is surfaced as it is, not relabelled as a timeout. */
  @Test
  void aFailureOfTheLeaderIsNotReportedAsATimeout() {
    final ReadBoundedInputStream in = new ReadBoundedInputStream(new InputStream() {
      @Override
      public int read() throws IOException {
        throw new IOException("connection reset");
      }
    }, BUDGET_MS, timer(new AtomicInteger()));

    assertThatThrownBy(in::read).isInstanceOf(IOException.class).isNotInstanceOf(HttpTimeoutException.class)
        .hasMessage("connection reset");
    assertThat(in.hasExpired()).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * A leader body that sends nothing: every read parks until the stream is closed under it, then fails with
   * "closed" - or returns end of stream - which is what the JDK's {@code HttpResponseInputStream} does.
   */
  private static final class SilentLeaderBody extends InputStream {
    private final CountDownLatch closed = new CountDownLatch(1);
    private final boolean        endOfStreamOnClose;

    SilentLeaderBody(final boolean endOfStreamOnClose) {
      this.endOfStreamOnClose = endOfStreamOnClose;
    }

    @Override
    public int read() throws IOException {
      return block();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      return block();
    }

    @Override
    public void close() {
      closed.countDown();
    }

    private int block() throws IOException {
      try {
        if (!closed.await(PEER_GIVES_UP_MS, TimeUnit.MILLISECONDS))
          // Nothing released the read: report it as the leader finally going away, which is not a timeout.
          throw new IOException("the leader went away");
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (endOfStreamOnClose)
        return -1;
      throw new IOException("closed");
    }
  }

  private ReadBoundedInputStream.Timer timer(final AtomicInteger fired) {
    return (task, delayMs) -> {
      final ScheduledFuture<?> scheduled = scheduler.schedule(() -> {
        fired.incrementAndGet();
        task.run();
      }, delayMs, TimeUnit.MILLISECONDS);
      return () -> scheduled.cancel(false);
    };
  }

  @FunctionalInterface
  private interface ThrowingRead {
    void on(InputStream in) throws IOException;
  }
}
