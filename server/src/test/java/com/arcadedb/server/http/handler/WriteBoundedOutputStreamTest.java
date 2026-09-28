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

import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7381, part 2: the streamed answer of {@code POST /api/v1/batch} is written while the request body is
 * still being read and grows with the size of the load, so a client that uploads everything before reading
 * anything can fill the socket buffers between the two and leave the server blocked inside a response write -
 * which stops it reading the upload as well. Nothing then completes, and the worker thread is held for as long
 * as the client keeps the connection open.
 * <p>
 * The bound is not a cap on the number or the rate of progress lines: neither knows how large the socket buffers
 * are, so neither can promise the stall will not happen. It is a watchdog that turns an indefinite block into a
 * bounded one, the write-side counterpart of the read-side watchdog of issue #5470.
 * <p>
 * Tested here rather than through a socket because the moment the stall begins is a function of buffer sizes the
 * kernel picks, which a test cannot set on both ends; the end-to-end reachability of this class from the
 * endpoint is {@code Issue7381BatchContractIT}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class WriteBoundedOutputStreamTest {

  /** The watchdog budget under test. Short: nothing here waits on a real socket. */
  private static final long BUDGET_MS = 250;
  /**
   * How long the blocked write gives up on its own when NOTHING releases it. It stands in for the peer finally
   * going away, and it is what makes the unbounded case observable as a failed assertion rather than as a test
   * that never ends - a {@code @Timeout} cannot interrupt a thread parked in {@code await()}.
   */
  private static final long PEER_GIVES_UP_MS = 20_000;
  /**
   * Separates "the watchdog released the write" from "nothing did". An order of magnitude above the budget and
   * two below the fallback, so neither a loaded runner nor a stalled JVM can make the two outcomes meet.
   */
  private static final long SEPARATION_MS = 5_000;

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  @AfterEach
  void stopScheduler() {
    scheduler.shutdownNow();
  }

  /**
   * The property the whole part-2 fix rests on: a write that cannot make progress is released by the watchdog,
   * so the thread inside it is given back instead of being held until the peer goes away.
   */
  @Test
  void aBlockedWriteIsReleasedByTheWatchdogInsteadOfBeingHeldUntilThePeerGoesAway() throws Exception {
    final CountDownLatch connectionClosed = new CountDownLatch(1);
    final WriteBoundedOutputStream out = new WriteBoundedOutputStream(blockingUntil(connectionClosed),
        watchdogFiringAfter(BUDGET_MS, connectionClosed, new AtomicInteger()));

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(out::flush)
        .as("the write fails as an I/O error, which is what lets the caller report a response that could not be "
            + "written rather than a request body that was truncated")
        .isInstanceOf(IOException.class);
    watch.assertGaveUpWithin(SEPARATION_MS,
        "a write bounded by the watchdog from one held until the peer goes away");
  }

  /**
   * Every call that can reach the response is bounded: a write too large for the stream's own buffer, a flush and a
   * close - the last two also when all they have to hand on is what earlier, unarmed writes buffered.
   */
  @Test
  void theBoundCoversEveryBlockingCallOfTheStream() throws Exception {
    for (final ThrowingCall call : new ThrowingCall[] {
        stream -> stream.write(new byte[WriteBoundedOutputStream.BUFFER_SIZE + 1]),
        OutputStream::flush,
        OutputStream::close,
        stream -> {
          stream.write(7);
          stream.flush();
        },
        stream -> {
          stream.write(new byte[] { 1, 2, 3 });
          stream.close();
        } }) {

      final CountDownLatch connectionClosed = new CountDownLatch(1);
      final WriteBoundedOutputStream out = new WriteBoundedOutputStream(blockingUntil(connectionClosed),
          watchdogFiringAfter(BUDGET_MS, connectionClosed, new AtomicInteger()));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> call.on(out)).isInstanceOf(IOException.class);
      watch.assertGaveUpWithin(SEPARATION_MS,
          "a bounded blocking call of the stream from one held until the peer goes away");
    }
  }

  /**
   * The disarm half, and the reason an ordinary client sees no change at all: a write that returns takes its
   * watchdog with it, so a load whose commits are slower than the budget - the 195-second index compaction of
   * issue #5470 - never has a timer running between two of its lines.
   */
  @Test
  void aWriteThatReturnsDisarmsItsWatchdog() throws Exception {
    final AtomicInteger fired = new AtomicInteger();
    final CountDownLatch neverClosed = new CountDownLatch(1);
    final WriteBoundedOutputStream out = new WriteBoundedOutputStream(OutputStream.nullOutputStream(),
        watchdogFiringAfter(BUDGET_MS, neverClosed, fired));

    out.write("{\"progress\":{}}\n".getBytes());
    out.flush();

    // Well past the budget: a watchdog that was not disarmed would have fired several times over by now.
    assertThat(fired.get())
        .as("a write that completed must leave no timer behind")
        .isZero();
    Thread.sleep(BUDGET_MS * 4);
    assertThat(fired.get())
        .as("and must still leave none once the budget it would have used has elapsed")
        .isZero();
  }

  /**
   * Issue #7806: the NDJSON query encoding writes one row per call. A call that only fills the stream's buffer cannot
   * reach the socket, so it must not pay for a timer either - only the call that hands the buffer on does.
   */
  @Test
  void aWriteThatFitsTheBufferArmsNothing() throws Exception {
    final AtomicInteger armed = new AtomicInteger();
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final WriteBoundedOutputStream out = new WriteBoundedOutputStream(sink, countingWatchdog(armed));

    final byte[] row = "{\"record\":{\"n\":1}}\n".getBytes();
    final int rows = WriteBoundedOutputStream.BUFFER_SIZE / row.length;
    for (int i = 0; i < rows; i++)
      out.write(row);
    assertThat(armed.get()).as("rows that fit the buffer arm no timer").isZero();
    assertThat(sink.size()).as("and reach nothing below it").isZero();

    out.flush();
    assertThat(armed.get()).as("the flush hands the buffer on under a timer").isEqualTo(2);
    assertThat(sink.size()).isEqualTo(rows * row.length);
  }

  /**
   * A write larger than the buffer is handed on in chunks, each armed on its own, so a large row sent to a slow but
   * reading client is bounded per chunk of progress rather than charged its whole transfer time (code review).
   */
  @Test
  void aLargeWriteIsBoundedPerChunk() throws Exception {
    final AtomicInteger armed = new AtomicInteger();
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final WriteBoundedOutputStream out = new WriteBoundedOutputStream(sink, countingWatchdog(armed));

    final byte[] large = new byte[WriteBoundedOutputStream.MAX_BOUNDED_CHUNK * 3 + 17];
    for (int i = 0; i < large.length; i++)
      large[i] = (byte) i;
    out.write(large);

    assertThat(armed.get()).isEqualTo(4);
    assertThat(sink.toByteArray()).isEqualTo(large);
  }

  /** Bytes buffered by writes of every shape come out in order, whole, on close. */
  @Test
  void bufferedBytesAreDeliveredInOrderOnClose() throws Exception {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final ByteArrayOutputStream expected = new ByteArrayOutputStream();
    try (final WriteBoundedOutputStream out = new WriteBoundedOutputStream(sink,
        WriteBoundedOutputStream.WriteWatchdog.NONE)) {
      for (int i = 0; i < 5_000; i++) {
        final byte[] chunk = ("line-" + i + "\n").getBytes();
        out.write(chunk);
        expected.write(chunk);
        out.write('#');
        expected.write('#');
        if (i % 997 == 0) {
          final byte[] big = new byte[WriteBoundedOutputStream.BUFFER_SIZE + i];
          out.write(big);
          expected.write(big);
        }
      }
    }
    assertThat(sink.toByteArray()).isEqualTo(expected.toByteArray());
  }

  /** {@code NONE} is what a non-positive budget configures, and it must not get in the way of anything. */
  @Test
  void theDisabledBoundWritesStraightThrough() throws Exception {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    try (final WriteBoundedOutputStream out = new WriteBoundedOutputStream(sink,
        WriteBoundedOutputStream.WriteWatchdog.NONE)) {
      out.write("line\n".getBytes());
      out.flush();
    }
    assertThat(sink.toString()).isEqualTo("line\n");
  }

  // ---------------------------------------------------------------------------------------------------------

  /**
   * A stream whose every call blocks until the connection is closed under it, then fails - which is what a
   * socket write does when the peer has stopped reading and the watchdog closes the connection.
   */
  private static OutputStream blockingUntil(final CountDownLatch connectionClosed) {
    return new OutputStream() {
      @Override
      public void write(final int b) throws IOException {
        block();
      }

      @Override
      public void write(final byte[] b, final int off, final int len) throws IOException {
        block();
      }

      @Override
      public void flush() throws IOException {
        block();
      }

      @Override
      public void close() throws IOException {
        block();
      }

      private void block() throws IOException {
        try {
          connectionClosed.await(PEER_GIVES_UP_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        throw new IOException("the connection was closed under the write");
      }
    };
  }

  /** The production watchdog's shape: arm schedules the close, the returned handle cancels it. */
  private WriteBoundedOutputStream.WriteWatchdog watchdogFiringAfter(final long budgetMs,
      final CountDownLatch connectionClosed, final AtomicInteger fired) {
    return () -> {
      final ScheduledFuture<?> scheduled = scheduler.schedule(() -> {
        fired.incrementAndGet();
        connectionClosed.countDown();
      }, budgetMs, TimeUnit.MILLISECONDS);
      return () -> scheduled.cancel(false);
    };
  }

  /** Counts how often a timer is armed; never fires. */
  private static WriteBoundedOutputStream.WriteWatchdog countingWatchdog(final AtomicInteger armed) {
    return () -> {
      armed.incrementAndGet();
      return () -> {
      };
    };
  }

  @FunctionalInterface
  private interface ThrowingCall {
    void on(OutputStream stream) throws IOException;
  }
}
