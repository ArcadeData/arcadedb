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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * An {@link InputStream} on which no single read can block longer than a budget: a timer is armed before each read
 * and disarmed as soon as it returns, and when it fires it closes the underlying stream, which is what releases a
 * reader parked on it (issues #7738, #8473).
 * <p>
 * It exists for streamed HTTP answers read with {@code BodyHandlers.ofInputStream()}. That handler has no per-read
 * timeout, the JDK client sets no socket read timeout, and the request timeout ({@code HttpRequest.Builder.timeout})
 * covers different things on different JDKs: on JDK 21-25 it expires only until the response HEADERS arrive, so a
 * peer that answered its status and then stalled parks the reader for as long as it keeps the connection open; on
 * JDK 26+ it covers the whole body, which turns it into a cap on the total length of a stream that is working.
 * <p>
 * The bound is on SILENCE, not on the length of the answer: a peer that keeps emitting data keeps the stream alive
 * however long that is. Only a gap longer than the budget while a read is waiting fails the read. Time the reader
 * spends elsewhere between two reads is not counted.
 * <p>
 * A read released by the timer fails with an {@link HttpTimeoutException} rather than with whatever the closed stream
 * reports ("closed", or an end of stream on some JDKs), so the caller can tell a stalled peer from one that finished
 * or dropped the connection. Arming and firing are settled by one compare-and-set, so a read that returns just as its
 * timer fires is either released or not, never both. The one ambiguity left is a genuine end of stream that arrives in
 * the same instant the timer closes the stream: it is reported as a timeout. That errs on the side of the caller, who
 * sees a failure it can retry rather than a truncated answer passed off as a complete one.
 * <p>
 * An optional ACTIVITY counter extends what counts as "not silent" (issue #9216): a peer answering a request whose body
 * is still being sent to it - a full-duplex upload - may legitimately say nothing for longer than the budget while it
 * takes that body in. A timer that fires while the counter has moved since it was armed is re-armed rather than fired,
 * so the read is given up on only once both the answer and the activity have stood still for a whole budget; that
 * happens between one and two budgets after the last activity.
 */
public class SilenceBoundedInputStream extends FilterInputStream {
  /** Schedules one task; the returned {@link Runnable} cancels it and must tolerate running after it fired. */
  @FunctionalInterface
  public interface Timer {
    Runnable schedule(Runnable task, long delayMs);
  }

  private final    long         timeoutMs;
  private final    Timer        timer;
  private final    LongSupplier activity;
  private volatile boolean      expired;

  public SilenceBoundedInputStream(final InputStream in, final long timeoutMs, final Timer timer) {
    this(in, timeoutMs, timer, null);
  }

  /**
   * @param activity a counter that moves whenever the exchange makes progress other than through this stream, safe to
   *                 read from the timer's thread; {@code null} for none, which bounds the silence of this stream alone
   */
  public SilenceBoundedInputStream(final InputStream in, final long timeoutMs, final Timer timer,
      final LongSupplier activity) {
    super(in);
    this.timeoutMs = timeoutMs;
    this.timer = timer;
    this.activity = activity;
  }

  @Override
  public int read() throws IOException {
    final Runnable disarm = arm();
    final int b;
    try {
      b = in.read();
    } catch (final IOException e) {
      throw expired ? timedOut(e) : e;
    } finally {
      disarm.run();
    }
    if (b < 0 && expired)
      throw timedOut(null);
    return b;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    final Runnable disarm = arm();
    final int n;
    try {
      n = in.read(b, off, len);
    } catch (final IOException e) {
      throw expired ? timedOut(e) : e;
    } finally {
      disarm.run();
    }
    if (n < 0 && expired)
      throw timedOut(null);
    return n;
  }

  /** Whether a read was released by the timer: the stream is closed from then on. */
  public boolean hasExpired() {
    return expired;
  }

  private Runnable arm() {
    final AtomicBoolean armed = new AtomicBoolean(true);
    final AtomicReference<Runnable> cancel = new AtomicReference<>();
    // Plain arrays, not atomics: after the arming thread hands them over through Timer.schedule, only the timer's task
    // touches them, one run at a time, each scheduled by the run before it. The timer's own hand-over (a queue, a lock)
    // orders each run after the schedule that created it, which is all the visibility they need.
    final long[] seen = { activity != null ? activity.getAsLong() : 0L };
    final Runnable[] fire = new Runnable[1];
    fire[0] = () -> {
      if (activity != null && armed.get()) {
        final long now = activity.getAsLong();
        if (now != seen[0]) {
          // Not silent: the exchange moved while this read waited. Waits another budget from here; a read that returns
          // meanwhile finds the newer task through the reference and cancels that one instead.
          seen[0] = now;
          cancel.set(timer.schedule(fire[0], timeoutMs));
          return;
        }
      }
      if (!armed.compareAndSet(true, false))
        // The read returned between this task being dequeued and this line: nothing to release.
        return;
      expired = true;
      try {
        in.close();
      } catch (final Exception ignored) {
        // Whatever close() throws, checked or not, must not escape onto the timer's thread. The reader is released
        // either way, and learns about the expiry from the flag.
      }
    };
    cancel.set(timer.schedule(fire[0], timeoutMs));
    return () -> {
      if (armed.compareAndSet(true, false))
        cancel.get().run();
    };
  }

  private HttpTimeoutException timedOut(final IOException cause) {
    final HttpTimeoutException e = new HttpTimeoutException("No data received for " + timeoutMs + "ms");
    if (cause != null)
      e.initCause(cause);
    return e;
  }
}
