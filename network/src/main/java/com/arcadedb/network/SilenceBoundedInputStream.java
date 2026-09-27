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
 */
public class SilenceBoundedInputStream extends FilterInputStream {
  /** Schedules one task; the returned {@link Runnable} cancels it and must tolerate running after it fired. */
  @FunctionalInterface
  public interface Timer {
    Runnable schedule(Runnable task, long delayMs);
  }

  private final    long    timeoutMs;
  private final    Timer   timer;
  private volatile boolean expired;

  public SilenceBoundedInputStream(final InputStream in, final long timeoutMs, final Timer timer) {
    super(in);
    this.timeoutMs = timeoutMs;
    this.timer = timer;
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
    final Runnable cancel = timer.schedule(() -> {
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
    }, timeoutMs);
    return () -> {
      if (armed.compareAndSet(true, false))
        cancel.run();
    };
  }

  private HttpTimeoutException timedOut(final IOException cause) {
    final HttpTimeoutException e = new HttpTimeoutException("No data received for " + timeoutMs + "ms");
    if (cause != null)
      e.initCause(cause);
    return e;
  }
}
