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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.ServerConnection;
import org.xnio.IoUtils;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * An {@link OutputStream} on which no single call can block forever: a watchdog is armed before each one and
 * disarmed as soon as it returns (issues #7381 and #7806).
 * <p>
 * Every streamed response this server writes - the NDJSON answer of {@code POST /api/v1/batch}, the NDJSON query
 * encoding of {@code /query} and {@code /command}, the Server-Sent Events of the AI chat and of the long-running
 * server commands, and a follower's relay of any of them - is written through a blocking {@code write()} on a
 * worker thread, and its size is not bounded by anything the server controls: it grows with the size of a load,
 * of a result set, of a conversation. A client that stops reading (an exception in its own row loop, a paused
 * debugger, a proxy that went away, a client that uploads everything before reading anything) stops draining the
 * socket; once the buffers between the two fill, the server blocks inside that {@code write()}, and the worker
 * thread is held for as long as the client keeps the connection open.
 * <p>
 * The bound does NOT prevent the stall - nothing that does not know the buffer sizes can - it converts it into a
 * failure with a diagnosis: the connection is closed, the blocked call fails with an {@link IOException}, and the
 * worker thread is released.
 * <p>
 * A client that reads while it is written to never arms anything that fires, so nothing about the cadence or the
 * content of a stream changes for it. And the timer exists only while a write is in progress, so a long pause
 * BETWEEN two writes - a commit between two batch progress lines, a slow query producing its next row, a tool call
 * between two chat events - never trips it.
 * <p>
 * Watchdog-injected, so the bound can be tested without a socket whose buffer sizes the test does not control;
 * {@link #of} builds the production one.
 */
public final class WriteBoundedOutputStream extends OutputStream {
  /**
   * Arms the bound for one blocking call. The returned {@link Runnable} disarms it and is always run, so an
   * implementation must tolerate being disarmed after it has already fired.
   */
  @FunctionalInterface
  public interface WriteWatchdog {
    /** No bound at all: what a non-positive budget configures. */
    WriteWatchdog NONE = () -> () -> {
    };

    Runnable arm();
  }

  private final OutputStream  out;
  private final WriteWatchdog watchdog;

  WriteBoundedOutputStream(final OutputStream out, final WriteWatchdog watchdog) {
    this.out = out;
    this.watchdog = watchdog;
  }

  /**
   * The output stream of {@code exchange}, every write of which is bounded by {@code timeoutMs} (issue #7806). The
   * exchange must already be in blocking mode.
   *
   * @param timeoutMs the budget of {@link GlobalConfiguration#SERVER_HTTP_STREAMING_WRITE_TIMEOUT}; not positive
   *                  leaves the writes unbounded
   * @param what      names the response in the warning logged when the bound fires, e.g. "the streamed result of a
   *                  query on database 'x'"
   */
  public static OutputStream of(final HttpServerExchange exchange, final int timeoutMs, final String what) {
    return new WriteBoundedOutputStream(exchange.getOutputStream(), connectionWatchdog(exchange, timeoutMs, what));
  }

  /**
   * The watchdog that bounds one blocking write of a streamed response on this exchange: it closes the connection
   * when the write has made no progress for the budget, which is what turns an indefinite block into an I/O failure
   * the caller can report.
   * <p>
   * Scheduled on the connection's own XNIO thread, which is where Undertow schedules its own read and write
   * timeouts, so this adds no pool and no thread; arming is an insertion into that thread's delay queue and
   * disarming is its removal. It is deliberately NOT Undertow's {@code Options.WRITE_TIMEOUT}: that conduit is
   * installed only at connection open - setting the option later does nothing at all - and it measures the
   * interval BETWEEN two successful writes, so a long server-side pause between two writes (the 195-second index
   * compaction of issue #5470 between two batch progress lines) would kill the connection on the next write that
   * SUCCEEDED.
   * <p>
   * Arming and firing are settled by one compare-and-set, so a write that returns just as its timer fires can
   * never have the connection closed under the request that follows it on the same keep-alive connection:
   * whichever of the two wins the flag, the other does nothing.
   * <p>
   * Closing the CONNECTION is the only lever a streamed response has - a body already on the wire cannot be
   * retracted - and it is the same lever Undertow's own write-timeout conduit pulls. On an HTTP/2 connection that
   * also ends the sibling streams multiplexed on it; the alternative is holding a worker thread for a peer that has
   * stopped reading.
   *
   * @return {@link WriteWatchdog#NONE} when the budget is not positive, which is how the setting switches the
   *         bound off
   */
  static WriteWatchdog connectionWatchdog(final HttpServerExchange exchange, final int timeoutMs, final String what) {
    if (timeoutMs <= 0)
      return WriteWatchdog.NONE;

    final ServerConnection connection = exchange.getConnection();
    final XnioIoThread ioThread = exchange.getIoThread();
    return () -> {
      final AtomicBoolean armed = new AtomicBoolean(true);
      final XnioExecutor.Key key = ioThread.executeAfter(() -> {
        if (!armed.compareAndSet(true, false))
          // The write returned between this task being dequeued and this line. Closing now would take down a
          // connection that is already serving something else.
          return;
        LogManager.instance().log(WriteBoundedOutputStream.class, Level.WARNING,
            "%s could not be written for %,d ms - the client is not reading it - so the connection is closed "
                + "rather than holding a worker thread indefinitely. Raise '%s' to allow a longer block", null,
            capitalize(what), timeoutMs, GlobalConfiguration.SERVER_HTTP_STREAMING_WRITE_TIMEOUT.getKey());
        IoUtils.safeClose(connection);
      }, timeoutMs, TimeUnit.MILLISECONDS);
      return () -> {
        if (armed.compareAndSet(true, false))
          key.remove();
      };
    };
  }

  @Override
  public void write(final int b) throws IOException {
    final Runnable disarm = watchdog.arm();
    try {
      out.write(b);
    } finally {
      disarm.run();
    }
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    final Runnable disarm = watchdog.arm();
    try {
      out.write(b, off, len);
    } finally {
      disarm.run();
    }
  }

  /**
   * The call that actually reaches the socket: {@code UndertowOutputStream} accumulates into a pooled buffer and
   * only writes through when it fills, so on a response of short lines this is where a client that stopped reading
   * blocks the worker thread.
   */
  @Override
  public void flush() throws IOException {
    final Runnable disarm = watchdog.arm();
    try {
      out.flush();
    } finally {
      disarm.run();
    }
  }

  /** Bounded as well: closing the response flushes whatever is still pending, which blocks for the same reason. */
  @Override
  public void close() throws IOException {
    final Runnable disarm = watchdog.arm();
    try {
      out.close();
    } finally {
      disarm.run();
    }
  }

  private static String capitalize(final String text) {
    return text == null || text.isEmpty() ? "A streamed response" :
        Character.toUpperCase(text.charAt(0)) + text.substring(1);
  }
}
