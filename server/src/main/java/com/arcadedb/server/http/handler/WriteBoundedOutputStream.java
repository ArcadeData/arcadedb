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
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.ServerConnection;
import org.xnio.IoUtils;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * An {@link OutputStream} on which no call that can reach the socket blocks forever: a watchdog is armed before
 * each one and disarmed as soon as it returns (issues #7381 and #7806). Writes are buffered here first, so a call
 * that only fills the buffer arms nothing, and a large write is handed on in bounded chunks, each armed on its own:
 * what the budget bounds is one chunk of at most {@value #MAX_BOUNDED_CHUNK} bytes making no progress.
 * <p>
 * Because of that buffer a caller MUST flush or close the stream: bytes still held here are not flushed by the
 * exchange when it completes. Not thread-safe, like the response stream it wraps; a caller writing from several
 * threads serializes them itself, as {@code PostServerCommandHandler}'s SSE sink does.
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

  /** Writes that fit here arm no timer: they cannot reach the socket. */
  static final int BUFFER_SIZE       = 16 * 1024;
  /** The most one armed call hands to the response, so a large write is bounded per chunk of progress. */
  static final int MAX_BOUNDED_CHUNK = 64 * 1024;

  private final OutputStream  out;
  private final WriteWatchdog watchdog;
  private final byte[]        buffer = new byte[BUFFER_SIZE];
  private       int           count;

  WriteBoundedOutputStream(final OutputStream out, final WriteWatchdog watchdog) {
    this.out = out;
    this.watchdog = watchdog;
  }

  /**
   * The write-side budget of every streamed response as configured on {@code server}, in milliseconds. Read when a
   * stream is created, so SET SERVER SETTING applies to the next streamed response without a restart; a response
   * already in flight keeps the budget it started with.
   */
  public static int budgetMs(final HttpServer server) {
    return server.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_STREAMING_WRITE_TIMEOUT);
  }

  /**
   * The output stream of {@code exchange}, every write of which is bounded by {@code timeoutMs} (issue #7806).
   * Switches the exchange to blocking mode if it is not already.
   *
   * @param timeoutMs the budget of {@link GlobalConfiguration#SERVER_HTTP_STREAMING_WRITE_TIMEOUT}; not positive
   *                  leaves the writes unbounded
   * @param what      names the response in the warning logged when the bound fires, e.g. "the streamed result of a
   *                  query on database 'x'"; evaluated only then
   */
  public static OutputStream of(final HttpServerExchange exchange, final int timeoutMs, final Supplier<String> what) {
    if (!exchange.isBlocking())
      exchange.startBlocking();
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
  static WriteWatchdog connectionWatchdog(final HttpServerExchange exchange, final int timeoutMs,
      final Supplier<String> what) {
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
            capitalize(what.get()), timeoutMs, GlobalConfiguration.SERVER_HTTP_STREAMING_WRITE_TIMEOUT.getKey());
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
    if (count == buffer.length)
      drain();
    buffer[count++] = (byte) b;
  }

  /**
   * Copies into this stream's own buffer when the bytes fit, which costs no timer at all: only a call that can reach
   * the socket arms one. On the NDJSON query encoding that is once per {@value #BUFFER_SIZE} bytes rather than once
   * per row. A write larger than the buffer goes through in chunks of at most {@value #MAX_BOUNDED_CHUNK} bytes, each
   * armed on its own, so a large row sent to a slow but reading client restarts the timer as it makes progress
   * instead of being charged its whole transfer time against one budget.
   */
  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    Objects.checkFromIndexSize(off, len, b.length);
    if (len <= buffer.length - count) {
      System.arraycopy(b, off, buffer, count, len);
      count += len;
      return;
    }
    drain();
    if (len < buffer.length) {
      System.arraycopy(b, off, buffer, 0, len);
      count = len;
      return;
    }
    writeThrough(b, off, len);
  }

  /**
   * Hands everything buffered to the response and flushes it, which is where a client that stopped reading blocks
   * the worker thread: {@code UndertowOutputStream} writes to the socket only when its own pooled buffer fills or is
   * flushed.
   */
  @Override
  public void flush() throws IOException {
    // One timer for the hand-off and the flush together: one budget per event, not two in a row.
    final Runnable disarm = watchdog.arm();
    try {
      writePending();
      out.flush();
    } finally {
      disarm.run();
    }
  }

  /**
   * Bounded as well: closing the response flushes whatever is still pending, which blocks for the same reason. The
   * response is closed even when handing on the pending bytes fails, and that failure - the write the watchdog
   * released - is the one reported, with a failing close attached to it as suppressed.
   */
  @Override
  public void close() throws IOException {
    final Runnable disarm = watchdog.arm();
    try {
      try {
        writePending();
      } catch (final IOException e) {
        try {
          out.close();
        } catch (final IOException closeFailure) {
          e.addSuppressed(closeFailure);
        }
        throw e;
      }
      out.close();
    } finally {
      disarm.run();
    }
  }

  /** Hands the buffer on under the caller's timer. At most {@value #BUFFER_SIZE} bytes, so one call. */
  private void writePending() throws IOException {
    if (count == 0)
      return;
    final int pending = count;
    // Reset first: a write that failed killed the response, and close() must not try to send the same bytes again.
    count = 0;
    out.write(buffer, 0, pending);
  }

  private void drain() throws IOException {
    if (count == 0)
      return;
    final int pending = count;
    // Reset first: a write that failed killed the response, and close() must not try to send the same bytes again.
    count = 0;
    writeThrough(buffer, 0, pending);
  }

  private void writeThrough(final byte[] b, final int off, final int len) throws IOException {
    for (int pos = off, end = off + len; pos < end; ) {
      final int chunk = Math.min(end - pos, MAX_BOUNDED_CHUNK);
      final Runnable disarm = watchdog.arm();
      try {
        out.write(b, pos, chunk);
      } finally {
        disarm.run();
      }
      pos += chunk;
    }
  }

  private static String capitalize(final String text) {
    return text == null || text.isEmpty() ? "A streamed response" :
        Character.toUpperCase(text.charAt(0)) + text.substring(1);
  }
}
