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

import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes a response to a client as newline-delimited JSON, one line per event, without ever holding more
 * than one event in memory (issue #7306).
 * <p>
 * <b>Wire format.</b> Every line is a JSON object carrying exactly one key, which names the kind of event:
 * <ul>
 * <li>{@code {"record": { ... }}} - one result row, serialized exactly as it would appear inside the
 *     {@code result} array of the buffered {@code application/json} response;</li>
 * <li>{@code {"stats": {"limit": n, "returned": n, "truncated": b}}} - the trailer, carrying the same three
 *     numbers the buffered response reports at top level. Always the last line of a successful stream;</li>
 * <li>{@code {"error": {"message": "...", "status": n, "exception": "...", "exceptionArgs": "..."}}} - the stream
 *     failed after the response had already begun. {@code status} is the code the buffered encoding would have
 *     answered the same failure with, {@code exception} the class it would have reported and {@code exceptionArgs}
 *     its structured arguments, present only when the failure has any (issue #8235).</li>
 * </ul>
 * The envelope is what makes the stream self-delimiting. Emitting bare rows and then a bare trailer would leave
 * a consumer unable to tell the trailer from a row that happens to carry the same property names, and a stream
 * cut short by a dropped connection indistinguishable from a complete one: a consumer that saw no
 * {@code stats} line knows it did not get everything.
 * <p>
 * <b>Flushing.</b> The first line is flushed immediately, so a consumer sees the stream open as soon as the
 * engine produces anything. After that a flush costs a blocking write, so one is forced only when
 * {@value #FLUSH_THRESHOLD_BYTES} bytes have accumulated or {@value #FLUSH_INTERVAL_MS} ms have passed since
 * the last one - whichever comes first. That bounds both the syscall rate on a fast query and the delivery
 * latency on a slow one, instead of trading one away for the other. The trailer and the error line always
 * flush.
 * <p>
 * <b>Other event kinds.</b> {@link #writeEvent} takes the envelope key, so a second surface can define its own
 * vocabulary on the same discipline instead of a second writer. {@code POST /api/v1/batch} does, with
 * {@code progress} / {@code summary} / {@code error} (issue #7311).
 * <p>
 * <b>Liveness.</b> A consumer cannot tell a server that is still scanning for the next matching row from one that
 * has gone away: on the wire both are silence, and a client that bounds silence (the Java driver does, issue #8473)
 * would fail a healthy query whose next row is slow to produce (issue #8565). {@link #keepAlive} therefore writes a
 * bare newline when nothing has been flushed for a while, from a timer rather than from the worker thread, which is
 * the one blocked producing the row. A blank line is part of the NDJSON convention every consumer already skips, so
 * it carries no event and needs no opt-in: a driver that predates it ignores it, and it is not a row, a trailer or
 * an error.
 * <p>
 * Not thread-safe in general, and does not need to be: one request is serialized by the one worker thread serving it.
 * The one exception is {@link #keepAlive}, which may run on another thread, so every write and the close are
 * serialized by a lock that {@code keepAlive} only ever tries, never waits on: a stream somebody is writing to is
 * not silent.
 */
public final class NdJsonResultStream implements AutoCloseable {
  /**
   * Media type a client sends in {@code Accept} to select this encoding, and that the response carries back.
   * <p>
   * Deliberately duplicated as {@code RemoteDatabase.NDJSON_CONTENT_TYPE} in the {@code network} module, which
   * cannot depend on {@code server}. Change one and you must change the other, or the driver stops selecting
   * the encoding and silently falls back to the buffered body.
   */
  public static final String CONTENT_TYPE = "application/x-ndjson";

  private static final byte[] NEWLINE = { '\n' };

  static final int  FLUSH_THRESHOLD_BYTES = 8 * 1024;
  static final long FLUSH_INTERVAL_MS     = 50;

  private final OutputStream  out;
  private final long          flushIntervalNanos;
  private final ReentrantLock lock = new ReentrantLock();

  private int     pendingBytes;
  private long    lastFlushNanos;
  private volatile boolean started;
  private boolean closed;

  public NdJsonResultStream(final OutputStream out) {
    this(out, FLUSH_INTERVAL_MS);
  }

  /**
   * Package-private constructor taking an explicit flush interval, so a test can pin the time-based half of the
   * policy instead of racing a wall clock.
   */
  NdJsonResultStream(final OutputStream out, final long flushIntervalMs) {
    this.out = out;
    this.flushIntervalNanos = flushIntervalMs * 1_000_000L;
    this.lastFlushNanos = System.nanoTime();
  }

  /**
   * Emits one result row. The row object is written as-is inside the {@code record} envelope, so the bytes of a
   * row are the bytes the buffered response would have put in its {@code result} array.
   */
  public void writeRecord(final JSONObject row) throws IOException {
    writeLine(new JSONObject().put("record", row), false);
  }

  /**
   * Emits the trailer and flushes it. Carries the same {@code limit} / {@code returned} / {@code truncated}
   * triple the buffered response reports, so a client can apply one piece of logic to both encodings.
   */
  public void writeStats(final int limit, final int returned, final boolean truncated) throws IOException {
    writeLine(new JSONObject().put("stats", new JSONObject()
        .put(AbstractQueryHandler.LIMIT_FIELD, limit > 0 ? limit : -1)
        .put(AbstractQueryHandler.RETURNED_FIELD, returned)
        .put(AbstractQueryHandler.TRUNCATED_FIELD, truncated)), true);
  }

  /**
   * Emits a failure that happened after the 200 had already gone out, and flushes it. The status line cannot be
   * taken back at that point, so the only way to tell the client the result is incomplete is in-band - and it is
   * distinguishable from a complete stream because no {@code stats} line follows.
   * <p>
   * The line carries what the buffered encoding would have answered with (issue #8235): the status, the reported
   * exception class and its structured arguments, so a client can tell a retryable conflict from a security refusal
   * or a server fault without parsing {@code message}. {@code status} and {@code exception} are the members to key on:
   * {@code message} is the raw failure text, not concealed in production mode (issue #8875).
   *
   * @param status        the HTTP status the buffered encoding would have sent for the same failure
   * @param exception     the class name of the reported exception, or null to leave the member out
   * @param exceptionArgs the structured arguments of the failure, or null when it has none
   */
  public void writeError(final String message, final int status, final String exception, final String exceptionArgs)
      throws IOException {
    final JSONObject error = new JSONObject().put("message", message).put("status", status);
    if (exception != null)
      error.put("exception", exception);
    if (exceptionArgs != null)
      error.put("exceptionArgs", exceptionArgs);
    writeLine(new JSONObject().put("error", error), true);
  }

  /**
   * Emits one event under an arbitrary envelope key, for a surface whose vocabulary is not the query one. The
   * caller decides whether the line is flushed at once: an event a consumer is waiting on - a batch progress
   * line, a trailer - has to be, while a high-rate event leaves the decision to the size/interval policy above.
   */
  public void writeEvent(final String kind, final JSONObject body, final boolean forceFlush) throws IOException {
    writeLine(new JSONObject().put(kind, body), forceFlush);
  }

  /**
   * Whether anything has been written yet, which for this stream means whether the response has been committed:
   * the first line is always flushed. A caller that has written nothing can still let an exception travel to the
   * standard error mapping and receive a real status code, instead of reporting it in band under a 200 that was
   * never sent (issue #7311).
   */
  public boolean hasStarted() {
    return started;
  }

  /**
   * Keeps the stream from looking dead while the engine is slow to produce the next row: when nothing has been
   * flushed for {@code idleMs} it flushes what is pending, or writes a bare newline when nothing is (see the class
   * javadoc, "Liveness"). Meant to be called from a timer thread. Never waits for a writer: when the stream is being
   * written to it is not silent, and when the stream is closed there is nobody to tell.
   *
   * @return how long, in milliseconds, until the stream could next be idle for {@code idleMs}, so the caller can check
   *         again then instead of a full interval later (which would let the silence reach twice the interval); or
   *         -1 once the stream is closed or the write failed, so the caller stops its timer
   */
  public long keepAlive(final long idleMs) {
    if (!lock.tryLock())
      return idleMs;
    try {
      if (closed)
        return -1;
      final long idleNanos = System.nanoTime() - lastFlushNanos;
      if (idleNanos < idleMs * 1_000_000L)
        return Math.max(1, idleMs - idleNanos / 1_000_000L);
      if (pendingBytes == 0)
        out.write(NEWLINE);
      out.flush();
      pendingBytes = 0;
      lastFlushNanos = System.nanoTime();
      started = true;
      return idleMs;
    } catch (final IOException e) {
      // The client is gone, or the write bound closed the connection: the worker thread finds out on its own write
      return -1;
    } finally {
      lock.unlock();
    }
  }

  private void writeLine(final JSONObject event, final boolean forceFlush) throws IOException {
    final byte[] bytes = (event + "\n").getBytes(StandardCharsets.UTF_8);
    lock.lock();
    try {
      out.write(bytes);
      pendingBytes += bytes.length;

      final long now = System.nanoTime();
      if (forceFlush || !started || pendingBytes >= FLUSH_THRESHOLD_BYTES || now - lastFlushNanos >= flushIntervalNanos) {
        out.flush();
        pendingBytes = 0;
        lastFlushNanos = now;
      }
      started = true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Flushes whatever is still pending and closes the underlying stream, which is what ends the chunked response.
   */
  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      closed = true;
      if (pendingBytes > 0)
        out.flush();
      out.close();
    } finally {
      lock.unlock();
    }
  }
}
