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

/**
 * Writes a query result to a client as newline-delimited JSON, one line per event, without ever holding more
 * than one row in memory (issue #7306).
 * <p>
 * <b>Wire format.</b> Every line is a JSON object carrying exactly one key, which names the kind of event:
 * <ul>
 * <li>{@code {"record": { ... }}} - one result row, serialized exactly as it would appear inside the
 *     {@code result} array of the buffered {@code application/json} response;</li>
 * <li>{@code {"stats": {"limit": n, "returned": n, "truncated": b}}} - the trailer, carrying the same three
 *     numbers the buffered response reports at top level. Always the last line of a successful stream;</li>
 * <li>{@code {"error": {"message": "..."}}} - the stream failed after the response had already begun.</li>
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
 * Not thread-safe, and does not need to be: one query is serialized by the one worker thread serving it.
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

  static final int  FLUSH_THRESHOLD_BYTES = 8 * 1024;
  static final long FLUSH_INTERVAL_MS     = 50;

  private final OutputStream out;
  private final long         flushIntervalNanos;

  private int     pendingBytes;
  private long    lastFlushNanos;
  private boolean started;

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
   */
  public void writeError(final String message) throws IOException {
    writeLine(new JSONObject().put("error", new JSONObject().put("message", message)), true);
  }

  private void writeLine(final JSONObject event, final boolean forceFlush) throws IOException {
    final byte[] bytes = (event + "\n").getBytes(StandardCharsets.UTF_8);
    out.write(bytes);
    pendingBytes += bytes.length;

    final long now = System.nanoTime();
    if (forceFlush || !started || pendingBytes >= FLUSH_THRESHOLD_BYTES || now - lastFlushNanos >= flushIntervalNanos) {
      out.flush();
      pendingBytes = 0;
      lastFlushNanos = now;
    }
    started = true;
  }

  /**
   * Flushes whatever is still pending and closes the underlying stream, which is what ends the chunked response.
   */
  @Override
  public void close() throws IOException {
    if (pendingBytes > 0)
      out.flush();
    out.close();
  }
}
