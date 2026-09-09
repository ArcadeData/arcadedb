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
package com.arcadedb.remote;

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Client end of the HTTP streaming query encoding (issue #7306): a {@link ResultSet} that pulls one
 * newline-delimited JSON line off the connection per row, so a caller iterating it holds one row at a time
 * instead of the whole result.
 * <p>
 * The stream is self-delimiting. A {@code record} line is a row; the {@code stats} line is the trailer and ends
 * the iteration; an {@code error} line is a failure the server raised after it had already answered 200, and is
 * rethrown here because the status code could no longer carry it. A stream that ends without any of the three -
 * a dropped connection - is reported as such rather than silently read as an empty or complete result, which is
 * the failure mode a bare line-per-row encoding would have.
 * <p>
 * {@link #close()} closes the underlying connection, so a caller that stops early does not leave it hanging;
 * it is idempotent, and reaching the trailer closes it too. Not thread-safe - a {@link ResultSet} never is.
 */
public class RemoteStreamingResultSet implements ResultSet {
  private final BufferedReader               reader;
  private final Function<JSONObject, Result> rowMapper;
  private final boolean                      warnOnTruncation;

  private Result  next;
  private boolean closed;
  private boolean trailerSeen;

  /**
   * @param reader    the response body, one JSON event per line
   * @param rowMapper        turns a {@code record} payload into a {@link Result}; the same conversion the
   *                         buffered encoding uses, so a row reaches the caller with the same Java types either
   *                         way
   * @param warnOnTruncation whether a truncated trailer is worth a log line. False when the application set its
   *                         own cap with {@code setMaxResultRows()}: it asked for the truncation, exactly as on
   *                         the buffered path
   */
  public RemoteStreamingResultSet(final BufferedReader reader, final Function<JSONObject, Result> rowMapper,
      final boolean warnOnTruncation) {
    this.reader = reader;
    this.rowMapper = rowMapper;
    this.warnOnTruncation = warnOnTruncation;
  }

  @Override
  public boolean hasNext() {
    if (next != null)
      return true;
    if (closed)
      return false;

    // A loop rather than a recursive call: a stream of blank lines is not something the server produces, but a
    // proxy or a hostile peer can, and recursing once per line would turn that into a StackOverflowError.
    String line;
    while (true) {
      try {
        line = reader.readLine();
      } catch (final IOException e) {
        close();
        throw new RemoteException("Error while reading the streamed result", e);
      }

      if (line == null) {
        close();
        if (!trailerSeen)
          // No trailer and no error line: the connection ended mid-stream. Saying so is the whole reason the
          // encoding has a trailer - the alternative is handing back a silently short result.
          throw new RemoteException("The streamed result ended before the server sent its 'stats' trailer: "
              + "the response is incomplete");
        return false;
      }

      if (!line.isBlank())
        break;
    }

    final JSONObject event = new JSONObject(line);
    if (event.has("record")) {
      next = rowMapper.apply(event.getJSONObject("record"));
      return true;
    }
    if (event.has("stats")) {
      trailerSeen = true;
      warnIfTruncated(event.getJSONObject("stats"));
      close();
      return false;
    }
    if (event.has("error")) {
      trailerSeen = true;
      final String message = event.getJSONObject("error").getString("message", "unknown error");
      close();
      throw new RemoteException("The server failed while streaming the result: " + message);
    }

    throw new RemoteException("Unrecognized event in the streamed result: " + line);
  }

  @Override
  public Result next() {
    if (!hasNext())
      throw new NoSuchElementException();
    final Result result = next;
    next = null;
    return result;
  }

  @Override
  public void close() {
    if (closed)
      return;
    closed = true;
    try {
      reader.close();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.FINE, "Error closing the streamed result", e);
    }
  }

  /**
   * Mirrors the warning the buffered path emits when the server dropped rows the caller never asked to drop, so
   * the two encodings do not disagree about whether a short result is worth mentioning.
   */
  private void warnIfTruncated(final JSONObject stats) {
    if (!warnOnTruncation || !stats.getBoolean("truncated", false))
      return;
    LogManager.instance().log(this, Level.WARNING,
        "The server truncated the streamed result to %d rows (its limit is %d): the returned result is incomplete. "
            + "Add an explicit LIMIT to the query, or raise the cap with RemoteDatabase.setMaxResultRows().",
        stats.getInt("returned", -1), stats.getInt("limit", -1));
  }
}
