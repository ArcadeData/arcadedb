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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * A {@link ResultSet} fed by an {@code application/x-ndjson} response body, one row at a time (issue #7306).
 * <p>
 * Nothing is accumulated: {@link #hasNext()} reads exactly as far as the next line and {@link #next()} hands it
 * over, so a result set larger than the client heap is consumable and the first row is available before the
 * server has produced the last. That is the whole point of the streaming endpoint, and it is lost the moment
 * anything here keeps a list.
 * <p>
 * The response framing is the server's: each line is an object with one key - {@code result} for a row,
 * {@code summary} for the end of a complete stream, {@code error} for the end of a failed one. An {@code error}
 * line is the only way a mid-stream failure can be reported, because the 200 status line was sent with the first
 * row, so it is raised here as a {@link RemoteException} rather than ending the iteration quietly - a silently
 * short result set is exactly what a caller cannot detect.
 */
public class NdjsonResultSet implements ResultSet {
  private final BufferedReader          reader;
  private final Function<JSONObject, Result> rowMapper;
  private final boolean                 warnOnTruncation;

  private Result  next;
  private boolean exhausted;
  private boolean closed;

  /**
   * @param body             the response body; this result set owns it and closes it
   * @param warnOnTruncation whether a truncated stream should be logged. False when the caller set its own row
   *                         cap, since it then asked for the truncation - the same rule the buffered path applies
   * @param rowMapper        converts one serialized row into a {@link Result}, so the streamed and buffered paths
   *                         produce identical rows
   */
  NdjsonResultSet(final InputStream body, final boolean warnOnTruncation, final Function<JSONObject, Result> rowMapper) {
    this.reader = new BufferedReader(new InputStreamReader(body, DatabaseFactory.getDefaultCharset()));
    this.warnOnTruncation = warnOnTruncation;
    this.rowMapper = rowMapper;
  }

  @Override
  public boolean hasNext() {
    if (next != null)
      return true;
    if (exhausted || closed)
      return false;

    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank())
          continue;

        final JSONObject parsed = new JSONObject(line);
        if (parsed.has("result")) {
          next = rowMapper.apply(parsed.getJSONObject("result"));
          return true;
        }
        if (parsed.has("error")) {
          exhausted = true;
          close();
          throw new RemoteException("The server failed while streaming the result: " + parsed.getString("error"));
        }
        if (parsed.has("summary")) {
          final JSONObject summary = parsed.getJSONObject("summary");
          if (warnOnTruncation && summary.getBoolean("truncated", false))
            // Same warning, and the same reason for it, as the buffered path: without it a caller cannot tell a
            // complete result from one the server cut, and would silently disagree with the embedded API
            // (issue #5711).
            LogManager.instance().log(this, Level.WARNING,
                "The server truncated the result set to %d rows (its limit is %d): the returned result is incomplete. "
                    + "Add an explicit LIMIT to the query, or raise the cap with RemoteDatabase.setMaxResultRows().",
                summary.getInt("returned", -1), summary.getInt("limit", -1));
          break;
        }
        // An unrecognized key is a newer server speaking a framing this client does not know. Skipping it keeps
        // an old client working against a new server for the lines it does understand, which is the whole reason
        // every line is keyed rather than positional.
      }
    } catch (final IOException e) {
      exhausted = true;
      close();
      throw new RemoteException("Error on reading the streamed result", e);
    }

    exhausted = true;
    close();
    return false;
  }

  @Override
  public Result next() {
    if (!hasNext())
      throw new NoSuchElementException();
    final Result current = next;
    next = null;
    return current;
  }

  @Override
  public void close() {
    if (closed)
      return;
    closed = true;
    try {
      reader.close();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.FINE, "Error on closing the streamed result body: %s", e.getMessage());
    }
  }

  /**
   * Always empty. The streamed response carries rows and an end-of-stream summary and nothing else, so there is
   * no plan to report; a caller that needs one asks for the buffered response, which carries it.
   */
  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }
}
