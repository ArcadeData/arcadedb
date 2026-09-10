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
package com.arcadedb.server.http.ws.insert;

import com.arcadedb.serializer.json.JSONObject;

import java.util.Locale;

/**
 * The subset of the gRPC {@code InsertOptions} message a {@code /ws} insert session understands, parsed from the
 * {@code options} object of a {@code start} frame (issue #7382).
 * <p>
 * Only the fields whose behaviour the control frames change are here. {@code conflictMode}, {@code keyColumns},
 * {@code updateColumnsOnConflict} and {@code validateOnly} are gRPC-only for now and are rejected rather than
 * ignored, so a client porting a working {@code InsertBidirectional} loader is told what is missing instead of
 * silently getting plain inserts where it asked for upserts - see issue #7404.
 *
 * @author Arcade Data Ltd
 */
public class InsertSessionOptions {
  /**
   * When each transaction the session writes through is committed. Mirrors {@code InsertOptions.TransactionMode},
   * which is the part of the gRPC shape that most needs the control frames: over {@code POST /api/v1/batch} the
   * commit policy is fixed by query parameters before the load starts and cannot be changed once it is running.
   */
  public enum TransactionMode {
    /**
     * One transaction for the whole session, committed or rolled back when the CLIENT says so. The only mode in
     * which a {@code rollback} frame can still undo a chunk the client has already been acknowledged for.
     * {@code PER_REQUEST} is accepted as an alias: a {@code /ws} session IS the request, so gRPC's distinction
     * between "the RPC" and "the stream" has no counterpart here.
     */
    PER_STREAM,
    /** One transaction per {@code chunk} frame, committed before its {@code batchAck} is written. */
    PER_BATCH,
    /** One transaction per row. */
    PER_ROW,
    /**
     * The caller manages the transaction externally. Refused at {@code start}: a {@code /ws} session has no way to
     * name an existing transaction yet, which is issue #7403.
     */
    NONE
  }

  /** Default type of the records of every chunk, overridable per record with {@code @class}. */
  public final String          targetType;
  public final TransactionMode transactionMode;

  private InsertSessionOptions(final String targetType, final TransactionMode transactionMode) {
    this.targetType = targetType;
    this.transactionMode = transactionMode;
  }

  /**
   * @param options the {@code options} object of a {@code start} frame, or {@code null} when the frame carried none
   *
   * @throws IllegalArgumentException when a value is not one this server implements. Thrown rather than defaulted:
   *                                  a load that silently ran under a different commit policy than the one asked
   *                                  for is exactly the failure the control frames exist to prevent
   */
  public static InsertSessionOptions parse(final JSONObject options) {
    if (options == null)
      return new InsertSessionOptions(null, TransactionMode.PER_STREAM);

    for (final String unsupported : new String[] { "conflictMode", "keyColumns", "updateColumnsOnConflict",
        "validateOnly" })
      if (options.has(unsupported) && !options.isNull(unsupported))
        throw new IllegalArgumentException(
            "Option '" + unsupported + "' is not supported by a /ws insert session yet (issue #7404)");

    final String targetType = options.getString("targetType", null);

    final String rawMode = options.getString("transactionMode", null);
    final TransactionMode mode;
    if (rawMode == null || rawMode.isBlank())
      mode = TransactionMode.PER_STREAM;
    else {
      final String normalized = rawMode.trim().toUpperCase(Locale.ENGLISH);
      if ("PER_REQUEST".equals(normalized))
        mode = TransactionMode.PER_STREAM;
      else
        try {
          mode = TransactionMode.valueOf(normalized);
        } catch (final IllegalArgumentException e) {
          throw new IllegalArgumentException("Unknown transactionMode '" + rawMode
              + "'. Expected one of: per_stream, per_request, per_batch, per_row, none");
        }
    }

    if (mode == TransactionMode.NONE)
      throw new IllegalArgumentException(
          "transactionMode 'none' needs an externally-managed transaction, which a /ws insert session cannot name yet (issue #7403)");

    return new InsertSessionOptions(targetType, mode);
  }

  /** The name this mode is spelled with on the wire, i.e. what a {@code started} frame echoes back. */
  public String transactionModeName() {
    return transactionMode.name().toLowerCase(Locale.ENGLISH);
  }
}
