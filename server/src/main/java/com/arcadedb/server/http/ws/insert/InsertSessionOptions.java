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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The gRPC {@code InsertOptions} message as a {@code /ws} insert session understands it, parsed from the
 * {@code options} object of a {@code start} frame (issue #7382).
 * <p>
 * The control frames decide WHEN a transaction commits ({@code transactionMode}); the conflict options decide
 * WHAT HAPPENS TO A ROW THAT ALREADY EXISTS ({@code conflictMode}, {@code keyColumns},
 * {@code updateColumnsOnConflict}) and {@code validateOnly} whether anything is written at all. All of them are
 * honoured with the semantics gRPC gives them (issue #7404), so a loader ported from {@code InsertBidirectional}
 * keeps its behaviour. The gRPC spellings of the enum values ({@code CONFLICT_UPDATE}, {@code PER_BATCH}) are
 * accepted next to the short ones.
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

  /**
   * What to do with a row whose key is already taken. Mirrors {@code InsertOptions.ConflictMode}, and like it a
   * "key" is either the {@link #keyColumns} the session names or, when it names none, whatever unique index the
   * insert trips over.
   */
  public enum ConflictMode {
    /** The row is counted in {@code failed} and described in {@code errors} with the code {@code CONFLICT}. */
    ERROR,
    /**
     * The matching record is updated in place with the incoming values - {@link #updateColumnsOnConflict} when
     * given, every non-key property of the record otherwise - and the row is counted in {@code updated}. Needs
     * {@link #keyColumns}: with nothing to look the existing record up by, there is nothing to update.
     */
    UPDATE,
    /** The row is dropped and counted in {@code ignored}. */
    IGNORE,
    /**
     * Accepted for parity with gRPC, where it is answered exactly as {@link #ERROR} is: the row is reported as a
     * {@code CONFLICT} and the rest of the chunk still goes in.
     */
    ABORT
  }

  /** Default type of the records of every chunk, overridable per record with {@code @class}. */
  public final String          targetType;
  public final TransactionMode transactionMode;
  public final ConflictMode    conflictMode;
  /** The properties a row is matched on for {@link ConflictMode#UPDATE} and {@link ConflictMode#IGNORE}. */
  public final List<String>    keyColumns;
  /** {@link #keyColumns} as a set, for the per-property "is this a key" check of a merge. */
  public final Set<String>     keyColumnSet;
  /** The properties an {@link ConflictMode#UPDATE} overwrites; empty means every non-key property sent. */
  public final List<String>    updateColumnsOnConflict;
  /** Rows are received, parsed and counted but nothing is written. */
  public final boolean         validateOnly;

  private InsertSessionOptions(final String targetType, final TransactionMode transactionMode,
      final ConflictMode conflictMode, final List<String> keyColumns, final List<String> updateColumnsOnConflict,
      final boolean validateOnly) {
    this.targetType = targetType;
    this.transactionMode = transactionMode;
    this.conflictMode = conflictMode;
    this.keyColumns = keyColumns;
    this.keyColumnSet = Set.copyOf(keyColumns);
    this.updateColumnsOnConflict = updateColumnsOnConflict;
    this.validateOnly = validateOnly;
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
      return new InsertSessionOptions(null, TransactionMode.PER_STREAM, ConflictMode.ERROR, List.of(), List.of(), false);

    final String targetType = options.getString("targetType", null);
    final TransactionMode mode = parseTransactionMode(options.getString("transactionMode", null));
    final ConflictMode conflictMode = parseConflictMode(options.getString("conflictMode", null));
    final List<String> keyColumns = parseColumns(options, "keyColumns");
    final List<String> updateColumns = parseColumns(options, "updateColumnsOnConflict");
    final boolean validateOnly = options.getBoolean("validateOnly", false);

    // gRPC accepts this combination and then reports every conflicting row as a CONFLICT, because with no key
    // to look the existing record up by tryUpsertByRecord never matches. A session that asked for updates and
    // can never perform one is better refused at start than discovered chunk by chunk.
    if (conflictMode == ConflictMode.UPDATE && keyColumns.isEmpty())
      throw new IllegalArgumentException(
          "conflictMode 'update' needs 'keyColumns': the properties an existing record is matched on before it is updated");

    return new InsertSessionOptions(targetType, mode, conflictMode, keyColumns, updateColumns, validateOnly);
  }

  private static TransactionMode parseTransactionMode(final String rawMode) {
    if (rawMode == null || rawMode.isBlank())
      return TransactionMode.PER_STREAM;

    final String normalized = rawMode.trim().toUpperCase(Locale.ENGLISH);
    if ("PER_REQUEST".equals(normalized))
      return TransactionMode.PER_STREAM;

    final TransactionMode mode;
    try {
      mode = TransactionMode.valueOf(normalized);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown transactionMode '" + rawMode
          + "'. Expected one of: per_stream, per_request, per_batch, per_row, none");
    }

    if (mode == TransactionMode.NONE)
      throw new IllegalArgumentException(
          "transactionMode 'none' needs an externally-managed transaction, which a /ws insert session cannot name yet (issue #7403)");

    return mode;
  }

  private static ConflictMode parseConflictMode(final String rawMode) {
    if (rawMode == null || rawMode.isBlank())
      return ConflictMode.ERROR;

    // "CONFLICT_UPDATE" is how the gRPC enum spells it; a ported loader may well send that.
    String normalized = rawMode.trim().toUpperCase(Locale.ENGLISH);
    if (normalized.startsWith("CONFLICT_"))
      normalized = normalized.substring("CONFLICT_".length());

    try {
      return ConflictMode.valueOf(normalized);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown conflictMode '" + rawMode + "'. Expected one of: error, update, ignore, abort");
    }
  }

  /**
   * A list of property names. Refuses a blank name up front, the way gRPC's {@code INVALID_KEY_COLUMN} does,
   * rather than letting every row fail on the quoting of an empty identifier.
   */
  private static List<String> parseColumns(final JSONObject options, final String key) {
    if (!options.has(key) || options.isNull(key))
      return List.of();

    final JSONArray array = options.getJSONArray(key);
    final String[] columns = new String[array.length()];
    for (int i = 0; i < columns.length; i++) {
      final Object column = array.isNull(i) ? null : array.get(i);
      if (!(column instanceof String name) || name.isBlank())
        throw new IllegalArgumentException("Option '" + key + "' must not contain empty names");
      columns[i] = name;
    }
    return List.of(columns);
  }

  /** The name this mode is spelled with on the wire, i.e. what a {@code started} frame echoes back. */
  public String transactionModeName() {
    return transactionMode.name().toLowerCase(Locale.ENGLISH);
  }

  /** The name this mode is spelled with on the wire, i.e. what a {@code started} frame echoes back. */
  public String conflictModeName() {
    return conflictMode.name().toLowerCase(Locale.ENGLISH);
  }
}
