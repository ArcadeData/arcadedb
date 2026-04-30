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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * REBUILD TYPE typeName [POLYMORPHIC] [WITH batchSize = N] - re-serialises records to apply schema layout changes.
 * <p>
 * <b>Behaviour under Raft HA.</b> This statement extends {@link DDLStatement}, so
 * {@code Statement.isDDL()} returns {@code true}. {@code RaftReplicatedDatabase.command()} therefore forwards
 * any REBUILD TYPE issued on a follower to the leader via {@code forwardCommandToLeaderViaRaft}; the leader is
 * the only node that runs the loop locally. Each in-loop {@code db.commit()} produces one Raft log entry;
 * followers replay them in order. A follower that crashes mid-replay can resync from the leader's snapshot
 * (the half-migrated intermediate state is recoverable by re-running REBUILD on the leader).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RebuildTypeStatement extends DDLStatement {
  private static final int DEFAULT_BATCH_SIZE = 10_000;

  public Identifier                  typeName;
  public boolean                     polymorphic = false;
  public final Map<Expression, Expression> settings = new HashMap<>();

  public RebuildTypeStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Database db = context.getDatabase();
    final Schema schema = db.getSchema();
    final DocumentType type = schema.getType(typeName.getStringValue());
    if (type == null)
      throw new CommandExecutionException("Type not found: " + typeName.getStringValue());

    int batchSize = DEFAULT_BATCH_SIZE;
    for (final Map.Entry<Expression, Expression> e : settings.entrySet()) {
      final String key = e.getKey().toString();
      if (key.equalsIgnoreCase("batchSize")) {
        final Object raw = e.getValue().value;
        try {
          batchSize = Integer.parseInt(raw == null ? "null" : raw.toString());
        } catch (final NumberFormatException nfe) {
          throw new CommandSQLParsingException(
              "REBUILD TYPE setting 'batchSize' must be a positive integer, got: " + raw);
        }
        // batchSize is the modulus for the in-rebuild commit cadence (count[0] % batchSize == 0). A zero or
        // negative value would either ArithmeticException (mod-zero) or never trip the commit branch (modulo by
        // a negative still works but the boundary semantics are nonsensical). Reject up front.
        if (batchSize <= 0)
          throw new CommandSQLParsingException(
              "REBUILD TYPE setting 'batchSize' must be a positive integer, got: " + batchSize);
      } else
        throw new CommandSQLParsingException(
            "Unrecognized setting '" + key + "' in REBUILD TYPE statement (supported: batchSize)");
    }
    final int finalBatchSize = batchSize;

    final long[] count = { 0L };
    // Records committed in earlier batches and therefore NOT rolled back by a later failure: needed so we can
    // tell the caller exactly how many rows are already in the new layout when the rebuild aborts mid-stream.
    final long[] committedBefore = { 0L };
    final boolean implicitTx = !db.isTransactionActive();
    if (implicitTx)
      db.begin();

    final long startNanos = System.nanoTime();
    try {
      db.scanType(typeName.getStringValue(), polymorphic, rec -> {
        final MutableDocument m = (MutableDocument) rec.modify();
        m.markDirty();
        m.save();
        count[0]++;
        // Batch only when we own the transaction. Committing inside a caller-supplied TX would leak the user's
        // writes prematurely and leave the trailing batch uncommitted.
        if (implicitTx && count[0] % finalBatchSize == 0) {
          db.commit();
          committedBefore[0] = count[0];
          // Per-batch progress line so an operator running REBUILD on a 100M-record type can observe forward
          // motion via the server log instead of staring at a frozen prompt for many minutes. Logged at INFO
          // so it's on by default but easy to silence by raising the level for this class.
          final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
          LogManager.instance().log(this, java.util.logging.Level.INFO,
              "REBUILD TYPE '%s': %,d records re-serialised so far (last batch=%,d, elapsed=%,d ms)",
              null, typeName.getStringValue(), count[0], finalBatchSize, elapsedMs);
          db.begin();
        }
        return true;
      });

      if (implicitTx)
        db.commit();
      final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
      LogManager.instance().log(this, java.util.logging.Level.INFO,
          "REBUILD TYPE '%s' completed: %,d records re-serialised in %,d ms",
          null, typeName.getStringValue(), count[0], elapsedMs);
    } catch (Exception e) {
      if (implicitTx && db.isTransactionActive())
        db.rollback();
      // REBUILD TYPE is NOT atomic across batch boundaries: every db.commit() above already persisted those
      // records in the new layout. After rollback, only the in-flight batch is reverted; the type is left
      // half-migrated. Surface the boundary clearly so the operator knows to re-run REBUILD TYPE to finish.
      final long migratedAndKept = committedBefore[0];
      final long rolledBack = count[0] - committedBefore[0];
      throw new CommandExecutionException(
          "Error on rebuilding type '" + typeName.getStringValue() + "' after " + count[0] + " records ("
              + migratedAndKept + " committed in earlier batches and remain in the new layout, " + rolledBack
              + " rolled back from the in-flight batch). REBUILD TYPE is NOT atomic across batches; re-run the"
              + " command to migrate the remaining records once the underlying issue is fixed."
              + " If the failed batch left external-bucket blobs that no primary record references (typical when the"
              + " rebuild crashes between writing the external value and re-serializing the primary record), run"
              + " CHECK DATABASE FIX to delete the orphaned external records.", e);
    }

    // If the rebuild was triggered to revert a property from EXTERNAL to inline (i.e. the type no longer has
    // any EXTERNAL property), the previously-paired external buckets are now empty (orphan-cleanup in
    // serializeProperties deleted every external blob during the re-save). Drop them so they don't accumulate
    // across toggle cycles, and persist the cleared mapping.
    // Reclaim paired external buckets only if REBUILD owned the transaction. With a caller-supplied tx the
    // queued record updates haven't flushed yet (LocalDatabase.updateRecord defers serialization to commit),
    // so the orphan cleanup hasn't run and the buckets are still non-empty. The caller can re-run REBUILD
    // outside their tx to trigger reclaim, or invoke it explicitly.
    boolean reclaimSkipped = false;
    if (type instanceof LocalDocumentType ldt && !ldt.hasExternalProperties()) {
      if (implicitTx)
        ldt.reclaimEmptyExternalBuckets();
      else if (ldt.hasExternalBuckets())
        // Caller-supplied tx with empty-EXTERNAL state and paired buckets still around: we cannot drop them
        // here (records haven't flushed), so surface the situation in the result + log so the operator knows
        // to re-run REBUILD outside a transaction to reclaim the buckets.
        reclaimSkipped = true;
    }

    final ResultInternal result = new ResultInternal(db);
    result.setProperty("operation", "rebuild type");
    result.setProperty("typeName", typeName.getStringValue());
    result.setProperty("polymorphic", polymorphic);
    result.setProperty("recordsRebuilt", count[0]);
    if (reclaimSkipped) {
      final String warning = "REBUILD TYPE " + typeName.getStringValue()
          + " ran inside a caller-supplied transaction; paired external buckets were NOT reclaimed because "
          + "queued record updates haven't been flushed yet. Re-run REBUILD TYPE outside a transaction (or "
          + "commit and re-run) to drop the now-empty paired buckets.";
      result.setProperty("warning", warning);
      LogManager.instance().log(this, java.util.logging.Level.WARNING, warning);
    }
    final InternalResultSet rs = new InternalResultSet();
    rs.add(result);
    return rs;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("REBUILD TYPE ");
    typeName.toString(params, builder);
    if (polymorphic)
      builder.append(" POLYMORPHIC");
    if (!settings.isEmpty()) {
      builder.append(" WITH ");
      boolean first = true;
      for (final Map.Entry<Expression, Expression> e : settings.entrySet()) {
        if (!first)
          builder.append(", ");
        e.getKey().toString(params, builder);
        builder.append(" = ");
        e.getValue().toString(params, builder);
        first = false;
      }
    }
  }

  @Override
  public RebuildTypeStatement copy() {
    final RebuildTypeStatement result = new RebuildTypeStatement(-1);
    result.typeName = typeName == null ? null : typeName.copy();
    result.polymorphic = polymorphic;
    for (final Map.Entry<Expression, Expression> e : settings.entrySet())
      result.settings.put(e.getKey().copy(), e.getValue().copy());
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final RebuildTypeStatement that = (RebuildTypeStatement) o;
    return polymorphic == that.polymorphic
        && Objects.equals(typeName, that.typeName)
        && Objects.equals(settings, that.settings);
  }

  @Override
  public int hashCode() {
    int result = typeName != null ? typeName.hashCode() : 0;
    result = 31 * result + (polymorphic ? 1 : 0);
    result = 31 * result + settings.hashCode();
    return result;
  }
}
