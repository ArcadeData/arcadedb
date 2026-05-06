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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
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
    boolean repartition = false;
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
      } else if (key.equalsIgnoreCase("repartition")) {
        // Boolean opt-in. When true, every record whose current bucket no longer matches its
        // partition strategy's hash is deleted from its current bucket and re-inserted into the
        // target bucket - which gives it a NEW RID. The flag is cleared on full success only.
        // RID-stable in-place moves are not supported by the current bucket layout, so callers
        // with edges or RID-pinned references must handle re-pointing themselves before running
        // this; partitioned types without inbound edges are the supported workload.
        // Use execute(...) to evaluate uniformly across booleanValue / numeric / string literal
        // shapes - {@link Expression#value} is null for boolean literals, populated for numbers.
        final Object raw = e.getValue().execute((com.arcadedb.query.sql.executor.Result) null, context);
        if (raw instanceof Boolean b)
          repartition = b;
        else if ("true".equalsIgnoreCase(String.valueOf(raw)))
          repartition = true;
        else if ("false".equalsIgnoreCase(String.valueOf(raw)))
          repartition = false;
        else
          throw new CommandSQLParsingException(
              "REBUILD TYPE setting 'repartition' must be true or false, got: " + raw);
      } else
        throw new CommandSQLParsingException(
            "Unrecognized setting '" + key + "' in REBUILD TYPE statement (supported: batchSize, repartition)");
    }
    final int finalBatchSize = batchSize;
    final boolean finalRepartition = repartition;

    final long[] count = { 0L };
    final long[] moved = { 0L };
    // Records committed in earlier batches and therefore NOT rolled back by a later failure: needed so we can
    // tell the caller exactly how many rows are already in the new layout when the rebuild aborts mid-stream.
    final long[] committedBefore = { 0L };
    final boolean implicitTx = !db.isTransactionActive();
    if (implicitTx)
      db.begin();

    // Buffered relocations: when {@code finalRepartition} is true we cannot delete+insert during
    // the type scan because the new record would land in a later bucket and be visited again,
    // double-counting and (depending on iterator semantics) breaking the in-flight bucket
    // iterator. Instead we capture only the RIDs of misplaced records and re-read each one
    // during the post-scan move phase. We deliberately do NOT snapshot the property maps at
    // scan time: holding full record payloads in heap for every misplaced record on a large
    // type is GC-prohibitive (per the project's memory mandate), and a per-record re-read in
    // the move phase costs one extra page-cache hit at most. The re-read also lets a record
    // that was updated after the scan land at the bucket its current value hashes to.
    final List<RID> pendingMoveRids = finalRepartition ? new ArrayList<>() : null;

    final long startNanos = System.nanoTime();
    try {
      db.scanType(typeName.getStringValue(), polymorphic, rec -> {
        if (finalRepartition) {
          final MutableDocument m = (MutableDocument) rec.modify();
          final RID currentRid = m.getIdentity();
          final Bucket targetBucket = type.getBucketIdByRecord(m, false);
          if (currentRid != null && targetBucket != null && targetBucket.getFileId() != currentRid.getBucketId()) {
            // Buffered move - apply after scan to keep iterator stability. Only the RID is
            // retained; the payload is re-read during the move phase to keep heap usage low.
            pendingMoveRids.add(currentRid);
            count[0]++;
          } else {
            m.markDirty();
            m.save();
            count[0]++;
          }
        } else {
          final MutableDocument m = (MutableDocument) rec.modify();
          m.markDirty();
          m.save();
          count[0]++;
        }
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

      // Apply buffered relocations now that the scan is complete. Each move re-reads the
      // current record state (so updates concurrent with the scan land at the bucket their new
      // values hash to), deletes it, and creates a fresh one through the type's bucket-selection
      // strategy. RIDs change.
      // <p>
      // <b>Concurrent-delete safety.</b> Between the scan's RID capture and this loop another
      // commit may have deleted the original record. {@code lookupByRID} returns {@code null}
      // in that case; we MUST skip the insert too, otherwise we'd materialise a ghost record
      // with no original to replace. The whole delete+insert pair is gated on
      // {@code oldRecord != null}.
      if (finalRepartition && pendingMoveRids != null) {
        // Move-phase commit cadence tracks the move counter, not the (now-frozen) scan
        // counter. The error-recovery message uses {@code committedBefore[0]} to tell the
        // operator how many rows are durably in the new layout when the rebuild aborts; using
        // the scan counter here would over-report by the rebuild-only-touched-and-not-moved
        // delta and confuse the recovery story.
        long movePhaseCommittedBefore = 0L;
        for (int i = 0; i < pendingMoveRids.size(); i++) {
          final RID oldRid = pendingMoveRids.get(i);
          final var oldRecord = db.lookupByRID(oldRid, true);
          if (oldRecord instanceof com.arcadedb.database.Document oldDoc) {
            // Snapshot the current property map at move time, not at scan time, so concurrent
            // updates between scan and move are honoured (the new value lands at the bucket
            // its current property hash routes to, not the bucket the scan-time value would
            // have hit). This also keeps heap usage proportional to RID count rather than to
            // total record payload.
            final Map<String, Object> snapshot = new HashMap<>(oldDoc.toMap(false));
            db.deleteRecord(oldRecord);
            final MutableDocument fresh = db.newDocument(typeName.getStringValue());
            fresh.fromMap(snapshot);
            fresh.save();
            moved[0]++;
          }
          if (implicitTx && (i + 1) % finalBatchSize == 0) {
            db.commit();
            movePhaseCommittedBefore = moved[0];
            committedBefore[0] = count[0] + movePhaseCommittedBefore;
            db.begin();
          }
        }
      }

      if (implicitTx)
        db.commit();
      // Clear the needsRepartition flag only after the whole scan succeeded. Mid-rebuild crash
      // leaves the flag set so a re-run is correct.
      if (finalRepartition && type instanceof LocalDocumentType ldt)
        ldt.setNeedsRepartition(false);
      final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
      LogManager.instance().log(this, java.util.logging.Level.INFO,
          "REBUILD TYPE '%s' completed: %,d records re-serialised (repartition moves=%,d) in %,d ms",
          null, typeName.getStringValue(), count[0], moved[0], elapsedMs);
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
    result.setProperty("repartition", finalRepartition);
    result.setProperty("recordsRebuilt", count[0]);
    if (finalRepartition)
      result.setProperty("recordsMoved", moved[0]);
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
