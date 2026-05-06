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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * REBUILD TYPE typeName [POLYMORPHIC] [WITH batchSize = N, repartition = true] - re-serialises records to apply
 * schema layout changes. With {@code repartition = true}, additionally relocates records whose current bucket
 * no longer matches the partition strategy's hash.
 * <p>
 * <b>Idempotent on partial failure.</b> The command is NOT atomic across batch boundaries: every committed
 * batch persists into the new layout immediately. If the rebuild aborts mid-run (e.g. an exception during
 * the move phase, or the leader crashing after some batches), the {@code needsRepartition} flag stays {@code
 * true} so a re-run is required. That re-run is safe to issue without manual cleanup:
 * <ul>
 *   <li>The scan visits every record in the type. Records already relocated in the previous run are now in
 *       their correct bucket and pass through the in-place save path; they are not queued for another move.
 *   <li>The move phase looks up each pending RID with {@link com.arcadedb.database.Database#lookupByRID};
 *       a previously-deleted record returns {@code null} and the loop skips it cleanly without creating a
 *       ghost record.
 * </ul>
 * Repeated invocations therefore converge: a fully-clean rebuild is a no-op (no misplaced records to move),
 * a partially-completed one finishes the remaining work.
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
  static final int DEFAULT_BATCH_SIZE = 10_000;

  public Identifier                  typeName;
  public boolean                     polymorphic = false;
  public final Map<Expression, Expression> settings = new HashMap<>();

  public RebuildTypeStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
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
        final Object raw = e.getValue().execute((Result) null, context);
        repartition = parseBooleanSetting("REBUILD TYPE", "repartition", raw);
      } else
        throw new CommandSQLParsingException(
            "Unrecognized setting '" + key + "' in REBUILD TYPE statement (supported: batchSize, repartition)");
    }
    return executeRebuild(context, batchSize, repartition);
  }

  /**
   * Runs the rebuild loop with already-resolved parameters. Direct entry point for callers that
   * have the parameters in hand and want to bypass the settings-map parsing - notably the
   * chained REBUILD path in {@link AlterTypeStatement#executeDDL} where building a SQL string
   * to feed back through the parser would re-introduce identifier-quoting injection surface.
   * <p>
   * <b>Transaction scope.</b> When invoked standalone the caller is outside a transaction and
   * the {@code implicitTx == true} branch commits every {@code finalBatchSize} records, capping
   * memory + WAL pressure. When chained from {@link AlterTypeStatement#executeDDL} the DDL has
   * already opened a transaction, so {@code implicitTx == false} and no intermediate commits
   * fire (committing inside a caller-supplied transaction would leak the user's writes
   * prematurely). For multi-million-record types the chained path can therefore exceed the
   * single-transaction memory + WAL budget; operators should split the workflow into a
   * standalone {@code REBUILD TYPE ... WITH repartition = true} followed by the bare
   * {@code ALTER TYPE ...} (without {@code WITH repartition = true}) when the type is large.
   */
  ResultSet executeRebuild(final CommandContext context, final int finalBatchSize, final boolean finalRepartition) {
    final Database db = context.getDatabase();
    final Schema schema = db.getSchema();
    final DocumentType type = schema.getType(typeName.getStringValue());
    if (type == null)
      throw new CommandExecutionException("Type not found: " + typeName.getStringValue());

    // Repartitioning gives every record a new RID. The move-phase re-creation path uses
    // {@code db.newDocument}, which only writes {@code Document.RECORD_TYPE}. Any type whose
    // records carry a different kind byte (vertices, edges, time-series, future kinds) would
    // be silently corrupted on the way back. Use the kind byte itself as the allowlist gate so
    // future record-type subclasses get blocked by default rather than slipping through an
    // exclusion list. Graph types additionally suffer integrity loss (inbound edges dangle on
    // the old RID, outbound edge segments stored on the vertex are wiped by delete+re-insert);
    // the supported workflow for any non-Document type is drop and re-import under the new
    // partitioning.
    if (finalRepartition && type.getType() != Document.RECORD_TYPE) {
      throw new CommandSQLParsingException(
          "REBUILD TYPE on type '" + typeName.getStringValue() + "' with repartition = true is not supported: "
              + "every record would receive a new RID, and the move-phase re-creation path writes a Document "
              + "record-type byte that is incompatible with the type's actual record kind. Drop and re-import "
              + "the type under the new partitioning instead.");
    }

    final long[] count = { 0L };
    final long[] moved = { 0L };
    // Records that have been written to their final layout AND committed in an earlier batch.
    // For non-repartition rebuilds this advances on every per-record save (every visit produces
    // a save). For repartition rebuilds the scan only saves the records that are already in the
    // right bucket (misplaced ones go to {@code pendingMoveRids} and aren't saved until the
    // post-scan move phase), so we must NOT use {@code count[0]} here - it would over-report
    // by the misplaced-and-buffered delta. Tracked explicitly via {@code scanCommittedSaves}.
    final long[] committedBefore = { 0L };
    // Cumulative running total of records that the scan loop wrote via {@code m.save()} (i.e.
    // non-relocate path). After each scan-batch commit, this becomes the new {@code committedBefore}.
    final long[] scanCommittedSaves = { 0L };
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
    // <p>
    // Heap requirement: this list holds one RID (~16 bytes including ArrayList overhead) per
    // misplaced record across the whole scan. For a fully-misplaced 100M-record type that's
    // ~1.6GB of heap. The cap below ({@link GlobalConfiguration#REBUILD_REPARTITION_MAX_BUFFERED_RIDS},
    // default 10M = ~160MB) refuses to continue past the limit so a runaway rebuild doesn't
    // silently OOM the server. Operators with larger types should split the work (drop and
    // re-import, or partial rebuilds in disjoint key ranges); a streaming move-during-scan path
    // would require a different bucket-iteration strategy and is tracked as a follow-up.
    final int maxBufferedRids = db.getConfiguration().getValueAsInteger(
        GlobalConfiguration.REBUILD_REPARTITION_MAX_BUFFERED_RIDS);
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
            // Misplaced records are NOT counted in {@code scanCommittedSaves} - they aren't
            // committed in the new layout until the move phase rewrites them.
            // Enforce the buffer cap before adding so we fail before crossing the limit. Caller
            // sees an actionable error rather than the JVM running OOM mid-scan.
            if (pendingMoveRids.size() >= maxBufferedRids)
              throw new CommandExecutionException(
                  "REBUILD TYPE WITH repartition = true exceeded the buffered-RIDs cap of " + maxBufferedRids
                      + " on type '" + typeName.getStringValue() + "': too many records are misplaced for a single "
                      + "in-place rebuild. Raise the cap via `arcadedb.rebuild.repartition.maxBufferedRids` if heap "
                      + "allows, or drop and re-import the type under the new partitioning.");
            pendingMoveRids.add(currentRid);
            count[0]++;
          } else {
            m.markDirty();
            m.save();
            count[0]++;
            scanCommittedSaves[0]++;
          }
        } else {
          final MutableDocument m = (MutableDocument) rec.modify();
          m.markDirty();
          m.save();
          count[0]++;
          scanCommittedSaves[0]++;
        }
        // Batch only when we own the transaction. Committing inside a caller-supplied TX would leak the user's
        // writes prematurely and leave the trailing batch uncommitted.
        if (implicitTx && count[0] % finalBatchSize == 0) {
          db.commit();
          // Only the records actually saved in this scan phase land in the new layout when the
          // commit fires. Misplaced records counted in {@code count[0]} are still in their old
          // bucket; they'll be rewritten (and counted) in the move phase below.
          committedBefore[0] = scanCommittedSaves[0];
          // Per-batch progress line so an operator running REBUILD on a 100M-record type can observe forward
          // motion via the server log instead of staring at a frozen prompt for many minutes. Logged at INFO
          // so it's on by default but easy to silence by raising the level for this class.
          // Reports the saved-and-committed count separately from the visited count: under
          // {@code repartition = true}, {@code count[0]} includes records buffered in
          // {@code pendingMoveRids} that have NOT yet been written to the new layout, so logging
          // it alone would overstate progress until the move phase runs.
          final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
          final long pendingMoves = finalRepartition ? count[0] - scanCommittedSaves[0] : 0L;
          LogManager.instance().log(this, java.util.logging.Level.INFO,
              "REBUILD TYPE '%s': %,d records visited, %,d saved-and-committed, %,d buffered for move (last batch=%,d, elapsed=%,d ms)",
              null, typeName.getStringValue(), count[0], scanCommittedSaves[0], pendingMoves, finalBatchSize, elapsedMs);
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
        // Move-phase commit cadence tracks the move counter and adds it to the scan-phase
        // committed saves. The error-recovery message uses {@code committedBefore[0]} to tell
        // the operator how many rows are durably in the new layout when the rebuild aborts;
        // adding {@code count[0]} here (which includes misplaced-but-not-yet-moved records)
        // would over-report and confuse the recovery story.
        // Transaction state on entry: under {@code implicitTx} the scan lambda always exits with
        // an open TX (the trailing partial-batch never commits inside the scan, and the last
        // full-batch commit re-opens via {@code db.begin()}). The first move iteration therefore
        // executes inside the still-open scan TX; commit cadence below mirrors the scan phase.
        for (int i = 0; i < pendingMoveRids.size(); i++) {
          final RID oldRid = pendingMoveRids.get(i);
          final var oldRecord = db.lookupByRID(oldRid, true);
          if (oldRecord instanceof Document oldDoc) {
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
            committedBefore[0] = scanCommittedSaves[0] + moved[0];
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
