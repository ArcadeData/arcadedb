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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.LongHashSet;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.ProgressCallback;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * Check graph integrity. If fix mode is enabled, it tries to fix the graph by removing corrupted records and
 * reconnecting edges.
 *
 * @author Luca Garulli (l.garulli@arcadedata.it)
 */
public class GraphDatabaseChecker {
  private final DatabaseInternal database;
  private final GraphEngine      graphEngine;
  /**
   * Modified-page budget of one repair transaction (issue #6128) - see {@link #commitRepairBatchIfFull()}. Read
   * once per checker rather than per repaired record: it is a database-scoped setting, and re-reading it inside the
   * repair loops would put a configuration lookup on the per-record path for a value that cannot change under it.
   */
  private final int              repairBatchPages;

  // Progress reporting (issue #5372): the step identity (name/index/totalSteps) is assigned by the caller
  // (DatabaseChecker owns the step plan); this class emits within-step done/total, throttled to integer
  // percentage changes so the scan hot loops never pay for the callback.
  private ProgressCallback progress;
  private String           progressStepName;
  private int              progressStepIndex;
  private int              progressTotalSteps;
  private long             progressDone;
  private long             progressTotal;
  private int              lastReportedPct;

  public GraphDatabaseChecker(DatabaseInternal database) {
    this.database = database;
    this.graphEngine = database.getGraphEngine();
    this.repairBatchPages = database.getConfiguration()
        .getValueAsInteger(GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES);
  }

  /** Installs the progress receiver and this checker's step identity in the caller's step plan. */
  public GraphDatabaseChecker setProgress(final ProgressCallback progress, final String stepName, final int stepIndex,
      final int totalSteps) {
    this.progress = progress;
    this.progressStepName = stepName;
    this.progressStepIndex = stepIndex;
    this.progressTotalSteps = totalSteps;
    return this;
  }

  /** Starts a (sub-)phase of the current step, emitting it immediately so pollers see the transition. */
  private void progressBegin(final String name, final long total) {
    if (progress == null)
      return;
    progressStepName = name;
    progressDone = 0;
    progressTotal = total;
    lastReportedPct = -1;
    progress.onProgress(progressStepName, progressStepIndex, progressTotalSteps, 0, total);
  }

  /** One unit of work done; emits only when the integer percentage changes. */
  private void progressTick() {
    if (progress == null)
      return;
    ++progressDone;
    if (progressTotal > 0 && progressDone > progressTotal)
      progressDone = progressTotal; // COUNT DRIFT (concurrent writes, placeholders): clamp, never report over 100%
    final int pct = progressTotal > 0 ? (int) (progressDone * 100 / progressTotal) : (int) (progressDone >>> 13);
    if (pct != lastReportedPct) {
      lastReportedPct = pct;
      progress.onProgress(progressStepName, progressStepIndex, progressTotalSteps, progressDone, progressTotal);
    }
  }

  /** Emits the current phase as finished (done == total). */
  private void progressComplete() {
    if (progress == null)
      return;
    if (progressTotal > 0)
      progressDone = progressTotal;
    progress.onProgress(progressStepName, progressStepIndex, progressTotalSteps, progressDone,
        progressTotal > 0 ? progressTotal : progressDone);
  }

  /**
   * Collects every edge-list segment reachable from any vertex's OUT/IN chains (classic chunks, stripe
   * directories, stripe chains and generation-0 chains of promoted super-nodes) and deletes the segments in
   * the dedicated edge-list buckets that are NOT reachable (issue #5375). Orphans are left behind when a
   * broken chain is rebuilt from the surviving edge records, or by historical bugs. MUST only run on a FULL
   * check (no type/bucket filter): a partial walk would classify the unwalked vertices' segments as orphans.
   * Like the rest of fix mode, assumes no concurrent writers - run it in a maintenance window.
   * <p>
   * DATA-SAFETY GUARANTEES (both failure modes here delete live data, so they are guarded rather than left to
   * the caller):
   * <ul>
   *   <li>the segment buckets are identified from the SCHEMA - the engine-created {@code _out_edges}/
   *   {@code _in_edges} bucket of every vertex bucket and every vertex type's super-node stripe pool - never by
   *   matching bucket names alone (see {@link #collectEdgeSegmentBucketIds()}); a user data bucket whose name
   *   merely collides with the naming scheme is therefore never scanned or deleted;</li>
   *   <li>the reclaim FAILS CLOSED: if in phase 1 any vertex record could not be loaded OR its edge chain could
   *   not be fully walked (a segment read failed ANYWHERE along a live chain - unreadable head, or a mid-chain
   *   chunk lookup that threw), that chain's live tail is missing from the reachable set, so the whole deletion
   *   phase is skipped (nothing is deleted) rather than risk destroying it. The skip is DELIBERATELY
   *   database-wide (one unwalkable vertex blocks reclaim everywhere), not scoped to the affected type: a
   *   vertex's segments can land in any edge-list bucket, so a narrower scope cannot be proven safe. Scoping it
   *   is a possible future refinement.</li>
   * </ul>
   * <p>
   * Known limitation (matching the corrupted-records pattern in the vertex/edge checks): the whole reachable set
   * (a position entry for EVERY reachable segment across the database, which dominates memory on a large healthy
   * graph) and the orphan RID list are held in memory and the delete runs inside one transaction, so an extreme
   * database grows them unbounded. Batched commits + a segment-count-bounded reachable set are the mitigation
   * before this is safe on the very large deployments the feature targets. Phase 1 is also a SECOND full vertex
   * scan (the preceding {@code checkVertices} already scanned them all): the reachable set could instead be
   * accumulated during that pass to halve the vertex-scan cost of a full fix - deferred, since keeping the
   * reclaim self-contained is worth the extra pass on an already-damaged database run in a maintenance window.
   * <p>
   * DELIBERATELY ALWAYS-ON: this runs on every full-scope fix, even a no-repair run that reclaims zero segments
   * (a healthy large graph still pays the scans + reachable set). That is intentional - orphans also come from
   * historical bugs, not only from a rebuild in THIS run, so gating on "did we rebuild a chain this run" would
   * never sweep a database that is otherwise clean. An explicit opt-in (e.g. a {@code FIX RECLAIM} keyword) is
   * the alternative if the always-on cost proves unwelcome; left always-on for now.
   */
  public Map<String, Object> reclaimOrphanedEdgeSegments(final int verboseLevel, final int maxWarnings) {
    // Warnings only: this pass reports, it never flags a record corrupted, so the corrupted cap is irrelevant.
    final CheckReport report = new CheckReport(maxWarnings, maxWarnings, verboseLevel);
    final Map<String, Object> stats = new HashMap<>();

    final Map<Integer, LongHashSet> reachable = new HashMap<>();
    long totalVertices = 0;
    final List<DocumentType> vertexTypes = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof LocalVertexType) {
        vertexTypes.add(type);
        totalVertices += database.countType(type.getName(), false);
      }

    long orphans = 0;
    long reclaimed = 0;

    database.begin();
    try {
      // PHASE 1: walk every vertex's chains and mark every reachable segment. LongHashSet keyed by position
      // per bucket keeps the footprint primitive-sized (same approach as the external-property orphan scan).
      // FAIL CLOSED: a vertex we cannot walk leaves its live segments unmarked, so ANY walk failure disables
      // the deletion phase entirely - deleting then would treat live data as garbage.
      final AtomicBoolean reachabilityComplete = new AtomicBoolean(true);
      progressBegin("Reclaiming orphaned edge segments - collecting reachable segments", totalVertices);
      for (final DocumentType type : vertexTypes)
        database.scanType(type.getName(), false, record -> {
          progressTick();
          try {
            final VertexInternal vertex = (VertexInternal) record.asVertex(true);
            // Walk BOTH directions (no short-circuit) so every readable segment is still marked, but a read
            // failure on EITHER chain fails the whole reclaim closed - the chain is live, so an unmarked tail
            // must not be deleted as an orphan.
            final boolean outComplete = markReachableSegments(vertex.getOutEdgesHeadChunk(), reachable);
            final boolean inComplete = markReachableSegments(vertex.getInEdgesHeadChunk(), reachable);
            if (!outComplete || !inComplete) {
              reachabilityComplete.set(false);
              report.warn("vertex " + record.getIdentity()
                  + " edge chain could not be fully walked during the orphan reclaim (a segment read failed); "
                  + "the reclaim will be skipped to avoid deleting live data");
            }
          } catch (final Exception e) {
            // DEFENSIVE TWIN of the scan error callback below: a vertex's edge-pointer prefix is validated at
            // record construction, so a truncated/corrupt vertex normally fails to load inside the scan and
            // surfaces through that callback; this catches any residual failure of asVertex here. Same effect.
            reachabilityComplete.set(false);
            report.warn("vertex " + record.getIdentity() + " could not be walked during the orphan reclaim (error: " + describe(e)
                    + ")");
          }
          return true;
        }, (rid, exception) -> {
          // A vertex record that cannot even be loaded during the scan is the same fail-closed case: its
          // segments were never marked, so the deletion phase must not run.
          reachabilityComplete.set(false);
          report.warn("vertex " + rid + " could not be loaded during the orphan reclaim (error: " + describe(exception) + ")");
          return true;
        });

      if (reachabilityComplete.get()) {
        // PHASE 2: scan ONLY the internal edge-list buckets the graph engine created for the vertices (derived
        // from the schema, never matched by name), and delete anything not marked reachable.
        progressBegin("Reclaiming orphaned edge segments - scanning segment buckets", -1);
        final List<RID> orphansToDelete = new ArrayList<>();
        for (final Integer bucketId : collectEdgeSegmentBucketIds()) {
          final Bucket b = database.getSchema().getBucketById(bucketId);
          final LongHashSet reachableInBucket = reachable.get(bucketId);
          b.scan((rid, view) -> {
            progressTick();
            if (reachableInBucket == null || !reachableInBucket.contains(rid.getPosition()))
              orphansToDelete.add(rid);
            return true;
          }, null);
        }
        orphans = orphansToDelete.size();

        for (final RID orphan : orphansToDelete) {
          try {
            deleteCorruptedRecord(orphan);
            ++reclaimed;
            commitRepairBatchIfFull();
          } catch (final RecordNotFoundException e) {
            // ALREADY GONE
          } catch (final Exception e) {
            report.warn("orphaned edge segment " + orphan + " could not be reclaimed (error: " + describe(e) + ")");
          }
        }
      } else {
        report.warn("orphaned edge segment reclaim skipped: the reachability walk did not complete (one or more vertices "
                + "could not be walked); no segments were deleted to avoid destroying live data");
      }

      if (verboseLevel > 0)
        for (final String warning : report.warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();
      progressComplete();

    } finally {
      stats.put("orphanedEdgeSegments", orphans);
      stats.put("orphanedEdgeSegmentsReclaimed", reclaimed);
      stats.put("warnings", report.warnings);
      stats.put("totalWarnings", report.totalWarnings);
    }
    return stats;
  }

  /**
   * Removes one record this checker decided is beyond repair, and pays the bucket-counter debt that comes with it.
   * <p>
   * {@code LocalBucket.deleteRecord} does NOT touch {@code cachedRecordCount}, and that counter - not a scan - is
   * what {@code count(*)} and {@code countType()} answer from. Every caller that deletes through the bucket
   * therefore owes the matching {@code updateBucketRecordDelta(-1)}, the same accounting
   * {@code LocalDatabase.cascadeDeleteExternalValues} and {@code DatabaseChecker}'s document arm do.
   * <p>
   * Missing it was invisible on a type-wide run, which is why it survived: {@code DatabaseChecker.checkBuckets}
   * recomputes every bucket counter afterwards and repaired the drift as a side effect. The RECORD scope
   * deliberately skips the database-wide passes, so there the type simply kept over-reporting the deleted record
   * for good. Pinned by {@code CheckDatabaseRecordScopeTest#aRecordScopedFixKeepsTheCachedRecordCountConsistent}
   * and its edge twin.
   * <p>
   * Shared by all three delete sites in this class - the vertex arm, the edge arm and the orphan-segment reclaim -
   * rather than repeated: they were byte-identical copies, and the rule is the thing that must not drift apart
   * again. The reclaim's edge-list buckets belong to no type, so nothing user-facing reads their counter today;
   * it goes through here anyway, because "which buckets have a reader" is not a distinction worth encoding in
   * three places.
   *
   * @throws com.arcadedb.exception.RecordNotFoundException if the record is already gone - callers decide whether
   *                                                        that is worth reporting
   */
  private void deleteCorruptedRecord(final RID rid) {
    final Bucket bucket = database.getSchema().getBucketById(rid.getBucketId());
    // LocalBucket.deleteCorruptedRecord escalates to a force delete for a structurally broken chunk chain, which a
    // plain delete cannot clear (#4932). Without it the RECORD scope reported "error on delete" and left the record
    // in place: the bucket-wide pass that force-deletes it (LocalBucket.check) is one of the database-wide passes
    // that scope skips. The instanceof rather than a cast keeps a non-local Bucket implementation on the plain
    // delete instead of failing outright, since the escalation is a repair nicety, not a precondition.
    if (bucket instanceof LocalBucket localBucket)
      localBucket.deleteCorruptedRecord(rid);
    else
      bucket.deleteRecord(rid);
    database.getTransaction().updateBucketRecordDelta(rid.getBucketId(), -1);
  }

  /**
   * Commits the repair so far and opens the next transaction once it has dirtied
   * {@link GlobalConfiguration#CHECK_DATABASE_REPAIR_BATCH_PAGES} pages (issue #6128).
   * <p>
   * Without this the repair of one type - every reconnected edge and every deleted record - was one transaction,
   * and on a replicated database one Raft log entry. {@code RaftTransactionBroker.replicateTransaction} submits it
   * whole and {@code RaftGroupCommitter.submitAndWait} rejects anything above
   * {@code min(appendBufferSize, grpcMessageSizeMax)} with a {@code ReplicatedEntryTooLargeException}, which is not
   * a {@code NeedRetryException} and so is never retried: a repair large enough to matter ran for hours and was
   * then rolled back whole. A schema entry has had a splitter since #4743; a transaction entry has none.
   * <p>
   * PAGES, not repaired records: the entry carries page images, and how many records a repair touched says nothing
   * about how many distinct pages it dirtied. {@code TransactionContext.getModifiedPages()} counts both modified
   * and newly-created pages, which is exactly what the WAL will hold.
   * <p>
   * PRECONDITION ON CALL SITES, because the budget silently stops bounding anything if it is broken: whatever a
   * call site does between two checks must land in {@code modifiedPages}/{@code newPages} by the time the next
   * check runs. Pending INDEX entries do not - {@code TransactionContext} keeps {@code indexChanges} separately
   * (its own {@code hasChanges()} ORs the two) and materialises them into pages only in {@code commit1stPhase},
   * which is after this check. Every current call site is safe on that count: a raw {@code bucket.deleteRecord}
   * and an {@code getOrCreateEdgeList(...).add(...)} maintain no type index. A future call site that performs
   * index-maintained updates would accumulate a backlog invisible here and could overshoot the budget by the whole
   * of it, so bound that by entry count as well before adding one.
   * <p>
   * WHAT THIS CHANGES, stated plainly because it is a semantic change and not only a performance one: a repair is
   * no longer all-or-nothing. A failure part-way through now leaves the earlier batches committed. That is the
   * behaviour a multi-type run has always had - {@code check()} commits each type before starting the next - so
   * this makes one type behave like the whole run rather than inventing a new semantics; and the alternative on a
   * replicated database is not an atomic repair but no repair at all. Set the budget to 0 to get the single
   * transaction back.
   * <p>
   * Only ever called BETWEEN units of repair work, never inside a {@code scanType}/{@code scan} callback:
   * {@code LocalDatabase.scanType} holds the database read lock and owns an implicit transaction for the length of
   * the scan, so committing under it would commit a transaction the scan believes it still owns. Every call site
   * here is therefore in a post-scan apply or delete loop, which is also where the page volume actually is.
   * <p>
   * WHAT THAT LEAVES UNBOUNDED, named rather than implied. #6136 moved the two repairs that used to write from
   * inside the scan - the {@code connectOutgoingEdge}/{@code connectIncomingEdge} back-reference fix-up and
   * {@link #resetChain} - into {@link #applyPendingLinks} and {@link #applyPendingChainResets}, which are post-scan
   * loops and therefore inside this budget. ONE in-scan write remains: the {@code in.remove()}/{@code out.remove()}
   * that prunes a dangling adjacency entry, in both {@link #checkIncomingEdges} and {@link #checkOutgoingEdges}.
   * It is not deferrable in the way the others were - the removal is made through the chunk iterator's live
   * position, and replaying it afterwards would mean re-walking the list per pruned entry, turning a linear pass
   * into a quadratic one - and it is the mildest of the three: it rewrites a chunk page the walk is already
   * reading and never allocates a new one, where the back-reference fix-up appended to a FAR vertex's list and
   * could allocate a chunk per repair. Still unbounded in principle, so a database whose damage is overwhelmingly
   * dangling entries in very many distinct chunks can exceed the budget by that much.
   * <p>
   * A SOFT ceiling: the check happens between units, so a transaction can exceed the budget by whatever the unit in
   * flight dirties. Leave headroom when tuning it close to the replicated-entry limit.
   * <p>
   * WHY THIS WORKS UNDER AN OUTER TRANSACTION, which is the linchpin and not obvious: {@code CHECK DATABASE} runs
   * through the HTTP handler, which wraps the command in its own transaction, so these are NESTED begin/commit
   * pairs. They are not savepoints. {@code DatabaseContext.DatabaseContextTL.pushTransaction} gives each one a
   * genuinely separate {@code TransactionContext}, and {@code commit()} runs the full
   * {@code commit1stPhase}/{@code commit2ndPhase} on THAT context - a real WAL write and, under HA, a real
   * replication round trip. If nesting deferred the write to the outermost commit instead, this whole change would
   * be inert: the repair would still reach Raft as one entry.
   * <p>
   * FAILURE PATH: a batch commit that throws propagates to the caller and leaves no transaction open on the
   * thread - {@code commit()} disposes its context even when the write fails, so the checker does not have to roll
   * back after it. Batches already committed stay committed, which is the semantic change stated above. Pinned by
   * {@code CheckDatabaseRepairBatchFailureTest}.
   */
  private void commitRepairBatchIfFull() {
    if (repairBatchPages <= 0)
      return;
    if (database.getTransaction().getModifiedPages() < repairBatchPages)
      return;
    database.commit();
    database.begin();
  }

  /**
   * The file-ids of the internal edge-list buckets the graph engine created for the registered vertex types: the
   * {@code _out_edges}/{@code _in_edges} bucket of every vertex bucket, and every vertex type's super-node stripe
   * pool. DERIVED FROM THE SCHEMA, never by matching bucket names blindly, so a user data bucket whose name
   * collides with the edge-list naming scheme is never treated as reclaimable. A bucket that IS owned by a type
   * is user data by definition and is excluded - the same collision guard {@link StripedEdgeList#ensureStripePool}
   * uses, since the engine's edge-list buckets are created standalone ({@code schema.createBucket}) and belong to
   * no type.
   */
  private Set<Integer> collectEdgeSegmentBucketIds() {
    final Schema schema = database.getSchema();
    final Set<Integer> ids = new HashSet<>();
    final int configuredStripes = database.getConfiguration().getValueAsInteger(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES);
    for (final DocumentType type : schema.getTypes()) {
      if (!(type instanceof LocalVertexType))
        continue;
      for (final Bucket vb : type.getBuckets(false)) {
        addSegmentBucketIfInternal(ids, vb.getName() + GraphEngine.OUT_EDGES_SUFFIX);
        addSegmentBucketIfInternal(ids, vb.getName() + GraphEngine.IN_EDGES_SUFFIX);
      }
      // Super-node stripe pool (per type). Pools are created contiguously from slot 0 (see
      // GraphEngine.dropVertexType): sweep until the first gap AT OR PAST the configured pool size, stepping over
      // gaps below it (a partially-created pool). No slot 0 means the type never promoted - skip the whole sweep.
      // NOTE: the bound is the LIVE GRAPH_SUPERNODE_STRIPES, not the size persisted at promotion (same assumption
      // as dropVertexType). If it was shrunk since promotion, buckets past the new size are simply not scanned -
      // never deleted, and their live segments are still marked via the phase-1 directory walk - so orphans there
      // just leak; not a data-loss path.
      for (int i = 0; ; i++) {
        final String stripeBucketName = StripedEdgeList.stripeBucketName(type.getName(), i);
        if (!schema.existsBucket(stripeBucketName)) {
          if (i == 0 || i >= configuredStripes)
            break;
          continue;
        }
        addSegmentBucketIfInternal(ids, stripeBucketName);
      }
    }
    return ids;
  }

  /**
   * Adds the named bucket's file-id to {@code ids} only when it exists AND is not owned by a type. A type-owned
   * bucket is user data that merely collides with the edge-list naming scheme and must never be reclaimed.
   */
  private void addSegmentBucketIfInternal(final Set<Integer> ids, final String bucketName) {
    final Schema schema = database.getSchema();
    if (!schema.existsBucket(bucketName))
      return;
    final Bucket b = schema.getBucketByName(bucketName);
    if (schema.getTypeByBucketId(b.getFileId()) != null)
      return; // USER DATA BUCKET colliding with the naming scheme
    ids.add(b.getFileId());
  }

  /**
   * Marks every segment reachable from the given head: a classic chain, or a stripe directory + all its chains.
   *
   * @return false when a segment read FAILED mid-walk (unreadable head, or a chunk lookup threw) so the chain
   * could not be fully accounted for. The caller must then FAIL CLOSED (skip the deletion phase): the chain is
   * currently referenced by a live vertex, so a read error here - unlike genuine corruption the earlier
   * checkVertices rebuild already re-attached - may leave live tail segments unmarked, and deleting them would
   * destroy live data. A healthy chain always returns true.
   */
  private boolean markReachableSegments(final RID head, final Map<Integer, LongHashSet> reachable) {
    if (head == null)
      return true;
    final Record headRecord;
    try {
      headRecord = database.lookupByRID(head, true);
    } catch (final Exception e) {
      // UNREADABLE HEAD: cannot account for this vertex's segments - fail closed rather than risk deleting them.
      return false;
    }
    if (headRecord instanceof StripeDirectory directory) {
      mark(reachable, head);
      boolean complete = true;
      for (int g = 0; g < directory.getGenerationCount(); g++)
        for (int s = 0; s < directory.getStripes(g); s++)
          // Do NOT short-circuit: keep marking the readable stripe chains, just remember the walk was incomplete.
          if (!markChain(directory.getHead(g, s), reachable))
            complete = false;
      return complete;
    }
    return markChain(head, reachable);
  }

  /**
   * Walks a classic chunk chain marking every chunk; stops on cycles and unreadable chunks.
   *
   * @return false when a chunk lookup THREW (a broken/unreadable tail), leaving the rest of the chain unmarked;
   * true when the whole chain was walked (including a cyclic chain fully covered before the cycle guard fired).
   */
  private boolean markChain(final RID head, final Map<Integer, LongHashSet> reachable) {
    RID current = head;
    while (current != null) {
      if (!mark(reachable, current))
        return true; // ALREADY VISITED: cyclic chain fully covered (not a read failure) - guards infinite loop
      try {
        current = ((EdgeSegment) database.lookupByRID(current, true)).getPreviousRID();
      } catch (final Exception e) {
        return false; // BROKEN/UNREADABLE TAIL: cannot confirm the rest of the chain - fail closed
      }
    }
    return true;
  }

  /** @return true when the RID was newly marked as reachable. */
  private boolean mark(final Map<Integer, LongHashSet> reachable, final RID rid) {
    return reachable.computeIfAbsent(rid.getBucketId(), k -> new LongHashSet()).add(rid.getPosition());
  }

  public Map<String, Object> checkVertices(final String typeName, final boolean fix, final int verboseLevel) {
    return checkVertices(typeName, fix, verboseLevel, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  public Map<String, Object> checkVertices(final String typeName, final boolean fix, final int verboseLevel,
      final int maxWarnings, final int maxCorrupted) {
    return checkVertices(typeName, null, fix, verboseLevel, maxWarnings, maxCorrupted);
  }

  /**
   * #5680: same check restricted to {@code scopedRecords} when it is non-null - the {@code CHECK DATABASE RECORD}
   * scope. The per-vertex work and the fix (rebuilding the adjacency from the surviving edge records) are
   * identical; only the enumeration changes, from two passes over every bucket of the type to a lookup per listed
   * RID. That is what makes "one vertex's edge chain is broken, so the vertex cannot be deleted" repairable
   * without paying a full type scan - the strict delete path in {@code GraphEngine.deleteVertex} names this
   * command in its error message.
   * <p>
   * The edge-record scan inside {@link #collectEdgesToReconnect} is NOT scoped: rebuilding an adjacency means finding
   * every surviving edge that points at the vertex, and no index maps endpoints back to edges. So the scoped run
   * saves the vertex passes, not the edge pass.
   * <p>
   * #5764/#5773 - both arms materialise each record EXACTLY ONCE, which is not obvious from the shapes and is the
   * reason the type-wide arm no longer opens with a raw bucket scan of its own. The scoped arm materialises through
   * {@code LocalDatabase.lookupByRID}; the type-wide arm through {@code LocalDatabase.scanType}, whose callback does
   * the same {@code newImmutableRecord(type, rid, view)} from the same raw page view that {@code lookupByRID} builds
   * from {@code bucket.getRecord(rid).copyOfContent()}. Both then hand the record to the shared
   * {@code checkConnectivity}, which opens with the same {@code asVertex(true)} whose {@code loadContent} flag forces
   * the buffer decode. No corruption shape reaches one enumeration but not the other: {@code LocalBucket.getRecord}
   * and {@code LocalBucket.scan} resolve placeholders and multi-page chains identically and skip the same slot
   * markers. Pinned by {@code CheckDatabaseRecordScopeTest.theScopedAndTypeWideRunsAgreeOnAGenuinelyCorruptedRecord}.
   * <p>
   * #5773 - what the dropped type-wide first pass did, and why removing it detects strictly no less. It scanned the
   * buckets, built each record with {@code newImmutableRecord} and called {@code asVertex(true)}, flagging a failure
   * of either. The surviving {@code scanType} pass does the FIRST of those inside {@code LocalBucket.scan}'s per-slot
   * {@code try}, so a construction failure is routed to the {@code errorRecordCallback} below (which warns and flags
   * the record), and the SECOND inside {@code checkConnectivity}'s {@code catch (Throwable)}, which warns and flags
   * it likewise. The dropped pass in fact saw LESS: it passed a {@code null} error callback, so a failure inside the
   * bucket machinery itself (placeholder resolution, a multi-page chain read) was only logged, never reported.
   * <p>
   * What removing it is and is not worth, MEASURED rather than assumed - the record-materialisation count halves,
   * but materialisation is not what the step spends its time on. On a warm 200k-vertex / 200k-edge graph with no
   * super-node, the per-type steps went 173 ms -> 162 ms (vertices) and 155 ms -> 142 ms (edges), about 7% each; a
   * full {@code CHECK DATABASE} went 509 ms -> 472 ms. The pass is cheap because {@code asVertex(true)} only parses
   * the edge pointers, not the properties. The real reasons to drop it are that it was provably redundant and that
   * it emitted a "vertex/edge <rid> cannot be loaded, REMOVING IT" warning on runs without {@code FIX}, which
   * remove nothing - and a duplicate warning per corrupt record besides. Pinned by
   * {@code CheckDatabaseSinglePassTest}.
   */
  public Map<String, Object> checkVertices(final String typeName, final Collection<RID> scopedRecords,
      final boolean fix, final int verboseLevel, final int maxWarnings, final int maxCorrupted) {
    final AtomicLong autoFix = new AtomicLong();
    final CheckReport report = new CheckReport(maxWarnings, maxCorrupted, verboseLevel);
    // Every repair this scan decides on, applied AFTER it under the page budget (issue #6136) - see RepairPlan.
    final RepairPlan plan = new RepairPlan(database);
    final Map<RID, Long> missingReferences = new HashMap<>();
    final Map<RID, String> missingReferenceErrors = new HashMap<>();
    /** Records this run actually removed, surfaced as {@code deletedRecordsAfterFix}. */
    final Set<RID> deletedRecords = new LinkedHashSet<>();
    /** Adjacency entries this run re-linked, of either kind - see the {@code reconnectedEdges} stat (issue #6136). */
    long reconnectedEdges = 0;

    final Map<String, Object> stats = new HashMap<>();

    database.begin();
    try {
      // The connectivity check of ONE vertex: shared by the type-wide scan and the RECORD-scoped enumeration.
      final Consumer<Record> checkConnectivity = record -> {
        progressTick();
        try {
          final Vertex vertex = record.asVertex(true);

          final RID vertexIdentity = vertex.getIdentity();

          // No longer re-assigned from the two checks: neither of them writes the vertex any more (#6136), so the
          // saved mutable copy they used to hand back does not exist and the immutable one stays current.
          checkOutgoingEdges(fix, vertex, vertexIdentity, plan, missingReferences, missingReferenceErrors, report);

          checkIncomingEdges(fix, vertex, vertexIdentity, plan, missingReferences, missingReferenceErrors, report);

        } catch (final Throwable e) {
          report.warn("vertex " + record.getIdentity() + " cannot be loaded (error: " + describe(e) + ")");
          report.corrupt(record.getIdentity());
        }
      };

      if (scopedRecords != null) {
        progressBegin(progressStepName, scopedRecords.size());

        for (final RID rid : scopedRecords) {
          try {
            checkConnectivity.accept(database.lookupByRID(rid, true));
          } catch (final RecordNotFoundException e) {
            // A RID that simply is not there is NOT corruption, and the difference is expensive: a record flagged
            // corrupted puts its BUCKET into affectedBuckets, and CHECK DATABASE ... FIX then drops and rebuilds
            // every index on that bucket - a full bucket scan. Since the RECORD scope exists to be hand-typed
            // after a failed delete, a typo'd or already-deleted RID would otherwise buy exactly the cost the
            // scope was reached for. Reported, not repaired.
            progressTick();
            report.warn("vertex " + rid + " does not exist");
          } catch (final Exception e) {
            // Exception, not Throwable, so an Error raised by the LOOKUP (OOM, StackOverflow) is not recorded as
            // "this record is corrupt" and does not have fix mode delete a healthy record. Scope of that, stated
            // honestly: the shared consumer below still catches Throwable around the connectivity check itself,
            // matching the type-wide path, so an Error raised in THERE is still flagged. Narrowing that one would
            // change the type-wide behaviour too, which is not this change's business.
            progressTick();
            report.warn("vertex " + rid + " cannot be loaded (error: " + describe(e) + ")");
            report.corrupt(rid);
          }
        }
      } else {
        // ONE FULL PASS OVER THE TYPE (#5773): progress total is the record count. countType reads the maintained
        // bucket counters (no scan), so sizing the total is negligible.
        progressBegin(progressStepName, database.countType(typeName, false));

        database.scanType(typeName, false, record -> {
          checkConnectivity.accept(record);
          return true;
        }, (rid, exception) -> {
          progressTick();
          report.warn("vertex " + rid + " cannot be loaded (error: " + describe(exception) + ")");
          report.corrupt(rid);
          return true;
        });
      }

      progressComplete();

      if (fix) {
        reconnectedEdges = applyRepairPlan(plan, report);

        for (final RID rid : report.corruptedRecords) {
          if (rid == null)
            continue;

          try {
            deleteCorruptedRecord(rid);
            // Counted AFTER the delete returns, not before it is attempted (issue #6128). autoFix is what an
            // operator reads to decide whether a run did anything, so it must not include a repair that failed -
            // and the failure here is routine rather than exotic: checkEdges flags both ends of a dangling edge,
            // and the far end is flagged precisely because it is not there, so its delete always raises
            // RecordNotFoundException. One dangling edge used to report two repairs.
            autoFix.incrementAndGet();
            // Reported, not only counted: an operator reads deletedRecordsAfterFix to learn WHICH records a repair
            // removed, and until this arm populated it the answer depended on which pass happened to do the delete -
            // a broken-chain record was listed (LocalBucket.check removed it) and every other corrupt record was not.
            // Bounded by the same cap as report.corruptedRecords, which this iterates.
            deletedRecords.add(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            report.warn("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
          commitRepairBatchIfFull();
        }
      }

      if (verboseLevel > 0)
        for (final String warning : report.warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      putRepairCounters(stats, autoFix.get(), report.prunedDanglingEntries, reconnectedEdges);
      stats.put("deletedRecordsAfterFix", deletedRecords);
      stats.put("corruptedRecords", report.corruptedRecords);
      stats.put("duplicateLightEdges", report.duplicateLightEdges);
      stats.put("invalidLinks", report.invalidLinks);
      stats.put("warnings", report.warnings);
      stats.put("totalWarnings", report.totalWarnings);
      stats.put("totalCorruptedRecords", report.totalCorrupted);
      stats.put("missingReferences", missingReferences);
      stats.put("missingReferenceErrors", missingReferenceErrors);
    }

    return stats;
  }

  /**
   * Performs every repair the vertex scan planned, in the ONE order that is correct (issue #6136).
   * <p>
   * ORDER IS LOAD-BEARING. The chains are dropped FIRST: {@code getOrCreateEdgeList} appends to whatever head
   * pointer it finds, so re-linking a vertex whose unreadable chain is still attached would append to the damaged
   * list instead of building a fresh one. Then the surviving edge records are collected, and only then is anything
   * written into a list - by which point every list being rebuilt is empty.
   * <p>
   * Both kinds of link go through the same apply loop, because an entry rebuilt from a surviving edge record and a
   * back-reference the far vertex was missing are the same write: add (edge, other endpoint) to a list. They are
   * counted and reported apart for the reason given on {@link RepairPlan}.
   *
   * @return how many adjacency entries were re-linked, of either kind
   */
  private long applyRepairPlan(final RepairPlan plan, final CheckReport report) {
    applyPendingChainResets(plan, report);

    if (!plan.reconnectOutEdges.isEmpty() || !plan.reconnectInEdges.isEmpty())
      collectEdgesToReconnect(plan, report);

    final long rebuiltOut = applyPendingLinks(plan.rebuiltOutLinks, Vertex.DIRECTION.OUT, null, report);
    if (rebuiltOut > 0)
      report.warn("reconnected " + rebuiltOut + " outgoing edges");

    final long rebuiltIn = applyPendingLinks(plan.rebuiltInLinks, Vertex.DIRECTION.IN, null, report);
    if (rebuiltIn > 0)
      report.warn("reconnected " + rebuiltIn + " incoming edges");

    final long backRefs = applyPendingLinks(plan.backRefOutLinks, Vertex.DIRECTION.OUT, plan.reconnectOutEdges, report)
        + applyPendingLinks(plan.backRefInLinks, Vertex.DIRECTION.IN, plan.reconnectInEdges, report);
    if (backRefs > 0)
      report.warn("re-linked " + backRefs + " edges that were not connected from the other side");

    return rebuiltOut + rebuiltIn + backRefs;
  }

  /**
   * Plans one back-reference, unless this walk of THIS list already planned one for the same edge (issue #6136).
   * <p>
   * The de-duplication is needed because the repair is deferred: the {@code isConnectedTo} probe that used to
   * suppress a second attempt answered on a list the previous repair had already written to, and now it answers on
   * the pre-repair list. So a list naming the same edge twice - itself corruption, and reported as such - would
   * plant two back-references where one used to be planted. It is scoped to the one list because that is the only
   * scope in which the duplicate can arise: across vertices the planned entries differ in the endpoint they
   * record, which is exactly what the probe distinguished too.
   * <p>
   * Shared by {@link #checkIncomingEdges} and {@link #checkOutgoingEdges} rather than copied into each: the two
   * drifting apart is what produced the bug {@link #handleDanglingEdgeListEntry} exists to fix.
   *
   * @param seen the caller's set, created on first use so a healthy list never allocates one
   *
   * @return the set to keep, whether it was just created or was already there
   */
  private static EdgeIdentitySet planBackReference(EdgeIdentitySet seen, final PendingLinks target,
      final RID farVertex, final RID edge, final RID nearVertex) {
    if (seen == null)
      seen = new EdgeIdentitySet();
    if (seen.add(edge))
      target.add(farVertex, edge, nearVertex);
    return seen;
  }

  /**
   * Drops the unreadable chains the scan decided to rebuild (issue #6136, item 2). Every {@link #resetChain} call
   * site used to do this from inside the {@code scanType} callback, one vertex record write per damaged chain and
   * none of them inside the page budget; every one of those sites ALSO registered its vertex in one of the two
   * reconnect sets, so those sets already were the complete list and deferring the write needs no new state.
   * <p>
   * Deferring it changes what the REST of the scan observes about an already-visited vertex, and the two places
   * that can see the difference both improve: a far vertex whose list is unreadable answers the
   * {@code isConnectedTo} probe with an exception instead of a silent empty list, and the probe's handler already
   * treats that as "register it for a rebuild" (the set membership test that follows is what decides, and it is
   * unchanged); and the "current vertex points at ANOTHER vertex's list" test at the head-chunk comparison now
   * sees the shared chunk it is looking for rather than the {@code null} an earlier reset had already written,
   * which is the case that test exists to catch.
   * <p>
   * A vertex registered for BOTH directions is loaded and saved twice. Left as two passes rather than merged
   * through a union map: the second load reads the first one's uncommitted image, so both nulls survive, and the
   * two writes land on the same page - the merge would buy a map allocation and no page.
   */
  private void applyPendingChainResets(final RepairPlan plan, final CheckReport report) {
    for (final RID rid : plan.reconnectOutEdges)
      resetChain(rid, Vertex.DIRECTION.OUT, report);
    for (final RID rid : plan.reconnectInEdges)
      resetChain(rid, Vertex.DIRECTION.IN, report);
  }

  /**
   * Collects the surviving edge records that belong in the chains being rebuilt. NOTE: this is a FULL scan of
   * every edge type, even when a single vertex needs reconnection - acceptable because it runs only in fix mode on
   * an already-damaged database, and the pre-existing reconnect path had the same cost. A rebuilt entry may still
   * point to a far endpoint vertex that no longer exists (the edge record survives, its target does not): that is
   * the same behaviour as before and the {@code checkEdges} pass reports it.
   * <p>
   * Collects RIDs, not records: the {@code ArrayList<Edge>} this used to fill held a fully materialised edge per
   * entry and was the second unbounded accumulation #6136 names - see {@link PendingLinks}.
   */
  private void collectEdgesToReconnect(final RepairPlan plan, final CheckReport report) {
    // BROWSE ALL THE EDGES AND COLLECT THE ONES PART OF THE RECONNECTION
    final List<EdgeType> edgeTypes = new ArrayList<>();
    for (DocumentType schemaType : database.getSchema().getTypes()) {
      if (schemaType instanceof EdgeType t)
        edgeTypes.add(t);
    }

    progressBegin(progressStepName == null ? "Rebuilding edge lists" : progressStepName + " - rebuilding edge lists", -1);

    for (EdgeType edgeType : edgeTypes) {
      final boolean bidirectional = edgeType.isBidirectional();
      // Scan with an error callback: on a damaged database an unreadable edge record must not abort the whole
      // rebuild - it is skipped (checkEdges reports/deletes it), the surviving records still reconnect.
      database.scanType(edgeType.getName(), false, record -> {
        progressTick();
        try {
          final Edge e = record.asEdge(true);
          if (report.corruptedRecords.contains(e.getIdentity()))
            // ABOUT TO BE DELETED BY THE FIX: re-adding it would rebuild a dangling entry
            return true;
          if (plan.reconnectOutEdges.contains(e.getOut()))
            plan.rebuiltOutLinks.add(e.getOut(), e.getIdentity(), e.getIn());
          // A unidirectional edge is never stored in the target's IN list: rebuilding it there would invent
          // adjacency that never existed.
          if (bidirectional && plan.reconnectInEdges.contains(e.getIn()))
            plan.rebuiltInLinks.add(e.getIn(), e.getIdentity(), e.getOut());
        } catch (final Exception e) {
          warnUnreadableEdgeDuringRebuild(report, record.getIdentity(), e);
        }
        return true;
      }, (rid, exception) -> {
        warnUnreadableEdgeDuringRebuild(report, rid, exception);
        return true;
      });
    }
  }

  /**
   * Writes the planned adjacency entries, committing every {@code arcadedb.checkDatabaseRepairBatchPages} dirtied
   * pages. This is the loop the in-scan repairs of #6136 were moved into: it is a post-scan apply, so
   * {@link #commitRepairBatchIfFull()} is legal here in a way it never was inside the scan.
   *
   * @param skipVertices when non-null, vertices whose whole list is being rebuilt from the surviving edge records:
   *                     a back-reference planned for one of them is dropped rather than written, because the
   *                     rebuild re-creates the entry anyway. The set can still grow AFTER the entry was planned -
   *                     a later vertex in the same scan can register this one - which is why the test is repeated
   *                     here against its final contents instead of being left to the one made during the scan.
   *
   * @return how many entries were actually written
   */
  private long applyPendingLinks(final PendingLinks pending, final Vertex.DIRECTION direction,
      final Set<RID> skipVertices, final CheckReport report) {
    long applied = 0;
    for (int i = 0; i < pending.size(); i++) {
      final RID vertexRID = pending.vertex(i);
      if (skipVertices != null && skipVertices.contains(vertexRID))
        continue;

      final RID edgeRID = pending.edge(i);
      if (report.corruptedRecords.contains(edgeRID))
        // ABOUT TO BE DELETED BY THE FIX: re-linking it would build a dangling entry. Same guard the rebuild scan
        // applies, repeated here because the corrupted set also keeps growing while the scan runs.
        continue;

      try {
        final MutableVertex vertex = database.lookupByRID(vertexRID, true).asVertex(true).modify();
        // getOrCreateEdgeList dispatches on the head record type, so promoted (striped) vertices work too
        graphEngine.getOrCreateEdgeList(vertex, direction).add(edgeRID, pending.other(i));
        ++applied;
      } catch (final RecordNotFoundException e) {
        // The vertex is gone - another arm of this same run deleted it - so there is no list to link into.
        report.warn("vertex " + vertexRID + " no longer exists, edge " + edgeRID + " was not re-linked to it");
      } catch (final Exception e) {
        report.warn("vertex " + vertexRID + " could not be re-linked to edge " + edgeRID + " (error: " + describe(e) + ")");
      }
      commitRepairBatchIfFull();
    }
    return applied;
  }

  private static void warnUnreadableEdgeDuringRebuild(final CheckReport report, final Object rid,
      final Throwable error) {
    report.warn("edge " + rid + " could not be read during the edge-list rebuild, skipping it (error: " + describe(error) + ")");
  }

  /**
   * Marks a {@link ClassCastException} thrown specifically by casting THIS list entry's {@code edgeRID} to an
   * {@code Edge} (a vertex wrongly linked into the adjacency list by an older build), as distinct from a
   * {@code ClassCastException} that can be thrown later in the same {@code try} block while processing a
   * DIFFERENT RID - the sibling-entry rescan ({@code nextEntry.getFirst().asEdge(true)}) or the far-vertex
   * resolution ({@code edge.getIn()/getOut().asVertex(false)}). Only the former means "this list entry is
   * simply dangling, the record it points at is fine": the latter means some OTHER record is unreadable while
   * {@code edgeRID} itself already cast to a real edge, which is genuine corruption and must keep falling into
   * the generic handler that flags the record for deletion in FIX mode.
   */
  private static final class DanglingEdgeListEntryException extends RuntimeException {
    DanglingEdgeListEntryException(final ClassCastException cause) {
      super(cause);
    }
  }

  private static Edge asEdgeOrDanglingEntry(final RID edgeRID) {
    try {
      return edgeRID.asEdge(true);
    } catch (final ClassCastException e) {
      throw new DanglingEdgeListEntryException(e);
    }
  }

  /**
   * Shared by the {@code DanglingEdgeListEntryException} catch in both {@link #checkIncomingEdges} and
   * {@link #checkOutgoingEdges}: the record loaded fine, only the (Edge) cast on {@code edgeRID} failed. Drop
   * just the dangling LIST entry and NEVER schedule the pointed-to record for deletion - fix mode raw-deletes
   * every {@code corruptedRecords} RID with {@code bucket.deleteRecord}, which would destroy that valid record
   * and bypass graph-aware cleanup (a deleted vertex would leave its OWN edges dangling, cascading the
   * damage). Report it and repair the list, nothing else. Extracted so the two copies - drifting apart is
   * exactly what caused the bug this handler fixes - cannot go out of sync again.
   */
  private static void handleDanglingEdgeListEntry(final CheckReport report, final RID edgeRID,
      final DanglingEdgeListEntryException e, final boolean fix) {
    report.warn("edge " + edgeRID + " error on loading (error: " + describe(e.getCause()) + ")"
            + (fix ? ", dropping the dangling list entry (record preserved)" : ""));
    ++report.invalidLinks;
  }

  private void checkIncomingEdges(final boolean fix, final Vertex vertex, final RID vertexIdentity,
      final RepairPlan plan, final Map<RID, Long> missingReferences, final Map<RID, String> missingReferenceErrors,
      final CheckReport report) {
    final Set<RID> reconnectInEdges = plan.reconnectInEdges;
    final Set<RID> reconnectOutEdges = plan.reconnectOutEdges;

    // Already condemned by an EARLIER vertex's far-endpoint probe: nothing here can improve on a list that is
    // about to be dropped and rebuilt from the surviving edge records, and walking it can make things worse -
    // an entry pointing at an edge that belongs to another vertex is diagnosed as a corrupt EDGE and deleted in
    // fix mode, destroying a record the rebuild would have re-linked. Before #6136 this was implicit and
    // scan-order dependent: resetChain ran inside the scan, so a vertex condemned before its own turn arrived
    // with a null head chunk and fell out at the guard below, while the same vertex reached first was walked in
    // full. Deferring the write made the skip explicit, which also makes it deterministic.
    if (fix && reconnectInEdges.contains(vertexIdentity))
      return;

    if (((VertexInternal) vertex).getInEdgesHeadChunk() != null) {
      EdgeLinkedList inEdges = null;
      try {
        inEdges = graphEngine.getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.IN);
      } catch (Exception e) {
        // IGNORE IT: HANDLED AS AN UNREADABLE LIST BELOW
      }

      if (inEdges == null) {
        final RID headChunkRID = ((VertexInternal) vertex).getInEdgesHeadChunk();
        report.warn("vertex " + vertexIdentity + " in edges record " + headChunkRID
            + " is not valid" + (fix ? ", rebuilding the edge list from the surviving edge records" : ""));
        if (fix)
          reconnectInEdges.add(vertexIdentity);
      } else {
        Iterator<Pair<RID, RID>> in = null;
        // Back-references already planned WHILE WALKING THIS ONE LIST (issue #6136). Deferring the repair means a
        // second entry for the same edge no longer sees the first one's write, and the isConnectedTo probe that
        // used to suppress it answers on the pre-repair list; without this a list that names the same edge twice -
        // itself corruption, and reported as such - would plant two back-references where one used to be planted.
        // Scoped to the list because that is the only scope in which the duplicate can arise: across vertices the
        // planned entries differ in the endpoint they record, which is exactly what the probe distinguished too.
        // Lazily created, so a healthy list never pays for it.
        EdgeIdentitySet plannedBackRefs = null;
        boolean chainBroken = false;
        String chainError = null;
        try {
          in = inEdges.entryIterator();
        } catch (final Exception e) {
          chainBroken = true;
          chainError = describe(e);
        }

        while (in != null) {
          try {
            // hasNext() HOPS ONTO THE NEXT CHUNK OF THE LINKED LIST, so an unreadable chunk fails HERE, not in
            // next(). Before this guard the failure escaped the walk, the scan callback flagged the whole VERTEX
            // as corrupted and fix mode deleted it: the vertex was lost over a repairable adjacency list.
            if (!in.hasNext())
              break;
          } catch (final Exception e) {
            chainBroken = true;
            chainError = describe(e);
            break;
          }
          try {
            final Pair<RID, RID> current = in.next();
            final RID edgeRID = current.getFirst();
            final RID vertexRID = current.getSecond();

            boolean removeEntry = false;

            if (edgeRID == null) {
              report.warn("outgoing edge null from vertex " + vertexIdentity);
              removeEntry = true;
              ++report.invalidLinks;
            } else if (vertexRID == null) {
              report.warn("outgoing vertex null from vertex " + vertexIdentity);
              report.corrupt(edgeRID);
              removeEntry = true;
              ++report.invalidLinks;
            } else {
              if (edgeRID.getPosition() < 0)
                // LIGHTWEIGHT EDGE
                continue;

              try {
                final Edge edge = asEdgeOrDanglingEntry(edgeRID);

                VertexInternal inVertex = null;

                if (edge.getOut() == null || !edge.getOut().isValid()) {
                  report.warn("edge " + edgeRID + " has an invalid outgoing link " + edge.getIn());
                  report.corrupt(edgeRID);
                  removeEntry = true;
                  ++report.invalidLinks;
                } else {
                  try {
                    inVertex = (VertexInternal) edge.getOutVertex().asVertex(true);
                  } catch (final RecordNotFoundException e) {
                    report.warn("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " that is not found (deleted?)");
                    trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getOut(), describe(e));
                    report.corrupt(edgeRID);
                    removeEntry = true;
                    report.corrupt(edge.getOut());
                    ++report.invalidLinks;
                  } catch (final Exception e) {
                    // UNKNOWN ERROR ON LOADING
                    report.warn("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " which cannot be loaded (error: "
                            + describe(e) + ")");
                    trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getOut(), describe(e));
                    report.corrupt(edgeRID);
                    removeEntry = true;
                    report.corrupt(edge.getOut());
                  }
                }

                if (!edge.getIn().equals(vertexIdentity)) {
                  report.warn("edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected " + vertexIdentity);

                  // CHECK ALL INCOMING EDGES
                  int totalEdges = 0;
                  int totalEdgesOk = 0;
                  int totalEdgesError = 0;
                  int totalEdgesErrorFromSameVertex = 0;
                  final Iterator<Pair<RID, RID>> inEdgeIterator = inEdges.entryIterator();
                  while (inEdgeIterator.hasNext()) {
                    final Pair<RID, RID> nextEntry = inEdgeIterator.next();

                    ++totalEdges;

                    final RID edgeIn = nextEntry.getFirst().asEdge(true).getIn();
                    if (edgeIn.equals(vertexIdentity))
                      ++totalEdgesOk;
                    else if (edgeIn.equals(edge.getIn()))
                      ++totalEdgesErrorFromSameVertex;
                    else
                      ++totalEdgesError;
                  }
                  report.warn("edge " + edgeRID + " has an incoming link " + edge.getOut() + " different from expected " + vertexIdentity
                          + ". Found " + totalEdges + " edges, of which " + totalEdgesOk + " are correct, "
                          + totalEdgesErrorFromSameVertex + " are from the same vertex and " + totalEdgesError + " are different");

                  if (totalEdges == totalEdgesErrorFromSameVertex) {
                    // ORIGINAL OUT VERTEX POINTER MUST BE WRONG, CHECKING
                    final VertexInternal wrongInVertex = (VertexInternal) edge.getIn().asVertex(false);
                    if (((VertexInternal) vertex).getInEdgesHeadChunk().equals(wrongInVertex.getInEdgesHeadChunk())) {
                      // CURRENT VERTEX POINTS TO ANOTHER LINKED LIST. SEARCHING FOR ITS CORRECT LINKED LIST LATER.
                      // Mutate ONLY in fix mode: a plain check must stay read-only.
                      if (fix)
                        reconnectInEdges.add(vertexIdentity);

                      // SKIP THE REST OF THE EDGES
                      break;
                    } else {
                      report.corrupt(edgeRID);
                      removeEntry = true;
                      ++report.invalidLinks;
                    }
                  } else {
                    report.corrupt(edgeRID);
                    removeEntry = true;
                    ++report.invalidLinks;
                  }

                } else if (!edge.getOut().equals(vertexRID)) {
                  report.warn("edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected " + vertexRID);
                  report.corrupt(edgeRID);
                  removeEntry = true;
                  ++report.invalidLinks;
                }

                if (((EdgeType) edge.getType()).isBidirectional() && inVertex != null) {
                  Boolean connected = null;
                  try {
                    connected = inVertex.isConnectedTo(vertexIdentity, Vertex.DIRECTION.OUT, edge.getTypeName());
                  } catch (final Exception probeError) {
                    // The FAR vertex's OUT list is unreadable: never blame this edge record for it (before this
                    // guard the probe failure flagged the edge as corrupted and fix mode deleted a VALID edge).
                    // Register the far vertex so its list is rebuilt from the surviving edge records instead.
                    if (reconnectOutEdges.add(inVertex.getIdentity()))
                      report.warn("vertex " + inVertex.getIdentity() + " outgoing edge list is unreadable (error: "
                              + describe(probeError) + ")" + (fix ? ", rebuilding it from the surviving edge records" : ""));
                  }
                  if (connected != null && !connected && !reconnectOutEdges.contains(inVertex.getIdentity())) {
                    report.warn("edge " + edgeRID + " was not connected from the incoming vertex " + edge.getOut() + " to the vertex "
                            + vertexIdentity);
                    // PLANNED, not written (issue #6136): this is the repair that wrote one far vertex per defective
                    // edge from inside the scan, where nothing could commit. Same write, applied afterwards under
                    // the page budget - connectOutgoingEdge is getOrCreateEdgeList(v, OUT).add(edge, target).
                    if (fix)
                      plannedBackRefs = planBackReference(plannedBackRefs, plan.backRefOutLinks,
                          inVertex.getIdentity(), edge.getIdentity(), vertexIdentity);
                  }
                }

              } catch (final RecordNotFoundException e) {
                report.warn("edge " + edgeRID + " not found");
                report.corrupt(edgeRID);
                removeEntry = true;
                ++report.invalidLinks;
              } catch (final DanglingEdgeListEntryException e) {
                handleDanglingEdgeListEntry(report, edgeRID, e, fix);
                removeEntry = true;
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING - also catches a ClassCastException thrown while processing a
                // DIFFERENT RID than edgeRID (the sibling-entry rescan or the far-vertex resolution above):
                // edgeRID itself already cast fine, so this IS genuine corruption, not a dangling list entry.
                report.warn("edge " + edgeRID + " error on loading (error: " + describe(e) + ")");
                report.corrupt(edgeRID);
                removeEntry = true;
              }
            }

            if (fix && removeEntry) {
              in.remove();
              // Pruning a dangling list entry IS a repair, and counting it is what keeps autoFix meaning "repairs
              // performed" now that it no longer counts deletes that failed (issue #6128). Without this an operator
              // who ran FIX over a database whose only damage was dangling references - the commonest shape, since
              // the far record is usually already gone - would read autoFix = 0 and conclude nothing was done.
              ++report.prunedDanglingEntries;
            }
          } catch (Exception e) {
            // UNKNOWN ERROR WHILE WALKING THE LIST: the chain is unreliable, rebuild it below
            chainBroken = true;
            chainError = describe(e);
            break;
          }
        }

        if (chainBroken) {
          report.warn("error on loading incoming edges from vertex " + vertexIdentity + " (error: " + chainError + ")"
                  + (fix ? ", rebuilding the edge list from the surviving edge records" : ""));
          if (fix)
            reconnectInEdges.add(vertexIdentity);
        }
      }
    }
  }

  private void checkOutgoingEdges(final boolean fix, final Vertex vertex, final RID vertexIdentity,
      final RepairPlan plan, final Map<RID, Long> missingReferences, final Map<RID, String> missingReferenceErrors,
      final CheckReport report) {
    final Set<RID> reconnectOutEdges = plan.reconnectOutEdges;
    final Set<RID> reconnectInEdges = plan.reconnectInEdges;

    // See the twin in checkIncomingEdges: a list already condemned by an earlier vertex's probe is skipped, which
    // is what the immediate resetChain used to achieve by leaving a null head chunk behind (issue #6136).
    if (fix && reconnectOutEdges.contains(vertexIdentity))
      return;

    // CHECK THE EDGE IS CONNECTED FROM THE OTHER SIDE
    if (((VertexInternal) vertex).getOutEdgesHeadChunk() != null) {
      EdgeLinkedList outEdges = null;
      try {
        outEdges = graphEngine.getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.OUT);
      } catch (Exception e) {
        // IGNORE IT: HANDLED AS AN UNREADABLE LIST BELOW
      }

      if (outEdges == null) {
        final RID headChunkRID = ((VertexInternal) vertex).getOutEdgesHeadChunk();
        report.warn("vertex " + vertexIdentity + " out edges record " + headChunkRID
            + " is not valid" + (fix ? ", rebuilding the edge list from the surviving edge records" : ""));
        if (fix)
          reconnectOutEdges.add(vertexIdentity);
      } else {
        Iterator<Pair<RID, RID>> out = null;
        // Lazily created: only a vertex that actually has lightweight edges pays for it.
        EdgeIdentitySet seenLightEdges = null;
        // Per-list de-duplication of the planned back-references - see the twin in checkIncomingEdges (#6136).
        EdgeIdentitySet plannedBackRefs = null;
        boolean chainBroken = false;
        String chainError = null;
        try {
          out = outEdges.entryIterator();
        } catch (final Exception e) {
          chainBroken = true;
          chainError = describe(e);
        }

        while (out != null) {
          try {
            // hasNext() HOPS ONTO THE NEXT CHUNK OF THE LINKED LIST, so an unreadable chunk fails HERE, not in
            // next(). Before this guard the failure escaped the walk, the scan callback flagged the whole VERTEX
            // as corrupted and fix mode deleted it: the vertex was lost over a repairable adjacency list.
            if (!out.hasNext())
              break;
          } catch (final Exception e) {
            chainBroken = true;
            chainError = describe(e);
            break;
          }
          try {
            final Pair<RID, RID> current = out.next();
            final RID edgeRID = current.getFirst();
            final RID vertexRID = current.getSecond();

            boolean removeEntry = false;

            VertexInternal outVertex = null;

            if (edgeRID == null) {
              report.warn("outgoing edge null from vertex " + vertexIdentity);
              removeEntry = true;
              ++report.invalidLinks;
            } else if (vertexRID == null) {
              report.warn("outgoing vertex null from vertex " + vertexIdentity);
              report.corrupt(edgeRID);
              removeEntry = true;
              ++report.invalidLinks;
            } else {
              try {
                if (edgeRID.getPosition() < 0) {
                  // LIGHTWEIGHT EDGE: there is no record to validate. What CAN go wrong is the same edge appearing
                  // twice - a lightweight edge is the triple (type, out, in), so two entries with the same edge-type
                  // bucket and the same destination are one edge stored twice. That state is reachable because the
                  // uniqueness check is opt-in (it is O(degree)), and it is harmless to read, but it makes
                  // traversals yield the edge twice and makes delete() need repeating.
                  //
                  // Reported, never auto-fixed. The duplicate exists in the source vertex's OUT list and in the
                  // target's IN list, which are walked in separate passes, so collapsing one side here could leave
                  // the two sides holding different counts - a worse state than the one being repaired. Removing
                  // the extra copy is a one-line application fix (delete the edge once per copy).
                  if (seenLightEdges == null)
                    seenLightEdges = new EdgeIdentitySet();

                  if (!seenLightEdges.add(new LightEdgeRID(vertex.getDatabase(), edgeRID.getBucketId(), vertexIdentity,
                      vertexRID)))
                    // Counted, not warned: warnings mean "something is wrong with this database", and a duplicate
                    // lightweight edge is a modelling advisory, not damage. It reads fine, it just yields the edge
                    // twice. Counted once per duplicated EDGE - only the OUT lists are scanned, and every edge
                    // appears in exactly one of them, so the IN side would only double the same finding.
                    ++report.duplicateLightEdges;
                  continue;
                }

                final Edge edge = asEdgeOrDanglingEntry(edgeRID);

                if (edge.getIn() == null || !edge.getIn().isValid()) {
                  report.warn("edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
                  report.corrupt(edgeRID);
                  removeEntry = true;
                  ++report.invalidLinks;
                } else {
                  try {
                    outVertex = (VertexInternal) edge.getInVertex().asVertex(true);
                  } catch (final RecordNotFoundException e) {
                    report.warn("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " that is not found (deleted?)");
                    trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getIn(), describe(e));
                    report.corrupt(edgeRID);
                    removeEntry = true;
                    report.corrupt(edge.getIn());
                    ++report.invalidLinks;
                  } catch (final Exception e) {
                    // UNKNOWN ERROR ON LOADING
                    report.warn("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " which cannot be loaded (error: "
                            + describe(e) + ")");
                    trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getIn(), describe(e));
                    report.corrupt(edgeRID);
                    removeEntry = true;
                    report.corrupt(edge.getIn());
                  }
                }

                if (!edge.getOut().equals(vertexIdentity)) {
                  // CHECK ALL OUT EDGES
                  int totalEdges = 0;
                  int totalEdgesOk = 0;
                  int totalEdgesError = 0;
                  int totalEdgesErrorFromSameVertex = 0;

                  final Iterator<Pair<RID, RID>> outEdgesIterator = outEdges.entryIterator();
                  while (outEdgesIterator.hasNext()) {
                    final Pair<RID, RID> nextEntry = outEdgesIterator.next();

                    ++totalEdges;

                    final RID edgeOut = nextEntry.getFirst().asEdge(true).getOut();
                    if (edgeOut.equals(vertexIdentity))
                      ++totalEdgesOk;
                    else if (edgeOut.equals(edge.getOut()))
                      ++totalEdgesErrorFromSameVertex;
                    else
                      ++totalEdgesError;
                  }
                  report.warn("edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected " + vertexIdentity
                          + ". Found " + totalEdges + " edges, of which " + totalEdgesOk + " are correct, "
                          + totalEdgesErrorFromSameVertex + " are from the same vertex and " + totalEdgesError + " are different");

                  if (totalEdges == totalEdgesErrorFromSameVertex) {
                    // ORIGINAL OUT VERTEX POINTER MUST BE WRONG, CHECKING
                    final VertexInternal wrongOutVertex = (VertexInternal) edge.getOut().asVertex(false);
                    if (((VertexInternal) vertex).getOutEdgesHeadChunk().equals(wrongOutVertex.getOutEdgesHeadChunk())) {
                      // CURRENT VERTEX POINTS TO ANOTHER LINKED LIST. SEARCHING FOR ITS CORRECT LINKED LIST LATER.
                      // Mutate ONLY in fix mode: a plain check must stay read-only.
                      if (fix)
                        reconnectOutEdges.add(vertexIdentity);

                      // SKIP THE REST OF THE EDGES
                      break;

                    } else {
                      report.corrupt(edgeRID);
                      removeEntry = true;
                      ++report.invalidLinks;
                    }
                  } else {
                    report.corrupt(edgeRID);
                    removeEntry = true;
                    ++report.invalidLinks;
                  }

                } else if (!edge.getIn().equals(vertexRID)) {
                  report.warn("edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected " + vertexRID);
                  report.corrupt(edgeRID);
                  removeEntry = true;
                  ++report.invalidLinks;
                }

                if (((EdgeType) edge.getType()).isBidirectional() && outVertex != null) {
                  // CHECK THE EDGE IS CONNECTED FROM THE OTHER SIDE
                  Boolean connected = null;
                  try {
                    connected = outVertex.isConnectedTo(vertexIdentity, Vertex.DIRECTION.IN, edge.getTypeName());
                  } catch (final Exception probeError) {
                    // The FAR vertex's IN list is unreadable: never blame this edge record for it (before this
                    // guard the probe failure flagged the edge as corrupted and fix mode deleted a VALID edge).
                    // Register the far vertex so its list is rebuilt from the surviving edge records instead.
                    if (reconnectInEdges.add(outVertex.getIdentity()))
                      report.warn("vertex " + outVertex.getIdentity() + " incoming edge list is unreadable (error: "
                              + describe(probeError) + ")" + (fix ? ", rebuilding it from the surviving edge records" : ""));
                  }
                  if (connected != null && !connected && !reconnectInEdges.contains(outVertex.getIdentity())) {
                    report.warn("edge " + edgeRID + " was not connected from the outgoing vertex " + edge.getIn() + " back to the vertex "
                            + vertexIdentity);
                    // PLANNED, not written - see the twin in checkIncomingEdges (issue #6136), including why the
                    // per-list de-duplication is needed now that the probe no longer sees the previous repair.
                    if (fix)
                      plannedBackRefs = planBackReference(plannedBackRefs, plan.backRefInLinks,
                          outVertex.getIdentity(), edgeRID, vertexIdentity);
                  }
                }

              } catch (final RecordNotFoundException e) {
                report.warn("edge " + edgeRID + " not found");
                report.corrupt(edgeRID);
                removeEntry = true;
                ++report.invalidLinks;
              } catch (final DanglingEdgeListEntryException e) {
                handleDanglingEdgeListEntry(report, edgeRID, e, fix);
                removeEntry = true;
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING - also catches a ClassCastException thrown while processing a
                // DIFFERENT RID than edgeRID (the sibling-entry rescan or the far-vertex resolution above):
                // edgeRID itself already cast fine, so this IS genuine corruption, not a dangling list entry.
                report.warn("edge " + edgeRID + " error on loading (error: " + describe(e) + ")");
                report.corrupt(edgeRID);
                removeEntry = true;
              }
            }

            if (fix && removeEntry) {
              out.remove();
              // See the twin in checkIncomingEdges: a pruned dangling entry is a repair and is counted as one.
              ++report.prunedDanglingEntries;
            }

          } catch (Exception e) {
            // UNKNOWN ERROR WHILE WALKING THE LIST: the chain is unreliable, rebuild it below
            chainBroken = true;
            chainError = describe(e);
            break;
          }
        }

        if (chainBroken) {
          report.warn("error on loading outgoing edges from vertex " + vertexIdentity + " (error: " + chainError + ")"
                  + (fix ? ", rebuilding the edge list from the surviving edge records" : ""));
          if (fix)
            reconnectOutEdges.add(vertexIdentity);
        }
      }
    }
  }

  /**
   * Nulls the vertex's head-chunk pointer for the given direction, dropping the unreadable chain so the
   * adjacency can be rebuilt from the surviving edge records by {@link #collectEdgesToReconnect}. Each edge record
   * stores its own out/in vertex RIDs, so losing the linked list does not lose the graph.
   * <p>
   * Takes a RID and re-loads: since #6136 this only ever runs AFTER the scan that decided on it, so there is no
   * live vertex object to hand it. Loaded with content - {@code modify()} copies whatever the immutable holds, so
   * a lazily-loaded one would save the properties away.
   */
  private void resetChain(final RID vertexRID, final Vertex.DIRECTION direction, final CheckReport report) {
    try {
      final MutableVertex mutable = database.lookupByRID(vertexRID, true).asVertex(true).modify();
      if (direction == Vertex.DIRECTION.OUT)
        mutable.setOutEdgesHeadChunk(null);
      else
        mutable.setInEdgesHeadChunk(null);
      mutable.save();
    } catch (final RecordNotFoundException e) {
      // The vertex is gone - another arm of this same run deleted it - so there is no chain left to drop.
    } catch (final Exception e) {
      report.warn("vertex " + vertexRID + " " + direction + " edge list could not be dropped for rebuild (error: "
          + describe(e) + ")");
    }
    commitRepairBatchIfFull();
  }

  public Map<String, Object> checkEdges(final String typeName, final boolean fix, final int verboseLevel) {
    return checkEdges(typeName, fix, verboseLevel, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  public Map<String, Object> checkEdges(final String typeName, final boolean fix, final int verboseLevel,
      final int maxWarnings, final int maxCorrupted) {
    return checkEdges(typeName, null, fix, verboseLevel, maxWarnings, maxCorrupted);
  }

  /**
   * #5680: same check restricted to {@code scopedRecords} when it is non-null - the {@code CHECK DATABASE RECORD}
   * scope. Per-edge work is identical; only the enumeration changes, from two passes over every bucket of the
   * type to a lookup per listed RID.
   */
  public Map<String, Object> checkEdges(final String typeName, final Collection<RID> scopedRecords,
      final boolean fix, final int verboseLevel, final int maxWarnings, final int maxCorrupted) {
    final AtomicLong autoFix = new AtomicLong();
    final AtomicLong missingReferenceBack = new AtomicLong();
    // CheckReport keeps corruptedRecords as a Set (matching checkVertices) so the same RID flagged on both sides of
    // an edge is recorded once, and totalCorrupted - which counts only genuinely new entries, see
    // CollectionUtils.addBounded - stays aligned with its size.
    final CheckReport report = new CheckReport(maxWarnings, maxCorrupted, verboseLevel);
    final Map<RID, Long> missingReferences = new HashMap<>();
    final Map<RID, String> missingReferenceErrors = new HashMap<>();
    // Vertices whose edge LIST failed to walk during the back-reference probe: warned once each (a broken
    // super-node chain is referenced by millions of edges), never flagged corrupted - see the probe guards.
    final Set<RID> unreadableListVertices = new HashSet<>();
    /** Records this run actually removed, surfaced as {@code deletedRecordsAfterFix}. */
    final Set<RID> deletedRecords = new LinkedHashSet<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();

    try {
      // The endpoint check of ONE edge: shared by the type-wide scan and the RECORD-scoped enumeration.
      final Consumer<Record> checkEndpoints = record -> {
        progressTick();
        final RID edgeRID = record.getIdentity();

        try {
          final Edge edge = record.asEdge(true);

          if (edge == null) {
            report.warn("edge " + edgeRID + " cannot be loaded");
            report.corrupt(edgeRID);

          } else if (edge.getIn() == null || !edge.getIn().isValid()) {
            report.warn("edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
            report.corrupt(edgeRID);
            ++report.invalidLinks;

          } else if (edge.getOut() == null || !edge.getOut().isValid()) {
            report.warn("edge " + edgeRID + " has an invalid outgoing link " + edge.getOut());
            report.corrupt(edgeRID);
            ++report.invalidLinks;

          } else {
            Vertex inVertex = null;
            try {
              inVertex = edge.getInVertex().asVertex(true);
            } catch (final RecordNotFoundException e) {
              report.warn("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " that is not found (deleted?)");
              trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getIn(), describe(e));
              report.corrupt(edgeRID);
              report.corrupt(edge.getIn());
              ++report.invalidLinks;
            } catch (final Exception e) {
              // UNKNOWN ERROR ON LOADING
              report.warn("edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " which cannot be loaded (error: "
                      + describe(e) + ")");
              trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getIn(), describe(e));
              report.corrupt(edgeRID);
              report.corrupt(edge.getIn());
            }

            if (inVertex != null)
              try {
                final EdgeLinkedList inEdges = graphEngine.getEdgeHeadChunk((VertexInternal) inVertex, Vertex.DIRECTION.IN);
                if (inEdges == null || !inEdges.containsEdge(edgeRID))
                  // UNI DIRECTIONAL EDGE
                  missingReferenceBack.incrementAndGet();
              } catch (final Exception e) {
                // The vertex record is FINE but its edge LIST is unreadable: neither the edge nor the vertex is
                // at fault, so NOTHING is flagged corrupted here (before this guard the vertex was deleted by
                // fix mode over its broken chain). checkVertices runs after this phase and rebuilds the list
                // from the surviving edge records.
                if (unreadableListVertices.add(inVertex.getIdentity()))
                  report.warn("vertex " + inVertex.getIdentity() + " incoming edge list is unreadable (error: " + describe(e)
                          + "), left to the vertex check to rebuild");
              }

            Vertex outVertex = null;
            try {
              outVertex = edge.getOutVertex().asVertex(true);
            } catch (final RecordNotFoundException e) {
              report.warn("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " that is not found (deleted?)");
              trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getOut(), describe(e));
              report.corrupt(edgeRID);
              ++report.invalidLinks;
            } catch (final Exception e) {
              // UNKNOWN ERROR ON LOADING
              report.warn("edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " which cannot be loaded (error: "
                      + describe(e) + ")");
              trackMissingReference(missingReferences, missingReferenceErrors, report.maxWarnings, edge.getOut(), describe(e));
              report.corrupt(edgeRID);
              report.corrupt(edge.getOut());
            }

            if (outVertex != null)
              try {
                final EdgeLinkedList outEdges = graphEngine.getEdgeHeadChunk((VertexInternal) outVertex, Vertex.DIRECTION.OUT);
                if (outEdges == null || !outEdges.containsEdge(edgeRID))
                  // UNI DIRECTIONAL EDGE
                  missingReferenceBack.incrementAndGet();
              } catch (final Exception e) {
                // Same as the incoming side: an unreadable LIST is not a corrupted edge or vertex.
                if (unreadableListVertices.add(outVertex.getIdentity()))
                  report.warn("vertex " + outVertex.getIdentity() + " outgoing edge list is unreadable (error: " + describe(e)
                          + "), left to the vertex check to rebuild");
              }
          }

        } catch (final Throwable e) {
          report.warn("edge " + record.getIdentity() + " cannot be loaded (error: " + describe(e) + ")");
          report.corrupt(edgeRID);
        }
      };

      if (scopedRecords != null) {
        progressBegin(progressStepName, scopedRecords.size());

        for (final RID rid : scopedRecords) {
          try {
            checkEndpoints.accept(database.lookupByRID(rid, true));
          } catch (final RecordNotFoundException e) {
            // See the vertex arm: a missing RID is reported, never flagged corrupted, so FIX does not rebuild
            // this bucket's indexes over what is usually just a stale or mistyped RID.
            progressTick();
            report.warn("edge " + rid + " does not exist");
          } catch (final Exception e) {
            // See the vertex arm, including what that guard does and does not cover.
            progressTick();
            report.warn("edge " + rid + " cannot be loaded (error: " + describe(e) + ")");
            report.corrupt(rid);
          }
        }
      } else {
        // ONE FULL PASS OVER THE TYPE (#5773): progress total is the record count. See checkVertices for why the
        // record-type scan that used to precede this detected nothing the endpoint pass misses.
        progressBegin(progressStepName, database.countType(typeName, false));

        database.scanType(typeName, false, record -> {
          checkEndpoints.accept(record);
          return true;
        }, (rid, exception) -> {
          progressTick();
          report.warn("edge " + rid + " cannot be loaded (error: " + describe(exception) + ")");
          report.corrupt(rid);
          return true;
        });
      }

      progressComplete();

      if (fix) {
        for (final RID rid : report.corruptedRecords) {
          if (rid == null)
            continue;

          try {
            deleteCorruptedRecord(rid);
            // Counted AFTER the delete returns, not before it is attempted (issue #6128). autoFix is what an
            // operator reads to decide whether a run did anything, so it must not include a repair that failed -
            // and the failure here is routine rather than exotic: checkEdges flags both ends of a dangling edge,
            // and the far end is flagged precisely because it is not there, so its delete always raises
            // RecordNotFoundException. One dangling edge used to report two repairs.
            autoFix.incrementAndGet();
            // Reported, not only counted: an operator reads deletedRecordsAfterFix to learn WHICH records a repair
            // removed, and until this arm populated it the answer depended on which pass happened to do the delete -
            // a broken-chain record was listed (LocalBucket.check removed it) and every other corrupt record was not.
            // Bounded by the same cap as report.corruptedRecords, which this iterates.
            deletedRecords.add(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            report.warn("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
          commitRepairBatchIfFull();
        }
      }

      if (verboseLevel > 0)
        for (final String warning : report.warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      // Same sum as the vertex arm, and prunedDanglingEntries is structurally ZERO here: pruning happens in
      // checkIncomingEdges/checkOutgoingEdges, which only checkVertices calls. Kept rather than simplified to
      // autoFix.get() so the two arms report autoFix identically, and so an edge arm that one day does prune -
      // #5777 is about exactly this arm's handling of endpoints - cannot silently stop counting it. Do not read
      // it as evidence that this arm prunes today. reconnectedEdges is structurally zero here for the same reason:
      // this arm plans no re-links.
      putRepairCounters(stats, autoFix.get(), report.prunedDanglingEntries, 0L);
      stats.put("deletedRecordsAfterFix", deletedRecords);
      stats.put("corruptedRecords", report.corruptedRecords);
      stats.put("invalidLinks", report.invalidLinks);
      stats.put("missingReferenceBack", missingReferenceBack.get());
      stats.put("warnings", report.warnings);
      stats.put("totalWarnings", report.totalWarnings);
      stats.put("totalCorruptedRecords", report.totalCorrupted);
      stats.put("missingReferences", missingReferences);
      stats.put("missingReferenceErrors", missingReferenceErrors);
    }

    return stats;
  }

  /**
   * Publishes what a repair run did: the {@code autoFix} total both arms have always reported, plus the per-kind
   * breakdown behind it (issue #6136, item 3).
   * <p>
   * {@code autoFix} keeps its meaning for every existing reader - it is the count of repair ACTIONS, records
   * removed plus dangling adjacency entries pruned - and the breakdown says which arms it decomposes into.
   * {@code reconnectedEdges} is deliberately OUTSIDE that sum: a rebuilt chain has never contributed to
   * {@code autoFix}, and folding it in would change every number a current run reports.
   * <p>
   * One helper rather than the same four lines in both arms, so a reader never has to check whether they still
   * agree - the drift the {@code #5777} comment in {@code checkEdges} is guarding against from the other side.
   */
  private static void putRepairCounters(final Map<String, Object> stats, final long removedRecords,
      final long prunedDanglingEntries, final long reconnectedEdges) {
    stats.put("autoFix", removedRecords + prunedDanglingEntries);
    stats.put("removedRecords", removedRecords);
    stats.put("prunedDanglingEntries", prunedDanglingEntries);
    stats.put("reconnectedEdges", reconnectedEdges);
  }

  /**
   * Returns the exception message, or the exception's simple class name when the message is {@code null} (e.g. a
   * {@link NullPointerException}). Without this, CHECK DATABASE prints an undiagnosable "error: null" for any failure
   * whose exception carries no message, which makes triaging a corrupted/diverged replica impossible.
   */
  static String describe(final Throwable e) {
    final String msg = e.getMessage();
    return msg != null ? msg : e.getClass().getSimpleName();
  }

  /**
   * Records that {@code target} (a vertex an edge points to) could not be loaded, accumulating a per-target reference
   * count instead of emitting one line per dangling edge. A single missing supernode can be referenced by millions of
   * edges; this collapses that fan-out into "vertex X could not be loaded, referenced by N edge(s)". The distinct-target
   * set is bounded by {@code maxTracked} to keep memory in check (counts for already-tracked targets keep incrementing).
   */
  private static void trackMissingReference(final Map<RID, Long> missingReferences, final Map<RID, String> missingReferenceErrors,
      final int maxTracked, final RID target, final String error) {
    if (target == null)
      return;
    if (missingReferences.containsKey(target))
      missingReferences.merge(target, 1L, Long::sum);
    else if (missingReferences.size() < maxTracked) {
      missingReferences.put(target, 1L);
      missingReferenceErrors.put(target, error);
    }
  }

  /**
   * The repairs a vertex scan DECIDED on but deliberately did not perform, so that every write leaves the scan and
   * lands in a loop {@link #commitRepairBatchIfFull()} can bound (issue #6136, item 2).
   * <p>
   * Before this, the two checks wrote from inside the {@code scanType} callback - one vertex record per
   * {@link #resetChain} and one adjacency entry per edge "not connected from the other side" - and nothing could
   * commit under them, because {@code LocalDatabase.scanType} holds the database read lock and the chunk iterator
   * being walked would not survive a commit. A database with a very large number of edges in that state therefore
   * still accumulated one oversized transaction and hit the {@code ReplicatedEntryTooLargeException} that #6131
   * removed everywhere else.
   * <p>
   * The two RID sets were already here doing half of this: EVERY {@code resetChain} call site also registered its
   * vertex in one of them, so the set of lists to rebuild IS the set of chains to reset, and deferring the reset
   * costs no extra memory at all. Only the back-reference fix-up needed an accumulator, and it is a
   * {@link PendingLinks} rather than a collection of records for the reason stated there.
   */
  private static final class RepairPlan {
    /** Vertices whose OUT list must be dropped and rebuilt from the surviving edge records. */
    final Set<RID>     reconnectOutEdges = new HashSet<>();
    /** Vertices whose IN list must be dropped and rebuilt from the surviving edge records. */
    final Set<RID>     reconnectInEdges  = new HashSet<>();
    /** OUT-list entries rebuilt from the surviving edge records, for the vertices in {@link #reconnectOutEdges}. */
    final PendingLinks rebuiltOutLinks;
    /** IN-list entries rebuilt from the surviving edge records, for the vertices in {@link #reconnectInEdges}. */
    final PendingLinks rebuiltInLinks;
    /**
     * Back-references the OUT list of an otherwise healthy far vertex was missing. Kept apart from the rebuilt
     * links rather than merged into them, even though the write is identical, because the two are reported
     * separately: "reconnected N outgoing edges" has always meant "a broken chain was rebuilt from N edge records"
     * and folding a different repair into that number would change what an existing report says.
     */
    final PendingLinks backRefOutLinks;
    /** Back-references the IN list of an otherwise healthy far vertex was missing. */
    final PendingLinks backRefInLinks;

    RepairPlan(final DatabaseInternal database) {
      rebuiltOutLinks = new PendingLinks(database);
      rebuiltInLinks = new PendingLinks(database);
      backRefOutLinks = new PendingLinks(database);
      backRefInLinks = new PendingLinks(database);
    }
  }

  /**
   * An append-only list of adjacency entries to (re)create, each the triple (vertex whose list gains the entry,
   * edge, the other endpoint the entry records).
   * <p>
   * PRIMITIVE ARRAYS, not a {@code List} of anything. This is the one structure #6136 adds that grows with the
   * amount of damage, and the accumulation it replaces is the warning: {@code reconnectEdges} held an
   * {@code ArrayList<Edge>} of every edge to re-link, i.e. a fully materialised record - properties and all - per
   * entry, which is unbounded heap in exactly the way the WAL growth this change fixes was unbounded. Three RIDs
   * flattened into an {@code int[]} plus a {@code long[]} is 36 bytes per entry with no per-object header and no
   * card-marking, against a couple of hundred for the record it used to keep alive, and the apply loop rebuilds
   * the RID objects one entry at a time.
   * <p>
   * Still unbounded in the number of ENTRIES, and deliberately so: an entry is dropped only once its repair has
   * been applied, and a cap would mean silently declining to repair part of the damage. The bound that matters -
   * and the one #6128/#6136 are about - is on the transaction, not on the plan.
   * <p>
   * The RIDs it hands back are BOUND TO THE DATABASE ({@code database.newRID}), which is not cosmetic. Flattening
   * to primitives loses the owning database, and a bare {@code RID} resolves one through
   * {@code RID.resolveActiveDatabase()} - a thread-local lookup that THROWS when more than one database is in
   * scope on the thread. That exception would be caught by the apply loop's generic handler and turned into a
   * per-entry warning, so on a multi-database server the repair would silently decline to apply while the run
   * still reported success. Rebuilding them here rather than at each call site means no future caller can
   * reintroduce that.
   */
  private static final class PendingLinks {
    private static final int TRIPLE = 3;

    private final DatabaseInternal database;

    private int[]  buckets   = new int[TRIPLE * 16];
    private long[] positions = new long[TRIPLE * 16];
    private int    size;

    PendingLinks(final DatabaseInternal database) {
      this.database = database;
    }

    void add(final RID vertex, final RID edge, final RID other) {
      final int base = size * TRIPLE;
      if (base + TRIPLE > buckets.length) {
        final int newLength = buckets.length + (buckets.length >> 1) + TRIPLE;
        buckets = Arrays.copyOf(buckets, newLength);
        positions = Arrays.copyOf(positions, newLength);
      }
      set(base, vertex);
      set(base + 1, edge);
      set(base + 2, other);
      ++size;
    }

    private void set(final int slot, final RID rid) {
      buckets[slot] = rid.getBucketId();
      positions[slot] = rid.getPosition();
    }

    private RID get(final int slot) {
      return database.newRID(buckets[slot], positions[slot]);
    }

    int size() {
      return size;
    }

    boolean isEmpty() {
      return size == 0;
    }

    RID vertex(final int i) {
      return get(i * TRIPLE);
    }

    RID edge(final int i) {
      return get(i * TRIPLE + 1);
    }

    RID other(final int i) {
      return get(i * TRIPLE + 2);
    }
  }

  /**
   * What a check run reports: the retained warning messages and the retained corrupted RIDs, each with its own cap
   * and its own running total.
   * <p>
   * #5773 bundled the two collections with their caps and totals because they are ONE policy - both go through
   * {@link CollectionUtils#addBounded}, which is where the retain-and-de-duplicate rule is documented - and because
   * threading them as loose parameters is what made {@link #checkIncomingEdges} and {@link #checkOutgoingEdges}
   * take fourteen and fifteen arguments (now eight each). There is no behaviour here that was not previously in the
   * two static helpers this replaces; the value is that a caller can no longer pair one run's warnings with
   * another's cap.
   * <p>
   * The caps are separate parameters rather than one because the callers pass different REMAINING budgets for the
   * two collections (see {@code DatabaseChecker.checkScopedRecords}), even though both derive from that class's
   * single {@code maxWarnings} setting.
   */
  private static final class CheckReport {
    final LinkedHashSet<String> warnings         = new LinkedHashSet<>();
    final LinkedHashSet<RID>    corruptedRecords = new LinkedHashSet<>();
    // PLAIN longs, not AtomicLong: a check run is single-threaded (scanType walks sequentially) and the sets beside
    // them are not thread-safe either, so an atomic counter here would advertise a concurrency this class does not
    // have and cannot support. They were atomics only because the previous shape passed them as parameters, which
    // a mutable holder is the Java way to do; as fields of one object they no longer need to be.
    long                        totalWarnings;
    long                        totalCorrupted;
    // Uncapped run counters, here for the same reason: they are per-run accumulators every helper needs and
    // nothing else does. checkEdges leaves duplicateLightEdges at zero and does not publish it, as before.
    long                        invalidLinks;
    long                        duplicateLightEdges;
    /**
     * Dangling adjacency-list entries this run PRUNED (issue #6128). Folded into the reported {@code autoFix}
     * alongside the records actually deleted, because both are repairs the run performed and {@code autoFix} is the
     * one number an operator reads to decide whether it did anything. Lives here rather than as another parameter
     * for the same reason the counters above do: every helper that prunes needs it and nothing else does.
     * <p>
     * So {@code autoFix} counts REPAIR ACTIONS, not corruption instances, and the difference is visible: one edge
     * that is both listed in an adjacency chain and corrupt as a record contributes a prune AND a delete, because
     * those are two distinct writes to two distinct pages. An operator reading the number as "how many broken
     * things were there" will over-count; it answers "how many repairs did this run perform". The alternative -
     * counting defects instead - would need the arms to agree on what one defect IS across a dangling entry, a
     * corrupt record and a rebuilt chain, which they cannot without collapsing information the warnings carry.
     */
    long                        prunedDanglingEntries;
    final int                   maxWarnings;
    final int                   maxCorrupted;
    /**
     * Only used to decide whether a message the cap forced us to DROP is still logged. #5773: the two
     * {@code addWarning} twins disagreed here - this one logged unconditionally while
     * {@code DatabaseChecker.addWarning} gated on the same flag - so a caller asking for silence got it from one and
     * not the other. Aligned on honouring it: a caller passing 0 asked for no logging, and the retained set plus
     * {@code totalWarnings} still tell it how many were dropped.
     */
    final int                   verboseLevel;

    CheckReport(final int maxWarnings, final int maxCorrupted, final int verboseLevel) {
      this.maxWarnings = maxWarnings;
      this.maxCorrupted = maxCorrupted;
      this.verboseLevel = verboseLevel;
    }

    /**
     * Records a warning, and LOGS it when the cap meant it could not be retained - so a capped run does not lose
     * the message silently, unless the caller asked for silence with {@code verboseLevel == 0}.
     * <p>
     * #5773: {@code totalWarnings} used to count OCCURRENCES while {@code DatabaseChecker} publishes the retained
     * warnings as a {@code Set}, so two findings rendering to the same message (a vertex with two null entries in
     * its edge list, say) collapsed to one line and counted two - the total exceeded the retained size on a run
     * nowhere near its cap, which is exactly what makes a total useless. Both sides now answer "distinct messages",
     * so an uncapped run has {@code totalWarnings == warnings.size()} and a capped one reports how many it could
     * not keep.
     */
    void warn(final String message) {
      final CollectionUtils.BoundedAdd outcome = CollectionUtils.addBounded(warnings, maxWarnings, message);
      if (!outcome.isFirstSighting())
        return;
      if (outcome == CollectionUtils.BoundedAdd.DROPPED && verboseLevel > 0)
        LogManager.instance().log(GraphDatabaseChecker.class, Level.WARNING, message);
      ++totalWarnings;
    }

    /** Flags a record as corrupted under the same bounded, de-duplicating rule {@link #warn} uses. */
    void corrupt(final RID rid) {
      if (CollectionUtils.addBounded(corruptedRecords, maxCorrupted, rid).isFirstSighting())
        ++totalCorrupted;
    }
  }
}
