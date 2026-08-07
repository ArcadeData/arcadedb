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
            database.getSchema().getBucketById(orphan.getBucketId()).deleteRecord(orphan);
            ++reclaimed;
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
   * The edge-record scan inside {@link #reconnectEdges} is NOT scoped: rebuilding an adjacency means finding
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
    final Set<RID> reconnectOutEdges = new HashSet<>();
    final Set<RID> reconnectInEdges = new HashSet<>();
    final Map<RID, Long> missingReferences = new HashMap<>();
    final Map<RID, String> missingReferenceErrors = new HashMap<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();
    try {
      // The connectivity check of ONE vertex: shared by the type-wide scan and the RECORD-scoped enumeration.
      final Consumer<Record> checkConnectivity = record -> {
        progressTick();
        try {
          Vertex vertex = record.asVertex(true);

          final RID vertexIdentity = vertex.getIdentity();

          vertex = checkOutgoingEdges(fix, vertex, vertexIdentity, reconnectOutEdges, reconnectInEdges,
              missingReferences, missingReferenceErrors, report);

          checkIncomingEdges(fix, vertex, vertexIdentity, reconnectInEdges, reconnectOutEdges, missingReferences,
              missingReferenceErrors, report);

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
        if (!reconnectOutEdges.isEmpty() || !reconnectInEdges.isEmpty())
          reconnectEdges(reconnectOutEdges, reconnectInEdges, report, stats);

        for (final RID rid : report.corruptedRecords) {
          if (rid == null)
            continue;

          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            report.warn("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (final String warning : report.warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
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
   * Rebuilds the edge lists of the registered vertices from the surviving edge records. NOTE: this is a FULL
   * scan of every edge type, even when a single vertex needs reconnection - acceptable because it runs only in
   * fix mode on an already-damaged database, and the pre-existing reconnect path had the same cost. A rebuilt
   * entry may still point to a far endpoint vertex that no longer exists (the edge record survives, its target
   * does not): that is the same behaviour as before and the {@code checkEdges} pass reports it.
   */
  private void reconnectEdges(Set<RID> reconnectOutEdges, Set<RID> reconnectInEdges, CheckReport report,
      Map<String, Object> stats) {
    // BROWSE ALL THE EDGES AND COLLECT THE ONES PART OF THE RECONNECTION
    final List<EdgeType> edgeTypes = new ArrayList<>();
    for (DocumentType schemaType : database.getSchema().getTypes()) {
      if (schemaType instanceof EdgeType t)
        edgeTypes.add(t);
    }

    final List<Edge> outEdgesToReconnect = new ArrayList<>();
    final List<Edge> inEdgesToReconnect = new ArrayList<>();

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
          if (reconnectOutEdges.contains(e.getOut()))
            outEdgesToReconnect.add(e);
          // A unidirectional edge is never stored in the target's IN list: rebuilding it there would invent
          // adjacency that never existed.
          if (bidirectional && reconnectInEdges.contains(e.getIn()))
            inEdgesToReconnect.add(e);
        } catch (final Exception e) {
          warnUnreadableEdgeDuringRebuild(report, record.getIdentity(), e);
        }
        return true;
      }, (rid, exception) -> {
        warnUnreadableEdgeDuringRebuild(report, rid, exception);
        return true;
      });
    }

    if (!outEdgesToReconnect.isEmpty()) {
      for (Edge e : outEdgesToReconnect) {
        final MutableVertex vertex = e.getOutVertex().modify();
        // getOrCreateEdgeList dispatches on the head record type, so promoted (striped) vertices work too
        graphEngine.getOrCreateEdgeList(vertex, Vertex.DIRECTION.OUT).add(e.getIdentity(), e.getIn());
      }
      report.warn("reconnected " + outEdgesToReconnect.size() + " outgoing edges");
      stats.put("outEdgesToReconnect", outEdgesToReconnect);
    }

    if (!inEdgesToReconnect.isEmpty()) {
      for (Edge e : inEdgesToReconnect) {
        final MutableVertex vertex = e.getInVertex().modify();
        graphEngine.getOrCreateEdgeList(vertex, Vertex.DIRECTION.IN).add(e.getIdentity(), e.getOut());
      }
      report.warn("reconnected " + inEdgesToReconnect.size() + " incoming edges");
      stats.put("inEdgesToReconnect", inEdgesToReconnect);
    }
  }

  private static void warnUnreadableEdgeDuringRebuild(final CheckReport report, final Object rid,
      final Throwable error) {
    report.warn("edge " + rid + " could not be read during the edge-list rebuild, skipping it (error: " + describe(error) + ")");
  }

  private void checkIncomingEdges(boolean fix, Vertex vertex, RID vertexIdentity, Set<RID> reconnectInEdges,
      Set<RID> reconnectOutEdges, Map<RID, Long> missingReferences, Map<RID, String> missingReferenceErrors,
      CheckReport report) {
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
        if (fix) {
          vertex = resetChain(vertex, Vertex.DIRECTION.IN);
          reconnectInEdges.add(vertexIdentity);
        }
      } else {
        Iterator<Pair<RID, RID>> in = null;
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
                final Edge edge = edgeRID.asEdge(true);

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
                      if (fix) {
                        reconnectInEdges.add(vertexIdentity);
                        vertex = resetChain(vertex, Vertex.DIRECTION.IN);
                      }

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
                    if (reconnectOutEdges.add(inVertex.getIdentity())) {
                      report.warn("vertex " + inVertex.getIdentity() + " outgoing edge list is unreadable (error: "
                              + describe(probeError) + ")" + (fix ? ", rebuilding it from the surviving edge records" : ""));
                      if (fix)
                        resetChain(inVertex, Vertex.DIRECTION.OUT);
                    }
                  }
                  if (connected != null && !connected && !reconnectOutEdges.contains(inVertex.getIdentity())) {
                    report.warn("edge " + edgeRID + " was not connected from the incoming vertex " + edge.getOut() + " to the vertex "
                            + vertexIdentity);
                    if (fix) {
                      inVertex = inVertex.modify();
                      database.getGraphEngine().connectOutgoingEdge(inVertex, vertexIdentity, edge);
                      ((MutableVertex) inVertex).save();
                    }
                  }
                }

              } catch (final RecordNotFoundException e) {
                report.warn("edge " + edgeRID + " not found");
                report.corrupt(edgeRID);
                removeEntry = true;
                ++report.invalidLinks;
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING
                report.warn("edge " + edgeRID + " error on loading (error: " + describe(e) + ")");
                report.corrupt(edgeRID);
                removeEntry = true;
              }
            }

            if (fix && removeEntry)
              in.remove();
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
          if (fix) {
            vertex = resetChain(vertex, Vertex.DIRECTION.IN);
            reconnectInEdges.add(vertexIdentity);
          }
        }
      }
    }
  }

  private Vertex checkOutgoingEdges(final boolean fix, Vertex vertex, final RID vertexIdentity,
      final Set<RID> reconnectOutEdges, final Set<RID> reconnectInEdges, final Map<RID, Long> missingReferences,
      final Map<RID, String> missingReferenceErrors, final CheckReport report) {
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
        if (fix) {
          vertex = resetChain(vertex, Vertex.DIRECTION.OUT);
          reconnectOutEdges.add(vertexIdentity);
        }
      } else {
        Iterator<Pair<RID, RID>> out = null;
        // Lazily created: only a vertex that actually has lightweight edges pays for it.
        EdgeIdentitySet seenLightEdges = null;
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

                final Edge edge = edgeRID.asEdge(true);

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
                      if (fix) {
                        reconnectOutEdges.add(vertexIdentity);
                        vertex = resetChain(vertex, Vertex.DIRECTION.OUT);
                      }

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
                    if (reconnectInEdges.add(outVertex.getIdentity())) {
                      report.warn("vertex " + outVertex.getIdentity() + " incoming edge list is unreadable (error: "
                              + describe(probeError) + ")" + (fix ? ", rebuilding it from the surviving edge records" : ""));
                      if (fix)
                        resetChain(outVertex, Vertex.DIRECTION.IN);
                    }
                  }
                  if (connected != null && !connected && !reconnectInEdges.contains(outVertex.getIdentity())) {
                    report.warn("edge " + edgeRID + " was not connected from the outgoing vertex " + edge.getIn() + " back to the vertex "
                            + vertexIdentity);
                    if (fix) {
                      outVertex = outVertex.modify();
                      database.getGraphEngine().connectIncomingEdge(outVertex, vertexIdentity, edgeRID);
                      ((MutableVertex) outVertex).save();
                    }
                  }
                }

              } catch (final RecordNotFoundException e) {
                report.warn("edge " + edgeRID + " not found");
                report.corrupt(edgeRID);
                removeEntry = true;
                ++report.invalidLinks;
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING
                report.warn("edge " + edgeRID + " error on loading (error: " + describe(e) + ")");
                report.corrupt(edgeRID);
                removeEntry = true;
              }
            }

            if (fix && removeEntry)
              out.remove();

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
          if (fix) {
            vertex = resetChain(vertex, Vertex.DIRECTION.OUT);
            reconnectOutEdges.add(vertexIdentity);
          }
        }
      }
    }
    return vertex;
  }

  /**
   * Nulls the vertex's head-chunk pointer for the given direction, dropping the unreadable chain so the
   * adjacency can be rebuilt from the surviving edge records by {@link #reconnectEdges}. Each edge record
   * stores its own out/in vertex RIDs, so losing the linked list does not lose the graph. Returns the saved
   * mutable copy so callers keep operating on the fresh vertex.
   */
  private Vertex resetChain(final Vertex vertex, final Vertex.DIRECTION direction) {
    final MutableVertex mutable = vertex.modify();
    if (direction == Vertex.DIRECTION.OUT)
      mutable.setOutEdgesHeadChunk(null);
    else
      mutable.setInEdgesHeadChunk(null);
    mutable.save();
    return mutable;
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

          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            report.warn("Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (final String warning : report.warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
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
