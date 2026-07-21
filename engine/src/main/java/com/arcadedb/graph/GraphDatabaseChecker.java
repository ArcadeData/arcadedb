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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.utility.LongHashSet;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.ProgressCallback;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
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
   */
  public Map<String, Object> reclaimOrphanedEdgeSegments(final int verboseLevel, final int maxWarnings) {
    final List<String> warnings = new ArrayList<>();
    final AtomicLong totalWarnings = new AtomicLong();
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
      progressBegin("Reclaiming orphaned edge segments - collecting reachable segments", totalVertices);
      for (final DocumentType type : vertexTypes)
        database.scanType(type.getName(), false, record -> {
          progressTick();
          try {
            final VertexInternal vertex = (VertexInternal) record.asVertex(true);
            markReachableSegments(vertex.getOutEdgesHeadChunk(), reachable);
            markReachableSegments(vertex.getInEdgesHeadChunk(), reachable);
          } catch (final Exception e) {
            addWarning(warnings, totalWarnings, maxWarnings,
                "vertex " + record.getIdentity() + " could not be walked during the orphan reclaim (error: " + describe(e)
                    + ")");
          }
          return true;
        }, (rid, exception) -> true);

      // PHASE 2: scan the dedicated edge-list buckets; anything not marked reachable is an orphan.
      progressBegin("Reclaiming orphaned edge segments - scanning segment buckets", -1);
      final List<RID> orphansToDelete = new ArrayList<>();
      for (final Bucket b : database.getSchema().getBuckets()) {
        final String name = b.getName();
        if (!name.endsWith(GraphEngine.OUT_EDGES_SUFFIX) && !name.endsWith(GraphEngine.IN_EDGES_SUFFIX)
            && !name.contains(StripedEdgeList.STRIPE_BUCKET_INFIX))
          continue;

        final LongHashSet reachableInBucket = reachable.get(b.getFileId());
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
          addWarning(warnings, totalWarnings, maxWarnings,
              "orphaned edge segment " + orphan + " could not be reclaimed (error: " + describe(e) + ")");
        }
      }

      if (verboseLevel > 0)
        for (final String warning : warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();
      progressComplete();

    } finally {
      stats.put("orphanedEdgeSegments", orphans);
      stats.put("orphanedEdgeSegmentsReclaimed", reclaimed);
      stats.put("warnings", warnings);
      stats.put("totalWarnings", totalWarnings.get());
    }
    return stats;
  }

  /** Marks every segment reachable from the given head: a classic chain, or a stripe directory + all its chains. */
  private void markReachableSegments(final RID head, final Map<Integer, LongHashSet> reachable) {
    if (head == null)
      return;
    final Record headRecord;
    try {
      headRecord = database.lookupByRID(head, true);
    } catch (final Exception e) {
      // UNREADABLE HEAD: the rebuild in checkVertices handles the chain itself; nothing reachable to mark.
      return;
    }
    if (headRecord instanceof StripeDirectory directory) {
      mark(reachable, head);
      for (int g = 0; g < directory.getGenerationCount(); g++)
        for (int s = 0; s < directory.getStripes(g); s++)
          markChain(directory.getHead(g, s), reachable);
    } else
      markChain(head, reachable);
  }

  /** Walks a classic chunk chain marking every chunk; stops on cycles and unreadable chunks. */
  private void markChain(final RID head, final Map<Integer, LongHashSet> reachable) {
    RID current = head;
    while (current != null) {
      if (!mark(reachable, current))
        return; // ALREADY VISITED: guards against a corrupt cyclic chain looping forever
      try {
        current = ((EdgeSegment) database.lookupByRID(current, true)).getPreviousRID();
      } catch (final Exception e) {
        return; // BROKEN TAIL: the rebuild in checkVertices handles the chain itself
      }
    }
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
    final AtomicLong autoFix = new AtomicLong();
    final AtomicLong invalidLinks = new AtomicLong();
    final AtomicLong totalWarnings = new AtomicLong();
    final AtomicLong totalCorrupted = new AtomicLong();
    final LinkedHashSet<RID> corruptedRecords = new LinkedHashSet<>();
    final List<String> warnings = new ArrayList<>();
    final Set<RID> reconnectOutEdges = new HashSet<>();
    final Set<RID> reconnectInEdges = new HashSet<>();
    final Map<RID, Long> missingReferences = new HashMap<>();
    final Map<RID, String> missingReferenceErrors = new HashMap<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();
    try {
      // TWO FULL PASSES OVER THE TYPE (record-type scan + connectivity walk): progress total is 2x the count.
      // countType reads the maintained bucket counters (no scan), so sizing the total is negligible.
      progressBegin(progressStepName, 2 * database.countType(typeName, false));

      // CHECK RECORD IS OF THE RIGHT TYPE
      final DocumentType type = database.getSchema().getType(typeName);
      for (final Bucket b : type.getBuckets(false)) {
        b.scan((rid, view) -> {
          try {
            final Record record = database.getRecordFactory().newImmutableRecord(database, type, rid, view, null);
            record.asVertex(true);
          } catch (Exception e) {
            addWarning(warnings, totalWarnings, maxWarnings, "vertex " + rid + " cannot be loaded, removing it");
            addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, rid);
          }
          progressTick();
          return true;
        }, null);
      }

      database.scanType(typeName, false, record -> {
        progressTick();
        try {
          Vertex vertex = record.asVertex(true);

          final RID vertexIdentity = vertex.getIdentity();

          vertex = checkOutgoingEdges(fix, vertex, warnings, totalWarnings, maxWarnings, vertexIdentity, invalidLinks,
              corruptedRecords, totalCorrupted, maxCorrupted, reconnectOutEdges, reconnectInEdges, missingReferences,
              missingReferenceErrors);

          checkIncomingEdges(fix, vertex, warnings, totalWarnings, maxWarnings, vertexIdentity, invalidLinks,
              corruptedRecords, totalCorrupted, maxCorrupted, reconnectInEdges, reconnectOutEdges, missingReferences,
              missingReferenceErrors);

        } catch (final Throwable e) {
          addWarning(warnings, totalWarnings, maxWarnings,
              "vertex " + record.getIdentity() + " cannot be loaded (error: " + describe(e) + ")");
          addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, record.getIdentity());
        }

        return true;
      }, (rid, exception) -> {
        progressTick();
        addWarning(warnings, totalWarnings, maxWarnings,
            "vertex " + rid + " cannot be loaded (error: " + describe(exception) + ")");
        addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, rid);
        return true;
      });

      progressComplete();

      if (fix) {
        if (!reconnectOutEdges.isEmpty() || !reconnectInEdges.isEmpty())
          reconnectEdges(reconnectOutEdges, reconnectInEdges, corruptedRecords, warnings, totalWarnings, maxWarnings, stats);

        for (final RID rid : corruptedRecords) {
          if (rid == null)
            continue;

          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            addWarning(warnings, totalWarnings, maxWarnings,
                "Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (final String warning : warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
      stats.put("corruptedRecords", corruptedRecords);
      stats.put("invalidLinks", invalidLinks.get());
      stats.put("warnings", warnings);
      stats.put("totalWarnings", totalWarnings.get());
      stats.put("totalCorruptedRecords", totalCorrupted.get());
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
  private void reconnectEdges(Set<RID> reconnectOutEdges, Set<RID> reconnectInEdges, Set<RID> corruptedRecords,
      List<String> warnings, AtomicLong totalWarnings, int maxWarnings, Map<String, Object> stats) {
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
          if (corruptedRecords.contains(e.getIdentity()))
            // ABOUT TO BE DELETED BY THE FIX: re-adding it would rebuild a dangling entry
            return true;
          if (reconnectOutEdges.contains(e.getOut()))
            outEdgesToReconnect.add(e);
          // A unidirectional edge is never stored in the target's IN list: rebuilding it there would invent
          // adjacency that never existed.
          if (bidirectional && reconnectInEdges.contains(e.getIn()))
            inEdgesToReconnect.add(e);
        } catch (final Exception e) {
          warnUnreadableEdgeDuringRebuild(warnings, totalWarnings, maxWarnings, record.getIdentity(), e);
        }
        return true;
      }, (rid, exception) -> {
        warnUnreadableEdgeDuringRebuild(warnings, totalWarnings, maxWarnings, rid, exception);
        return true;
      });
    }

    if (!outEdgesToReconnect.isEmpty()) {
      for (Edge e : outEdgesToReconnect) {
        final MutableVertex vertex = e.getOutVertex().modify();
        // getOrCreateEdgeList dispatches on the head record type, so promoted (striped) vertices work too
        graphEngine.getOrCreateEdgeList(vertex, Vertex.DIRECTION.OUT).add(e.getIdentity(), e.getIn());
      }
      addWarning(warnings, totalWarnings, maxWarnings, "reconnected " + outEdgesToReconnect.size() + " outgoing edges");
      stats.put("outEdgesToReconnect", outEdgesToReconnect);
    }

    if (!inEdgesToReconnect.isEmpty()) {
      for (Edge e : inEdgesToReconnect) {
        final MutableVertex vertex = e.getInVertex().modify();
        graphEngine.getOrCreateEdgeList(vertex, Vertex.DIRECTION.IN).add(e.getIdentity(), e.getOut());
      }
      addWarning(warnings, totalWarnings, maxWarnings, "reconnected " + inEdgesToReconnect.size() + " incoming edges");
      stats.put("inEdgesToReconnect", inEdgesToReconnect);
    }
  }

  private static void warnUnreadableEdgeDuringRebuild(final List<String> warnings, final AtomicLong totalWarnings,
      final int maxWarnings, final Object rid, final Throwable error) {
    addWarning(warnings, totalWarnings, maxWarnings,
        "edge " + rid + " could not be read during the edge-list rebuild, skipping it (error: " + describe(error) + ")");
  }

  private void checkIncomingEdges(boolean fix, Vertex vertex, List<String> warnings, AtomicLong totalWarnings, int maxWarnings,
      RID vertexIdentity, AtomicLong invalidLinks, LinkedHashSet<RID> corruptedRecords, AtomicLong totalCorrupted,
      int maxCorrupted, Set<RID> reconnectInEdges, Set<RID> reconnectOutEdges, Map<RID, Long> missingReferences,
      Map<RID, String> missingReferenceErrors) {
    if (((VertexInternal) vertex).getInEdgesHeadChunk() != null) {
      EdgeLinkedList inEdges = null;
      try {
        inEdges = graphEngine.getEdgeHeadChunk((VertexInternal) vertex, Vertex.DIRECTION.IN);
      } catch (Exception e) {
        // IGNORE IT: HANDLED AS AN UNREADABLE LIST BELOW
      }

      if (inEdges == null) {
        final RID headChunkRID = ((VertexInternal) vertex).getInEdgesHeadChunk();
        addWarning(warnings, totalWarnings, maxWarnings, "vertex " + vertexIdentity + " in edges record " + headChunkRID
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
              addWarning(warnings, totalWarnings, maxWarnings, "outgoing edge null from vertex " + vertexIdentity);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else if (vertexRID == null) {
              addWarning(warnings, totalWarnings, maxWarnings, "outgoing vertex null from vertex " + vertexIdentity);
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else {
              if (edgeRID.getPosition() < 0)
                // LIGHTWEIGHT EDGE
                continue;

              try {
                final Edge edge = edgeRID.asEdge(true);

                VertexInternal inVertex = null;

                if (edge.getOut() == null || !edge.getOut().isValid()) {
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "edge " + edgeRID + " has an invalid outgoing link " + edge.getIn());
                  addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                } else {
                  try {
                    inVertex = (VertexInternal) edge.getOutVertex().asVertex(true);
                  } catch (final RecordNotFoundException e) {
                    addWarning(warnings, totalWarnings, maxWarnings,
                        "edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " that is not found (deleted?)");
                    trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getOut(), describe(e));
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                    removeEntry = true;
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edge.getOut());
                    invalidLinks.incrementAndGet();
                  } catch (final Exception e) {
                    // UNKNOWN ERROR ON LOADING
                    addWarning(warnings, totalWarnings, maxWarnings,
                        "edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " which cannot be loaded (error: "
                            + describe(e) + ")");
                    trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getOut(), describe(e));
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                    removeEntry = true;
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edge.getOut());
                  }
                }

                if (!edge.getIn().equals(vertexIdentity)) {
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected " + vertexIdentity);

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
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "edge " + edgeRID + " has an incoming link " + edge.getOut() + " different from expected " + vertexIdentity
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
                      addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                      removeEntry = true;
                      invalidLinks.incrementAndGet();
                    }
                  } else {
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  }

                } else if (!edge.getOut().equals(vertexRID)) {
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected " + vertexRID);
                  addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
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
                      addWarning(warnings, totalWarnings, maxWarnings,
                          "vertex " + inVertex.getIdentity() + " outgoing edge list is unreadable (error: "
                              + describe(probeError) + ")" + (fix ? ", rebuilding it from the surviving edge records" : ""));
                      if (fix)
                        resetChain(inVertex, Vertex.DIRECTION.OUT);
                    }
                  }
                  if (connected != null && !connected && !reconnectOutEdges.contains(inVertex.getIdentity())) {
                    addWarning(warnings, totalWarnings, maxWarnings,
                        "edge " + edgeRID + " was not connected from the incoming vertex " + edge.getOut() + " to the vertex "
                            + vertexIdentity);
                    if (fix) {
                      inVertex = inVertex.modify();
                      database.getGraphEngine().connectOutgoingEdge(inVertex, vertexIdentity, edge);
                      ((MutableVertex) inVertex).save();
                    }
                  }
                }

              } catch (final RecordNotFoundException e) {
                addWarning(warnings, totalWarnings, maxWarnings, "edge " + edgeRID + " not found");
                addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING
                addWarning(warnings, totalWarnings, maxWarnings,
                    "edge " + edgeRID + " error on loading (error: " + describe(e) + ")");
                addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
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
          addWarning(warnings, totalWarnings, maxWarnings,
              "error on loading incoming edges from vertex " + vertexIdentity + " (error: " + chainError + ")"
                  + (fix ? ", rebuilding the edge list from the surviving edge records" : ""));
          if (fix) {
            vertex = resetChain(vertex, Vertex.DIRECTION.IN);
            reconnectInEdges.add(vertexIdentity);
          }
        }
      }
    }
  }

  private Vertex checkOutgoingEdges(final boolean fix, Vertex vertex, final List<String> warnings,
      final AtomicLong totalWarnings, final int maxWarnings, final RID vertexIdentity, final AtomicLong invalidLinks,
      final LinkedHashSet<RID> corruptedRecords, final AtomicLong totalCorrupted, final int maxCorrupted,
      final Set<RID> reconnectOutEdges, final Set<RID> reconnectInEdges, final Map<RID, Long> missingReferences,
      final Map<RID, String> missingReferenceErrors) {
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
        addWarning(warnings, totalWarnings, maxWarnings, "vertex " + vertexIdentity + " out edges record " + headChunkRID
            + " is not valid" + (fix ? ", rebuilding the edge list from the surviving edge records" : ""));
        if (fix) {
          vertex = resetChain(vertex, Vertex.DIRECTION.OUT);
          reconnectOutEdges.add(vertexIdentity);
        }
      } else {
        Iterator<Pair<RID, RID>> out = null;
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
              addWarning(warnings, totalWarnings, maxWarnings, "outgoing edge null from vertex " + vertexIdentity);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else if (vertexRID == null) {
              addWarning(warnings, totalWarnings, maxWarnings, "outgoing vertex null from vertex " + vertexIdentity);
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
              removeEntry = true;
              invalidLinks.incrementAndGet();
            } else {
              try {
                if (edgeRID.getPosition() < 0)
                  // LIGHTWEIGHT EDGE
                  continue;

                final Edge edge = edgeRID.asEdge(true);

                if (edge.getIn() == null || !edge.getIn().isValid()) {
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
                  addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
                } else {
                  try {
                    outVertex = (VertexInternal) edge.getInVertex().asVertex(true);
                  } catch (final RecordNotFoundException e) {
                    addWarning(warnings, totalWarnings, maxWarnings,
                        "edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " that is not found (deleted?)");
                    trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getIn(), describe(e));
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                    removeEntry = true;
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edge.getIn());
                    invalidLinks.incrementAndGet();
                  } catch (final Exception e) {
                    // UNKNOWN ERROR ON LOADING
                    addWarning(warnings, totalWarnings, maxWarnings,
                        "edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " which cannot be loaded (error: "
                            + describe(e) + ")");
                    trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getIn(), describe(e));
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                    removeEntry = true;
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edge.getIn());
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
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "edge " + edgeRID + " has an outgoing link " + edge.getOut() + " different from expected " + vertexIdentity
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
                      addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                      removeEntry = true;
                      invalidLinks.incrementAndGet();
                    }
                  } else {
                    addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                    removeEntry = true;
                    invalidLinks.incrementAndGet();
                  }

                } else if (!edge.getIn().equals(vertexRID)) {
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "edge " + edgeRID + " has an incoming link " + edge.getIn() + " different from expected " + vertexRID);
                  addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                  removeEntry = true;
                  invalidLinks.incrementAndGet();
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
                      addWarning(warnings, totalWarnings, maxWarnings,
                          "vertex " + outVertex.getIdentity() + " incoming edge list is unreadable (error: "
                              + describe(probeError) + ")" + (fix ? ", rebuilding it from the surviving edge records" : ""));
                      if (fix)
                        resetChain(outVertex, Vertex.DIRECTION.IN);
                    }
                  }
                  if (connected != null && !connected && !reconnectInEdges.contains(outVertex.getIdentity())) {
                    addWarning(warnings, totalWarnings, maxWarnings,
                        "edge " + edgeRID + " was not connected from the outgoing vertex " + edge.getIn() + " back to the vertex "
                            + vertexIdentity);
                    if (fix) {
                      outVertex = outVertex.modify();
                      database.getGraphEngine().connectIncomingEdge(outVertex, vertexIdentity, edgeRID);
                      ((MutableVertex) outVertex).save();
                    }
                  }
                }

              } catch (final RecordNotFoundException e) {
                addWarning(warnings, totalWarnings, maxWarnings, "edge " + edgeRID + " not found");
                addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
                removeEntry = true;
                invalidLinks.incrementAndGet();
              } catch (final Exception e) {
                // UNKNOWN ERROR ON LOADING
                addWarning(warnings, totalWarnings, maxWarnings,
                    "edge " + edgeRID + " error on loading (error: " + describe(e) + ")");
                addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
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
          addWarning(warnings, totalWarnings, maxWarnings,
              "error on loading outgoing edges from vertex " + vertexIdentity + " (error: " + chainError + ")"
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
    final AtomicLong autoFix = new AtomicLong();
    final AtomicLong invalidLinks = new AtomicLong();
    final AtomicLong missingReferenceBack = new AtomicLong();
    final AtomicLong totalWarnings = new AtomicLong();
    final AtomicLong totalCorrupted = new AtomicLong();
    // Use a Set (matching checkVertices) so the same RID flagged on both sides of an edge is recorded once and
    // totalCorrupted (which counts only genuinely new entries, see addCorrupted) stays aligned with its size.
    final LinkedHashSet<RID> corruptedRecords = new LinkedHashSet<>();
    final List<String> warnings = new ArrayList<>();
    final Map<RID, Long> missingReferences = new HashMap<>();
    final Map<RID, String> missingReferenceErrors = new HashMap<>();
    // Vertices whose edge LIST failed to walk during the back-reference probe: warned once each (a broken
    // super-node chain is referenced by millions of edges), never flagged corrupted - see the probe guards.
    final Set<RID> unreadableListVertices = new HashSet<>();

    final Map<String, Object> stats = new HashMap<>();

    database.begin();

    try {
      // TWO FULL PASSES OVER THE TYPE (record-type scan + endpoint checks): progress total is 2x the count.
      progressBegin(progressStepName, 2 * database.countType(typeName, false));

      // CHECK RECORD IS OF THE RIGHT TYPE
      final DocumentType type = database.getSchema().getType(typeName);
      for (final Bucket b : type.getBuckets(false)) {
        b.scan((rid, view) -> {
          try {
            final Record record = database.getRecordFactory().newImmutableRecord(database, type, rid, view, null);
            record.asEdge(true);
          } catch (Exception e) {
            addWarning(warnings, totalWarnings, maxWarnings, "edge " + rid + " cannot be loaded, removing it");
            addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, rid);
          }
          progressTick();
          return true;
        }, null);
      }

      database.scanType(typeName, false, record -> {
        progressTick();
        final RID edgeRID = record.getIdentity();

        try {
          final Edge edge = record.asEdge(true);

          if (edge == null) {
            addWarning(warnings, totalWarnings, maxWarnings, "edge " + edgeRID + " cannot be loaded");
            addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);

          } else if (edge.getIn() == null || !edge.getIn().isValid()) {
            addWarning(warnings, totalWarnings, maxWarnings, "edge " + edgeRID + " has an invalid incoming link " + edge.getIn());
            addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
            invalidLinks.incrementAndGet();

          } else if (edge.getOut() == null || !edge.getOut().isValid()) {
            addWarning(warnings, totalWarnings, maxWarnings, "edge " + edgeRID + " has an invalid outgoing link " + edge.getOut());
            addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
            invalidLinks.incrementAndGet();

          } else {
            Vertex inVertex = null;
            try {
              inVertex = edge.getInVertex().asVertex(true);
            } catch (final RecordNotFoundException e) {
              addWarning(warnings, totalWarnings, maxWarnings,
                  "edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " that is not found (deleted?)");
              trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getIn(), describe(e));
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edge.getIn());
              invalidLinks.incrementAndGet();
            } catch (final Exception e) {
              // UNKNOWN ERROR ON LOADING
              addWarning(warnings, totalWarnings, maxWarnings,
                  "edge " + edgeRID + " points to the incoming vertex " + edge.getIn() + " which cannot be loaded (error: "
                      + describe(e) + ")");
              trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getIn(), describe(e));
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edge.getIn());
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
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "vertex " + inVertex.getIdentity() + " incoming edge list is unreadable (error: " + describe(e)
                          + "), left to the vertex check to rebuild");
              }

            Vertex outVertex = null;
            try {
              outVertex = edge.getOutVertex().asVertex(true);
            } catch (final RecordNotFoundException e) {
              addWarning(warnings, totalWarnings, maxWarnings,
                  "edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " that is not found (deleted?)");
              trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getOut(), describe(e));
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
              invalidLinks.incrementAndGet();
            } catch (final Exception e) {
              // UNKNOWN ERROR ON LOADING
              addWarning(warnings, totalWarnings, maxWarnings,
                  "edge " + edgeRID + " points to the outgoing vertex " + edge.getOut() + " which cannot be loaded (error: "
                      + describe(e) + ")");
              trackMissingReference(missingReferences, missingReferenceErrors, maxWarnings, edge.getOut(), describe(e));
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
              addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edge.getOut());
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
                  addWarning(warnings, totalWarnings, maxWarnings,
                      "vertex " + outVertex.getIdentity() + " outgoing edge list is unreadable (error: " + describe(e)
                          + "), left to the vertex check to rebuild");
              }
          }

        } catch (final Throwable e) {
          addWarning(warnings, totalWarnings, maxWarnings,
              "edge " + record.getIdentity() + " cannot be loaded (error: " + describe(e) + ")");
          addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, edgeRID);
        }

        return true;
      }, (rid, exception) -> {
        progressTick();
        addWarning(warnings, totalWarnings, maxWarnings,
            "edge " + rid + " cannot be loaded (error: " + describe(exception) + ")");
        addCorrupted(corruptedRecords, totalCorrupted, maxCorrupted, rid);
        return true;
      });

      progressComplete();

      if (fix) {
        for (final RID rid : corruptedRecords) {
          if (rid == null)
            continue;

          autoFix.incrementAndGet();
          try {
            database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
          } catch (final RecordNotFoundException e) {
            // IGNORE IT
          } catch (final Throwable e) {
            addWarning(warnings, totalWarnings, maxWarnings,
                "Cannot fix the record " + rid + ": error on delete (error: " + e.getMessage() + ")");
          }
        }
      }

      if (verboseLevel > 0)
        for (final String warning : warnings)
          LogManager.instance().log(this, Level.WARNING, "- " + warning);

      database.commit();

    } finally {
      stats.put("autoFix", autoFix.get());
      stats.put("corruptedRecords", corruptedRecords);
      stats.put("invalidLinks", invalidLinks.get());
      stats.put("missingReferenceBack", missingReferenceBack.get());
      stats.put("warnings", warnings);
      stats.put("totalWarnings", totalWarnings.get());
      stats.put("totalCorruptedRecords", totalCorrupted.get());
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

  private static void addWarning(final List<String> warnings, final AtomicLong totalWarnings, final int maxWarnings,
      final String message) {
    totalWarnings.incrementAndGet();
    if (warnings.size() < maxWarnings)
      warnings.add(message);
    else
      LogManager.instance().log(GraphDatabaseChecker.class, Level.WARNING, message);
  }

  private static <T> void addCorrupted(final Collection<T> corrupted, final AtomicLong totalCorrupted, final int maxCorrupted,
      final T item) {
    if (corrupted.size() < maxCorrupted) {
      // Under the cap: count the record only the first time it is recorded so totalCorrupted stays aligned with the
      // de-duplicated collection size. Collection.add() returns false for an item already present in a Set.
      if (corrupted.add(item))
        totalCorrupted.incrementAndGet();
    } else
      // At/over the cap the item is not stored, so de-duplication is no longer possible: count the occurrence.
      totalCorrupted.incrementAndGet();
  }
}
