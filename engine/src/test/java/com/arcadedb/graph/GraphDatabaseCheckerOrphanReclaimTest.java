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
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.exception.RecordNotFoundException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CHECK DATABASE FIX reclaims orphaned edge-list segments (issue #5375): when a broken chain is rebuilt from
 * the surviving edge records, the old chunks (and, for promoted super-nodes, the old stripe directory and
 * stripe chains) become unreachable garbage on disk. The fix pass now collects the set of reachable segments
 * by walking every vertex's chains and deletes the rest.
 * <p>
 * The most important test here is the HEALTHY-database one: the reachability walk must cover every layout
 * (classic chains, stripe directories, stripe chains, generation-0 chains), because a missed case would make
 * the reclaim delete LIVE segments.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphDatabaseCheckerOrphanReclaimTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";

  private int savedThreshold;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
  }

  /** Creates the hub plus {@code degree} sources, each with one edge source -> hub (hub's IN list). */
  private RID createHub(final int degree) {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
    });

    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());

    final int batch = 500;
    for (int from = 0; from < degree; from += batch) {
      final int start = from;
      final int end = Math.min(from + batch, degree);
      database.transaction(() -> {
        for (int i = start; i < end; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, hub[0]);
      });
    }
    return hub[0];
  }

  /** All chunk RIDs of the hub's IN chain (classic layout), head first. */
  private List<RID> collectClassicChainChunks(final RID hubRid) {
    final List<RID> chunks = new ArrayList<>();
    database.transaction(() -> {
      RID current = ((VertexInternal) hubRid.asVertex(true)).getInEdgesHeadChunk();
      while (current != null) {
        chunks.add(current);
        current = ((EdgeSegment) ((DatabaseInternal) database).lookupByRID(current, true)).getPreviousRID();
      }
    });
    return chunks;
  }

  private void deleteRecordLowLevel(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
  }

  private Map<String, Object> runCheck(final boolean fix) {
    return new DatabaseChecker(database).setVerboseLevel(0).setFix(fix).check();
  }

  /**
   * Every old segment must be either deleted by the reclaim or - because {@code LocalBucket} recycles freed
   * positions - reused as a segment of the hub's REBUILT (reachable) layout. A readable old RID that is not
   * part of the rebuilt layout is a missed orphan.
   */
  private void assertOldSegmentsReclaimedOrReused(final RID hubRid, final List<RID> oldSegments) {
    database.transaction(() -> {
      final List<RID> reachableNow = collectHubReachableSegments(hubRid);
      for (final RID oldSegment : oldSegments) {
        Record survivor = null;
        try {
          survivor = ((DatabaseInternal) database).lookupByRID(oldSegment, true);
        } catch (final RecordNotFoundException e) {
          // RECLAIMED (or already gone): fine
        }
        if (survivor != null)
          assertThat(reachableNow).as("old segment " + oldSegment + " is readable, so its slot must have been "
              + "reused by the rebuilt layout - otherwise the reclaim missed an orphan").contains(oldSegment);
      }
    });
  }

  /** All segment RIDs of the hub's current IN layout: classic chain, or directory + every stripe chain. */
  private List<RID> collectHubReachableSegments(final RID hubRid) {
    final List<RID> segments = new ArrayList<>();
    final RID head = ((VertexInternal) hubRid.asVertex(true)).getInEdgesHeadChunk();
    if (head == null)
      return segments;
    final Record headRecord = ((DatabaseInternal) database).lookupByRID(head, true);
    if (headRecord instanceof StripeDirectory directory) {
      segments.add(head);
      for (int g = 0; g < directory.getGenerationCount(); g++)
        for (int s = 0; s < directory.getStripes(g); s++)
          collectChain(directory.getHead(g, s), segments);
    } else
      collectChain(head, segments);
    return segments;
  }

  private void collectChain(final RID head, final List<RID> segments) {
    RID current = head;
    while (current != null) {
      segments.add(current);
      current = ((EdgeSegment) ((DatabaseInternal) database).lookupByRID(current, true)).getPreviousRID();
    }
  }

  @Test
  void fixReclaimsOrphanedChunksAfterChainRebuild() {
    final int degree = 500;
    final RID hubRid = createHub(degree);

    final List<RID> oldChunks = collectClassicChainChunks(hubRid);
    assertThat(oldChunks.size()).as("the hub degree must span more than one chunk").isGreaterThan(1);

    // BREAK THE CHAIN MID-WAY: fix rebuilds the adjacency, the surviving old chunks become orphans.
    deleteRecordLowLevel(oldChunks.get(1));

    final Map<String, Object> stats = runCheck(true);

    // THE OLD CHUNKS HAVE BEEN RECLAIMED: each one is either deleted or its SLOT was legitimately reused by
    // a segment of the rebuilt chain (LocalBucket recycles freed positions).
    assertThat(((Number) stats.get("orphanedEdgeSegmentsReclaimed")).longValue())
        .as("the old chain's surviving chunks must be reclaimed").isGreaterThanOrEqualTo(1L);
    assertOldSegmentsReclaimedOrReused(hubRid, oldChunks);

    // THE REBUILT ADJACENCY IS INTACT AND A FOLLOW-UP PLAIN CHECK IS CLEAN.
    database.transaction(() ->
        assertThat(hubRid.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)).isEqualTo(degree));
    assertThat(((Number) runCheck(false).get("totalWarnings")).longValue()).isEqualTo(0L);
  }

  @Test
  void fixReclaimsOrphanedStripeDirectoryAfterSuperNodeRebuild() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);

    final int degree = 2_000;
    final RID hubRid = createHub(degree);

    // COLLECT THE OLD STRIPED LAYOUT: directory + every chain head.
    final List<RID> oldSegments = new ArrayList<>();
    final RID[] gen0Mid = new RID[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) hubRid.asVertex(true)).getInEdgesHeadChunk();
      final Record headRecord = ((DatabaseInternal) database).lookupByRID(head, true);
      assertThat(headRecord).as("the hub must have been promoted").isInstanceOf(StripeDirectory.class);
      final StripeDirectory directory = (StripeDirectory) headRecord;
      oldSegments.add(head);
      for (int g = 0; g < directory.getGenerationCount(); g++)
        for (int s = 0; s < directory.getStripes(g); s++)
          if (directory.getHead(g, s) != null)
            oldSegments.add(directory.getHead(g, s));
      final EdgeSegment gen0Head = (EdgeSegment) ((DatabaseInternal) database).lookupByRID(directory.getHead(0, 0), true);
      gen0Mid[0] = gen0Head.getPreviousRID();
      assertThat(gen0Mid[0]).as("the generation-0 chain must span more than one chunk").isNotNull();
    });

    deleteRecordLowLevel(gen0Mid[0]);

    final Map<String, Object> stats = runCheck(true);

    assertThat(((Number) stats.get("orphanedEdgeSegmentsReclaimed")).longValue()).isGreaterThanOrEqualTo(1L);
    // THE OLD DIRECTORY AND CHAIN HEADS ARE GONE (or their slots were reused by the rebuilt layout).
    assertOldSegmentsReclaimedOrReused(hubRid, oldSegments);

    database.transaction(() ->
        assertThat(hubRid.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)).isEqualTo(degree));
    assertThat(((Number) runCheck(false).get("totalWarnings")).longValue()).isEqualTo(0L);
  }

  /**
   * THE SAFETY TEST: on a healthy database - classic chains AND a promoted super-node - the reclaim must
   * delete NOTHING. A reachability walk missing any layout would classify live segments as orphans and
   * destroy data.
   */
  @Test
  void healthyDatabaseLosesNothing() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);

    final int degree = 2_000;
    final RID hubRid = createHub(degree); // promoted super-node + 2000 classic-layout sources

    final Map<String, Object> stats = runCheck(true);

    assertThat(((Number) stats.get("orphanedEdgeSegmentsReclaimed")).longValue())
        .as("a healthy database has no orphans to reclaim").isEqualTo(0L);
    database.transaction(() ->
        assertThat(hubRid.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)).isEqualTo(degree));
    assertThat(((Number) runCheck(false).get("totalWarnings")).longValue()).isEqualTo(0L);
  }

  /**
   * Focused regression for the checkEdges data-loss fix: the back-reference probe walking a vertex's BROKEN
   * edge list must warn (once) and leave the repair to the vertex check - it must NOT flag the vertex or the
   * edge as corrupted. Before the fix, running just checkEdges with fix deleted the perfectly healthy hub
   * vertex over its broken chain.
   */
  @Test
  void checkEdgesDoesNotBlameVertexForUnreadableEdgeList() {
    final int degree = 500;
    final RID hubRid = createHub(degree);

    final List<RID> oldChunks = collectClassicChainChunks(hubRid);
    deleteRecordLowLevel(oldChunks.get(1));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkEdges(EDGE_TYPE, true, 0);

    // NOTHING IS FLAGGED CORRUPTED: not the hub, not the edges probing its broken list.
    assertThat((Collection<?>) stats.get("corruptedRecords")).isEmpty();
    // THE UNREADABLE LIST IS WARNED EXACTLY ONCE despite hundreds of edges probing it.
    assertThat(((Collection<String>) stats.get("warnings")).stream()
        .filter(w -> w.contains("edge list is unreadable")).count()).isEqualTo(1);
    // AND THE HUB SURVIVED.
    database.transaction(() -> assertThat(hubRid.asVertex(true)).isNotNull());

    // REPAIR THE DELIBERATELY BROKEN CHAIN so the TestHelper post-test integrity check finds a clean database.
    runCheck(true);
  }

  /**
   * Data-loss guard (PR #5379 review): the reclaim must identify the internal edge-list buckets from the SCHEMA,
   * never by matching bucket names alone. A USER data bucket whose name collides with the edge-list naming scheme
   * (ends with {@code _out_edges}/{@code _in_edges}, or contains the stripe infix {@code _sn_stripe_}) has no
   * entry in the reachable set, so a name-only match would classify every record in it as an orphan and DELETE
   * it. Here two such colliding user buckets are populated and a full CHECK DATABASE FIX must leave them intact.
   */
  @Test
  void fullFixDoesNotDeleteUserBucketsCollidingWithSegmentNaming() {
    createHub(500); // a healthy graph so the reclaim runs (full scope) but finds no real orphans

    // A document type whose ONLY bucket is literally named "<x>_out_edges" (suffix collision), and a document
    // type whose default bucket name CONTAINS the stripe infix "_sn_stripe_" (infix collision).
    final String suffixBucketName = "Ledger" + GraphEngine.OUT_EDGES_SUFFIX;
    final String infixTypeName    = "Audit" + StripedEdgeList.STRIPE_BUCKET_INFIX + "journal";
    final int    suffixDocs       = 20;
    final int    infixDocs        = 15;

    database.transaction(() -> {
      final Bucket ledgerBucket = database.getSchema().createBucket(suffixBucketName);
      database.getSchema().createDocumentType("Ledger", List.of(ledgerBucket));
      database.getSchema().createDocumentType(infixTypeName, 1);
    });
    database.transaction(() -> {
      for (int i = 0; i < suffixDocs; i++)
        database.newDocument("Ledger").set("i", i).save();
      for (int i = 0; i < infixDocs; i++)
        database.newDocument(infixTypeName).set("i", i).save();
    });

    final Map<String, Object> stats = runCheck(true);

    // NO real orphans in a healthy graph, and NONE of the colliding user records were touched.
    assertThat(((Number) stats.get("orphanedEdgeSegmentsReclaimed")).longValue())
        .as("a healthy database has no orphans to reclaim").isEqualTo(0L);
    database.transaction(() -> {
      assertThat(database.countType("Ledger", false)).as("the _out_edges-named user bucket must be untouched")
          .isEqualTo((long) suffixDocs);
      assertThat(database.countType(infixTypeName, false)).as("the _sn_stripe_-named user bucket must be untouched")
          .isEqualTo((long) infixDocs);
    });
  }

  /** A type-filtered check walks only part of the graph, so it must NOT reclaim (false orphans otherwise). */
  @Test
  void filteredCheckDoesNotReclaim() {
    final int degree = 500;
    final RID hubRid = createHub(degree);

    final List<RID> oldChunks = collectClassicChainChunks(hubRid);
    deleteRecordLowLevel(oldChunks.get(1));

    final Map<String, Object> stats = database.command("sql", "CHECK DATABASE TYPE " + VERTEX_TYPE + " FIX")
        .next().toMap();

    // THE CHAIN IS STILL REBUILT, BUT NO RECLAIM RUNS UNDER A FILTER.
    assertThat(stats.get("orphanedEdgeSegmentsReclaimed")).isNull();
    database.transaction(() -> {
      assertThat(hubRid.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)).isEqualTo(degree);
      // The old head chunk survived: orphaned but not reclaimed.
      assertThat(((DatabaseInternal) database).lookupByRID(oldChunks.getFirst(), true)).isNotNull();
    });
  }
}
