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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Super-node striped edge list (#5156): when a vertex crosses GRAPH_SUPERNODE_THRESHOLD its edge list is
 * promoted to a {@link StripeDirectory} + N per-stripe chains hosted in a per-type bucket pool, so concurrent
 * appends stop contending on one page/file. These tests cover promotion, correctness of every edge-list
 * operation across the promotion boundary, persistence across reopen, disabled mode, and concurrency.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SuperNodeStripingTest extends TestHelper {
  private int savedThreshold;
  private int savedStripes;
  private int savedRetryDelay;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 8);
      database.getSchema().createEdgeType("LINK", 8);
    });
  }

  private RID createHub() {
    final MutableVertex[] holder = new MutableVertex[1];
    database.transaction(() -> {
      holder[0] = database.newVertex("Hub");
      holder[0].save();
    });
    return holder[0].getIdentity();
  }

  /** Inserts {@code count} edges Src->Hub, one transaction each, returning the source vertex RIDs in order. */
  private List<RID> insertEdges(final RID hubRID, final int count) {
    final List<RID> sources = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final RID[] srcHolder = new RID[1];
      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        src.newEdge("LINK", hubRID);
        srcHolder[0] = src.getIdentity();
      });
      sources.add(srcHolder[0]);
    }
    return sources;
  }

  private Record loadInHead(final RID hubRID) {
    final Record[] head = new Record[1];
    database.transaction(() -> {
      final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      head[0] = database.lookupByRID(headRID, true);
    });
    return head[0];
  }

  @Test
  void promotionAtThresholdKeepsEveryEdgeAndEveryOperation() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    final int total = 300;
    final List<RID> sources = insertEdges(hubRID, total);

    // THE HUB'S IN LIST MUST HAVE BEEN PROMOTED TO THE STRIPED LAYOUT
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);
    // THE SOURCES (LOW DEGREE) MUST STAY CLASSIC
    database.transaction(() -> {
      final RID srcOutHead = ((VertexInternal) sources.getFirst().asVertex(true)).getOutEdgesHeadChunk();
      assertThat(database.lookupByRID(srcOutHead, true)).isInstanceOf(MutableEdgeSegment.class);
    });

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);

      // COUNT: EVERY EDGE, ACROSS GENERATION 0 (PRE-PROMOTION) AND THE STRIPES
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total);

      // ITERATION: NO EDGE LOST, NO EDGE DUPLICATED
      final Set<RID> found = new HashSet<>();
      for (final Iterator<Vertex> it = hub.getVertices(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); )
        assertThat(found.add(it.next().getIdentity())).isTrue();
      assertThat(found).hasSize(total);
      assertThat(found).containsAll(sources);

      // CONNECTIVITY: LOCALISED LOOKUPS WORK FOR PRE-PROMOTION AND POST-PROMOTION NEIGHBOURS
      assertThat(hub.isConnectedTo(sources.getFirst(), Vertex.DIRECTION.IN)).isTrue();
      assertThat(hub.isConnectedTo(sources.getLast(), Vertex.DIRECTION.IN)).isTrue();
      assertThat(hub.isConnectedTo(hubRID, Vertex.DIRECTION.IN)).isFalse();
    });
  }

  @Test
  void promotionInsideSingleBigTransactionKeepsEveryEdge() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(128);
    createSchema();
    final RID hubRID = createHub();

    final int total = 1_000;
    // ONE BIG TRANSACTION: the hub's OUT list crosses the promotion threshold mid-transaction while the SAME
    // vertex instance keeps appending - the InsertGraphIndexTest pattern that surfaced lost edges (2026-07-10).
    database.transaction(() -> {
      final MutableVertex hub = hubRID.asVertex(true).modify();
      for (int i = 0; i < total; i++) {
        final MutableVertex peer = database.newVertex("Src");
        peer.save();
        hub.newEdge("LINK", peer);
      }
    });

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(total);

      final Set<RID> found = new HashSet<>();
      for (final Vertex v : hub.getVertices(Vertex.DIRECTION.OUT, "LINK"))
        assertThat(found.add(v.getIdentity())).isTrue();
      assertThat(found).hasSize(total);
    });
  }

  @Test
  void belowThresholdStaysClassic() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(10_000);
    createSchema();
    final RID hubRID = createHub();

    insertEdges(hubRID, 200);

    assertThat(loadInHead(hubRID)).isInstanceOf(MutableEdgeSegment.class);
    database.transaction(() -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(200));
  }

  @Test
  void zeroThresholdDisablesPromotion() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
    createSchema();
    final RID hubRID = createHub();

    insertEdges(hubRID, 300);

    assertThat(loadInHead(hubRID)).isInstanceOf(MutableEdgeSegment.class);
    // NO STRIPE BUCKETS MUST HAVE BEEN CREATED
    assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", 0))).isFalse();
  }

  @Test
  void promotedVertexSurvivesReopen() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    final int total = 300;
    final List<RID> sources = insertEdges(hubRID, total);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    reopenDatabase();

    // REBUILD PLAIN RIDS: THE CAPTURED ONES ARE BOUND TO THE CLOSED DATABASE INSTANCE
    final RID hub2 = new RID(hubRID.getBucketId(), hubRID.getPosition());
    final RID firstSrc = new RID(sources.getFirst().getBucketId(), sources.getFirst().getPosition());
    final RID lastSrc = new RID(sources.getLast().getBucketId(), sources.getLast().getPosition());

    assertThat(loadInHead(hub2)).isInstanceOf(StripeDirectory.class);
    database.transaction(() -> {
      final Vertex hub = hub2.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total);
      assertThat(hub.isConnectedTo(firstSrc, Vertex.DIRECTION.IN)).isTrue();
      assertThat(hub.isConnectedTo(lastSrc, Vertex.DIRECTION.IN)).isTrue();
    });

    // AND THE PROMOTED VERTEX MUST STILL ACCEPT NEW EDGES AFTER THE REOPEN
    insertEdges(hub2, 10);
    database.transaction(() -> assertThat(hub2.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total + 10));
  }

  @Test
  void removeAndDeleteOnPromotedVertex() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    final int total = 300;
    final List<RID> sources = insertEdges(hubRID, total);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    // DELETE A PRE-PROMOTION EDGE (GENERATION 0) AND A POST-PROMOTION ONE (STRIPES)
    database.transaction(() -> {
      sources.getFirst().asVertex(true).getEdges(Vertex.DIRECTION.OUT, "LINK").iterator().next().delete();
      sources.getLast().asVertex(true).getEdges(Vertex.DIRECTION.OUT, "LINK").iterator().next().delete();
    });

    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total - 2);
      assertThat(hub.isConnectedTo(sources.getFirst(), Vertex.DIRECTION.IN)).isFalse();
      assertThat(hub.isConnectedTo(sources.getLast(), Vertex.DIRECTION.IN)).isFalse();
      assertThat(hub.isConnectedTo(sources.get(1), Vertex.DIRECTION.IN)).isTrue();
    });

    // DELETING THE PROMOTED VERTEX MUST DROP THE DIRECTORY AND EVERY STRIPE CHAIN (integrity checked at teardown)
    database.transaction(() -> hubRID.asVertex(true).delete());
    database.transaction(() -> assertThat(database.countType("Hub", false)).isEqualTo(0));
  }

  /**
   * Mass lifecycle: 100K edges into one promoted hub, then delete EVERY edge and audit the residue - the
   * drained stripe chains and directory must stay structurally sound (and reusable), and deleting the hub
   * must leave ZERO records behind in the stripe pool buckets and in the hub's edge-list bucket.
   */
  @Test
  @Tag("slow")
  void massAddThenDeleteAllLeavesNoResidue() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(128);
    createSchema();
    final RID hubRID = createHub();

    final int total = 100_000;
    final int batch = 1_000;
    for (int b = 0; b < total / batch; b++)
      database.transaction(() -> {
        for (int i = 0; i < batch; i++) {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          src.newEdge("LINK", hubRID);
        }
      });

    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);
    database.transaction(() -> assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total));

    // DELETE EVERY EDGE (batched): collect the RIDs first, then drop them
    final List<RID> edgeRIDs = new ArrayList<>(total);
    database.transaction(() -> {
      for (final Edge e : hubRID.asVertex(true).getEdges(Vertex.DIRECTION.IN, "LINK"))
        edgeRIDs.add(e.getIdentity());
    });
    assertThat(edgeRIDs).hasSize(total);
    for (int b = 0; b < edgeRIDs.size(); b += batch) {
      final int from = b, to = Math.min(b + batch, edgeRIDs.size());
      database.transaction(() -> {
        for (int i = from; i < to; i++)
          edgeRIDs.get(i).asEdge(true).delete();
      });
    }

    // THE DRAINED HUB: no edges left in either direction, still promoted, still fully usable
    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(0);
      assertThat(hub.getEdges(Vertex.DIRECTION.IN, "LINK").iterator().hasNext()).isFalse();
      assertThat(hub.getVertices(Vertex.DIRECTION.IN, "LINK").iterator().hasNext()).isFalse();
    });
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);
    assertThat(database.countType("LINK", false)).isEqualTo(0);

    // THE DRAINED STRIPES MUST ACCEPT NEW EDGES (structure reusable after a full drain)
    final List<RID> after = insertEdges(hubRID, 10);
    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(10);
      assertThat(hub.isConnectedTo(after.getFirst(), Vertex.DIRECTION.IN)).isTrue();
    });

    // DELETE THE HUB: the directory and EVERY chunk must go - audit the buckets for leaked records
    database.transaction(() -> {
      for (final Edge e : hubRID.asVertex(true).getEdges(Vertex.DIRECTION.IN, "LINK"))
        e.delete();
      hubRID.asVertex(true).delete();
    });
    database.transaction(() -> {
      for (int i = 0; i < GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger(); i++) {
        final String bucketName = StripedEdgeList.stripeBucketName("Hub", i);
        long residue = 0;
        for (final Iterator<Record> it = database.iterateBucket(bucketName); it.hasNext(); it.next())
          residue++;
        assertThat(residue).as("records leaked in stripe bucket " + bucketName).isEqualTo(0);
      }
      long inChunks = 0;
      for (final Iterator<Record> it = database.iterateBucket("Hub_0_in_edges"); it.hasNext(); it.next())
        inChunks++;
      assertThat(inChunks).as("records leaked in the hub's edge-list bucket").isEqualTo(0);
    });
  }

  /** The no-content factory path creates a LAZY directory placeholder: every accessor must self-load (Gemini review). */
  @Test
  void lazyLoadedDirectoryIsReadable() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();
    insertEdges(hubRID, 300);

    database.transaction(() -> {
      final RID dirRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
      final Record lazy = database.lookupByRID(dirRID, false);
      assertThat(lazy).isInstanceOf(StripeDirectory.class);
      final StripeDirectory directory = (StripeDirectory) lazy;
      assertThat(directory.getGenerationCount()).isEqualTo(2);
      assertThat(directory.getStripes(1)).isEqualTo(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger());
      assertThat(directory.getChainCount()).isGreaterThan(1);
      assertThat(directory.getContent().size()).isGreaterThan(0);
      assertThat(directory.toJSON(false).getJSONArray("stripes").length()).isEqualTo(2);
    });
  }

  /**
   * Appends must actually SPREAD across the stripes (placement by neighbour-RID hash): a regression in
   * {@link StripeDirectory#stripeOf} concentrating everything on one stripe would silently re-serialise the
   * writers the layout exists to parallelise.
   */
  @Test
  void appendsSpreadAcrossStripes() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    final int total = 2_000;
    insertEdges(hubRID, total);
    final StripeDirectory directory = (StripeDirectory) loadInHead(hubRID);

    database.transaction(() -> {
      final int stripes = directory.getStripes(1);
      long striped = 0;
      long min = Long.MAX_VALUE;
      for (int slot = 0; slot < stripes; slot++) {
        long entries = 0;
        RID chunkRID = directory.getHead(1, slot);
        while (chunkRID != null) {
          final EdgeSegment chunk = (EdgeSegment) database.lookupByRID(chunkRID, true);
          entries += chunk.count(null);
          chunkRID = chunk.getPreviousRID();
        }
        striped += entries;
        min = Math.min(min, entries);
      }
      // Every stripe must have received a fair share: with ~uniform hashing over distinct source RIDs the
      // expected per-stripe load is striped/stripes; a stripe below a QUARTER of it means the hash regressed.
      assertThat(striped).isGreaterThan(total / 2);
      assertThat(min).isGreaterThanOrEqualTo(striped / stripes / 4);
    });
  }

  /** Same distribution guard with a NON-power-of-two stripe count (the documented tuning allows any value):
   * the murmur-style finaliser in {@link StripeDirectory#stripeOf} must not alias on the modulo. */
  @Test
  void appendsSpreadAcrossNonPowerOfTwoStripes() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(12);
    createSchema();
    final RID hubRID = createHub();

    final int total = 2_000;
    insertEdges(hubRID, total);
    final StripeDirectory directory = (StripeDirectory) loadInHead(hubRID);

    database.transaction(() -> {
      final int stripes = directory.getStripes(1);
      assertThat(stripes).isEqualTo(12);
      long striped = 0;
      long min = Long.MAX_VALUE;
      for (int slot = 0; slot < stripes; slot++) {
        long entries = 0;
        RID chunkRID = directory.getHead(1, slot);
        while (chunkRID != null) {
          final EdgeSegment chunk = (EdgeSegment) database.lookupByRID(chunkRID, true);
          entries += chunk.count(null);
          chunkRID = chunk.getPreviousRID();
        }
        striped += entries;
        min = Math.min(min, entries);
      }
      assertThat(striped).isGreaterThan(total / 2);
      assertThat(min).isGreaterThanOrEqualTo(striped / stripes / 4);
    });
  }

  /**
   * Promotion is ONE-WAY: an already-promoted vertex must keep working (reads AND writes) when promotion is
   * later disabled ({@code threshold=0}) - the setting only stops FUTURE promotions.
   */
  @Test
  void promotedVertexKeepsWorkingWithPromotionDisabled() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();
    insertEdges(hubRID, 300);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);

    final List<RID> after = insertEdges(hubRID, 100);
    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(400);
      assertThat(hub.isConnectedTo(after.getLast(), Vertex.DIRECTION.IN)).isTrue();
      final Set<RID> found = new HashSet<>();
      for (final Vertex v : hub.getVertices(Vertex.DIRECTION.IN, "LINK"))
        assertThat(found.add(v.getIdentity())).isTrue();
      assertThat(found).hasSize(400);
    });

    // AND STILL ACCEPTS REMOVALS
    database.transaction(() -> after.getFirst().asVertex(true).getEdges(Vertex.DIRECTION.OUT, "LINK").iterator().next().delete());
    database.transaction(() -> assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(399));
  }

  /** The drop sweep must remove a pool created under a LARGER stripe setting than the current one (the
   * contiguity walk past the configured count - no fixed cap to leak beyond). */
  @Test
  void dropTypeRemovesStripePoolCreatedUnderLargerSetting() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(16);
    createSchema();
    final RID hubRID = createHub();
    insertEdges(hubRID, 300);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);
    for (int i = 0; i < 16; i++)
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", i))).isTrue();

    // THE SETTING SHRINKS AFTER THE POOL WAS CREATED
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(4);

    database.transaction(() -> hubRID.asVertex(true).delete());
    database.getSchema().dropType("Hub");

    for (int i = 0; i < 16; i++)
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", i)))
          .as("stripe bucket %d must be dropped despite the smaller configured count", i).isFalse();
  }

  /**
   * A GENUINELY missing head chunk (real corruption, not the transient cross-file publication window) must
   * surface as a RETRYABLE conflict on writes - never silently reset the head and orphan the rest of the list,
   * which was the previous recovery - and must stay repairable via CHECK DATABASE FIX (the checker nulls the
   * broken head and reconnects the edges from the edge records).
   */
  @Test
  void missingHeadChunkFailsRetryablyAndCheckDatabaseRepairs() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(10_000); // classic layout: this is the shared path
    createSchema();
    final RID hubRID = createHub();
    insertEdges(hubRID, 10);

    // SIMULATE CORRUPTION: the IN head chunk record vanishes
    database.transaction(
        () -> database.lookupByRID(((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk(), true).delete());

    // A WRITE must fail with a retryable exception, not silently rebuild the head
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableVertex src = database.newVertex("Src");
      src.save();
      src.newEdge("LINK", hubRID);
    })).isInstanceOf(NeedRetryException.class);

    // CHECK DATABASE FIX repairs: the broken head is nulled and every edge reconnected from its record
    try (final ResultSet result = database.command("sql", "check database fix")) {
      assertThat(result.hasNext()).isTrue();
    }

    database.transaction(() -> assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(10));

    // AND THE REPAIRED LIST ACCEPTS NEW EDGES
    insertEdges(hubRID, 5);
    database.transaction(() -> assertThat(hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(15));
  }

  /**
   * A transiently unresolvable stripe chain must split by caller intent: MUTATING and neighbour-keyed
   * (dedup-feeding) operations surface it as a RETRYABLE conflict - silently skipping the chain would commit a
   * removal that leaves a stale back-reference, or feed a MERGE-style existence check a false negative - while
   * bulk READS stay best-effort (skip the chain, no failure).
   */
  @Test
  void transientlyMissingStripeChainIsRetryableForWritesBestEffortForReads() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = insertEdges(hubRID, 300);
    final StripeDirectory directory = (StripeDirectory) loadInHead(hubRID);

    // PICK A POST-PROMOTION SOURCE AND DROP ITS GEN-1 STRIPE HEAD (simulates the publication window)
    final RID victim = sources.getLast();
    final int slot = StripeDirectory.stripeOf(victim, directory.getStripes(1));
    database.transaction(() -> database.lookupByRID(directory.getHead(1, slot), true).delete());

    // NEIGHBOUR-KEYED LOOKUPS AND REMOVALS: RETRYABLE CONFLICT, NEVER A SILENT SKIP
    assertThatThrownBy(() -> database.transaction(() -> hubRID.asVertex(true).isConnectedTo(victim, Vertex.DIRECTION.IN)))
        .isInstanceOf(NeedRetryException.class);
    assertThatThrownBy(() -> database.transaction(
        () -> victim.asVertex(true).getEdges(Vertex.DIRECTION.OUT, "LINK").iterator().next().delete()))
        .isInstanceOf(NeedRetryException.class);

    // BULK READS: BEST-EFFORT, NO FAILURE (the unresolvable chain is skipped)
    database.transaction(() -> {
      final long count = hubRID.asVertex(true).countEdges(Vertex.DIRECTION.IN, "LINK");
      assertThat(count).isGreaterThan(0).isLessThanOrEqualTo(300);
      int iterated = 0;
      for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); it.next())
        iterated++;
      assertThat(iterated).isGreaterThan(0);
    });

    // REPAIR SO THE TEARDOWN INTEGRITY GATE PASSES
    try (final ResultSet result = database.command("sql", "check database fix")) {
      assertThat(result.hasNext()).isTrue();
    }
    try (final ResultSet result = database.command("sql", "check database fix")) {
      assertThat(result.hasNext()).isTrue();
    }
  }

  /**
   * A USER bucket owned by a type that happens to match the stripe pool naming scheme must NEVER be adopted as
   * a stripe (writing edge segments into another type's data = cross-type corruption): promotion is skipped and
   * the vertex stays classic and fully functional.
   */
  @Test
  void poolNameCollisionWithUserBucketSkipsPromotion() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    // A user bucket named like slot 0 of Hub's pool, OWNED by another type
    database.transaction(() -> {
      final com.arcadedb.engine.Bucket collision = database.getSchema().createBucket(StripedEdgeList.stripeBucketName("Hub", 0));
      database.getSchema().getType("Src").addBucket(collision);
    });
    final RID hubRID = createHub();

    final int total = 300;
    final List<RID> sources = insertEdges(hubRID, total);

    // PROMOTION MUST HAVE BEEN SKIPPED: CLASSIC LAYOUT, EVERYTHING WORKS
    assertThat(loadInHead(hubRID)).isInstanceOf(MutableEdgeSegment.class);
    database.transaction(() -> {
      final Vertex hub = hubRID.asVertex(true);
      assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(total);
      assertThat(hub.isConnectedTo(sources.getFirst(), Vertex.DIRECTION.IN)).isTrue();
    });
  }

  @Test
  void dropTypeRemovesStripePool() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    createSchema();
    final RID hubRID = createHub();

    insertEdges(hubRID, 300);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);
    assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", 0))).isTrue();

    // DELETE THE HUB FIRST (WITH ITS EDGES): DROPPING A TYPE NEVER CASCADES ON CONNECTED EDGES
    database.transaction(() -> hubRID.asVertex(true).delete());

    database.getSchema().dropType("Hub");

    for (int i = 0; i < GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger(); i++)
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Hub", i))).isFalse();
  }

  @Tag("slow")
  @Test
  void concurrentAppendsOnPromotedVertex() throws InterruptedException {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    createSchema();
    final RID hubRID = createHub();

    // PROMOTE FIRST WITH A SINGLE THREAD, THEN HAMMER THE STRIPES CONCURRENTLY
    final int warmup = 200;
    insertEdges(hubRID, warmup);
    assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

    final int threads = 6;
    final int perThread = 500;
    final AtomicLong failures = new AtomicLong();
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          done.countDown();
          return;
        }
        for (int i = 0; i < perThread; i++) {
          try {
            database.transaction(() -> {
              final MutableVertex src = database.newVertex("Src");
              src.save();
              src.newEdge("LINK", hubRID);
            }, false, 10_000);
          } catch (final Exception e) {
            failures.incrementAndGet();
          }
        }
        done.countDown();
      }).start();
    }
    start.countDown();
    done.await();

    // Retry-exhausted transactions are tolerated (a loaded CI box can starve a writer past its retry budget;
    // each failed lambda never counted its edge). The REAL lost-update gate: every SUCCESSFUL transaction's
    // edge must be present - the expected degree accounts for the failures exactly.
    database.transaction(() -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
        .isEqualTo(warmup + threads * perThread - failures.get()));
  }
}
