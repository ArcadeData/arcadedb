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
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5760: deleting a vertex must not disconnect its edges from ITSELF.
 * <p>
 * {@code GraphEngine.deleteVertex} walked its own edge list, then handed every edge to {@code deleteEdge}, which
 * disconnects it from BOTH endpoints. One of those two endpoints is always the vertex being deleted, and
 * disconnecting it means a chain walk from the head probing each chunk for the entry, an anchor and a page copy of
 * the chunk that holds it, a compaction of that chunk and a write-back - per edge, over a list
 * {@code deleteRemainingChunks} deletes wholesale a moment later. Pure waste, and worst on exactly the shape where
 * it hurts most: a promoted super-node, where {@code StripedEdgeList.removeEdge} pays it once per generation.
 * <p>
 * Skipping it also removed the reason the walk could not stream. The two-phase "collect every edge into a list,
 * then delete them" existed because the self-side removals relinked and deleted chunks underneath the iterator; with
 * the self side skipped, the delete no longer writes the list it is reading, so one pass is legal and the
 * accumulator - one live {@code Edge} per edge, retained for the whole delete - is gone.
 * <p>
 * The mechanism tests below use the per-session counters in {@code DatabaseStats}, which count exactly the two
 * operations the removed work performed: {@code existsRecord} once per endpoint resolution
 * ({@code resolveEndpointToDisconnect}), and {@code readRecord} once per {@code lookupByRID}. They are assertions
 * about work NOT done, so they are stated as exact counts rather than bounds - a regression that quietly restores
 * the self-side walk moves them immediately.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5760VertexDeleteSelfSideSkipTest extends TestHelper {

  private static final int EDGES = 200;

  /**
   * The mechanism, on the simplest shape: a vertex with N outgoing edges to N distinct neighbours. Deleting it
   * resolves exactly N endpoints - the far ones. It used to resolve 2N, and the extra N were resolutions of the
   * vertex being deleted, each one followed by a walk of the list about to be dropped.
   */
  @Test
  void deletingAVertexResolvesOnlyTheFarEndpointOfEachEdge() {
    createSchema();
    final RID srcRID = createStar(EDGES);

    database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK"))
        .as("the fixture must have something to disconnect").isEqualTo(EDGES));

    final long resolutions = countEndpointResolutions(() -> database.transaction(() -> srcRID.asVertex().delete()));

    assertThat(resolutions).as("one endpoint resolution per edge (the far one), never two").isEqualTo(EDGES);
  }

  /**
   * The shape that leaves nothing at all to do, and the one the skip has to get right rather than merely fast:
   * every edge is a self-loop, so BOTH of its endpoints are the vertex being deleted and both lists are dropped.
   * Zero resolutions - it used to be 2N - and the self-loops still die with their only endpoint.
   */
  @Test
  void deletingAVertexOfSelfLoopsResolvesNoEndpointAtAll() {
    createSchema();

    final RID vertexRID;
    final List<RID> selfLoops = new ArrayList<>(20);
    {
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Src");
        v.save();
        holder[0] = v.getIdentity();
      });
      vertexRID = holder[0];
      for (int i = 0; i < 20; i++) {
        final RID[] edge = new RID[1];
        database.transaction(() -> edge[0] = vertexRID.asVertex().modify().newEdge("LINK", vertexRID).getIdentity());
        selfLoops.add(edge[0]);
      }
    }

    database.transaction(() -> {
      assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(selfLoops.size());
      assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(selfLoops.size());
    });

    final long resolutions = countEndpointResolutions(() -> database.transaction(() -> vertexRID.asVertex().delete()));

    assertThat(resolutions).as("a self-loop has no far endpoint: nothing to resolve, nothing to disconnect")
        .isEqualTo(0L);

    database.transaction(() -> {
      assertThat(database.existsRecord(vertexRID)).isFalse();
      for (final RID loop : selfLoops)
        assertThat(database.existsRecord(loop)).as("self-loop " + loop + " must not outlive its only endpoint")
            .isFalse();
    });

    assertIntegrityClean();
  }

  /**
   * The walk streams. Measured where the difference actually shows: how much reading has happened by the time the
   * FIRST edge record is deleted. Two-phase collection reads the whole list first (two {@code lookupByRID} per
   * entry - one to validate it while peeking, one to materialise it - so at least 2N reads before anything is
   * deleted); a streaming walk has read only the head chunk and that one edge.
   */
  @Test
  void theRemovalWalkDeletesAsItGoesInsteadOfMaterialisingEveryEdgeFirst() {
    createSchema();
    final RID srcRID = createStar(EDGES);

    final AtomicLong readsAtFirstEdgeDelete = new AtomicLong(-1);
    final AtomicLong edgesDeleted = new AtomicLong();
    final BeforeRecordDeleteListener probe = record -> {
      if (record instanceof Edge) {
        edgesDeleted.incrementAndGet();
        readsAtFirstEdgeDelete.compareAndSet(-1, stat("readRecord"));
      }
      return true;
    };

    database.getSchema().getType("LINK").getEvents().registerListener(probe);
    final long readsAtStart;
    try {
      readsAtStart = stat("readRecord");
      database.transaction(() -> srcRID.asVertex().delete());
    } finally {
      database.getSchema().getType("LINK").getEvents().unregisterListener(probe);
    }

    assertThat(edgesDeleted.get()).as("every edge must still be deleted").isEqualTo(EDGES);
    assertThat(readsAtFirstEdgeDelete.get() - readsAtStart)
        .as("record reads before the first edge is deleted: a two-phase collection reads the whole list first")
        .isLessThan(EDGES);

    assertIntegrityClean();
  }

  /**
   * Correctness of the skip on a mixed shape, which is where a "skip by which list the edge came from" instead of
   * "skip by the endpoint RID recorded on the edge" would come apart: the vertex has outgoing edges, incoming
   * edges, several parallel edges to the SAME neighbour, a MUTUAL pair (the same neighbour on both sides, so one
   * neighbour is reached from both walks) and a self-loop. Every far endpoint must lose its back-reference;
   * nothing else may.
   */
  @Test
  void everyEdgeOfADeletedVertexIsDisconnectedFromTheVertexAtItsOtherEnd() {
    createSchema();

    final RID[] holder = new RID[5];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      final MutableVertex out = database.newVertex("Src");
      out.save();
      final MutableVertex in = database.newVertex("Src");
      in.save();
      final MutableVertex peer = database.newVertex("Src");
      peer.save();
      final MutableVertex bystander = database.newVertex("Src");
      bystander.save();

      // Parallel edges to the same neighbour, both directions, a mutual pair and a self-loop.
      for (int i = 0; i < 5; i++)
        hub.newEdge("LINK", out);
      for (int i = 0; i < 5; i++)
        in.newEdge("LINK", hub);
      hub.newEdge("LINK", peer);
      peer.newEdge("LINK", hub);
      hub.newEdge("LINK", hub);
      // An edge that does NOT touch the hub: it must survive untouched.
      in.newEdge("LINK", bystander);

      holder[0] = hub.getIdentity();
      holder[1] = out.getIdentity();
      holder[2] = in.getIdentity();
      holder[3] = peer.getIdentity();
      holder[4] = bystander.getIdentity();
    });
    final RID hubRID = holder[0], outRID = holder[1], inRID = holder[2], peerRID = holder[3];
    final RID bystanderRID = holder[4];

    database.transaction(() -> {
      assertThat(outRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(5);
      assertThat(inRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(6);
      assertThat(peerRID.asVertex().countEdges(Vertex.DIRECTION.BOTH, "LINK")).isEqualTo(2);
      assertThat(database.countType("LINK", false)).isEqualTo(14L);
    });

    database.transaction(() -> hubRID.asVertex().delete());

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).isFalse();
      assertThat(outRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
          .as("the parallel edges into the neighbour must all be disconnected").isEqualTo(0);
      assertThat(inRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK"))
          .as("only the edge to the bystander may survive").isEqualTo(1);
      assertThat(peerRID.asVertex().countEdges(Vertex.DIRECTION.BOTH, "LINK"))
          .as("the mutual pair must be disconnected from both of the peer's lists").isEqualTo(0);
      assertThat(bystanderRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
          .as("an edge that never touched the deleted vertex is untouched").isEqualTo(1);
      assertThat(database.countType("LINK", false)).as("13 of the 14 edges touched the hub").isEqualTo(1L);
    });

    assertIntegrityClean();
  }

  /**
   * The walk over a list this very transaction is still building. Creating a vertex, its edges and then deleting it
   * without committing in between means every chunk the walk reads is the transaction's own pending copy rather
   * than a committed page - the one case where "the delete does not write the list it is reading" has to hold
   * against writes that are already there.
   */
  @Test
  void aVertexCreatedAndDeletedInTheSameTransactionLeavesNothingBehind() {
    createSchema();

    final RID[] neighbours = new RID[30];
    database.transaction(() -> {
      for (int i = 0; i < neighbours.length; i++) {
        final MutableVertex n = database.newVertex("Src");
        n.save();
        neighbours[i] = n.getIdentity();
      }
    });

    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      for (int i = 0; i < neighbours.length; i++)
        if (i % 2 == 0)
          hub.newEdge("LINK", neighbours[i]);
        else
          neighbours[i].asVertex().modify().newEdge("LINK", hub);
      hub.newEdge("LINK", hub);

      assertThat(hub.countEdges(Vertex.DIRECTION.BOTH, "LINK")).isEqualTo(neighbours.length + 2);

      hub.delete();
    });

    database.transaction(() -> {
      assertThat(database.countType("LINK", false)).as("no edge may outlive the vertex created with it").isEqualTo(0L);
      for (final RID neighbour : neighbours)
        assertThat(neighbour.asVertex().countEdges(Vertex.DIRECTION.BOTH, "LINK"))
            .as("neighbour " + neighbour + " must keep no back-reference").isEqualTo(0);
    });

    assertIntegrityClean();
  }

  /**
   * The same on a PROMOTED SUPER-NODE (#5156), which is where the removed work was most expensive: the striped
   * layout resolves one chain per generation for every single removal, and generation 0 is the whole pre-promotion
   * chain. The skip must leave the far-end disconnection - and the chunk drain across every stripe - intact.
   */
  @Test
  void deletingAPromotedSuperNodeStillDisconnectsEveryNeighbour() {
    final int threshold = 64;
    final int edges = 400;
    final Object savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValue();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(threshold);
    try {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = createIncomingEdges(hubRID, edges);

      database.transaction(() -> {
        assertThat(((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk()).isNotNull();
        assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(edges);
        assertThat(database.lookupByRID(((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk(), true))
            .as("the hub must really be promoted, otherwise this is the classic layout again")
            .isInstanceOf(StripeDirectory.class);
      });

      final long resolutions = countEndpointResolutions(
          () -> database.transaction(() -> hubRID.asVertex().delete()));
      assertThat(resolutions).as("one resolution per edge (the source), never the hub itself").isEqualTo(edges);

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).isFalse();
        assertThat(database.countType("LINK", false)).as("every edge pointed at the hub").isEqualTo(0L);
        for (final RID source : sources)
          assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK"))
              .as("source " + source + " must not keep a back-reference to a deleted edge").isEqualTo(0);
      });

      assertIntegrityClean();
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }

  /**
   * A lightweight edge has no record to delete, so ALL the work of deleting one is the two list removals - which
   * makes it the shape where skipping the self side is the entire operation. It is also located in the far list by
   * (edge type bucket, far endpoint) rather than by an edge RID, so the skip must not disturb that lookup.
   */
  @Test
  void deletingAVertexOfLightweightEdgesLeavesNoBackReference() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 4);
      database.getSchema().buildEdgeType().withName("LIGHT").withTotalBuckets(4).withLightweight(true).create();
    });

    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      holder[0] = hub.getIdentity();
    });
    final RID hubRID = holder[0];

    final List<RID> sources = new ArrayList<>(50);
    for (int i = 0; i < 50; i++) {
      final RID[] src = new RID[1];
      database.transaction(() -> {
        final MutableVertex s = database.newVertex("Src");
        s.save();
        s.newEdge("LIGHT", hubRID);
        src[0] = s.getIdentity();
      });
      sources.add(src[0]);
    }

    database.transaction(() -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LIGHT"))
        .isEqualTo(sources.size()));

    database.transaction(() -> hubRID.asVertex().delete());

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).isFalse();
      for (final RID source : sources)
        assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "LIGHT"))
            .as("source " + source + " must not keep a lightweight back-reference to a deleted vertex").isEqualTo(0);
    });

    assertIntegrityClean();
  }

  /**
   * A forced delete keeps every tolerance it had: the far-end disconnection still happens on a healthy neighbour
   * (that is #5680's contract, and the skip must not be mistaken for "force skips the disconnection"), and the
   * vertex still goes.
   */
  @Test
  void aForcedDeleteStillDisconnectsTheFarEndpoints() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> sources = createIncomingEdges(hubRID, 10);

    database.transaction(
        () -> ((DatabaseInternal) database).getGraphEngine().deleteVertex((VertexInternal) hubRID.asVertex(), true));

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).isFalse();
      assertThat(database.countType("LINK", false)).isEqualTo(0L);
      for (final RID source : sources)
        assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(0);
    });

    assertIntegrityClean();
  }

  // ---------------------------------------------------------------------------------------------------------------

  /**
   * The {@code existsRecord} calls made while {@code body} runs. {@code GraphEngine.resolveEndpointToDisconnect} is
   * the only thing on the vertex-delete path that calls it, exactly once per endpoint it is about to disconnect, so
   * this counts endpoint resolutions and nothing else.
   */
  private long countEndpointResolutions(final Runnable body) {
    final long before = stat("existsRecord");
    body.run();
    return stat("existsRecord") - before;
  }

  private long stat(final String name) {
    return ((Number) database.getStats().get(name)).longValue();
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 8);
      database.getSchema().createEdgeType("LINK", 8);
    });
  }

  private RID createHub() {
    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      holder[0] = hub.getIdentity();
    });
    return holder[0];
  }

  /** A vertex with {@code count} outgoing edges, one per transaction so its OUT chain grows chunk by chunk. */
  private RID createStar(final int count) {
    final RID srcRID = createSource();
    for (int i = 0; i < count; i++)
      database.transaction(() -> {
        final MutableVertex target = database.newVertex("Src");
        target.save();
        srcRID.asVertex().modify().newEdge("LINK", target);
      });
    return srcRID;
  }

  private RID createSource() {
    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex src = database.newVertex("Src");
      src.save();
      holder[0] = src.getIdentity();
    });
    return holder[0];
  }

  /** {@code count} distinct sources each pointing at the hub, one per transaction. Returns the source RIDs. */
  private List<RID> createIncomingEdges(final RID hubRID, final int count) {
    final List<RID> sources = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        src.newEdge("LINK", hubRID);
        holder[0] = src.getIdentity();
      });
      sources.add(holder[0]);
    }
    return sources;
  }

  private void assertIntegrityClean() {
    try (final ResultSet rs = database.command("sql", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("check database autoFix: " + row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalErrors")).as("check database totalErrors: " + row.toJSON()).isEqualTo(0L);
      }
    }
  }

  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }
}
