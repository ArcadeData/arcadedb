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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.VertexNotFoundException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the VertexDeleteRepair area, one nested class per former stand-alone regression class.
 */
class VertexDeleteRepairTest {
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
  @Nested
  class VertexDeleteSelfSideSkip extends TestHelper {

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
     * A self-loop is reachable from BOTH of the vertex's lists, and both walks used to yield it, so the delete pipeline
     * ran for it twice - the second time over a record the first pass had already removed - and {@code onBeforeDelete}
     * fired TWICE for one deletion. A record-backed edge carries its RID, which lets a listener tell the replay apart; a
     * lightweight one does not, and a Graph Analytical View counted the second report as a second edge gone (issue
     * #10369). The incoming walk now skips the self-loops the outgoing walk deleted, so the event fires once, in both
     * shapes. Pinned as an exact count: a change that makes it two again moves this assertion.
     */
    @Test
    void aSelfLoopIsDeletedOnceSoItsDeleteEventFiresOnce() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Loop", 1);
        database.getSchema().createEdgeType("LINK", 4);
        database.getSchema().buildEdgeType().withName("LIGHT").withTotalBuckets(4).withLightweight(true).create();
      });

      assertThat(countDeleteEventsDeletingASelfLoop("LINK"))
          .as("a record-backed self-loop: deleted by the OUT walk, skipped by the IN walk").isEqualTo(1);
      assertThat(countDeleteEventsDeletingASelfLoop("LIGHT"))
          .as("a lightweight self-loop has no identity to tell a replay by, so it must not be replayed")
          .isEqualTo(1);

      assertIntegrityClean();
    }

    /**
     * A unidirectional type stores a self-loop in the vertex's OUTGOING list only, so the incoming walk has nothing to skip: the
     * outgoing walk deletes it, once, and it leaves no record behind.
     */
    @Test
    void aUnidirectionalSelfLoopIsDeletedWithItsVertex() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Loop", 1);
        database.getSchema().buildEdgeType().withName("ONEWAY").withTotalBuckets(4).withBidirectional(false).create();
      });
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Loop").save();
        v.newEdge("ONEWAY", v);
        holder[0] = v.getIdentity();
      });
      database.transaction(() -> assertThat(database.countType("ONEWAY", false)).isEqualTo(1L));

      final AtomicLong events = new AtomicLong();
      final BeforeRecordDeleteListener probe = record -> {
        if (record instanceof Edge)
          events.incrementAndGet();
        return true;
      };
      database.getSchema().getType("ONEWAY").getEvents().registerListener(probe);
      try {
        database.transaction(() -> holder[0].asVertex().delete());
      } finally {
        database.getSchema().getType("ONEWAY").getEvents().unregisterListener(probe);
      }

      assertThat(events.get()).isEqualTo(1);
      database.transaction(() -> {
        assertThat(database.existsRecord(holder[0])).isFalse();
        assertThat(database.countType("ONEWAY", false)).as("the self-loop is gone with its vertex").isZero();
      });

      assertIntegrityClean();
    }

    /**
     * Creates a vertex carrying a single self-loop of the given edge type, deletes it, and returns how many delete
     * events the edge type saw. Also asserts the delete actually happened, so a count of 0 cannot pass as a result.
     */
    private int countDeleteEventsDeletingASelfLoop(final String edgeTypeName) {
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("Loop");
        v.save();
        v.newEdge(edgeTypeName, v);
        holder[0] = v.getIdentity();
      });
      final RID vertexRID = holder[0];

      database.transaction(() -> {
        assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.OUT, edgeTypeName)).isEqualTo(1);
        assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.IN, edgeTypeName)).isEqualTo(1);
      });

      final AtomicLong events = new AtomicLong();
      final BeforeRecordDeleteListener probe = record -> {
        if (record instanceof Edge)
          events.incrementAndGet();
        return true;
      };

      database.getSchema().getType(edgeTypeName).getEvents().registerListener(probe);
      try {
        database.transaction(() -> vertexRID.asVertex().delete());
      } finally {
        database.getSchema().getType(edgeTypeName).getEvents().unregisterListener(probe);
      }

      database.transaction(() -> {
        assertThat(database.existsRecord(vertexRID)).as("the vertex must be gone").isFalse();
        assertThat(database.countType(edgeTypeName, false)).as("the self-loop must be gone with it").isEqualTo(0L);
      });

      return (int) events.get();
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

      database.transaction(() -> {
        // GraphEngine.deleteVertex removes the vertex's own record through Bucket.deleteRecord and leaves the bucket
        // delta to its caller, which on the ordinary path is LocalDatabase.deleteRecord. Calling the engine directly -
        // the only way to ask for force - skips that, so the fixture books it, or the counter it leaves behind is one
        // too high and the #8040 check reports the fixture rather than the delete under test.
        ((DatabaseInternal) database).getGraphEngine().deleteVertex((VertexInternal) hubRID.asVertex(), true);
        ((DatabaseInternal) database).getTransaction().updateBucketRecordDelta(hubRID.getBucketId(), -1);
      });

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).isFalse();
        assertThat(database.countType("LINK", false)).isEqualTo(0L);
        for (final RID source : sources)
          assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(0);
      });

      assertIntegrityClean();
    }

    /**
     * The interleaving the streaming rewrite introduces, exercised under real concurrency.
     * <p>
     * Before #5760 every far-endpoint write happened in a SECOND pass, after the walk of the hub's own list had
     * finished. Now they interleave: the delete is reading the hub's chunks while it writes the neighbours' lists.
     * The safety argument is that those are different records in different buckets, so the walk cannot be disturbed
     * - but the argument is only as good as the case that tests it, and until now nothing put another transaction
     * on the far list AT THE SAME TIME. That is what this does: while the hub is being deleted, other threads
     * append edges to the very neighbour lists the delete is removing the hub's back-references from.
     * <p>
     * Two invariants, and both have to hold for the test to mean anything. No edge may outlive the hub - that is the
     * #5670/#5680 family invariant, and a delete that lost track of its walk under the interleave would leave one.
     * And the concurrent appends that COMMITTED must still be there, because a delete that rebuilt a neighbour's
     * chunk from a stale copy would silently erase them - a lost update the version check exists to prevent, and
     * the shape #5147 was about.
     */
    @Test
    @Tag("slow")
    void deletingAVertexWhileItsNeighboursListsAreBeingAppendedTo() throws Exception {
      final int rounds = 6;
      final int neighbours = 40;
      final int appenders = 4;

      createSchema();

      // Counted across ALL rounds, not per round. Per round it is a timing assertion - a delete window short enough
      // that no append happens to COMMIT inside it proves nothing, but it also is not a defect - and asserting it
      // there would have made this test flaky on a slow runner for a reason unrelated to what it guards. One round
      // genuinely overlapping is all the non-vacuity argument needs.
      final AtomicLong appendsDuringDelete = new AtomicLong();

      final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
      GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
      try {
        for (int round = 0; round < rounds; round++) {
          final RID hubRID = createHub();
          final List<RID> sources = createIncomingEdges(hubRID, neighbours);

          database.transaction(() -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
              .as("the round must start with a full hub").isEqualTo(neighbours));

          final AtomicLong committedAppends = new AtomicLong();
          final AtomicLong deleteFailures = new AtomicLong();
          final AtomicBoolean deleting = new AtomicBoolean();
          final AtomicBoolean keepAppending = new AtomicBoolean(true);
          // Tripped once every appender has committed one edge, so the delete is guaranteed to start while they are
          // running rather than after they have all finished - without which "appends happened" and "appends raced
          // the delete" are two different claims and only the first would be tested.
          final CountDownLatch appendersWarm = new CountDownLatch(appenders);
          final CountDownLatch start = new CountDownLatch(1);
          final CountDownLatch done = new CountDownLatch(appenders + 1);

          // Appenders grow the OUT list of the SAME sources the delete is about to remove the hub's edge from, so
          // the two transactions meet on those chunk pages. They keep going until the delete has finished.
          for (int t = 0; t < appenders; t++) {
            final int worker = t;
            new Thread(() -> {
              if (!awaitStart(start)) {
                done.countDown();
                return;
              }
              boolean warm = false;
              for (int i = 0; keepAppending.get(); i++) {
                final RID source = sources.get((worker + i) % neighbours);
                try {
                  database.transaction(() -> {
                    final MutableVertex target = database.newVertex("Src");
                    target.save();
                    source.asVertex().modify().newEdge("LINK", target);
                  }, false, 10_000);
                  committedAppends.incrementAndGet();
                  if (deleting.get())
                    appendsDuringDelete.incrementAndGet();
                } catch (final Exception ignored) {
                  // A neighbour whose list the deleter is rewriting can exhaust its retries; only the delete side
                  // and the invariants below are held to a standard.
                }
                if (!warm) {
                  warm = true;
                  appendersWarm.countDown();
                }
              }
              done.countDown();
            }).start();
          }

          new Thread(() -> {
            if (!awaitStart(start)) {
              done.countDown();
              return;
            }
            try {
              // Wait for the appenders to be mid-flight before touching the hub.
              if (!appendersWarm.await(1, TimeUnit.MINUTES))
                deleteFailures.incrementAndGet();
              deleting.set(true);
              database.transaction(() -> hubRID.asVertex().delete(), false, 10_000);
            } catch (final Throwable unexpected) {
              deleteFailures.incrementAndGet();
            } finally {
              deleting.set(false);
              keepAppending.set(false);
            }
            done.countDown();
          }).start();

          start.countDown();
          assertThat(done.await(5, TimeUnit.MINUTES)).as("round " + round + ": all workers finished").isTrue();
          assertThat(deleteFailures.get()).as("round " + round + ": deletes that never completed").isEqualTo(0L);

          final int currentRound = round;
          database.transaction(() -> {
            assertThat(database.existsRecord(hubRID)).as("round " + currentRound + ": the hub must be gone").isFalse();
            long survivingToHub = 0;
            for (final RID source : sources)
              for (final Edge edge : source.asVertex().getEdges(Vertex.DIRECTION.OUT, "LINK"))
                if (hubRID.equals(edge.getIn()))
                  ++survivingToHub;
            assertThat(survivingToHub)
                .as("round " + currentRound + ": back-references to the deleted hub left in the neighbours' lists")
                .isEqualTo(0L);
          });

          database.transaction(() -> {
            long reachable = 0;
            for (final RID source : sources)
              for (final Edge edge : source.asVertex().getEdges(Vertex.DIRECTION.OUT, "LINK"))
                if (!hubRID.equals(edge.getIn()))
                  ++reachable;
            assertThat(reachable).as("round " + currentRound + ": a committed append was lost by the delete")
                .isEqualTo(committedAppends.get());
          });
        }
      } finally {
        GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
      }

      // The appends really did race the delete somewhere across the rounds - committed WHILE one was in flight, not
      // merely at some point. Without this the whole test could pass having only ever run the two operations back
      // to back, which is the ordering it exists to NOT test.
      assertThat(appendsDuringDelete.get())
          .as("appends that committed while a delete was running, across all rounds").isGreaterThan(0L);

      assertIntegrityClean();
    }

    /** Blocks until the round starts; false means the wait was interrupted and the worker must give up. */
    private static boolean awaitStart(final CountDownLatch start) {
      try {
        start.await();
        return true;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
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

  /**
   * #5764: what {@code GraphEngine.deleteVertex} tells an operator when it cannot walk an edge list, and what it
   * keeps of the failure that made it say so.
   * <p>
   * #5680 made the delete strict and #5710 added {@code CHECK DATABASE RECORD}, but they landed separately, so every
   * recovery hint the delete emitted still predated the scope it was written for: they all advised a whole-database
   * or whole-type run, costing two full passes over the vertex type plus an edge sweep, while the operator was
   * holding the one piece of information - the RID - that makes the repair cheap. And the retryable arm rethrew the
   * conflict bare, so what surfaced was {@code getEdgeHeadChunkForWrite}'s "concurrent commit in flight", which is
   * the right diagnosis for the transient case nobody ever sees and says nothing about the permanent one that
   * reaches a human.
   * <p>
   * These tests pin the advice by EXECUTING it: the command is parsed out of the message the delete raised and run
   * verbatim, and the delete must then go through. A message that named the wrong RID, or a command shape the SQL
   * parser does not accept, fails here rather than in production.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeleteVertexRepairAdvice extends TestHelper {

    /** These tests deliberately break an edge-list chain, so the blanket end-of-test check would always fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    private static final Pattern SCOPED_COMMAND = Pattern.compile("`(CHECK DATABASE RECORD [^`]+)`");

    /**
     * The case the {@code RECORD} scope exists for: the vertex's OWN list is unreadable, so the delete is REFUSED and
     * the vertex is still there to be repaired. The message must name the command aimed at that vertex, and running
     * exactly what it says must make the delete succeed and take every edge with it.
     */
    @Test
    void theRefusedDeleteNamesAScopedRepairCommandThatReallyRepairsIt() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 200);

      deleteRecord(inChunkChain(hubRID).get(0));

      final Throwable thrown = catchThrowable(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1));
      assertThat(thrown).isInstanceOf(ConcurrentModificationException.class);

      final String command = scopedRepairCommandIn(thrown.getMessage());
      assertThat(command).as("the delete must name the scoped repair: %s", thrown.getMessage())
          .isEqualTo("CHECK DATABASE RECORD " + hubRID + " FIX");
      // The original diagnosis is not replaced by the advice, only extended by it.
      assertThat(thrown.getMessage()).contains("concurrent commit in flight");

      // Not decoration: the exact string the operator was handed is what repairs the graph.
      try (final ResultSet rs = database.command("sql", command)) {
        assertThat(rs.hasNext()).isTrue();
      }

      database.transaction(() -> hubRID.asVertex().delete(), false, 1);

      database.transaction(() -> {
        assertThat(database.existsRecord(hubRID)).isFalse();
        for (final RID edge : edges)
          assertThat(database.existsRecord(edge)).as("edge " + edge + " outlived its vertex").isFalse();
      });
    }

    /**
     * The list that needs rebuilding is not always the one being deleted. When the conflict comes from disconnecting
     * a collected edge at its OTHER end, the broken list belongs to the NEIGHBOUR - so that is the RID the advice has
     * to name. Naming the vertex under delete would send the operator to repair a record that is perfectly healthy.
     */
    @Test
    void aConflictRaisedByTheNeighbourNamesTheNeighbourAndNotTheVertexBeingDeleted() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 20);
      final RID srcRID = outVertexOf(edges.get(edges.size() - 1));

      deleteRecord(inHeadChunk(hubRID));

      final Throwable thrown = catchThrowable(() -> database.transaction(() -> srcRID.asVertex().delete(), false, 1));
      assertThat(thrown).isInstanceOf(ConcurrentModificationException.class);

      final String command = scopedRepairCommandIn(thrown.getMessage());
      assertThat(command).as("%s", thrown.getMessage()).isEqualTo("CHECK DATABASE RECORD " + hubRID + " FIX");
      assertThat(command).as("the healthy vertex under delete must not be the repair target")
          .doesNotContain(srcRID.toString());
    }

    /**
     * The conflict keeps the failure that produced it. A retryable is normally absorbed and never seen, so the run
     * that does surface one is the retry-exhausted run - exactly the run whose stack trace has to be diagnosable, and
     * the one that used to arrive with the cause discarded because
     * {@code ConcurrentModificationException} had no {@code (message, cause)} constructor at all.
     */
    @Test
    void theConflictCarriesTheOriginalFailureAsItsCause() {
      createSchema();
      final RID hubRID = createHub();
      createEdges(hubRID, 200);

      deleteRecord(inChunkChain(hubRID).get(0));

      final Throwable thrown = catchThrowable(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1));
      assertThat(thrown).isInstanceOf(ConcurrentModificationException.class);
      assertThat(thrown.getCause()).as("the advice must WRAP the conflict, not replace it").isNotNull();

      final List<Throwable> chain = causeChainOf(thrown);
      assertThat(chain).as("cause chain: %s", chain)
          .anyMatch(RecordNotFoundException.class::isInstance);
    }

    /**
     * The other outcome, and the reason it gets DIFFERENT advice: under {@code force} the delete goes THROUGH, so by
     * the time the warning is logged the record a scoped check would be aimed at no longer exists. Pointing there
     * would be worse than the whole-database form it replaced - it names a repair that cannot run. The message must
     * therefore keep the whole-database command for the references left dangling, and say what the scoped form buys
     * when run BEFORE the delete.
     * <p>
     * NOTE on the capture: {@link LogManager} is a singleton, so swapping its logger is PROCESS-WIDE for the duration
     * of the forced delete below. Restored in a {@code finally}, and safe under the suite as it runs today (surefire
     * executes classes sequentially within a fork), but it is shared state: if class-level parallelism is ever
     * enabled in this module, a concurrent test's WARNING output would land in {@code warnings} here and this test's
     * delegate would be whatever that test installed. There is no per-invocation seam to capture through - the log
     * call sites go straight to the singleton - so the alternative is asserting on nothing, which is worse than a
     * documented window.
     */
    @Test
    void aForcedDeleteAdvisesTheWholeDatabaseFixBecauseTheScopedFormCannotHelpAfterwards() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 1);
      final RID srcRID = outVertexOf(edges.get(0));

      deleteRecord(outHeadChunk(srcRID));

      final List<String> warnings = new CopyOnWriteArrayList<>();
      // getLogger(), not reflection on the private field: LogManager exposes the accessor for exactly this - a
      // caller that temporarily swaps the logger and has to put the original back.
      final Logger originalLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(new CapturingLogger(warnings, originalLogger));
      try {
        database.transaction(() -> graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true));
      } finally {
        LogManager.instance().setLogger(originalLogger);
      }

      database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());

      final List<String> advice = warnings.stream().filter(w -> w.contains(srcRID.toString())).toList();
      assertThat(advice).as("the forced delete must say what it left behind (captured=%s)", warnings).isNotEmpty();
      assertThat(advice).allSatisfy(w -> {
        assertThat(w).as("%s", w).contains("CHECK DATABASE FIX");
        // The scoped form appears only as the placeholder shape describing what to do BEFORE a delete. It must never
        // be handed out with the RID substituted here: the record it would name is the one just deleted.
        assertThat(scopedRepairCommandIn(w)).as("%s", w).isEqualTo("CHECK DATABASE RECORD <vertex> FIX");
        assertThat(w).as("%s", w).doesNotContain("CHECK DATABASE RECORD " + srcRID);
      });
    }

    /** The scoped repair command inside a message, or {@code null} when it names none. */
    private static String scopedRepairCommandIn(final String message) {
      if (message == null)
        return null;
      final Matcher m = SCOPED_COMMAND.matcher(message);
      return m.find() ? m.group(1) : null;
    }

    private static List<Throwable> causeChainOf(final Throwable e) {
      final List<Throwable> chain = new ArrayList<>();
      for (Throwable current = e; current != null && !chain.contains(current); current = current.getCause())
        chain.add(current);
      return chain;
    }

    private GraphEngine graphEngine() {
      return ((DatabaseInternal) database).getGraphEngine();
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 16);
        database.getSchema().createEdgeType("LINK", 16);
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

    /** One edge per transaction, so the hub's IN chain grows chunk by chunk exactly as it does in production. */
    private List<RID> createEdges(final RID hubRID, final int count) {
      final List<RID> edges = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final RID[] holder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          holder[0] = src.newEdge("LINK", hubRID).getIdentity();
        });
        edges.add(holder[0]);
      }
      return edges;
    }

    private RID outVertexOf(final RID edgeRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = edgeRID.asEdge().getOut());
      return holder[0];
    }

    private RID outHeadChunk(final RID vertexRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getOutEdgesHeadChunk());
      assertThat(holder[0]).as("the vertex must have an outgoing edge list").isNotNull();
      return holder[0];
    }

    private RID inHeadChunk(final RID vertexRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getInEdgesHeadChunk());
      assertThat(holder[0]).as("the vertex must have an incoming edge list").isNotNull();
      return holder[0];
    }

    /** The hub's IN chunk chain, head first (newest chunk) to tail (the chunk created with the first edge). */
    private List<RID> inChunkChain(final RID hubRID) {
      final List<RID> chain = new ArrayList<>();
      database.transaction(() -> {
        RID rid = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk();
        while (rid != null) {
          chain.add(rid);
          rid = ((EdgeSegment) database.lookupByRID(rid, true)).getPreviousRID();
        }
      });
      return chain;
    }

    private void deleteRecord(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
      database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
    }

    /**
     * Captures WARNING-and-above messages into a list while forwarding every record to the production logger, so the
     * test output still shows what fired.
     */
    private static final class CapturingLogger implements Logger {
      private final List<String> warnings;
      private final Logger       delegate;

      CapturingLogger(final List<String> warnings, final Logger delegate) {
        this.warnings = warnings;
        this.delegate = delegate;
      }

      private void capture(final Level level, final String message, final Object... args) {
        if (message == null || level.intValue() < Level.WARNING.intValue())
          return;
        String formatted = message;
        if (args != null && args.length > 0) {
          try {
            formatted = message.formatted(args);
          } catch (final Exception ignored) {
            // Fall back to the raw template - good enough for the substring matching above.
          }
        }
        warnings.add(formatted);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
            arg15, arg16, arg17);
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
            arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        capture(level, message, args);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public void flush() {
        delegate.flush();
      }
    }
  }

  /**
   * #5777: what {@code CHECK DATABASE} does with an edge endpoint that IS NOT THERE.
   * <p>
   * Two defects, one cause. {@code GraphDatabaseChecker} carried FOUR copies of the same fifteen-line endpoint
   * probe - both sides of {@code checkEdges}, plus the far-endpoint load in {@code checkIncomingEdges} and
   * {@code checkOutgoingEdges} - and one of them had drifted: {@code checkEdges}' incoming side flagged the MISSING
   * VERTEX corrupted alongside the edge, where its outgoing twin flagged only the edge. So the same damage reported
   * differently depending on which end of the edge had lost its vertex.
   * <p>
   * The drift also picked the wrong side. A RID that raised {@code RecordNotFoundException} is not corruption, it is
   * absence, and the distinction is expensive rather than cosmetic: every RID in {@code corruptedRecords} puts its
   * BUCKET into {@code affectedBuckets}, and {@code FIX} drops and rebuilds every index on it. Flagging the absent
   * vertex therefore had a dangling-edge sweep after a bulk delete rebuild the indexes of every vertex bucket the
   * deleted vertices lived in - and bought nothing, because the repair loop's delete of a RID that is not there
   * raises {@code RecordNotFoundException} and is ignored. That is the same rule #5680/#5764 settled for a
   * hand-typed RID in the {@code RECORD} scope.
   * <p>
   * These tests pin the resulting contract from both ends: an ABSENT endpoint is reported and never flagged, an
   * UNREADABLE one still is, and the two directions answer identically.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DanglingEndpointScope extends TestHelper {
    /** These tests deliberately delete records underneath live edges, so the blanket end-of-test check would fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * THE ASYMMETRY. An edge whose IN vertex is gone and an edge whose OUT vertex is gone are the same finding, so
     * the pass must flag the same thing for both: the edge, and only the edge.
     * <p>
     * Before the fix the incoming side additionally flagged {@code edge.getIn()}, so this run reported three
     * corrupted records for two dangling edges - and which three depended on which end had been deleted.
     */
    @Test
    void bothDirectionsFlagTheEdgeAndNothingElse() {
      final Graph graph = createGraph();

      // One edge loses its IN endpoint (the company), the other its OUT endpoint (the person).
      rawDelete(graph.company1);
      rawDelete(graph.person2);

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
          .checkEdges("WorksAt", false, 0, 100, 100);

      assertThat((Collection<RID>) stats.get("corruptedRecords"))
          .as("only the two dangling EDGES are corrupt, neither absent vertex: %s", stats)
          .containsExactlyInAnyOrder(graph.edge1, graph.edge2);
      assertThat((Long) stats.get("totalCorruptedRecords"))
          .as("and the total must not count the phantoms either: %s", stats).isEqualTo(2L);
      // Both sides still count the dangling link, which is the number that DOES describe this damage.
      assertThat((Long) stats.get("invalidLinks")).as("%s", stats).isEqualTo(2L);
    }

    /**
     * The finding is not lost by not being flagged: {@code trackMissingReference} is the channel built for it, and it
     * names both absent endpoints with the count of edges that referenced them.
     */
    @Test
    void theAbsentEndpointsAreStillReportedToTheOperator() {
      final Graph graph = createGraph();

      rawDelete(graph.company1);
      rawDelete(graph.person2);

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
          .checkEdges("WorksAt", false, 0, 100, 100);

      assertThat((Map<RID, Long>) stats.get("missingReferences"))
          .as("each absent vertex is named once, with its reference count: %s", stats)
          .containsOnly(entryOf(graph.company1), entryOf(graph.person2));

      final Collection<String> warnings = (Collection<String>) stats.get("warnings");
      assertThat(warnings).as("%s", stats).anyMatch(w -> w.equals(
          "edge " + graph.edge1 + " points to the incoming vertex " + graph.company1 + " that is not found (deleted?)"));
      assertThat(warnings).as("%s", stats).anyMatch(w -> w.equals(
          "edge " + graph.edge2 + " points to the outgoing vertex " + graph.person2 + " that is not found (deleted?)"));
    }

    /**
     * THE COST, measured where an operator pays it. {@code rebuiltIndexes} is the set {@code FIX} drops and rebuilds,
     * derived from the buckets of the corrupted records, and a vertex that is not there must not put its bucket in
     * it - a full scan per index on that bucket, bought with a phantom.
     * <p>
     * Run through the full {@code CHECK DATABASE} rather than the graph checker alone, because the derivation from
     * {@code corruptedRecords} to {@code affectedBuckets} to {@code rebuiltIndexes} lives in {@code DatabaseChecker}
     * and is the whole point of the distinction.
     */
    @Test
    void anAbsentEndpointDoesNotDragItsBucketIntoTheIndexRebuild() {
      final Graph graph = createGraph();

      rawDelete(graph.person2);

      // The BUCKET-level names, which is what rebuiltIndexes reports - not the "Person[name]" TypeIndex wrapper the
      // schema is asked for by name. Asserting the wrapper's name would never have matched and would have passed
      // whatever the checker did.
      final Set<String> personIndexes = indexNamesOnBucketOf(graph.person2);
      assertThat(personIndexes).as("precondition: the Person bucket must carry an index there is a cost to rebuild")
          .isNotEmpty();

      final Result row = check("CHECK DATABASE");

      assertThat((Collection<String>) row.getProperty("rebuiltIndexes"))
          .as("no index on the Person bucket may be rebuilt over a Person that is not there: %s", row.toJSON())
          .doesNotContainAnyElementsOf(personIndexes);
      assertThat((Collection<RID>) row.getProperty("corruptedRecords"))
          .as("the dangling edge is the finding, the absent vertex is not: %s", row.toJSON())
          .contains(graph.edge2).doesNotContain(graph.person2);
    }

    /**
     * The other half of the contract: an endpoint whose record IS there and cannot be READ is corruption, and stays
     * flagged - together with the edge. Rebuilding the indexes on ITS bucket is exactly what a corrupt record in that
     * bucket calls for, so the expensive path is still taken where it is earned.
     */
    @Test
    void anUnreadableEndpointIsStillFlaggedCorrupted() {
      final Graph graph = createGraph();

      // Present in its slot, decodable by nobody: the endpoint load fails with something that is NOT
      // RecordNotFoundException, which is the arm that must keep flagging the vertex.
      //
      // TRUNCATION rather than a bogus record-type byte, which does NOT work here: an endpoint is resolved by
      // getOutVertex() as lookupByRID(rid, false), which types the record from its BUCKET, and the deferred
      // asVertex(true) then reads the buffer without ever re-reading the type byte. A record shorter than the fixed
      // vertex prefix is the shape that fails in that deferred read.
      shrinkRecordBuffer(graph.person2);
      // The endpoint load resolves through lookupByRID, which answers from the record cache, and person2 is in it
      // from the fixture. Reopening is what makes the check read the bytes this test just wrote.
      reopenDatabase();

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
          .checkEdges("WorksAt", false, 0, 100, 100);

      assertThat((Collection<RID>) stats.get("corruptedRecords"))
          .as("a record that is there but unreadable is corrupt, and so is the edge that points at it: %s", stats)
          .contains(graph.edge2, graph.person2);
      assertThat((Collection<String>) stats.get("warnings")).as("%s", stats).anyMatch(w -> w.startsWith(
          "edge " + graph.edge2 + " points to the outgoing vertex " + graph.person2 + " which cannot be loaded (error:"));
    }

    /**
     * The same rule in the OTHER family of caller. {@code checkVertices} walks each vertex's adjacency lists and loads
     * the FAR endpoint of every entry; those two copies flagged the absent far vertex too. The dangling entry is still
     * pruned - that repair is unaffected - but the RID that is not there is no longer called corrupt.
     */
    @Test
    void theAdjacencyWalkAlsoStopsFlaggingAnAbsentFarEndpoint() {
      final Graph graph = createGraph();

      // company1 is edge1's IN endpoint, so walking person1's OUT list loads a vertex that is gone.
      rawDelete(graph.company1);

      final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
          .checkVertices("Person", null, false, 0, 100, 100);

      assertThat((Collection<RID>) stats.get("corruptedRecords"))
          .as("the walk flags the edge it cannot follow, not the vertex that is not there: %s", stats)
          .contains(graph.edge1).doesNotContain(graph.company1);
      assertThat((Map<RID, Long>) stats.get("missingReferences"))
          .as("still reported, through the channel meant for it: %s", stats).containsKey(graph.company1);
    }

    /**
     * FIX still repairs what there is to repair, and reports one repair per record it actually removed. The absent
     * vertex contributes nothing to {@code autoFix} - it never did, because the count happens after the delete
     * returns (#6128) and a delete of a RID that is not there raises - which is precisely why flagging it was pure
     * cost.
     */
    @Test
    void fixStillRemovesTheDanglingEdgeAndCountsItOnce() {
      final Graph graph = createGraph();

      rawDelete(graph.person2);

      final Result row = check("CHECK DATABASE TYPE WorksAt FIX");

      assertThat(longProperty(row, "autoFix")).as("one record removed: %s", row.toJSON()).isEqualTo(1L);
      assertThat((Collection<RID>) row.getProperty("deletedRecordsAfterFix"))
          .as("%s", row.toJSON()).containsExactly(graph.edge2);
      database.transaction(() -> assertThat(database.existsRecord(graph.edge2))
          .as("the dangling edge must be gone").isFalse());
    }

    /** The names {@code rebuiltIndexes} would use for the indexes attached to {@code rid}'s bucket. */
    private Set<String> indexNamesOnBucketOf(final RID rid) {
      return Arrays.stream(database.getSchema().getIndexes())
          .filter(index -> index.getAssociatedBucketId() == rid.getBucketId())
          .map(Index::getName).collect(Collectors.toSet());
    }

    /** The fixture's RIDs, named so an assertion says which vertex or edge it means rather than which array slot. */
    private record Graph(RID person1, RID person2, RID company1, RID company2, RID edge1, RID edge2) {
    }

    /**
     * Two disjoint person-to-company edges, so one can lose its IN endpoint and the other its OUT endpoint in the
     * same run without the two findings overlapping. The unique index on {@code Person.name} is what makes
     * {@code rebuiltIndexes} observable.
     */
    private Graph createGraph() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Person", 1).createProperty("name", Type.STRING)
            .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
        database.getSchema().createVertexType("Company", 1);
        database.getSchema().createEdgeType("WorksAt", 1);
      });

      final RID[] rids = new RID[6];
      database.transaction(() -> {
        final MutableVertex person1 = database.newVertex("Person").set("name", "p1").save();
        final MutableVertex person2 = database.newVertex("Person").set("name", "p2").save();
        final MutableVertex company1 = database.newVertex("Company").set("name", "c1").save();
        final MutableVertex company2 = database.newVertex("Company").set("name", "c2").save();

        rids[0] = person1.getIdentity();
        rids[1] = person2.getIdentity();
        rids[2] = company1.getIdentity();
        rids[3] = company2.getIdentity();
        rids[4] = person1.newEdge("WorksAt", company1).getIdentity();
        rids[5] = person2.newEdge("WorksAt", company2).getIdentity();
      });

      return new Graph(rids[0], rids[1], rids[2], rids[3], rids[4], rids[5]);
    }

    /**
     * Removes the record through the BUCKET, leaving every reference to it - the adjacency entries and the index
     * entries - behind. That is the shape a check meets after a crash or a raw repair, and the only way to produce a
     * RID that resolves to nothing while an edge still names it.
     */
    private void rawDelete(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
    }

    /**
     * Replaces the record-size varint with a single-byte varint encoding a size far below the fixed vertex prefix:
     * the record still occupies its slot and still resolves, but cannot be DECODED. zigzag(8) == 16.
     */
    private void shrinkRecordBuffer(final RID rid) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = rid.getBucketId();
      final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(fileId);
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      final int maxRecordsInPage = bucket.getMaxRecordsInPage();

      final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
      final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, pageId), pageSize, false);
          final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
          final int recordOffset = (int) page.readUnsignedInt(slotOffset);
          assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
          page.writeByte(recordOffset, (byte) 16);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    /** The {@code missingReferences} entry for a vertex exactly one edge pointed at. */
    private static Map.Entry<RID, Long> entryOf(final RID rid) {
      return Map.entry(rid, 1L);
    }

    /** Runs a CHECK DATABASE statement and returns its single result row. */
    private Result check(final String command) {
      try (final ResultSet rs = database.command("sql", command)) {
        assertThat(rs.hasNext()).isTrue();
        return rs.next();
      }
    }

    /** Reads a numeric check-database property, failing loudly when the field does not exist. */
    private static long longProperty(final Result row, final String name) {
      final Object value = row.getProperty(name);
      assertThat(value).as("check database must report '%s': %s", name, row.toJSON()).isNotNull();
      return ((Number) value).longValue();
    }
  }

  /**
   * #5779: {@code GraphEngine.moveEdge} re-points an edge by deleting the old edge record and creating a replacement,
   * and it reaches the removal through the public {@code GraphEngine.deleteEdge(Edge)}. That method performs the
   * PHYSICAL removal only - it is normally entered from {@code LocalDatabase.deleteRecordNoLock}, which has already
   * cleaned the record's index entries and its EXTERNAL property values by the time the edge arrives. {@code moveEdge}
   * is the one caller that does not come that way, so the old record's index entries survived it.
   * <p>
   * It used to look harmless because the bucket normally hands the just-freed slot straight back to the replacement,
   * leaving the stale entry pointing at a new record carrying the same key - indistinguishable from a correct entry.
   * That is an allocation coincidence, not an invariant. The fixtures below break it the way production would: the
   * edge type has SEVERAL buckets and the default round-robin selection puts the replacement in a DIFFERENT bucket
   * from the record just deleted, so the stale entry names a RID that no longer resolves.
   * <p>
   * The assertions go through {@code Index.countEntries()} and a raw {@code get()} on the index rather than through a
   * query: {@code SELECT} skips entries whose RID does not resolve, which is exactly what hides this leak.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MoveEdgeIndexCleanup extends TestHelper {

    private static final int    BUCKETS         = 4;
    /** Long enough to be found verbatim in the page, and distinctive enough not to collide with anything else. */
    private static final String CORRUPT_MARKER  = "CORRUPT5779MARKERVALUE";

    /**
     * The leak itself, on a NOTUNIQUE index: after the move the index must hold exactly one entry for the edge's key,
     * and that entry must name the surviving record.
     */
    @Test
    void movingAnEdgeCleansTheOldRecordsIndexEntries() {
      createSchema(false);

      final RID[] rids = createTriangleWithOneEdge();
      final RID edgeRID = rids[0], farRID = rids[2];

      final Index index = database.getSchema().getIndexByName("Link[code]");
      assertThat(index.countEntries()).as("one edge, one index entry").isEqualTo(1L);

      final RID movedRID = moveEdgeIn(edgeRID, farRID);

      assertThat(movedRID).as("the replacement must land in a different bucket, otherwise the fixture proves nothing")
          .isNotEqualTo(edgeRID);

      assertThat(index.countEntries())
          .as("the entry of the deleted record must be gone: one edge, one entry").isEqualTo(1L);

      final List<RID> found = lookup(index, "E1");
      assertThat(found).as("the single surviving entry must name the replacement record").containsExactly(movedRID);

      database.transaction(() -> {
        assertThat(database.existsRecord(edgeRID)).as("the old record is deleted").isFalse();
        assertThat(database.countType("Link", false)).as("a move is not a duplication").isEqualTo(1L);
      });

      assertIntegrityClean();
    }

    /**
     * The sharper consequence of the same leak. The stale entry named a RID in a bucket whose slot is free, so an
     * UNRELATED edge created afterwards can be handed that exact slot - and then a lookup of the OLD key silently
     * returns the unrelated record. This is the failure mode that is worse than a plain dangling entry, because
     * nothing about the result looks wrong.
     */
    @Test
    void aFreedSlotReusedByAnUnrelatedEdgeIsNotReturnedForTheOldKey() {
      createSchema(false);

      final RID[] rids = createTriangleWithOneEdge();
      final RID edgeRID = rids[0], nearRID = rids[1], farRID = rids[2];

      moveEdgeIn(edgeRID, farRID);

      // Fill every bucket of the type so the freed slot of the deleted record is certainly handed out again.
      database.transaction(() -> {
        final Vertex near = nearRID.asVertex();
        final Vertex far = farRID.asVertex();
        for (int i = 0; i < BUCKETS * 3; i++)
          near.newEdge("Link", far, "code", "OTHER" + i).save();
      });

      final Index index = database.getSchema().getIndexByName("Link[code]");
      final List<RID> found = lookup(index, "E1");

      assertThat(found).as("the moved edge's key must resolve to exactly one record").hasSize(1);
      database.transaction(() -> assertThat(found.getFirst().asEdge().getString("code"))
          .as("a stale entry pointing at a reused slot returns a record carrying somebody else's key").isEqualTo("E1"));

      assertIntegrityClean();
    }

    /**
     * A UNIQUE index turns the leak from a silent wrong answer into a hard failure: the stale entry keeps the old key
     * taken, so a later legitimate insert of that key is rejected as a duplicate even though the record that owned it
     * is gone.
     */
    @Test
    void aUniqueIndexAcceptsTheKeyOfAMovedEdgeAfterItIsFreed() {
      createSchema(true);

      final RID[] rids = createTriangleWithOneEdge();
      final RID edgeRID = rids[0], nearRID = rids[1], farRID = rids[2];

      moveEdgeIn(edgeRID, farRID);

      final Index index = database.getSchema().getIndexByName("Link[code]");
      assertThat(index.countEntries()).as("a unique index must not hold two entries for one key").isEqualTo(1L);

      // Deleting the moved edge frees the key for good; re-inserting it must be accepted.
      database.transaction(() -> lookup(index, "E1").getFirst().asEdge().delete());
      database.transaction(() -> nearRID.asVertex().newEdge("Link", farRID.asVertex(), "code", "E1").save());

      assertThat(index.countEntries()).isEqualTo(1L);
      assertIntegrityClean();
    }

    /**
     * The OUT side takes the same route with {@code Vertex.DIRECTION.OUT}, and nothing about the cleanup is
     * direction-specific - which is exactly why it is asserted rather than assumed.
     */
    @Test
    void movingTheOutEndpointCleansTheOldRecordsIndexEntriesToo() {
      createSchema(false);

      final RID[] rids = createTriangleWithOneEdge();
      final RID edgeRID = rids[0], farRID = rids[2];

      final RID[] moved = new RID[1];
      database.transaction(() -> {
        final MutableEdge edge = edgeRID.asEdge().modify();
        edge.set("@out", farRID);
        moved[0] = edge.getIdentity();
      });

      final Index index = database.getSchema().getIndexByName("Link[code]");
      assertThat(moved[0]).isNotEqualTo(edgeRID);
      assertThat(index.countEntries()).isEqualTo(1L);
      assertThat(lookup(index, "E1")).containsExactly(moved[0]);
      database.transaction(() -> assertThat(moved[0].asEdge().getOut()).isEqualTo(farRID));

      assertIntegrityClean();
    }

    /**
     * A move that ALSO changes an indexed property in the same session. {@code DocumentIndexer.deleteDocument} builds
     * the key it removes from the LIVE property values of the record handed to it, and after
     * {@code edge.set("code", ...)} those are the new ones - the index still holds the old key. The cleanup therefore
     * has to run against the state the index was built from, not against the caller's in-flight edits, or it removes
     * a key that was never there and leaves the real entry dangling: the very leak this fix exists to close, reached
     * through a different door.
     */
    @Test
    void movingAnEdgeThatAlsoChangedItsIndexedPropertyCleansTheKeyTheIndexActuallyHolds() {
      createSchema(false);

      final RID[] rids = createTriangleWithOneEdge();
      final RID edgeRID = rids[0], farRID = rids[2];

      final RID[] moved = new RID[1];
      database.transaction(() -> {
        final MutableEdge edge = edgeRID.asEdge().modify();
        edge.set("code", "E2");
        edge.set("@in", farRID);
        moved[0] = edge.getIdentity();
      });

      final Index index = database.getSchema().getIndexByName("Link[code]");
      assertThat(lookup(index, "E1")).as("the key the record used to carry must be gone").isEmpty();
      assertThat(lookup(index, "E2")).as("the key it carries now must name the surviving record")
          .containsExactly(moved[0]);
      assertThat(index.countEntries()).as("one edge, one entry").isEqualTo(1L);

      assertIntegrityClean();
    }

    /**
     * The same move, but with the indexed property already SAVED before it. The record's buffer stays frozen until
     * commit (serialization is deferred), so it still describes {@code E1} while the index has already been moved to
     * {@code E2} by that save - the committed buffer is the wrong pre-image here, and the transaction's indexed
     * snapshot (#4935) is the right one.
     */
    @Test
    void movingAnEdgeAfterAnIndexedPropertyWasAlreadySavedInTheSameTransaction() {
      createSchema(false);

      final RID[] rids = createTriangleWithOneEdge();
      final RID edgeRID = rids[0], farRID = rids[2];

      final RID[] moved = new RID[1];
      database.transaction(() -> {
        final MutableEdge edge = edgeRID.asEdge().modify();
        edge.set("code", "E2");
        edge.save();
        edge.set("@in", farRID);
        moved[0] = edge.getIdentity();
      });

      final Index index = database.getSchema().getIndexByName("Link[code]");
      assertThat(lookup(index, "E1")).as("the committed key was already replaced by the save").isEmpty();
      assertThat(lookup(index, "E2")).as("the key the index actually held must name the surviving record")
          .containsExactly(moved[0]);
      assertThat(index.countEntries()).as("one edge, one entry").isEqualTo(1L);

      assertIntegrityClean();
    }

    /**
     * A move of an edge CREATED in the same transaction, with its indexed property changed in between - the sequence
     * that looks like it should reach {@code indexedImageOf}'s buffer-less fallback and does not: {@code newEdge()}
     * saves the edge, the save serializes, so it arrives with a committed buffer like any other record and the
     * ordinary pre-image path handles it. Worth pinning precisely because the intuition points the other way.
     */
    @Test
    void movingAnEdgeCreatedInTheSameTransactionUsesItsCommittedPreImage() {
      createSchema(false);

      final RID[] moved = new RID[1];
      database.transaction(() -> {
        final MutableVertex near = database.newVertex("Node").set("name", "near").save();
        final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
        final MutableVertex far = database.newVertex("Node").set("name", "far").save();
        final MutableEdge edge = near.newEdge("Link", far0, "code", "E1");
        edge.set("code", "E2");
        edge.set("@in", far.getIdentity());
        moved[0] = edge.getIdentity();
      });

      final Index index = database.getSchema().getIndexByName("Link[code]");
      assertThat(index.countEntries()).as("one edge, one entry - no entry left over from the create").isEqualTo(1L);
      assertThat(lookup(index, "E2")).containsExactly(moved[0]);
      assertThat(lookup(index, "E1")).isEmpty();
      assertThat(scalar("select count() as n from Link")).isEqualTo(1L);
      assertThat(database.countType("Link", false)).isEqualTo(1L);

      assertIntegrityClean();
    }

    /**
     * The BOUNDARY of the guarantee, measured rather than assumed, because it is the one case where
     * {@code cleanUpBeforePhysicalDelete} cannot deliver: an indexed property whose value cannot be deserialized.
     * <p>
     * This fixture uses the #4420 shape - a corrupt length prefix on an inline STRING. The reader TOLERATES it by
     * reporting the value as ABSENT rather than throwing (see {@code EdgeDeleteRepairRegressionTest.TolerantDelete}), so nothing here
     * can learn the key the index actually holds, and the entry for the deleted record survives. That is not a
     * regression this fix introduces and not one it can close: {@code LocalDatabase.deleteRecordNoLock} degrades
     * identically on the same record, by design and with a logged warning. CHECK DATABASE does not detect a dangling
     * LSM entry either ({@code totalErrors} 0, {@code corruptedIndexes} empty), so this test is what records the
     * boundary instead.
     * <p>
     * What the move must still get right, and does: the record is removed, the counter stays truthful, and no key is
     * fabricated for the replacement.
     */
    @Test
    void movingAnEdgeWithAnUnreadablePropertyIsBestEffortLikeTheDeletePath() {
      createSchema(false);

      final RID[] rids = createTriangleWithOneEdge(CORRUPT_MARKER);
      final RID edgeRID = rids[0], farRID = rids[2];

      corruptPropertyLength(edgeRID);
      // Reopen so the record is re-read from the corrupted page instead of the in-memory record cache. Every RID
      // captured before this point still names the closed instance, so rebind them to the reopened one.
      reopenDatabase();
      final RID staleEdgeRID = RID.create(database, edgeRID.getBucketId(), edgeRID.getPosition());
      final RID newInRID = RID.create(database, farRID.getBucketId(), farRID.getPosition());

      final Index index = database.getSchema().getIndexByName("Link[code]");
      final RID movedRID = moveEdgeIn(staleEdgeRID, newInRID);

      database.transaction(
          () -> assertThat(database.existsRecord(staleEdgeRID)).as("the old record is still removed").isFalse());
      assertThat(scalar("select count() as n from Link")).as("a move is still not a duplication").isEqualTo(1L);
      assertThat(database.countType("Link", false))
          .as("and the counter stays truthful even on the degraded path").isEqualTo(1L);
      assertThat(movedRID).isNotEqualTo(staleEdgeRID);

      database.transaction(() -> assertThat((Object) movedRID.asEdge().get("code"))
          .as("the unreadable value cannot be carried over, and must not be invented either").isNull());

      // The boundary itself: the entry keyed by the unreadable value outlives the record. Asserted so that a future
      // change able to recover the key (or a decision to fail the move instead) shows up here rather than silently.
      assertThat(lookup(index, CORRUPT_MARKER))
          .as("unreadable key: the stale entry survives, exactly as it does on the ordinary delete path")
          .containsExactly(staleEdgeRID);
    }

    /**
     * {@code cleanUpBeforePhysicalDelete} returns early for a LIGHTWEIGHT edge, because {@code deleteEdge} performs
     * no physical removal for one: there is no index entry, no external value and no counted record, and folding a
     * {@code -1} would drift the cached bucket count the other way and persist that drift to
     * {@code statistics.json}. That branch is currently UNREACHABLE, and this test is what says so out loud - both
     * doors into {@code moveEdge} are shut for a lightweight edge, and if either is ever opened this fails and
     * points at the guard that is then load-bearing rather than defensive.
     */
    @Test
    void aLightweightEdgeCannotReachMoveEdgeAtAll() {
      database.getSchema().createVertexType("Node", BUCKETS).createProperty("name", Type.STRING);
      database.getSchema().buildEdgeType().withName("Link").withTotalBuckets(BUCKETS).withLightweight(true).create();

      final RID[] rids = new RID[2];
      database.transaction(() -> {
        final MutableVertex near = database.newVertex("Node").set("name", "near").save();
        final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
        near.newEdge("Link", far0);
        rids[0] = near.getIdentity();
        rids[1] = far0.getIdentity();
      });

      assertThat(database.countType("Link", false))
          .as("a lightweight edge allocates no record, so there is nothing for the cleanup to account for")
          .isEqualTo(0L);

      // Door 1: a persisted lightweight edge reached through its endpoint's list cannot be made mutable at all.
      database.transaction(() -> assertThatThrownBy(
          () -> rids[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "Link").iterator().next().asEdge().modify())
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("cannot be modified"));

      // Door 2: the MutableLightEdge newEdge() hands back inside the creating session refuses set() outright, so
      // the "@in"/"@out" override in MutableEdge - the only caller of moveEdge - is never entered.
      database.transaction(() -> assertThatThrownBy(
          () -> rids[0].asVertex().modify().newEdge("Link", rids[1].asVertex()).set("@in", rids[0]))
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("LIGHTWEIGHT"));

      assertIntegrityClean();
    }

    /**
     * The same bypass, on the bucket record counter. {@code deleteRecordNoLock} folds a {@code -1} for the record it
     * removes and the create path folds a {@code +1}, so a move is net zero. Reaching the removal through
     * {@code deleteEdge} skipped the {@code -1} only, and the counter is what {@code count(*)} reads - and it is
     * persisted to {@code statistics.json}, so the drift survives a reopen. Asserted against a full scan
     * ({@code count()}), which is ground truth.
     */
    @Test
    void movingAnEdgeLeavesTheBucketRecordCounterAtTheNumberOfEdgesThatExist() {
      createSchema(false);

      final RID[] rids = createTriangleWithOneEdge();
      final RID edgeRID = rids[0], farRID = rids[2];

      moveEdgeIn(edgeRID, farRID);

      assertThat(scalar("select count() as n from Link")).as("ground truth: one edge survives the move").isEqualTo(1L);
      assertThat(scalar("select count(*) as n from Link"))
          .as("count(*) reads the cached bucket counter: a move must not inflate it").isEqualTo(1L);
      assertThat(database.countType("Link", false)).isEqualTo(1L);
    }

    /**
     * And on EXTERNAL property values. {@code moveEdge} writes FRESH external records for the replacement, so the
     * ones owned by the record it deletes have to go with it or they are orphaned in the paired bucket -
     * {@code deleteRecordNoLock} cascade-deletes them, {@code deleteEdge} does not. {@code CHECK DATABASE} counts
     * exactly this leak.
     */
    @Test
    void movingAnEdgeCascadeDeletesTheOldRecordsExternalValues() {
      final VertexType node = database.getSchema().createVertexType("Node", BUCKETS);
      node.createProperty("name", Type.STRING);
      database.getSchema().createEdgeType("Link", BUCKETS).createProperty("blob", Type.STRING).setExternal(true);

      final RID[] rids = new RID[3];
      database.transaction(() -> {
        final MutableVertex near = database.newVertex("Node").set("name", "near").save();
        final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
        final MutableVertex far = database.newVertex("Node").set("name", "far").save();
        rids[0] = near.newEdge("Link", far0, "blob", "x".repeat(4096)).save().getIdentity();
        rids[1] = near.getIdentity();
        rids[2] = far.getIdentity();
      });

      final RID movedRID = moveEdgeIn(rids[0], rids[2]);

      database.transaction(() -> assertThat(movedRID.asEdge().getString("blob"))
          .as("the replacement must still carry the external value").isEqualTo("x".repeat(4096)));

      assertOrphanedExternalRecords(0L);
      assertIntegrityClean();
    }

    // ---------------------------------------------------------------------------------------------------------------

    private void createSchema(final boolean unique) {
      final VertexType node = database.getSchema().createVertexType("Node", BUCKETS);
      node.createProperty("name", Type.STRING);
      database.getSchema().createEdgeType("Link", BUCKETS).createProperty("code", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, unique, "Link", "code");
    }

    /**
     * Three vertices and a single {@code near -> far0} edge carrying the indexed key {@code E1}. Returns
     * {@code [edge, near, far]}.
     */
    private RID[] createTriangleWithOneEdge() {
      return createTriangleWithOneEdge("E1");
    }

    private RID[] createTriangleWithOneEdge(final String code) {
      final RID[] rids = new RID[3];
      database.transaction(() -> {
        final MutableVertex near = database.newVertex("Node").set("name", "near").save();
        final MutableVertex far0 = database.newVertex("Node").set("name", "far0").save();
        final MutableVertex far = database.newVertex("Node").set("name", "far").save();
        rids[0] = near.newEdge("Link", far0, "code", code).save().getIdentity();
        rids[1] = near.getIdentity();
        rids[2] = far.getIdentity();
      });
      return rids;
    }

    /**
     * The #4420 corruption shape, applied to the edge's indexed STRING property: the length-prefix varint of the
     * inline value is overwritten with one that decodes above {@link Integer#MAX_VALUE}. The record's declared size
     * in the page record table is left alone, so the record still loads - only deserializing that property fails.
     * Lifted from {@code EdgeDeleteRepairRegressionTest.TolerantDelete}, which pins the same shape on the delete path.
     */
    private void corruptPropertyLength(final RID rid) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = rid.getBucketId();
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      final byte[] marker = CORRUPT_MARKER.getBytes(StandardCharsets.UTF_8);

      final Binary varintBuffer = new Binary();
      // 4_294_967_245L == (2^32 - 51); cast to int it becomes -51, the exact value reported in issue #4420.
      varintBuffer.putUnsignedNumber(4_294_967_245L);
      final byte[] corruptVarint = varintBuffer.toByteArray();

      db.transaction(() -> {
        try {
          // Page 0: the fixture puts a single edge in this bucket, so its slot is on the first page.
          final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
          final byte[] content = new byte[page.getContentSize()];
          page.readByteArray(0, content);

          final int markerStart = indexOf(content, marker);
          assertThat(markerStart).as("marker value must be present inline in the page").isGreaterThan(0);

          // The byte immediately before the inline value is its length varint; overwrite it (and the bytes it now
          // spans) with the multi-byte corrupt length. The marker is long enough that this stays inside the record.
          for (int i = 0; i < corruptVarint.length; i++)
            page.writeByte(markerStart - 1 + i, corruptVarint[i]);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    private static int indexOf(final byte[] haystack, final byte[] needle) {
      outer:
      for (int i = 0; i <= haystack.length - needle.length; i++) {
        for (int j = 0; j < needle.length; j++)
          if (haystack[i + j] != needle[j])
            continue outer;
        return i;
      }
      return -1;
    }

    /**
     * Re-points the IN endpoint of {@code edgeRID} at {@code newInRID} through the embedded API and returns the RID of
     * the replacement record.
     */
    private RID moveEdgeIn(final RID edgeRID, final RID newInRID) {
      final RID[] moved = new RID[1];
      database.transaction(() -> {
        final MutableEdge edge = edgeRID.asEdge().modify();
        // set("@in") IS the move: GraphEngine.moveEdge saves the replacement itself and re-points this instance at it,
        // so there is nothing left to save afterwards.
        edge.set("@in", newInRID);
        moved[0] = edge.getIdentity();
      });
      return moved[0];
    }

    private List<RID> lookup(final Index index, final String key) {
      final List<RID> result = new ArrayList<>();
      database.transaction(() -> {
        final IndexCursor cursor = index.get(new Object[] { key });
        while (cursor.hasNext()) {
          final var next = cursor.next();
          if (next != null)
            result.add(next.getIdentity());
        }
      });
      return result;
    }

    private long scalar(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private void assertOrphanedExternalRecords(final long expected) {
      try (final ResultSet rs = database.command("sql", "check database")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(longProperty(row, "orphanedExternalRecords"))
              .as("check database orphanedExternalRecords: " + row.toJSON()).isEqualTo(expected);
        }
      }
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

  /**
   * #6572: meeting a DANGLING VERTEX REFERENCE must be reported as what it is.
   * <p>
   * An adjacency entry whose target record was removed without the entry being disconnected - the documented legacy
   * of a pre-#5670 best-effort edge delete - hands a caller a vertex handle to a record that is not there. Deleting
   * through it reached {@code GraphEngine.getEdgeHeadChunkForWrite}, whose lazy load of the vertex raised
   * {@code RecordNotFoundException}, and that was converted wholesale into the retryable conflict the method exists
   * to produce for an unreadable CHUNK. Two things were then wrong at once, and both hurt a batch job:
   * <ul>
   *   <li>the type said "retry me" for a record that is gone, so every attempt failed identically, the retry budget
   *   was spent on a foregone conclusion and the whole transaction rolled back - one stale reference killing a
   *   nightly sweep, permanently;</li>
   *   <li>the advice said {@code CHECK DATABASE RECORD <rid> FIX}, which rebuilds the edge list OF that record from
   *   the surviving edges - and that record is precisely the one that no longer exists.</li>
   * </ul>
   * The evidence separating the two cases was already on the exception: the RID it names. When it is the vertex's
   * own, this is not a publication window, and these tests pin that it is answered with a non-retryable
   * {@link VertexNotFoundException} naming a repair that actually applies - while the CHUNK case keeps the conflict
   * and the scoped advice #5764 gave it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DanglingVertexReference extends TestHelper {
    private RID srcRID;
    private RID targetRID;
    private RID edgeRID;

    /** These tests deliberately leave a reference into a deleted record, so the end-of-test check would always fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * The reported shape, single-threaded and with nothing else touching the database: the vertex named by the
     * failure and the record reported missing are the SAME RID, which is the one thing a concurrent commit in flight
     * can never produce.
     */
    @Test
    void aDeleteReachedThroughAStaleReferenceFailsAsNotFoundRatherThanAsAConflict() {
      createDanglingReference();

      final Throwable thrown = catchThrowable(() -> database.transaction(this::deleteEverythingSrcPointsAt, false, 3));

      assertThat(thrown).isInstanceOf(VertexNotFoundException.class).isInstanceOf(RecordNotFoundException.class);
      assertThat(thrown).as("a record that is gone must not be advertised as retryable: %s", thrown)
          .isNotInstanceOf(NeedRetryException.class);
      assertThat(((RecordNotFoundException) thrown).getRID()).isEqualTo(targetRID);

      assertThat(thrown.getMessage()).as("%s", thrown.getMessage())
          .contains(targetRID.toString())
          .contains("does not exist")
          .doesNotContain("concurrent commit in flight");

      // The advice must name a repair that can be run, and must not hand the operator a copy-pasteable command
      // aimed at the record that is gone - which is precisely what the conflict this replaces used to do.
      assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains("CHECK DATABASE FIX");
      assertThat(scopedRepairCommandIn(thrown.getMessage()))
          .as("no runnable RECORD-scoped command may appear: %s", thrown.getMessage()).isNull();
      assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).doesNotContain("CHECK DATABASE RECORD " + targetRID);

      // The original not-found is kept, so the trace still says which record and which read found it missing.
      assertThat(thrown.getCause()).as("the diagnosis must WRAP the not-found, not replace it")
          .isInstanceOf(RecordNotFoundException.class);
    }

    /**
     * The half of the report that hurt most: the exception type is what the retry machinery reads, so reporting a
     * gone record as a conflict re-ran the whole transaction to fail identically each time. Counted rather than
     * inferred - the block must run exactly ONCE out of the three attempts offered.
     */
    @Test
    void theFailureDoesNotSpendTheRetryBudget() {
      createDanglingReference();

      final AtomicInteger attempts = new AtomicInteger();
      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        deleteEverythingSrcPointsAt();
      }, false, 3)).isInstanceOf(VertexNotFoundException.class);

      assertThat(attempts.get()).as("a failure that can never succeed must not be retried").isEqualTo(1);
    }

    /**
     * The advice is only worth printing if it repairs the database, so this runs it. {@code CHECK DATABASE FIX} at
     * DATABASE scope drops the entry pointing at the missing record; the delete that failed then goes through.
     */
    @Test
    void theRepairItAdvertisesIsTheOneThatWorks() {
      createDanglingReference();

      assertThatThrownBy(() -> database.transaction(this::deleteEverythingSrcPointsAt, false, 1))
          .isInstanceOf(VertexNotFoundException.class);

      try (final ResultSet rs = database.command("sql", "check database fix")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("the fix must repair something: %s", row.toJSON()).isGreaterThan(0L);
      }

      // The stale entry is gone, so the sweep that could not run at all now completes.
      database.transaction(this::deleteEverythingSrcPointsAt);
      database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L));

      assertIntegrityClean();
    }

    /**
     * The other half of the split, guarded so the fix cannot creep over it: a vertex that EXISTS whose head chunk
     * cannot be read is still the publication window #5670 answers with a retryable conflict, and still carries the
     * scoped repair #5764 gave it - the list of a record that is there really can be rebuilt.
     */
    @Test
    void anUnreadableChunkKeepsItsRetryableConflictAndItsScopedAdvice() {
      createDanglingReference();

      final RID headChunk = outHeadChunk(srcRID);
      deleteRecord(headChunk);

      final Throwable thrown = catchThrowable(() -> database.transaction(() -> srcRID.asVertex().delete(), false, 1));

      assertThat(thrown).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
      assertThat(thrown).isNotInstanceOf(VertexNotFoundException.class);
      assertThat(thrown.getMessage()).as("%s", thrown.getMessage()).contains(headChunk.toString());
      assertThat(scopedRepairCommandIn(thrown.getMessage())).as("%s", thrown.getMessage())
          .isEqualTo("CHECK DATABASE RECORD " + srcRID + " FIX");
    }

    /**
     * Unchanged, and the policy the new tolerance in {@code disconnectEndpoint} extends: an EDGE whose endpoint
     * vertex is gone has nothing to disconnect on that side, so its delete succeeds instead of failing. Deleting the
     * edge is in fact the hand repair for this corruption, and it leaves the graph clean.
     */
    @Test
    void deletingAnEdgeWhoseEndpointVertexIsGoneStaysTolerated() {
      createDanglingReference();

      database.transaction(() -> edgeRID.asEdge().delete());

      database.transaction(() -> {
        assertThat(database.existsRecord(edgeRID)).isFalse();
        assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(0L);
      });
      assertIntegrityClean();
    }

    /**
     * {@code src --E1--> target}, with target's RECORD dropped straight out of its bucket and the adjacency entry on
     * src left in place: the state a pre-#5670 delete left behind, reproduced without any concurrency.
     */
    private void createDanglingReference() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Src", 1);
        database.getSchema().createVertexType("Target", 1);
        database.getSchema().createEdgeType("E1", 1);
      });

      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        final MutableVertex target = database.newVertex("Target");
        target.save();
        srcRID = src.getIdentity();
        targetRID = target.getIdentity();
        edgeRID = src.newEdge("E1", target).getIdentity();
      });

      deleteRecord(targetRID);

      // The reference really is still there: src still reports one outgoing edge into the record that is gone.
      database.transaction(() -> assertThat(srcRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "E1")).isEqualTo(1L));
    }

    /** The batch job's inner loop: walk what src points at and delete it - reaching the target through the entry. */
    private void deleteEverythingSrcPointsAt() {
      for (final Vertex v : srcRID.asVertex().getVertices(Vertex.DIRECTION.OUT, "E1"))
        v.delete();
    }

    private RID outHeadChunk(final RID vertexRID) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getOutEdgesHeadChunk());
      assertThat(holder[0]).as("the vertex must have an outgoing edge list").isNotNull();
      return holder[0];
    }

    private void deleteRecord(final RID rid) {
      database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
      database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
    }

    /** The {@code CHECK DATABASE RECORD <rid> FIX} command a message tells the operator to run, or null. */
    private static String scopedRepairCommandIn(final String message) {
      final int start = message.indexOf("`CHECK DATABASE RECORD ");
      if (start < 0)
        return null;
      final int end = message.indexOf('`', start + 1);
      return end < 0 ? null : message.substring(start + 1, end);
    }

    /** Asserts on the fields {@code check database} actually reports, so a typo cannot make this vacuously pass. */
    private void assertIntegrityClean() {
      try (final ResultSet rs = database.command("sql", "check database")) {
        assertThat(rs.hasNext()).isTrue();
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat(longProperty(row, "autoFix")).as("autoFix: %s", row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "invalidLinks")).as("invalidLinks: %s", row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalWarnings")).as("totalWarnings: %s", row.toJSON()).isEqualTo(0L);
          assertThat(longProperty(row, "totalCorruptedRecords")).as("totalCorruptedRecords: %s", row.toJSON()).isEqualTo(0L);
        }
      }
    }

    /** Reads a numeric check-database property, failing loudly when the field does not exist (a vacuous assertion). */
    private static long longProperty(final Result row, final String name) {
      final Object value = row.getProperty(name);
      assertThat(value).as("check database must report '%s': %s", name, row.toJSON()).isNotNull();
      return ((Number) value).longValue();
    }
  }

  /**
   * Regression test for issue #9208: threads deleting vertices that share no record failed with a
   * {@code ConcurrentModificationException} when their edge list segments shared pages, because neither the edge-append
   * merge nor the disjoint-slot merge could rebase a page that holds an edge removal or a deleted segment.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class ConcurrentEdgeDeleteRepair extends TestHelper {
    private static final int THREADS = 4;
    private static final int PER_THREAD = 1500;
    private static final int HUBS = 100;

    @Test
    void concurrentDeletesOfVerticesSharingEdgeListPagesAreMerged() throws Exception {
      database.transaction(() -> {
        database.getSchema().createVertexType("V");
        database.getSchema().createEdgeType("E");
      });

      final RID[][] hubs = new RID[THREADS][HUBS];
      final RID[][] victims = new RID[THREADS][PER_THREAD];

      database.transaction(() -> {
        for (int i = 0; i < HUBS; i++)
          for (int t = 0; t < THREADS; t++)
            hubs[t][i] = database.newVertex("V").save().getIdentity();
      });

      final SplittableRandom random = new SplittableRandom(42);
      for (int start = 0; start < PER_THREAD; start += 500) {
        final int from = start;
        database.transaction(() -> {
          for (int i = from; i < Math.min(PER_THREAD, from + 500); i++)
            for (int t = 0; t < THREADS; t++) {
              victims[t][i] = database.newVertex("V").save().getIdentity();
              hubs[t][random.nextInt(HUBS)].asVertex().newEdge("E", victims[t][i]);
            }
        });
      }

      final AtomicLong failedToCaller = new AtomicLong();
      final long mergesBefore = PageManager.INSTANCE.getStats().txPageSlotMerges;

      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      try {
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
          final int thread = t;
          futures.add(pool.submit(() -> {
            for (int i = 0; i < PER_THREAD; i++) {
              final RID rid = victims[thread][i];
              try {
                database.transaction(() -> database.lookupByRID(rid, true).delete());
              } catch (final ConcurrentModificationException e) {
                failedToCaller.incrementAndGet();
              }
            }
          }));
        }
        for (final Future<?> f : futures)
          f.get(5, TimeUnit.MINUTES);
      } finally {
        pool.shutdownNow();
      }

      assertThat(failedToCaller.get()).as("deletes of unrelated vertices that failed after their retries").isZero();
      assertThat(PageManager.INSTANCE.getStats().txPageSlotMerges - mergesBefore).as("page conflicts rebased by the slot merge")
          .isPositive();
      assertThat(database.countType("V", false)).isEqualTo((long) THREADS * HUBS);
      for (int t = 0; t < THREADS; t++)
        for (int i = 0; i < HUBS; i++)
          assertThat(hubs[t][i].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isZero();
    }

    @Test
    void concurrentAppendsAndDeletesOnSharedPagesKeepEveryEdgeList() throws Exception {
      database.transaction(() -> {
        database.getSchema().createVertexType("V");
        database.getSchema().createEdgeType("E");
      });

      final int perThread = 600;
      final RID[][] hubs = new RID[THREADS][HUBS];
      final RID[][] victims = new RID[THREADS][perThread];
      final int[][] expected = new int[THREADS][HUBS];

      database.transaction(() -> {
        for (int i = 0; i < HUBS; i++)
          for (int t = 0; t < THREADS; t++)
            hubs[t][i] = database.newVertex("V").save().getIdentity();
      });
      final SplittableRandom random = new SplittableRandom(7);
      final int[][] hubOf = new int[THREADS][perThread];
      for (int start = 0; start < perThread; start += 300) {
        final int from = start;
        database.transaction(() -> {
          for (int i = from; i < Math.min(perThread, from + 300); i++)
            for (int t = 0; t < THREADS; t++) {
              victims[t][i] = database.newVertex("V").save().getIdentity();
              hubOf[t][i] = random.nextInt(HUBS);
              hubs[t][hubOf[t][i]].asVertex().newEdge("E", victims[t][i]);
              expected[t][hubOf[t][i]]++;
            }
        });
      }

      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      try {
        final List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
          final int thread = t;
          futures.add(pool.submit(() -> {
            final SplittableRandom r = new SplittableRandom(thread);
            for (int idx = 0; idx < perThread; idx++) {
              final int i = idx;
              final int newHub = r.nextInt(HUBS);
              try {
                // One transaction both deletes a vertex (removing an edge) and appends a new edge to the same hub list
                database.transaction(() -> {
                  database.lookupByRID(victims[thread][i], true).delete();
                  final MutableVertex created = database.newVertex("V").save();
                  hubs[thread][newHub].asVertex().newEdge("E", created);
                });
                expected[thread][hubOf[thread][i]]--;
                expected[thread][newHub]++;
              } catch (final ConcurrentModificationException e) {
                // The whole transaction was rolled back: nothing to account for
              }
            }
          }));
        }
        for (final Future<?> f : futures)
          f.get(5, TimeUnit.MINUTES);
      } finally {
        pool.shutdownNow();
      }

      for (int t = 0; t < THREADS; t++)
        for (int h = 0; h < HUBS; h++)
          assertThat(hubs[t][h].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).as("hub %d of thread %d", h, t)
              .isEqualTo(expected[t][h]);

      final ResultSet check = database.command("sql", "CHECK DATABASE");
      assertThat(check.hasNext()).isTrue();
      assertThat(((Number) check.next().getProperty("totalErrors")).longValue()).isZero();
    }

    @Test
    void appendAndDeleteInOneTransactionOnPageSharedWithAnotherCommit() throws Exception {
      database.transaction(() -> {
        database.getSchema().createVertexType("V");
        database.getSchema().createEdgeType("E");
      });

      // Two hubs and their victims are created together, so both hubs' edge list segments share pages
      final RID[] ids = new RID[4];
      database.transaction(() -> {
        for (int i = 0; i < 4; i++)
          ids[i] = database.newVertex("V").save().getIdentity();
        ids[0].asVertex().newEdge("E", ids[2]);
        ids[1].asVertex().newEdge("E", ids[3]);
      });
      final RID hub1 = ids[0];
      final RID hub2 = ids[1];
      final RID victim1 = ids[2];
      final RID victim2 = ids[3];

      // T1 (this thread) appends an edge to hub1 AND deletes victim1 (an edge removal on the same list); it stays open
      database.begin();
      final MutableVertex added = database.newVertex("V").save();
      hub1.asVertex().newEdge("E", added);
      database.lookupByRID(victim1, true).delete();

      // T2 commits a delete of the other hub's victim meanwhile, on the very same pages
      final ExecutorService pool = Executors.newSingleThreadExecutor();
      try {
        pool.submit(() -> database.transaction(() -> database.lookupByRID(victim2, true).delete())).get(1, TimeUnit.MINUTES);
      } finally {
        pool.shutdownNow();
      }

      boolean committed = true;
      try {
        database.commit();
      } catch (final ConcurrentModificationException e) {
        // A refused merge is a legitimate outcome, a corrupted edge list is not: the transaction is rolled back whole
        committed = false;
      }

      assertThat(hub2.asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isZero();
      // hub1 lost victim1 and gained the new vertex, or (rolled back) kept victim1 and gained nothing: one edge either way
      assertThat(hub1.asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
      assertThat(hub1.asVertex().getVertices(Vertex.DIRECTION.OUT, "E").iterator().next().getIdentity())
          .isEqualTo(committed ? added.getIdentity() : victim1);

      final ResultSet check = database.command("sql", "CHECK DATABASE");
      assertThat(((Number) check.next().getProperty("totalErrors")).longValue()).isZero();
    }
  }
}
