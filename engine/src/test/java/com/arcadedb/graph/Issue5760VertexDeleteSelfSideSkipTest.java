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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
   * A self-loop is reachable from BOTH of the vertex's lists, so both walks yield it and the delete pipeline runs
   * for it twice - the second time over a record the first pass already removed, which `EdgeIterator.hasNext`
   * does not filter out because it resolves the edge with {@code loadContent=false} and a lazy handle to a record
   * deleted earlier in the same transaction still resolves.
   * <p>
   * That means {@code onBeforeDelete} fires TWICE for a self-loop, and a listener that is not idempotent sees it.
   * Pinned here as an exact count, in both the record-backed and the lightweight shape, because the number is the
   * whole point: it is what this path did BEFORE #5760 as well (the two-phase walk collected the self-loop from
   * each list and called {@code delete()} on it twice, the second reaching the tolerated
   * {@code RecordNotFoundException} inside {@code deleteEdge}), so streaming changed nothing here and must keep
   * not changing it. Verified against the parent commit: two events there, two events now. A future change that
   * makes it one - or three - moves this assertion.
   */
  @Test
  void aSelfLoopIsWalkedFromBothListsSoItsDeleteEventFiresTwice() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Loop", 1);
      database.getSchema().createEdgeType("LINK", 4);
      database.getSchema().buildEdgeType().withName("LIGHT").withTotalBuckets(4).withLightweight(true).create();
    });

    assertThat(countDeleteEventsDeletingASelfLoop("LINK"))
        .as("a record-backed self-loop: yielded by the OUT walk and again by the IN walk").isEqualTo(2);
    assertThat(countDeleteEventsDeletingASelfLoop("LIGHT"))
        .as("a lightweight self-loop has no record to go missing, so both walks yield it just the same")
        .isEqualTo(2);

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
  void deletingAVertexWhileItsNeighboursListsAreBeingAppendedTo() throws InterruptedException {
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
