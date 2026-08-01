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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #5680: deleting a vertex must never delete the record while leaving its own edges behind.
 * <p>
 * {@code GraphEngine.deleteVertex} collected the edges to remove through the best-effort
 * {@code getEdgeHeadChunk} and walked the chain with a read iterator, with the whole collection wrapped in a
 * {@code catch (Exception)}. A chunk that could not be read - the transient window #5670 was about, where a
 * concurrent commit publishes its pages one at a time so a vertex page can expose a head RID before that head's
 * own page is visible, or an emptied chunk is relinked out from under a walker - was therefore read as "nothing
 * to remove here", and the vertex record was deleted anyway. The edges outlived their endpoint: ghost edges
 * whose {@code out}/{@code in} names a record that no longer exists, plus back-references on innocent
 * neighbours. On a promoted super-node (#5156) the loss had no exception behind it at all: the read walk skips a
 * stripe chain it cannot load, so a whole stripe's worth of edges simply was not collected. Under {@code force}
 * that tolerance is the documented contract, but it did not reach far enough: a broken NEIGHBOUR blocked a forced
 * delete just as it blocked an ordinary one.
 * <p>
 * These tests pin both halves: strict (retryable) by default, tolerant end-to-end under {@code force}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5680VertexDeleteEdgeCollectionTest extends TestHelper {

  /** These tests deliberately corrupt an edge-list chain, so the blanket end-of-test check would always fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The window as measured on the issue: the vertex's OWN outgoing head chunk is unreadable while the head RID on
   * the vertex record still points at it. Collecting nothing there and deleting the vertex regardless is what left
   * the edge record alive with a dangling {@code out} and the neighbour still pointing at it.
   */
  @Test
  void deletingAVertexWhoseOwnHeadChunkIsUnreadableReportsAConflictRatherThanLosingItsEdges() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 1);
    final RID srcRID = outVertexOf(edges.get(0));

    final RID headChunk = outHeadChunk(srcRID);
    assertPrecondition(hubRID, edges.get(0), 1);
    deleteRecord(headChunk);

    assertThatThrownBy(() -> database.transaction(() -> srcRID.asVertex().delete(), false, 1))
        .isInstanceOf(ConcurrentModificationException.class)
        .isInstanceOf(NeedRetryException.class);

    // The whole delete was rolled back, so a retry can complete it properly instead of leaving a ghost edge.
    database.transaction(() -> {
      assertThat(database.existsRecord(srcRID)).as("the source vertex must survive the failed delete").isTrue();
      assertThat(database.existsRecord(edges.get(0))).as("its edge must survive too").isTrue();
      assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(1);
    });
  }

  /**
   * Same contract one hop further in: the hole is in the MIDDLE of the vertex's own chain, which is what a
   * concurrent commit leaves behind when it empties a chunk and relinks the chain past it. The collection walk
   * used to stop at the hole - keeping the edges it had already seen, silently dropping every edge behind it -
   * and the vertex record was deleted on top of that partial view.
   */
  @Test
  void deletingAVertexWhoseChainIsBrokenMidWayReportsAConflictRatherThanLosingTheEdgesBehindTheHole() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 200);

    // The hub's IN chain is the multi-chunk one; break it in the middle and delete the HUB itself.
    final List<RID> chain = inChunkChain(hubRID);
    assertThat(chain).as("the hub's IN list must span several chunks").hasSizeGreaterThan(3);
    final RID midChunk = chain.get(1);
    assertPrecondition(hubRID, edges.get(0), edges.size());

    deleteRecord(midChunk);

    assertThatThrownBy(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1))
        .isInstanceOf(ConcurrentModificationException.class)
        .isInstanceOf(NeedRetryException.class);

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).as("the hub must survive the failed delete").isTrue();
      // The edges behind the hole - the ones the partial walk used to drop - are all still there.
      for (final RID edge : edges)
        assertThat(database.existsRecord(edge)).as("edge " + edge).isTrue();
    });
  }

  /**
   * The same contract on the striped layout (#5156). A promoted super-node's list is N per-stripe chains, and a
   * chain whose head cannot be read is legitimately SKIPPED on a read - so the collection walk lost a whole
   * stripe's worth of edges without ever raising anything, and the hub was deleted on top of that.
   */
  @Test
  void deletingAPromotedSuperNodeWhoseStripeChainIsUnreadableReportsAConflict() {
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
    try {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> edges = createEdges(hubRID, 1_000);
      assertPrecondition(hubRID, edges.get(0), edges.size());

      // The hub must really have been promoted, otherwise this is just the classic-layout test again.
      final RID[] stripeHead = new RID[1];
      database.transaction(() -> {
        final RID head = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk();
        assertThat(database.lookupByRID(head, true)).as("the hub must be a promoted super-node")
            .isInstanceOf(StripeDirectory.class);
        final StripeDirectory directory = (StripeDirectory) database.lookupByRID(head, true);
        // Generation 1 stripe 0: a chain created by the promotion, alongside the generation-0 classic chain.
        stripeHead[0] = directory.getHead(1, 0);
        assertThat(stripeHead[0]).as("the promotion must have pre-warmed the stripe chains").isNotNull();
      });
      deleteRecord(stripeHead[0]);

      // The two walks over the same broken list, side by side. The READ one still skips the chain it cannot load -
      // that is the documented best-effort read contract and this fix does not touch it - and therefore under-reports
      // the degree without a word. The REMOVAL one, which the vertex delete uses, refuses instead: skipping there
      // means deleting the hub while a whole stripe's worth of edges keeps pointing at it.
      database.transaction(() -> {
        final EdgeLinkedList list = graphEngine()
            .getEdgeHeadChunkForWrite((VertexInternal) hubRID.asVertex(), Vertex.DIRECTION.IN);
        assertThat(count(list.edgeIterator())).as("the read walk silently loses the unreadable stripe")
            .isGreaterThan(0).isLessThan(edges.size());
        assertThatThrownBy(() -> count(list.edgeIteratorForRemoval()))
            .isInstanceOf(ConcurrentModificationException.class)
            .hasMessageContaining(stripeHead[0].toString());
      });

      assertThatThrownBy(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1))
          .isInstanceOf(ConcurrentModificationException.class)
          .isInstanceOf(NeedRetryException.class);

      database.transaction(() -> assertThat(database.existsRecord(hubRID)).isTrue());
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }

  /**
   * The documented escape hatch, unchanged: {@code force} means "this record is known broken, get it out", so a
   * head chunk that cannot be read stays tolerated there. The surviving edges are the price the caller accepted;
   * {@code CHECK DATABASE} is the repair path.
   */
  @Test
  void aForcedDeleteStillRemovesAVertexWhoseOwnEdgeListIsUnreadable() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 1);
    final RID srcRID = outVertexOf(edges.get(0));

    deleteRecord(outHeadChunk(srcRID));

    database.transaction(() -> graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true));

    database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());
  }

  /**
   * {@code force} did not reach far enough before this fix: deleting a vertex disconnects its edges from the
   * vertices at the OTHER end too, and since #5678 that removal is strict - so a broken NEIGHBOUR raised a
   * retryable conflict out of the per-edge loop, which {@code force} never absorbed. A forced delete was blocked
   * by corruption on a vertex the caller never named, which is exactly what {@code force} promises to override.
   */
  @Test
  void aForcedDeleteIsNotBlockedByAnUnreadableNeighbourEdgeList() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 20);
    final RID srcRID = outVertexOf(edges.get(edges.size() - 1));

    // Break the HUB's IN list - the neighbour of the healthy vertex about to be force-deleted.
    assertPrecondition(hubRID, edges.get(0), edges.size());
    deleteRecord(inHeadChunk(hubRID));

    // Without force this is a conflict (pinned by #5670's test); with force it must go through.
    assertThatCode(
        () -> database.transaction(() -> graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true)))
        .doesNotThrowAnyException();

    database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());
  }

  /**
   * The tolerant path must stay tolerant end to end: a healthy graph is deleted whole, and a forced delete of a
   * vertex with a readable list still disconnects its neighbours properly. Guards the fix against becoming
   * "force skips the disconnection".
   */
  @Test
  void aForcedDeleteOfAHealthyVertexStillDisconnectsItsNeighbours() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 5);
    final RID srcRID = outVertexOf(edges.get(0));

    database.transaction(() -> graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true));

    database.transaction(() -> {
      assertThat(database.existsRecord(srcRID)).isFalse();
      assertThat(database.existsRecord(edges.get(0))).as("the edge record must be gone with its vertex").isFalse();
      assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
          .as("the neighbour must not keep a back-reference to the deleted edge").isEqualTo(edges.size() - 1);
    });

    assertIntegrityClean();
  }

  /** An ordinary delete on an intact graph is untouched by the strict read: everything goes, integrity is clean. */
  @Test
  void anOrdinaryDeleteOfAHealthyVertexRemovesEveryEdgeOnBothSides() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 200);

    database.transaction(() -> hubRID.asVertex().delete());

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).isFalse();
      for (final RID edge : edges)
        assertThat(database.existsRecord(edge)).as("edge " + edge + " must not outlive its endpoint").isFalse();
    });

    assertIntegrityClean();
  }

  /**
   * A self-loop is the one shape the two collection walks BOTH yield: {@code A --> A} is reachable from A's OUT list
   * and from its IN list, so it lands in the delete list twice and the second removal runs against a chain the first
   * already cleaned. Harmless today - and unchanged by this fix - but the removal walk is stricter now, so this pins
   * that the duplicate resolves quietly instead of surfacing as a conflict and failing an ordinary delete.
   */
  @Test
  void deletingAVertexWithASelfLoopRemovesItCleanly() {
    createSchema();

    final RID[] holder = new RID[2];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Src");
      v.save();
      holder[0] = v.getIdentity();
      holder[1] = v.newEdge("LINK", v).getIdentity();
    });
    final RID vertexRID = holder[0];
    final RID selfLoop = holder[1];

    // The self-loop really is reachable from both directions, otherwise the duplicate never arises.
    database.transaction(() -> {
      assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.OUT, "LINK")).isEqualTo(1);
      assertThat(vertexRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(1);
    });

    database.transaction(() -> vertexRID.asVertex().delete());

    database.transaction(() -> {
      assertThat(database.existsRecord(vertexRID)).isFalse();
      assertThat(database.existsRecord(selfLoop)).as("the self-loop must not outlive its only endpoint").isFalse();
    });

    assertIntegrityClean();
  }

  /**
   * The shape the strict read has to survive in production: vertices being deleted while other transactions append
   * edges to them, so the deleter reads an edge list a concurrent commit is publishing right now. Every edge in the
   * fixture points at a hub, so once every hub is gone NO edge record may survive - a single ghost edge is exactly
   * the defect this issue is about. The retries must also be enough to absorb the contention: a delete that gives
   * up here would mean the strict read traded a silent loss for a workload that cannot make progress.
   * <p>
   * This test was originally scoped to the FIXTURE edges only - the ones already in the hub's list when the deleter
   * started - because an edge an APPENDER committed into a hub being deleted could still outlive it through a
   * separate window on the append path, reported as #5725. That window is closed (the delete now pins every page
   * its list can grow through and re-reads the vertex's head pointers, so an append behind the collection is a
   * retryable conflict), so the assertion below is the whole invariant again. The full append-side fixture, and the
   * deterministic tests that pin the mechanism, live in {@code Issue5725GhostEdgeOnAppendRaceTest}.
   */
  @Test
  @Tag("slow")
  void concurrentVertexDeletionAndEdgeAppendingLeaveNoEdgeBehind() throws InterruptedException {
    final int rounds = 10;
    final int hubs = 4;
    final int pool = 100;
    final int appenders = 6;
    final int appendsPerThread = 120;

    final AtomicLong appendsLanded = new AtomicLong();

    final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    try {
      createSchema();

      for (int round = 0; round < rounds; round++) {
        final List<RID> hubRIDs = new ArrayList<>(hubs);
        final List<RID> fixtureEdges = new ArrayList<>(hubs * pool);
        for (int h = 0; h < hubs; h++) {
          final RID hubRID = createHub();
          fixtureEdges.addAll(createEdges(hubRID, pool));
          hubRIDs.add(hubRID);
        }

        // The round has something to lose before it starts: without this the invariant below holds vacuously. Stated
        // per hub rather than as a global LINK count, so an edge an earlier round's appenders left behind (see the
        // note on the end-of-round assertion) cannot make a healthy round look wrong.
        database.transaction(() -> {
          for (final RID hubRID : hubRIDs)
            assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"))
                .as("hub " + hubRID + " must start the round with a full edge list").isEqualTo(pool);
        });

        final AtomicLong deleteFailures = new AtomicLong();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(appenders + hubs);

        for (int t = 0; t < appenders; t++) {
          final int worker = t;
          new Thread(() -> {
            if (!awaitStart(start)) {
              done.countDown();
              return;
            }
            for (int i = 0; i < appendsPerThread; i++) {
              final RID hubRID = hubRIDs.get((worker + i) % hubs);
              try {
                database.transaction(() -> {
                  final MutableVertex src = database.newVertex("Src");
                  src.save();
                  src.newEdge("LINK", hubRID);
                }, false, 10_000);
                appendsLanded.incrementAndGet();
              } catch (final Throwable expected) {
                // Appending to a hub that has just been deleted cannot succeed: whichever way the race resolves,
                // the append ends as a "record not found" (or a conflict whose retry finds the hub gone). Only the
                // DELETE side is held to a failure count - the invariant below is what this test is really about.
              }
            }
            done.countDown();
          }).start();
        }

        for (int h = 0; h < hubs; h++) {
          final RID hubRID = hubRIDs.get(h);
          new Thread(() -> {
            if (!awaitStart(start)) {
              done.countDown();
              return;
            }
            try {
              database.transaction(() -> hubRID.asVertex().delete(), false, 10_000);
            } catch (final Throwable unexpected) {
              deleteFailures.incrementAndGet();
            }
            done.countDown();
          }).start();
        }

        start.countDown();
        // Bounded: a future regression that wedges a worker should fail this test, not hang CI until the job times
        // out. The whole round is seconds of work, so minutes of headroom cannot fire on a merely slow machine.
        assertThat(done.await(5, TimeUnit.MINUTES)).as("round " + round + ": all workers finished").isTrue();
        assertThat(deleteFailures.get()).as("round " + round + ": vertex deletes that never completed").isEqualTo(0);

        final int currentRound = round;
        database.transaction(() -> {
          for (final RID hubRID : hubRIDs)
            assertThat(database.existsRecord(hubRID)).as("round " + currentRound + ": hub " + hubRID + " must be gone")
                .isFalse();
          // The invariant this test owns: every edge that was IN the hub's list when the deleter started must be
          // gone with it. Those are exactly the fixture edges - the deleter's collection walk saw all of them, so a
          // survivor here is the collection having lost one, which is the defect this issue is about.
          for (final RID edge : fixtureEdges)
            assertThat(database.existsRecord(edge))
                .as("round " + currentRound + ": edge " + edge + " must not outlive the hub that was deleting it")
                .isFalse();
          // And, since #5725, the same for an edge an APPENDER committed into a hub being deleted: it did not
          // exist when the collection ran, so nothing in the collection could have caught it, but the delete now
          // conflicts on the pages that append landed on and retries instead of committing over it.
          assertThat(database.countType("LINK", false))
              .as("round " + currentRound + ": edges outliving the hub they pointed at").isEqualTo(0L);
        });
      }

      // Appends really did race the deletes rather than all losing to a hub that was already gone: if none of them
      // ever landed, the rounds above only measured a plain sequential delete.
      assertThat(appendsLanded.get()).as("appends committed against a hub still being deleted").isGreaterThan(0L);
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
    }

    // Since #5725 this fixture ends clean: no edge survives its hub from either side of the race, so there is no
    // broken link left for CHECK DATABASE to report.
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

  private static int count(final Iterator<Edge> iterator) {
    int total = 0;
    while (iterator.hasNext()) {
      iterator.next();
      ++total;
    }
    return total;
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

  /** The graph is intact and the edge under test is really reachable from the hub before the chain is broken. */
  private void assertPrecondition(final RID hubRID, final RID edgeRID, final int expectedDegree) {
    database.transaction(() -> {
      assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(expectedDegree);
      assertThat(database.existsRecord(edgeRID)).isTrue();
    });
  }

  private void deleteRecord(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
    database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
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

  /** Null-tolerant read of a numeric check-database property, so a missing field fails clearly instead of NPE. */
  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }
}
