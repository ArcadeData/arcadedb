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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #5670: deleting an edge must never leave its back-reference behind.
 * <p>
 * The removal walk over an endpoint's edge list used to treat an unreadable chunk as "nothing to remove": the head
 * chunk was read through the best-effort {@code getEdgeHeadChunk} (null on a miss) and the chain hops through a
 * plain lookup, while {@code deleteEdge} wrapped both in a {@code catch (RecordNotFoundException)}. Under
 * concurrency a chunk is regularly unreadable for reasons that have nothing to do with the graph: a commit
 * publishes its pages one at a time and the reader takes no commit lock, so a vertex page can expose a head RID
 * before that head's own page is visible, and an emptied chunk is relinked out of the chain under a walker's feet.
 * The removal was then skipped while the edge record was deleted anyway - one edge too many in the endpoint's
 * degree, plus one integrity error. The append path already answered the same window with a retryable conflict;
 * these tests pin that the removal path now does too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5670EdgeDeleteDanglingBackRefTest extends TestHelper {

  /** These tests deliberately corrupt an edge-list chain, so the blanket end-of-test check would always fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The transient window as observed in the field: the hub's IN head chunk RID is readable on the vertex but the
   * chunk itself is not. Simulated by removing the head chunk record. The edge delete must raise a retryable
   * conflict, leaving the edge (and its back-reference) intact for the retry - not commit a delete that strips the
   * record while the back-reference survives.
   */
  @Test
  void edgeDeleteRaisesRetryableConflictWhenTheEndpointHeadChunkIsUnreadable() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 200);

    final List<RID> chain = inChunkChain(hubRID);
    assertThat(chain).as("the hub's IN list must span several chunks").hasSizeGreaterThan(3);

    final RID victim = edges.get(0);
    assertPrecondition(hubRID, victim, edges.size());

    final RID headChunk = chain.get(0);
    deleteRecord(headChunk);

    assertThatThrownBy(() -> database.transaction(() -> victim.asEdge().delete(), false, 1))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining(headChunk.toString());

    // The whole delete was rolled back: the edge record is still there, so a retry can complete it properly.
    database.transaction(() -> assertThat(database.existsRecord(victim)).isTrue());
  }

  /**
   * Same contract one hop further in: a chunk in the MIDDLE of the chain is gone (what a concurrent commit does
   * when it empties a chunk and relinks the chain past it), and the edge to remove lives behind it. Abandoning the
   * walk there is what left the back-reference dangling.
   */
  @Test
  void edgeDeleteRaisesRetryableConflictWhenAMidChainChunkIsUnreadable() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 200);

    final List<RID> chain = inChunkChain(hubRID);
    assertThat(chain).as("the hub's IN list must span several chunks").hasSizeGreaterThan(3);

    // The oldest chunk is the chain's tail, so the first edge created is the one furthest from the head.
    final RID victim = edges.get(0);
    final RID midChunk = chain.get(1);
    assertThat(chunkHolding(victim, chain)).as("the victim must sit BEHIND the chunk about to vanish")
        .isGreaterThan(chain.indexOf(midChunk));
    assertPrecondition(hubRID, victim, edges.size());

    deleteRecord(midChunk);

    assertThatThrownBy(() -> database.transaction(() -> victim.asEdge().delete(), false, 1))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining(midChunk.toString());

    database.transaction(() -> assertThat(database.existsRecord(victim)).isTrue());
  }

  /**
   * The head RID is read off the vertex INSIDE the strict lookup's try, because on a handle that has not loaded its
   * record yet that read lazy-loads it - so an endpoint deleted since the caller resolved it surfaces here rather
   * than at the chunk lookup. Escaping raw, that is a plain {@code RecordNotFoundException}: not a
   * {@code NeedRetryException}, so it would fail the transaction outright instead of retrying it.
   */
  @Test
  void headChunkForWriteRaisesRetryableConflictWhenTheVertexItselfVanishes() {
    createSchema();
    final RID hubRID = createHub();
    createEdges(hubRID, 20);

    database.begin();
    try {
      // A handle that has NOT materialised its record (loadContent=false): reading its head RID lazy-loads it.
      final VertexInternal lazyHub = (VertexInternal) database.lookupByRID(hubRID, false);
      database.getSchema().getBucketById(hubRID.getBucketId()).deleteRecord(hubRID);

      assertThatThrownBy(
          () -> ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunkForWrite(lazyHub, Vertex.DIRECTION.IN))
          .isInstanceOf(ConcurrentModificationException.class)
          .isInstanceOf(NeedRetryException.class);
    } finally {
      database.rollback();
    }
  }

  /**
   * The second-order effect of the strict removal, locked in deliberately: deleting a vertex disconnects its edges
   * from the vertices on the OTHER end too, so a healthy vertex whose NEIGHBOUR's edge list cannot be read now
   * reports a retryable conflict instead of succeeding.
   * <p>
   * That is the intended answer, not an oversight. Succeeding here means deleting the edge record while the
   * neighbour keeps pointing at it - creating exactly the dangling back-reference this issue is about, on a vertex
   * nobody asked to touch. Under the concurrency this fix targets the retry resolves it; on a genuinely broken
   * neighbour list the delete fails and {@code CHECK DATABASE} is the repair path. See issue #5680, which tracks
   * whether vertex deletion should keep a tolerant escape hatch for that case.
   */
  @Test
  void deletingAVertexWhoseNeighbourListIsUnreadableReportsAConflictRatherThanDanglingTheReference() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 20);

    // The SOURCE of the last edge: a perfectly healthy vertex, whose only edge points at the hub.
    final RID[] srcHolder = new RID[1];
    database.transaction(() -> srcHolder[0] = edges.get(edges.size() - 1).asEdge().getOut());

    // Break the HUB's IN list - the neighbour of the vertex we are about to delete - leaving the hub record intact.
    final RID[] headHolder = new RID[1];
    database.transaction(() -> headHolder[0] = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk());
    assertPrecondition(hubRID, edges.get(0), edges.size());
    deleteRecord(headHolder[0]);

    assertThatThrownBy(() -> database.transaction(() -> srcHolder[0].asVertex().delete(), false, 1))
        .isInstanceOf(ConcurrentModificationException.class)
        .isInstanceOf(NeedRetryException.class);

    // Rolled back whole: the source vertex is still there, so a retry can complete it properly.
    database.transaction(() -> assertThat(database.existsRecord(srcHolder[0])).isTrue());
  }

  /**
   * The reported shape (#5670): concurrent transactions that each delete one pre-existing edge of a hub and append
   * one new edge. Net degree is invariant, so the hub's IN-degree must stay at the pool size and the integrity
   * check must be clean. Before the fix this reported one edge too many - the surplus being an edge whose record
   * was deleted while its back-reference stayed in the hub's list.
   */
  @Test
  @Tag("slow")
  void concurrentDeleteAndAppendNeverLeaveADanglingBackReference() throws InterruptedException {
    final int rounds = 40;
    final int threads = 8;
    final int perThread = 60;
    final int pool = threads * perThread; // exactly one removal per iteration drains the pool

    final int savedRetryDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(1);
    try {
      createSchema();

      for (int round = 0; round < rounds; round++) {
        final RID hubRID = createHub();
        final ConcurrentLinkedQueue<RID> removable = new ConcurrentLinkedQueue<>(createEdges(hubRID, pool));

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
              final RID toRemove = removable.poll();
              try {
                database.transaction(() -> {
                  if (toRemove != null)
                    toRemove.asEdge().delete();
                  final MutableVertex src = database.newVertex("Src");
                  src.save();
                  src.newEdge("LINK", hubRID);
                }, false, 10_000);
              } catch (final Throwable unexpected) {
                // Nothing here is expected to surface: the retry budget above is far beyond what this contention
                // needs, so ANY throwable reaching this point is the bug, not a tolerated retry. Counted rather
                // than rethrown only so the assertions below run on a fully drained pool - and Throwable, not
                // Exception, so an Error cannot skip the countDown below and hang the run instead of failing it.
                failures.incrementAndGet();
              }
            }
            done.countDown();
          }).start();
        }
        start.countDown();
        // Bounded: a future regression that wedges a worker should fail this test, not hang CI until the job times
        // out. The whole round is seconds of work, so minutes of headroom cannot fire on a merely slow machine.
        assertThat(done.await(5, TimeUnit.MINUTES)).as("round " + round + ": all workers finished").isTrue();

        assertThat(failures.get()).as("round " + round).isEqualTo(0);

        final long[] inDegree = new long[1];
        database.transaction(() -> inDegree[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
        assertThat(inDegree[0]).as("round " + round + ": hub IN-degree").isEqualTo(pool);
      }

      assertIntegrityClean();
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedRetryDelay);
    }
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

  /** Index in {@code chain} of the chunk holding {@code edgeRID}, or -1. */
  private int chunkHolding(final RID edgeRID, final List<RID> chain) {
    final int[] found = new int[] { -1 };
    database.transaction(() -> {
      for (int i = 0; i < chain.size(); i++)
        if (((EdgeSegment) database.lookupByRID(chain.get(i), true)).containsEdge(edgeRID)) {
          found[0] = i;
          break;
        }
    });
    return found[0];
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
