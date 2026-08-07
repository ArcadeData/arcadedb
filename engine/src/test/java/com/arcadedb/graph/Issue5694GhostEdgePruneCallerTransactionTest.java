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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5694: the opportunistic prune of a ghost edge, run from inside a READ, must never touch the transaction the
 * caller owns.
 * <p>
 * {@code EdgeIteratorFilter.handleCorruption} pruned through {@code database.transaction(block, true)}. With a
 * caller transaction already open that block JOINS it, and a retryable condition inside the block makes
 * {@code LocalDatabase.transaction} roll the joined transaction back - the CALLER'S transaction, not one it
 * created - and then begin, run and commit one of its own. The caller, which only iterated edges, lost every
 * write it had made before the iteration, silently: the prune is best effort and absorbs the retryable exception.
 * <p>
 * The state the two tests below plant is the state that race leaves behind, so neither depends on a schedule:
 * a ghost edge reachable by the iterator, and (for the rollback case) a second defect on the write path that makes
 * the prune raise {@link com.arcadedb.exception.ConcurrentModificationException} on every attempt.
 */
class Issue5694GhostEdgePruneCallerTransactionTest extends TestHelper {
  /** Both tests deliberately end on a graph that still holds the planted ghost: the blanket check would fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The defect itself. The hub is a promoted super-node, so its removal walk visits every stripe chain STRICTLY
   * ({@code StripedEdgeList.allChains(true)}) and a chain whose head cannot be read is a retryable conflict rather
   * than a chain to skip. One broken chain therefore makes the prune raise on every attempt, which is exactly the
   * condition the issue describes, without a second thread or a timing window.
   * <p>
   * Before the fix the caller came out of the loop with no transaction at all: the retry loop rolled its
   * transaction back on the first attempt, then ran and committed three of its own.
   */
  @Test
  void aGhostEdgeMetInsideTheCallersTransactionLeavesThatTransactionAlone() {
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
    try {
      createSchema();
      final RID hubRID = createHub();
      createEdges(hubRID, 600);
      assertPromotedToSuperNode(hubRID);

      // The prune's removal walk is strict over every stripe chain, so an unreadable one is a retryable conflict.
      breakOneStripeChain(hubRID);
      // A ghost the READ can still reach: the broken chain is skipped by the read, so the ghost must live
      // elsewhere or the iterator would never meet it.
      final RID ghost = ghostAnEdgeStillReachableByAReader(hubRID);

      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      database.begin();
      final TransactionContext callerTx = ((DatabaseInternal) database).getTransaction();

      final MutableVertex marker = database.newVertex("Src").set("marker", "5694").save();
      final RID markerRID = marker.getIdentity();

      int seen = 0;
      for (final Edge ignored : hubRID.asVertex().getEdges(Vertex.DIRECTION.IN, "LINK"))
        ++seen;

      assertThat(seen).as("the traversal must have yielded the edges of the readable chains").isGreaterThan(0);
      assertThat(database.isTransactionActive())
          .as("iterating edges must not end the transaction the caller began").isTrue();
      assertThat(((DatabaseInternal) database).getTransaction())
          .as("the caller's transaction must not have been replaced by one the prune began").isSameAs(callerTx);
      assertThat(callerTx.getRecordFromCache(markerRID))
          .as("the write the caller made before the iteration must still be in its transaction").isNotNull();

      database.commit();

      database.transaction(() -> {
        assertThat(database.existsRecord(markerRID)).as("the caller's write must survive its own commit").isTrue();
        // The documented cost of skipping: the ghost is left for a pass that owns its transaction, or for
        // CHECK DATABASE. Losing the caller's writes to clean it up was never a trade worth making.
        assertThat(database.existsRecord(ghost)).as("the ghost edge record is still gone").isFalse();
      });
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }
  }

  /**
   * The other half of the guard, on the classic layout where nothing raises and the prune would have succeeded:
   * a repair must not be smuggled into a transaction the caller owns even when it works.
   * <p>
   * It is a WRITE performed from inside a read, so it lands in the caller's transaction and commits or rolls back
   * with it, it dirties an edge-list page a read-only transaction never touched (the conflict unit is the page,
   * so that page can now cost the caller a {@code ConcurrentModificationException} at commit), and under HA it
   * rides along in the caller's replicated transaction. Leaving the ghost alone is the whole point of the guard,
   * so pin it: with the guard removed the reference is pruned and this commit takes the repair with it.
   */
  @Test
  void thePruneIsNotSmuggledIntoATransactionTheCallerOwns() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 3);
    ghostEdgeRecord(edges.getFirst());

    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    database.begin();

    int seen = 0;
    for (final Edge ignored : hubRID.asVertex().getEdges(Vertex.DIRECTION.IN, "LINK"))
      ++seen;
    assertThat(seen).as("the ghost is skipped, the two healthy edges are yielded").isEqualTo(2);

    database.commit();

    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    database.transaction(() -> assertThat(inChunkEntries(hubRID))
        .as("the reference to the ghost must still be in the edge list: the caller's transaction is not the "
            + "place to repair the graph")
        .contains(edges.getFirst()));
  }

  private void assertPromotedToSuperNode(final RID hubRID) {
    database.transaction(() -> assertThat(
        database.lookupByRID(((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk(), true))
        .as("the hub must have been promoted to a super-node, otherwise the removal walk is not strict")
        .isInstanceOf(StripeDirectory.class));
  }

  /** Deletes the head record of the first non-null stripe chain, newest generation first. */
  private void breakOneStripeChain(final RID hubRID) {
    final RID[] head = new RID[1];
    database.transaction(() -> {
      final StripeDirectory directory = (StripeDirectory) database.lookupByRID(
          ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk(), true);
      for (int g = directory.getGenerationCount() - 1; g >= 0 && head[0] == null; g--)
        for (int s = 0; s < directory.getStripes(g) && head[0] == null; s++)
          head[0] = directory.getHead(g, s);
    });
    assertThat(head[0]).as("the directory must list at least one chain to break").isNotNull();
    deleteRecordAtBucketLevel(head[0]);
  }

  /**
   * Turns the first edge a plain reader still yields into a ghost, so the iterator is guaranteed to meet it. A
   * chain whose head was deleted is skipped by the read path, so an edge picked blindly could sit behind the break
   * and never be reached.
   */
  private RID ghostAnEdgeStillReachableByAReader(final RID hubRID) {
    final RID[] reachable = new RID[1];
    database.transaction(() -> {
      for (final Edge e : hubRID.asVertex().getEdges(Vertex.DIRECTION.IN, "LINK")) {
        reachable[0] = e.getIdentity();
        break;
      }
    });
    assertThat(reachable[0]).as("the readable chains must still yield an edge to turn into a ghost").isNotNull();
    ghostEdgeRecord(reachable[0]);
    return reachable[0];
  }

  /** Removes the edge RECORD only, so the reference to it survives in the vertex's edge list: a ghost edge. */
  private void ghostEdgeRecord(final RID edgeRID) {
    deleteRecordAtBucketLevel(edgeRID);
  }

  private void deleteRecordAtBucketLevel(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
    database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
  }

  /** Every edge RID the hub's IN chunk chain still references, ghosts included. */
  private List<RID> inChunkEntries(final RID hubRID) {
    final List<RID> entries = new ArrayList<>();
    final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
        .getEdgeHeadChunk((VertexInternal) hubRID.asVertex(), Vertex.DIRECTION.IN);
    assertThat(list).as("the hub must still have an IN edge list").isNotNull();
    list.entryIterator("LINK").forEachRemaining(entry -> entries.add(entry.getFirst()));
    return entries;
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 1);
      database.getSchema().createEdgeType("LINK", 1);
    });
  }

  private RID createHub() {
    final MutableVertex[] holder = new MutableVertex[1];
    database.transaction(() -> holder[0] = database.newVertex("Hub").save());
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
}
