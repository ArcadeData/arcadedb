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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.EdgeType;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #10302, the lightweight-edge half of #10263: a transaction deleting a lightweight edge that a
 * concurrent commit appended to an edge list after the transaction had taken its own copy of that list found nothing to
 * remove and committed anyway, so the delete was lost (one lightweight edge too many).
 * <p>
 * A lightweight edge has no RID, and its twins (same type, same neighbour) are indistinguishable, so the check counts:
 * occurrences in the committed list against those this transaction removed itself.
 */
class Issue10302ConcurrentLightEdgeDeleteTest extends TestHelper {

  /**
   * The transaction appended to the OUT list of v0 and the IN list of v1 (its own copies of their head chunks) before a
   * concurrent commit appended a lightweight edge v0->v1 to the same chunks. The delete finds nothing in the lists it
   * reads: it must be refused, not committed as a no-op.
   */
  @Test
  void deletingALightEdgeAddedAfterTheListWasWrittenIsAConflict() throws Exception {
    final RID[] v = createVertices(2);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      v[0].asVertex().newEdge("F", v[1].asVertex());

      final Edge added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

      assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
  }

  /**
   * Only the IN side is stale: the OUT list of v0 is read fresh and its entry removed, the IN list of v1 is this
   * transaction's own older copy and holds none. The IN side alone must refuse the delete.
   */
  @Test
  void aStaleIncomingListAloneIsAConflict() throws Exception {
    final RID[] v = createVertices(3);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      // ONLY v1's IN LIST BECOMES THIS TRANSACTION'S OWN COPY
      v[2].asVertex().newEdge("F", v[1].asVertex());

      final Edge added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

      assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
  }

  /** The same window on a super-node, whose list is striped: the committed occurrences are counted along the stripes. */
  @Test
  void deletingALightEdgeAddedAfterAStripedListWasWrittenIsAConflict() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });
    assertThat(database.lookupByRID(((VertexInternal) v[0].asVertex()).getOutEdgesHeadChunk(), true))
        .isInstanceOf(StripeDirectory.class);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    final Edge added;
    try {
      database.begin();
      // SAME NEIGHBOUR, SO THE SAME STRIPE CHAIN
      v[0].asVertex().newEdge("F", v[1].asVertex());
      // THIS TRANSACTION REMOVES ALL 40 TWINS IT CAN SEE: THE 41ST, COMMITTED CONCURRENTLY, IS NOT IN ITS VIEW
      for (int i = 0; i < 40; i++)
        lightEdge(v[0], v[1]).delete();

      added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

      assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(41);
  }

  /**
   * No false conflict: deleting more twins than the committed list holds, with nothing committed concurrently, is a
   * no-op for the extra deletes, as before. The committed list still holds the twins this transaction removed itself.
   */
  @Test
  void deletingMoreTwinsThanCommittedIsNotAConflict() {
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });

    database.transaction(() -> {
      final Edge twin = lightEdge(v[0], v[1]);
      for (int i = 0; i < 5; i++)
        twin.delete();
    });

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
  }

  /** The same on a striped list, where the twins live in one stripe chain per generation. */
  @Test
  void deletingAllTwinsOfAStripedListIsNotAConflict() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });

    database.transaction(() -> {
      final Edge twin = lightEdge(v[0], v[1]);
      for (int i = 0; i < 41; i++)
        twin.delete();
    });

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
  }

  /**
   * The edge-list iterator removes the entry it stands on itself (the delete skips that list, #9779): that removal is
   * this transaction's too, so a later delete of a twin through the same list must not take the committed twin it
   * removed for one it never saw.
   */
  @Test
  void aTwinRemovedThroughTheIteratorCountsAsRemovedByTheTransaction() {
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });

    database.transaction(() -> {
      final Iterator<Edge> it = v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "L").iterator();
      it.next();
      it.remove();
      final Edge twin = lightEdge(v[0], v[1]);
      twin.delete();
      // NOTHING LEFT: THE COMMITTED LIST HOLDS 2, BOTH REMOVED BY THIS TRANSACTION
      twin.delete();
    });

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
  }

  /**
   * The removals are counted per transaction: those of a committed transaction must not carry over to the next one on
   * the same context, where they would hide its stale view.
   */
  @Test
  void removalsOfAnEndedTransactionDoNotHideAStaleView() throws Exception {
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });
    // TWO REMOVALS COUNTED FOR (v0, OUT, L, v1), THEN THE TRANSACTION COMMITS
    database.transaction(() -> {
      final Edge twin = lightEdge(v[0], v[1]);
      twin.delete();
      twin.delete();
    });

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      v[0].asVertex().newEdge("F", v[1].asVertex());

      final Edge added = addLightEdgeInOwnTransaction(other, v[0], v[1]);

      assertThatThrownBy(added::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
  }

  private RID[] createVertices(final int count) {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT").close();
    // A REGULAR TYPE: AN EDGE OF IT MAKES THE TRANSACTION TAKE ITS OWN COPY OF A LIST WITHOUT TOUCHING THE L ENTRIES
    database.command("sql", "CREATE EDGE TYPE F").close();
    final RID[] vertices = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        vertices[i] = database.newVertex("V").set("id", i).save().getIdentity();
    });
    return vertices;
  }

  private Edge addLightEdgeInOwnTransaction(final ExecutorService other, final RID from, final RID to) throws Exception {
    return other.submit(() -> {
      final Edge[] added = new Edge[1];
      database.transaction(() -> added[0] = from.asVertex().newEdge("L", to.asVertex()));
      assertThat(added[0]).isInstanceOf(LightEdge.class);
      return added[0];
    }).get();
  }

  private Edge lightEdge(final RID from, final RID to) {
    final EdgeType type = (EdgeType) database.getSchema().getType("L");
    return new ImmutableLightEdge(database, type, type.getFirstBucketId(), from, to);
  }

  private long countLightEdges(final RID vertex, final Vertex.DIRECTION direction) {
    final long[] count = new long[1];
    database.transaction(() -> count[0] = vertex.asVertex().countEdges(direction, "L"));
    return count[0];
  }
}
