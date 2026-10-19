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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.EdgeType;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #10315, the residual gap of #10302: the check that refuses the delete of a lightweight edge read
 * through a stale edge list compared the committed twins against the twins this transaction removed, and those include
 * the twins it CREATED and then removed, which were never in the committed list. A transaction that created k twins of
 * a pair and deleted more than k of them, while a concurrent commit appended one more twin, saw
 * {@code committed <= removed} and committed: the delete was lost. The check now nets the creations out:
 * {@code stale <=> committed > removed - created}.
 */
class Issue10315CreatedTwinLightEdgeDeleteTest extends TestHelper {

  /**
   * The transaction creates one twin v0->v1, a concurrent commit appends another one, and the transaction deletes two:
   * the first removes its own twin, the second finds nothing in the list it reads while the committed list holds one
   * twin the transaction never saw. That delete must be refused.
   */
  @Test
  void deletingMoreTwinsThanCreatedWhileOneIsCommittedConcurrentlyIsAConflict() throws Exception {
    final RID[] v = createVertices(2);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      v[0].asVertex().newEdge("L", v[1].asVertex());

      addLightEdgeInOwnTransaction(other, v[0], v[1]);

      final Edge twin = lightEdge(v[0], v[1]);
      // REMOVES THE TWIN THIS TRANSACTION CREATED
      twin.delete();
      // FINDS NOTHING: THE COMMITTED TWIN IS NOT IN THIS TRANSACTION'S VIEW
      assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
  }

  /**
   * Same, with twins already committed before the transaction started: it sees two committed twins and creates two
   * more, a concurrent commit appends a fifth, and deleting five of them must be refused on the fifth.
   */
  @Test
  void createdTwinsOnTopOfCommittedOnesAreNettedOut() throws Exception {
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      for (int i = 0; i < 2; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());

      addLightEdgeInOwnTransaction(other, v[0], v[1]);

      final Edge twin = lightEdge(v[0], v[1]);
      for (int i = 0; i < 4; i++)
        twin.delete();
      assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(3);
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(3);
  }

  /** The same window on a super-node, whose list is striped: the creations are netted out along the stripes too. */
  @Test
  void createdTwinsOnAStripedListAreNettedOut() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });
    assertThat(database.lookupByRID(((VertexInternal) v[0].asVertex()).getOutEdgesHeadChunk(), true))
        .isInstanceOf(StripeDirectory.class);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      for (int i = 0; i < 3; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());

      addLightEdgeInOwnTransaction(other, v[0], v[1]);

      // ONE DELETE CAN REMOVE A TWIN IN EACH GENERATION OF THE STRIPES (#10314): THE 43 TWINS IN VIEW RUN OUT IN AT MOST 43
      // DELETES, AND THE NEXT ONE FINDS NOTHING WHILE THE COMMITTED LIST HOLDS THE 44TH
      final Edge twin = lightEdge(v[0], v[1]);
      assertThatThrownBy(() -> {
        for (int i = 0; i < 44; i++)
          twin.delete();
      }).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(41);
  }

  /** No false conflict on a striped list: deleting every twin, committed and created, with nothing committed concurrently. */
  @Test
  void deletingAllCreatedAndCommittedTwinsOfAStripedListIsNotAConflict() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });

    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
      final Edge twin = lightEdge(v[0], v[1]);
      for (int i = 0; i < 50; i++)
        twin.delete();
    });

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
  }

  /**
   * Only the IN list of v1 is stale: the twins created on it are counted on the IN side too, and the IN side alone must
   * refuse the delete.
   */
  @Test
  void createdTwinsAreNettedOutOnTheIncomingSide() throws Exception {
    final RID[] v = createVertices(3);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      // v2 -> v1: ONLY v1's IN LIST BECOMES THIS TRANSACTION'S OWN COPY, AND IT CREATES ONE TWIN v0 -> v1 ON IT
      v[2].asVertex().newEdge("F", v[1].asVertex());
      v[0].asVertex().newEdge("L", v[1].asVertex());

      addLightEdgeInOwnTransaction(other, v[0], v[1]);

      final Edge twin = lightEdge(v[0], v[1]);
      twin.delete();
      assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isEqualTo(1);
  }

  /** The creations reach the count through newEdges() too, the batch API, not only through newEdge(). */
  @Test
  void twinsCreatedThroughNewEdgesAreNettedOut() throws Exception {
    final RID[] v = createVertices(2);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      ((DatabaseInternal) database).getGraphEngine().newEdges((VertexInternal) v[0].asVertex(),
          List.of(new GraphEngine.CreateEdgeOperation("L", v[1].asVertex(), new Object[0]),
              new GraphEngine.CreateEdgeOperation("L", v[1].asVertex(), new Object[0])), true);

      addLightEdgeInOwnTransaction(other, v[0], v[1]);

      final Edge twin = lightEdge(v[0], v[1]);
      twin.delete();
      twin.delete();
      assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
  }

  /** The creations reach the count through SQL CREATE EDGE too, which wires the edge through ConnectEdgeStep. */
  @Test
  void twinsCreatedThroughSqlAreNettedOut() throws Exception {
    final RID[] v = createVertices(2);

    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      database.begin();
      database.command("sql", "CREATE EDGE L FROM " + v[0] + " TO " + v[1]).close();

      addLightEdgeInOwnTransaction(other, v[0], v[1]);

      final Edge twin = lightEdge(v[0], v[1]);
      twin.delete();
      assertThatThrownBy(twin::delete).isInstanceOf(ConcurrentModificationException.class);
      database.rollback();
    } finally {
      other.shutdownNow();
    }

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isEqualTo(1);
  }

  /**
   * The cost: an append is counted only between two vertices that both predate the transaction. A lightweight edge with
   * an endpoint created in this transaction leaves the count untouched, so a bulk load of new vertices and their edges
   * fills no map; one between two committed vertices is counted on both sides.
   */
  @Test
  void onlyAppendsBetweenCommittedVerticesAreCounted() {
    final RID[] v = createVertices(2);

    database.transaction(() -> {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final EdgeType type = (EdgeType) database.getSchema().getType("L");
      final int bucket = type.getFirstBucketId();

      final Vertex created = database.newVertex("V").save();
      v[0].asVertex().newEdge("L", created);
      created.newEdge("L", v[1].asVertex());
      assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[0], true, bucket, created.getIdentity())))
          .isZero();
      assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[1], false, bucket, created.getIdentity())))
          .isZero();

      v[0].asVertex().newEdge("L", v[1].asVertex());
      v[0].asVertex().newEdge("L", v[1].asVertex());
      assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[0], true, bucket, v[1]))).isEqualTo(2);
      assertThat(tx.getLightEdgeEntriesAppended(new TransactionContext.LightEdgeEntry(v[1], false, bucket, v[0]))).isEqualTo(2);
    });
  }

  /**
   * No false conflict: creating twins and deleting more of them than exist, with nothing committed concurrently, stays
   * the no-op it always was for the extra deletes.
   */
  @Test
  void deletingMoreTwinsThanCreatedAndCommittedIsNotAConflict() {
    final RID[] v = createVertices(2);
    database.transaction(() -> v[0].asVertex().newEdge("L", v[1].asVertex()));

    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
      final Edge twin = lightEdge(v[0], v[1]);
      for (int i = 0; i < 5; i++)
        twin.delete();
    });

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
    assertThat(countLightEdges(v[1], Vertex.DIRECTION.IN)).isZero();
  }

  /**
   * No false conflict on a vertex created in this transaction: nothing committed can be appended to its lists, and
   * deleting more twins than were created on it is a no-op for the extra deletes.
   */
  @Test
  void deletingMoreTwinsThanCreatedOnANewVertexIsNotAConflict() {
    createVertices(0);
    database.transaction(() -> {
      final Vertex from = database.newVertex("V").save();
      final Vertex to = database.newVertex("V").save();
      from.newEdge("L", to);
      from.newEdge("L", to);
      final EdgeType type = (EdgeType) database.getSchema().getType("L");
      final Edge twin = new ImmutableLightEdge(database, type, type.getFirstBucketId(), from.getIdentity(), to.getIdentity());
      for (int i = 0; i < 3; i++)
        twin.delete();
      assertThat(from.asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isZero();
    });
  }

  /**
   * The creations are counted per transaction: those of a committed transaction must not carry over to the next one on
   * the same context, where they would invent a conflict.
   */
  @Test
  void creationsOfAnEndedTransactionDoNotInventAConflict() {
    final RID[] v = createVertices(2);
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        v[0].asVertex().newEdge("L", v[1].asVertex());
    });

    database.transaction(() -> {
      final Edge twin = lightEdge(v[0], v[1]);
      for (int i = 0; i < 4; i++)
        twin.delete();
    });

    assertThat(countLightEdges(v[0], Vertex.DIRECTION.OUT)).isZero();
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

  private void addLightEdgeInOwnTransaction(final ExecutorService other, final RID from, final RID to) throws Exception {
    other.submit(() -> {
      final Edge[] added = new Edge[1];
      database.transaction(() -> added[0] = from.asVertex().newEdge("L", to.asVertex()));
      assertThat(added[0]).isInstanceOf(LightEdge.class);
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
