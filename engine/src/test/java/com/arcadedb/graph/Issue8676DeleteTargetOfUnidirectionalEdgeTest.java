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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8676: deleting the TARGET vertex of a unidirectional edge walked only the target's own lists, which hold no
 * trace of the edge, so the edge record survived and the source kept a pointer to a deleted vertex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8676DeleteTargetOfUnidirectionalEdgeTest extends TestHelper {
  private RID a;
  private RID b;

  @Test
  void cypherDetachDeleteOfTheTargetRemovesTheEdge() {
    createSchema(false);
    createPair("U8");
    database.transaction(() -> database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DETACH DELETE b").close());
    assertEdgeGone("U8");
  }

  @Test
  void vertexDeleteOfTheTargetRemovesTheEdge() {
    createSchema(false);
    createPair("U8");
    database.transaction(() -> b.asVertex().delete());
    assertEdgeGone("U8");
  }

  @Test
  void sqlDeleteVertexOfTheTargetRemovesTheEdge() {
    createSchema(false);
    createPair("U8");
    database.transaction(() -> database.command("sql", "DELETE VERTEX FROM V8 WHERE n = 'b'").close());
    assertEdgeGone("U8");
  }

  @Test
  void lightweightUnidirectionalEdgeIsRemovedToo() {
    createSchema(true);
    createPair("U8");
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isEqualTo(1);
    database.transaction(() -> b.asVertex().delete());
    assertEdgeGone("U8");
  }

  @Test
  void plainCypherDeleteOfAConnectedTargetIsRefused() {
    createSchema(false);
    createPair("U8");
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DELETE b").close()))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("still has relationships");
    assertThat(database.existsRecord(b)).isTrue();
    assertThat(database.countType("U8", false)).isEqualTo(1);
  }

  @Test
  void edgeCreatedInTheSameTransactionIsRemoved() {
    createSchema(false);
    database.transaction(() -> {
      a = database.newVertex("V8").set("n", "a").save().getIdentity();
      b = database.newVertex("V8").set("n", "b").save().getIdentity();
      a.asVertex().newEdge("U8", b);
      b.asVertex().delete();
    });
    assertEdgeGone("U8");
  }

  @Test
  void manyTargetsInOneTransactionShareOneScan() {
    createSchema(false);
    final int n = 50;
    final RID[] targets = new RID[n];
    database.transaction(() -> {
      a = database.newVertex("V8").set("n", "a").save().getIdentity();
      for (int i = 0; i < n; i++) {
        targets[i] = database.newVertex("V8").set("n", "t" + i).save().getIdentity();
        a.asVertex().newEdge("U8", targets[i]);
      }
    });
    final long before = IncomingEdgeLookup.getScansTaken();
    database.transaction(() -> {
      for (final RID target : targets)
        target.asVertex().delete();
    });
    assertThat(IncomingEdgeLookup.getScansTaken() - before).isEqualTo(1L);
    assertThat(database.countType("U8", false)).isZero();
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isZero();
  }

  @Test
  void typeTooLargeToIndexInHeapIsScannedForTheVertexAlone() {
    createSchema(false);
    createPair("U8");
    // A CAP OF ONE ELEMENT IS EXCEEDED BY THE SECOND EDGE OF THE TYPE
    database.transaction(() -> database.newVertex("V8").set("n", "c").save().asVertex().newEdge("U8", b));
    final long cap = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    final long fallbacksBefore = IncomingEdgeLookup.getFallbackScans();
    try {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(1L);
      database.transaction(() -> b.asVertex().delete());
      assertThat(IncomingEdgeLookup.getFallbackScans() - fallbacksBefore).as("the streaming scan answered").isEqualTo(1L);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(cap);
    }
    assertThat(database.countType("U8", false)).isZero();
  }

  @Test
  void aTypeTooLargeToIndexIsNotTriedAgainByTheNextDeletesOfTheTransaction() {
    createSchema(false);
    final int n = 5;
    final RID[] targets = new RID[n];
    database.transaction(() -> {
      a = database.newVertex("V8").set("n", "a").save().getIdentity();
      for (int i = 0; i < n; i++) {
        targets[i] = database.newVertex("V8").set("n", "t" + i).save().getIdentity();
        a.asVertex().newEdge("U8", targets[i]);
      }
    });
    final long cap = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    final long scansBefore = IncomingEdgeLookup.getScansTaken();
    final long fallbacksBefore = IncomingEdgeLookup.getFallbackScans();
    GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(1L);
    try {
      database.transaction(() -> {
        for (final RID target : targets)
          target.asVertex().delete();
      });
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(cap);
    }
    assertThat(IncomingEdgeLookup.getScansTaken() - scansBefore).as("one failed attempt to index, then none").isEqualTo(1L);
    assertThat(IncomingEdgeLookup.getFallbackScans() - fallbacksBefore).isEqualTo(n);
    assertThat(database.countType("U8", false)).isZero();
  }

  @Test
  void cypherDetachDeleteCountsTheIncomingEdgeOfAUnidirectionalType() {
    createSchema(false);
    createPair("U8");
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "MATCH (b:V8 {n: 'b'}) DETACH DELETE b");
      while (rs.hasNext())
        rs.next();
      final QueryStatistics stats = rs.getStatistics().orElseThrow();
      assertThat(stats.getNodesDeleted()).isEqualTo(1);
      assertThat(stats.getRelationshipsDeleted()).isEqualTo(1);
    });
    assertEdgeGone("U8");
  }

  @Test
  void aRegularAndALightweightUnidirectionalTypeAreBothCleaned() {
    createSchema(false);
    database.getSchema().buildEdgeType().withName("L8").withBidirectional(false).withLightweight(true).create();
    createPair("U8");
    database.transaction(() -> a.asVertex().newEdge("L8", b));
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "L8")).isEqualTo(1);

    database.transaction(() -> b.asVertex().delete());
    assertEdgeGone("U8");
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "L8")).isZero();
  }

  @Test
  void anEdgeDeletedEarlierInTheTransactionIsNotDeletedAgain() {
    createSchema(false);
    final RID[] c = new RID[1];
    createPair("U8");
    database.transaction(() -> {
      c[0] = database.newVertex("V8").set("n", "c").save().getIdentity();
      a.asVertex().newEdge("U8", c[0]);
    });
    database.transaction(() -> {
      // THE FIRST DELETE TAKES THE SCAN, THEN THE EDGE INTO c IS DELETED BY HAND, THEN c ITSELF
      b.asVertex().delete();
      a.asVertex().getEdges(Vertex.DIRECTION.OUT, "U8").forEach(Edge::delete);
      c[0].asVertex().delete();
    });
    assertThat(database.countType("U8", false)).isZero();
  }

  @Test
  void theIncomingEdgesAreFoundOutsideATransactionToo() {
    createSchema(false);
    createPair("U8");
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(IncomingEdgeLookup.getIncomingUnidirectionalEdges((DatabaseInternal) database, b)).hasSize(1);
  }

  @Test
  void movingTheTargetKeepsItsIncomingUnidirectionalEdges() {
    createSchema(false);
    database.getSchema().createVertexType("V8Other");
    createPair("U8");
    database.transaction(() -> a.asVertex().newEdge("U8", b).set("w", 7).save());

    database.transaction(() -> database.command("sql", "MOVE VERTEX " + b + " TO TYPE:V8Other").close());
    final RID[] moved = new RID[] { database.iterateType("V8Other", false).next().getIdentity() };

    assertThat(database.countType("U8", false)).isEqualTo(2);
    assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, "U8")).extracting(v -> v.getIdentity()).containsOnly(moved[0]);
    assertThat(IncomingEdgeLookup.getIncomingUnidirectionalEdges((DatabaseInternal) database, moved[0])).hasSize(2);
  }

  @Test
  void otherEdgesOfTheSourceSurvive() {
    createSchema(false);
    createPair("U8");
    final RID[] c = new RID[1];
    database.transaction(() -> {
      c[0] = database.newVertex("V8").set("n", "c").save().getIdentity();
      a.asVertex().newEdge("U8", c[0]);
    });
    database.transaction(() -> b.asVertex().delete());
    assertThat(database.countType("U8", false)).isEqualTo(1);
    assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, "U8")).extracting(v -> v.getIdentity()).containsExactly(c[0]);
  }

  @Test
  void selfLoopOfAUnidirectionalTypeIsRemoved() {
    createSchema(false);
    database.transaction(() -> {
      a = database.newVertex("V8").set("n", "a").save().getIdentity();
      a.asVertex().newEdge("U8", a);
    });
    database.transaction(() -> a.asVertex().delete());
    assertThat(database.countType("U8", false)).isZero();
  }

  @Test
  void lightweightSelfLoopIsRemoved() {
    createSchema(true);
    database.transaction(() -> {
      a = database.newVertex("V8").set("n", "a").save().getIdentity();
      a.asVertex().newEdge("U8", a);
    });
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isEqualTo(1);
    database.transaction(() -> a.asVertex().delete());
    assertThat(database.existsRecord(a)).isFalse();
  }

  @Test
  void everySourceOfATargetIsCleaned() {
    createSchema(false);
    createPair("U8");
    final RID[] c = new RID[1];
    database.transaction(() -> {
      c[0] = database.newVertex("V8").set("n", "c").save().getIdentity();
      c[0].asVertex().newEdge("U8", b);
    });
    database.transaction(() -> b.asVertex().delete());
    assertThat(database.countType("U8", false)).isZero();
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isZero();
    assertThat(c[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "U8")).isZero();
  }

  @Test
  void aRolledBackDeleteLeavesNoStaleScanForTheNextTransaction() {
    createSchema(false);
    createPair("U8");
    database.begin();
    b.asVertex().delete();
    database.rollback();
    assertThat(database.countType("U8", false)).isEqualTo(1);

    database.transaction(() -> b.asVertex().delete());
    assertEdgeGone("U8");
  }

  @Test
  void bidirectionalTypeIsUnchanged() {
    createSchema(false);
    createPair("B8");
    database.transaction(() -> b.asVertex().delete());
    assertEdgeGone("B8");
  }

  private void createSchema(final boolean lightweight) {
    database.getSchema().createVertexType("V8");
    database.getSchema().buildEdgeType().withName("U8").withBidirectional(false).withLightweight(lightweight).create();
    database.getSchema().createEdgeType("B8");
  }

  private void createPair(final String edgeType) {
    database.transaction(() -> {
      a = database.newVertex("V8").set("n", "a").save().getIdentity();
      b = database.newVertex("V8").set("n", "b").save().getIdentity();
      a.asVertex().newEdge(edgeType, b);
    });
  }

  private void assertEdgeGone(final String edgeType) {
    assertThat(database.countType(edgeType, false)).isZero();
    assertThat(a.asVertex().getVertices(Vertex.DIRECTION.OUT, edgeType)).isEmpty();
    assertThat(database.existsRecord(b)).isFalse();
  }
}
