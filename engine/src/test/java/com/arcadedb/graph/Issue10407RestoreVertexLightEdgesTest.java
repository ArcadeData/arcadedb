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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10407: {@code RESTORE VERTEX} / {@link GraphEngine#restoreVertexAt} rebuilt the adjacency of the restored vertex from
 * edge RECORDS only, so every LIGHTWEIGHT edge it had was missing afterwards, and the reported counts did not say so.
 * <p>
 * A lightweight edge of a bidirectional type is stored twice, once in each endpoint's list, so the copy in the far
 * endpoint's list is what the restore rebuilds it from. A unidirectional one leaving the restored vertex was stored in
 * its own list only: that one cannot come back, and the restore has to say which types may have lost edges that way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10407RestoreVertexLightEdgesTest extends TestHelper {
  private static final String V = "V10407";
  private static final String E = "E10407";
  private static final String L = "L10407";
  /** LIGHTWEIGHT and UNIDIRECTIONAL. */
  private static final String U = "U10407";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(V);
      database.getSchema().createEdgeType(E);
      database.getSchema().buildEdgeType().withName(L).withLightweight(true).create();
      database.getSchema().buildEdgeType().withName(U).withLightweight(true).withBidirectional(false).create();
    });
  }

  @Test
  void restoreVertexAtRebuildsTheLightweightEdgesFromTheFarEndpoints() {
    final RID[] graph = createHub();
    final RID hub = graph[0];

    deleteAtLowLevel(hub);
    database.transaction(() -> ((DatabaseInternal) database).getGraphEngine().restoreVertexAt(hub, V));

    assertRestoredAdjacency(graph);
  }

  @Test
  void restoreVertexStatementRebuildsAndCountsTheLightweightEdges() {
    final RID[] graph = createHub();
    final RID hub = graph[0];

    deleteAtLowLevel(hub);

    final Result[] row = new Result[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "RESTORE VERTEX " + V + " RID " + hub)) {
        row[0] = rs.next();
      }
    });

    // 2 regular + 3 lightweight (one of them twice) out, 1 regular + 2 lightweight in
    assertThat(((Number) row[0].getProperty("reconnectedOutEdges")).longValue()).isEqualTo(6L);
    assertThat(((Number) row[0].getProperty("reconnectedInEdges")).longValue()).isEqualTo(3L);
    assertThat(((Number) row[0].getProperty("reconnectedLightweightOutEdges")).longValue()).isEqualTo(4L);
    assertThat(((Number) row[0].getProperty("reconnectedLightweightInEdges")).longValue()).isEqualTo(2L);
    // THE UNIDIRECTIONAL LIGHTWEIGHT EDGE THE HUB HAD (hub -> r1 of U) LIVED ONLY IN ITS OWN LIST: THE RESULT NAMES THE TYPE
    assertThat(row[0].<Collection<String>>getProperty("unrecoverableLightweightEdgeTypes")).containsExactly(U);

    assertRestoredAdjacency(graph);
  }

  @Test
  void aVertexWithoutLightweightEdgesIsRestoredAsBefore() {
    final RID[] hub = new RID[1];
    final RID[] leaf = new RID[1];
    database.transaction(() -> {
      final MutableVertex h = database.newVertex(V).save();
      hub[0] = h.getIdentity();
      leaf[0] = database.newVertex(V).save().getIdentity();
      h.newEdge(E, leaf[0]);
    });

    deleteAtLowLevel(hub[0]);
    database.transaction(() -> ((DatabaseInternal) database).getGraphEngine().restoreVertexAt(hub[0], V));

    assertThat(neighbours(hub[0], Vertex.DIRECTION.OUT, E)).containsExactly(leaf[0]);
    assertThat(neighbours(hub[0], Vertex.DIRECTION.OUT, L)).isEmpty();
    assertThat(neighbours(hub[0], Vertex.DIRECTION.IN, L)).isEmpty();
    assertCleanAfterwards();
  }

  /**
   * A lightweight self-loop is stored in the vertex's own two lists only, and both were lost with the vertex: nothing holds
   * a copy, so it is not rebuilt (and nothing is invented), while every other edge of the vertex is.
   */
  @Test
  void aLightweightSelfLoopOfTheRestoredVertexHasNoCopyLeft() {
    final RID[] hub = new RID[1];
    final RID[] leaf = new RID[1];
    database.transaction(() -> {
      final MutableVertex h = database.newVertex(V).save();
      hub[0] = h.getIdentity();
      leaf[0] = database.newVertex(V).save().getIdentity();
      h.newEdge(L, h);
      h.newEdge(L, leaf[0]);
    });

    deleteAtLowLevel(hub[0]);
    database.transaction(() -> ((DatabaseInternal) database).getGraphEngine().restoreVertexAt(hub[0], V));

    assertThat(neighbours(hub[0], Vertex.DIRECTION.OUT, L)).containsExactly(leaf[0]);
    assertThat(neighbours(hub[0], Vertex.DIRECTION.IN, L)).isEmpty();
    assertCleanAfterwards();
  }

  // ------------------------------------------------------------------------------------------------------------------

  /**
   * The hub and 9 neighbours r1..r9: lightweight L edges to r1 (twice: the type is not unique, so both are edges), r2 and
   * r3, and from r4 and r5; unidirectional lightweight U edges to r1 and from r6; regular E edges to r7 and r8, and from r9.
   *
   * @return the hub at index 0, then r1..r9
   */
  private RID[] createHub() {
    final RID[] r = new RID[10];
    database.transaction(() -> {
      final MutableVertex h = database.newVertex(V).set("name", "hub").save();
      r[0] = h.getIdentity();
      for (int i = 1; i <= 9; i++)
        r[i] = database.newVertex(V).set("i", i).save().getIdentity();

      h.newEdge(E, r[7]);
      h.newEdge(E, r[8]);
      r[9].asVertex().newEdge(E, h);

      h.newEdge(L, r[1]);
      h.newEdge(L, r[1]);
      h.newEdge(L, r[2]);
      h.newEdge(L, r[3]);
      r[4].asVertex().newEdge(L, h);
      r[5].asVertex().newEdge(L, h);

      h.newEdge(U, r[1]);
      r[6].asVertex().newEdge(U, h);
    });
    return r;
  }

  private void assertRestoredAdjacency(final RID[] r) {
    final RID hub = r[0];
    assertThat(neighbours(hub, Vertex.DIRECTION.OUT, E)).containsExactlyInAnyOrder(r[7], r[8]);
    assertThat(neighbours(hub, Vertex.DIRECTION.IN, E)).containsExactly(r[9]);

    assertThat(neighbours(hub, Vertex.DIRECTION.OUT, L)).containsExactlyInAnyOrder(r[1], r[1], r[2], r[3]);
    assertThat(neighbours(hub, Vertex.DIRECTION.IN, L)).containsExactlyInAnyOrder(r[4], r[5]);

    // THE UNIDIRECTIONAL hub -> r1 HAD NO OTHER COPY; r6 -> hub NEVER NEEDED ONE ON THE HUB'S SIDE AND IS STILL THERE
    assertThat(neighbours(hub, Vertex.DIRECTION.OUT, U)).isEmpty();
    assertThat(neighbours(r[6], Vertex.DIRECTION.OUT, U)).containsExactly(hub);

    // THE FAR ENDPOINTS' OWN LISTS ARE UNTOUCHED
    assertThat(neighbours(r[1], Vertex.DIRECTION.IN, L)).containsExactly(hub, hub);
    assertThat(neighbours(r[4], Vertex.DIRECTION.OUT, L)).containsExactly(hub);

    // EVERY REBUILT LIGHTWEIGHT EDGE IS A REAL ONE: TRAVERSED FROM THE HUB IT NAMES THE HUB AND THE FAR ENDPOINT
    database.transaction(() -> {
      for (final Edge e : hub.asVertex(true).getEdges(Vertex.DIRECTION.OUT, L))
        assertThat(e.getOut()).isEqualTo(hub);
      for (final Edge e : hub.asVertex(true).getEdges(Vertex.DIRECTION.IN, L))
        assertThat(e.getIn()).isEqualTo(hub);
    });
    assertCleanAfterwards();
  }

  private void deleteAtLowLevel(final RID rid) {
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, rid));
  }

  private List<RID> neighbours(final RID vertex, final Vertex.DIRECTION direction, final String type) {
    final List<RID> result = new ArrayList<>();
    database.transaction(() -> {
      for (final Vertex v : vertex.asVertex(true).getVertices(direction, type))
        result.add(v.getIdentity());
    });
    return result;
  }

  private void assertCleanAfterwards() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result row = rs.next();
      final Number errors = row.getProperty("totalErrors");
      assertThat(errors == null ? 0L : errors.longValue()).as("warnings: " + row.getProperty("warnings")).isEqualTo(0L);
    }
  }
}
