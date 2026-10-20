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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10369: deleting a vertex that carries a LIGHTWEIGHT self-loop reported the self-loop deleted twice, once from each
 * of the vertex's two edge lists. A record-backed edge is reported by its RID, so the overlay of a Graph Analytical View
 * absorbed the second report; a lightweight one has no identity of its own, so the second report spent a second deletion
 * against the base run of the pair and the view's edge count drifted one lower per self-loop, down to negative numbers.
 * The engine now walks a self-loop once: the incoming walk of a deleted vertex skips the self-loops the outgoing walk
 * already deleted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10369ViewSelfLoopOfDeletedVertexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("P");
      database.getSchema().createEdgeType("R");
      database.getSchema().buildEdgeType().withName("W").withLightweight(true).create();
    });
  }

  /** The reproduction of the issue, in every cell of the matrix: edge kind x where the self-loop is x update mode. */
  @ParameterizedTest
  @CsvSource({
      "W, true, SYNCHRONOUS", "W, false, SYNCHRONOUS", "R, true, SYNCHRONOUS", "R, false, SYNCHRONOUS",
      "W, true, ASYNCHRONOUS", "W, false, ASYNCHRONOUS", "R, true, ASYNCHRONOUS", "R, false, ASYNCHRONOUS" })
  void aDeletedVertexTakesItsSelfLoopOnce(final String edgeType, final boolean selfLoopInTheBase, final String mode) {
    final RID[] v = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        v[i] = database.newVertex("P").set("id", i).save().getIdentity();
      // a control edge the delete must not touch
      v[1].asVertex().modify().newEdge(edgeType, v[2]);
    });

    if (selfLoopInTheBase)
      createSelfLoop(v[0], edgeType);
    final GraphAnalyticalView view = createView(edgeType, mode);
    if (!selfLoopInTheBase) {
      createSelfLoop(v[0], edgeType);
      awaitMerged(view);
    }
    assertThat(view.getEdgeCount()).as("the self-loop and the control edge").isEqualTo(2);

    database.transaction(() -> v[0].asVertex().delete());
    awaitMerged(view);

    assertThat(view.getEdgeCount()).as("only the control edge is left").isEqualTo(1);
    assertDegreesMatchTheRecords(view, edgeType);
  }

  /** The drift accumulated one edge per self-loop: five of them took the count of a one-edge graph to -4. */
  @ParameterizedTest
  @CsvSource({ "W, SYNCHRONOUS", "W, ASYNCHRONOUS", "R, SYNCHRONOUS", "R, ASYNCHRONOUS" })
  void manySelfLoopsDoNotDrift(final String edgeType, final String mode) {
    final RID[] v = new RID[7];
    database.transaction(() -> {
      for (int i = 0; i < v.length; i++)
        v[i] = database.newVertex("P").set("id", i).save().getIdentity();
      v[5].asVertex().modify().newEdge(edgeType, v[6]);
      for (int i = 0; i < 5; i++)
        v[i].asVertex().modify().newEdge(edgeType, v[i]);
    });
    final GraphAnalyticalView view = createView(edgeType, mode);
    assertThat(view.getEdgeCount()).isEqualTo(6);

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        v[i].asVertex().delete();
    });
    awaitMerged(view);

    assertThat(view.getEdgeCount()).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "SELECT edgeCount FROM schema:graphAnalyticalViews WHERE name = 'gav'")) {
      assertThat(rs.next().<Number>getProperty("edgeCount").longValue()).isEqualTo(1L);
    }
    assertDegreesMatchTheRecords(view, edgeType);
  }

  /** A self-loop deleted directly, and one of two parallel self-loops, were already right: they stay so. */
  @ParameterizedTest
  @CsvSource({ "W, SYNCHRONOUS", "R, SYNCHRONOUS", "W, ASYNCHRONOUS", "R, ASYNCHRONOUS" })
  void parallelSelfLoopsLeaveWithTheirVertex(final String edgeType, final String mode) {
    final RID[] v = new RID[2];
    database.transaction(() -> {
      v[0] = database.newVertex("P").set("id", 0).save().getIdentity();
      v[1] = database.newVertex("P").set("id", 1).save().getIdentity();
      final MutableVertex loop = v[0].asVertex().modify();
      loop.newEdge(edgeType, loop);
      loop.newEdge(edgeType, loop);
      loop.newEdge(edgeType, v[1]);
    });
    final GraphAnalyticalView view = createView(edgeType, mode);
    assertThat(view.getEdgeCount()).isEqualTo(3);

    database.transaction(() -> v[0].asVertex().delete());
    awaitMerged(view);

    assertThat(view.getEdgeCount()).isEqualTo(0);
    assertDegreesMatchTheRecords(view, edgeType);
  }

  private void createSelfLoop(final RID vertex, final String edgeType) {
    database.transaction(() -> {
      final MutableVertex loop = vertex.asVertex().modify();
      loop.newEdge(edgeType, loop);
    });
  }

  /** Every live vertex's degrees, read every way the view answers them, are the ones its records give. */
  private void assertDegreesMatchTheRecords(final GraphAnalyticalView view, final String edgeType) {
    final int bound = view.getNodeIdUpperBound();
    final int[] outDegrees = new int[bound];
    final int[] inDegrees = new int[bound];
    view.getDegrees(outDegrees, Vertex.DIRECTION.OUT, edgeType);
    view.getDegrees(inDegrees, Vertex.DIRECTION.IN, edgeType);
    database.transaction(() -> {
      for (int nodeId = 0; nodeId < bound; nodeId++) {
        if (!view.isNodeLive(nodeId)) {
          assertThat(outDegrees[nodeId]).as("out-degree of the deleted node " + nodeId).isZero();
          assertThat(inDegrees[nodeId]).as("in-degree of the deleted node " + nodeId).isZero();
          assertThat(view.countEdges(nodeId, Vertex.DIRECTION.BOTH, edgeType)).as("edges of the deleted node " + nodeId).isZero();
          continue;
        }
        final Vertex vertex = view.getRID(nodeId).asVertex();
        final long out = vertex.countEdges(Vertex.DIRECTION.OUT, edgeType);
        final long in = vertex.countEdges(Vertex.DIRECTION.IN, edgeType);
        assertThat(outDegrees[nodeId]).as("out-degree of " + vertex.getIdentity()).isEqualTo(out);
        assertThat(inDegrees[nodeId]).as("in-degree of " + vertex.getIdentity()).isEqualTo(in);
        assertThat(view.countEdges(nodeId, Vertex.DIRECTION.OUT, edgeType)).isEqualTo(out);
        assertThat(view.countEdges(nodeId, Vertex.DIRECTION.IN, edgeType)).isEqualTo(in);
      }
    });
  }

  private GraphAnalyticalView createView(final String edgeType, final String mode) {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (" + edgeType + ") PROPERTIES (id) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  private void awaitMerged(final GraphAnalyticalView view) {
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }
}
