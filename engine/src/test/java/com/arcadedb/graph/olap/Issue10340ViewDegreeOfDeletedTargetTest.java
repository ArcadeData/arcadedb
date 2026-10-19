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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10340: {@code algo.degree()} on a Graph Analytical View answered a negative out-degree for the source of an edge
 * created in the transaction that deleted its target, when the target had been created after the view was built. The edge was
 * never added to the overlay (its target had left the overflow by the time the added edges were resolved), but its cascaded
 * deletion was then recorded against the base CSR as the deletion of an edge the overlay holds, and the degree read off the
 * view subtracted an edge it had never counted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10340ViewDegreeOfDeletedTargetTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE EDGE TYPE L");
  }

  /** The reproduction of the issue: the view on the empty database, so every vertex is an overflow node. */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void sourceAndTargetOnlyInTheOverlay(final String mode) {
    final GraphAnalyticalView view = createView(mode);

    final MutableVertex[] v = new MutableVertex[4];
    database.transaction(() -> {
      v[0] = database.newVertex("P").set("id", 3L).save();
      v[3] = database.newVertex("P").set("id", 2L).save();
    });
    database.transaction(() -> v[1] = database.newVertex("P").set("w", 0.2).save());
    database.transaction(() -> {
      v[0].newEdge("L", v[3]);
      v[2] = database.newVertex("Q").set("id", 5L).save();
    });
    database.transaction(() -> {
      v[1].newEdge("L", v[2]);
      v[2].delete();
    });
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void sourceInTheBaseTargetOnlyInTheOverlay(final String mode) {
    final RID[] source = new RID[1];
    database.transaction(() -> source[0] = database.newVertex("P").set("id", 1L).save().getIdentity());
    final GraphAnalyticalView view = createView(mode);

    database.transaction(() -> {
      final MutableVertex target = database.newVertex("Q").set("id", 2L).save();
      source[0].asVertex().modify().newEdge("L", target);
      target.delete();
    });
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 0);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void sourceOnlyInTheOverlayTargetInTheBase(final String mode) {
    final RID[] target = new RID[1];
    database.transaction(() -> target[0] = database.newVertex("Q").set("id", 2L).save().getIdentity());
    final GraphAnalyticalView view = createView(mode);

    database.transaction(() -> {
      final MutableVertex source = database.newVertex("P").set("id", 1L).save();
      source.newEdge("L", target[0]);
      target[0].asVertex().delete();
    });
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 0);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void sourceAndTargetInTheBase(final String mode) {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("P").set("id", 1L).save().getIdentity();
      rids[1] = database.newVertex("Q").set("id", 2L).save().getIdentity();
    });
    final GraphAnalyticalView view = createView(mode);

    database.transaction(() -> {
      rids[0].asVertex().modify().newEdge("L", rids[1]);
      rids[1].asVertex().delete();
    });
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 0);
  }

  /** A self-loop is an edge of an overflow vertex at both ends: it goes with the vertex, and so does the edge to its neighbour. */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aSelfLoopOnAnOverflowVertexDeletedInTheSameTransaction(final String mode) {
    final GraphAnalyticalView view = createView(mode);

    final MutableVertex[] v = new MutableVertex[2];
    database.transaction(() -> {
      v[0] = database.newVertex("P").set("id", 1L).save();
      v[1] = database.newVertex("P").set("id", 2L).save();
    });
    database.transaction(() -> {
      v[0].newEdge("L", v[0]);
      v[0].newEdge("L", v[1]);
      v[0].delete();
    });
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 0);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aSelfLoopOnAnOverflowVertexIsCountedOnceAndLeavesWithIt(final String mode) {
    final GraphAnalyticalView view = createView(mode);

    final MutableVertex[] v = new MutableVertex[1];
    database.transaction(() -> v[0] = database.newVertex("P").set("id", 1L).save());
    database.transaction(() -> v[0].newEdge("L", v[0]));
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 1);

    database.transaction(() -> v[0].delete());
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 0);
  }

  /** The edges of the overlay deleted on their own, in a later transaction than the one that created them, are withdrawn. */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void anOverlayEdgeDeletedInALaterTransactionIsWithdrawn(final String mode) {
    final RID[] base = new RID[1];
    database.transaction(() -> base[0] = database.newVertex("P").set("id", 1L).save().getIdentity());
    final GraphAnalyticalView view = createView(mode);

    final RID[] edges = new RID[2];
    database.transaction(() -> {
      final MutableVertex b = database.newVertex("Q").set("id", 2L).save();
      final MutableVertex c = database.newVertex("Q").set("id", 3L).save();
      // a base source to an overflow target, and two overflow vertices
      edges[0] = base[0].asVertex().modify().newEdge("L", b).save().getIdentity();
      edges[1] = b.newEdge("L", c).save().getIdentity();
    });
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 2);

    database.transaction(() -> edges[0].asEdge().delete());
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 1);

    database.transaction(() -> edges[1].asEdge().delete());
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 0);
  }

  /** A target that survives keeps the edge, whichever side of the build it was created on. */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aSurvivingTargetKeepsTheEdge(final String mode) {
    final GraphAnalyticalView view = createView(mode);

    final MutableVertex[] v = new MutableVertex[2];
    database.transaction(() -> {
      v[0] = database.newVertex("P").set("id", 1L).save();
      v[1] = database.newVertex("Q").set("id", 2L).save();
    });
    database.transaction(() -> v[0].newEdge("L", v[1]));
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 1);

    database.transaction(() -> v[1].delete());
    awaitMerged(view);

    assertDegreesMatchTheRecords(view, 0);
  }

  /** Every degree the view answers is the one the records give, and the view counts the edges that are left. */
  private void assertDegreesMatchTheRecords(final GraphAnalyticalView view, final int expectedEdges) {
    int nodes = 0;
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.degree() YIELD node, inDegree, outDegree, degree RETURN node AS n, inDegree AS i, outDegree AS o, degree AS d")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Vertex vertex = r.<Identifiable>getProperty("n").asVertex();
        final long out = vertex.countEdges(Vertex.DIRECTION.OUT, "L");
        final long in = vertex.countEdges(Vertex.DIRECTION.IN, "L");
        assertThat(r.<Number>getProperty("o").longValue()).as("out-degree of " + vertex.getIdentity()).isEqualTo(out);
        assertThat(r.<Number>getProperty("i").longValue()).as("in-degree of " + vertex.getIdentity()).isEqualTo(in);
        assertThat(r.<Number>getProperty("d").longValue()).as("degree of " + vertex.getIdentity()).isEqualTo(out + in);
        nodes++;
      }
    }
    assertThat(nodes).as("every live vertex is answered").isEqualTo(countVertices());
    assertThat(view.getEdgeCount()).as("the edges the view holds").isEqualTo(expectedEdges);
  }

  private long countVertices() {
    long count = 0;
    for (final String type : new String[] { "P", "Q" })
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
        count += rs.next().<Number>getProperty("c").longValue();
      }
    return count;
  }

  private GraphAnalyticalView createView(final String mode) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P, Q) EDGE TYPES (L) PROPERTIES (id) UPDATE MODE " + mode)
        .close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  private void awaitMerged(final GraphAnalyticalView view) {
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
  }
}
