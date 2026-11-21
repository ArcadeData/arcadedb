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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12249: after {@code DROP TYPE <vertex type> UNSAFE} the surviving vertices kept the
 * LIGHTWEIGHT edge entries that named the deleted vertices. CHECK DATABASE reported only the record-backed edges that
 * pointed at the missing vertex, and CHECK DATABASE FIX left the lightweight entries in place, so SQL traversals kept
 * returning edges to a vertex that does not exist while the next CHECK DATABASE was clean.
 */
class CheckDatabaseLightEdgeMissingEndpointIssue12249Test extends TestHelper {
  private RID a;

  private void createGraph(final int bVertices) {
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createEdgeType("F");
    database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
    database.getSchema().buildEdgeType().withName("LU").withLightweight(true).withBidirectional(false).create();
    database.transaction(() -> {
      final MutableVertex va = database.newVertex("A").save();
      final MutableVertex keep = database.newVertex("A").save();
      a = va.getIdentity();
      for (int i = 0; i < bVertices; i++) {
        final MutableVertex b = database.newVertex("B").save();
        va.newEdge("F", b);
        b.newEdge("F", va);
        va.newEdge("L", b);
        b.newEdge("L", va);
        va.newEdge("LU", b);
      }
      // A healthy lightweight edge between two surviving vertices: must survive the repair
      va.newEdge("L", keep);
    });
  }

  private Result check(final boolean fix) {
    try (final ResultSet rs = database.command("sql", fix ? "CHECK DATABASE FIX" : "CHECK DATABASE")) {
      return rs.next();
    }
  }

  private List<RID> neighbours(final Vertex.DIRECTION direction, final String type) {
    final List<RID> list = new ArrayList<>();
    for (final Edge e : a.asVertex().getEdges(direction, type))
      list.add(direction == Vertex.DIRECTION.OUT ? e.getIn() : e.getOut());
    return list;
  }

  private void dropAndRepair(final int bVertices) {
    createGraph(bVertices);
    database.command("sql", "DROP TYPE B UNSAFE");
    assertThat(neighbours(Vertex.DIRECTION.OUT, "L")).hasSize(bVertices + 1);

    final Result before = check(false);
    final Collection<String> warnings = before.getProperty("warnings");
    assertThat(warnings).anyMatch(w -> w.contains("lightweight edge") && w.contains("not found"));
    // per B vertex: F out + F in (record edges) and L out + L in + LU out (lightweight entries) on a
    assertThat(before.<Long>getProperty("invalidLinks")).isGreaterThanOrEqualTo(5L * bVertices);

    final Result fix = check(true);
    assertThat(fix.<Long>getProperty("prunedDanglingEntries")).isGreaterThanOrEqualTo(3L * bVertices);

    final Result after = check(false);
    assertThat(after.<Long>getProperty("totalWarnings")).isZero();
    assertThat(after.<Long>getProperty("totalErrors")).isZero();

    assertThat(neighbours(Vertex.DIRECTION.OUT, "L")).hasSize(1);
    assertThat(neighbours(Vertex.DIRECTION.IN, "L")).isEmpty();
    assertThat(neighbours(Vertex.DIRECTION.OUT, "LU")).isEmpty();
    assertThat(neighbours(Vertex.DIRECTION.OUT, "F")).isEmpty();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM L")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(1L);
    }
  }

  @Test
  void lightEdgesToADroppedVertexTypeAreReportedAndPruned() {
    dropAndRepair(1);
  }

  @Test
  void lightEdgesOfAPromotedSuperNodeAreReportedAndPruned() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 16);
    dropAndRepair(40);
  }

  @Test
  void healthyLightEdgesAreNotReported() {
    createGraph(3);
    final Result result = check(false);
    assertThat(result.<Long>getProperty("totalWarnings")).isZero();
    assertThat(result.<Long>getProperty("invalidLinks")).isZero();
  }
}
