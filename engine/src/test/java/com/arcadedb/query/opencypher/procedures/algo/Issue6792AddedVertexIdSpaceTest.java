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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The other half of issue #6792: a vertex added to a {@code SYNCHRONOUS} Graph Analytical View takes an ID above
 * the base node mapping, so the view's node count outgrows the arrays the {@code GraphAlgorithms} kernels size
 * from that mapping. Every CSR-accelerated procedure then threw {@code ArrayIndexOutOfBoundsException} on the
 * added vertex's ID - one added vertex was enough, no deletion involved.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6792AddedVertexIdSpaceTest {
  private Database            database;
  private GraphAnalyticalView view;
  private RID                 a;
  private RID                 b;
  private RID                 c;
  private RID                 fresh;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6792-added-vertex");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");

    final MutableVertex[] created = new MutableVertex[3];
    database.transaction(() -> {
      created[0] = database.newVertex("N").set("name", "A").save();
      created[1] = database.newVertex("N").set("name", "B").save();
      created[2] = database.newVertex("N").set("name", "C").save();
      created[0].newEdge("E", created[1]).save();
    });
    a = created[0].getIdentity();
    b = created[1].getIdentity();
    c = created[2].getIdentity();

    view = GraphAnalyticalView.builder(database)
        .withName("issue-6792-added-vertex")
        .withVertexTypes("N")
        .withEdgeTypes("E")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .withCompactionThreshold(Integer.MAX_VALUE)
        .build();

    final MutableVertex[] added = new MutableVertex[1];
    database.transaction(() -> {
      added[0] = database.newVertex("N").set("name", "FRESH").save();
      c.asVertex().modify().newEdge("E", added[0]).save();
    });
    fresh = added[0].getIdentity();
  }

  @AfterEach
  void teardown() {
    if (view != null)
      view.drop();
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void theAddedVertexIsNumberedAboveTheBaseMapping() {
    assertThat(view.hasPendingChanges()).isTrue();
    assertThat(view.getNodeCount()).isEqualTo(4);
    assertThat(view.getNodeIdUpperBound()).isEqualTo(4);
    // No holes here, so the ID space is compact - and still one wider than the base mapping the CSR kernels
    // size their result arrays from.
    assertThat(view.getNodeId(fresh)).isEqualTo(view.getNodeMapping().size());
    assertThat(view.isNodeLive(view.getNodeId(fresh))).isTrue();
  }

  @Test
  void everyWholeGraphProcedureAnswersForTheAddedVertex() {
    final String[][] procedures = {
        { "algo.wcc()", "node" },
        { "algo.pagerank()", "node" },
        { "algo.articlerank()", "node" },
        { "algo.labelpropagation()", "node" },
        { "algo.localClusteringCoefficient()", "node" },
        { "algo.degree()", "node" },
        { "algo.closeness()", "node" },
        { "algo.harmonic(null, 'BOTH', true)", "node" },
        { "algo.eigenvector(null, 'BOTH')", "node" },
        { "algo.betweenness()", "node" },
        { "algo.kcore()", "node" },
        { "algo.scc()", "node" },
        { "algo.triangleCount()", "node" },
        { "algo.katz()", "nodeId" },
    };

    for (final String[] procedure : procedures) {
      final String query = "CALL " + procedure[0] + " YIELD " + procedure[1] + " RETURN " + procedure[1];
      final Set<RID> nodes = new HashSet<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        nodes.add((RID) rs.next().getProperty(procedure[1]));
      assertThat(nodes).as(query).containsExactlyInAnyOrder(a, b, c, fresh);
    }
  }

  @Test
  void wccJoinsTheAddedVertexToTheComponentItsEdgeReaches() {
    final ResultSet rs = database.query("opencypher", "CALL algo.wcc() YIELD node, componentId RETURN node, componentId");
    final Map<RID, Object> components = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      components.put(row.getProperty("node"), row.getProperty("componentId"));
    }
    assertThat(components).hasSize(4);
    assertThat(components.get(a)).isEqualTo(components.get(b));
    // The edge added to the overlay is what joins C to FRESH; a kernel reading only the base CSR would put them
    // in two components of their own.
    assertThat(components.get(c)).isEqualTo(components.get(fresh));
    assertThat(components.get(a)).isNotEqualTo(components.get(c));
  }

  @Test
  void pageRankInDirectionFollowsIncomingEdgesWhileTheViewHasPendingChanges() {
    assertThat(view.hasPendingChanges()).isTrue();

    final Map<RID, Double> scores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.pagerank({direction: 'IN', maxIterations: 20, tolerance: 0.0}) YIELD node, score RETURN node, score")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.put(row.getProperty("node"), ((Number) row.getProperty("score")).doubleValue());
      }
    }

    assertThat(scores).hasSize(4);
    // A -> B and C -> FRESH. Reversing the traversal must therefore rank A above B and C above FRESH.
    assertThat(scores.get(a)).isGreaterThan(scores.get(b));
    assertThat(scores.get(c)).isGreaterThan(scores.get(fresh));
  }
}
