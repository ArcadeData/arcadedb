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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for issue #6792: a Graph Analytical View serving a delta overlay reports a live node count
 * smaller than the exclusive bound of its own node ID space, because the overlay keeps the slot of every deleted
 * node and allocates the ID of every added one above the base mapping.
 * <p>
 * The fixture builds exactly that shape - one base vertex deleted, one vertex added above the base mapping, and
 * an edge reaching it - and asserts that no {@code algo.*} procedure either drops the added vertex or trips over
 * the ID that names it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6792GraphDataNodeIdBoundTest {
  private Database           database;
  private GraphAnalyticalView view;
  private RID                a;
  private RID                b;
  private RID                fresh;
  private RID                deleted;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6792-node-id-bound");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");

    // SPARE first so that it is the one the base mapping is most likely to number lowest; which vertex actually
    // gets which ID is read back below rather than assumed.
    final MutableVertex[] created = new MutableVertex[3];
    database.transaction(() -> {
      created[0] = database.newVertex("N").set("name", "SPARE").save();
      created[1] = database.newVertex("N").set("name", "A").save();
      created[2] = database.newVertex("N").set("name", "B").save();
      created[1].newEdge("E", created[2]).save();
    });

    view = GraphAnalyticalView.builder(database)
        .withName("issue-6792-node-id-bound")
        .withVertexTypes("N")
        .withEdgeTypes("E")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .withCompactionThreshold(Integer.MAX_VALUE)
        .build();

    a = created[1].getIdentity();
    b = created[2].getIdentity();
    deleted = created[0].getIdentity();

    final MutableVertex[] added = new MutableVertex[1];
    database.transaction(() -> {
      deleted.asVertex().delete();
      added[0] = database.newVertex("N").set("name", "FRESH").save();
      b.asVertex().modify().newEdge("E", added[0]).save();
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
  void theViewSeparatesItsLiveNodeCountFromItsNodeIdSpace() {
    assertThat(view.hasPendingChanges()).isTrue();
    assertThat(view.getNodeCount()).isEqualTo(3);
    assertThat(view.getNodeIdUpperBound()).isEqualTo(4);

    final int freshId = view.getNodeId(fresh);
    // The whole of issue #6792 in one assertion: a live node whose ID is not smaller than the live count.
    assertThat(freshId).isGreaterThanOrEqualTo(view.getNodeCount());
    assertThat(freshId).isLessThan(view.getNodeIdUpperBound());

    assertThat(view.isNodeLive(freshId)).isTrue();
    assertThat(view.isNodeLive(view.getNodeId(a))).isTrue();
    assertThat(view.isNodeLive(view.getNodeId(b))).isTrue();
    // The deleted base node keeps its slot, and its RID is still in the base mapping - which is exactly why
    // liveness has to be asked of the view rather than inferred from getRID() answering non-null.
    assertThat(view.isNodeLive(deletedBaseId())).isFalse();

    assertThat(view.getNeighborIds(view.getNodeId(b), Vertex.DIRECTION.OUT, "E")).contains(freshId);
  }

  @Test
  void graphDataHandsTheProceduresACompactNodeIdSpace() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final ProbeAPSP procedure = new ProbeAPSP();
    final AbstractAlgoProcedure.GraphData graph = procedure.loadGraph(database, context);

    // Every node-indexed array a procedure allocates is nodeCount long, so nodeCount has to be both the size of
    // the ID space and the number of live nodes in it. The renumbering is what makes those the same number.
    assertThat(graph.nodeCount).isEqualTo(view.getNodeCount());
    assertThat(graph.isCSRBacked()).isTrue();

    final Set<RID> resolved = new HashSet<>();
    for (int i = 0; i < graph.nodeCount; i++) {
      assertThat(graph.getRID(i)).isNotNull();
      resolved.add(graph.getRID(i));
    }
    assertThat(resolved).containsExactlyInAnyOrder(a, b, fresh);

    final int denseB = graph.indexOf(b);
    final int denseFresh = graph.indexOf(fresh);
    assertThat(denseB).isBetween(0, graph.nodeCount - 1);
    assertThat(denseFresh).isBetween(0, graph.nodeCount - 1);
    assertThat(graph.adjacency(Vertex.DIRECTION.OUT)[denseB]).contains(denseFresh);
    assertThat(graph.degrees(Vertex.DIRECTION.OUT, "E")[denseB]).isEqualTo(1);
  }

  @Test
  void apspReachesTheAddedVertexThroughTheSurvivingOne() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final ProbeAPSP procedure = new ProbeAPSP();
    final List<Result> rows = drain(procedure.execute(new Object[0], null, context));

    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isEqualTo(true);
    assertThat(rows).anySatisfy(row -> {
      assertThat((RID) row.getProperty("source")).isEqualTo(b);
      assertThat((RID) row.getProperty("target")).isEqualTo(fresh);
      assertThat(((Number) row.getProperty("distance")).doubleValue()).isEqualTo(1.0);
    });
    assertThat(rows).anySatisfy(row -> {
      assertThat((RID) row.getProperty("source")).isEqualTo(a);
      assertThat((RID) row.getProperty("target")).isEqualTo(fresh);
      assertThat(((Number) row.getProperty("distance")).doubleValue()).isEqualTo(2.0);
    });
    assertThat(rows).noneSatisfy(row -> assertThat((RID) row.getProperty("source")).isEqualTo(deleted));
    assertThat(rows).noneSatisfy(row -> assertThat((RID) row.getProperty("target")).isEqualTo(deleted));
  }

  /**
   * The sweep issue #6792 asks for: every whole-graph procedure that emits one row per node has to emit one for
   * each of the three live vertices and none for the deleted one. Before the fix the CSR-accelerated ones threw
   * {@code ArrayIndexOutOfBoundsException} on the added vertex's ID, and the ones reading the graph through
   * {@code GraphData} silently left it out of the answer.
   */
  @Test
  void everyWholeGraphProcedureSeesExactlyTheLiveVertices() {
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
      assertThat(nodes).as(query).containsExactlyInAnyOrder(a, b, fresh);
    }
  }

  @Test
  void traversalProceduresReachTheAddedVertex() {
    final ResultSet bfs = database.query("opencypher",
        "MATCH (s:N {name: 'A'}) CALL algo.bfs(s, 'E', 'OUT') YIELD node, depth RETURN node, depth");
    final List<Result> hops = new ArrayList<>();
    while (bfs.hasNext())
      hops.add(bfs.next());
    assertThat(hops).anySatisfy(row -> {
      assertThat((RID) row.getProperty("node")).isEqualTo(fresh);
      assertThat(((Number) row.getProperty("depth")).intValue()).isEqualTo(2);
    });

    final ResultSet dijkstra = database.query("opencypher",
        "MATCH (s:N {name: 'A'}) CALL algo.dijkstra.singleSource(s, 'E', 'w', 'OUT') YIELD node, cost RETURN node, cost");
    final Set<RID> reached = new HashSet<>();
    while (dijkstra.hasNext())
      reached.add((RID) dijkstra.next().getProperty("node"));
    assertThat(reached).contains(fresh);
    assertThat(reached).doesNotContain(deleted);
  }

  private int deletedBaseId() {
    // The deleted vertex's RID no longer resolves through the overlay, so its base ID is the one below the base
    // mapping's size that neither survivor holds.
    final Set<Integer> live = Set.of(view.getNodeId(a), view.getNodeId(b));
    for (int id = 0; id < view.getNodeMapping().size(); id++)
      if (!live.contains(id))
        return id;
    throw new IllegalStateException("no deleted base id");
  }

  private static List<Result> drain(final Stream<Result> rows) {
    final List<Result> results = new ArrayList<>();
    for (final Iterator<Result> it = rows.iterator(); it.hasNext(); )
      results.add(it.next());
    return results;
  }

  private static final class ProbeAPSP extends AlgoAPSP {
    private GraphData loadGraph(final Database database, final CommandContext context) {
      return loadGraph(database, null, null, context);
    }
  }
}
