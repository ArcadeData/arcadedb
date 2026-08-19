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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6317: the graph an {@code algo.*} call loads is the first allocation of every procedure in the package
 * and was priced by no budget - so a call could be refused for a 64 MB matrix having already allocated a
 * multi-gigabyte graph to measure it against, and a call that was accepted held that graph for its whole
 * duration with {@code arcadedb.cypher.algoMaxWorkingMemory} none the wiser.
 * <p>
 * Three allocations, now all reserved: the {@code List<Vertex>} of loaded records, the {@code Map<RID, Integer>}
 * index built beside it (a boxed {@code Integer} and a {@code HashMap.Node} per vertex), and the {@code int[][]}
 * adjacency, which is the larger of the two - 4 bytes per edge plus a row header per node, with
 * {@code Vertex.DIRECTION.BOTH} materialising each edge twice.
 * <p>
 * Both are bounded where the count becomes known rather than after the structure exists: the vertex walk carries
 * its own stopping rule through {@code MemoryBudget.capacityFor}, and the adjacency build already counts every
 * entry before it allocates a single row, so the total is handed to the budget at the one moment the result is
 * still free to refuse. That placement is the difference between refusing a call and paying in full for a
 * traversal before refusing it - the same argument {@code algo.mst} settled in issue #6300.
 * <p>
 * The budget is also now one budget per call rather than one per component: the graph is charged first and every
 * later reservation accumulates on top of it, which is what {@link #theWorkingSetAccumulatesOnTopOfTheGraph()}
 * holds it to.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6317AlgoGraphLoadBudgetTest {
  /** Eleven nodes, so the loaded graph is a figure the assertions below can name exactly. */
  private static final int NODE_COUNT = 11;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6317-graph-load-budget");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // A path, so the graph is connected and every procedure below has something to traverse.
    database.transaction(() -> {
      final List<MutableVertex> nodes = new ArrayList<>(NODE_COUNT);
      for (int i = 0; i < NODE_COUNT; i++)
        nodes.add(database.newVertex("Node").set("idx", i).set("name", "N" + i).save());
      for (int i = 0; i < NODE_COUNT - 1; i++)
        nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      database.drop();
      database = null;
    }
  }

  // ── The graph itself ─────────────────────────────────────────────────────

  @Test
  void theLoadedGraphIsRefusedWhenItDoesNotFitTheBudget() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    assertThatThrownBy(() -> drain("CALL algo.pageRank() YIELD node RETURN node"))
        .as("the graph is the largest thing the working set holds and must be priced like any other component")
        .hasStackTraceContaining("algo.pagerank(): the loaded graph would need")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void theRefusalNamesTheNodeCountItStoppedAt() {
    // The walk stops when the budget runs out, so the message quotes the count reached rather than the graph's
    // total - there is no total to quote yet. That is the observable difference between bounding the walk and
    // reserving after it, and it is the whole point of the placement.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    assertThatThrownBy(() -> drain("CALL algo.pageRank() YIELD node RETURN node"))
        .hasStackTraceContaining("at least 2 nodes");
  }

  @Test
  void theProceduresThatLoadTheirOwnVertexListArePricedToo() {
    // algo.maxFlow, algo.kShortestPaths and algo.mst build the list and the RID index by hand rather than
    // through loadGraph, and went on bypassing the budget when loadGraph stopped doing so.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    for (final String[] call : new String[][] {
        { "algo.maxFlow", "MATCH (a:Node {idx: 0}), (z:Node {idx: 10}) CALL algo.maxFlow(a, z) YIELD maxFlow RETURN maxFlow" },
        { "algo.kShortestPaths",
            "MATCH (a:Node {idx: 0}), (z:Node {idx: 10}) CALL algo.kShortestPaths(a, z, 2) YIELD weight RETURN weight" },
        { "algo.mst", "CALL algo.mst('w') YIELD source RETURN source" } })
      assertThatThrownBy(() -> drain(call[1]))
          .as("%s loads the same graph and must price it", call[0])
          .hasStackTraceContaining(call[0] + "(): the loaded graph would need");
  }

  // ── The adjacency ────────────────────────────────────────────────────────

  @Test
  void theAdjacencyListIsRefusedWhenItDoesNotFitTheBudget() {
    // 11 nodes cost 1056 bytes, so a budget just above that admits the graph and leaves nothing for the
    // adjacency - which is the larger of the two on any real graph. algo.wcc rather than algo.pagerank here
    // because it takes its neighbour lists through GraphData.adjacency, which is where the copy is priced.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1100L);

    assertThatThrownBy(() -> drain("CALL algo.wcc() YIELD node RETURN node"))
        .hasStackTraceContaining("the adjacency list would need")
        .hasStackTraceContaining(NODE_COUNT + " nodes, ")
        .hasStackTraceContaining("edge entries");
  }

  @Test
  void theWorkingSetAccumulatesOnTopOfTheGraph() {
    // One budget per call, not one per component: the distance matrix is charged on top of the graph and the
    // adjacency, so the message names what was already reserved.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1500L);

    assertThatThrownBy(() -> drain("CALL algo.apsp() YIELD distance RETURN distance"))
        .hasStackTraceContaining("bytes this call already reserved");
  }

  // ── The counterweights ───────────────────────────────────────────────────

  @Test
  void aBudgetThatFitsLetsTheCallThrough() {
    // The reservation must not refuse a graph it can serve: at the default budget this one is trivial.
    assertThat(drain("CALL algo.pageRank() YIELD node RETURN node")).hasSize(NODE_COUNT);
    assertThat(drain("CALL algo.wcc() YIELD node RETURN node")).hasSize(NODE_COUNT);
  }

  @Test
  void aDisabledBudgetPricesNothing() {
    // Negative means no limit, and that has to reach the graph load and the adjacency too.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

    assertThat(drain("CALL algo.pageRank() YIELD node RETURN node")).hasSize(NODE_COUNT);
    assertThat(drain("CALL algo.mst('w') YIELD source RETURN source")).hasSize(NODE_COUNT - 1);
  }

  private List<Object> drain(final String query) {
    final List<Object> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }
}
