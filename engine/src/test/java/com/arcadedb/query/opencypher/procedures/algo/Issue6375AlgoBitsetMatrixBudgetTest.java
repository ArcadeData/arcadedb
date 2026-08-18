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
 * Issue #6375: five algorithms built a {@code nodeCount x nodeCount} {@code BitSet} matrix up front,
 * unconditionally, without reserving against {@code arcadedb.cypher.algoMaxWorkingMemory} - the budget whose own
 * documentation names the {@code nodeCount x nodeCount} matrices as exactly what it exists to cap.
 * <p>
 * {@code new BitSet(n)} allocates {@code ceil(n/64)} longs whatever the node's real degree turns out to be, so a
 * {@code BitSet[n]} of them is a genuine {@code n²/8}-byte structure. Eight times denser per element than the
 * {@code double[n][n]} matrices the budget already refused, it slipped through to a far larger graph and then
 * exhausted the heap - the {@code OutOfMemoryError} the budget exists to replace with a client error naming the
 * component and the setting.
 * <p>
 * The build loops were also the unguarded phase of an otherwise abortable call, the same blind spot issue #6295
 * closed for the SLPA/GraphSAGE/HashGNN initialisation loops: the consumption loops carried the checkpoint and
 * the {@code O(n²/64)} build ahead of them did not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6375AlgoBitsetMatrixBudgetTest {
  /**
   * A budget that admits the four-node graph and its adjacency and refuses the bit matrix on top of them. The
   * matrix itself is 288 bytes at this size; what matters to the assertions below is only that it is the
   * component the refusal names.
   */
  private static final long BUDGET_ABOVE_THE_GRAPH_BELOW_THE_MATRIX = 700L;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6375-bitset-budget");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // A directed cycle over four nodes: every node has a neighbour in both directions, so each of the five
    // procedures has a matrix to build and something to find in it.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
      d.newEdge("LINK", a, true, (Object[]) null).save();
      a.newEdge("LINK", c, true, (Object[]) null).save();
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

  /** The five procedures, each with a call that reaches its bit matrix. */
  private static final String[][] CALLS = {
      { "algo.triangleCount", "CALL algo.triangleCount() YIELD node RETURN node" },
      { "algo.knn", "CALL algo.knn(2) YIELD node RETURN node" },
      { "algo.kTruss", "CALL algo.kTruss() YIELD node RETURN node" },
      { "algo.clique", "CALL algo.clique() YIELD clique RETURN clique" },
      { "algo.hierarchicalClustering", "CALL algo.hierarchicalClustering(2) YIELD node RETURN node" } };

  @Test
  void everyBitsetMatrixIsRefusedWhenItDoesNotFitTheBudget() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
        BUDGET_ABOVE_THE_GRAPH_BELOW_THE_MATRIX);

    for (final String[] call : CALLS)
      assertThatThrownBy(() -> drain(call[1]))
          .as("%s builds a nodeCount x nodeCount bit matrix and must price it", call[0])
          .hasStackTraceContaining(call[0] + "(): the neighbour bitsets")
          .hasStackTraceContaining("(4 x 4 nodes)")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void kTrussPricesItsSecondMatrixSeparately() {
    // computeFullTrussDecomposition builds a second bit matrix while the first is still alive, so the peak is
    // twice the shape - which is what a running total is for.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1000L);

    assertThatThrownBy(() -> drain("CALL algo.kTruss() YIELD node RETURN node"))
        .hasStackTraceContaining("the neighbour bitsets of the truss decomposition")
        .hasStackTraceContaining("(4 x 4 nodes)");
  }

  @Test
  void cliquePricesItsSearchStackAtThePeakDepth() {
    // The Bron-Kerbosch frames are five bit sets each and the stack is as deep as the largest clique reached,
    // so at its peak the stack is a second nodeCount x nodeCount structure.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1000L);

    assertThatThrownBy(() -> drain("CALL algo.clique() YIELD clique RETURN clique"))
        .hasStackTraceContaining("the clique search stack")
        .hasStackTraceContaining("frame");
  }

  @Test
  void aBudgetThatFitsLetsEveryCallThrough() {
    // The counterweight: the reservation must not refuse a graph it can serve. At the default budget none of
    // these five is anywhere near the limit.
    for (final String[] call : CALLS)
      assertThat(drain(call[1])).as("%s must still run at the default budget", call[0]).isNotNull();
  }

  @Test
  void aDisabledBudgetPricesNothing() {
    // Negative means no limit, and that has to keep meaning no limit for the newly priced component too.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

    for (final String[] call : CALLS)
      assertThat(drain(call[1])).as("%s must be unbounded when the budget is disabled", call[0]).isNotNull();
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
