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
 * Regression tests for issue #6300 - {@code algo.mst} was the one dense {@code algo.*} path the #6263
 * working-memory budget did not price.
 * <p>
 * #6263 reserved the dense working set of every procedure that builds one, choosing them by two criteria: sized by
 * a knob, or quadratic in the node count. {@code algo.mst} is neither. It is linear in the <em>edge</em> count -
 * three parallel arrays plus the index sort, 24 bytes per edge - which reads like the graph paying for itself.
 * But linear in the edge count is not small: the edge count is the largest linear dimension a graph has, usually
 * an order of magnitude above the node count, and 100M edges is ~2.4 GB requested with no check and no error
 * naming what asked for it. "Linear" was never the criterion; the criterion is whether the caller can predict a
 * ceiling, and here there is none.
 * <p>
 * The reservation is made as the counting pass runs rather than once it has finished. Both refuse the same calls,
 * but a check afterwards first pays in full for a traversal it will then throw away - the same argument that puts
 * {@code algo.steinerTree}'s reservation ahead of its adjacency build. The refusal itself still goes through
 * {@code MemoryBudget.reserve}, so the message names the same component and the same setting either way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6300AlgoMSTEdgeBudgetTest {
  /** Ten edges, so a budget sized for four of them is refused while a default budget is not. */
  private static final int EDGE_COUNT = 10;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6300-mst-edge-budget");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // A path of EDGE_COUNT + 1 nodes: connected, so the MST spans every node and no edge is redundant.
    database.transaction(() -> {
      final List<MutableVertex> nodes = new ArrayList<>(EDGE_COUNT + 1);
      for (int i = 0; i <= EDGE_COUNT; i++)
        nodes.add(database.newVertex("Node").set("idx", i).save());
      for (int i = 0; i < EDGE_COUNT; i++)
        nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void theEdgeArraysAreRefusedWhenTheyDoNotFitTheBudget() {
    // The graph is priced first since #6317 - 11 nodes at OLTP_VERTEX_BYTES is 1056 bytes - and what is left
    // over is what buys edges: 24 bytes each, so the 100 bytes above the graph buy four of them and the fifth
    // is over.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1156L);

    assertThatThrownBy(() -> drain("CALL algo.mst('w') YIELD source, target RETURN source, target"))
        .as("the edge arrays are the dense working set of an algo.mst call and must be priced like any other")
        .hasStackTraceContaining("algo.mst(): the edge arrays would need")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void theRefusalNamesTheEdgeCountItStoppedAt() {
    // The message quotes the count reached, not the graph's total: the walk stops when the budget runs out, so
    // there is no total to quote yet. That is the observable difference between reserving during the counting
    // pass and reserving after it - and it is the whole point of the placement.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1156L);

    assertThatThrownBy(() -> drain("CALL algo.mst('w') YIELD source RETURN source"))
        .hasStackTraceContaining("more than 4 edges");
  }

  @Test
  void aBudgetThatFitsLetsTheCallThrough() {
    // The counterweight: the reservation must not refuse a graph it can serve. 11 nodes cost 1056 bytes and
    // 10 edges at 24 bytes is 240, so 4 KB is comfortable, and the MST of a path is the path itself - every
    // edge, at its own weight.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 4096L);

    final List<Object> rows = drain("CALL algo.mst('w') YIELD source, weight, totalWeight RETURN source, weight, totalWeight");
    assertThat(rows).hasSize(EDGE_COUNT);
  }

  @Test
  void aDisabledBudgetPricesNothing() {
    // A negative limit means "no limit" throughout the budget, and it must reach the per-edge capacity too -
    // a capacity computed by dividing a negative limit would refuse every graph instead of accepting them all.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

    assertThat(drain("CALL algo.mst('w') YIELD source RETURN source")).hasSize(EDGE_COUNT);
  }

  @Test
  void theMinimumSpanningTreeIsUnchanged() {
    // The reservation and the checkpoints must not move the answer. On a path every edge is a bridge, so the MST
    // is the whole path and its total weight is 1 + 2 + ... + EDGE_COUNT.
    final ResultSet rs = database.query("opencypher",
        "CALL algo.mst('w') YIELD weight, totalWeight RETURN weight, totalWeight");
    double sum = 0.0;
    int rows = 0;
    double reportedTotal = -1.0;
    while (rs.hasNext()) {
      final var row = rs.next();
      sum += ((Number) row.getProperty("weight")).doubleValue();
      reportedTotal = ((Number) row.getProperty("totalWeight")).doubleValue();
      rows++;
    }

    assertThat(rows).isEqualTo(EDGE_COUNT);
    assertThat(sum).isEqualTo(EDGE_COUNT * (EDGE_COUNT + 1) / 2.0);
    assertThat(reportedTotal).isEqualTo(sum);
  }

  private List<Object> drain(final String query) {
    final List<Object> rows = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }
}
