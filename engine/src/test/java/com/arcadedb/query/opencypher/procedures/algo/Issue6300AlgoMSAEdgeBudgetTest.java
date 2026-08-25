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
 * Regression tests for issue #6300, the {@code algo.msa} half of a PR #6714 review round-6 finding: this
 * procedure allocates its own flat {@code eFrom}/{@code eTo}/{@code eW} copy of the edges - the same shape as
 * {@code algo.mst}'s {@code eu}/{@code ev}/{@code ew} - but, unlike {@code algo.mst}, never reserved anything for
 * it. {@code weightedAdjacency}'s own incremental reservation (added in an earlier round of this same PR, see
 * {@link Issue6300AlgoMSTEdgeBudgetTest}) prices its own {@code int[][]}/{@code double[][]} pair, but says
 * nothing about a caller's additional copy on top of it - so a graph sized just under that threshold could still
 * take an unpriced ~16 bytes/edge, the same "OutOfMemoryError instead of a clean refusal" failure mode #6300 was
 * written to close.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6300AlgoMSAEdgeBudgetTest {
  /** Ten edges in a rooted chain, so the arborescence spans every node and no edge is redundant. */
  private static final int EDGE_COUNT = 10;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6300-msa-edge-budget");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // A directed chain of EDGE_COUNT + 1 nodes rooted at node 0: every non-root vertex has exactly one incoming
    // edge, so the arborescence is the whole chain and no edge is redundant.
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
    // 11 nodes at OLTP_VERTEX_BYTES=96 is 1056, weightedAdjacency's row headers (2 x 32 bytes/node) are 704, and
    // its 10 edge entries (12 bytes each) are 120 - 1880 total, all of which fits comfortably below this budget.
    // What does not fit is algo.msa's own eFrom/eTo/eW copy on top: 10 edges x 16 bytes = 160, taking the total
    // to 2040 - over a 1900-byte budget, refusing at this procedure's own reservation rather than
    // weightedAdjacency's.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1900L);

    assertThatThrownBy(() -> drain("MATCH (r:Node {idx: 0}) CALL algo.msa(r, 'LINK', 'w') YIELD source RETURN source"))
        .as("algo.msa's own edge arrays are additional working set on top of weightedAdjacency's and must be priced too")
        .hasStackTraceContaining("algo.msa(): the edge arrays would need")
        .hasStackTraceContaining(EDGE_COUNT + " edges")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void aBudgetThatFitsLetsTheCallThrough() {
    // The counterweight: the reservation must not refuse a graph it can serve. 1056 (graph) + 704 (row headers)
    // + 120 (weightedAdjacency entries) + 160 (this procedure's own copy) = 2040, comfortable under 4 KB.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 4096L);

    final List<Object> rows = drain(
        "MATCH (r:Node {idx: 0}) CALL algo.msa(r, 'LINK', 'w') YIELD source, weight, totalWeight RETURN source, weight, totalWeight");
    assertThat(rows).hasSize(EDGE_COUNT);
  }

  @Test
  void aDisabledBudgetPricesNothing() {
    // A negative limit means "no limit" throughout the budget, and it must reach this procedure's own edge-array
    // capacity too - a capacity computed by dividing a negative limit would refuse every graph instead of
    // accepting them all.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

    assertThat(drain("MATCH (r:Node {idx: 0}) CALL algo.msa(r, 'LINK', 'w') YIELD source RETURN source")).hasSize(EDGE_COUNT);
  }

  private List<Object> drain(final String query) {
    final List<Object> rows = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }
}
