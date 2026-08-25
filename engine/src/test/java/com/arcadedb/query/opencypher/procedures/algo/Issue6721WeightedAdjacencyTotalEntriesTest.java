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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.opencypher.procedures.algo.AbstractAlgoProcedure.GraphData;
import com.arcadedb.query.opencypher.procedures.algo.AbstractAlgoProcedure.GraphData.WeightedAdjacency;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6721: {@code GraphData.weightedAdjacency()}'s unit-weight branch summed {@code neighbors[i].length} across
 * every node to size its own memory reservation, even though {@code adjacency()}/{@code reserveAdjacency} had
 * already computed (and discarded) that same total while building the array. {@code AlgoMST}/{@code
 * AlgoMinSpanningArborescence} then each summed it again after {@code weightedAdjacency()} returned, to size their
 * own flat edge arrays - the same node-count-sized total recomputed two or three times per call.
 * <p>
 * The fix threads a {@code totalEntries} field through {@link WeightedAdjacency} itself, computed once - in the
 * same walk that already builds or prices the rows, on whichever of the three internal paths runs (unit-weight,
 * CSR-backed columnar edge properties, or OLTP edge records) - so callers read it instead of re-summing. These
 * tests pin that value against an independently-counted expected total on each of the three paths, and
 * {@code AlgoMST}/{@code AlgoMinSpanningArborescence}'s own tests (unchanged by this fix) continue to cover that
 * the edge arrays sized from it are the right size.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6721WeightedAdjacencyTotalEntriesTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6721-weightedadjacency-totalentries");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E").createProperty("w", Type.DOUBLE);
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  /**
   * Asymmetric out-degrees (3 + 1 + 0 + 1 = 5 total OUT edges), so a totalEntries that accidentally summed IN
   * degree, or double-counted a row, would not go unnoticed.
   */
  private void buildGraph() {
    database.transaction(() -> {
      final Vertex a = database.newVertex("N").set("name", "A").save();
      final Vertex b = database.newVertex("N").set("name", "B").save();
      final Vertex c = database.newVertex("N").set("name", "C").save();
      final Vertex d = database.newVertex("N").set("name", "D").save();
      a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
      a.newEdge("E", c, true, new Object[] { "w", 2.0 }).save();
      a.newEdge("E", d, true, new Object[] { "w", 3.0 }).save();
      b.newEdge("E", c, true, new Object[] { "w", 4.0 }).save();
      c.newEdge("E", d, true, new Object[] { "w", 5.0 }).save();
    });
  }

  @Test
  void unitWeightBranchReportsTheCorrectTotal() {
    buildGraph();
    final CommandContext context = newContext();
    final AlgoMST algo = new AlgoMST();
    final GraphData graph = algo.loadGraph(database, null, null, context);
    final WorkGuard guard = algo.newWorkGuard(context);

    final WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, null);

    assertThat(weighted.totalEntries()).isEqualTo(sumOfRowLengths(weighted));
    assertThat(weighted.totalEntries()).isEqualTo(5L);
  }

  @Test
  void recordBackedBranchReportsTheCorrectTotal() {
    buildGraph();
    final CommandContext context = newContext();
    final AlgoMST algo = new AlgoMST();
    final GraphData graph = algo.loadGraph(database, null, null, context);
    final WorkGuard guard = algo.newWorkGuard(context);

    // No Graph Analytical View registered, so this falls onto the edge-record path (servesEdgeProperty has
    // nothing to consult) rather than the columnar one.
    final WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, "w");

    assertThat(weighted.totalEntries()).isEqualTo(sumOfRowLengths(weighted));
    assertThat(weighted.totalEntries()).isEqualTo(5L);
  }

  @Test
  void columnarCsrBackedBranchReportsTheCorrectTotal() {
    buildGraph();
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("issue-6721-view")
        .withVertexTypes("N")
        .withEdgeTypes("E")
        .withEdgeProperties("w")
        .build();
    try {
      final CommandContext context = newContext();
      final AlgoMST algo = new AlgoMST();
      final GraphData graph = algo.loadGraph(database, null, null, context);
      final WorkGuard guard = algo.newWorkGuard(context);

      assertThat(graph.isCSRBacked()).as("the view must actually back this call, or the columnar path is untested").isTrue();

      final WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, "w");

      assertThat(weighted.totalEntries()).isEqualTo(sumOfRowLengths(weighted));
      assertThat(weighted.totalEntries()).isEqualTo(5L);
    } finally {
      view.drop();
    }
  }

  private static long sumOfRowLengths(final WeightedAdjacency weighted) {
    long sum = 0;
    for (final int[] row : weighted.neighbors())
      sum += row.length;
    return sum;
  }
}
