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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6795 (follow-up on #6375/#6300): {@code weightedAdjacencyFromColumns} (the CSR path, see
 * {@link Issue6300AlgoMSTEdgeBudgetTest}) checkpoints its edge-entry reservation at
 * {@code min(ADJACENCY_CHECKPOINT_ENTRIES / 3, memory.capacityFor(INT_BYTES + DOUBLE_BYTES))}, recomputed
 * after every reservation so the checkpoint stride shrinks as the budget fills. {@code weightedAdjacencyFromRecords}
 * (the plain OLTP fallback, exercised here since no Graph Analytical View is built) used the flat
 * {@code ADJACENCY_CHECKPOINT_ENTRIES} constant with no {@code capacityFor} cap, so on a graph small enough that
 * neither that constant nor the every-1024-nodes boundary is ever reached mid-walk, the mid-loop checkpoint never
 * fired at all: the whole weighted adjacency list was built in memory before the single unconditional
 * post-loop reservation call finally refused it - the "up to ~12 MB overshoot before refusing" this issue
 * describes.
 * <p>
 * Pins the fix by budget: with a tight enough {@code arcadedb.cypher.algo.maxWorkingMemory}, the refusal must name
 * FEWER edge entries than the graph actually has - proof the checkpoint fired mid-walk instead of only after every
 * edge was already read into the adjacency arrays.
 */
class Issue6795WeightedAdjacencyRecordsCheckpointTest {
  /** Below the every-1024-nodes checkpoint boundary and far below ADJACENCY_CHECKPOINT_ENTRIES (1_048_576). */
  private static final int  NODE_COUNT  = 200;
  private static final long GRAPH_BYTES = (NODE_COUNT + 1) * 96L; // OLTP_VERTEX_BYTES, +1 for the load's own headroom check
  private static final long ROW_HEADER_BYTES = NODE_COUNT * 64L; // 2 * MATRIX_ROW_OVERHEAD_BYTES per node

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6795-weighted-adjacency-records");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    // No edge property declared, and no Graph Analytical View built anywhere in this test: both keep the call on
    // weightedAdjacencyFromRecords rather than the CSR-columnar weightedAdjacencyFromColumns (see Issue6300's
    // setup note on the same distinction).
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final List<MutableVertex> nodes = new ArrayList<>(NODE_COUNT);
      for (int i = 0; i < NODE_COUNT; i++)
        nodes.add(database.newVertex("Node").set("idx", i).save());
      for (int i = 0; i < NODE_COUNT - 1; i++)
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
  void theRecordPathChecksInMidWalkInsteadOfOnlyAfterMaterialisingEveryEdge() {
    // Enough to admit the graph load and the weighted-adjacency row headers, but only a handful of edge entries
    // on top - nowhere near the (NODE_COUNT - 1) = 199 edges the walk would fully materialise before a single
    // flat-threshold checkpoint ever fired.
    final long budget = GRAPH_BYTES + ROW_HEADER_BYTES + 200L;
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, budget);

    final Throwable failure = catchFailure(() -> drain("CALL algo.mst('w') YIELD source RETURN source"));

    assertThat(failure).as("a graph this far over budget must still be refused").isNotNull();
    assertThat(failure).hasStackTraceContaining("the weighted adjacency list would need");

    final Integer reportedEntries = reportedEdgeEntries(failure);
    assertThat(reportedEntries).as("the refusal message must name the edge-entry count it actually reached").isNotNull();
    // The unfixed code checkpoints only every 1024 nodes or ADJACENCY_CHECKPOINT_ENTRIES (1_048_576) entries -
    // neither ever reached by 199 total edges - so it reports the FULL edge count (199, measured) at the single
    // unconditional post-loop reservation: the whole adjacency list was already built by the time it refuses.
    // A checkpoint that also consults capacityFor(), sized against this test's tight budget, refuses within the
    // first handful of entries instead (1, measured) - comfortably under half of the full count either way.
    assertThat(reportedEntries)
        .as("a checkpoint that consults capacityFor must refuse mid-walk, well before every one of this "
            + "chain's " + (NODE_COUNT - 1) + " edge entries has been read into the adjacency arrays")
        .isLessThan((NODE_COUNT - 1) / 2);
  }

  private Throwable catchFailure(final Runnable body) {
    try {
      body.run();
      return null;
    } catch (final Throwable t) {
      return t;
    }
  }

  private static final Pattern EDGE_ENTRIES_PATTERN = Pattern.compile("(\\d+) edge entries");

  private static Integer reportedEdgeEntries(final Throwable failure) {
    // The refusal's own message may be wrapped by the query engine's own exception, so scan every cause's
    // message rather than only the outermost one - the same span assertj's hasStackTraceContaining covers.
    for (Throwable t = failure; t != null; t = t.getCause()) {
      final Matcher matcher = EDGE_ENTRIES_PATTERN.matcher(String.valueOf(t.getMessage()));
      if (matcher.find())
        return Integer.valueOf(matcher.group(1));
    }
    return null;
  }

  @Test
  void aBudgetThatFitsLetsTheCallThrough() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
    assertThat(drain("CALL algo.mst('w') YIELD source RETURN source")).hasSize(NODE_COUNT - 1);
  }

  private List<Object> drain(final String query) {
    final List<Object> rows = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }
}
