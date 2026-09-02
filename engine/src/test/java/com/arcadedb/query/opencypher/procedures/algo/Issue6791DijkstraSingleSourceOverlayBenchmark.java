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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures, rather than assumes, the claim issue #6791 makes: against a {@code SYNCHRONOUS} Graph Analytical
 * View - the state such a view is in after every single commit - routing {@code algo.dijkstra.singleSource}
 * through the overlay-aware {@link com.arcadedb.graph.olap.GraphAlgorithms#dijkstraSingleSource} instead of
 * abandoning the CSR path outright is faster than the OLTP fallback it replaces, on a graph large enough for
 * the per-popped-node allocation the fix adds to actually show up against the win of not walking edge records.
 * <p>
 * Both arms run the same query against the same committed graph, one immediately after the other, so a single
 * {@link StallAwareStopwatch} window each keeps a JVM-wide pause from landing lopsidedly on one arm and not the
 * other (see the class Javadoc there, and issue #6260). Vertex lookups are resolved once, before either arm is
 * timed, so an SQL lookup's own cost never enters either measurement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue6791DijkstraSingleSourceOverlayBenchmark {
  private static final int NODES   = 4000;
  private static final int SOURCES = 40;

  private Database        database;
  private MutableVertex[] nodes;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6791-dijkstra-overlay-bench");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);

    // A ring backbone guarantees strong connectivity, plus a handful of chords per node so Dijkstra has real
    // branching to explore rather than following one deterministic path.
    nodes = new MutableVertex[NODES];
    database.transaction(() -> {
      for (int i = 0; i < NODES; i++)
        nodes[i] = database.newVertex("N").set("idx", i).save();
      for (int i = 0; i < NODES; i++) {
        nodes[i].newEdge("ROAD", nodes[(i + 1) % NODES], true, new Object[] { "w", 1.0 + (i % 7) }).save();
        for (int c = 1; c <= 3; c++) {
          final int target = (i + 17 * c + 1) % NODES;
          nodes[i].newEdge("ROAD", nodes[target], true, new Object[] { "w", 1.0 + ((i * c) % 11) }).save();
        }
      }
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void overlayAwareCsrPathBeatsTheOltpFallbackItReplaces() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("dijkstra-overlay-benchmark")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .withCompactionThreshold(Integer.MAX_VALUE)
        .build();
    boolean viewDropped = false;
    try {
      // One committed change is all a SYNCHRONOUS view needs to carry an active overlay from here on - exactly
      // the state issue #6791 says this procedure used to treat as "read the edge records instead".
      database.transaction(() -> nodes[0].newEdge("ROAD", nodes[1], true, new Object[] { "w", 1.0 }).save());
      assertThat(view.hasPendingChanges()).isTrue();

      final long acceleratedMs = timeRuns(true);
      view.drop();
      viewDropped = true;
      final long oltpMs = timeRuns(false);

      assertThat(acceleratedMs)
          .as("overlay-aware CSR (%d ms) must beat the full OLTP fallback (%d ms) it replaces on a %d-node graph",
              acceleratedMs, oltpMs, NODES)
          .isLessThan(oltpMs);
    } finally {
      if (!viewDropped)
        view.drop();
    }
  }

  /** Runs algo.dijkstra.singleSource from SOURCES distinct nodes and returns the discounted elapsed time. */
  private long timeRuns(final boolean expectCsrAcceleration) {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    for (int s = 0; s < SOURCES; s++) {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final Stream<Result> rows = new AlgoDijkstraSingleSource().execute(
          new Object[] { nodes[s * (NODES / SOURCES)], "ROAD", "w", "OUT" }, null, context);
      long count = 0;
      for (final Result ignored : (Iterable<Result>) rows::iterator)
        count++;
      assertThat(count).as("every node must be reachable over the ring backbone alone").isEqualTo(NODES - 1);
      if (expectCsrAcceleration)
        assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("the accelerated arm must actually be accelerated, or the comparison measures nothing")
            .isEqualTo(true);
    }
    return watch.elapsedMs();
  }
}
