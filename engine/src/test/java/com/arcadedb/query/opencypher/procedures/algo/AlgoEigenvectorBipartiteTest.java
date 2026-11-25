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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Plain power iteration on the adjacency matrix does not converge on a bipartite graph: the extreme eigenvalues are +l and -l, so
 * the iterate alternates between two vectors and the scores depend on the parity of {@code maxIterations}. On a star the default
 * call tied every vertex at 1.0 and an odd count answered a hub of 1.0 over leaves of 0.25, never the eigenvector (hub 1.0, leaves
 * 0.5).
 */
class AlgoEigenvectorBipartiteTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-eigenvector-bipartite");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void starAnswersTheEigenvectorWhateverTheParityOfTheIterationCount() {
    // hub 0 with four leaves: eigenvalue 2, hub 1.0 and leaves 1/2
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").set("id", 0).save();
      for (int i = 1; i <= 4; i++)
        hub.newEdge("E", database.newVertex("N").set("id", i).save()).save();
    });

    for (final String call : new String[] { "algo.eigenvector()", "algo.eigenvector(null, 'BOTH', 100, 1e-12)", "algo.eigenvector(null, 'BOTH', 101, 1e-12)" }) {
      final Map<Integer, Double> scores = scores(call);
      assertThat(scores.get(0)).as(call).isCloseTo(1.0, within(1e-4));
      for (int i = 1; i <= 4; i++)
        assertThat(scores.get(i)).as(call + " leaf " + i).isCloseTo(0.5, within(1e-4));
    }
  }

  @Test
  void pathOfThreeAnswersTheEigenvector() {
    // 0 - 1 - 2: eigenvalue sqrt(2), centre 1.0 and ends 1/sqrt(2)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("id", 0).save();
      final MutableVertex b = database.newVertex("N").set("id", 1).save();
      final MutableVertex c = database.newVertex("N").set("id", 2).save();
      a.newEdge("E", b).save();
      b.newEdge("E", c).save();
    });

    final Map<Integer, Double> scores = scores("algo.eigenvector(null, 'BOTH', 200, 1e-12)");
    assertThat(scores.get(1)).isCloseTo(1.0, within(1e-6));
    assertThat(scores.get(0)).isCloseTo(Math.sqrt(0.5), within(1e-6));
    assertThat(scores.get(2)).isCloseTo(Math.sqrt(0.5), within(1e-6));
  }

  @Test
  void vertexWithoutNeighboursStaysAtZero() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("id", 0).save();
      final MutableVertex b = database.newVertex("N").set("id", 1).save();
      database.newVertex("N").set("id", 2).save();
      a.newEdge("E", b).save();
    });

    final Map<Integer, Double> scores = scores("algo.eigenvector(null, 'BOTH', 200, 1e-12)");
    assertThat(scores.get(0)).isCloseTo(1.0, within(1e-6));
    assertThat(scores.get(1)).isCloseTo(1.0, within(1e-6));
    assertThat(scores.get(2)).isEqualTo(0.0);
  }

  @Test
  void directedAcyclicGraphScoresZeroWhateverTheIterationCount() {
    // 0 -> 1 -> 2, plus 3 -> 1: no directed cycle, so A is nilpotent and its only eigenvalue is 0. A + I has eigenvalue 1 with
    // a Jordan block: without pinning the vertices that reach no cycle, 1 keeps a constant raw score while 0 and 3 grow linearly
    // and the answer is 1/(k+1), a function of maxIterations that never meets the tolerance.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("id", 0).save();
      final MutableVertex b = database.newVertex("N").set("id", 1).save();
      final MutableVertex c = database.newVertex("N").set("id", 2).save();
      final MutableVertex d = database.newVertex("N").set("id", 3).save();
      a.newEdge("E", b).save();
      b.newEdge("E", c).save();
      d.newEdge("E", b).save();
    });

    for (final String dir : new String[] { "OUT", "IN" })
      for (final int iterations : new int[] { 1, 20, 1000 }) {
        final String call = "algo.eigenvector(null, '" + dir + "', " + iterations + ", 1e-12)";
        final Map<Integer, Double> scores = scores(call);
        for (int i = 0; i <= 3; i++)
          assertThat(scores.get(i)).as(call + " vertex " + i).isEqualTo(0.0);
      }
  }

  @Test
  void directedCycleWithATailConvergesWhateverTheParityOfTheIterationCount() {
    // 0 <-> 1 is a directed 2-cycle, 2 -> 0 a tail. Followed inwards, 0 collects from 1 and 2 and 1 from 0: on A the iterate
    // alternates between (1, 0.5) and (0.5, 1). The tail 2 has no in-neighbour, so it reaches no cycle and scores 0.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("id", 0).save();
      final MutableVertex b = database.newVertex("N").set("id", 1).save();
      final MutableVertex t = database.newVertex("N").set("id", 2).save();
      a.newEdge("E", b).save();
      b.newEdge("E", a).save();
      t.newEdge("E", a).save();
    });

    for (final String call : new String[] { "algo.eigenvector(null, 'IN', 100, 1e-12)", "algo.eigenvector(null, 'IN', 101, 1e-12)" }) {
      final Map<Integer, Double> scores = scores(call);
      assertThat(scores.get(0)).as(call).isCloseTo(1.0, within(1e-6));
      assertThat(scores.get(1)).as(call).isCloseTo(1.0, within(1e-6));
      assertThat(scores.get(2)).as(call).isEqualTo(0.0);
    }
  }

  private Map<Integer, Double> scores(final String call) {
    final Map<Integer, Double> scores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher", "CALL " + call + " YIELD node, score RETURN node.id AS id, score")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.put(((Number) row.getProperty("id")).intValue(), ((Number) row.getProperty("score")).doubleValue());
      }
    }
    return scores;
  }
}
