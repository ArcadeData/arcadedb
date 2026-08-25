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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6318 part 1 - {@code algo.localClusteringCoefficient} was abortable on its OLTP
 * path (a {@code guard.check()} per node, already covered by {@code Issue6302AlgoGraphDrivenWorkGuardTest}, whose
 * fixture never builds a {@link GraphAnalyticalView}) and not on its CSR path, which handed the whole computation
 * to {@code GraphAlgorithms#localClusteringCoefficient} with nothing threaded through to stop it.
 * <p>
 * Same deterministic technique as {@code Issue6302AlgoGraphDrivenWorkGuardTest}: the interrupt is armed before the
 * call starts, so the first checkpoint the procedure reaches must observe it regardless of the machine's speed.
 * Before this fix, the CSR path's checkpoint did not exist at all, so this call ran to completion and returned a
 * result despite the pending interrupt.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6318LocalClusteringCoefficientCsrAbortabilityTest {
  private Database             database;
  private GraphAnalyticalView  view;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6318-lcc-csr-abortability");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // A small triangle-bearing graph is enough: the point is whether the checkpoint is consulted at all, not
    // how much work the kernel does before finding it.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", a, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
    });

    view = GraphAnalyticalView.builder(database)
        .withName("lcc-abortability-view")
        .withVertexTypes("Node")
        .withEdgeTypes("LINK")
        .build();
  }

  @AfterEach
  void teardown() {
    // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
    Thread.interrupted();
    if (view != null)
      view.drop();
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  @Timeout(60)
  void theCSRPathStopsOnAThreadInterrupt() {
    Thread.currentThread().interrupt();

    assertThatThrownBy(() -> drain("CALL algo.localClusteringCoefficient() YIELD node RETURN node"))
        .as("a Graph Analytical View must not make algo.localClusteringCoefficient any less abortable than its "
            + "OLTP path")
        .hasStackTraceContaining("algo.localClusteringCoefficient() has been interrupted");
  }

  /**
   * The counterweight: with nothing armed, the CSR path still answers, and still through the view rather than
   * silently falling back to OLTP - a guard wired in wrong could break either half.
   */
  @Test
  void anUninterruptedCallStillReturnsItsResultThroughTheView() {
    assertThat(drain("CALL algo.localClusteringCoefficient() YIELD node RETURN node")).hasSize(4);
  }

  private List<Result> drain(final String query) {
    final List<Result> results = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext())
      results.add(rs.next());
    return results;
  }
}
