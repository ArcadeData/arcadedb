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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression coverage for issue #6792. */
class Issue6792GraphDataNodeIdBoundTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6792-node-id-bound");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");
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
  void graphDataUsesTheNodeIdSpaceRatherThanTheLiveNodeCount() {
    final List<MutableVertex> baseVertices = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        baseVertices.add(database.newVertex("N").set("name", "base-" + i).save());
      baseVertices.get(0).newEdge("E", baseVertices.get(1)).save();
    });

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("issue-6792-node-id-bound")
        .withVertexTypes("N")
        .withEdgeTypes("E")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .withCompactionThreshold(Integer.MAX_VALUE)
        .build();

    try {
      final MutableVertex lowestId = baseVertices.stream()
          .min(Comparator.comparingInt(v -> view.getNodeId(v.getIdentity())))
          .orElseThrow();
      final MutableVertex highestId = baseVertices.stream()
          .max(Comparator.comparingInt(v -> view.getNodeId(v.getIdentity())))
          .orElseThrow();

      final MutableVertex[] fresh = new MutableVertex[1];
      database.transaction(() -> {
        lowestId.getIdentity().asVertex().delete();
        fresh[0] = database.newVertex("N").set("name", "fresh").save();
        highestId.getIdentity().asVertex().modify().newEdge("E", fresh[0]).save();
      });

      final int freshId = view.getNodeId(fresh[0].getIdentity());
      assertThat(view.getNodeCount()).isEqualTo(3);
      assertThat(freshId).isGreaterThanOrEqualTo(view.getNodeCount());
      assertThat(view.getNodeIdUpperBound()).isEqualTo(freshId + 1);
      assertThat(view.getNeighborIds(view.getNodeId(highestId.getIdentity()), Vertex.DIRECTION.OUT, "E"))
          .contains(freshId);
      assertThat(view.getNeighborIds(view.getNodeId(highestId.getIdentity()), Vertex.DIRECTION.OUT))
          .contains(freshId);

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final ProbeAPSP procedure = new ProbeAPSP();
      final AbstractAlgoProcedure.GraphData graph = procedure.loadGraph(database, context);
      assertThat(graph.nodeCount).isEqualTo(view.getNodeIdUpperBound());
      assertThat(graph.adjacency(Vertex.DIRECTION.OUT)[view.getNodeId(highestId.getIdentity())])
          .contains(freshId);

      final List<Result> rows = drain(procedure.execute(new Object[0], null, context));

      assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isEqualTo(true);
      assertThat(rows).anySatisfy(row -> {
        assertThat((RID) row.getProperty("source")).isEqualTo(highestId.getIdentity());
        assertThat((RID) row.getProperty("target")).isEqualTo(fresh[0].getIdentity());
        assertThat(((Number) row.getProperty("distance")).doubleValue()).isEqualTo(1.0);
      });
    } finally {
      view.drop();
    }
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
