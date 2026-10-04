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
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A whole-graph algorithm called right after a reopen, while the persisted Graph Analytical View is still a deferred
 * restore-from-disk, must wait for that restore instead of falling back to the record-by-record path (issue #9220).
 * Every case here calls the procedure straight away, with no {@code awaitReady()}: the first call must be the one that
 * resolves the deferred restore.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoWholeGraphDeferredRestoreTest {
  private static final String DB_PATH   = "./target/databases/test-algo-deferred-restore";
  private static final String VIEW_NAME = "deferred-restore-wcc";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("EDGE");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("EDGE", b, true, (Object[]) null).save();
      database.newVertex("Node").set("name", "D").save();
      b.newEdge("EDGE", c, true, (Object[]) null).save();
    });
    GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("Node")
        .withEdgeTypes("EDGE")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.close();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  private void reopen() {
    database.close();
    database = new DatabaseFactory(DB_PATH).open();
  }

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private Vertex vertexNamed(final String name) {
    try (final ResultSet rs = database.query("sql", "select from Node where name = ?", name)) {
      return rs.next().getVertex().orElseThrow();
    }
  }

  @Test
  void wccFirstCallAfterReopenUsesTheRestoredView() {
    reopen();

    final BasicCommandContext context = newContext();
    final List<Result> rows = new AlgoWCC().execute(new Object[0], null, context).collect(Collectors.toList());

    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
        .as("the first call after a reopen must wait for the restored view, not scan the records")
        .isEqualTo(true);
    assertThat(rows).hasSize(4);
    assertThat(rows.stream().map(r -> (Integer) r.getProperty("componentId")).distinct().count()).isEqualTo(2);
  }

  /** The change sits in the base class every whole-graph procedure goes through, so each of them is checked. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "pagerank", "wcc", "lcc", "labelPropagation", "bfs (edge type filter)" })
  void everyProcedureUsesTheRestoredViewOnItsFirstCall(final String procedure) {
    reopen();

    final CypherProcedure algo;
    final Object[] args;
    switch (procedure) {
    case "pagerank" -> {
      algo = new AlgoPageRank();
      args = new Object[0];
    }
    case "wcc" -> {
      algo = new AlgoWCC();
      args = new Object[0];
    }
    case "lcc" -> {
      algo = new AlgoLocalClusteringCoefficient();
      args = new Object[0];
    }
    case "labelPropagation" -> {
      algo = new AlgoLabelPropagation();
      args = new Object[0];
    }
    default -> {
      algo = new AlgoBFS();
      // a non-null relationship type list takes the typed lookup, not the whole-graph one
      args = new Object[] { vertexNamed("A"), "EDGE" };
    }
    }

    final BasicCommandContext context = newContext();
    final List<Result> rows = algo.execute(args, null, context).collect(Collectors.toList());

    assertThat(rows).isNotEmpty();
    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
        .as("the first %s call after a reopen must wait for the restored view", procedure)
        .isEqualTo(true);
  }

  @Test
  void aPersistedViewThatCannotBeRestoredIsRebuiltAndStillAnswersCorrectly() throws IOException {
    database.close();
    // Truncate the persisted CSR: the restore finds it unusable and falls back to a full rebuild
    try (final RandomAccessFile file = new RandomAccessFile(DB_PATH + "/gav-" + VIEW_NAME + ".csr", "rw")) {
      file.setLength(16);
    }
    database = new DatabaseFactory(DB_PATH).open();

    final BasicCommandContext context = newContext();
    final List<Result> rows = new AlgoWCC().execute(new Object[0], null, context).collect(Collectors.toList());

    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
        .as("the call waits for the rebuild the failed restore falls back to")
        .isEqualTo(true);
    assertThat(rows).hasSize(4);
    assertThat(rows.stream().map(r -> (Integer) r.getProperty("componentId")).distinct().count()).isEqualTo(2);
  }

  @Test
  void aViewThatCannotServeTheRequestIsNeitherRestoredNorWaitedFor() {
    // Both views cover every vertex type of the database, so a whole-graph algorithm could use either; they differ
    // only in the edge type, and the request asks for EDGE.
    database.getSchema().createVertexType("Other");
    database.getSchema().createEdgeType("OTHER_EDGE");
    GraphAnalyticalViewRegistry.get(database, VIEW_NAME).drop();
    GraphAnalyticalView.builder(database)
        .withName(VIEW_NAME)
        .withVertexTypes("Node", "Other")
        .withEdgeTypes("EDGE")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();
    GraphAnalyticalView.builder(database)
        .withName("unrelated-view")
        .withVertexTypes("Node", "Other")
        .withEdgeTypes("OTHER_EDGE")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.OFF)
        .build();
    reopen();
    final GraphAnalyticalView unrelated = GraphAnalyticalViewRegistry.get(database, "unrelated-view");
    assertThat(unrelated).isNotNull();

    final BasicCommandContext context = newContext();
    new AlgoBFS().execute(new Object[] { vertexNamed("A"), "EDGE" }, null, context).collect(Collectors.toList());

    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isEqualTo(true);
    assertThat(unrelated.isRestoring())
        .as("a view the request cannot use must not have its restore dispatched, nor be waited for")
        .isFalse();
    assertThat(unrelated.isBuilt()).isFalse();
  }
}
