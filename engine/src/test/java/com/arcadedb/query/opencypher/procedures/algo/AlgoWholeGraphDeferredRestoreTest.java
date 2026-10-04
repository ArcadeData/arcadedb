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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A whole-graph algorithm called right after a reopen, while the persisted Graph Analytical View is still a deferred
 * restore-from-disk, must wait for that restore instead of falling back to the record-by-record path: on a large
 * database the fallback took 173 seconds for the first {@code algo.wcc} call, against seconds once the view was usable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoWholeGraphDeferredRestoreTest {
  private static final String DB_PATH = "./target/databases/test-algo-deferred-restore";

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
        .withName("deferred-restore-wcc")
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

  @Test
  void wccFirstCallAfterReopenUsesTheRestoredView() {
    database.close();
    database = new DatabaseFactory(DB_PATH).open();

    // No awaitReady(): the first call must be the one that resolves the deferred restore.
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final List<Result> rows = new AlgoWCC().execute(new Object[0], null, context).collect(Collectors.toList());

    assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
        .as("the first call after a reopen must wait for the restored view, not scan the records")
        .isEqualTo(true);
    assertThat(rows).hasSize(4);
    assertThat(rows.stream().map(r -> (Integer) r.getProperty("componentId")).distinct().count()).isEqualTo(2);
  }
}
