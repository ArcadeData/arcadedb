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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10491: after {@code ALTER TYPE U SUPERTYPE +Base}, where {@code Base} is a vertex type a Graph Analytical View lists and
 * {@code U} already holds vertices, a vertex inserted into {@code U} right after the change was missing from the view and stayed
 * missing, while the view reported READY and {@code algo.wcc} answered one node fewer than the same call without the view. The
 * type attached after the scan is claimed only if it was scanned or held nothing, and otherwise the view rebuilds; here the check
 * took the type for empty, so it was claimed. Every run is compared with the answer the same call gives once the view is dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10491ViewVertexTypeAttachedAfterBuildTest {
  private static final String DB_PATH = "./target/databases/test-issue-10491";
  private static final int    REPS    = 12;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  @AfterEach
  void teardown() {
    if (database != null && database.isOpen())
      database.drop();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aVertexInsertedRightAfterAnExistingTypeJoinedIsInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode) throws Exception {
    for (int rep = 0; rep < REPS; rep++)
      runOnce(mode, 4, 1, true);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void severalVerticesInsertedRightAfterTheTypeJoinedAreInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode)
      throws Exception {
    for (int rep = 0; rep < REPS; rep++)
      runOnce(mode, 4, 2, true);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aVertexWithNoEdgeInsertedRightAfterTheTypeJoinedIsInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode)
      throws Exception {
    for (int rep = 0; rep < REPS; rep++)
      runOnce(mode, 4, 1, false);
  }

  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aTypeThatWasEmptyWhenItJoinedKeepsBeingServed(final GraphAnalyticalView.UpdateMode mode) throws Exception {
    for (int rep = 0; rep < REPS; rep++)
      runOnce(mode, 0, 1, true);
  }

  /** The delete of a vertex of the joined type meets the same fate as an insert: the listener dropped it, the rebuild kept the vertex. */
  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aVertexDeletedRightAfterTheTypeJoinedIsNotInTheViewAnswer(final GraphAnalyticalView.UpdateMode mode) throws Exception {
    for (int rep = 0; rep < REPS; rep++)
      runOnce(mode, 4, 0, false, "DELETE FROM U WHERE id = 30");
  }

  /**
   * The update of a vertex of the joined type meets the same fate as an insert: the listener dropped it, and the rebuild it
   * dispatched read the old value. Read through the view itself, once it holds the type.
   */
  @ParameterizedTest
  @EnumSource(value = GraphAnalyticalView.UpdateMode.class, names = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void aPropertyUpdatedRightAfterTheTypeJoinedIsWhatTheViewHoldsOnceItHoldsTheType(final GraphAnalyticalView.UpdateMode mode)
      throws Exception {
    for (int rep = 0; rep < REPS; rep++) {
      database = new DatabaseFactory(DB_PATH).create();
      try {
        database.command("sql", "CREATE VERTEX TYPE Base").close();
        database.command("sql", "CREATE VERTEX TYPE U").close();
        database.command("sql", "CREATE EDGE TYPE E1").close();
        database.transaction(() -> {
          for (int k = 0; k < 4; k++) {
            database.command("sql", "INSERT INTO Base SET id = " + k).close();
            database.command("sql", "INSERT INTO U SET id = " + (30 + k)).close();
          }
        });
        database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (Base) EDGE TYPES (E1) UPDATE MODE " + mode).close();
        awaitReady();

        database.command("sql", "ALTER TYPE U SUPERTYPE +Base").close();
        database.transaction(() -> database.command("sql", "UPDATE U SET id = 999 WHERE id = 30").close());
        awaitReady();

        final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "g");
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
            .until(() -> view.isReady() && view.coversVertexType("U"));

        final RID rid;
        try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM U WHERE id = 999")) {
          rid = rs.next().<RID>getProperty("r");
        }
        assertThat(((Number) view.getProperty(view.getNodeId(rid), "id")).intValue()).as("%s, rep %d", mode, rep).isEqualTo(999);
      } finally {
        if (database.isOpen())
          database.drop();
      }
    }
  }

  private void runOnce(final GraphAnalyticalView.UpdateMode mode, final int existing, final int inserts, final boolean edge)
      throws Exception {
    runOnce(mode, existing, inserts, edge, null);
  }

  /**
   * Base and U are vertex types, E1 an edge type; a view over Base and E1; {@code ALTER TYPE U SUPERTYPE +Base} with
   * {@code existing} vertices in U; then {@code inserts} new U vertices, each with an E1 edge from a Base vertex when
   * {@code edge}; the nodes {@code algo.wcc(['E1'])} answers with the view and after it is dropped must be the same.
   */
  private void runOnce(final GraphAnalyticalView.UpdateMode mode, final int existing, final int inserts, final boolean edge,
      final String afterJoin) throws Exception {
    database = new DatabaseFactory(DB_PATH).create();
    try {
      database.command("sql", "CREATE VERTEX TYPE Base").close();
      database.command("sql", "CREATE VERTEX TYPE U").close();
      database.command("sql", "CREATE EDGE TYPE E1").close();
      database.transaction(() -> {
        for (int k = 0; k < 4; k++)
          database.command("sql", "INSERT INTO Base SET id = " + k).close();
        for (int k = 0; k < existing; k++)
          database.command("sql", "INSERT INTO U SET id = " + (30 + k)).close();
        for (int k = 0; k < 3; k++)
          database.command("sql",
              "CREATE EDGE E1 FROM (SELECT FROM Base WHERE id = " + k + ") TO (SELECT FROM Base WHERE id = " + (k + 1) + ")").close();
      });
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (Base) EDGE TYPES (E1) UPDATE MODE " + mode).close();
      awaitReady();

      database.command("sql", "ALTER TYPE U SUPERTYPE +Base").close();
      if (afterJoin != null)
        database.transaction(() -> database.command("sql", afterJoin).close());
      for (int k = 0; k < inserts; k++) {
        final int id = 130 + k;
        database.transaction(() -> {
          database.command("sql", "INSERT INTO U SET id = " + id).close();
          if (edge)
            database.command("sql",
                "CREATE EDGE E1 FROM (SELECT FROM Base WHERE id = 0) TO (SELECT FROM U WHERE id = " + id + ")").close();
        });
      }
      awaitReady();

      final Set<String> withView = wccNodes();
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g").close();
      final Set<String> withoutView = wccNodes();

      assertThat(withView).as("nodes of algo.wcc with the view (%s, existing=%d, inserts=%d, edge=%s)", mode, existing, inserts, edge)
          .isEqualTo(withoutView);
    } finally {
      if (database.isOpen())
        database.drop();
    }
  }

  private void awaitReady() {
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS).until(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:graphAnalyticalViews WHERE name = 'g'")) {
        return rs.hasNext() && "READY".equals(String.valueOf(rs.next().<Object>getProperty("status")));
      }
    });
  }

  private Set<String> wccNodes() {
    final Set<String> nodes = new TreeSet<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.wcc(['E1']) YIELD node, componentId RETURN node.id AS pid, componentId AS c")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        nodes.add(String.valueOf(row.<Object>getProperty("pid")));
      }
    }
    return nodes;
  }
}
