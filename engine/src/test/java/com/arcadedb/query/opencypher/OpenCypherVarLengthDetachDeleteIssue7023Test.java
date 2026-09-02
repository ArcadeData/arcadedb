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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #7023: {@code MATCH p = ()<-[*1..6]-(n0) DETACH DELETE n0} failed with
 * {@code RecordNotFoundException: Record #12:0 not found} raised from inside {@code MutableEdgeSegment
 * #getPrevious()}.
 * <p>
 * A variable-length relationship is expanded by {@code DepthFirstTraverser}, which keeps edge-segment
 * cursors open across the rows it is still producing. The DELETE ran on the streaming path, so the very
 * first row's {@code DETACH DELETE n0} unlinked the edge chunks that same cursor was about to follow, and
 * the next {@code hasNext()} dereferenced an already-removed segment record. That is the issue #6491
 * hazard - a row observing what an earlier row's write already removed - reached through the traversal
 * cursor instead of through a disconnected pattern's cross join, so it gets the same remedy: the DELETE
 * reads the upstream MATCH to completion before removing anything.
 */
class OpenCypherVarLengthDetachDeleteIssue7023Test {
  private static final String CREATE_NODES =
      "UNWIND [0,1,2,3,4,7,8,9,11,14,15,16,18,19,20,22,23,24,26,28,30,32,33,35,37,39,40,44,47,49,52,53,54,55,56,57,58,59,60,62,63,64,68,69,73,74,75,76,77,78,79,80,81,82,84,85,87,89,91,92,94,95,96,97,98,99,100,101,102,108,110,111,112,116,117,119,120,121,122,123,124,126,127] AS i CREATE (:V {id: i})";

  private static final String CREATE_EDGES =
      "UNWIND [[73,85],[52,77],[116,56],[47,74],[58,16],[108,94],[87,2],[76,94],[28,23],[39,97],[23,62],[108,47],[77,49],[111,7],[28,87],[39,87],[80,119],[68,52],[116,35],[35,112],[127,39],[11,49],[97,81],[26,28],[35,62],[44,77],[22,108],[18,49],[47,63],[73,73],[127,108],[39,108],[74,68],[44,2],[121,19],[63,35],[119,49],[14,122],[0,30],[101,8],[111,44],[120,68],[102,40],[108,55],[112,112],[108,85],[73,73],[28,28],[26,53],[60,112],[82,53],[99,76],[111,120],[116,108],[40,108],[1,19],[108,126],[80,68],[16,26],[49,22],[28,99],[85,62],[24,39],[110,47],[30,8],[35,14],[56,68],[54,37],[2,97],[116,3],[98,4],[14,7],[64,44],[7,122],[68,81],[80,39],[28,108],[47,89],[49,37],[124,108],[121,30],[99,102],[14,2],[91,7],[68,47],[64,94],[116,22],[94,26],[116,117],[28,1],[124,119],[9,112],[57,108],[116,23],[59,91],[78,87],[100,87],[124,39],[81,19],[49,49],[68,108],[89,32],[78,23],[73,9],[16,91],[52,14],[102,126],[68,55],[2,26],[68,96],[3,60],[56,2],[14,91],[3,3],[68,28],[22,91],[52,91],[89,37],[53,53],[47,73],[1,75],[7,68],[52,127],[119,4],[14,52],[23,19],[37,116],[28,60],[37,108],[84,52],[1,68],[39,99],[123,98],[30,91],[117,30],[119,102],[52,35],[99,24],[99,116],[116,68],[22,58],[117,23],[111,108],[110,22],[37,116],[7,52],[0,8],[95,77],[116,57],[23,116],[39,91],[19,100],[7,18],[68,68],[37,52],[116,11],[40,11],[108,121],[56,62],[112,22],[14,76],[15,19],[81,20],[112,49],[8,127],[89,68],[40,14],[120,39],[73,73],[37,37],[116,39],[58,11],[55,92],[126,80],[35,91],[116,8],[80,73],[4,110],[4,75],[47,75],[119,119],[44,32],[94,116],[23,52],[57,28],[7,108],[37,37],[7,121],[2,119],[111,110],[124,2],[26,102],[68,74],[121,30],[18,62],[119,58],[35,84],[8,2],[60,91],[59,82],[69,58],[64,32],[11,55],[120,60],[32,73],[121,94],[14,53],[123,117],[33,39],[122,35],[116,22],[73,79],[26,126],[122,74],[89,89],[54,78],[14,121],[80,116],[99,99],[47,30],[98,102],[110,60],[39,26],[98,98],[7,55],[116,3],[19,19],[14,112],[116,116],[75,30],[4,4],[32,32],[30,94],[32,14],[96,81],[68,0],[28,7],[32,15],[91,24],[78,47],[26,111],[22,47],[28,28],[52,121],[78,56],[44,60],[26,7],[32,49],[97,2],[62,62],[52,19],[54,39],[68,68],[56,58],[62,122]] AS e MATCH (a {id: e[0]}), (b {id: e[1]}) CREATE (a)-[:E]->(b)";

  private Database database;

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isOpen())
        database.drop();
      database = null;
    }
  }

  private static boolean eagerMaterializeOf(final Object step) {
    try {
      final Field field = step.getClass().getDeclaredField("eagerMaterialize");
      field.setAccessible(true);
      return field.getBoolean(step);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static <T> T findStep(final ResultSet result, final Class<T> type) {
    final Optional<ExecutionPlan> plan = result.getExecutionPlan();
    assertThat(plan).isPresent();
    for (final ExecutionStep step : plan.get().getSteps())
      if (type.isInstance(step))
        return type.cast(step);
    throw new IllegalStateException("No " + type.getSimpleName() + " found in execution plan");
  }

  private void createFixture(final String databaseName) {
    database = new DatabaseFactory("./target/databases/" + databaseName).create();
    database.transaction(() -> {
      database.command("opencypher", CREATE_NODES);
      database.command("opencypher", "MATCH (n) WHERE n.id IN [65, 66, 67, 68] SET n:l3:l2 SET n.k4 = -1012660514");
      database.command("opencypher", CREATE_EDGES);
      database.command("opencypher", "MATCH (a {id: 26}), (b {id: 52}) CREATE (a)-[:E]->(b)");
    });
  }

  /** Number of paths the MATCH alone produces, i.e. how many rows the DELETE variant must also return. */
  private long countPathsWithoutDelete() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH p = () <-[*1..6]-(n0:l3&l2 {k4: -1012660514}) RETURN count(p) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void detachDeleteWhileVariableLengthTraversalIsStillStreaming() {
    createFixture("testopencypher-issue7023");

    final long expectedRows = countPathsWithoutDelete();
    assertThat(expectedRows)
        .withFailMessage("fixture did not land: the variable-length MATCH must produce rows for this test to mean anything")
        .isGreaterThan(1L);

    final long[] returnedRows = { -1 };
    database.transaction(() -> assertThatCode(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH p = () <-[*1..6]-(n0:l3&l2 {k4: -1012660514}) DETACH DELETE n0 RETURN 1 AS ok")) {
        long rows = 0;
        while (rs.hasNext()) {
          assertThat(rs.next().<Number>getProperty("ok").intValue()).isEqualTo(1);
          rows++;
        }
        returnedRows[0] = rows;
      }
    }).doesNotThrowAnyException());

    assertThat(returnedRows[0])
        .withFailMessage("the DELETE must not truncate the traversal it is fed by")
        .isEqualTo(expectedRows);

    // The single node carrying both labels (id 68) and every edge incident to it are gone...
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:l3) RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n {id: 68}) RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
    // ...while the rest of the graph is untouched: 83 created nodes minus the one deleted.
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(82L);
    }
    // Only edges incident to id 68 disappear: 256 created, 19 of them touch node 68.
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(237L);
    }
  }

  @Test
  void deleteFedByAVariableLengthMatchIsEagerlyMaterialized() {
    createFixture("testopencypher-issue7023-plan");

    database.transaction(() -> {
      try (final ResultSet result = database.command("opencypher",
          "PROFILE MATCH p = () <-[*1..6]-(n0:l3&l2 {k4: -1012660514}) DETACH DELETE n0 RETURN 1 AS ok")) {
        while (result.hasNext())
          result.next();

        assertThat(eagerMaterializeOf(findStep(result, DeleteStep.class)))
            .withFailMessage("a DELETE fed by a variable-length MATCH must read the traversal to completion "
                + "before removing anything, or the still-open edge-segment cursor follows an unlinked chunk")
            .isTrue();
      }
    });
  }

  @Test
  void deleteOfAVariableLengthBoundVariableForwardedThroughWithIsEagerlyMaterialized() {
    createFixture("testopencypher-issue7023-with");

    database.transaction(() -> {
      try (final ResultSet result = database.command("opencypher",
          "PROFILE MATCH p = () <-[*1..6]-(n0:l3&l2 {k4: -1012660514}) WITH n0 DETACH DELETE n0 RETURN 1 AS ok")) {
        while (result.hasNext())
          result.next();

        assertThat(eagerMaterializeOf(findStep(result, DeleteStep.class)))
            .withFailMessage("a plain WITH forwards rows one at a time, so it does not neutralize the hazard "
                + "for a variable-length-bound variable deleted after it")
            .isTrue();
      }
    });
  }

  @Test
  void deleteFedByAPlainConnectedMatchStaysOnTheStreamingPath() {
    createFixture("testopencypher-issue7023-streaming");

    database.transaction(() -> {
      try (final ResultSet result = database.command("opencypher",
          "PROFILE MATCH (n:l3&l2 {k4: -1012660514}) DETACH DELETE n RETURN 1 AS ok")) {
        while (result.hasNext())
          result.next();

        assertThat(eagerMaterializeOf(findStep(result, DeleteStep.class)))
            .withFailMessage("a fixed-length, connected MATCH keeps no traversal cursor open across rows, so it "
                + "must not pay the eager-materialization memory cost")
            .isFalse();
      }
    });
  }
}
