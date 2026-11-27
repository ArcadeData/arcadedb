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

import com.arcadedb.TestHelper;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #13653: a DELETE fed by a {@code CALL { }} subquery whose body reads the graph ran on the streaming
 * path. The delete pulls a batch of rows, which ran the body for the first input rows only, and deleted what they returned
 * before the body ran for the next input row: that run then read a graph the earlier rows had already emptied, and the statement
 * answered (and ran every following clause for) fewer rows than Neo4j, silently. On the report's graph (34 {@code :l10} nodes)
 * {@code UNWIND [1,2,3,4] AS x CALL (x) { MATCH (n:l10) RETURN n } DETACH DELETE n RETURN x} answered 102 rows instead of 136.
 * Neo4j reads the rows the body returns to completion before the first delete (its Eager operator).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherDeleteAfterReadingSubqueryIssue13653Test extends TestHelper {
  private static final int L10_NODES = 34;

  @BeforeEach
  void createGraph() {
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(1, 33) AS i CREATE (:l10 {i: i})");
      database.command("opencypher",
          "CREATE (:l10 {k2: true, k3: true, klist: ['r']})-[:rt4 {k0: -1981328940, k6: 1764292665}]->(:Target)");
    });
  }

  @Test
  void reportedLeftQueryAnswersEveryRow() {
    final List<String> rows = run("""
        UNWIND [1,2] AS alias0
        UNWIND [3,4] AS alias1
        CALL (alias1, alias0) {
          MATCH (n0:l10), (n1)-[:rt4]->()
          RETURN n1 AS n2, n0 AS n3
        }
        DETACH DELETE n2, n3
        RETURN alias1, alias0
        ORDER BY alias1 DESC, alias0 DESC""", "alias1", "alias0");

    assertThat(rows).hasSize(4 * L10_NODES);
    assertThat(rows).isEqualTo(expectedReportRows());
    assertOnlyTargetLeft();
  }

  @Test
  void reportedRightQueryAnswersTheSameRows() {
    final List<String> rows = run("""
        UNWIND [1,2] AS alias0
        UNWIND [3,4] AS alias1
        CALL (alias1, alias0) {
          MATCH (n0:l10), (n1)-[:rt4]->()
          RETURN n1 AS n2, n0 AS n3
        }
        CALL (alias1, alias0, n2, n3) {
          CALL (alias1, alias0, n2, n3) {
            OPTIONAL MATCH (leftNode)-[rel]-(rightNode)
            WHERE leftNode = alias1 AND rightNode = alias0
            RETURN count(rel) AS edgeCount
          }
          WITH edgeCount
          WHERE edgeCount >= 0
          RETURN 1 AS marker
        }
        WITH alias1, alias0, n2, n3
        DETACH DELETE n2, n3
        RETURN alias1, alias0
        ORDER BY alias1 DESC, alias0 DESC""", "alias1", "alias0");

    assertThat(rows).isEqualTo(expectedReportRows());
    assertOnlyTargetLeft();
  }

  @Test
  void minimalUnwindCallDetachDelete() {
    final List<String> rows = run("UNWIND [1,2,3,4] AS x CALL (x) { MATCH (n:l10) RETURN n } DETACH DELETE n RETURN x", "x");

    assertThat(rows).hasSize(4 * L10_NODES);
    assertOnlyTargetLeft();
  }

  @Test
  void importlessSubqueryRunsPerRowToo() {
    final List<String> rows = run("UNWIND [1,2,3,4] AS x CALL { MATCH (n:l10) RETURN n } DETACH DELETE n RETURN x", "x");

    assertThat(rows).hasSize(4 * L10_NODES);
    assertOnlyTargetLeft();
  }

  @Test
  void forwardingWithBetweenSubqueryAndDelete() {
    final List<String> rows = run("UNWIND range(1, 12) AS x CALL (x) { MATCH (n:l10) RETURN n } WITH x, n DETACH DELETE n RETURN x",
        "x");

    assertThat(rows).hasSize(12 * L10_NODES);
    assertOnlyTargetLeft();
  }

  @Test
  void nestedSubqueryReadsTheGraph() {
    final List<String> rows = run(
        "UNWIND [1,2,3,4] AS x CALL (x) { CALL (x) { MATCH (n:l10) RETURN n } RETURN n } DETACH DELETE n RETURN x", "x");

    assertThat(rows).hasSize(4 * L10_NODES);
    assertOnlyTargetLeft();
  }

  @Test
  void unionBranchReadsTheGraph() {
    final List<String> rows = run(
        "UNWIND [1,2,3,4] AS x CALL (x) { RETURN null AS n UNION MATCH (n:l10) RETURN n } DETACH DELETE n RETURN x", "x");

    assertThat(rows).hasSize(4 * (L10_NODES + 1));
    assertOnlyTargetLeft();
  }

  @Test
  void foreachDeleteAfterReadingSubquery() {
    final List<String> rows = run(
        "UNWIND [1,2,3,4] AS x CALL (x) { MATCH (n:l10) RETURN n } FOREACH (m IN [n] | DETACH DELETE m) RETURN x", "x");

    assertThat(rows).hasSize(4 * L10_NODES);
    assertOnlyTargetLeft();
  }

  @Test
  void deletingSubqueryAfterReadingSubquery() {
    final List<String> rows = run(
        "UNWIND [1,2,3,4] AS x CALL (x) { MATCH (n:l10) RETURN n } CALL (n) { DETACH DELETE n } RETURN x", "x");

    assertThat(rows).hasSize(4 * L10_NODES);
    assertOnlyTargetLeft();
  }

  @Test
  void plainDeleteOfIsolatedNodes() {
    final List<String> rows = run(
        "UNWIND range(1, 10) AS x CALL (x) { MATCH (n:l10) WHERE n.i IS NOT NULL RETURN n } DELETE n RETURN x", "x");

    assertThat(rows).hasSize(10 * (L10_NODES - 1));
    assertThat(countNodes("l10")).isEqualTo(1L);
  }

  @Test
  void bulkDeleteWithoutSubqueryStillDeletesEverything() {
    final List<String> rows = run("MATCH (n:l10) DETACH DELETE n RETURN n", "n");

    assertThat(rows).hasSize(L10_NODES);
    assertOnlyTargetLeft();
  }

  @Test
  void streamingIsKeptWhereNoSubqueryReadsAgain() {
    // the body runs once, on the one row the statement starts from
    assertThat(eagerMaterializeOf("CALL { MATCH (n:l10) RETURN n } DETACH DELETE n")).isFalse();
    // the body runs per row, but reads nothing of the graph
    assertThat(eagerMaterializeOf("UNWIND [1,2] AS x CALL (x) { RETURN x AS y } MATCH (n:l10 {i: y}) DELETE n")).isFalse();
    // an aggregating WITH reads every run of the body before the delete
    assertThat(eagerMaterializeOf(
        "UNWIND [1,2] AS x CALL (x) { MATCH (n:l10) RETURN n } WITH count(n) AS c MATCH (m:l10) DETACH DELETE m")).isFalse();
    // a bulk delete with no subquery
    assertThat(eagerMaterializeOf("MATCH (n:l10) DETACH DELETE n")).isFalse();

    assertThat(eagerMaterializeOf("UNWIND [1,2] AS x CALL (x) { MATCH (n:l10) RETURN n } DETACH DELETE n")).isTrue();
    assertThat(eagerMaterializeOf("MATCH (t:Target) CALL (t) { MATCH (n:l10) RETURN n } DETACH DELETE n")).isTrue();
    assertThat(eagerMaterializeOf("UNWIND [1,2] AS x CALL (x) { MATCH (n:l10) RETURN n } WITH x, n DETACH DELETE n")).isTrue();
    assertThat(eagerMaterializeOf(
        "UNWIND [1,2] AS x CALL (x) { RETURN COUNT { MATCH (m:l10) } AS c } MATCH (n:l10) DETACH DELETE n")).isTrue();
  }

  private List<String> expectedReportRows() {
    final List<String> expected = new ArrayList<>();
    for (final int alias1 : new int[] { 4, 3 })
      for (final int alias0 : new int[] { 2, 1 })
        for (int i = 0; i < L10_NODES; i++)
          expected.add(alias1 + "," + alias0);
    return expected;
  }

  /** The {@code eagerMaterialize} flag of the DELETE the statement is planned with, which no result shows by itself. */
  private boolean eagerMaterializeOf(final String cypher) {
    boolean eager = false;
    database.begin();
    try (final ResultSet rs = database.command("opencypher", "PROFILE " + cypher)) {
      while (rs.hasNext())
        rs.next();
      final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
      boolean found = false;
      for (final ExecutionStep step : plan.getSteps())
        if (step instanceof DeleteStep deleteStep) {
          eager = eagerMaterializeFlag(deleteStep);
          found = true;
        }
      assertThat(found).as("a DELETE step in the plan of %s", cypher).isTrue();
    } finally {
      database.rollback();
    }
    return eager;
  }

  private static boolean eagerMaterializeFlag(final DeleteStep step) {
    try {
      final Field field = DeleteStep.class.getDeclaredField("eagerMaterialize");
      field.setAccessible(true);
      return field.getBoolean(step);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private List<String> run(final String query, final String... columns) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder line = new StringBuilder();
          for (final String column : columns) {
            if (!line.isEmpty())
              line.append(',');
            line.append((Object) row.getProperty(column));
          }
          rows.add(line.toString());
        }
      }
    });
    return rows;
  }

  private void assertOnlyTargetLeft() {
    assertThat(countNodes("l10")).isZero();
    assertThat(countNodes("Target")).isEqualTo(1L);
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM rt4")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  private long countNodes(final String label) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + label)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
