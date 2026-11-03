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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11005: a value projected from a property before a later clause writes that property must be the value the
 * property had when the projection ran, for every row, whatever the pull batch size.
 * <p>
 * Issue #11004: an empty FOREACH inside a {@code CALL (n) {..} IN TRANSACTIONS} must not change what the outer row reads
 * from a node written earlier in the same query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11004And11005StaleProjectionTest extends TestHelper {
  private static final String EAGER = "EAGER";

  @Test
  void projectionBeforeRemoveOfTheSameKeyKeepsTheValueForEveryRow() {
    final List<Object> snapshots = column("""
        CALL {
          CREATE (n:N {k: 42})
          UNWIND range(1, 101) AS duplicate
          RETURN n
        }
        WITH *, n.k AS snapshot
        REMOVE n.k
        RETURN snapshot""", "snapshot");
    assertThat(snapshots).hasSize(101).containsOnly(42L);
  }

  @Test
  void projectionBeforeRemoveWithRedundantWithStarIsUnchanged() {
    final List<Object> snapshots = column("""
        CALL {
          CREATE (n:N {k: 42})
          WITH *
          UNWIND range(1, 101) AS duplicate
          WITH *
          WITH *
          RETURN n
        }
        WITH *
        WITH *, n.k AS snapshot
        WITH *
        REMOVE n.k
        WITH *
        RETURN snapshot""", "snapshot");
    assertThat(snapshots).hasSize(101).containsOnly(42L);
  }

  @Test
  void projectionBeforeSetOfTheSameKeyOnARepeatedNodeKeepsTheValueForEveryRow() {
    database.command("opencypher", "CREATE (:M {k: 1})");
    final List<Object> snapshots = column("""
        MATCH (n:M)
        UNWIND range(1, 250) AS i
        WITH n, n.k AS before
        SET n.k = n.k + 1
        RETURN before""", "before");
    assertThat(snapshots).hasSize(250).containsOnly(1L);
    assertThat(column("MATCH (n:M) RETURN n.k AS k", "k")).containsExactly(251L);
  }

  @Test
  void emptyForeachInsideCallInTransactionsDoesNotStaleTheOuterRow() {
    database.command("opencypher", "CREATE ({k: 0})");
    final List<Object> current = column("""
        MATCH (n)
        SET n.k = 42
        CALL (n) {
          FOREACH (x IN [] | CREATE (:Never))
          RETURN 1 AS seen
        }
        IN TRANSACTIONS OF 1 ROWS
        RETURN n.k AS current""", "current");
    assertThat(current).containsExactly(42L);
    assertThat(column("MATCH (n) RETURN n.k AS stored", "stored")).containsExactly(42L);
    assertThat(column("MATCH (n:Never) RETURN count(n) AS c", "c")).containsExactly(0L);
  }

  @Test
  void callInTransactionsWithoutForeachReadsTheWrittenValue() {
    database.command("opencypher", "CREATE ({k: 0})");
    final List<Object> current = column("""
        MATCH (n)
        SET n.k = 42
        CALL (n) {
          RETURN 1 AS seen
        }
        IN TRANSACTIONS OF 1 ROWS
        RETURN n.k AS current""", "current");
    assertThat(current).containsExactly(42L);
  }

  @Test
  void projectionBeforeSetThroughAnotherVariableKeepsTheValueForEveryRow() {
    final List<Object> snapshots = column("""
        CREATE (b:B {k: 1})
        WITH b
        UNWIND range(1, 150) AS i
        CREATE (a:A {i: i})-[:R]->(b)
        WITH a, b AS target, b.k AS snapshot
        SET target.k = target.k + 1
        RETURN snapshot""", "snapshot");
    assertThat(snapshots).hasSize(150).containsOnly(1L);
    assertThat(column("MATCH (b:B) RETURN b.k AS k", "k")).containsExactly(151L);
  }

  @Test
  void projectionBeforeForeachSetKeepsTheValueForEveryRow() {
    final List<Object> snapshots = column("""
        CREATE (n:F {k: 7})
        WITH n
        UNWIND range(1, 120) AS i
        WITH n, n.k AS snapshot
        FOREACH (ignored IN [1] | SET n.k = null)
        RETURN snapshot""", "snapshot");
    assertThat(snapshots).hasSize(120).containsOnly(7L);
  }

  @Test
  void projectionBeforeAWritingSubqueryKeepsTheValueForEveryRow() {
    final List<Object> snapshots = column("""
        CREATE (n:S {k: 3})
        WITH n
        UNWIND range(1, 120) AS i
        WITH n, n.k AS snapshot
        CALL (n) { SET n.k = 0 }
        RETURN snapshot""", "snapshot");
    assertThat(snapshots).hasSize(120).containsOnly(3L);
  }

  @Test
  void projectionBeforeMergeOnMatchSetKeepsTheValueForEveryRow() {
    final List<Object> snapshots = column("""
        CREATE (g:G {id: 1, k: 5})
        WITH g
        UNWIND range(1, 120) AS i
        WITH g, g.k AS snapshot
        MERGE (m:G {id: 1}) ON MATCH SET m.k = 0
        RETURN snapshot""", "snapshot");
    assertThat(snapshots).hasSize(120).containsOnly(5L);
  }

  @Test
  void createNamingAMergedNodeDoesNotMakeItOnePerRow() {
    database.command("opencypher", "UNWIND range(1, 150) AS i CREATE (:X {i: i})");
    database.command("opencypher", "CREATE (:Q {id: 1, k: 0})");
    column("MATCH (x:X) MERGE (m:Q {id: 1}) CREATE (m)-[:R]->(:Z {v: m.k}) SET m.k = m.k + 1", "m");
    assertThat(column("MATCH (z:Z) RETURN z.v AS v", "v")).hasSize(150).containsOnly(0L);
    assertThat(column("MATCH (m:Q) RETURN m.k AS k", "k")).containsExactly(150L);
    // the merged node is the same on every row, so the SET waits for the CREATE of every row to read m.k first
    final String plan = explain("MATCH (x:X) MERGE (m:Q {id: 1}) CREATE (m)-[:R]->(:Z {v: m.k}) SET m.k = m.k + 1");
    assertThat(plan.substring(plan.indexOf("+ CREATE"))).contains(EAGER);
  }

  @Test
  void barrierIsPlannedWhereTheProjectionCanSeeAnotherRowsWrite() {
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b.k = 0 RETURN s")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s REMOVE b.k RETURN s")).contains(EAGER);
    assertThat(explain("MATCH (n:P) UNWIND [1, 2] AS i WITH n, n.k AS s SET n.k = 0 RETURN s")).contains(EAGER);
    assertThat(explain("MATCH (n:P), (m:P) WITH n, m, m.k AS s SET n.k = 0 RETURN s")).contains(EAGER);
    assertThat(explain("MATCH (n:P) WITH n, n.k AS s, n AS m SET m.k = 0 RETURN s")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b += $props RETURN s")).contains(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, properties(b) AS p SET b.k = 0 RETURN p")).contains(EAGER);
  }

  @Test
  void barrierIsPlannedForEveryKindOfWrite() {
    // a MERGE action, and a write procedure, which can write any key
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s MERGE (m:Q {id: 1}) ON CREATE SET m.k = s RETURN s")).contains(EAGER);
    assertThat(explain("UNWIND range(1, 2) AS i CREATE (n:P {k: i}) WITH n, n.k AS s CALL merge.node(['Q'], {k: s}) YIELD node RETURN s"))
        .contains(EAGER);
    assertThat(explain("UNWIND range(1, 2) AS i CREATE (n:P {k: i}) WITH n CALL merge.node(['Q'], {k: 1}) YIELD node RETURN n"))
        .doesNotContain(EAGER);
    // the plan the optimizer builds: no WITH, the read is the right-hand side of an earlier SET
    assertThat(explain("MATCH (a)-[:R]->(b) SET a.x = b.k SET b.k = 0")).contains(EAGER);
    assertThat(explain("MATCH (n:P) SET n.x = n.k SET n.k = 0")).doesNotContain(EAGER);
    // a subquery that returns can hand on the same row several times
    assertThat(explain("MATCH (n:P) CALL (n) { RETURN 1 AS x UNION RETURN 2 AS x } WITH n, n.k AS s SET n.k = 0 RETURN s"))
        .contains(EAGER);
  }

  @Test
  void projectionThroughAVariableBoundOncePerRowKeepsStreaming() {
    assertThat(explain("MATCH (n:P) WITH n, n.k AS old SET n.k = old + 1 RETURN old")).doesNotContain(EAGER);
    assertThat(explain("MATCH (n:P) WITH n, n.k AS old REMOVE n.k RETURN old")).doesNotContain(EAGER);
    assertThat(explain("MATCH (n:P) WITH n WHERE n.k > 1 SET n.k = 0")).doesNotContain(EAGER);
    assertThat(explain("UNWIND range(1, 10) AS i CREATE (n:P {k: i}) WITH n, n.k AS s SET n.k = s * 2")).doesNotContain(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, count(*) AS c, b.k AS k SET b.k = k + c")).doesNotContain(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b.other = s RETURN s")).doesNotContain(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b += {other: s} RETURN s")).doesNotContain(EAGER);
  }

  @Test
  void readsOfRowsThatCannotHoldARecordKeepStreaming() {
    assertThat(explain("UNWIND [{id: 1, v: 2}] AS row WITH row, row.id AS id MATCH (n:P {id: id}) SET n += row")).doesNotContain(EAGER);
    // a MATCH's own pattern and WHERE choose the rows, they project nothing into them
    assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
    assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 SET b.k = 2")).doesNotContain(EAGER);
  }

  private String explain(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<Object> column(final String query, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.command("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final Object value = row.getProperty(column);
        values.add(value instanceof Number number ? (Object) number.longValue() : value);
      }
    }
    return values;
  }
}
