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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.executor.steps.DeleteStep;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12521: a {@code DELETE r} (or {@code DETACH DELETE r}) of a relationship ran on the streaming
 * path, so the first pull batch deleted the relationship while a later MATCH expansion of the same edge list was still to
 * start for the next rows. The expansions of those rows no longer found it, and the statement answered (and ran every
 * following write clause for) fewer rows than Neo4j, silently: {@code MATCH (a:V {k:0}) UNWIND range(1, 500) AS i MATCH
 * (a)-[r:F]->(b) DELETE r RETURN i} answered 100 rows. Neo4j reads the rows of the MATCH to completion first.
 * <p>
 * What reaches an edge list again is whatever starts another expansion after the delete of an earlier row: a row source
 * ahead of the pattern (UNWIND, a CALL subquery, an earlier segment forwarded by a WITH) and a second relationship pattern
 * of the same segment. A bulk delete of a segment that is one relationship pattern fed by one scan reaches every
 * relationship once, so it stays on the streaming path, which holds no row in memory.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherDeleteRelationshipReExpansionIssue12521Test extends TestHelper {
  private static final int FAN_OUT = 500;
  private static final int RING    = 300;

  private void createTypes() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
  }

  /** The graph of the report: one relationship, k = 0 to k = 1. */
  private void createSingleRelationship() {
    createTypes();
    database.transaction(() -> database.command("opencypher", "CREATE (:V {k: 0})-[:F]->(:V {k: 1})"));
  }

  /** One vertex k = 0 with a relationship to each of the {@code fanOut} vertices k = 1..fanOut. */
  private void createFan(final int fanOut) {
    createTypes();
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, " + fanOut + ") AS i CREATE (:V {k: i})");
      database.command("opencypher", "MATCH (a:V {k: 0}), (b:V) WHERE b.k > 0 CREATE (a)-[:F]->(b)");
    });
  }

  /** {@link #RING} vertices k = 0..RING-1, each with one relationship to the vertex k + 1 (the last one to k = 0). */
  private void createRing() {
    createTypes();
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, " + (RING - 1) + ") AS i CREATE (:V {k: i})");
      database.command("opencypher", "MATCH (a:V), (b:V) WHERE b.k = (a.k + 1) % " + RING + " CREATE (a)-[:F]->(b)");
    });
  }

  private long countEdges() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM F")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long rowsOf(final String cypher) {
    final long[] rows = { 0 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext()) {
          rs.next();
          rows[0]++;
        }
      }
    });
    return rows[0];
  }

  /** The {@code eagerMaterialize} flag of the DELETE the statement is planned with, which no result shows by itself. */
  private boolean eagerMaterializeOf(final String cypher) {
    final boolean[] eager = { false };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "PROFILE " + cypher)) {
        while (rs.hasNext())
          rs.next();
        final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
        for (final ExecutionStep step : plan.getSteps())
          if (step instanceof DeleteStep deleteStep) {
            eager[0] = eagerMaterializeFlag(deleteStep);
            return;
          }
        throw new IllegalStateException("No DeleteStep found in the execution plan of " + cypher);
      }
    });
    return eager[0];
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

  // THE REPORT

  @Test
  void unwindBeforeTheExpansionKeepsEveryRow() {
    createSingleRelationship();
    assertThat(rowsOf("MATCH (a:V {k: 0}) UNWIND range(1, 500) AS i MATCH (a)-[r:F]->(b) DELETE r RETURN i")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  @Test
  void detachDeleteAfterUnwindKeepsEveryRow() {
    createSingleRelationship();
    assertThat(rowsOf("MATCH (a:V {k: 0}) UNWIND range(1, 500) AS i MATCH (a)-[r:F]->(b) DETACH DELETE r RETURN i")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  @Test
  void unlabelledEndsKeepEveryRow() {
    createSingleRelationship();
    assertThat(rowsOf("MATCH (a {k: 0}) UNWIND range(1, 500) AS i MATCH (a)-[r]->(b) DELETE r RETURN i")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  /** The walk of an earlier relationship pattern forwarded by a WITH starts the expansion of the next one for every row. */
  @Test
  void expansionAfterAWithForwardingAWalkKeepsEveryRow() {
    createFan(50);
    assertThat(rowsOf("MATCH (a:V {k: 0})-[:F]->(b) WITH a MATCH (a)-[s:F]->(c) DELETE s RETURN 1 AS one")).isEqualTo(2500);
    assertThat(countEdges()).isZero();
  }

  /** Rows that an UNWIND produced before a WITH reach the expansion after it. */
  @Test
  void expansionAfterAWithForwardingAnUnwindKeepsEveryRow() {
    createSingleRelationship();
    assertThat(rowsOf("UNWIND range(1, 500) AS i WITH i MATCH (a:V {k: 0})-[r:F]->(b) DELETE r RETURN i")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  /** One line of the file per row, 500 of them: each row starts the expansion again. */
  @Test
  void expansionAfterALoadCsvKeepsEveryRow() throws IOException {
    createSingleRelationship();
    final Path dir = Files.createDirectories(Path.of("./target/test-data/cypher-delete-reexpansion-12521"));
    final Path file = dir.resolve("rows.csv");
    final StringBuilder csv = new StringBuilder("id\n");
    for (int i = 1; i <= 500; i++)
      csv.append(i).append('\n');
    Files.writeString(file, csv.toString());
    assertThat(rowsOf("LOAD CSV WITH HEADERS FROM '" + file.toAbsolutePath().toUri()
        + "' AS row MATCH (a:V {k: 0})-[r:F]->(b) DELETE r RETURN row.id AS id")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  @Test
  void expansionAfterAProcedureCallIsPlannedEager() {
    createFan(5);
    assertThat(eagerMaterializeOf("CALL meta.stats() YIELD value MATCH (a:V {k: 0})-[r:F]->(b) DELETE r")).isTrue();
  }

  @Test
  void expansionAfterACallSubqueryKeepsEveryRow() {
    createSingleRelationship();
    assertThat(rowsOf(
        "MATCH (a:V {k: 0}) CALL (a) { UNWIND range(1, 500) AS i RETURN i } MATCH (a)-[r:F]->(b) DELETE r RETURN i")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  /** The body of a FOREACH deletes per row, behind the expansions of the rows before it: a row after the first finds none. */
  @Test
  void foreachBodyDeletingTheRelationshipAfterAnUnwindKeepsEveryRow() {
    createSingleRelationship();
    assertThat(rowsOf(
        "MATCH (a:V {k: 0}) UNWIND range(1, 500) AS i MATCH (a)-[r:F]->(b) FOREACH (x IN [r] | DELETE x) RETURN i")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  /** The body of a CALL subquery runs per row of the walk behind it, and the rows must all be there when it does. */
  @Test
  void subqueryBodyDeletingTheRelationshipAfterAnUnwindKeepsEveryRow() {
    createSingleRelationship();
    assertThat(rowsOf(
        "MATCH (a:V {k: 0}) UNWIND range(1, 500) AS i MATCH (a)-[r:F]->(b) CALL (r) { DELETE r } RETURN i")).isEqualTo(500);
    assertThat(countEdges()).isZero();
  }

  @Test
  void foreachBodyOverASinglePatternStaysStreaming() {
    createFan(5);
    // the FOREACH is no DeleteStep: reading its rows first is what the heap limit would refuse
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 3L);
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r:F]->(b) FOREACH (x IN [r] | DELETE x) RETURN 1 AS one")).isEqualTo(5);
    assertThat(countEdges()).isZero();
  }

  // A SECOND RELATIONSHIP PATTERN OF THE SEGMENT

  /** Two MATCH clauses, planned by the optimizer: the second reads the edge list of {@code a} for every row of the first. */
  @Test
  void secondMatchClauseReadingTheSameEdgeListKeepsEveryRow() {
    createFan(50);
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r:F]->(b) MATCH (a)-[s:F]->(c) DELETE r RETURN 1 AS one")).isEqualTo(2500);
    assertThat(countEdges()).isZero();
  }

  @Test
  void secondMatchClauseOfUnlabelledNodesKeepsEveryRow() {
    createFan(50);
    assertThat(rowsOf("MATCH (a {k: 0})-[r]->(b) MATCH (a)-[s]->(c) DELETE r RETURN 1 AS one")).isEqualTo(2500);
    assertThat(countEdges()).isZero();
  }

  @Test
  void secondPatternOfTheSameMatchKeepsEveryRow() {
    createFan(50);
    // a relationship is matched once per MATCH clause: 50 x 49 pairs
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r:F]->(b), (a)-[s:F]->(c) DELETE r RETURN 1 AS one")).isEqualTo(2450);
    assertThat(countEdges()).isZero();
  }

  @Test
  void detachDeleteOfTheFirstRelationshipOfTwoKeepsEveryRow() {
    createFan(50);
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r:F]->(b) MATCH (a)-[s:F]->(c) DETACH DELETE r RETURN 1 AS one")).isEqualTo(2500);
    assertThat(countEdges()).isZero();
  }

  /** The delete names the second relationship: the rows of the first one still reach the walk of the second. */
  @Test
  void deleteOfTheSecondRelationshipOfTwoKeepsEveryRow() {
    createFan(50);
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r:F]->(b) MATCH (a)-[s:F]->(c) DELETE s RETURN 1 AS one")).isEqualTo(2500);
    assertThat(countEdges()).isZero();
  }

  /**
   * One path of two hops over a ring: the row of the relationship r(i) also reads r(i + 1), which an earlier row deleted for
   * the one that closes the ring. Neo4j answers a row per relationship.
   */
  @Test
  void twoHopsOfOnePathOverARingKeepEveryRow() {
    createRing();
    assertThat(rowsOf("MATCH (a:V)-[r:F]->(b:V)-[s:F]->(c:V) DELETE r RETURN 1 AS one")).isEqualTo(RING);
    assertThat(countEdges()).isZero();
  }

  /** A relationship matched without a direction is reached from both of its ends: one row per end, as Neo4j answers. */
  @Test
  void undirectedPatternReachesEveryRelationshipFromBothEnds() {
    createRing();
    assertThat(rowsOf("MATCH (a:V)-[r:F]-(b:V) DELETE r RETURN 1 AS one")).isEqualTo(2 * RING);
    assertThat(countEdges()).isZero();
  }

  @Test
  void undirectedPatternOfUnlabelledNodesReachesEveryRelationshipFromBothEnds() {
    createRing();
    assertThat(rowsOf("MATCH ()-[r:F]-() DELETE r RETURN 1 AS one")).isEqualTo(2 * RING);
    assertThat(countEdges()).isZero();
  }

  // THE SHAPES THAT REACH EVERY RELATIONSHIP ONCE STAY ON THE STREAMING PATH

  @Test
  void bulkDeleteOfEveryRelationshipStaysStreaming() {
    createFan(FAN_OUT);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    assertThat(rowsOf("MATCH ()-[r:F]->() DELETE r RETURN 1 AS one")).isEqualTo(FAN_OUT);
    assertThat(countEdges()).isZero();
  }

  @Test
  void bulkDetachDeleteOfEveryRelationshipStaysStreaming() {
    createFan(FAN_OUT);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r:F]->(b) DETACH DELETE r RETURN 1 AS one")).isEqualTo(FAN_OUT);
    assertThat(countEdges()).isZero();
  }

  @Test
  void deleteOfARelationshipOfOnePatternIsPlannedStreaming() {
    createFan(5);
    assertThat(eagerMaterializeOf("MATCH ()-[r:F]->() DELETE r")).isFalse();
  }

  @Test
  void deleteOfARelationshipAfterANodeOnlyMatchIsPlannedStreaming() {
    createFan(5);
    assertThat(eagerMaterializeOf("MATCH (a:V {k: 0}) MATCH (a)-[r:F]->(b) DELETE r")).isFalse();
  }

  /** A WITH over no row but the one the statement starts from hands the expansion that one row. */
  @Test
  void deleteAfterALeadingWithIsPlannedStreaming() {
    createFan(5);
    assertThat(eagerMaterializeOf("WITH 0 AS x MATCH (a:V {k: x})-[r:F]->(b) DELETE r")).isFalse();
  }

  /** An aggregation without a grouping key answers one row, whatever it read. */
  @Test
  void deleteAfterAnAggregatingWithOfOneRowIsPlannedStreaming() {
    createFan(5);
    assertThat(eagerMaterializeOf("MATCH (n:V) WITH count(n) AS c MATCH (a:V {k: 0})-[r:F]->(b) DELETE r")).isFalse();
  }

  /** A WITH over rows hands the expansion that many rows, and a property of them is no proof that none repeats. */
  @Test
  void deleteAfterAWithOverRowsIsPlannedEager() {
    createFan(5);
    assertThat(eagerMaterializeOf("MATCH (n:V) WITH n.k AS k MATCH (a:V {k: k})-[r:F]->(b) DELETE r")).isTrue();
  }

  /** The row of the delete carries the relationship already: nothing starts another expansion behind the walk. */
  @Test
  void deleteOfARelationshipForwardedByAWithIsPlannedStreaming() {
    createFan(5);
    assertThat(eagerMaterializeOf("MATCH (a:V {k: 0})-[r:F]->(b) WITH r DELETE r")).isFalse();
  }

  // THE SHAPES THAT START ANOTHER EXPANSION ARE READ FIRST, UNDER THE HEAP LIMIT

  @Test
  void deleteBehindAnUnwindReadsItsRowsUnderTheHeapLimit() {
    createSingleRelationship();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    assertThatThrownBy(() -> rowsOf("MATCH (a:V {k: 0}) UNWIND range(1, 500) AS i MATCH (a)-[r:F]->(b) DELETE r RETURN i"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    assertThat(countEdges()).as("the failed delete was rolled back").isEqualTo(1);
  }

  @Test
  void deleteBehindASecondMatchIsPlannedEager() {
    createFan(5);
    assertThat(eagerMaterializeOf("MATCH (a:V {k: 0})-[r:F]->(b) MATCH (a)-[s:F]->(c) DELETE r")).isTrue();
  }
}
