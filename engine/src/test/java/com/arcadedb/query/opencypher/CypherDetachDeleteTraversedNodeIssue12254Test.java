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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12254: a {@code DETACH DELETE} of a node bound by a relationship pattern ran on the
 * streaming path, so the first row deleted every relationship of the anchor while the MATCH was still walking the
 * anchor's edge list. The walk then stopped early and the remaining rows were never produced:
 * {@code MATCH (a {k:0})-[r]->(b) DETACH DELETE a, b} over 500 relationships deleted 166 nodes and answered 200, and a
 * {@code SET} on {@code b} in the same statement reached a third of them. Neo4j reads the MATCH to completion first.
 * <p>
 * Every consumer of the rows of such a MATCH carries the hazard, not only a DELETE written right after it: a WITH, an UNWIND,
 * a FOREACH body and a CALL subquery body hand the entity on while the walk is still open, so each of them is covered, and so
 * is the statement that reaches a node only through a relationship variable ({@code endNode(r)}).
 */
class CypherDetachDeleteTraversedNodeIssue12254Test extends TestHelper {
  private static final int FAN_OUT = 500;

  private void createFan() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, " + FAN_OUT + ") AS i CREATE (:V {k: i})");
      database.command("opencypher", "MATCH (a:V {k: 0}), (b:V) WHERE b.k > 0 CREATE (a)-[:F]->(b)");
    });
    assertThat(countVertices()).isEqualTo(FAN_OUT + 1);
  }

  private void createFan(final int fanOut) {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, " + fanOut + ") AS i CREATE (:V {k: i})");
      database.command("opencypher", "MATCH (a:V {k: 0}), (b:V) WHERE b.k > 0 CREATE (a)-[:F]->(b)");
    });
  }

  private long countEdges() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM F")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long countVertices() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM V")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long countWithX() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM V WHERE x = 1")) {
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

  @Test
  void detachDeleteOfAnchorAndNeighboursDeletesEveryMatchedNode() {
    createFan();
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a, b RETURN b")).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isZero();
  }

  @Test
  void reversedPatternDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (b)<-[r]-(a:V {k: 0}) DETACH DELETE a, b");
    assertThat(countVertices()).isZero();
  }

  @Test
  void countOfMatchedNeighboursIsNotCutShort() {
    createFan();
    final long[] count = { -1 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a RETURN count(b) AS c")) {
        count[0] = rs.next().<Number>getProperty("c").longValue();
      }
    });
    assertThat(count[0]).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isEqualTo(FAN_OUT);
  }

  @Test
  void setBeforeDetachDeleteReachesEveryNeighbour() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) SET b.x = 1 DETACH DELETE a");
    assertThat(countWithX()).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isEqualTo(FAN_OUT);
  }

  @Test
  void setAfterDetachDeleteReachesEveryNeighbour() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a SET b.x = 1");
    assertThat(countWithX()).isEqualTo(FAN_OUT);
  }

  @Test
  void detachDeleteAfterWithPassthroughDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) WITH a, b DETACH DELETE a, b");
    assertThat(countVertices()).isZero();
  }

  @Test
  void secondDetachDeleteAfterWithDeletesEveryNeighbour() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a WITH b DETACH DELETE b");
    assertThat(countVertices()).isZero();
  }

  /** A list projection of the traversed nodes carries the hazard of the nodes it holds. */
  @Test
  void detachDeleteOfListProjectionDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r:F]->(b) WITH [a, b] AS victims DETACH DELETE victims");
    assertThat(countVertices()).isZero();
  }

  /** Neo4j deletes y and z: both rows of the chain x->y->z are read before either target is deleted. */
  @Test
  void chainDetachDeleteOfTargetsMatchesNeo4j() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:V {k: 'x'})-[:F]->(:V {k: 'y'})-[:F]->(:V {k: 'z'})"));
    rowsOf("MATCH (a:V)-[r]->(b:V) DETACH DELETE b");
    final List<String> left = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM V ORDER BY k")) {
      while (rs.hasNext())
        left.add(rs.next().getProperty("k"));
    }
    assertThat(left).containsExactly("x");
  }

  /** A node-only MATCH keeps the streaming path: nothing it reads is reached by the DETACH of another row. */
  @Test
  void nodeOnlyDetachDeleteStillDeletesEverything() {
    createFan();
    rowsOf("MATCH (n:V) DETACH DELETE n");
    assertThat(countVertices()).isZero();
  }

  /** Both ends labelled: the statement is planned by the optimizer, whose DeleteStep has its own construction site. */
  @Test
  void labelledEndsPlannedByTheOptimizerDeleteEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r:F]->(b:V) DETACH DELETE a, b");
    assertThat(countVertices()).isZero();
  }

  @Test
  void pathVariableTargetDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH p = (a:V {k: 0})-[r]->(b) DETACH DELETE p");
    assertThat(countVertices()).isZero();
  }

  /** The UNWIND hands the nodes on one at a time while the walk behind it is still open. */
  @Test
  void unwindOfTraversedNodesDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) UNWIND [a, b] AS victim DETACH DELETE victim");
    assertThat(countVertices()).isZero();
  }

  @Test
  void unwindOfAListProjectedByAWithDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) WITH [a, b] AS victims UNWIND victims AS victim DETACH DELETE victim");
    assertThat(countVertices()).isZero();
  }

  /** The body of a FOREACH deletes per row, behind the walk of the MATCH that feeds it. */
  @Test
  void foreachBodyDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) FOREACH (victim IN [a, b] | DETACH DELETE victim)");
    assertThat(countVertices()).isZero();
  }

  @Test
  void foreachBodyNamingTheMatchedNodesDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) FOREACH (i IN [1] | DETACH DELETE a, b)");
    assertThat(countVertices()).isZero();
  }

  /** The body of a CALL subquery runs once per row of the walk behind it. */
  @Test
  void callSubqueryBodyDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) CALL (a, b) { DETACH DELETE a, b }");
    assertThat(countVertices()).isZero();
  }

  @Test
  void implicitImportSubqueryBodyDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) CALL { WITH a, b DETACH DELETE a, b }");
    assertThat(countVertices()).isZero();
  }

  /** The far end is anonymous: it is reached through the relationship variable, which names no node variable. */
  @Test
  void anonymousEndpointReachedThroughAnAliasIsDeletedForEveryRelationship() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->() WITH endNode(r) AS b DETACH DELETE b");
    assertThat(countVertices()).isEqualTo(1);
  }

  @Test
  void anonymousEndpointReachedThroughAnAliasKeepsTheRowsAfterTheDelete() {
    createFan();
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r]->() WITH a, endNode(r) AS b DETACH DELETE a RETURN b")).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isEqualTo(FAN_OUT);
  }

  /** A WITH item that is no entity (b.k) does not end the walk: the rows behind it are still produced one at a time. */
  @Test
  void withOfAScalarComputedFromTheNeighbourKeepsEveryRow() {
    createFan();
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r]->(b) WITH a, b.k AS m DETACH DELETE a RETURN m")).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isEqualTo(FAN_OUT);
  }

  /**
   * The node deleted is bound by a node-only MATCH of a later segment, fed by the rows of the walk: it names none of the
   * variables of the relationship pattern, and its DETACH still removes the relationships the walk has not reached.
   */
  @Test
  void nodeOnlyMatchAfterAWithFedByTheWalkDeletesEveryNeighbour() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) WITH b MATCH (n:V) WHERE n.k = b.k DETACH DELETE n");
    assertThat(countVertices()).isEqualTo(1);
  }

  /**
   * The mixed graph of the report: several vertex and edge types, a hub of 4,500 relationships and a WHERE on the far end.
   * The report saw a RecordNotFoundException on its own graph; this fixture does not reproduce that error without the guard,
   * so it pins the result of the eager read on a hub of several types: only the hub and the matched neighbours go.
   */
  @Test
  void hubOfSeveralTypesWithAWhereOnTheFarEndDeletesEveryMatchedNode() {
    database.getSchema().createVertexType("V");
    database.getSchema().createVertexType("W");
    database.getSchema().createEdgeType("F");
    database.getSchema().createEdgeType("G");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 3000) AS i CREATE (:V {k: i})");
      database.command("opencypher", "UNWIND range(1, 1500) AS i CREATE (:W {k: i})");
      database.command("opencypher", "MATCH (a:V {k: 0}), (b:V) WHERE b.k > 0 CREATE (a)-[:F]->(b)");
      database.command("opencypher", "MATCH (a:V {k: 0}), (b:W) CREATE (a)-[:G]->(b)");
    });
    rowsOf("MATCH (a {k: 0})-[r]->(b) WHERE b.k < 30 DETACH DELETE a, b");
    // the hub and the 29 + 29 neighbours with k < 30 are gone, nothing else
    assertThat(countVertices()).isEqualTo(3001 - 1 - 29);
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM W")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1500 - 29);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM F")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /**
   * The relationship deleted is an entry of the edge list the first MATCH is still walking, read again by the second MATCH
   * for every row of the first: reading the rows first is what keeps all 50 x 50 of them (the streaming path answered 100).
   * A delete that names only the relationships of its own segment is streaming only while no earlier walk is open.
   */
  @Test
  void relationshipDeletedBehindAnEarlierOpenWalkKeepsEveryRow() {
    createFan(50);
    assertThat(rowsOf("MATCH (a:V {k: 0})-[:F]->(b) WITH a MATCH (a)-[s:F]->(c) DETACH DELETE s RETURN 1 AS one")).isEqualTo(2500);
    assertThat(countEdges()).isZero();
  }

  /** A WITH that sorts reads the walk to the end, like one that aggregates: what follows it is decided on its own. */
  @Test
  void deleteAfterASortingWithDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) WITH a, b ORDER BY b.k DETACH DELETE a, b");
    assertThat(countVertices()).isZero();
  }

  /** Deleting the matched relationship itself never disturbs the walk: it stays on the streaming path. */
  @Test
  void relationshipOnlyDetachDeleteKeepsStreaming() {
    createFan();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE r");
    assertThat(countVertices()).isEqualTo(FAN_OUT + 1);
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:F]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /** Reading the rows first is what bounds a node delete by the heap limit: the same cap fails it where it spares the one above. */
  @Test
  void nodeDetachDeleteBehindARelationshipPatternReadsItsRowsUnderTheHeapLimit() {
    createFan();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    assertThatThrownBy(() -> rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE b"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    assertThat(countVertices()).as("the failed delete was rolled back").isEqualTo(FAN_OUT + 1);
  }

  @Test
  void nodeOnlyDetachDeleteKeepsStreamingUnderTheHeapLimit() {
    createFan();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    rowsOf("MATCH (n:V) DETACH DELETE n");
    assertThat(countVertices()).isZero();
  }

  /** A DELETE without DETACH refuses a node that still has a relationship: it has no walk to race and keeps streaming. */
  @Test
  void plainDeleteOfTheRelationshipsKeepsStreamingUnderTheHeapLimit() {
    createFan();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) DELETE r");
    assertThat(countVertices()).isEqualTo(FAN_OUT + 1);
    assertThat(countEdges()).isZero();
  }

  /** An aggregating WITH drains the walk before it emits its first row: what follows it streams again. */
  @Test
  void deleteAfterAnAggregatingWithKeepsStreamingUnderTheHeapLimit() {
    createFan();
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 10L);
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) WITH count(*) AS c MATCH (n:V) DETACH DELETE n");
    assertThat(countVertices()).isZero();
  }
}
