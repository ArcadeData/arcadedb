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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #7149: a 2-hop variable-length traversal whose rows repeat the same node,
 * followed by {@code SET} and {@code DETACH DELETE} on that node, reported a transaction deadlock to a single
 * client ({@code Neo.TransientError.Transaction.DeadlockDetected} over Bolt).
 * <p>
 * Two independent defects met on that query. The traversal is now read to completion before the first deletion is
 * applied (the eager gate widened for issue #7023), so every {@code SET} lands while its node is still there -
 * which is what Neo4j's own Eager operator does for the same shape. And underneath, a write to a record the same
 * transaction had already deleted is now dropped rather than queued for a commit that reported it as a conflict
 * with a concurrent transaction that never existed - see
 * {@link com.arcadedb.database.Issue7149UpdateAfterDeleteInSameTxTest}.
 * <p>
 * The graph is the reporter's: 128 nodes and 72 relationships, chosen so the 2-hop pattern yields 256 rows over
 * 128 nodes and therefore binds most nodes from several rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherVarLengthDetachDeleteIssue7149Test {
  private static final String EDGES = "[[14,126],[72,112],[13,11],[57,54],[2,105],[64,13],[123,26],[60,75],[101,2],[70,51],"
      + "[59,2],[33,113],[73,52],[94,46],[73,93],[93,104],[64,41],[33,38],[27,72],[118,118],[85,24],[85,67],[51,89],[19,38],"
      + "[113,22],[59,78],[0,33],[76,76],[78,64],[15,16],[95,95],[9,38],[2,2],[67,2],[126,87],[42,86],[54,54],[48,101],"
      + "[107,50],[85,39],[116,13],[101,47],[5,84],[123,81],[93,26],[107,127],[57,116],[10,57],[65,65],[101,81],[22,22],"
      + "[65,65],[117,116],[27,42],[116,116],[26,93],[41,5],[35,38],[57,71],[86,47],[67,84],[107,124],[98,110],[67,28],"
      + "[48,48],[72,17],[52,95],[123,2],[93,127],[52,65],[98,105],[13,5]]";

  private Database database;

  @BeforeEach
  void setUp() {
    // No pre-created schema, exactly as the reporter's clean database: the Cypher setup creates every type.
    database = new DatabaseFactory("./target/databases/testopencypher-7149-detach-delete").create();
    database.command("opencypher", "UNWIND range(0, 127) AS id CREATE (:N {id: id})").close();
    database.command("opencypher",
        "UNWIND " + EDGES + " AS e MATCH (a {id: e[0]}), (b {id: e[1]}) CREATE (a)-[:R]->(b)").close();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private int countRows(final String query) {
    int rows = 0;
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private long countNodes(final String label) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /**
   * The state every successful run of the reproduction must leave behind: the 57 nodes the 2-hop pattern binds are
   * gone with their relationships, the 71 that no 2-hop path reaches are untouched, and no surviving node carries
   * the marker - a marked survivor would mean a node was written and then NOT deleted.
   */
  private void assertEveryTraversedNodeWasDeleted() {
    assertThat(countNodes("N")).as("only the nodes no 2-hop path reaches survive").isEqualTo(71);
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:N) WHERE n.marker IS NOT NULL RETURN count(n) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).as("no marked node survived the delete").isZero();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (a)-[*2]-() RETURN count(a) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).as("no 2-hop path is left").isZero();
    }
  }

  @Test
  void controlWithoutDetachDeleteReturnsEveryTwoHopPath() {
    // The issue's control case: it must keep returning the 256 rows the reporter measured, so the reproduction
    // below is known to be exercising a traversal that really does bind the same node from several rows.
    assertThat(countRows("""
        MERGE (:DeadlockProbe {id: 999995})
        MATCH p0 = (n2) -[*2]-()
        SET n2.marker = 'probe'
        RETURN null AS alias0""")).isEqualTo(256);
  }

  @Test
  void varLengthTraversalFollowedByDetachDeleteCompletes() {
    // The issue's reproduction, verbatim. Pre-fix: ConcurrentModificationException ("... was deleted by a
    // concurrent transaction"), retried arcadedb.txRetries times and reported as DeadlockDetected over Bolt.
    assertThat(countRows("""
        MERGE (:DeadlockProbe {id: 999995})
        MATCH p0 = (n2) -[*2]-()
        SET n2.marker = 'probe'
        DETACH DELETE n2
        CALL merge.node(['Person'], {name: 'ArcadeGenerated'}, {name: 'ArcadeGenerated'}) YIELD node AS alias3
        RETURN null AS alias0""")).isEqualTo(256);

    assertEveryTraversedNodeWasDeleted();
    assertThat(countNodes("Person")).as("the procedure merged one node, not one per row").isEqualTo(1);
  }

  @Test
  void varLengthTraversalFollowedByDetachDeleteCompletesInsideAnExplicitTransaction() {
    // The same statement inside a caller-owned transaction: the deferred write reaches a commit the statement does
    // not own, so nothing about the drop of a write to an already-deleted record may depend on who commits.
    database.transaction(() -> assertThat(countRows("""
        MERGE (:DeadlockProbe {id: 999995})
        MATCH p0 = (n2) -[*2]-()
        SET n2.marker = 'probe'
        DETACH DELETE n2
        CALL merge.node(['Person'], {name: 'ArcadeGenerated'}, {name: 'ArcadeGenerated'}) YIELD node AS alias3
        RETURN null AS alias0""")).isEqualTo(256));

    assertEveryTraversedNodeWasDeleted();
  }

  @Test
  void aSetOnANodeThatSurvivesIsStillWritten() {
    // Regression guard for the drop: only a write to a node this statement has already deleted may vanish.
    assertThat(countRows("MATCH (n:N {id: 5}) SET n.marker = 'kept' RETURN n")).isEqualTo(1);

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N {id: 5}) RETURN n.marker AS marker")) {
      assertThat(rs.next().<String>getProperty("marker")).isEqualTo("kept");
    }
  }
}
