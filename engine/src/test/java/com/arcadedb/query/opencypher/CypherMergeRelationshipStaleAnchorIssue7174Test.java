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
 * Regression test for GitHub issue #7174: {@code CALL merge.relationship(a, 'U', {}, {}, b)} created a second
 * edge for a pair it had already merged.
 * <p>
 * The existence check walked {@code startNode.getEdges(OUT, type)} on the vertex instance the row carried.
 * That instance was loaded before the rows ahead of it applied their merges, and appending the first edge of
 * a direction rewrites the vertex record's edge-list head pointer, so the check looked at a pre-append
 * snapshot and concluded the edge was missing. The MERGE clause on the identical rows was already correct -
 * {@code MergeStep} re-reads the anchor by RID (issue #6461) - and the procedure now shares that re-read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeRelationshipStaleAnchorIssue7174Test {
  /**
   * 120 {@code C} vertices cross-joined with two {@code A} vertices: 240 rows, spanning several of the
   * pipeline's 100-row pull batches, over only two distinct {@code (a, b)} pairs.
   */
  private static final String MANY_ROWS_TWO_PAIRS = "MATCH (c:C), (a:A), (b:B)\n";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-merge-relationship-stale-anchor-7174").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 3})");
      database.command("opencypher", "UNWIND range(1, 120) AS i CREATE (:C {id: i})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mergeRelationshipCreatesOneEdgePerPairHoweverManyRowsReachIt() {
    assertThat(rowsOf(MANY_ROWS_TWO_PAIRS + "CALL merge.relationship(a, 'U', {}, {}, b) YIELD rel RETURN rel"))
        .isEqualTo(240);
    assertThat(edgeCount("U")).as("two distinct (a, b) pairs, so two edges").isEqualTo(2);
  }

  /** The MERGE clause was already right on these rows; the procedure must not disagree with it. */
  @Test
  void theMergeClauseAgreesWithTheProcedure() {
    database.command("opencypher", MANY_ROWS_TWO_PAIRS + "MERGE (a)-[:V]->(b)").close();
    assertThat(edgeCount("V")).isEqualTo(2);
  }

  /** A second run must find what the first one created rather than merging a fresh pair of edges. */
  @Test
  void aSecondRunMergesNothingNew() {
    database.command("opencypher", MANY_ROWS_TWO_PAIRS + "CALL merge.relationship(a, 'U', {}, {}, b) YIELD rel RETURN rel")
        .close();
    database.command("opencypher", MANY_ROWS_TWO_PAIRS + "CALL merge.relationship(a, 'U', {}, {}, b) YIELD rel RETURN rel")
        .close();
    assertThat(edgeCount("U")).isEqualTo(2);
  }

  /**
   * Distinct match properties still merge into distinct edges. This is the other half of the re-read: the
   * append writes the vertex record's edge-list head pointer back, so doing it on the row's stale instance
   * would drop the edges the rows before it appended, and 240 distinct merges would not end up as 240 edges.
   */
  @Test
  void matchPropertiesStillSeparateTheEdges() {
    database.command("opencypher", MANY_ROWS_TWO_PAIRS
        + "CALL merge.relationship(a, 'W', {kind: c.id}, {}, b) YIELD rel RETURN rel").close();
    assertThat(edgeCount("W")).as("two pairs x 120 distinct match-property values").isEqualTo(240);

    database.command("opencypher", MANY_ROWS_TWO_PAIRS
        + "CALL merge.relationship(a, 'W', {kind: c.id}, {}, b) YIELD rel RETURN rel").close();
    assertThat(edgeCount("W")).as("a second run finds every one of them").isEqualTo(240);
  }

  private long rowsOf(final String query) {
    long rows = 0;
    try (final ResultSet resultSet = database.command("opencypher", query)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        ++rows;
      }
    }
    return rows;
  }

  private long edgeCount(final String type) {
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH ()-[r:" + type + "]->() RETURN count(*) AS c")) {
      return ((Number) resultSet.next().getProperty("c")).longValue();
    }
  }
}
