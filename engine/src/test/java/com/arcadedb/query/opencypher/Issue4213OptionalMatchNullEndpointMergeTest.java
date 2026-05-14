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
 * Regression test for GitHub issue #4213.
 * <p>
 * When OPTIONAL MATCH leaves a relationship endpoint variable as null, a
 * subsequent relationship write (MERGE or CREATE) that references that null
 * variable must drop the row rather than emitting a result or rebinding the
 * variable to an unrelated node.
 */
class Issue4213OptionalMatchNullEndpointMergeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-4213-optional-match-null-endpoint").create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Node {id: 1})");
      database.command("opencypher", "CREATE (:Node {id: 2})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * OPTIONAL MATCH that finds no match leaves the endpoint null; the subsequent
   * MERGE must produce zero rows instead of rebinding the null endpoint and
   * emitting a spurious result.
   */
  @Test
  void mergeWithNullEndpointFromOptionalMatchProducesNoRows() {
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Node {id: 1}) "
            + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
            + "WITH a, b "
            + "MERGE (a)-[:KNOWS]->(b) "
            + "RETURN a.id AS aid, b.id AS bid");

    assertThat(rs.hasNext())
        .as("MERGE with null endpoint from OPTIONAL MATCH must produce no rows")
        .isFalse();
  }

  /**
   * OPTIONAL MATCH itself must still expose a null value for unmatched
   * variables - this verifies the control behaviour is unaffected.
   */
  @Test
  void optionalMatchAloneReturnsNullEndpoint() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 1}) "
            + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
            + "WITH a, b "
            + "RETURN a.id AS aid, b.id AS bid");

    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();
    assertThat(row.<Number>getProperty("aid").longValue()).isEqualTo(1L);
    assertThat(row.<Object>getProperty("bid")).isNull();
    assertThat(rs.hasNext()).isFalse();
  }

  /**
   * Once the endpoint is rebound via an explicit MATCH after the OPTIONAL MATCH,
   * the subsequent MERGE must succeed normally and return one row.
   */
  @Test
  void mergeAfterExplicitRebindSucceeds() {
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Node {id: 1}) "
            + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
            + "WITH a, b "
            + "MATCH (c:Node {id: 2}) "
            + "MERGE (a)-[:KNOWS]->(c) "
            + "RETURN a.id AS aid, c.id AS cid");

    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();
    assertThat(row.<Number>getProperty("aid").longValue()).isEqualTo(1L);
    assertThat(row.<Number>getProperty("cid").longValue()).isEqualTo(2L);
    assertThat(rs.hasNext()).isFalse();
  }

  /**
   * MERGE with both endpoints directly bound by a non-optional MATCH must
   * create exactly one relationship and return one row.
   */
  @Test
  void mergeWithBothEndpointsBoundCreatesRelationship() {
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Node {id: 1}), (b:Node {id: 2}) "
            + "MERGE (a)-[:KNOWS]->(b) "
            + "RETURN count(*) AS cnt");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(1L);
  }

  /**
   * When OPTIONAL MATCH succeeds and binds the endpoint, the subsequent MERGE
   * must create the relationship and return one row.
   */
  @Test
  void mergeWithNonNullEndpointFromOptionalMatchCreatesRelationship() {
    // Pre-create the KNOWS relationship so OPTIONAL MATCH will match.
    database.transaction(() ->
        database.command("opencypher",
            "MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:KNOWS]->(b)"));

    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Node {id: 1}) "
            + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
            + "WITH a, b "
            + "MERGE (a)-[:KNOWS]->(b) "
            + "RETURN a.id AS aid, b.id AS bid");

    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();
    assertThat(row.<Number>getProperty("aid").longValue()).isEqualTo(1L);
    assertThat(row.<Number>getProperty("bid").longValue()).isEqualTo(2L);
    assertThat(rs.hasNext()).isFalse();
  }

  /**
   * No spurious Node vertices must be created as a side effect of the skipped
   * MERGE row; the graph should still contain only the two original nodes.
   */
  @Test
  void mergeWithNullEndpointDoesNotCreateSpuriousVertex() {
    // Execute the buggy query (which must now produce 0 rows and no side effects).
    database.command("opencypher",
        "MATCH (a:Node {id: 1}) "
            + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 2}) "
            + "WITH a, b "
            + "MERGE (a)-[:KNOWS]->(b) "
            + "RETURN a.id AS aid, b.id AS bid");

    final ResultSet nodeCount = database.query("opencypher", "MATCH (n:Node) RETURN count(n) AS cnt");
    assertThat(nodeCount.next().<Number>getProperty("cnt").longValue())
        .as("No spurious Node vertex must be created by the skipped MERGE")
        .isEqualTo(2L);

    final ResultSet relCount = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt");
    assertThat(relCount.next().<Number>getProperty("cnt").longValue())
        .as("No KNOWS relationship must be created when endpoint is null")
        .isEqualTo(0L);
  }

  /**
   * Multi-hop MERGE pattern with a null intermediate node from OPTIONAL MATCH:
   * the null at index 1 must drop the row even though the endpoints at index 0
   * and index 2 are bound.
   */
  @Test
  void mergeMultiHopPatternWithNullIntermediateProducesNoRows() {
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Node {id: 1}) "
            + "OPTIONAL MATCH (a)-[:KNOWS]->(b:Node {id: 99}) "
            + "WITH a, b "
            + "MATCH (c:Node {id: 2}) "
            + "MERGE (a)-[:KNOWS]->(b)-[:KNOWS]->(c) "
            + "RETURN a.id AS aid, b.id AS bid, c.id AS cid");

    assertThat(rs.hasNext())
        .as("MERGE with null intermediate node must produce no rows")
        .isFalse();

    final ResultSet relCount = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt");
    assertThat(relCount.next().<Number>getProperty("cnt").longValue())
        .as("No KNOWS relationship must be created when an intermediate node is null")
        .isEqualTo(0L);
  }

}
