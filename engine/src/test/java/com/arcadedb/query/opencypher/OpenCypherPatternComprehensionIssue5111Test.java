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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5111: a single-hop pattern comprehension must honor the inline
 * relationship property filter {@code {prop: value}}. The reported query
 * {@code [(a)-[:R {w: 1}]->(b) | b.name]} returned every neighbor instead of applying the filter.
 * This is the same defect as issue #5139 (fixed in PatternComprehensionExpression.matchesEdgeProperties);
 * this test pins the exact scenario from the bug report. Neo4j 5.26 returns an empty list here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherPatternComprehensionIssue5111Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-pc-issue-5111").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("R");

    // Alice -[R {w: 3}]-> Bob
    // Alice -[R {w: 5}]-> Dave
    // No relationship from Alice has w = 1.
    database.command("opencypher",
        """
        CREATE (alice:Person {name: 'Alice'}), \
        (bob:Person {name: 'Bob'}), \
        (dave:Person {name: 'Dave'}), \
        (alice)-[:R {w: 3}]->(bob), \
        (alice)-[:R {w: 5}]->(dave)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void inlineRelPropertyFilterWithNoMatchReturnsEmpty() {
    // w = 1 matches no relationship, so the comprehension must be empty (Neo4j returns []).
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'})
        RETURN [(a)-[:R {w: 1}]->(b) | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).isEmpty();
  }

  @Test
  void inlineRelPropertyFilterWithOneMatchReturnsSubset() {
    // Only Alice -[R {w: 3}]-> Bob qualifies.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'})
        RETURN [(a)-[:R {w: 3}]->(b) | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("Bob");
  }

  @Test
  void explicitWhereFilterStillWorks() {
    // Control: the documented workaround must keep returning empty.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'})
        RETURN [(a)-[r:R]->(b) WHERE r.w = 1 | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).isEmpty();
  }

  @Test
  void noInlineFilterReturnsAllNeighbors() {
    // Sanity: without the inline filter both neighbors are returned.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'})
        RETURN [(a)-[:R]->(b) | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactlyInAnyOrder("Bob", "Dave");
  }
}
