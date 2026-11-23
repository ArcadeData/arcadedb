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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #13139: a relationship variable read only by the ORDER BY of a WITH, or only by the inline
 * property map of another MATCH pattern, was taken for unread. The hop was walked without binding its edge, so the ORDER BY
 * sorted by null (an arbitrary row won the LIMIT) and the inline map matched nothing. The expected values are what Neo4j 5.26
 * answers for the same statements.
 */
class CypherEdgeVariableReadPositionsIssue13139Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher13139").create();
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:A {id: 1}), (b1:B {id: 2}), (b2:B {id: 3}), (b3:B {id: 4}),
               (a)-[:R1 {w: 1}]->(b1), (a)-[:R1 {w: 3}]->(b2), (a)-[:R1 {w: 2}]->(b3), (a)-[:R2 {w: 1}]->(b3)"""));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private List<Object> column(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty("v"));
      }
    }
    return values;
  }

  @Test
  void withOrderByReadsTheEdgeToRankRows() {
    assertThat(column("MATCH (a:A)-[e:R1]->(b) WITH b ORDER BY e.w DESC LIMIT 1 RETURN b.id AS v")).containsExactly(3);
    assertThat(column("MATCH (a:A)-[e:R1]->(b) WITH b ORDER BY e.w LIMIT 1 RETURN b.id AS v")).containsExactly(2);
  }

  @Test
  void withOrderByKeepsTheWholeOrderBehindSkip() {
    assertThat(column("MATCH (a:A)-[e:R1]->(b) WITH b ORDER BY e.w DESC SKIP 1 RETURN b.id AS v")).containsExactly(4, 2);
    assertThat(column("MATCH (a:A)-[e:R1]->(b) WITH b ORDER BY e.w DESC RETURN collect(b.id) AS v"))
        .containsExactly(List.of(3, 4, 2));
  }

  @Test
  void withOrderByReadsTheEdgeInsideAnExpression() {
    assertThat(column("MATCH (a:A)-[e:R1]->(b) WITH b ORDER BY coalesce(e.w, 0) DESC LIMIT 1 RETURN b.id AS v")).containsExactly(3);
    assertThat(column("MATCH (a:A)-[e:R1]->(b) WITH b ORDER BY startNode(e).id, e.w DESC LIMIT 1 RETURN b.id AS v"))
        .containsExactly(3);
  }

  @Test
  void withOrderByInsideACallBodyReadsTheEdge() {
    assertThat(column("""
        CALL {
          MATCH (a:A)-[e:R1]->(b)
          WITH b ORDER BY e.w DESC LIMIT 1
          RETURN b.id AS v
        }
        RETURN v""")).containsExactly(3);
  }

  @Test
  void inlineRelationshipMapOfALaterMatchReadsTheEdge() {
    assertThat(column("MATCH (a:A)-[e1:R1]->(x) MATCH (a)-[e2:R2 {w: e1.w}]->(y) RETURN count(*) AS v")).containsExactly(1L);
    assertThat(column("MATCH (a:A)-[e1:R1]->(x) MATCH (a)-[e2:R2 {w: e1.w}]->(y) RETURN y.id AS v")).containsExactly(4);
  }

  @Test
  void inlineRelationshipMapOfASiblingPatternReadsTheEdge() {
    assertThat(column("MATCH (a:A)-[e1:R1]->(x), (a)-[e2:R2 {w: e1.w}]->(y) RETURN count(*) AS v")).containsExactly(1L);
  }

  @Test
  void inlineNodeMapOfALaterPatternReadsTheEdge() {
    assertThat(column("MATCH (a:A)-[e1:R1]->(x) MATCH (a)-[:R2]->(y {id: e1.w + 3}) RETURN count(*) AS v")).containsExactly(1L);
  }

  @Test
  void inlineMapInAnOptionalMatchReadsTheEdge() {
    assertThat(column("""
        MATCH (a:A)-[e1:R1]->(x)
        OPTIONAL MATCH (a)-[e2:R2 {w: e1.w}]->(y)
        RETURN x.id AS v ORDER BY v""")).containsExactly(2, 3, 4);
    assertThat(column("""
        MATCH (a:A)-[e1:R1]->(x)
        OPTIONAL MATCH (a)-[e2:R2 {w: e1.w}]->(y)
        RETURN count(y) AS v""")).containsExactly(1L);
  }

  @Test
  void anEdgeReadNowhereStillAnswersAsBefore() {
    assertThat(column("MATCH (a:A)-[e:R1]->(b) WITH b ORDER BY b.id DESC LIMIT 1 RETURN b.id AS v")).containsExactly(4);
    assertThat(column("MATCH (a:A)-[e:R1]->(b) RETURN count(*) AS v")).containsExactly(3L);
  }
}
