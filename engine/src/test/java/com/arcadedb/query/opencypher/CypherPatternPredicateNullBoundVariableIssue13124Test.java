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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #13124: a pattern predicate that names a node (or relationship) variable an earlier OPTIONAL MATCH
 * left null is false, whatever the rest of the pattern holds. The predicate used to take the null variable for an unbound one
 * and answered as if it stood for any node, so {@code WHERE (x)-->()} kept the row and {@code WHERE NOT (x)-->()} dropped it.
 * The expected values are what Neo4j 5.26 answers for the same statements on the same graph.
 */
class CypherPatternPredicateNullBoundVariableIssue13124Test {
  private static final String NULL_X = "MATCH (b:B) OPTIONAL MATCH (b)-[:ZZ]->(x) WITH b, x ";

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher13124").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:A {id: 1}), (b:B {id: 2}), (c:C {id: 3}), (a)-[:R]->(b), (b)-[:R]->(c)"));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private Object value(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final Object value = row.getProperty("v");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }

  @Test
  void predicateOverNullNodeIsFalse() {
    assertThat(value(NULL_X + "WHERE exists((x)-->()) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE (x)-->() RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE (x)-->(:C) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE (x)-[:R]->() RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE (x)-->()-->() RETURN count(*) AS v")).isEqualTo(0L);
  }

  @Test
  void predicateNamingNullNodeAsLastNodeIsFalse() {
    assertThat(value(NULL_X + "WHERE (b)-->(x) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE (b)<--(x) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE ()-->(x) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE (b)-->()-->(x) RETURN count(*) AS v")).isEqualTo(0L);
  }

  @Test
  void undirectedPredicateOverNullNodeIsFalse() {
    assertThat(value(NULL_X + "WHERE (x)--(b) RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "WHERE (x)--() RETURN count(*) AS v")).isEqualTo(0L);
  }

  @Test
  void negatedPredicateOverNullNodeIsTrue() {
    assertThat(value(NULL_X + "WHERE NOT (x)-->() RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(value(NULL_X + "WHERE NOT exists((x)-->(b)) RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(value(NULL_X + "WHERE NOT (b)-->(x) RETURN count(*) AS v")).isEqualTo(1L);
  }

  @Test
  void predicateOverNullNodeInAProjection() {
    assertThat(value(NULL_X + "RETURN exists((b)-->(x)) AS v")).isEqualTo(false);
    assertThat(value(NULL_X + "RETURN exists((x)-->()) AS v")).isEqualTo(false);
    assertThat(value(NULL_X + "RETURN NOT exists((x)-->()) AS v")).isEqualTo(true);
  }

  @Test
  void predicateOverNullRelationshipIsFalse() {
    final String nullE = "MATCH (a:A) OPTIONAL MATCH (a)-[e:ZZ]->() WITH a, e ";
    assertThat(value(nullE + "WHERE (a)-[e]->() RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(nullE + "WHERE NOT (a)-[e]->() RETURN count(*) AS v")).isEqualTo(1L);
  }

  @Test
  void predicateOverNullElementOfAList() {
    assertThat(value("UNWIND [null] AS x WITH x WHERE (x)-->() RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value("UNWIND [null] AS x WITH x WHERE NOT (x)-->() RETURN count(*) AS v")).isEqualTo(1L);
  }

  @Test
  void boundNodesAndUnrelatedNullsAreUnaffected() {
    // another variable being null does not touch a predicate that does not name it
    assertThat(value(NULL_X + "WHERE (b)-->() RETURN count(*) AS v")).isEqualTo(1L);
    // x is bound to c: c has no outgoing edge, b reaches it
    final String boundX = "MATCH (b:B) OPTIONAL MATCH (b)-[:R]->(x) WITH b, x ";
    assertThat(value(boundX + "WHERE (x)-->() RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(boundX + "WHERE NOT (x)-->() RETURN count(*) AS v")).isEqualTo(1L);
    assertThat(value(boundX + "WHERE (b)-->(x) RETURN count(*) AS v")).isEqualTo(1L);
    // a variable the row does not carry at all stands for any node, as an anonymous one does
    assertThat(value("MATCH (a:A), (d) WHERE (d)-->() RETURN count(*) AS v")).isEqualTo(2L);
  }

  @Test
  void subqueryAndComprehensionAgree() {
    assertThat(value(NULL_X + "WHERE EXISTS { (x)-->() } RETURN count(*) AS v")).isEqualTo(0L);
    assertThat(value(NULL_X + "RETURN size([(x)-->(q) | q]) AS v")).isEqualTo(0L);
  }
}
