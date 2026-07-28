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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A query parameter referenced inside an {@code EXISTS { }}, {@code COUNT { }} or {@code COLLECT { }}
 * subquery must resolve to the value the caller bound.
 * <p>
 * These subqueries are executed as standalone statements, and the parameter map handed to that nested
 * execution used to be created empty - it received only the generated correlation bindings. Every
 * {@code $param} the body mentioned was therefore unbound; an unbound parameter evaluates to null rather
 * than raising, so the subquery matched nothing and each caller absorbed that into its neutral value
 * (false / 0 / empty list). The failure was silent and inverted the meaning of a de-duplication guard
 * such as {@code WHERE NOT EXISTS { MATCH (a)-[:TRANSFER {transactionId: $id}]->(b) } CREATE ...}, which
 * then created a duplicate edge on every replayed row. Literals in the same position always worked, which
 * is why the tests covering these subqueries did not catch it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSubqueryParameterBindingTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-subquery-parameter-binding").create();
    database.getSchema().createVertexType("Account");
    database.getSchema().createEdgeType("TRANSFER");

    database.transaction(() -> {
      database.command("cypher", "CREATE (:Account {accountNumber:'A'})");
      database.command("cypher", "CREATE (:Account {accountNumber:'B'})");
      database.command("cypher", """
          MATCH (s:Account {accountNumber:'A'}), (d:Account {accountNumber:'B'})
          CREATE (s)-[:TRANSFER {transactionId:'S1'}]->(d)""");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void existsSeesParameterInInlineRelationshipProperty() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        WHERE EXISTS { MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->(dst) }
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  @Test
  void notExistsSeesParameterInInlineRelationshipProperty() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        WHERE NOT EXISTS { MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->(dst) }
        RETURN count(*) AS c""")).isEqualTo(0L);
  }

  @Test
  void notExistsStillMatchesNothingForAnUnrelatedParameterValue() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        WHERE NOT EXISTS { MATCH (src)-[t:TRANSFER {transactionId: $otherTranId}]->(dst) }
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  @Test
  void existsSeesParameterInSubqueryWhereClause() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        WHERE EXISTS { MATCH (src)-[t:TRANSFER]->(dst) WHERE t.transactionId = $tranId }
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  @Test
  void existsSeesParameterInInlineNodeProperty() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})
        WHERE EXISTS { MATCH (src)-[:TRANSFER]->(dst:Account {accountNumber: $creditAcct}) }
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  @Test
  void countSubquerySeesParameter() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        RETURN COUNT { MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->(dst) } AS c""")).isEqualTo(1L);
  }

  @Test
  void collectSubquerySeesParameter() {
    try (final ResultSet rs = database.query("cypher", """
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        RETURN COLLECT { MATCH (src)-[t:TRANSFER {transactionId: $tranId}]->(dst) RETURN t.transactionId } AS c""",
        params())) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((List<Object>) rs.next().getProperty("c")).containsExactly("S1");
    }
  }

  /**
   * An outer row variable must keep shadowing a parameter of the same name: the correlation bindings are
   * applied after the seeded parameters, and that order is what preserves the pre-existing behaviour.
   */
  @Test
  void outerVariableShadowsSameNamedParameter() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})
        MATCH (dst:Account {accountNumber:'B'})
        WHERE EXISTS { MATCH (src)-[t:TRANSFER {transactionId:'S1'}]->(dst) }
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("cypher", query, params())) {
      assertThat(rs.hasNext()).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static Map<String, Object> params() {
    return Map.of("tranId", "S1", "otherTranId", "S999", "creditAcct", "B", "src", "ignored");
  }
}
