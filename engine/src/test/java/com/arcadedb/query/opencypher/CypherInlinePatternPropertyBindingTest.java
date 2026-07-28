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
 * The inline property map of a pattern - the {@code {k: v}} in {@code (n:Account {accountNumber: $id})} or
 * {@code -[:TRANSFER {transactionId: row.id}]->} - must filter on what its values stand for, whatever the
 * pattern is written inside.
 * <p>
 * Every evaluator carried its own copy of the value resolution and the copies knew about different shapes,
 * so the same filter matched nothing depending on where it appeared: a {@code $param} inside a pattern
 * comprehension, a variable or an UNWIND row property inside a relationship property map, a {@code $param}
 * inside a variable-length or shortestPath relationship. An inline map is a filter, so an unresolved value
 * produces an empty result rather than an error - the wrong answer arrives silently, the same failure mode
 * as the subquery parameters of issue #5501. The relationship spelling also had the mirror-image defect:
 * a variable-length relationship inside a pattern predicate dropped the map altogether and matched paths
 * that do not carry the property at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherInlinePatternPropertyBindingTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-inline-pattern-property-binding").create();
    database.getSchema().createVertexType("Account");
    database.getSchema().createEdgeType("TRANSFER");

    database.transaction(() -> {
      database.command("cypher", "CREATE (:Account {accountNumber:'A', balance: 1.5})");
      database.command("cypher", "CREATE (:Account {accountNumber:'B', balance: 2.0})");
      database.command("cypher", """
          MATCH (s:Account {accountNumber:'A'}), (d:Account {accountNumber:'B'})
          CREATE (s)-[:TRANSFER {transactionId:'S1', amount: 10, rate: 1.5}]->(d)""");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void patternComprehensionSeesParameterInRelationshipProperty() {
    assertThat(this.<List<Object>>value("""
        MATCH (src:Account {accountNumber:'A'})
        RETURN [(src)-[t:TRANSFER {transactionId: $tranId}]->(x) | x.accountNumber] AS c""")).containsExactly("B");
  }

  @Test
  void patternComprehensionSeesParameterInNodeProperty() {
    assertThat(this.<List<Object>>value("""
        MATCH (src:Account {accountNumber:'A'})
        RETURN [(src)-[t:TRANSFER]->(x:Account {accountNumber: $creditAcct}) | x.accountNumber] AS c""")).containsExactly("B");
  }

  @Test
  void patternComprehensionSeesVariableBoundByWith() {
    assertThat(this.<List<Object>>value("""
        WITH 'S1' AS wanted
        MATCH (src:Account {accountNumber:'A'})
        RETURN [(src)-[t:TRANSFER {transactionId: wanted}]->(x) | x.accountNumber] AS c""")).containsExactly("B");
  }

  @Test
  void patternComprehensionMatchesNothingForAnUnrelatedParameterValue() {
    assertThat(this.<List<Object>>value("""
        MATCH (src:Account {accountNumber:'A'})
        RETURN [(src)-[t:TRANSFER {transactionId: $otherTranId}]->(x) | x.accountNumber] AS c""")).isEmpty();
  }

  @Test
  void matchRelationshipPropertySeesVariableBoundByWith() {
    assertThat(count("""
        WITH 'S1' AS wanted
        MATCH (src:Account {accountNumber:'A'})-[t:TRANSFER {transactionId: wanted}]->(x)
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  /**
   * The bulk-loader spelling: one row per transaction, the relationship filtered by a field of that row.
   */
  @Test
  void matchRelationshipPropertySeesUnwindRowProperty() {
    assertThat(count("""
        UNWIND [{id:'S1'}, {id:'S999'}] AS row
        MATCH (src:Account {accountNumber:'A'})-[t:TRANSFER {transactionId: row.id}]->(x)
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  @Test
  void variableLengthRelationshipPropertySeesParameter() {
    assertThat(count("""
        MATCH (a:Account {accountNumber:'A'})-[:TRANSFER*1..3 {transactionId: $tranId}]-(b:Account {accountNumber:'B'})
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  @Test
  void variableLengthRelationshipPropertyStillRejectsAnUnrelatedParameterValue() {
    assertThat(count("""
        MATCH (a:Account {accountNumber:'A'})-[:TRANSFER*1..3 {transactionId: $otherTranId}]-(b:Account {accountNumber:'B'})
        RETURN count(*) AS c""")).isEqualTo(0L);
  }

  @Test
  void shortestPathRelationshipPropertySeesParameter() {
    assertThat(count("""
        MATCH (a:Account {accountNumber:'A'}), (b:Account {accountNumber:'B'})
        MATCH p = shortestPath((a)-[:TRANSFER*1..3 {transactionId: $tranId}]-(b))
        RETURN length(p) AS c""")).isEqualTo(1L);
  }

  @Test
  void patternPredicateSeesVariableBoundByWith() {
    assertThat(count("""
        WITH 'S1' AS wanted
        MATCH (src:Account {accountNumber:'A'})
        WHERE (src)-[:TRANSFER {transactionId: wanted}]->()
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  /**
   * A variable-length relationship inside a pattern predicate used to be handed no property map at all, so
   * the predicate held for any path of the right shape - the mirror image of the silent no-match, and just
   * as dangerous in a guard clause.
   */
  @Test
  void patternPredicateEnforcesVariableLengthRelationshipProperty() {
    assertThat(count("""
        MATCH (a:Account {accountNumber:'A'})
        WHERE (a)-[:TRANSFER*1..3 {transactionId:'S999'}]-()
        RETURN count(*) AS c""")).isEqualTo(0L);

    assertThat(count("""
        MATCH (a:Account {accountNumber:'A'})
        WHERE (a)-[:TRANSFER*1..3 {transactionId:'S1'}]-()
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  /**
   * A stored Integer must keep matching an inline literal parsed as Long, and a parameter bound to either
   * (issue #5146).
   */
  @Test
  void numbersStillMatchAcrossIntegerAndLong() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})-[t:TRANSFER {amount: 10}]->(x)
        RETURN count(*) AS c""")).isEqualTo(1L);

    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})-[t:TRANSFER {amount: $amount}]->(x)
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  /**
   * Two decimals that share an integer part are different values: comparing them as longs truncated the
   * fraction away and declared 1.5 equal to 1.9.
   */
  @Test
  void decimalsThatDifferOnlyInTheFractionDoNotMatch() {
    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})-[t:TRANSFER {rate: 1.9}]->(x)
        RETURN count(*) AS c""")).isEqualTo(0L);

    assertThat(count("""
        MATCH (src:Account {accountNumber:'A'})-[t:TRANSFER {rate: 1.5}]->(x)
        RETURN count(*) AS c""")).isEqualTo(1L);
  }

  private long count(final String query) {
    return ((Number) value(query)).longValue();
  }

  private <T> T value(final String query) {
    try (final ResultSet rs = database.query("cypher", query, params())) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("c");
    }
  }

  private static Map<String, Object> params() {
    return Map.of("tranId", "S1", "otherTranId", "S999", "creditAcct", "B", "amount", 10L);
  }
}
