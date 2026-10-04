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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #9051: pattern parentheses, nested CALL/EXISTS/COLLECT subqueries and long chains of clauses
 * left a raw {@link StackOverflowError} escape from {@code db.query()}. They are now a {@link CommandParsingException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherNestingGuardIssue9051Test extends TestHelper {

  private void drain(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private void assertRejected(final String query) {
    // the message fragment keeps an unrelated parse error (a typo in the generated text) from satisfying the assertion
    assertThatThrownBy(() -> drain(query)).isInstanceOf(CommandParsingException.class)
        .hasMessageMatching("(?s).*(too many clauses|too deeply).*");
  }

  @Test
  void deepPatternParenthesesAreRejected() {
    assertRejected("MATCH " + "(".repeat(10000) + "(n)-[:R]->(m)" + ")".repeat(10000) + " RETURN n");
  }

  @Test
  void deepCallSubqueriesAreRejected() {
    String q = "RETURN 1 AS x";
    for (int i = 0; i < 1000; i++)
      q = "CALL { " + q + " } RETURN x";
    assertRejected(q);
  }

  @Test
  void deepCollectSubqueriesAreRejected() {
    assertRejected("RETURN " + "COLLECT { RETURN ".repeat(1000) + "1" + " }".repeat(1000) + " AS x");
  }

  @Test
  void deepExistsSubqueriesAreRejected() {
    assertRejected("MATCH (n) WHERE " + "EXISTS { MATCH (n) WHERE ".repeat(10000) + "true" + " }".repeat(10000) + " RETURN n");
  }

  @Test
  void longClauseChainsAreRejected() {
    assertRejected("MATCH (n) " + "WITH n ".repeat(10000) + "RETURN n");
    assertRejected("MATCH (n) " + "OPTIONAL MATCH (n)-[:R]->(m) ".repeat(10000) + "RETURN n");
    assertRejected("MATCH (n) ".repeat(10000) + "RETURN n");
  }

  @Test
  void maxClausesSettingIsHonoured() {
    database.getSchema().createVertexType("Person");
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_MAX_CLAUSES, 5);
    drain("MATCH (n:Person) WITH n WITH n RETURN n");
    drain("MATCH (n:Person) WITH n WITH n WITH n RETURN n");
    assertRejected("MATCH (n:Person) " + "WITH n ".repeat(5) + "RETURN n");
  }

  @Test
  void longUnionChainsAndNestedClauseBudgetsExecuteWithoutOverflow() {
    try (final ResultSet rs = database.query("opencypher", "RETURN 1 AS x" + " UNION ALL RETURN 1 AS x".repeat(3000))) {
      int n = 0;
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
      assertThat(n).isEqualTo(3001);
    }
    // each nested subquery body carries its own clause budget: 10 levels of 450 clauses must still be safe to execute
    String q = "RETURN 1 AS x";
    for (int i = 0; i < 10; i++)
      q = "WITH 1 AS a " + "WITH a ".repeat(450) + "CALL { " + q + " } RETURN x";
    drain(q);
  }

  @Test
  void defaultClauseLimitIsExactlyFiveHundred() {
    database.getSchema().createVertexType("Person");
    // MATCH + 498 WITH + RETURN = 500 clauses
    drain("MATCH (n:Person) " + "WITH n ".repeat(498) + "RETURN n");
    assertRejected("MATCH (n:Person) " + "WITH n ".repeat(499) + "RETURN n");
  }

  @Test
  void thirtyNestedExistsLevelsStillWork() {
    database.getSchema().createVertexType("Person");
    database.transaction(() -> database.newVertex("Person").set("id", 1).save());
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) WHERE " + "EXISTS { MATCH (n) WHERE ".repeat(30) + "true" + " }".repeat(30) + " RETURN n")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  @Test
  void clauseLimitAppliesInsideNestedSubqueryBodies() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_MAX_CLAUSES, 5);
    assertRejected("CALL { MATCH (n) " + "WITH n ".repeat(10) + "RETURN n } RETURN n");
  }

  @Test
  void nonPositiveMaxClausesFallsBackToTheDefault() {
    database.getSchema().createVertexType("Person");
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_MAX_CLAUSES, 0);
    drain("MATCH (n:Person) RETURN n");
    assertRejected("MATCH (n:Person) " + "WITH n ".repeat(600) + "RETURN n");
  }

  @Test
  void moderateNestingStillWorks() {
    database.getSchema().createVertexType("Person");
    database.transaction(() -> database.newVertex("Person").set("id", 1).save());
    String q = "RETURN 1 AS x";
    for (int i = 0; i < 20; i++)
      q = "CALL { " + q + " } RETURN x";
    try (final ResultSet rs = database.query("opencypher", q)) {
      assertThat(rs.next().<Long>getProperty("x")).isEqualTo(1L);
    }
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) " + "WITH n ".repeat(100) + "RETURN n.id AS id")) {
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person) WHERE EXISTS { MATCH (n) WHERE EXISTS { MATCH (n) } } RETURN n")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }
}
