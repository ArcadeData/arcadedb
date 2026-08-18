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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6370: {@code [*]} means {@code [*1..]} everywhere in Cypher, and a pattern comprehension read it as one
 * fixed hop.
 * <p>
 * The MATCH-side builder normalized the bare star; {@code CypherExpressionBuilder}, which carries its own copy of
 * the same block for expression-position patterns, did not, leaving both bounds {@code null} - and
 * {@code RelationshipPattern.isVariableLength()} is {@code minHops != null || maxHops != null}, so the hop was
 * not variable-length at all. The query did not fail; it answered as though the user had written a one-hop
 * pattern. Neo4j answers {@code ['a1']} for both spellings.
 * <p>
 * The fix is not a second copy of the normalization - the two blocks are already copies of each other, which is
 * how they drifted - so the two other ways they had drifted are pinned here as well: the expression-side block
 * split its label expression out of the raw text, which chops a backtick-quoted label containing a separator and
 * keeps the backticks on every name, and it left backticks on the pattern variables.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherBareStarComprehensionIssue6370Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-bare-star-6370");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:C {n:'c1'})-[:R]->(:E {n:'e1'})-[:R]->(:A {n:'a1'})");
      database.command("opencypher", "CREATE (:C {n:'c2'})-[:R]->(:A {n:'a2'})");
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
  void aBareStarInAComprehensionIsAVariableLengthHop() {
    // Two hops away, so a fixed single hop reaches nothing and answered [] without any error.
    assertThat(listOf("MATCH (a:C {n:'c1'}) RETURN [(a)-[*]->(b:A) | b.n] AS l")).containsExactly("a1");
    // The explicit spelling of the same thing, which already worked - the two must agree.
    assertThat(listOf("MATCH (a:C {n:'c1'}) RETURN [(a)-[*1..]->(b:A) | b.n] AS l")).containsExactly("a1");
    // And the MATCH clause, which normalized it all along.
    assertThat(keys("MATCH (a:C {n:'c1'})-[*]->(b:A) RETURN b.n AS k")).containsExactly("a1");
  }

  @Test
  void aBareStarStillReachesAOneHopNeighbour() {
    // The counterweight: [*1..] includes the single hop, so widening the bound must not lose it.
    assertThat(listOf("MATCH (a:C {n:'c2'}) RETURN [(a)-[*]->(b:A) | b.n] AS l")).containsExactly("a2");
  }

  @Test
  void theOtherBoundedSpellingsAreUnchanged() {
    assertThat(listOf("MATCH (a:C {n:'c1'}) RETURN [(a)-[*2]->(b:A) | b.n] AS l")).containsExactly("a1");
    assertThat(listOf("MATCH (a:C {n:'c1'}) RETURN [(a)-[*1..1]->(b:A) | b.n] AS l")).isEmpty();
    assertThat(listOf("MATCH (a:C {n:'c1'}) RETURN [(a)-[*..2]->(b:A) | b.n] AS l")).containsExactly("a1");
    assertThat(listOf("MATCH (a:C {n:'c1'}) RETURN [(a)-[:R]->(b:E) | b.n] AS l")).containsExactly("e1");
  }

  @Test
  void aBackquotedLabelInAComprehensionIsOneLabel() {
    // The expression-side builder split the label expression on ':', '&' and '|' in the raw text, so a quoted
    // name carrying one of those was chopped into pieces and every piece kept its backticks.
    database.transaction(() -> database.command("opencypher", "CREATE (:C {n:'c3'})-[:R]->(:`A|B` {n:'ab1'})"));

    assertThat(listOf("MATCH (a:C {n:'c3'}) RETURN [(a)-[:R]->(b:`A|B`) | b.n] AS l")).containsExactly("ab1");
    // The MATCH spelling of the same question, which is the answer the comprehension has to match.
    assertThat(keys("MATCH (a:C {n:'c3'})-[:R]->(b:`A|B`) RETURN b.n AS k")).containsExactly("ab1");
  }

  @Test
  void aBackquotedVariableInAComprehensionBindsTheSameNameAsInAMatch() {
    assertThat(listOf("MATCH (a:C {n:'c2'}) RETURN [(a)-[:R]->(`my node`:A) | `my node`.n] AS l"))
        .containsExactly("a2");
  }

  // ---------------------------------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private List<Object> listOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      return (List<Object>) resultSet.next().getProperty("l");
    }
  }

  private List<Object> keys(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        values.add(resultSet.next().getProperty("k"));
    }
    return values;
  }
}
