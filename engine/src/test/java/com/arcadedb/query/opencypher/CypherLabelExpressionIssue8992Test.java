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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8992: the label expression operators {@code !} and {@code %}, and a mix of {@code &}
 * and {@code |}, were parsed and then dropped, because the pattern and the {@code WHERE} predicate only received the
 * label names and a flag saying whether {@code |} was written. {@code (n:!A)} returned the {@code A} nodes and
 * {@code [r:!R]} the {@code R} relationships.
 * <p>
 * The expected rows are the ones Neo4j 2026.08.1 returns for the same fixture, as quoted in the issue.
 */
class CypherLabelExpressionIssue8992Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-label-expression-8992");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:A {id: 1}), (b:B {id: 2}), (c:A:B {id: 3}), (d {id: 4}), (e:C {id: 5}),
               (a)-[:R]->(b), (a)-[:S]->(c), (b)-[:T]->(d), (c)-[:R]->(e)"""));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---- the ten queries of the issue ---------------------------------------------------------------------------

  @Test
  void negatedLabelOnTheNodePattern() {
    assertThat(column("MATCH (n:!A) RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 4, 5);
  }

  @Test
  void negatedLabelInWhere() {
    assertThat(column("MATCH (n) WHERE n:!A RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 4, 5);
  }

  @Test
  void wildcardSkipsTheUnlabelledNode() {
    assertThat(column("MATCH (n:%) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 2, 3, 5);
  }

  @Test
  void negatedWildcardIsOnlyTheUnlabelledNode() {
    assertThat(column("MATCH (n:!%) RETURN n.id AS id ORDER BY id", "id")).containsExactly(4);
  }

  @Test
  void conjunctionWithANegation() {
    assertThat(column("MATCH (n:A&!B) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1);
  }

  @Test
  void parenthesizedDisjunctionAndANegation() {
    assertThat(column("MATCH (n:(A|C)&!B) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 5);
  }

  @Test
  void disjunctionWithANegation() {
    assertThat(column("MATCH (n:A|!B) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 3, 4, 5);
  }

  @Test
  void negatedRelationshipType() {
    assertThat(column("MATCH ()-[r:!R]->() RETURN type(r) AS t ORDER BY t", "t")).containsExactly("S", "T");
  }

  @Test
  void negatedRelationshipTypeInWhere() {
    assertThat(column("MATCH ()-[r]->() WHERE r:!R RETURN type(r) AS t ORDER BY t", "t")).containsExactly("S", "T");
  }

  @Test
  void conjunctionOfNegatedRelationshipTypes() {
    assertThat(column("MATCH ()-[r:!R&!S]->() RETURN type(r) AS t ORDER BY t", "t")).containsExactly("T");
  }

  // ---- the same operators in the other positions a label expression can take -----------------------------------

  @Test
  void mixedConjunctionAndDisjunctionWithoutNegation() {
    assertThat(column("MATCH (n:(A|C)&B) RETURN n.id AS id ORDER BY id", "id")).containsExactly(3);
    // & binds tighter than |: (A&B)|C
    assertThat(column("MATCH (n:A&B|C) RETURN n.id AS id ORDER BY id", "id")).containsExactly(3, 5);
  }

  @Test
  void conjunctionOfRelationshipTypesMatchesNothing() {
    // A relationship has exactly one type: R&S used to be read as R|S and returned both.
    assertThat(column("MATCH ()-[r:R&S]->() RETURN type(r) AS t", "t")).isEmpty();
    assertThat(column("MATCH ()-[r:R&!S]->() RETURN type(r) AS t", "t")).containsExactly("R", "R");
    assertThat(column("MATCH ()-[r:R|S]->() RETURN type(r) AS t ORDER BY t", "t")).containsExactly("R", "R", "S");
  }

  @Test
  void isSpellingOfTheNegation() {
    assertThat(column("MATCH (n IS !A) RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 4, 5);
  }

  @Test
  void wildcardInWhereOnNodesAndRelationships() {
    assertThat(column("MATCH (n) WHERE n:% RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 2, 3, 5);
    assertThat(column("MATCH (n) WHERE NOT n:!A RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 3);
    assertThat(column("MATCH ()-[r]->() WHERE r:% RETURN type(r) AS t ORDER BY t", "t")).containsExactly("R", "R", "S", "T");
    assertThat(column("MATCH ()-[r]->() WHERE r:!% RETURN type(r) AS t", "t")).isEmpty();
  }

  @Test
  void labelExpressionAsAProjectedValue() {
    assertThat(column("MATCH (n {id: 1}) RETURN n:!A AS x", "x")).containsExactly(false);
    assertThat(column("MATCH (n {id: 4}) RETURN n:!% AS x", "x")).containsExactly(true);
    assertThat(column("MATCH (n {id: 3}) RETURN n:(A|C)&!B AS x", "x")).containsExactly(false);
    assertThat(column("MATCH ()-[r:T]->() RETURN r:!R&!S AS x", "x")).containsExactly(true);
  }

  @Test
  void negationOnAnAnonymousExpandedNode() {
    assertThat(column("MATCH (a:A)-->(:!B) RETURN a.id AS id ORDER BY id", "id")).containsExactly(3);
  }

  @Test
  void negationOnAnAnonymousRelationship() {
    assertThat(column("MATCH (a)-[:!R]->(b) RETURN a.id AS id ORDER BY id", "id")).containsExactly(1, 2);
  }

  @Test
  void negationOnTheEndpointOfANamedHop() {
    assertThat(column("MATCH (a)-[r:R]->(b:!B) RETURN a.id AS id ORDER BY id", "id")).containsExactly(3);
  }

  @Test
  void negationInsideAPatternPredicate() {
    assertThat(column("MATCH (n) WHERE (n)-->(:!B) RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 3);
    assertThat(column("MATCH (n) WHERE exists((n)-[:!R]->()) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 2);
  }

  @Test
  void negationInsideAPatternComprehension() {
    assertThat(column("MATCH (n {id: 1}) RETURN [(n)-[r:!R]->(m) | m.id] AS ids", "ids")).containsExactly(List.of(3));
    assertThat(column("MATCH (n {id: 1}) RETURN [(n)-[:!S]->(m:%) | m.id] AS ids", "ids")).containsExactly(List.of(2));
    assertThat(column("MATCH (n {id: 1}) RETURN [(n)-->(:!B) | 1] AS ids", "ids")).containsExactly(List.of());
  }

  @Test
  void negationInsideAnExistsSubquery() {
    assertThat(column("MATCH (n) WHERE EXISTS { (n)-[:!R]->(:!A) } RETURN n.id AS id ORDER BY id", "id")).containsExactly(2);
  }

  @Test
  void returnStarDoesNotExposeTheVariableAnAnonymousElementWasGiven() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (:!A)-[r:T]->(:!%) RETURN *")) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().getPropertyNames()).containsExactly("r");
    }
  }

  @Test
  void optionalMatchKeepsTheRowWhenTheNegationExcludesEveryCandidate() {
    assertThat(column("MATCH (n {id: 2}) OPTIONAL MATCH (n)-->(m:!%&!A) RETURN m.id AS id", "id")).containsExactly(4);
    final List<Object> ids = column("MATCH (n {id: 2}) OPTIONAL MATCH (n)-->(m:%) RETURN m.id AS id", "id");
    assertThat(ids).hasSize(1);
    assertThat(ids.getFirst()).isNull();
  }

  @Test
  void countsAreNotPushedDownPastTheNegation() {
    // count() over a bare label pattern takes a push-down that reads type counters; the predicate must stop it.
    assertThat(column("MATCH (n:!A) RETURN count(n) AS c", "c")).containsExactly(3L);
    assertThat(column("MATCH (:!A) RETURN count(*) AS c", "c")).containsExactly(3L);
    assertThat(column("MATCH (n:%) RETURN count(*) AS c", "c")).containsExactly(4L);
    assertThat(column("MATCH ()-[r:!R]->() RETURN count(r) AS c", "c")).containsExactly(2L);
    assertThat(column("MATCH ()-[:!R]->() RETURN count(*) AS c", "c")).containsExactly(2L);
    assertThat(column("MATCH (:A)-[:!R]->() RETURN count(*) AS c", "c")).containsExactly(1L);
  }

  @Test
  void negationInsideAQuantifiedPathPattern() {
    assertThat(column("MATCH (a)((x)-[:!R]->(y:!A)){1,2}(b) RETURN a.id AS id ORDER BY id", "id")).containsExactly(2);
    assertThat(column("MATCH (a)((x)-[:!R]->(y)){1,2}(b) RETURN a.id AS id ORDER BY id", "id")).containsExactly(1, 2);
  }

  @Test
  void variableLengthRelationshipRefusesWhatItCannotEvaluate() {
    // Refused with an error naming the expression rather than run with the operator dropped (#9117).
    assertThatThrownBy(() -> column("MATCH ()-[:!R*1..2]->() RETURN 1 AS x", "x"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("!R");
    assertThatThrownBy(() -> column("MATCH ()-[r:R&S*1..3]->() RETURN 1 AS x", "x"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("R&S");
    assertThatThrownBy(() -> column("MATCH p = shortestPath((a {id: 1})-[:!R*]-(b {id: 4})) RETURN p", "p"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("!R");
  }

  @Test
  void variableLengthRelationshipStillAcceptsAPlainTypeDisjunction() {
    // The guard above must not catch the forms the expansion has always handled.
    assertThat(column("MATCH (a {id: 1})-[:S|R*1..2]->(b) RETURN b.id AS id ORDER BY id", "id")).containsExactly(2, 3, 5);
    assertThat(column("MATCH (a {id: 1})-[:R*1..2]->(b) RETURN b.id AS id ORDER BY id", "id")).containsExactly(2);
  }

  @Test
  void subtypesFollowTheSameRuleAsThePlainPattern() {
    // A label or a relationship type also matches its subtypes in a plain pattern; the expression must agree.
    database.getSchema().createVertexType("A2").addSuperType("A");
    database.getSchema().createEdgeType("R2").addSuperType("R");
    database.transaction(() -> database.command("opencypher", "CREATE (:A2 {id: 6})-[:R2]->(:C {id: 7})"));

    assertThat(column("MATCH (n:A) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 3, 6);
    assertThat(column("MATCH (n:A&!B) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 6);
    assertThat(column("MATCH (n:!A) RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 4, 5, 7);
    assertThat(column("MATCH (n) WHERE n:!A RETURN n.id AS id ORDER BY id", "id")).containsExactly(2, 4, 5, 7);

    assertThat(column("MATCH ()-[r:R]->() RETURN type(r) AS t ORDER BY t", "t")).containsExactly("R", "R", "R2");
    assertThat(column("MATCH ()-[r:R&!S]->() RETURN type(r) AS t ORDER BY t", "t")).containsExactly("R", "R", "R2");
    assertThat(column("MATCH ()-[r:!R]->() RETURN type(r) AS t ORDER BY t", "t")).containsExactly("S", "T");
    assertThat(column("MATCH ()-[r]->() WHERE r:!R RETURN type(r) AS t ORDER BY t", "t")).containsExactly("S", "T");
  }

  @Test
  void precedenceParityAndQuotedNames() {
    assertThat(column("MATCH (n:!!A) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 3);
    // & binds tighter than |: A|(B&C)
    assertThat(column("MATCH (n:A|B&C) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1, 3);
    assertThat(column("MATCH (n:((A))&!(B|C)) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1);

    database.transaction(() -> database.command("opencypher", "CREATE (:`Event Message` {id: 8})"));
    assertThat(column("MATCH (n:`Event Message`|!%) RETURN n.id AS id ORDER BY id", "id")).containsExactly(4, 8);
    assertThat(column("MATCH (n) WHERE n:!`Event Message`&!A&!B&!C RETURN n.id AS id", "id")).containsExactly(4);
  }

  @Test
  void dynamicLabelCannotBeCombinedWithTheNewOperators() {
    assertThatThrownBy(() -> column("MATCH (n:!$('A')) RETURN n.id AS id", "id"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("dynamic label");
  }

  // ---- writes cannot act on an expression that only a read can answer --------------------------------------------

  @Test
  void createRefusesANegatedLabel() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (n:!A)")))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("CREATE");
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (:A)-[:!R]->(:B)")))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("CREATE");
  }

  @Test
  void writesNestedInForeachAndCallRefuseToo() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "FOREACH (x IN [1] | CREATE (:!A))")))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("CREATE");
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CALL { CREATE (:!A) } RETURN 1 AS x")))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("CREATE");
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "FOREACH (x IN [1] | MERGE (:A|!B))")))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("MERGE");
  }

  @Test
  void mergeRefusesTheWildcard() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MERGE (n:%)")))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("MERGE");
  }

  @Test
  void conjunctionIsPrintedAsConjunctionInExplain() {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (n:A:B) RETURN n")) {
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).contains("MATCH NODE (n:A:B)").doesNotContain("(n:A|B)");
    }
    assertThat(column("MATCH (n:A:B) RETURN n.id AS id", "id")).containsExactly(3);
  }

  private List<Object> column(final String query, final String column) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, Map.of())) {
      rs.stream().forEach(r -> out.add(r.getProperty(column)));
    }
    return out;
  }
}
