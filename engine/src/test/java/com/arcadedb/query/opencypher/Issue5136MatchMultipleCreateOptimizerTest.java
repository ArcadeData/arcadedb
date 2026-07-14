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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5136 (follow-up of #5107): a {@code MATCH ... CREATE ... CREATE ...}
 * statement with more than one CREATE clause must be handled by the cost-based optimizer (which emits
 * {@code NodeIndexSeek} for the leading indexed MATCH) instead of bailing out to the legacy path.
 * <p>
 * The main risk being guarded is variable threading across the chained {@code CreateStep}s: a second
 * CREATE {@code (p)-[:R]->(q)} references {@code p} bound by the MATCH and {@code q} created by the
 * first CREATE. These tests assert both the optimizer plan is used AND the write side-effects are
 * correct (the created variable is propagated through the CreateStep chain).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5136MatchMultipleCreateOptimizerTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var personType = database.getSchema().createVertexType("Person");
      personType.createProperty("id", Integer.class);
      personType.createProperty("name", String.class);
      personType.createProperty("age", Integer.class);
      personType.createProperty("city", String.class);

      final var knows = database.getSchema().createEdgeType("KNOWS");
      knows.createProperty("since", Integer.class);

      // Unique index on Person.id: the leading MATCH must resolve through it via NodeIndexSeek.
      personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });

    // Populate 100 Person vertices with sequential ids.
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.command("opencypher",
            "CREATE (p:Person {name: 'Person" + i + "', id: " + i + ", age: " + (20 + (i % 50)) + ", city: 'c" + i + "'})");
    });
  }

  /**
   * The exact #5107/#5136 query: a leading indexed MATCH followed by two CREATE clauses, the second
   * one referencing both the matched node and the node created by the first CREATE. The optimizer
   * must now handle it (NodeIndexSeek) and the edge must connect the correct source to the new node.
   */
  @Test
  void twoCreateClausesUseOptimizerAndThreadCreatedVariable() {
    final String write = "MATCH (p:Person) WHERE p.id = 42 "
        + "CREATE (q:Person {id: 900001, name: 'w', age: 33, city: 'c'}) "
        + "CREATE (p)-[:KNOWS {since: 2026}]->(q)";

    final String plan = profilePlan(write);

    assertThat(plan)
        .as("leading MATCH before multiple CREATE must now use the optimizer index seek\n%s", plan)
        .contains("NodeIndexSeek");

    database.transaction(() -> {
      final ResultSet check = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 42 RETURN p.id AS src, q.id AS dst, q.name AS name");
      assertThat(check.hasNext()).isTrue();
      final Result row = check.next();
      assertThat((Integer) row.getProperty("src")).isEqualTo(42);
      assertThat((Integer) row.getProperty("dst")).isEqualTo(900001);
      assertThat((String) row.getProperty("name")).isEqualTo("w");
      assertThat(check.hasNext()).isFalse();
      check.close();
    });
  }

  /**
   * Same shape but the predicate value is a parameter. Parameters resolve without an input row so the
   * index seek must still be chosen.
   */
  @Test
  void twoCreateClausesWithParameterPredicate() {
    final Map<String, Object> params = new HashMap<>();
    params.put("id", 7);

    final String write = "MATCH (p:Person) WHERE p.id = $id "
        + "CREATE (q:Person {id: 900002, name: 'param', age: 40, city: 'x'}) "
        + "CREATE (p)-[:KNOWS {since: 2027}]->(q)";

    final String plan = profilePlan(write, params);
    assertThat(plan)
        .as("parameter predicate must still choose the optimizer index seek\n%s", plan)
        .contains("NodeIndexSeek");

    database.transaction(() -> {
      final ResultSet check = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 7 RETURN q.id AS dst", params);
      assertThat(check.hasNext()).isTrue();
      assertThat((Integer) check.next().getProperty("dst")).isEqualTo(900002);
      check.close();
    });
  }

  /**
   * Three CREATE clauses where each later CREATE references a variable created by an earlier one:
   * CREATE a, then CREATE b linked to a, then CREATE c linked to b. Verifies the created-variable
   * threading survives across a longer chain of CreateSteps.
   */
  @Test
  void threeCreateClausesEachReferencingPreviousCreatedVariable() {
    final String write = "MATCH (p:Person) WHERE p.id = 10 "
        + "CREATE (a:Person {id: 910001, name: 'a', age: 1, city: 'c'}) "
        + "CREATE (a)-[:KNOWS {since: 2001}]->(b:Person {id: 910002, name: 'b', age: 2, city: 'c'}) "
        + "CREATE (b)-[:KNOWS {since: 2002}]->(p)";

    final String plan = profilePlan(write);
    assertThat(plan).as("multi-CREATE chain must use the optimizer\n%s", plan).contains("NodeIndexSeek");

    database.transaction(() -> {
      // a -> b -> p(id=10) chain: verify every hop landed on the intended vertex.
      final ResultSet check = database.query("opencypher",
          "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(p:Person) "
              + "WHERE a.id = 910001 RETURN a.id AS a, b.id AS b, p.id AS p");
      assertThat(check.hasNext()).isTrue();
      final Result row = check.next();
      assertThat((Integer) row.getProperty("a")).isEqualTo(910001);
      assertThat((Integer) row.getProperty("b")).isEqualTo(910002);
      assertThat((Integer) row.getProperty("p")).isEqualTo(10);
      assertThat(check.hasNext()).isFalse();
      check.close();
    });
  }

  /**
   * MATCH + CREATE vertex + CREATE edge back to the matched node (edge direction into the matched
   * node). Confirms the matched variable stays bound through the CREATE chain.
   */
  @Test
  void createEdgeBackToMatchedNode() {
    final String write = "MATCH (p:Person) WHERE p.id = 20 "
        + "CREATE (q:Person {id: 920001, name: 'back', age: 5, city: 'c'}) "
        + "CREATE (q)-[:KNOWS {since: 1999}]->(p)";

    final String plan = profilePlan(write);
    assertThat(plan).contains("NodeIndexSeek");

    database.transaction(() -> {
      final ResultSet check = database.query("opencypher",
          "MATCH (q:Person)-[:KNOWS]->(p:Person) WHERE q.id = 920001 RETURN p.id AS dst");
      assertThat(check.hasNext()).isTrue();
      assertThat((Integer) check.next().getProperty("dst")).isEqualTo(20);
      assertThat(check.hasNext()).isFalse();
      check.close();
    });
  }

  /**
   * Mixed CREATE/SET/MERGE ordering after the MATCH. The clauses must be applied in textual order:
   * CREATE q, SET q.age, then MERGE the edge. A single MERGE keeps the optimizer eligible.
   */
  @Test
  void mixedCreateSetMergeOrdering() {
    final String write = "MATCH (p:Person) WHERE p.id = 30 "
        + "CREATE (q:Person {id: 930001, name: 'mix', age: 5, city: 'c'}) "
        + "SET q.age = 77 "
        + "MERGE (p)-[:KNOWS {since: 2030}]->(q)";

    final String plan = profilePlan(write);
    assertThat(plan).as("mixed CREATE/SET/MERGE must still use the optimizer\n%s", plan).contains("NodeIndexSeek");

    database.transaction(() -> {
      final ResultSet check = database.query("opencypher",
          "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 30 RETURN q.id AS dst, q.age AS age");
      assertThat(check.hasNext()).isTrue();
      final Result row = check.next();
      assertThat((Integer) row.getProperty("dst")).isEqualTo(930001);
      // SET after CREATE must have overwritten the created age.
      assertThat((Integer) row.getProperty("age")).isEqualTo(77);
      assertThat(check.hasNext()).isFalse();
      check.close();
    });
  }

  /**
   * The multi-CREATE optimizer path must create exactly the expected number of vertices and edges
   * (no duplicate or dropped writes) across the whole Person type.
   */
  @Test
  void multiCreateProducesExactCounts() {
    final long personsBefore = countType("Person");

    database.transaction(() -> database.command("opencypher",
        "MATCH (p:Person) WHERE p.id = 50 "
            + "CREATE (q:Person {id: 940001, name: 'x', age: 5, city: 'c'}) "
            + "CREATE (p)-[:KNOWS {since: 2040}]->(q)"));

    // Exactly one new Person and one new KNOWS edge.
    assertThat(countType("Person")).isEqualTo(personsBefore + 1);
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c");
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      rs.close();
    });
  }

  private long countType(final String type) {
    final long[] c = new long[1];
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM `" + type + "`");
      c[0] = ((Number) rs.next().getProperty("c")).longValue();
      rs.close();
    });
    return c[0];
  }

  private String profilePlan(final String cypher) {
    return profilePlan(cypher, null);
  }

  private String profilePlan(final String cypher, final Map<String, Object> params) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = params != null
          ? database.command("opencypher", "PROFILE " + cypher, params)
          : database.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }
}
