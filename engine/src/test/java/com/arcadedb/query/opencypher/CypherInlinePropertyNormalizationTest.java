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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An inline property map in a MATCH pattern is an equality predicate, exactly like the same
 * comparison written in WHERE. The cost-based optimizer used to refuse any statement whose pattern
 * carried one, so {@code MATCH (n:Person {id: $id})-[:KNOWS]->(m)} fell back to step-by-step
 * interpretation - no index selection, no anchor selection, no filter pushdown - while the identical
 * query written as {@code WHERE n.id = $id} got the full treatment.
 * <p>
 * These tests pin both halves of the contract: the two spellings must return the same rows, and they
 * must be planned the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherInlinePropertyNormalizationTest {
  private static final String DATABASE_PATH = "./target/databases/cypher-inline-property-normalization";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();
    final var person = database.getSchema().createVertexType("Person");
    person.createProperty("id", Type.STRING);
    person.createProperty("name", Type.STRING);
    person.createProperty("city", Type.STRING);
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      final Map<String, MutableVertex> people = new HashMap<>();
      people.put("alice", person("alice", "Alice", "Rome"));
      people.put("bob", person("bob", "Bob", "Rome"));
      people.put("carol", person("carol", "Carol", "Milan"));
      // a namesake in another city: an inline property map that is dropped would let it through
      people.put("alice2", person("alice2", "Alice", "Milan"));

      knows(people, "alice", "bob");
      knows(people, "alice", "carol");
      people.get("alice2").newEdge("KNOWS", people.get("carol"), true, (Object[]) null).save();

      for (int i = 0; i < 256; i++)
        person("decoy-" + i, "Decoy" + i, "Naples");
    });

    database.transaction(() -> database.getSchema().createTypeIndex(
        Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void anIndexedInlinePropertyIsPlannedLikeTheEquivalentWherePredicate() {
    assertSpellingsAgree(
        "MATCH (n:Person {id: 'alice'})-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value",
        "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.id = 'alice' RETURN m.name AS value ORDER BY value");
    assertThat(names("MATCH (n:Person {id: 'alice'})-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value"))
        .containsExactly("Bob", "Carol");
    assertThat(planOf("MATCH (n:Person {id: 'alice'})-[:KNOWS]->(m:Person) RETURN m.name AS value"))
        .contains("Cost-Based Optimizer")
        .contains("NodeIndexSeek");
  }

  @Test
  void aParameterizedInlinePropertyIsPlannedLikeTheEquivalentWherePredicate() {
    final Map<String, Object> parameters = Map.of("id", "alice");
    assertThat(names("MATCH (n:Person {id: $id})-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value", parameters))
        .containsExactly("Bob", "Carol");
    assertThat(planOf("MATCH (n:Person {id: $id})-[:KNOWS]->(m:Person) RETURN m.name AS value", parameters))
        .contains("Cost-Based Optimizer")
        .contains("NodeIndexSeek");
  }

  @Test
  void anUnindexedInlinePropertyStillFiltersEveryRow() {
    // "Alice" is not unique: only the Rome one knows Bob
    assertSpellingsAgree(
        "MATCH (n:Person {name: 'Alice', city: 'Rome'})-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value",
        "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.name = 'Alice' AND n.city = 'Rome' RETURN m.name AS value ORDER BY value");
    assertThat(names("MATCH (n:Person {name: 'Alice', city: 'Rome'})-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value"))
        .containsExactly("Bob", "Carol");
  }

  @Test
  void anInlinePropertyOnTheFarSideOfThePatternIsApplied() {
    assertSpellingsAgree(
        "MATCH (n:Person {id: 'alice'})-[:KNOWS]->(m:Person {city: 'Milan'}) RETURN m.name AS value ORDER BY value",
        "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.id = 'alice' AND m.city = 'Milan' RETURN m.name AS value ORDER BY value");
    assertThat(names("MATCH (n:Person {id: 'alice'})-[:KNOWS]->(m:Person {city: 'Milan'}) RETURN m.name AS value"))
        .containsExactly("Carol");
  }

  @Test
  void anInlinePropertyOnAnAnonymousNodeIsApplied() {
    assertSpellingsAgree(
        "MATCH (:Person {name: 'Alice', city: 'Milan'})-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value",
        "MATCH (a:Person)-[:KNOWS]->(m:Person) WHERE a.name = 'Alice' AND a.city = 'Milan' RETURN m.name AS value ORDER BY value");
    assertThat(names("MATCH (:Person {name: 'Alice', city: 'Milan'})-[:KNOWS]->(m:Person) RETURN m.name AS value"))
        .containsExactly("Carol");
  }

  @Test
  void anInlinePropertyOnASingleNodePatternIsApplied() {
    assertSpellingsAgree(
        "MATCH (n:Person {city: 'Rome'}) RETURN n.name AS value ORDER BY value",
        "MATCH (n:Person) WHERE n.city = 'Rome' RETURN n.name AS value ORDER BY value");
    assertThat(names("MATCH (n:Person {city: 'Rome'}) RETURN n.name AS value ORDER BY value"))
        .containsExactly("Alice", "Bob");
  }

  @Test
  void anInlinePropertyCombinesWithAWhereClauseOnTheSameNode() {
    assertSpellingsAgree(
        "MATCH (n:Person {name: 'Alice'}) WHERE n.city = 'Milan' RETURN n.id AS value ORDER BY value",
        "MATCH (n:Person) WHERE n.name = 'Alice' AND n.city = 'Milan' RETURN n.id AS value ORDER BY value");
    assertThat(names("MATCH (n:Person {name: 'Alice'}) WHERE n.city = 'Milan' RETURN n.id AS value"))
        .containsExactly("alice2");
  }

  @Test
  void anEmptyInlinePropertyMapMatchesEverything() {
    assertThat(names("MATCH (n:Person {})-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value"))
        .isEqualTo(names("MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN m.name AS value ORDER BY value"));
  }

  @Test
  void anInlinePropertyMatchingNothingReturnsNoRows() {
    assertThat(names("MATCH (n:Person {id: 'nobody'})-[:KNOWS]->(m:Person) RETURN m.name AS value")).isEmpty();
    assertThat(names("MATCH (n:Person {city: 'Atlantis'}) RETURN n.name AS value")).isEmpty();
  }

  /**
   * The label of an anonymous node constrains the pattern exactly like a named one's. The optimizer
   * kept only named nodes in its plan, so it had nothing to filter an anonymous target against and
   * silently returned rows that do not match the pattern.
   */
  @Test
  void theLabelOfAnAnonymousNodeIsApplied() {
    database.getSchema().createVertexType("Company").createProperty("id", Type.STRING);
    database.transaction(() -> {
      final MutableVertex acme = database.newVertex("Company").set("id", "acme").save();
      final MutableVertex alice = database.query("opencypher", "MATCH (p:Person) WHERE p.id = 'alice' RETURN p")
          .next().<Vertex>getProperty("p").modify();
      alice.newEdge("KNOWS", acme, true, (Object[]) null).save();
    });

    // Alice knows Bob and Carol (Person) plus Acme (Company)
    assertThat(names("MATCH (n:Person {id: 'alice'})-[:KNOWS]->(:Company) RETURN count(*) AS value"))
        .containsExactly("1");
    assertThat(names("MATCH (n:Person {id: 'alice'})-[:KNOWS]->(:Person) RETURN count(*) AS value"))
        .containsExactly("2");
    assertSpellingsAgree(
        "MATCH (n:Person {id: 'alice'})-[:KNOWS]->(:Company) RETURN count(*) AS value",
        "MATCH (n:Person)-[:KNOWS]->(c:Company) WHERE n.id = 'alice' RETURN count(*) AS value");
  }

  @Test
  void inlinePropertiesOfWriteClausesKeepTheirCreateSemantics() {
    database.transaction(() -> database.command("opencypher",
        "MERGE (n:Person {id: 'dave', name: 'Dave', city: 'Turin'})"));
    assertThat(names("MATCH (n:Person {id: 'dave'}) RETURN n.name AS value")).containsExactly("Dave");

    // a second MERGE with the same map must match the existing vertex, not create a twin
    database.transaction(() -> database.command("opencypher",
        "MERGE (n:Person {id: 'dave', name: 'Dave', city: 'Turin'})"));
    assertThat(names("MATCH (n:Person {name: 'Dave'}) RETURN n.id AS value")).containsExactly("dave");
  }

  private void assertSpellingsAgree(final String inlineForm, final String whereForm) {
    assertThat(names(inlineForm))
        .as("the inline property map must return what the WHERE predicate returns: %s", inlineForm)
        .isEqualTo(names(whereForm));
    assertThat(planOf(inlineForm))
        .as("the inline property map must be planned like the WHERE predicate: %s", inlineForm)
        .contains(planKind(planOf(whereForm)));
  }

  private static String planKind(final String plan) {
    return plan.contains("Cost-Based Optimizer") ? "Cost-Based Optimizer" : "Traditional";
  }

  private List<String> names(final String query) {
    return names(query, Map.of());
  }

  private List<String> names(final String query, final Map<String, Object> parameters) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query, parameters)) {
      while (resultSet.hasNext())
        values.add(String.valueOf(resultSet.next().<Object>getProperty("value")));
    }
    return values;
  }

  private String planOf(final String query) {
    return planOf(query, Map.of());
  }

  private String planOf(final String query, final Map<String, Object> parameters) {
    try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private MutableVertex person(final String id, final String name, final String city) {
    return database.newVertex("Person").set("id", id).set("name", name).set("city", city).save();
  }

  private void knows(final Map<String, MutableVertex> people, final String from, final String to) {
    people.get(from).newEdge("KNOWS", people.get(to), true, (Object[]) null).save();
  }
}
