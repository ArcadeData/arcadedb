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
import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8584: for a pattern whose parts no relationship connects - {@code MATCH (a:T), (b:T)}, or two MATCH clauses -
 * the planner crossed the parts with a Cartesian product and evaluated the whole WHERE above it. A predicate on one
 * part did not narrow it, and one relating the parts ran on every combination while the product held the whole right
 * side in heap.
 * <p>
 * A conjunct reading one part now narrows that part below any join, and the parts are joined on the conjuncts relating
 * them: by seeking an index per row, or by a hash join. The answer must not change: every join query is checked against
 * the same query with its relating conjunct written {@code (... OR rand() < 0)}, which no join can use and so still runs as a
 * filtered product - the semantics the joins must keep.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDisconnectedPatternJoinIssue8584Test extends TestHelper {
  private static final int PERSONS = 10;
  private static final int CITIES  = 400;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType city = database.getSchema().createVertexType("City");
      city.createProperty("id", Type.INTEGER);
      city.createProperty("code", Type.STRING);
      city.createProperty("country", Type.STRING);
      city.createProperty("rating", Type.DOUBLE);
      city.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      city.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "code");
      city.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "country", "code");
      city.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "rating");

      final VertexType person = database.getSchema().createVertexType("Person");
      person.createProperty("id", Type.INTEGER);
      person.createProperty("score", Type.FLOAT);
      person.createProperty("zip", Type.LONG);
      database.getSchema().createEdgeType("KNOWS");
      database.getSchema().createEdgeType("IN_CITY");

      final VertexType place = database.getSchema().createVertexType("Place");
      place.createProperty("pid", Type.INTEGER);
      place.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "pid");
      database.getSchema().createVertexType("Town").addSuperType(place);
      database.getSchema().createVertexType("Village").addSuperType(place);

      database.getSchema().createVertexType("Item");
      database.getSchema().createVertexType("Ref");

      for (int i = 0; i < CITIES; i++)
        database.newVertex("City").set("id", i).set("code", "C" + (i % 3)).set("country", i % 2 == 0 ? "IT" : "US")
            .set("rating", i / 20.0).save();
      // A code spelling a RID, which a number equals when it is the id() of that RID
      database.newVertex("City").set("id", 1000).set("code", "#3:7").set("country", "IT").set("rating", 99.5).save();

      final List<MutableVertex> persons = new ArrayList<>();
      for (int i = 0; i < PERSONS; i++) {
        final MutableVertex p = database.newVertex("Person").set("id", i).set("name", i % 5 == 0 ? "Ann" : "bob" + (i % 4))
            .set("cityCode", "C" + (i % 4)).set("score", (float) (i / 20.0)).set("zip", (long) i);
        // The cities past 399 do not exist, and persons 0 and 7 have none
        if (i % 7 != 0)
          p.set("cityId", (long) i * 70);
        persons.add(p.save());
      }
      persons.get(1).set("cityId", IdFunction.encodeRidAsLong(new RID("#3:7"))).save();
      for (int i = 0; i < PERSONS; i++)
        persons.get(i).newEdge("KNOWS", persons.get((i + 1) % PERSONS)).save();
      // A self loop, which two relationships of one MATCH clause must not both bind
      persons.get(5).newEdge("KNOWS", persons.get(5)).save();

      // Every person is in the city of its index, which an anonymous (:City {id: ...}) is sought by
      try (final ResultSet rs = database.query("opencypher", "MATCH (c:City) WHERE c.id < $n RETURN c ORDER BY c.id",
          Map.of("n", PERSONS))) {
        int i = 0;
        while (rs.hasNext())
          persons.get(i++).newEdge("IN_CITY", rs.next().<Vertex>getProperty("c")).save();
      }

      // Persons 5 and 9 live in a village, which a seek of the index Town inherits from Place also finds
      for (int k = 1; k <= 300; k++)
        database.newVertex(k == 5 || k == 9 ? "Village" : "Town").set("pid", 70 * k).save();

      // Values the Cypher = calls equal across Java types, and values it calls equal to nothing
      final Object[][] items = {
          { 1, 1 }, { 1L, 1.0d }, { 1.0d, 1.0f }, { 0.05f, 0.05d }, { -0.0d, 0.0d }, { Double.NaN, Double.NaN },
          { null, null }, { "1", "1" }, { true, true }, { 9007199254740993L, 9007199254740992.0d },
          { List.of(1, 2), List.of(1L, 2.0d) }, { LocalDate.of(2020, 1, 1), LocalDate.of(2020, 1, 1) }, { 2, "2" },
          { Map.of("k", 1), Map.of("k", 1L) } };
      for (int i = 0; i < items.length; i++) {
        final MutableVertex item = database.newVertex("Item").set("id", i).set("name", i % 2 == 0 ? "Same" : "same");
        if (items[i][0] != null)
          item.set("x", items[i][0]);
        if (items[i][1] != null)
          item.set("y", items[i][1]);
        item.save();
      }
    });

    database.transaction(() -> {
      // A string spelling the RID of each Ref's target
      try (final ResultSet rs = database.query("opencypher", "MATCH (c:City) RETURN c ORDER BY c.id LIMIT 3")) {
        int i = 0;
        while (rs.hasNext())
          database.newVertex("Ref").set("id", i++).set("ref", rs.next().<Vertex>getProperty("c").getIdentity().toString()).save();
      }
    });
  }

  @Test
  void conjunctsOnOnePartNarrowItBelowTheJoin() {
    final String query = "MATCH (a:Item), (b:Item) WHERE b.id > 5 AND a.id < 3 RETURN a.id AS a, b.id AS b";
    final String plan = plan(query);
    assertThat(plan).contains("NodeByLabelScan(a:Item) [filter: a.id < 3]");
    assertThat(plan).contains("NodeByLabelScan(b:Item) [filter: b.id > 5]");
    assertThat(plan).doesNotContain("+ Filter");
    assertThat(pairs(query)).hasSize(3 * 8);
  }

  @Test
  void crossEqualityWithoutAnIndexIsAHashJoin() {
    final String query = "MATCH (a:Item), (b:Item) WHERE a.x = b.y RETURN a.id AS a, b.id AS b";
    assertThat(plan(query)).contains("ValueHashJoin");
    assertSameAsFilteredProduct(query, "a.x = b.y");
  }

  @Test
  void hashJoinKeepsTheCypherEqualityAcrossJavaTypes() {
    final List<String> pairs = assertSameAsFilteredProduct("MATCH (a:Item), (b:Item) WHERE a.x = b.y RETURN a.id AS a, b.id AS b",
        "a.x = b.y");
    // 1 = 1 = 1.0, a float through its decimal form, -0.0 = 0.0, 2^53 + 1 = 2^53 as doubles, lists element-wise, dates
    assertThat(pairs).contains("0/0", "0/1", "0/2", "1/0", "2/1", "3/3", "4/4", "7/7", "8/8", "9/9", "10/10", "11/11");
    // NaN and null equal nothing, a number no string
    assertThat(pairs).noneMatch(p -> p.startsWith("5/") || p.startsWith("6/") || p.endsWith("/5") || p.endsWith("/6"));
    assertThat(pairs).doesNotContain("12/12");
  }

  @Test
  void aStringSpellingARidEqualsItsId() {
    final String query = "MATCH (c:City), (r:Ref) WHERE id(c) = r.ref RETURN c.id AS a, r.id AS b";
    final List<String> pairs = assertSameAsFilteredProduct(query, "id(c) = r.ref");
    assertThat(pairs).containsExactly("0/0", "1/1", "2/2");
  }

  @Test
  void crossEqualityOnAnIndexedPropertySeeksPerRow() {
    final String query = "MATCH (p:Person), (c:City) WHERE c.id = p.cityId RETURN p.id AS a, c.id AS b";
    assertThat(plan(query)).contains("IndexNestedLoopJoin(c:City)").doesNotContain("CartesianProduct");
    final List<String> pairs = assertSameAsFilteredProduct(query, "c.id = p.cityId");
    assertThat(pairs).isNotEmpty();

    // The issue's other shape: two MATCH clauses
    final String twoClauses = "MATCH (p:Person) MATCH (c:City) WHERE p.cityId = c.id RETURN p.id AS a, c.id AS b";
    assertThat(plan(twoClauses)).contains("IndexNestedLoopJoin(c:City)");
    assertThat(pairs(twoClauses)).isEqualTo(pairs);
  }

  @Test
  void theSoughtNodeKeepsItsOwnConjuncts() {
    final String query = "MATCH (p:Person), (c:City) WHERE c.id = p.cityId AND c.country = 'IT' AND p.id > 2 RETURN p.id AS a, c.id AS b";
    assertThat(plan(query)).contains("IndexNestedLoopJoin(c:City)");
    assertSameAsFilteredProduct(query, "c.id = p.cityId");
  }

  @Test
  void aCompositeIndexIsSoughtByAConstantAndTheRow() {
    final String query = "MATCH (p:Person), (c:City) WHERE c.country = $country AND c.code = p.cityCode RETURN p.id AS a, c.id AS b";
    assertThat(plan(query, Map.of("country", "IT"))).contains("IndexNestedLoopJoin(c:City)").contains("country=$country");
    final List<String> pairs = pairs(query, Map.of("country", "IT"));
    assertThat(pairs).isEqualTo(pairs(query.replace("c.code = p.cityCode", "(c.code = p.cityCode OR rand() < 0)"), Map.of("country", "IT")));
    assertThat(pairs).isNotEmpty();
  }

  @Test
  void aKeyTheIndexConversionCannotFollowReadsTheLabel() {
    // A number against a string property: only a string spelling the RID the number encodes equals it
    final String query = "MATCH (p:Person), (c:City) WHERE c.code = p.cityId RETURN p.id AS a, c.id AS b";
    assertThat(plan(query)).contains("IndexNestedLoopJoin(c:City)");
    assertThat(assertSameAsFilteredProduct(query, "c.code = p.cityId")).containsExactly("1/1000");
  }

  @Test
  void aKeyDeclaredOfAnotherKindIsNotSought() {
    // A LONG against a STRING index would read the whole label for every person: the planner knows it from the schema
    final String query = "MATCH (p:Person), (c:City) WHERE c.code = p.zip RETURN p.id AS a, c.id AS b";
    assertThat(plan(query)).doesNotContain("IndexNestedLoopJoin").contains("ValueHashJoin");
    assertThat(assertSameAsFilteredProduct(query, "c.code = p.zip")).isEmpty();
  }

  @Test
  void aFloatKeyIsSoughtThroughItsDecimalForm() {
    final String query = "MATCH (p:Person), (c:City) WHERE c.rating = p.score RETURN p.id AS a, c.id AS b";
    assertThat(plan(query)).contains("IndexNestedLoopJoin(c:City)");
    final List<String> pairs = assertSameAsFilteredProduct(query, "c.rating = p.score");
    // Every person's score is the rating of the city of the same index, 0.05 included
    assertThat(pairs).hasSize(PERSONS).contains("1/1", "3/3");
  }

  @Test
  void anIndexInheritedFromASuperTypeSeeksOnlyTheLabel() {
    final String query = "MATCH (p:Person), (t:Town) WHERE t.pid = p.cityId RETURN p.id AS a, t.pid AS b";
    assertThat(plan(query)).contains("IndexNestedLoopJoin(t:Town)");
    assertThat(assertSameAsFilteredProduct(query, "t.pid = p.cityId")).containsExactly("2/140", "3/210", "4/280", "6/420", "8/560");
  }

  @Test
  void otherConjunctsRelatingThePartsStillFilterTheJoin() {
    assertSameAsFilteredProduct("MATCH (a:Item), (b:Item) WHERE a.x = b.y AND a.id < b.id RETURN a.id AS a, b.id AS b",
        "a.x = b.y");
    assertSameAsFilteredProduct("MATCH (a:Person), (b:Person) WHERE toLower(a.name) = toLower(b.name) AND a.id <> b.id "
        + "RETURN a.id AS a, b.id AS b", "toLower(a.name) = toLower(b.name)");
  }

  @Test
  void threePartsAreJoinedPairByPair() {
    final String query = "MATCH (p:Person), (c:City), (i:Item) WHERE c.id = p.cityId AND i.id = c.id RETURN p.id AS a, i.id AS b";
    final String plan = plan(query);
    assertThat(plan).contains("IndexNestedLoopJoin(c:City)").doesNotContain("CartesianProduct");
    assertSameAsFilteredProduct(query, "c.id = p.cityId", "i.id = c.id");
  }

  @Test
  void aHashJoinHoldsTheSmallerSide() {
    // Persons and cities are joined first, a handful of rows: the Items joined onto them are not the side to hold
    final String query = "MATCH (p:Person), (c:City), (i:Item) WHERE c.id = p.cityId AND i.id = p.id RETURN p.id AS a, i.id AS b";
    assertThat(plan(query)).contains("ValueHashJoin [on=p.id = i.id] [build=left]");
    assertThat(assertSameAsFilteredProduct(query, "c.id = p.cityId", "i.id = p.id")).isNotEmpty();
  }

  @Test
  void relationshipComponentsAreJoinedAndKeepRelationshipUniqueness() {
    final String query = "MATCH (a:Person)-[r1:KNOWS]->(b:Person), (c:Person)-[r2:KNOWS]->(d:Person) WHERE b.id = c.id "
        + "RETURN a.id AS a, d.id AS b";
    assertThat(plan(query)).contains("ValueHashJoin").contains("RelationshipUniquenessFilter");
    final List<String> pairs = assertSameAsFilteredProduct(query, "b.id = c.id");
    // The self loop on 5 cannot be walked twice by r1 and r2
    assertThat(pairs).doesNotContain("5/5");
    assertThat(pairs).contains("4/5", "5/6", "4/6");
  }

  @Test
  void aLoneNodeAfterAConnectedPatternIsSoughtPerRow() {
    final String query = "MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (c:City) WHERE c.id = b.cityId RETURN a.id AS a, c.id AS b";
    assertThat(plan(query)).contains("IndexNestedLoopJoin(c:City)");
    assertSameAsFilteredProduct(query, "c.id = b.cityId");
  }

  @Test
  void anAnonymousPartIsStillCrossed() {
    // Planning the named node alone answered |City| rows, and left the anonymous one unbound
    assertThat(count("MATCH (:City), (c:City) RETURN count(*) AS c")).isEqualTo((long) (CITIES + 1) * (CITIES + 1));
    assertThat(count("MATCH (a:Person)-[:KNOWS]->(b:Person), (:City) RETURN count(*) AS c"))
        .isEqualTo((long) (PERSONS + 1) * (CITIES + 1));
    try (final ResultSet rs = database.query("opencypher", "MATCH (:City), (c:City) RETURN c.id AS id LIMIT 50")) {
      while (rs.hasNext())
        assertThat(rs.next().<Integer>getProperty("id")).isNotNull();
    }
  }

  @Test
  void anAnonymousNodeIsSoughtByItsIndex() {
    // The statistics of an anonymous node's label were never collected: its index read back as missing
    final String query = "MATCH (p:Person)-[:IN_CITY]->(:City {id: 3}) RETURN p.id AS a, 0 AS b";
    assertThat(plan(query)).containsPattern("NodeIndexSeek\\(  __anon[0-9]+:City\\)");
    assertThat(pairs(query)).containsExactly("3/0");
  }

  @Test
  void aPatternNamingNoNodeIsLeftToTheOrdinaryPipeline() {
    for (final String query : List.of("MATCH (:City), (:Item) RETURN count(*) AS c", "MATCH (:City) MATCH (:Item) RETURN count(*) AS c")) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).as(query).doesNotContain("Using Cost-Based Query Optimizer");
      }
      assertThat(count(query)).as(query).isEqualTo((long) (CITIES + 1) * 14);
    }
  }

  @Test
  void aWriteOverAJoinCreatesOneEdgePerMatch() {
    final long expected = pairs("MATCH (p:Person), (c:City) WHERE (c.id = p.cityId OR rand() < 0) RETURN p.id AS a, c.id AS b").size();
    database.transaction(() -> database.command("opencypher",
        "MATCH (p:Person), (c:City) WHERE c.id = p.cityId CREATE (p)-[:LIVES_IN]->(c)").close());
    assertThat(count("MATCH (:Person)-[r:LIVES_IN]->(:City) RETURN count(r) AS c")).isEqualTo(expected);
  }

  /**
   * Runs the query and the same query with each {@code relating} conjunct written as {@code (relating OR rand() < 0)}, which no join can
   * use, and checks they answer the same pairs.
   */
  private List<String> assertSameAsFilteredProduct(final String query, final String... relating) {
    String reference = query;
    for (final String conjunct : relating)
      reference = reference.replace(conjunct, "(" + conjunct + " OR rand() < 0)");
    assertThat(reference).isNotEqualTo(query);
    assertThat(plan(reference)).as("the reference must not join").doesNotContain("ValueHashJoin")
        .doesNotContain("IndexNestedLoopJoin");
    final List<String> pairs = pairs(query);
    assertThat(pairs).as(query).isEqualTo(pairs(reference));
    return pairs;
  }

  private List<String> pairs(final String query) {
    return pairs(query, Map.of());
  }

  private List<String> pairs(final String query, final Map<String, Object> parameters) {
    final List<String> pairs = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        pairs.add(row.<Object>getProperty("a") + "/" + row.<Object>getProperty("b"));
      }
    }
    Collections.sort(pairs);
    return pairs;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private String plan(final String query) {
    return plan(query, Map.of());
  }

  private String plan(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, parameters)) {
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).as(query).contains("Using Cost-Based Query Optimizer");
      return plan;
    }
  }
}
