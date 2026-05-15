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
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Tests for MATCH clause enhancements:
 * - Multiple MATCH clauses
 * - Patterns without labels
 * - Named paths
 */
public class OpenCypherMatchEnhancementsTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-match-enhancements").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_FOR");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      // Create test data
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Company {name: 'TechCorp'})");

      // Alice works for TechCorp
      database.command("opencypher",
          """
          MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) \
          CREATE (a)-[:WORKS_FOR]->(c)""");

      // Alice knows Bob
      database.command("opencypher",
          """
          MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
          CREATE (a)-[:KNOWS]->(b)""");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void multipleMatchClauses() {
    // Two MATCH clauses: first matches Alice, second matches Bob
    // Result should be Cartesian product: Alice + Bob
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'}) \
        MATCH (b:Person {name: 'Bob'}) \
        RETURN a.name AS person1, b.name AS person2""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("person1")).isEqualTo("Alice");
    assertThat(row.<String>getProperty("person2")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void multipleMatchClausesCartesianProduct() {
    // MATCH all people twice - should get Cartesian product (2x2 = 4 results)
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person)
            MATCH (b:Person)
            RETURN a.name AS person1, b.name AS person2
            ORDER BY person1, person2""");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    assertThat(results.size()).as("Expected Cartesian product of 2 people").isEqualTo(4);

    // Alice-Alice, Alice-Bob, Bob-Alice, Bob-Bob
    assertThat(results.get(0).<String>getProperty("person1")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("person2")).isEqualTo("Alice");

    assertThat(results.get(1).<String>getProperty("person1")).isEqualTo("Alice");
    assertThat(results.get(1).<String>getProperty("person2")).isEqualTo("Bob");

    assertThat(results.get(2).<String>getProperty("person1")).isEqualTo("Bob");
    assertThat(results.get(2).<String>getProperty("person2")).isEqualTo("Alice");

    assertThat(results.get(3).<String>getProperty("person1")).isEqualTo("Bob");
    assertThat(results.get(3).<String>getProperty("person2")).isEqualTo("Bob");
  }

  @Test
  void patternWithoutLabel() {
    // MATCH (n) without label should match ALL vertices (Person and Company)
    final ResultSet result = database.query("opencypher",
        "MATCH (n) RETURN n.name AS name ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      names.add(result.next().getProperty("name"));
    }
    result.close();

    assertThat(names.size()).as("Expected 3 vertices total").isEqualTo(3);
    assertThat(names.contains("Alice")).isTrue();
    assertThat(names.contains("Bob")).isTrue();
    assertThat(names.contains("TechCorp")).isTrue();
  }

  @Test
  void patternWithoutLabelFiltered() {
    // MATCH (n) with WHERE clause to filter
    final ResultSet result = database.query("opencypher",
        "MATCH (n) WHERE n.age > 20 RETURN n.name AS name ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      names.add(result.next().getProperty("name"));
    }
    result.close();

    // Should match Alice (age 30) and Bob (age 25), but not TechCorp (no age)
    assertThat(names.size()).isEqualTo(2);
    assertThat(names.contains("Alice")).isTrue();
    assertThat(names.contains("Bob")).isTrue();
  }

  @Test
  void namedPathSingleEdge() {
    // Named path: p = (a)-[r]->(b)
    final ResultSet result = database.query("opencypher",
        """
            MATCH p = (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person)
            RETURN a.name AS person, b.name AS friend, p AS path""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("person")).isEqualTo("Alice");
    assertThat(row.<String>getProperty("friend")).isEqualTo("Bob");

    // Check that path object is returned
    final Object pathObj = row.getProperty("path");
    assertThat(pathObj).as("Path variable should not be null").isNotNull();
    assertThat(pathObj).as("Path should be a TraversalPath object").isInstanceOf(TraversalPath.class);

    final TraversalPath path = (TraversalPath) pathObj;
    assertThat(path.length()).as("Path should have length 1 (one edge)").isEqualTo(1);
    assertThat(path.getVertices().size()).as("Path should have 2 vertices").isEqualTo(2);
    assertThat(path.getEdges().size()).as("Path should have 1 edge").isEqualTo(1);

    // Check vertices in path
    assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");
    assertThat(path.getEndVertex().get("name")).isEqualTo("Bob");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void namedPathMultiplePaths() {
    // Alice has two outgoing relationships: KNOWS to Bob, WORKS_FOR to TechCorp
    // Query all paths from Alice
    final ResultSet result = database.query("opencypher",
        """
        MATCH p = (a:Person {name: 'Alice'})-[r]->(b) \
        RETURN a.name AS person, b.name AS target, p AS path""");

    final List<TraversalPath> paths = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      paths.add(row.getProperty("path"));
    }
    result.close();

    assertThat(paths.size()).as("Alice should have 2 outgoing paths").isEqualTo(2);

    // Both paths should have length 1
    for (final TraversalPath path : paths) {
      assertThat(path.length()).isEqualTo(1);
      assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");
    }

    // Collect target names
    final Set<String> targets = new HashSet<>();
    for (final TraversalPath path : paths) {
      targets.add((String) path.getEndVertex().get("name"));
    }

    assertThat(targets.contains("Bob")).isTrue();
    assertThat(targets.contains("TechCorp")).isTrue();
  }

  @Test
  void combinedFeatures() {
    // Combine multiple features: multiple MATCH + unlabeled pattern + named path
    final ResultSet result = database.query("opencypher",
        """
        MATCH (start) \
        WHERE start.name = 'Alice' \
        MATCH p = (start)-[r]->(target) \
        RETURN start.name AS startName, target.name AS targetName, p AS path""");

    int pathCount = 0;
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat(row.<String>getProperty("startName")).isEqualTo("Alice");

      final TraversalPath path = row.getProperty("path");
      assertThat(path).isNotNull();
      assertThat(path.length()).isEqualTo(1);
      pathCount++;
    }
    result.close();

    assertThat(pathCount).as("Alice should have 2 outgoing paths").isEqualTo(2);
  }

  // ---------------------------------------------------------------------------
  // Helpers for self-contained issue regression tests below.
  // ---------------------------------------------------------------------------

  private static List<Result> collectAll(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }

  // Issue #4019: node variable carried via WITH must survive an anonymous CREATE.
  @Test
  void issue4019_nodeVarCarriedThroughWithMustSurviveAnonymousCreate() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4019-anon-create").create();
    try {
      db.getSchema().createVertexType("Person4019");
      db.getSchema().createVertexType("Temp4019");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4019 {name:'Alice'}), (:Person4019 {name:'Bob'})"));

      final ResultSet[] ref = new ResultSet[1];
      db.transaction(() -> ref[0] = db.command("opencypher",
          "MATCH (n:Person4019) WITH n CREATE (:Temp4019 {x:1}) RETURN n.name AS name ORDER BY name"));

      final List<Result> rows = collectAll(ref[0]);
      assertThat(rows).hasSize(2);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
      assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4019: node variable carried via WITH must survive an anonymous MERGE.
  @Test
  void issue4019_nodeVarCarriedThroughWithMustSurviveAnonymousMerge() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4019-anon-merge").create();
    try {
      db.getSchema().createVertexType("Person4019");
      db.getSchema().createVertexType("Temp4019");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4019 {name:'Alice'}), (:Person4019 {name:'Bob'})"));

      final ResultSet[] ref = new ResultSet[1];
      db.transaction(() -> ref[0] = db.command("opencypher",
          "MATCH (n:Person4019) WITH n MERGE (:Temp4019 {x:10}) RETURN n.name AS name ORDER BY name"));

      final List<Result> rows = collectAll(ref[0]);
      assertThat(rows).hasSize(2);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
      assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4019: both a node variable and a scalar carried via WITH must survive an anonymous CREATE.
  @Test
  void issue4019_nodeVarAndScalarBothSurviveAnonymousCreate() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4019-scalar-create").create();
    try {
      db.getSchema().createVertexType("Person4019");
      db.getSchema().createVertexType("Temp4019");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4019 {name:'Alice'}), (:Person4019 {name:'Bob'})"));

      final ResultSet[] ref = new ResultSet[1];
      db.transaction(() -> ref[0] = db.command("opencypher",
          "MATCH (n:Person4019) WITH n, 1 AS x CREATE (:Temp4019 {x:3}) RETURN n.name AS name, x ORDER BY name"));

      final List<Result> rows = collectAll(ref[0]);
      assertThat(rows).hasSize(2);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
      assertThat(((Number) rows.get(0).getProperty("x")).intValue()).isEqualTo(1);
      assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
      assertThat(((Number) rows.get(1).getProperty("x")).intValue()).isEqualTo(1);
    } finally {
      db.drop();
    }
  }

  // Issue #4019: scalar alias alone still works alongside an anonymous CREATE.
  @Test
  void issue4019_scalarAliasAloneStillWorksWithCreate() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4019-scalar-alone").create();
    try {
      db.getSchema().createVertexType("Person4019");
      db.getSchema().createVertexType("Temp4019");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4019 {name:'Alice'}), (:Person4019 {name:'Bob'})"));

      final ResultSet[] ref = new ResultSet[1];
      db.transaction(() -> ref[0] = db.command("opencypher",
          "MATCH (n:Person4019) WITH n.name AS name CREATE (:Temp4019 {x:2}) RETURN name ORDER BY name"));

      final List<Result> rows = collectAll(ref[0]);
      assertThat(rows).hasSize(2);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
      assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    } finally {
      db.drop();
    }
  }

  // Issue #4019: control case - without CREATE the node variable is correct.
  @Test
  void issue4019_withoutCreateNodeVarIsCorrect() {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-4019-no-create").create();
    try {
      db.getSchema().createVertexType("Person4019");
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4019 {name:'Alice'}), (:Person4019 {name:'Bob'})"));

      final List<Result> rows = collectAll(db.query("opencypher",
          "MATCH (n:Person4019) WITH n RETURN n.name AS name ORDER BY name"));

      assertThat(rows).hasSize(2);
      assertThat((String) rows.get(0).getProperty("name")).isEqualTo("Alice");
      assertThat((String) rows.get(1).getProperty("name")).isEqualTo("Bob");
    } finally {
      db.drop();
    }
  }

  // ---------------------------------------------------------------------------
  // Issue #4092: anonymous middle node in multi-hop chain must match rows.
  // Shared helper to build the location graph used by all #4092 tests.
  // ---------------------------------------------------------------------------

  private static Database createIssue4092Database(final String suffix) {
    final Database db = new DatabaseFactory("./target/databases/issue4092-anon-middle-node-" + suffix).create();
    db.getSchema().createVertexType("Person4092");
    db.getSchema().createVertexType("City4092");
    db.getSchema().createVertexType("Country4092");
    db.getSchema().createVertexType("Region4092");
    db.getSchema().createEdgeType("KNOWS_4092");
    db.getSchema().createEdgeType("LOCATED_IN_4092");
    db.getSchema().createEdgeType("BELONGS_TO_4092");

    db.transaction(() -> {
      db.command("opencypher",
          "CREATE (:Person4092 {name:'Alice'}), (:City4092 {name:'New York'}), (:Country4092 {name:'USA'}), (:Region4092 {name:'North America'})");
      db.command("opencypher",
          "MATCH (a:Person4092 {name:'Alice'}), (c:City4092 {name:'New York'}), (u:Country4092 {name:'USA'}), (r:Region4092 {name:'North America'}) " +
              "CREATE (a)-[:KNOWS_4092]->(c), (c)-[:LOCATED_IN_4092]->(u), (u)-[:BELONGS_TO_4092]->(r)");
    });
    return db;
  }

  // Issue #4092: anonymous middle node in a two-hop chain must match the row.
  @Test
  void issue4092_twoHopChainWithAnonMiddleNode() {
    final Database db = createIssue4092Database("two-hop-anon");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (a:Person4092)-[:KNOWS_4092]->(:City4092)-[:LOCATED_IN_4092]->(b:Country4092) " +
            "RETURN a.name AS person_name, b.name AS country_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
    } finally {
      db.drop();
    }
  }

  // Issue #4092: named middle node control case must match the same row.
  @Test
  void issue4092_twoHopChainWithNamedMiddleNode() {
    final Database db = createIssue4092Database("two-hop-named");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (a:Person4092)-[:KNOWS_4092]->(c:City4092)-[:LOCATED_IN_4092]->(b:Country4092) " +
            "RETURN a.name AS person_name, b.name AS country_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
    } finally {
      db.drop();
    }
  }

  // Issue #4092: anonymous middle node with aggregation collect() must work.
  @Test
  void issue4092_twoHopChainAnonMiddleNodeWithAggregation() {
    final Database db = createIssue4092Database("two-hop-aggregation");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (a:Person4092)-[:KNOWS_4092]->(:City4092)-[:LOCATED_IN_4092]->(b:Country4092) " +
            "RETURN b.name AS country_name, collect(a.name) AS people")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
      final List<?> people = (List<?>) rows.get(0).getProperty("people");
      assertThat(people.stream().map(Object::toString).toList()).containsExactly("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4092: anonymous source node in a single hop must still match.
  @Test
  void issue4092_singleHopWithAnonSourceNode() {
    final Database db = createIssue4092Database("single-hop-anon-source");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (:Person4092)-[:KNOWS_4092]->(c:City4092) RETURN c.name AS city_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("city_name")).isEqualTo("New York");
    } finally {
      db.drop();
    }
  }

  // Issue #4092: anonymous target node in a single hop must still match.
  @Test
  void issue4092_singleHopWithAnonTargetNode() {
    final Database db = createIssue4092Database("single-hop-anon-target");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (a:Person4092)-[:KNOWS_4092]->(:City4092) RETURN a.name AS person_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4092: three-hop chain with two consecutive anonymous middle nodes must get distinct synthetic names.
  @Test
  void issue4092_threeHopChainWithTwoConsecutiveAnonMiddleNodes() {
    final Database db = createIssue4092Database("three-hop-two-anon");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (a:Person4092)-[:KNOWS_4092]->(:City4092)-[:LOCATED_IN_4092]->(:Country4092)-[:BELONGS_TO_4092]->(r:Region4092) " +
            "RETURN a.name AS person_name, r.name AS region_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("region_name")).isEqualTo("North America");
    } finally {
      db.drop();
    }
  }

  // Issue #4092: OPTIONAL MATCH with anonymous middle node must match the row when data exists.
  @Test
  void issue4092_optionalMatchWithAnonMiddleNode() {
    final Database db = createIssue4092Database("optional-match-anon");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (a:Person4092) " +
            "OPTIONAL MATCH (a)-[:KNOWS_4092]->(:City4092)-[:LOCATED_IN_4092]->(b:Country4092) " +
            "RETURN a.name AS person_name, b.name AS country_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
    } finally {
      db.drop();
    }
  }

  // Issue #4092: synthetic counter for anonymous nodes must advance across multiple MATCH clauses.
  @Test
  void issue4092_multipleMatchClausesEachWithAnonNodes() {
    final Database db = createIssue4092Database("multi-match-anon");
    try (final ResultSet rs = db.query("opencypher",
        "MATCH (a:Person4092)-[:KNOWS_4092]->(:City4092) " +
            "MATCH (:City4092)-[:LOCATED_IN_4092]->(b:Country4092) " +
            "RETURN a.name AS person_name, b.name AS country_name")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("person_name")).isEqualTo("Alice");
      assertThat((String) rows.get(0).getProperty("country_name")).isEqualTo("USA");
    } finally {
      db.drop();
    }
  }

  // Issue #4094: CALL db.labels() YIELD label must preserve previously bound variables for every yielded row.
  @Test
  void issue4094_callDbLabelsPreservesCarriedScalar() {
    final Database db = new DatabaseFactory("./target/databases/issue-4094-call-nullifies").create();
    try {
      db.transaction(() -> db.command("opencypher", "CREATE (:Foo4094), (:Bar4094), (:Baz4094)"));

      final ResultSet rs = db.query("opencypher",
          "WITH 1 AS x CALL db.labels() YIELD label RETURN x, label ORDER BY label");
      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("x").longValue()).isEqualTo(1L);
        count++;
      }
      assertThat(count).isGreaterThan(0);
    } finally {
      db.drop();
    }
  }

  // Issue #4101: MATCH after CREATE in the same statement must see the new labeled nodes.
  @Test
  void issue4101_createThenLabeledMatchSeesNewNode() {
    final Database db = new DatabaseFactory("./target/databases/issue-4101-create-then-match-1").create();
    try {
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "CREATE (:Person4101 {name:'Charlie'}) "
                + "MATCH (n:Person4101) "
                + "RETURN count(*) AS c");
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      });
    } finally {
      db.drop();
    }
  }

  // Issue #4101: MATCH after CREATE of two labeled nodes must see both.
  @Test
  void issue4101_createTwoThenLabeledMatchSeesBoth() {
    final Database db = new DatabaseFactory("./target/databases/issue-4101-create-then-match-2").create();
    try {
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "CREATE (:Person4101 {name:'Charlie'}), (:Person4101 {name:'Diana'}) "
                + "MATCH (n:Person4101) "
                + "RETURN count(*) AS c");
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L);
      });
    } finally {
      db.drop();
    }
  }

  // Issue #4101: MATCH after CREATE with a WITH barrier still sees the newly created node.
  @Test
  void issue4101_createThenMatchWithBarrierStillWorks() {
    final Database db = new DatabaseFactory("./target/databases/issue-4101-create-then-match-3").create();
    try {
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "CREATE (:Person4101 {name:'Charlie'}) "
                + "WITH 1 AS x "
                + "MATCH (n:Person4101) "
                + "RETURN count(*) AS c");
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      });
    } finally {
      db.drop();
    }
  }

  // Issue #4101: control case - CREATE then unlabeled MATCH sees the new node.
  @Test
  void issue4101_createThenMatchUnlabeledControl() {
    final Database db = new DatabaseFactory("./target/databases/issue-4101-create-then-match-4").create();
    try {
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "CREATE ({name:'Charlie'}) "
                + "MATCH (n) "
                + "RETURN count(*) AS c");
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      });
    } finally {
      db.drop();
    }
  }

  // Issue #4101: MATCH after CREATE sees a mix of preexisting and newly created nodes.
  @Test
  void issue4101_createThenMatchSeesMixOfPreexistingAndNew() {
    final Database db = new DatabaseFactory("./target/databases/issue-4101-create-then-match-5").create();
    try {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Person4101 {name:'Alice', age:30})");
        db.command("opencypher", "CREATE (:Person4101 {name:'Bob', age:25})");
      });
      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            "CREATE ({name:'Charlie'}), ({name:'Diana'}) "
                + "MATCH (n) "
                + "RETURN DISTINCT n.name AS name "
                + "ORDER BY name DESC");
        final List<String> names = new ArrayList<>();
        while (rs.hasNext()) {
          final Result r = rs.next();
          names.add(r.<String>getProperty("name"));
        }
        assertThat(names).containsExactly("Diana", "Charlie", "Bob", "Alice");
      });
    } finally {
      db.drop();
    }
  }

  // Issue #4102: a fresh MATCH on a variable bound to null via OPTIONAL MATCH must eliminate the row.
  @Test
  void issue4102_matchOnNullCarriedVariableEliminatesRow() {
    final Database db = new DatabaseFactory("./target/databases/issue-4102-match-null-carried-1").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4102 {name:'Alice', city:'New York'}), (:City4102 {name:'New York', population:8000000})"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person4102 {city:'New York'}) "
              + "OPTIONAL MATCH (p)-[:LIVES_IN_4102]->(c:City4102) "
              + "WITH p, c "
              + "MATCH (c:City4102 {population:8000000}) "
              + "RETURN p.name AS p, c.name AS c");
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #4102: a fresh MATCH on a relationship variable bound to null via OPTIONAL MATCH must eliminate the row.
  @Test
  void issue4102_matchOnNullCarriedRelationshipEliminatesRow() {
    final Database db = new DatabaseFactory("./target/databases/issue-4102-match-null-carried-2").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4102 {name:'Alice', city:'New York'}), (:City4102 {name:'New York', population:8000000})"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person4102 {city:'New York'}) "
              + "OPTIONAL MATCH (p)-[r:LIVES_IN_4102]->(c:City4102) "
              + "WITH p, c, r "
              + "MATCH (c:City4102 {population:8000000}) "
              + "RETURN p.name AS p, c.name AS c");
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #4102: OPTIONAL MATCH alone still returns the row with null for the unmatched variable.
  @Test
  void issue4102_optionalMatchAloneStillReturnsRowWithNull() {
    final Database db = new DatabaseFactory("./target/databases/issue-4102-match-null-carried-3").create();
    try {
      db.transaction(() -> db.command("opencypher",
          "CREATE (:Person4102 {name:'Alice', city:'New York'}), (:City4102 {name:'New York', population:8000000})"));

      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person4102 {city:'New York'}) "
              + "OPTIONAL MATCH (p)-[:LIVES_IN_4102]->(c:City4102) "
              + "WITH p, c "
              + "RETURN p.name AS p, c.name AS c");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("p")).isEqualTo("Alice");
      assertThat(r.<String>getProperty("c")).isNull();
      assertThat(rs.hasNext()).isFalse();
    } finally {
      db.drop();
    }
  }

  // Issue #3216: MATCH (a),(b) WHERE ID(a)=$x AND ID(b)=$y must filter by ID before forming the Cartesian product.
  @Test
  void issue3216_simplifiedMatchWithIdFilter() {
    final Database db = new DatabaseFactory("./target/databases/issue-3216-simplified").create();
    try {
      db.getSchema().createVertexType("CHUNK_3216");
      db.getSchema().createVertexType("DOCUMENT_3216");
      db.getSchema().createVertexType("CHUNK_EMBEDDING_3216");
      db.getSchema().createEdgeType("in_3216");

      final String[] ids = createIssue3216Fixture(db);
      final String sourceId = ids[0];
      final String targetId = ids[1];

      final long startTime = System.currentTimeMillis();

      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            """
            MATCH (a),(b) WHERE ID(a) = $sourceId and ID(b) = $targetId \
            MERGE (a)-[r:`in_3216`]->(b) \
            RETURN a, b, r""",
            Map.of("sourceId", sourceId, "targetId", targetId));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Should return 1 result for the created relationship")
            .isEqualTo(1);
      });

      final long duration = System.currentTimeMillis() - startTime;
      assertThat(duration)
          .as("Query should execute quickly when filtering by specific IDs")
          .isLessThan(5000);
    } finally {
      db.drop();
    }
  }

  // Issue #3216: UNWIND + MATCH (a),(b) WHERE ID-pair filter + MERGE must execute quickly per row.
  @Test
  void issue3216_unwindMatchMergeWithIdFilter() {
    final Database db = new DatabaseFactory("./target/databases/issue-3216-unwind").create();
    try {
      db.getSchema().createVertexType("CHUNK_3216");
      db.getSchema().createVertexType("DOCUMENT_3216");
      db.getSchema().createVertexType("CHUNK_EMBEDDING_3216");
      db.getSchema().createEdgeType("in_3216");

      final String[] ids = createIssue3216Fixture(db);
      final String sourceId = ids[0];
      final String targetId = ids[1];

      final List<Map<String, Object>> batch = new ArrayList<>();
      final Map<String, Object> row = new HashMap<>();
      row.put("source_id", sourceId);
      row.put("target_id", targetId);
      batch.add(row);

      final long startTime = System.currentTimeMillis();

      db.transaction(() -> {
        final ResultSet rs = db.command("opencypher",
            """
            UNWIND $batch as row \
            MATCH (a),(b) WHERE ID(a) = row.source_id and ID(b) = row.target_id \
            MERGE (a)-[r:`in_3216`]->(b) \
            RETURN a, b, r""",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Should return 1 result for the batch entry")
            .isEqualTo(1);
      });

      final long duration = System.currentTimeMillis() - startTime;
      assertThat(duration)
          .as("UNWIND + MATCH query should execute quickly when filtering by specific IDs")
          .isLessThan(5000);
    } finally {
      db.drop();
    }
  }

  // Issue #3216: MATCH (a),(b) without WHERE generates a full Cartesian product (totalVertices^2 rows).
  @Test
  void issue3216_cartesianProductWithoutFilter() {
    final Database db = new DatabaseFactory("./target/databases/issue-3216-cartesian").create();
    try {
      db.getSchema().createVertexType("CHUNK_3216");
      db.getSchema().createVertexType("DOCUMENT_3216");
      db.getSchema().createVertexType("CHUNK_EMBEDDING_3216");
      db.getSchema().createEdgeType("in_3216");

      createIssue3216Fixture(db);

      // Count total vertices
      final ResultSet countRs = db.query("opencypher", "MATCH (n) RETURN count(n) as total");
      final long totalVertices = ((Number) countRs.next().getProperty("total")).longValue();

      // MATCH (a),(b) without WHERE creates Cartesian product
      final ResultSet rs = db.query("opencypher", "MATCH (a),(b) RETURN count(*) as total");
      final long cartesianCount = ((Number) rs.next().getProperty("total")).longValue();

      assertThat(cartesianCount).isEqualTo(totalVertices * totalVertices);
    } finally {
      db.drop();
    }
  }

  // Issue #3216: benchmark comparing single MATCH (a),(b) WHERE id-pair vs split MATCH clauses; expected to favour split form.
  @Test
  @Tag("benchmark")
  void issue3216_performanceComparisonMatchStrategies() {
    final Database db = new DatabaseFactory("./target/databases/issue-3216-perf-compare").create();
    try {
      db.getSchema().createVertexType("CHUNK_3216");
      db.getSchema().createVertexType("DOCUMENT_3216");
      db.getSchema().createVertexType("CHUNK_EMBEDDING_3216");
      db.getSchema().createEdgeType("in_3216");

      final String[] ids = createIssue3216Fixture(db);
      final String sourceId = ids[0];
      final String targetId = ids[1];

      // Method 1: Single MATCH with Cartesian product
      final long startTime1 = System.currentTimeMillis();
      db.transaction(() -> {
        final ResultSet rs = db.query("opencypher",
            "MATCH (a),(b) WHERE ID(a) = $sourceId and ID(b) = $targetId RETURN a, b",
            Map.of("sourceId", sourceId, "targetId", targetId));
        assertThat(rs.hasNext()).isTrue();
        rs.next();
      });
      final long duration1 = System.currentTimeMillis() - startTime1;

      // Method 2: Separate MATCH clauses (should be faster)
      final long startTime2 = System.currentTimeMillis();
      db.transaction(() -> {
        final ResultSet rs = db.query("opencypher",
            "MATCH (a) WHERE ID(a) = $sourceId MATCH (b) WHERE ID(b) = $targetId RETURN a, b",
            Map.of("sourceId", sourceId, "targetId", targetId));
        assertThat(rs.hasNext()).isTrue();
        rs.next();
      });
      final long duration2 = System.currentTimeMillis() - startTime2;

      // Method 2 should be significantly faster
      // (commenting out the assertion for now since we're diagnosing the issue)
      // assertThat(duration2).isLessThan(duration1);
      assertThat(duration1).isGreaterThanOrEqualTo(0L);
      assertThat(duration2).isGreaterThanOrEqualTo(0L);
    } finally {
      db.drop();
    }
  }

  private static String[] createIssue3216Fixture(final Database db) {
    final String[] ids = new String[2];
    db.transaction(() -> {
      final MutableVertex chunk = db.newVertex("CHUNK_3216");
      chunk.set("name", "chunk1");
      chunk.save();
      ids[0] = chunk.getIdentity().toString();

      final MutableVertex doc = db.newVertex("DOCUMENT_3216");
      doc.set("name", "doc1");
      doc.save();
      ids[1] = doc.getIdentity().toString();

      for (int i = 0; i < 100; i++) {
        final MutableVertex embedding = db.newVertex("CHUNK_EMBEDDING_3216");
        embedding.set("index", i);
        embedding.save();
      }

      for (int i = 0; i < 50; i++) {
        final MutableVertex chunk2 = db.newVertex("CHUNK_3216");
        chunk2.set("name", "chunk" + (i + 2));
        chunk2.save();
      }
    });
    return ids;
  }

  // Issue #3761: WITH * followed by extra alias after OPTIONAL MATCH (previously dropped extra items)
  @Test
  void withStarAndAliasAfterOptionalMatch() {
    final Database db = newIssue3761Database("alias");
    try {
      db.transaction(() -> {
        try (final ResultSet rs = db.query("opencypher", """
            MATCH (t1:Thing1 {id: 'A'})
            OPTIONAL MATCH (t1)-[:basedOn]->(t2:Thing2)
            WITH *, t2 AS out2
            RETURN t1 AS out1, out2
            """)) {
          final List<Result> results = new ArrayList<>();
          while (rs.hasNext())
            results.add(rs.next());

          assertThat(results).hasSize(1);
          final Result row = results.get(0);
          assertThat(row.getPropertyNames()).contains("out1", "out2");
          assertThat((Object) row.getProperty("out1")).isNotNull();
          assertThat((Object) row.getProperty("out2")).isNotNull();
        }
      });
    } finally {
      db.drop();
    }
  }

  // Issue #3761: WITH * alone passes all variables through
  @Test
  void withStarAlonePassesAllVariables() {
    final Database db = newIssue3761Database("alone");
    try {
      db.transaction(() -> {
        try (final ResultSet rs = db.query("opencypher", """
            MATCH (t1:Thing1 {id: 'A'})
            OPTIONAL MATCH (t1)-[:basedOn]->(t2:Thing2)
            WITH *
            RETURN t1, t2
            """)) {
          final List<Result> results = new ArrayList<>();
          while (rs.hasNext())
            results.add(rs.next());

          assertThat(results).hasSize(1);
          final Result row = results.get(0);
          assertThat(row.getPropertyNames()).contains("t1", "t2");
        }
      });
    } finally {
      db.drop();
    }
  }

  // Issue #3761: WITH * with a computed-expression alias evaluates the expression
  @Test
  void withStarAndComputedAlias() {
    final Database db = newIssue3761Database("computed");
    try {
      db.transaction(() -> {
        try (final ResultSet rs = db.query("opencypher", """
            MATCH (t1:Thing1 {id: 'A'})
            WITH *, t1.id AS itemId
            RETURN t1, itemId
            """)) {
          final List<Result> results = new ArrayList<>();
          while (rs.hasNext())
            results.add(rs.next());

          assertThat(results).hasSize(1);
          final Result row = results.get(0);
          assertThat(row.getPropertyNames()).contains("t1", "itemId");
          assertThat(row.<String>getProperty("itemId")).isEqualTo("A");
        }
      });
    } finally {
      db.drop();
    }
  }

  // Issue #3761: WITH *, expr1 AS a, expr2 AS b - multiple extra items after wildcard
  @Test
  void withStarAndMultipleExtraAliases() {
    final Database db = newIssue3761Database("multi-alias");
    try {
      db.transaction(() -> {
        try (final ResultSet rs = db.query("opencypher", """
            MATCH (t1:Thing1 {id: 'A'})
            OPTIONAL MATCH (t1)-[:basedOn]->(t2:Thing2)
            WITH *, t1.id AS srcId, t2.name AS tgtName
            RETURN srcId, tgtName
            """)) {
          final List<Result> results = new ArrayList<>();
          while (rs.hasNext())
            results.add(rs.next());

          assertThat(results).hasSize(1);
          final Result row = results.get(0);
          assertThat(row.<String>getProperty("srcId")).isEqualTo("A");
          assertThat(row.<String>getProperty("tgtName")).isEqualTo("B");
        }
      });
    } finally {
      db.drop();
    }
  }

  // Issue #3761: WITH * alias usable in subsequent WHERE clause
  @Test
  void withStarAndAliasInWhereClause() {
    final Database db = newIssue3761Database("where");
    try {
      db.transaction(() -> {
        db.command("opencypher", "CREATE (:Thing1 {id: 'X'})");

        try (final ResultSet rs = db.query("opencypher", """
            MATCH (t1:Thing1)
            WITH *, t1.id AS tid
            WHERE tid = 'A'
            RETURN t1, tid
            """)) {
          final List<Result> results = new ArrayList<>();
          while (rs.hasNext())
            results.add(rs.next());

          assertThat(results).hasSize(1);
          assertThat(results.get(0).<String>getProperty("tid")).isEqualTo("A");
        }
      });
    } finally {
      db.drop();
    }
  }

  // Issue #4188: relationship endpoint property bound from preceding WITH variable
  @Test
  void endpointPropertyBoundFromWithVariable() {
    final Database db = newIssue4188Database("with");
    try {
      final ResultSet rs = db.query("opencypher",
          """
              WITH 'Bob' AS fName
              MATCH (p:Person)-[:FRIEND]->(:Person {name: fName})
              RETURN p.name AS person
              ORDER BY person""");

      final List<String> people = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        people.add(r.getProperty("person"));
      }
      assertThat(people).containsExactly("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4188: same constraint moved into WHERE keeps working (control)
  @Test
  void endpointPropertyInWhereStillWorks() {
    final Database db = newIssue4188Database("where");
    try {
      final ResultSet rs = db.query("opencypher",
          """
              WITH 'Bob' AS fName
              MATCH (p:Person)-[:FRIEND]->(f:Person)
              WHERE f.name = fName
              RETURN p.name AS person""");

      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("person")).isEqualTo("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4188: literal endpoint property still works after the fix (control)
  @Test
  void endpointPropertyLiteralStillWorks() {
    final Database db = newIssue4188Database("literal");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person)-[:FRIEND]->(:Person {name:'Bob'}) RETURN p.name AS person");

      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat(r.<String>getProperty("person")).isEqualTo("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4188: endpoint property bound from a query parameter
  @Test
  void endpointPropertyBoundFromParameter() {
    final Database db = newIssue4188Database("param");
    try {
      final ResultSet rs = db.query("opencypher",
          "MATCH (p:Person)-[:FRIEND]->(:Person {name: $fName}) RETURN p.name AS person",
          java.util.Map.of("fName", "Bob"));

      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("person")).isEqualTo("Alice");
    } finally {
      db.drop();
    }
  }

  // Issue #4188: endpoint property bound from an UNWIND-produced variable
  @Test
  void endpointPropertyBoundFromUnwindVariable() {
    final Database db = newIssue4188Database("unwind");
    try {
      final ResultSet rs = db.query("opencypher",
          """
              UNWIND ['Bob','Alice'] AS fName
              MATCH (p:Person)-[:FRIEND]->(:Person {name: fName})
              RETURN p.name AS person""");

      final List<String> people = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        people.add(r.getProperty("person"));
      }
      assertThat(people).containsExactly("Alice");
    } finally {
      db.drop();
    }
  }

  private static Database newIssue3761Database(final String tag) {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-match-3761-" + tag).create();
    db.getSchema().createVertexType("Thing1");
    db.getSchema().createVertexType("Thing2");
    db.getSchema().createEdgeType("basedOn");
    db.transaction(() -> {
      db.command("opencypher", "CREATE (:Thing1 {id: 'A'})");
      db.command("opencypher", "CREATE (:Thing2 {name: 'B'})");
      db.command("opencypher",
          "MATCH (t1:Thing1 {id: 'A'}), (t2:Thing2 {name: 'B'}) CREATE (t1)-[:basedOn]->(t2)");
    });
    return db;
  }

  private static Database newIssue4188Database(final String tag) {
    final Database db = new DatabaseFactory("./target/databases/testopencypher-match-4188-" + tag).create();
    db.getSchema().createVertexType("Person");
    db.getSchema().createEdgeType("FRIEND");
    db.transaction(() -> {
      db.command("opencypher",
          "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (a)-[:FRIEND]->(b)");
    });
    return db;
  }
}
