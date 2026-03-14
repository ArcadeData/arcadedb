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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for collect(DISTINCT ...) functionality.
 * This ensures that DISTINCT is properly handled in the collect() aggregation function.
 */
class CollectDistinctTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/collect-distinct-test").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("LIVES_IN");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void collectDistinctStrings() {
    // Create people with duplicate cities
    database.transaction(() -> {
      MutableVertex alice = database.newVertex("Person");
      alice.set("name", "Alice");
      alice.set("city", "New York");
      alice.save();

      MutableVertex bob = database.newVertex("Person");
      bob.set("name", "Bob");
      bob.set("city", "New York"); // Same city as Alice
      bob.save();

      MutableVertex charlie = database.newVertex("Person");
      charlie.set("name", "Charlie");
      charlie.set("city", "London");
      charlie.save();

      MutableVertex david = database.newVertex("Person");
      david.set("name", "David");
      david.set("city", "London"); // Same city as Charlie
      david.save();
    });

    // Test collect without DISTINCT - should return 4 cities (with duplicates)
    ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN collect(p.city) as cities");
    assertThat(rs.hasNext()).isTrue();
    Result result = rs.next();
    @SuppressWarnings("unchecked")
    List<String> allCities = (List<String>) result.getProperty("cities");
    assertThat(allCities).hasSize(4);

    // Test collect with DISTINCT - should return only 2 unique cities
    rs = database.query("opencypher", "MATCH (p:Person) RETURN collect(DISTINCT p.city) as uniqueCities");
    assertThat(rs.hasNext()).isTrue();
    result = rs.next();
    @SuppressWarnings("unchecked")
    List<String> uniqueCities = (List<String>) result.getProperty("uniqueCities");
    assertThat(uniqueCities).hasSize(2);
    assertThat(uniqueCities).containsExactlyInAnyOrder("New York", "London");
  }

  @Test
  void collectDistinctVertices() {
    // Create a graph where the same vertex is matched multiple times
    database.transaction(() -> {
      MutableVertex nyc = database.newVertex("City");
      nyc.set("name", "New York");
      nyc.save();

      MutableVertex london = database.newVertex("City");
      london.set("name", "London");
      london.save();

      // Multiple people living in the same city
      for (int i = 0; i < 5; i++) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person" + i);
        person.save();
        person.newEdge("LIVES_IN", nyc, true, (Object[]) null);
      }

      for (int i = 5; i < 8; i++) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person" + i);
        person.save();
        person.newEdge("LIVES_IN", london, true, (Object[]) null);
      }
    });

    // Without DISTINCT: should return 8 cities (one per person)
    ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN collect(c) as cities");
    assertThat(rs.hasNext()).isTrue();
    Result result = rs.next();
    @SuppressWarnings("unchecked")
    List<?> allCities = (List<?>) result.getProperty("cities");
    assertThat(allCities).hasSize(8);

    // With DISTINCT: should return only 2 unique cities
    rs = database.query("opencypher",
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN collect(DISTINCT c) as uniqueCities");
    assertThat(rs.hasNext()).isTrue();
    result = rs.next();
    @SuppressWarnings("unchecked")
    List<?> uniqueCities = (List<?>) result.getProperty("uniqueCities");
    assertThat(uniqueCities).hasSize(2);
  }

  @Test
  void collectDistinctWithGroupBy() {
    // Create cities with multiple people having various ages
    database.transaction(() -> {
      MutableVertex nyc = database.newVertex("City");
      nyc.set("name", "New York");
      nyc.save();

      MutableVertex london = database.newVertex("City");
      london.set("name", "London");
      london.save();

      // NYC: people aged 25, 25, 30 (should collect distinct: 25, 30)
      for (int age : new int[]{25, 25, 30}) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person_NYC_" + age);
        person.set("age", age);
        person.save();
        person.newEdge("LIVES_IN", nyc, true, (Object[]) null);
      }

      // London: people aged 20, 20, 20, 35 (should collect distinct: 20, 35)
      for (int age : new int[]{20, 20, 20, 35}) {
        MutableVertex person = database.newVertex("Person");
        person.set("name", "Person_London_" + age);
        person.set("age", age);
        person.save();
        person.newEdge("LIVES_IN", london, true, (Object[]) null);
      }
    });

    // Group by city and collect distinct ages
    ResultSet rs = database.query("opencypher",
        """
        MATCH (p:Person)-[:LIVES_IN]->(c:City) \
        RETURN c.name as city, collect(DISTINCT p.age) as ages""");

    // Collect all results into a map for order-independent checking
    Map<String, List<Integer>> results = new HashMap<>();
    while (rs.hasNext()) {
      Result row = rs.next();
      String city = (String) row.getProperty("city");
      @SuppressWarnings("unchecked")
      List<Integer> ages = (List<Integer>) row.getProperty("ages");
      results.put(city, ages);
    }

    assertThat(results).hasSize(2);

    // Check London
    assertThat(results.containsKey("London")).isTrue();
    List<Integer> londonAges = results.get("London");
    assertThat(londonAges).hasSize(2);
    assertThat(londonAges).containsExactlyInAnyOrder(20, 35);

    // Check New York
    assertThat(results.containsKey("New York")).isTrue();
    List<Integer> nycAges = results.get("New York");
    assertThat(nycAges).hasSize(2);
    assertThat(nycAges).containsExactlyInAnyOrder(25, 30);
  }

  @Test
  void collectDistinctWithNulls() {
    // Create people with some null cities
    database.transaction(() -> {
      MutableVertex alice = database.newVertex("Person");
      alice.set("name", "Alice");
      alice.set("city", "New York");
      alice.save();

      MutableVertex bob = database.newVertex("Person");
      bob.set("name", "Bob");
      // No city set - null
      bob.save();

      MutableVertex charlie = database.newVertex("Person");
      charlie.set("name", "Charlie");
      charlie.set("city", "New York");
      charlie.save();

      MutableVertex david = database.newVertex("Person");
      david.set("name", "David");
      // No city set - null
      david.save();
    });

    // DISTINCT should exclude nulls and return only non-null unique values
    ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) RETURN collect(DISTINCT p.city) as cities");
    assertThat(rs.hasNext()).isTrue();
    Result result = rs.next();
    @SuppressWarnings("unchecked")
    List<String> cities = (List<String>) result.getProperty("cities");
    // Should only contain "New York" (nulls excluded, duplicate excluded)
    assertThat(cities).hasSize(1);
    assertThat(cities).containsExactly("New York");
  }

  /** See issue #3316 */
  @Nested
  class ReturnDistinctDuplicatesRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-3316");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Animal");

      database.transaction(() -> {
        database.newVertex("Person").set("name", "Alice").save();
        database.newVertex("Person").set("name", "Bob").save();
        database.newVertex("Person").set("name", "Charlie").save();
        database.newVertex("Animal").set("name", "Dog").save();
        database.newVertex("Animal").set("name", "Cat").save();
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
    void returnDistinctLabels() {
      // Original issue: MATCH (n) RETURN DISTINCT labels(n) returns as many labels as records
      final ResultSet result = database.query("opencypher", "MATCH (n) RETURN DISTINCT labels(n) AS lab");
      final List<Object> labels = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        labels.add(row.getProperty("lab"));
      }
      // Should only return 2 distinct label sets: [Person] and [Animal]
      assertThat(labels).hasSize(2);
    }

    @Test
    void countDistinctWithUnwind() {
      // From comment: UNWIND [1, 1, 2, 3] AS n RETURN count(DISTINCT n) AS unique_count
      // Should return 3, was returning 4
      final ResultSet result = database.query("opencypher",
          "UNWIND [1, 1, 2, 3] AS n RETURN count(DISTINCT n) AS unique_count");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Object count = row.getProperty("unique_count");
      assertThat(((Number) count).longValue()).isEqualTo(3L);
      assertThat(result.hasNext()).isFalse();
    }

    @Test
    void returnDistinctSimpleValues() {
      // UNWIND with duplicates, RETURN DISTINCT should deduplicate
      final ResultSet result = database.query("opencypher",
          "UNWIND [1, 2, 2, 3, 3, 3] AS n RETURN DISTINCT n");

      final List<Object> values = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        values.add(row.getProperty("n"));
      }
      assertThat(values).hasSize(3);
    }

    @Test
    void returnDistinctWithMatchProperties() {
      // RETURN DISTINCT on vertex properties
      // Add duplicate name to test deduplication
      database.transaction(() -> {
        database.newVertex("Person").set("name", "Alice").save();
      });

      final ResultSet result = database.query("opencypher",
          "MATCH (n:Person) RETURN DISTINCT n.name AS name");

      final List<String> names = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        names.add(row.getProperty("name"));
      }
      // Should be 3 distinct names: Alice, Bob, Charlie (not 4)
      assertThat(names).hasSize(3);
    }

    @Test
    void sumDistinct() {
      // sum(DISTINCT n) should sum only unique values
      final ResultSet result = database.query("opencypher",
          "UNWIND [1, 1, 2, 3] AS n RETURN sum(DISTINCT n) AS total");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Object total = row.getProperty("total");
      assertThat(((Number) total).longValue()).isEqualTo(6L);
      assertThat(result.hasNext()).isFalse();
    }
  }

  /** See issue #3404 */
  @Nested
  class CollectRelationshipsRegression {
    private Database database;
    private static final String DB_PATH = "./target/databases/test-issue3404";

    @BeforeEach
    void setUp() {
      // Delete any existing database
      new File(DB_PATH).mkdirs();
      database = new DatabaseFactory(DB_PATH).create();

      // Create schema matching the issue scenario
      database.getSchema().createVertexType("DOCUMENT");
      database.getSchema().createVertexType("CHUNK");
      database.getSchema().createEdgeType("in");

      // Create test data
      database.transaction(() -> {
        // Create 4 DOCUMENT nodes
        for (int i = 0; i < 4; i++) {
          database.command("opencypher", "CREATE (d:DOCUMENT {id: " + i + "})");
        }

        // Create multiple CHUNK nodes and connect them to DOCUMENTs
        // Document 0: 2 chunks
        database.command("opencypher", "CREATE (c:CHUNK {id: 0})");
        database.command("opencypher", "CREATE (c:CHUNK {id: 33})");
        database.command("opencypher",
            "MATCH (d:DOCUMENT {id: 0}), (c:CHUNK {id: 0}) CREATE (c)-[:in]->(d)");
        database.command("opencypher",
            "MATCH (d:DOCUMENT {id: 0}), (c:CHUNK {id: 33}) CREATE (c)-[:in]->(d)");

        // Document 1: 44 chunks
        for (int i = 1; i <= 44; i++) {
          database.command("opencypher", "CREATE (c:CHUNK {id: " + (100 + i) + "})");
          database.command("opencypher",
              "MATCH (d:DOCUMENT {id: 1}), (c:CHUNK {id: " + (100 + i) + "}) CREATE (c)-[:in]->(d)");
        }

        // Document 2: 11 chunks
        for (int i = 1; i <= 11; i++) {
          database.command("opencypher", "CREATE (c:CHUNK {id: " + (200 + i) + "})");
          database.command("opencypher",
              "MATCH (d:DOCUMENT {id: 2}), (c:CHUNK {id: " + (200 + i) + "}) CREATE (c)-[:in]->(d)");
        }

        // Document 3: 2 chunks
        database.command("opencypher", "CREATE (c:CHUNK {id: 6})");
        database.command("opencypher", "CREATE (c:CHUNK {id: 7})");
        database.command("opencypher",
            "MATCH (d:DOCUMENT {id: 3}), (c:CHUNK {id: 6}) CREATE (c)-[:in]->(d)");
        database.command("opencypher",
            "MATCH (d:DOCUMENT {id: 3}), (c:CHUNK {id: 7}) CREATE (c)-[:in]->(d)");
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
    void collectRelationshipsReturnsListNotCount() {
      // Query that collects relationships
      final String query = """
                           MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                           RETURN ID(nodeDOc), COLLECT(ID(chunk)), COLLECT(rel)""";

      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();

          final Object collectedChunks = row.getProperty("COLLECT(ID(chunk))");
          final Object collectedRels = row.getProperty("COLLECT(rel)");

          // COLLECT(rel) should return a List, not a Number
          assertThat(collectedRels)
              .as("COLLECT(rel) should return a List, not a count")
              .isInstanceOf(List.class);

          // The list should contain Edge objects
          final List<?> relList = (List<?>) collectedRels;
          assertThat(relList).isNotEmpty();

          for (final Object item : relList) {
            assertThat(item)
                .as("Each item in COLLECT(rel) should be an Edge object")
                .isInstanceOf(Edge.class);
          }

          // The size of collected chunks and edges should match
          final List<?> chunkList = (List<?>) collectedChunks;
          assertThat(relList.size())
              .as("Number of collected relationships should match number of collected chunks")
              .isEqualTo(chunkList.size());
        }
      }
    }

    @Test
    void collectRelationshipsWithIdStillWorks() {
      // Verify that the workaround (wrapping in ID()) still works
      final String query = """
                           MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                           RETURN ID(nodeDOc), COLLECT(ID(rel))""";

      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();

        while (rs.hasNext()) {
          final Result row = rs.next();
          final Object collectedIds = row.getProperty("COLLECT(ID(rel))");

          // COLLECT(ID(rel)) should return a List
          assertThat(collectedIds).isInstanceOf(List.class);

          final List<?> idList = (List<?>) collectedIds;
          assertThat(idList).isNotEmpty();

          // Each item should be a RID (string representation)
          for (final Object item : idList) {
            assertThat(item.toString()).matches("#\\d+:\\d+");
          }
        }
      }
    }

    @Test
    void collectWithNoGrouping() {
      // Test COLLECT without grouping (should collect all relationships into one list)
      final String query = "MATCH ()<-[rel:in]-() RETURN COLLECT(rel) AS allRels";

      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();

        final Result row = rs.next();
        final Object collectedRels = row.getProperty("allRels");

        // Should be a List
        assertThat(collectedRels).isInstanceOf(List.class);

        final List<?> relList = (List<?>) collectedRels;

        // We created 2 + 44 + 11 + 2 = 59 edges total
        assertThat(relList).hasSize(59);

        // All items should be Edge objects
        for (final Object item : relList) {
          assertThat(item).isInstanceOf(Edge.class);
        }
      }
    }

    @Test
    void collectNodesStillWorks() {
      // Verify that COLLECT still works correctly for nodes
      final String query = """
                           MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                           RETURN ID(nodeDOc), COLLECT(chunk) AS chunks""";

      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();

        while (rs.hasNext()) {
          final Result row = rs.next();
          final Object collectedChunks = row.getProperty("chunks");

          // Should be a List
          assertThat(collectedChunks).isInstanceOf(List.class);

          final List<?> chunkList = (List<?>) collectedChunks;
          assertThat(chunkList).isNotEmpty();
        }
      }
    }

    @Test
    void jsonSerializationForStudio() {
      // Test that JSON serialization (as used by Studio) correctly serializes COLLECT(rel) as a list, not a count
      final JsonGraphSerializer serializer = JsonGraphSerializer.createJsonGraphSerializer()
          .setExpandVertexEdges(false);
      serializer.setUseCollectionSize(false).setUseCollectionSizeForEdges(false);

      final String query = """
                           MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) \
                           RETURN ID(nodeDOc) AS docId, COLLECT(rel) AS rels""";

      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();

        while (rs.hasNext()) {
          final Result row = rs.next();
          final JSONObject json = serializer.serializeResult(database, row);

          // Check that "rels" is serialized as a JSONArray, not as a number
          final Object relsValue = json.get("rels");
          assertThat(relsValue)
              .as("COLLECT(rel) should be serialized as JSONArray, not as a count")
              .isInstanceOf(JSONArray.class);

          final JSONArray relsArray = (JSONArray) relsValue;
          assertThat(relsArray.length())
              .as("Collected relationships array should not be empty")
              .isGreaterThan(0);

          // Each item in the array should be a JSONObject representing an edge
          for (int i = 0; i < relsArray.length(); i++) {
            final Object item = relsArray.get(i);
            assertThat(item)
                .as("Each item in COLLECT(rel) should be serialized as JSONObject (edge)")
                .isInstanceOf(JSONObject.class);

            final JSONObject edgeJson = (JSONObject) item;
            assertThat(edgeJson.has("@rid"))
                .as("Serialized edge should have @rid")
                .isTrue();
            assertThat(edgeJson.has("@in"))
                .as("Serialized edge should have @in")
                .isTrue();
            assertThat(edgeJson.has("@out"))
                .as("Serialized edge should have @out")
                .isTrue();
          }
        }
      }
    }
  }

  /** See issue #3271 */
  @Nested
  class CollectDistinctMapLiteralRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue3271-test").create();

      // Create types matching the issue's data model:
      // NER nodes with subtype (DATE, PERSON, ORG, etc.)
      // CHUNK nodes containing text
      // DOCUMENT nodes
      // Edges between NER nodes and from NER to CHUNK to DOCUMENT
      database.getSchema().createVertexType("NER");
      database.getSchema().createVertexType("CHUNK");
      database.getSchema().createVertexType("DOCUMENT");
      database.getSchema().createEdgeType("RELATED_TO");
      database.getSchema().createEdgeType("IN_CHUNK");
      database.getSchema().createEdgeType("FROM_DOC");

      // Create test data
      database.transaction(() -> {
        database.command("opencypher",
            // Create documents
            "CREATE (doc1:DOCUMENT {name: 'Document1'}), " +
                "(doc2:DOCUMENT {name: 'Document2'}), " +
                // Create chunks linked to documents
                "(chunk1:CHUNK {text: 'This is chunk 1 text'}), " +
                "(chunk2:CHUNK {text: 'This is chunk 2 text'}), " +
                "(chunk1)-[:FROM_DOC]->(doc1), " +
                "(chunk2)-[:FROM_DOC]->(doc2), " +
                // Create NER nodes with subtype DATE
                "(date1:NER {subtype: 'DATE', name: '2024-01-15'}), " +
                "(date2:NER {subtype: 'DATE', name: '2024-02-20'}), " +
                // Create NER nodes with other subtypes
                "(person1:NER {subtype: 'PERSON', name: 'John Doe'}), " +
                "(person2:NER {subtype: 'PERSON', name: 'Jane Smith'}), " +
                "(org1:NER {subtype: 'ORG', name: 'ACME Corp'}), " +
                // Create relationships between NER nodes
                "(date1)-[:RELATED_TO]->(person1), " +
                "(date1)-[:RELATED_TO]->(org1), " +
                "(date2)-[:RELATED_TO]->(person2), " +
                // Link NER nodes to chunks
                "(date1)-[:IN_CHUNK]->(chunk1), " +
                "(date2)-[:IN_CHUNK]->(chunk2), " +
                "(person1)-[:IN_CHUNK]->(chunk1), " +
                "(person2)-[:IN_CHUNK]->(chunk2), " +
                "(org1)-[:IN_CHUNK]->(chunk1)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    /**
     * Test the simplified query that was reported as working in the issue.
     * This serves as a baseline to confirm the basic multi-hop MATCH works.
     */
    @Test
    void simplifiedMultiMatchQuery() {
      // Simplified query from issue - this should work
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[r:RELATED_TO]-(m:NER) \
          MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
          RETURN n.name AS name""");

      List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        names.add((String) result.getProperty("name"));
      }

      // date1 has 2 relations (person1, org1), date2 has 1 relation (person2)
      // So we should get date1 twice and date2 once = 3 results
      assertThat(names).hasSize(3);
      assertThat(names).contains("2024-01-15", "2024-02-20");
    }

    /**
     * Test COLLECT with map literals containing property accesses.
     * This is a building block for the full issue query.
     */
    @Test
    void collectWithMapLiteral() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[r:RELATED_TO]-(m:NER) \
          RETURN n.name AS name, \
          COLLECT({nameT: m.name, typeT: m.subtype}) AS relations""");

      List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }

      // Should have 2 rows: one for each DATE node
      assertThat(results).hasSize(2);

      for (Result row : results) {
        String name = (String) row.getProperty("name");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
        assertThat(relations).isNotNull();

        if ("2024-01-15".equals(name)) {
          // date1 has relations to person1 and org1
          assertThat(relations).hasSize(2);
        } else if ("2024-02-20".equals(name)) {
          // date2 has relation to person2
          assertThat(relations).hasSize(1);
        }
      }
    }

    /**
     * Test COLLECT(DISTINCT {...}) with map literals.
     * The DISTINCT modifier should work with map literals.
     */
    @Test
    void collectDistinctWithMapLiteral() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[r:RELATED_TO]-(m:NER) \
          RETURN n.name AS name, \
          COLLECT(DISTINCT {nameT: m.name, typeT: m.subtype}) AS relations""");

      List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }

      assertThat(results).hasSize(2);

      for (Result row : results) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
        assertThat(relations).isNotNull();

        // Verify the maps have the expected structure
        for (Map<String, Object> relation : relations) {
          assertThat(relation).containsKeys("nameT", "typeT");
        }
      }
    }

    /**
     * Test type() function inside map literals.
     * This tests the ability to use functions like type(r) in map expressions.
     */
    @Test
    void typeInMapLiteral() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[r:RELATED_TO]-(m:NER) \
          RETURN n.name AS name, \
          COLLECT({typeR: type(r), nameT: m.name}) AS relations""");

      List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }

      assertThat(results).hasSize(2);

      for (Result row : results) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
        assertThat(relations).isNotNull();

        // Verify type(r) is correctly resolved
        for (Map<String, Object> relation : relations) {
          assertThat(relation.get("typeR")).isEqualTo("RELATED_TO");
          assertThat(relation.get("nameT")).isNotNull();
        }
      }
    }

    /**
     * Test HEAD(COLLECT(...)) combination.
     * HEAD should return the first element from a collected list.
     */
    @Test
    void headCollect() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
          RETURN n.name AS name, \
          HEAD(COLLECT({text: chunk.text, doc_name: document.name})) AS context""");

      List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }

      assertThat(results).hasSize(2);

      for (Result row : results) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) row.getProperty("context");
        assertThat(context).isNotNull();
        assertThat(context).containsKeys("text", "doc_name");
      }
    }

    /**
     * Test HEAD(COLLECT(DISTINCT {...})) - combining HEAD and COLLECT DISTINCT with maps.
     */
    @Test
    void headCollectDistinctWithMapLiteral() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
          RETURN n.name AS name, \
          HEAD(COLLECT(DISTINCT {text: chunk.text, doc_name: document.name})) AS context""");

      List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }

      assertThat(results).hasSize(2);

      for (Result row : results) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) row.getProperty("context");
        assertThat(context).isNotNull();
        assertThat(context).containsKeys("text", "doc_name");
      }
    }

    /**
     * Test ID() function inside map literals.
     */
    @Test
    void idInMapLiteral() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
          RETURN n.name AS name, \
          COLLECT({doc_name: document.name, doc_id: ID(document)}) AS contexts""");

      List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }

      assertThat(results).hasSize(2);

      for (Result row : results) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contexts = (List<Map<String, Object>>) row.getProperty("contexts");
        assertThat(contexts).isNotNull();

        for (Map<String, Object> context : contexts) {
          assertThat(context.get("doc_name")).isNotNull();
          assertThat(context.get("doc_id")).isNotNull();
        }
      }
    }

    /**
     * The full query from issue #3271.
     * This combines multiple MATCH patterns, COLLECT(DISTINCT {...}) with type(),
     * and HEAD(COLLECT(DISTINCT {...})) with ID().
     */
    @Test
    void fullIssue3271Query() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[r:RELATED_TO]-(m:NER) \
          MATCH (n)-[:IN_CHUNK]-(chunk:CHUNK)-[:FROM_DOC]->(document:DOCUMENT) \
          RETURN n.name AS name, \
          COLLECT(DISTINCT {typeO: n.subtype, nameO: n.name, typeR: type(r), typeT: m.subtype, nameT: m.name}) AS relations, \
          HEAD(COLLECT(DISTINCT {text: chunk.text, doc_name: document.name, doc_id: ID(document)})) AS context""");

      List<Result> results = new ArrayList<>();
      while (rs.hasNext()) {
        results.add(rs.next());
      }

      // We expect results - the new engine was returning 0 results
      assertThat(results).isNotEmpty();
      assertThat(results).hasSize(2);

      for (Result row : results) {
        String name = (String) row.getProperty("name");
        assertThat(name).isNotNull();
        assertThat(name).isIn("2024-01-15", "2024-02-20");

        // Check relations
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> relations = (List<Map<String, Object>>) row.getProperty("relations");
        assertThat(relations).isNotNull();
        assertThat(relations).isNotEmpty();

        for (Map<String, Object> relation : relations) {
          assertThat(relation).containsKeys("typeO", "nameO", "typeR", "typeT", "nameT");
          assertThat(relation.get("typeO")).isEqualTo("DATE");
          assertThat(relation.get("typeR")).isEqualTo("RELATED_TO");
        }

        // Check context
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) row.getProperty("context");
        assertThat(context).isNotNull();
        assertThat(context).containsKeys("text", "doc_name", "doc_id");
      }
    }

    /**
     * Test with undirected edge pattern as in the original issue query.
     * The original query used (n)-[]-(chunk:CHUNK)-->(document:DOCUMENT)
     */
    @Test
    void undirectedEdgeInMultiMatch() {
      ResultSet rs = database.query("opencypher",
          """
          MATCH (n:NER {subtype:'DATE'}) \
          MATCH (n)-[r]-(m:NER) \
          MATCH (n)-[]-(chunk:CHUNK)-->(document:DOCUMENT) \
          RETURN n.name AS name""");

      List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        Result result = rs.next();
        names.add((String) result.getProperty("name"));
      }

      // Should return results for the DATE nodes
      assertThat(names).isNotEmpty();
    }
  }
}
