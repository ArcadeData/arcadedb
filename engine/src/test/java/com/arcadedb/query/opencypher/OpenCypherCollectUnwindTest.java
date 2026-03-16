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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Tests for COLLECT aggregation function and UNWIND clause.
 * COLLECT collects values into a list.
 * UNWIND expands a list into individual rows.
 */
class OpenCypherCollectUnwindTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-collect-unwind").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("LIVES_IN");

    // Create test data:
    //   People with hobbies in different cities
    database.command("opencypher",
        """
            CREATE (alice:Person {name: 'Alice', age: 30, hobbies: ['reading', 'hiking', 'cooking']}),
            (bob:Person {name: 'Bob', age: 25, hobbies: ['gaming', 'reading']}),
            (charlie:Person {name: 'Charlie', age: 35, hobbies: ['hiking', 'photography']}),
            (nyc:City {name: 'NYC'}),
            (la:City {name: 'LA'}),
            (alice)-[:LIVES_IN]->(nyc),
            (bob)-[:LIVES_IN]->(nyc),
            (charlie)-[:LIVES_IN]->(la)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
  }

  // ===== COLLECT Tests =====

  @Test
  void collectBasic() {
    // Collect all person names into a list
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) RETURN collect(n.name) AS names");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<String> names = (List<String>) row.getProperty("names");
    assertThat(names).hasSize(3);
    assertThat(names).containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void collectWithGroupBy() {
    // Collect names grouped by city
    final ResultSet result = database.command("opencypher",
        """
            MATCH (p:Person)-[:LIVES_IN]->(c:City)
            RETURN c.name AS city, collect(p.name) AS residents
            ORDER BY city""");

    // LA should have [Charlie]
    assertThat(result.hasNext()).isTrue();
    final Result laResult = result.next();
    assertThat((String) laResult.getProperty("city")).isEqualTo("LA");
    @SuppressWarnings("unchecked")
    final List<String> laResidents = (List<String>) laResult.getProperty("residents");
    assertThat(laResidents).containsExactly("Charlie");

    // NYC should have [Alice, Bob]
    assertThat(result.hasNext()).isTrue();
    final Result nycResult = result.next();
    assertThat((String) nycResult.getProperty("city")).isEqualTo("NYC");
    @SuppressWarnings("unchecked")
    final List<String> nycResidents = (List<String>) nycResult.getProperty("residents");
    assertThat(nycResidents).hasSize(2);
    assertThat(nycResidents).containsExactlyInAnyOrder("Alice", "Bob");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void collectNumbers() {
    // Collect ages
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) RETURN collect(n.age) AS ages");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<Integer> ages = (List<Integer>) row.getProperty("ages");
    assertThat(ages).hasSize(3);
    assertThat(ages).containsExactlyInAnyOrder(30, 25, 35);
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void collectEmpty() {
    // Collect from no matches (WHERE condition that matches nothing)
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) WHERE n.name = 'DoesNotExist' RETURN collect(n.name) AS names");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<Object> names = (List<Object>) row.getProperty("names");
    assertThat(names).isEmpty();
    assertThat(result.hasNext()).isFalse();
  }

  // ===== UNWIND Tests =====

  @Test
  void unwindSimpleList() {
    // Unwind a simple literal list
    final ResultSet result = database.command("opencypher",
        "UNWIND [1, 2, 3] AS x RETURN x");

    final List<Integer> values = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      values.add(((Number) row.getProperty("x")).intValue());
    }

    assertThat(values).containsExactly(1, 2, 3);
  }

  @Test
  void unwindStringList() {
    // Unwind a list of strings
    final ResultSet result = database.command("opencypher",
        "UNWIND ['a', 'b', 'c'] AS letter RETURN letter");

    final List<String> letters = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      letters.add((String) row.getProperty("letter"));
    }

    assertThat(letters).containsExactly("a", "b", "c");
  }

  @Test
  void unwindWithMatch() {
    // Unwind hobbies property from Person nodes
    final ResultSet result = database.command("opencypher",
        """
            MATCH (n:Person) WHERE n.name = 'Alice'
            UNWIND n.hobbies AS hobby
            RETURN n.name AS name, hobby""");

    final List<String> hobbies = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat((String) row.getProperty("name")).isEqualTo("Alice");
      hobbies.add((String) row.getProperty("hobby"));
    }

    assertThat(hobbies).containsExactly("reading", "hiking", "cooking");
  }

  @Test
  void unwindMultipleNodes() {
    // Unwind hobbies for all persons
    final ResultSet result = database.command("opencypher",
        """
            MATCH (n:Person)
            UNWIND n.hobbies AS hobby
            RETURN n.name AS name, hobby
            ORDER BY name, hobby""");

    // Alice: cooking, hiking, reading
    // Bob: gaming, reading
    // Charlie: hiking, photography
    final List<String> expected = List.of(
        "Alice:cooking",
        "Alice:hiking",
        "Alice:reading",
        "Bob:gaming",
        "Bob:reading",
        "Charlie:hiking",
        "Charlie:photography"
    );

    final List<String> actual = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      actual.add(row.getProperty("name") + ":" + row.getProperty("hobby"));
    }

    assertThat(actual).containsExactlyElementsOf(expected);
  }

  @Test
  void unwindNull() {
    // Unwind null produces no rows
    final ResultSet result = database.command("opencypher",
        "UNWIND null AS x RETURN x");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void unwindEmptyList() {
    // Unwind empty list produces no rows
    final ResultSet result = database.command("opencypher",
        "UNWIND [] AS x RETURN x");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void unwindWithRange() {
    // Unwind using range() function
    final ResultSet result = database.command("opencypher",
        "UNWIND range(1, 5) AS num RETURN num");

    final List<Integer> numbers = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      numbers.add(((Number) row.getProperty("num")).intValue());
    }

    assertThat(numbers).containsExactly(1, 2, 3, 4, 5);
  }

  // ===== Combined COLLECT and UNWIND Tests =====

  // TODO: Enable after WITH clause is implemented
  /*
  @Test
  void testCollectAndUnwind() {
    // Collect all hobbies, then unwind them
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "WITH collect(n.hobbies) AS allHobbiesLists " +
            "UNWIND allHobbiesLists AS hobbyList " +
            "UNWIND hobbyList AS hobby " +
            "RETURN DISTINCT hobby " +
            "ORDER BY hobby");

    final List<String> hobbies = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      hobbies.add((String) row.getProperty("hobby"));
    }

    // Unique hobbies across all people
    assertThat(hobbies).containsExactly("cooking", "gaming", "hiking", "photography", "reading");
  }

  @Test
  void testUnwindThenCollect() {
    // Unwind hobbies then collect them back (grouped by person)
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "UNWIND n.hobbies AS hobby " +
            "WITH n.name AS name, collect(hobby) AS hobbies " +
            "RETURN name, hobbies " +
            "ORDER BY name");

    // Alice
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Alice");
    @SuppressWarnings("unchecked")
    List<String> hobbies = (List<String>) row.getProperty("hobbies");
    assertThat(hobbies).containsExactlyInAnyOrder("reading", "hiking", "cooking");

    // Bob
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Bob");
    @SuppressWarnings("unchecked")
    List<String> hobbies2 = (List<String>) row.getProperty("hobbies");
    assertThat(hobbies2).containsExactlyInAnyOrder("gaming", "reading");

    // Charlie
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("name")).isEqualTo("Charlie");
    @SuppressWarnings("unchecked")
    List<String> hobbies3 = (List<String>) row.getProperty("hobbies");
    assertThat(hobbies3).containsExactlyInAnyOrder("hiking", "photography");

    assertThat(result.hasNext()).isFalse();
  }
  */

  @Test
  void unwindNestedLists() {
    // Test unwinding nested lists
    final ResultSet result = database.command("opencypher",
        """
        UNWIND [[1, 2], [3, 4]] AS innerList \
        UNWIND innerList AS num \
        RETURN num""");

    final List<Integer> numbers = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      numbers.add(((Number) row.getProperty("num")).intValue());
    }

    assertThat(numbers).containsExactly(1, 2, 3, 4);
  }

  // TODO: Enable after WITH clause and DISTINCT support
  /*
  @Test
  void testCollectDistinct() {
    // Test collecting distinct values
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person) " +
            "UNWIND n.hobbies AS hobby " +
            "RETURN collect(DISTINCT hobby) AS uniqueHobbies");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    @SuppressWarnings("unchecked")
    final List<String> hobbies = (List<String>) row.getProperty("uniqueHobbies");
    assertThat(hobbies).hasSize(5);
    assertThat(hobbies).containsExactlyInAnyOrder("reading", "hiking", "cooking", "gaming", "photography");
    assertThat(result.hasNext()).isFalse();
  }
  */

  // ===== Regression Test for Issue #3307 =====

  /**
   * Regression test for Issue #3307: Cypher head(collect()) returning null values.
   * <p>
   * Root cause: WithClause.hasAggregations() doesn't recursively detect aggregations
   * nested inside non-aggregation functions like head(collect()). This causes the
   * planner to use WithStep instead of AggregationStep, breaking the aggregation.
   * <p>
   * Test scenarios:
   * - head(collect(ID(doc))) with single document
   * - head(collect(ID(doc))) with multiple documents
   * - head(collect(doc.name)) with property access
   * - head(collect(doc)) with full node
   * - Edge case: no matching documents (should return null gracefully)
   */
  @Test
  void headCollectInWithClause() {
    // Setup: Create CHUNK -> DOCUMENT relationship matching issue #3307
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createEdgeType("BELONGS_TO");

    // Create test data: 3 chunks pointing to 2 documents
    database.command("opencypher",
        """
            CREATE (doc1:DOCUMENT {name: 'Document A', id: 'doc-a'}),
            (doc2:DOCUMENT {name: 'Document B', id: 'doc-b'}),
            (chunk1:CHUNK {text: 'chunk 1'}),
            (chunk2:CHUNK {text: 'chunk 2'}),
            (chunk3:CHUNK {text: 'chunk 3'}),
            (chunk1)-[:BELONGS_TO]->(doc1),
            (chunk2)-[:BELONGS_TO]->(doc1),
            (chunk3)-[:BELONGS_TO]->(doc2)""");

    // Test 1: head(collect(ID(doc))) - should return first document ID (not null!)
    ResultSet result = database.command("opencypher",
        """
            MATCH (c:CHUNK)-[:BELONGS_TO]->(doc:DOCUMENT)
            WITH c, head(collect(ID(doc))) AS documentId
            RETURN c.text AS chunk, documentId
            ORDER BY chunk""");

    // Verify chunk1 result
    assertThat(result.hasNext()).isTrue();
    Result row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 1");
    final Object documentId1 = row.getProperty("documentId");
    assertThat(documentId1).as("documentId should not be null for chunk 1").isNotNull();
    assertThat(documentId1).as("documentId should be a String or RID").isInstanceOf(Object.class);

    // Verify chunk2 result
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 2");
    final Object documentId2 = row.getProperty("documentId");
    assertThat(documentId2).as("documentId should not be null for chunk 2").isNotNull();

    // Verify chunk3 result
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 3");
    final Object documentId3 = row.getProperty("documentId");
    assertThat(documentId3).as("documentId should not be null for chunk 3").isNotNull();

    assertThat(result.hasNext()).isFalse();

    // Test 2: head(collect(doc.name)) - should return first document name
    result = database.command("opencypher",
        """
            MATCH (c:CHUNK)-[:BELONGS_TO]->(doc:DOCUMENT)
            WITH c, head(collect(doc.name)) AS documentName
            RETURN c.text AS chunk, documentName
            ORDER BY chunk""");

    // Verify chunk1
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 1");
    final String docName1 = (String) row.getProperty("documentName");
    assertThat(docName1).as("documentName should not be null for chunk 1").isNotNull();
    assertThat(docName1).as("documentName should be 'Document A'").isEqualTo("Document A");

    // Verify chunk2
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 2");
    final String docName2 = (String) row.getProperty("documentName");
    assertThat(docName2).as("documentName should not be null for chunk 2").isNotNull();
    assertThat(docName2).as("documentName should be 'Document A'").isEqualTo("Document A");

    // Verify chunk3
    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 3");
    final String docName3 = (String) row.getProperty("documentName");
    assertThat(docName3).as("documentName should not be null for chunk 3").isNotNull();
    assertThat(docName3).as("documentName should be 'Document B'").isEqualTo("Document B");

    assertThat(result.hasNext()).isFalse();

    // Test 3: Test with multiple documents per chunk to verify head() gets first element
    database.command("opencypher",
        """
            MATCH (chunk1:CHUNK {text: 'chunk 1'}), (doc2:DOCUMENT {name: 'Document B'})
            CREATE (chunk1)-[:BELONGS_TO]->(doc2)""");

    result = database.command("opencypher",
        """
            MATCH (c:CHUNK {text: 'chunk 1'})-[:BELONGS_TO]->(doc:DOCUMENT)
            WITH c, head(collect(doc.name)) AS firstDocName, collect(doc.name) AS allDocNames
            RETURN c.text AS chunk, firstDocName, allDocNames""");

    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 1");
    final String firstDoc = (String) row.getProperty("firstDocName");
    assertThat(firstDoc).as("firstDocName should not be null").isNotNull();
    assertThat(firstDoc).as("firstDocName should be one of the documents").isIn("Document A", "Document B");

    @SuppressWarnings("unchecked")
    final List<String> allDocs = (List<String>) row.getProperty("allDocNames");
    assertThat(allDocs).as("allDocNames should contain 2 documents").hasSize(2);
    assertThat(allDocs).as("allDocNames should contain both documents")
        .containsExactlyInAnyOrder("Document A", "Document B");
    assertThat(firstDoc).as("firstDocName should be the first element of allDocNames").isEqualTo(allDocs.get(0));

    assertThat(result.hasNext()).isFalse();

    // Test 4: Edge case - no matching documents (should return null gracefully)
    database.getSchema().createVertexType("ORPHAN_CHUNK");
    database.command("opencypher", "CREATE (orphan:ORPHAN_CHUNK {text: 'orphan'})");

    result = database.command("opencypher",
        """
            MATCH (c:ORPHAN_CHUNK)
            OPTIONAL MATCH (c)-[:BELONGS_TO]->(doc:DOCUMENT)
            WITH c, head(collect(doc.name)) AS documentName
            RETURN c.text AS chunk, documentName""");

    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("orphan");
    final Object orphanDocName = row.getProperty("documentName");
    // For no matches, head(collect(null)) should return null gracefully
    assertThat(orphanDocName).as("documentName should be null for orphan chunk with no relationships").isNull();

    assertThat(result.hasNext()).isFalse();

    // Test 5: Test head(collect(doc)) with full node objects
    result = database.command("opencypher",
        """
            MATCH (c:CHUNK {text: 'chunk 3'})-[:BELONGS_TO]->(doc:DOCUMENT)
            WITH c, head(collect(doc)) AS firstDoc
            RETURN c.text AS chunk, firstDoc.name AS docName, firstDoc.id AS docId""");

    assertThat(result.hasNext()).isTrue();
    row = result.next();
    assertThat((String) row.getProperty("chunk")).isEqualTo("chunk 3");
    final String fullDocName = (String) row.getProperty("docName");
    final String fullDocId = (String) row.getProperty("docId");
    assertThat(fullDocName).as("docName should not be null when accessing firstDoc.name").isNotNull();
    assertThat(fullDocName).as("docName should be 'Document B'").isEqualTo("Document B");
    assertThat(fullDocId).as("docId should not be null when accessing firstDoc.id").isNotNull();
    assertThat(fullDocId).as("docId should be 'doc-b'").isEqualTo("doc-b");

    assertThat(result.hasNext()).isFalse();
  }

  /** See issue #3129 */
  @Nested
  class UnwindMergeUniqueRidsRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-3129").create();
      database.getSchema().createVertexType("CHUNK");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void unwindMergeReturnsUniqueRids() {
      final List<Map<String, Object>> batch = new ArrayList<>();

      Map<String, Object> entry1 = new HashMap<>();
      entry1.put("subtype", "type1");
      entry1.put("name", "chunk1");
      batch.add(entry1);

      Map<String, Object> entry2 = new HashMap<>();
      entry2.put("subtype", "type2");
      entry2.put("name", "chunk2");
      batch.add(entry2);

      Map<String, Object> entry3 = new HashMap<>();
      entry3.put("subtype", "type1");
      entry3.put("name", "chunk3");
      batch.add(entry3);

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher",
            """
            UNWIND $batch AS BatchEntry \
            MERGE (n:CHUNK { subtype: BatchEntry.subtype, name: BatchEntry.name }) \
            RETURN ID(n) AS id""",
            Map.of("batch", batch));

        final Set<String> rids = new HashSet<>();
        int count = 0;

        while (rs.hasNext()) {
          final Result r = rs.next();
          final String id = (String) r.getProperty("id");

          assertThat(id).isNotNull();
          rids.add(id);

          assertThat(r.getPropertyNames()).as("Result should only contain 'id', not 'BatchEntry'")
              .containsExactly("id");

          count++;
        }

        assertThat(count).isEqualTo(3);
        assertThat(rids).as("All three chunks should have unique RIDs").hasSize(3);
      });
    }

    @Test
    void unwindMergeWithDuplicates() {
      final List<Map<String, Object>> batch = new ArrayList<>();

      Map<String, Object> entry1 = new HashMap<>();
      entry1.put("subtype", "type1");
      entry1.put("name", "chunk1");
      batch.add(entry1);

      Map<String, Object> entry2 = new HashMap<>();
      entry2.put("subtype", "type1");
      entry2.put("name", "chunk1");
      batch.add(entry2);

      Map<String, Object> entry3 = new HashMap<>();
      entry3.put("subtype", "type2");
      entry3.put("name", "chunk2");
      batch.add(entry3);

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher",
            """
            UNWIND $batch AS BatchEntry \
            MERGE (n:CHUNK { subtype: BatchEntry.subtype, name: BatchEntry.name }) \
            RETURN ID(n) AS id""",
            Map.of("batch", batch));

        final List<String> rids = new ArrayList<>();
        int count = 0;

        while (rs.hasNext()) {
          final Result r = rs.next();
          final String id = (String) r.getProperty("id");
          rids.add(id);
          count++;
        }

        assertThat(count).isEqualTo(3);

        assertThat(rids.get(0)).isEqualTo(rids.get(1)).as("First two entries are duplicates, should get same RID");
        assertThat(rids.get(2)).isNotEqualTo(rids.get(0)).as("Third entry is different, should get different RID");

        assertThat(new HashSet<>(rids)).hasSize(2);
      });

      final ResultSet verifyResult = database.query("opencypher", "MATCH (n:CHUNK) RETURN count(n) AS count");
      final long nodeCount = ((Number) verifyResult.next().getProperty("count")).longValue();
      assertThat(nodeCount).isEqualTo(2);
    }
  }

  /** See issue #3138 */
  @Nested
  class UnwindMatchMergeBatchRegression {
    private Database database;
    private List<String> sourceIds;
    private String targetId;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-3138").create();
      database.getSchema().createVertexType("Source");
      database.getSchema().createVertexType("Target");
      database.getSchema().createEdgeType("in");

      sourceIds = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 3; i++) {
          MutableVertex source = database.newVertex("Source");
          source.set("name", "source" + i);
          source.save();
          sourceIds.add(source.getIdentity().toString());
        }

        MutableVertex target = database.newVertex("Target");
        target.set("name", "target");
        target.save();
        targetId = target.getIdentity().toString();
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
    void unwindMatchMergeCreateRelations() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        row.put("target_id", targetId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher",
            """
            UNWIND $batch as row \
            MATCH (a),(b) WHERE ID(a) = row.source_id AND ID(b) = row.target_id \
            MERGE (a)-[r:in]->(b) \
            RETURN a, b, r""",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Should return 3 results, one for each relationship created")
            .isEqualTo(3);
      });

      final ResultSet verifyResult = database.query("opencypher",
          "MATCH (a:Source)-[r:in]->(b:Target) RETURN count(r) AS count");
      final long edgeCount = ((Number) verifyResult.next().getProperty("count")).longValue();

      assertThat(edgeCount)
          .as("Should have created 3 relationships")
          .isEqualTo(3);
    }

    @Test
    void unwindMatchMergeWithCreate() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        row.put("target_id", targetId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher",
            """
            UNWIND $batch as row \
            MATCH (a),(b) WHERE ID(a) = row.source_id AND ID(b) = row.target_id \
            CREATE (a)-[r:in]->(b) \
            RETURN a, b, r""",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Should return 3 results, one for each relationship created")
            .isEqualTo(3);
      });

      final ResultSet verifyResult = database.query("opencypher",
          "MATCH (a:Source)-[r:in]->(b:Target) RETURN count(r) AS count");
      final long edgeCount = ((Number) verifyResult.next().getProperty("count")).longValue();

      assertThat(edgeCount)
          .as("Should have created 3 relationships")
          .isEqualTo(3);
    }

    @Test
    void simplifiedUnwindMatchReturnOnly() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        row.put("target_id", targetId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            """
            UNWIND $batch as row \
            MATCH (a),(b) WHERE ID(a) = row.source_id AND ID(b) = row.target_id \
            RETURN a, b""",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Should return 3 results from UNWIND + MATCH")
            .isEqualTo(3);
      });
    }

    @Test
    void unwindOnly() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        row.put("target_id", targetId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "UNWIND $batch as row RETURN row.source_id as sid, row.target_id as tid",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("UNWIND should produce 3 rows")
            .isEqualTo(3);
      });
    }

    @Test
    void unwindMatchSingleNode() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = row.source_id RETURN a",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Should return 3 matched nodes")
            .isEqualTo(3);
      });
    }

    @Test
    void unwindMatchNoWhere() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet countRs = database.query("opencypher",
            "MATCH (a:Source) RETURN count(a) as c");
        countRs.next();

        final ResultSet rs = database.query("opencypher",
            "UNWIND $batch as row MATCH (a:Source) RETURN row.source_id as sid, ID(a) as aid",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Should return 9 results (3 batch rows x 3 vertices)")
            .isEqualTo(9);
      });
    }

    @Test
    void unwindMatchWithSimpleWhere() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "UNWIND $batch as row MATCH (a:Source) WHERE row.source_id = row.source_id RETURN a",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("WHERE row.source_id = row.source_id should return 9 results (always true)")
            .isEqualTo(9);
      });
    }

    @Test
    void unwindMatchWithIdEqual() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = ID(a) RETURN a",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("WHERE ID(a) = ID(a) should return 9 results (always true)")
            .isEqualTo(9);
      });
    }

    @Test
    void unwindMatchWithIdEqualsString() {
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher",
            "MATCH (a:Source) WHERE ID(a) = '#1:0' RETURN a");

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("WHERE ID(a) = '#1:0' should return 1 result")
            .isEqualTo(1);
      });
    }

    @Test
    void unwindMatchWithIdEqualsRowProperty() {
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String sourceId : sourceIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sourceId);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet debugRs = database.query("opencypher",
            """
            UNWIND $batch as row MATCH (a:Source) \
            RETURN ID(a) as aid, row.source_id as sid, ID(a) = row.source_id as isEqual""",
            Map.of("batch", batch));

        while (debugRs.hasNext())
          debugRs.next();
      });

      database.transaction(() -> {
        final ResultSet literalRs = database.query("opencypher",
            "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = '#1:0' RETURN a, row.source_id as sid",
            Map.of("batch", batch));
        while (literalRs.hasNext())
          literalRs.next();

        final ResultSet rs = database.query("opencypher",
            "UNWIND $batch as row MATCH (a:Source) WHERE ID(a) = row.source_id RETURN a",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("WHERE ID(a) = row.source_id should return 3 results (one per batch row)")
            .isEqualTo(3);
      });
    }

    /** See issue #1948 */
    @Test
    void issue1948UnwindMatchWithSameSourceId() {
      final List<String> targetIds = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 3; i++) {
          MutableVertex target = database.newVertex("Target");
          target.set("name", "multiTarget" + i);
          target.save();
          targetIds.add(target.getIdentity().toString());
        }
      });

      final String sameSourceId = sourceIds.get(0);
      final List<Map<String, Object>> batch = new ArrayList<>();
      for (String targetIdItem : targetIds) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_id", sameSourceId);
        row.put("target_id", targetIdItem);
        batch.add(row);
      }

      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher",
            """
            UNWIND $batch as row \
            MATCH (a:Source) WHERE ID(a) = row.source_id \
            MATCH (b) WHERE ID(b) = row.target_id \
            MERGE (a)-[r:in]->(b) \
            RETURN a, b, r""",
            Map.of("batch", batch));

        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }

        assertThat(count)
            .as("Issue #1948: With same source_id but different target_ids, should return 3 results")
            .isEqualTo(3);
      });

      final ResultSet verifyResult = database.query("opencypher",
          "MATCH (a:Source {name: 'source0'})-[r:in]->(b:Target) RETURN count(r) AS count");
      final long edgeCount = ((Number) verifyResult.next().getProperty("count")).longValue();

      assertThat(edgeCount)
          .as("Should have created 3 relationships from the same source")
          .isEqualTo(3);
    }

    /** See issue #1948 */
    @Test
    void issue1948SimpleMatchWithSameSourceId() {
      final String sameSourceId = sourceIds.get(0);
      final List<Map<String, Object>> batch = new ArrayList<>();
      batch.add(Map.of("source_id", sameSourceId));
      batch.add(Map.of("source_id", sameSourceId));
      batch.add(Map.of("source_id", sameSourceId));

      final ResultSet rs = database.query("opencypher",
          """
          UNWIND $batch as row \
          MATCH (a:Source) WHERE ID(a) = row.source_id \
          RETURN a, row""",
          Map.of("batch", batch));

      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }

      assertThat(count)
          .as("Issue #1948: UNWIND + MATCH with same source_id should return one result per batch entry")
          .isEqualTo(3);
    }
  }

  /** See issue #3154 */
  @Nested
  class UnwindCreatePropertyRefsRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./databases/test-issue-3154").create();
      database.getSchema().createVertexType("CHUNK");
      database.getSchema().createVertexType("CHUNK_EMBEDDING");
      database.getSchema().createVertexType("USER_RIGHTS");
      database.getSchema().createEdgeType("embb");
    }

    @AfterEach
    void tearDown() {
      if (database != null)
        database.drop();
    }

    @Test
    void unwindWithCreateSimple() {
      database.command("opencypher", "CREATE (c:CHUNK {name: 'test'})");

      final Map<String, Object> params = new HashMap<>();
      final List<Map<String, Object>> batch = new ArrayList<>();

      final Map<String, Object> entry = new HashMap<>();
      entry.put("name", "Alice");
      entry.put("age", 30);
      batch.add(entry);

      params.put("batch", batch);

      final ResultSet result = database.command("opencypher",
          """
          UNWIND $batch AS entry \
          CREATE (p:USER_RIGHTS {user_name: entry.name, age: entry.age}) \
          RETURN p""", params);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = (Vertex) row.toElement();

      assertThat((String) vertex.get("user_name")).isEqualTo("Alice");
      assertThat((Integer) vertex.get("age")).isEqualTo(30);
    }

    @Test
    void unwindWithMergePropertyReferences() {
      final Map<String, Object> params = new HashMap<>();
      params.put("user_name", "root");
      params.put("right_0", "OWNER");

      final ResultSet result = database.command("opencypher",
          """
          UNWIND [{user_name: $user_name, right: $right_0}] AS node \
          MERGE (n:USER_RIGHTS {user_name: node.user_name, right: node.right}) \
          RETURN n""", params);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = (Vertex) row.toElement();

      assertThat((String) vertex.get("user_name")).isEqualTo("root");
      assertThat((String) vertex.get("right")).isEqualTo("OWNER");
    }

    @Test
    void unwindWithCreateAndMatch() {
      final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
      assertThat(createResult.hasNext()).isTrue();
      final String chunkRid = createResult.next().getProperty("rid").toString();

      final Map<String, Object> params = new HashMap<>();
      final List<Map<String, Object>> batch = new ArrayList<>();

      final Map<String, Object> entry = new HashMap<>();
      entry.put("value", 42);
      entry.put("destRID", chunkRid);
      batch.add(entry);

      params.put("batch", batch);

      final ResultSet result = database.command("opencypher",
          """
          UNWIND $batch AS BatchEntry \
          MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
          CREATE (p:CHUNK_EMBEDDING {value: BatchEntry.value}) \
          CREATE (p)-[:embb]->(b) \
          RETURN p""", params);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = (Vertex) row.toElement();

      assertThat((Integer) vertex.get("value")).isEqualTo(42);
    }

    @Test
    void unwindWithCreateVectorProperty() {
      final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
      assertThat(createResult.hasNext()).isTrue();
      final String chunkRid = createResult.next().getProperty("rid").toString();

      final Map<String, Object> params = new HashMap<>();
      final List<Map<String, Object>> batch = new ArrayList<>();

      final Map<String, Object> entry = new HashMap<>();
      final float[] vector = new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
      entry.put("vector", vector);
      entry.put("destRID", chunkRid);
      batch.add(entry);

      params.put("batch", batch);

      final ResultSet result = database.command("opencypher",
          """
          UNWIND $batch AS BatchEntry \
          MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
          CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
          CREATE (p)-[:embb]->(b) \
          RETURN p""", params);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = (Vertex) row.toElement();

      final Object vectorProp = vertex.get("vector");
      assertThat(vectorProp).isNotNull();
      assertThat(vectorProp).isNotEqualTo("BatchEntry.vector");

      if (vectorProp instanceof float[])
        assertThat((float[]) vectorProp).containsExactly(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);
      else if (vectorProp instanceof List) {
        @SuppressWarnings("unchecked")
        final List<Number> vectorList = (List<Number>) vectorProp;
        assertThat(vectorList).hasSize(5);
      }
    }

    @Test
    void originalIssue3154Scenario() {
      final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
      assertThat(createResult.hasNext()).isTrue();
      final String chunkRid = createResult.next().getProperty("rid").toString();

      final Map<String, Object> params = new HashMap<>();
      final List<Map<String, Object>> batch = new ArrayList<>();

      final Map<String, Object> entry = new HashMap<>();
      final float[] vector = new float[]{
          -0.0159912109375f, 0.020538330078125f, -0.028778076171875f, 0.0081634521484375f,
          -0.0149993896484375f, -0.044219970703125f, 0.0160064697265625f, 0.0028247833251953125f,
          -0.006366729736328125f, 0.0203399658203125f
      };
      entry.put("vector", vector);
      entry.put("destRID", chunkRid);
      batch.add(entry);

      params.put("batch", batch);

      final ResultSet result = database.command("opencypher",
          """
          UNWIND $batch AS BatchEntry \
          MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
          CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
          CREATE (p)-[:embb]->(b) \
          RETURN p""", params);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = (Vertex) row.toElement();

      final Object vectorProp = vertex.get("vector");
      assertThat(vectorProp).isNotNull();
      assertThat(vectorProp).isNotEqualTo("BatchEntry.vector");
      assertThat(vectorProp).isNotInstanceOf(String.class);
    }

    @Test
    void issue3211Scenario() {
      final ResultSet createResult = database.command("opencypher", "CREATE (c:CHUNK {id: 1}) RETURN ID(c) as rid");
      assertThat(createResult.hasNext()).isTrue();
      final String chunkRid = createResult.next().getProperty("rid").toString();

      final Map<String, Object> params = new HashMap<>();
      final List<Map<String, Object>> batch = new ArrayList<>();

      final Map<String, Object> entry = new HashMap<>();
      final float[] vector = new float[]{
          -0.01425933837890625f, 0.016937255859375f, -0.037384033203125f,
          -0.0006160736083984375f, 7.736682891845703e-05f
      };
      entry.put("vector", vector);
      entry.put("destRID", chunkRid);
      batch.add(entry);

      params.put("batch", batch);

      final ResultSet result = database.command("opencypher",
          """
          UNWIND $batch AS BatchEntry \
          MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID \
          CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) \
          CREATE (p)-[:embb]->(b) \
          RETURN p""", params);

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = (Vertex) row.toElement();

      final Object vectorProp = vertex.get("vector");
      assertThat(vectorProp).isNotNull();
      assertThat(vectorProp).isNotEqualTo("BatchEntry.vector");
      assertThat(vectorProp).isNotInstanceOf(String.class);

      if (vectorProp instanceof float[])
        assertThat((float[]) vectorProp).hasSize(5);
      else if (vectorProp instanceof List)
        assertThat((List<?>) vectorProp).hasSize(5);
    }
  }

  /** See issue #3612 */
  @Nested
  class UnwindInlinePropertyFilterRegression {
    private Database database;
    private static final String DB_PATH = "./target/test-databases/issue-3612-test";

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      database = new DatabaseFactory(DB_PATH).create();

      database.transaction(() -> {
        final var personType = database.getSchema().createVertexType("Person");
        personType.createProperty("id", Type.INTEGER);
        personType.createProperty("name", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");
        database.getSchema().createEdgeType("KNOWS");
      });

      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.command("opencypher", "CREATE (:Person {id: " + i + ", name: 'Person" + i + "'})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void unwindWithInlinePropertyFilterCommaPattern() {
      database.command("opencypher", "MATCH ()-[k:KNOWS]->() DELETE k");

      final List<Map<String, Object>> batch = List.of(
          Map.of("src_id", 0, "dst_id", 1, "weight", 0.5),
          Map.of("src_id", 1, "dst_id", 2, "weight", 0.7),
          Map.of("src_id", 2, "dst_id", 3, "weight", 0.3)
      );

      database.command("opencypher",
          "UNWIND $batch AS e " +
              "MATCH (a:Person {id: e.src_id}), (b:Person {id: e.dst_id}) " +
              "CREATE (a)-[:KNOWS {weight: e.weight}]->(b)",
          Map.of("batch", batch));

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS cnt")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(3L);
      }
    }

    @Test
    void unwindWithWhereClause() {
      database.command("opencypher", "MATCH ()-[k:KNOWS]->() DELETE k");

      final List<Map<String, Object>> batch = List.of(
          Map.of("src_id", 0, "dst_id", 1, "weight", 0.5),
          Map.of("src_id", 1, "dst_id", 2, "weight", 0.7),
          Map.of("src_id", 2, "dst_id", 3, "weight", 0.3)
      );

      database.command("opencypher",
          "UNWIND $batch AS e " +
              "MATCH (a:Person) WHERE a.id = e.src_id " +
              "MATCH (b:Person) WHERE b.id = e.dst_id " +
              "CREATE (a)-[:KNOWS {weight: e.weight}]->(b)",
          Map.of("batch", batch));

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS cnt")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(3L);
      }
    }

    @Test
    void unwindWithSingleMatchInlineFilter() {
      final List<Map<String, Object>> batch = List.of(
          Map.of("id", 0),
          Map.of("id", 1),
          Map.of("id", 2)
      );

      try (final ResultSet rs = database.query("opencypher",
          "UNWIND $batch AS e " +
              "MATCH (a:Person {id: e.id}) " +
              "RETURN a.name AS name ORDER BY name",
          Map.of("batch", batch))) {
        final List<String> names = new ArrayList<>();
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
        assertThat(names).containsExactly("Person0", "Person1", "Person2");
      }
    }

    @Test
    void inlinePropertyFilterWithLiteral() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Person {id: 0}) RETURN a.name AS name")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("Person0");
      }
    }

    @Test
    void inlinePropertyFilterWithParameter() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Person {id: $id}) RETURN a.name AS name",
          Map.of("id", 0))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("Person0");
      }
    }

    @Test
    void unwindBatchWhereUsesIndex() {
      database.command("opencypher", "MATCH ()-[k:KNOWS]->() DELETE k");

      final List<Map<String, Object>> batch = new ArrayList<>();
      for (int i = 0; i < 4; i++)
        batch.add(Map.of("src_id", i, "dst_id", i + 1, "weight", 0.5, "since", "2024"));

      database.command("opencypher",
          "UNWIND $batch AS e " +
              "MATCH (a:Person) WHERE a.id = e.src_id " +
              "MATCH (b:Person) WHERE b.id = e.dst_id " +
              "CREATE (a)-[:KNOWS {weight: e.weight, since: e.since}]->(b)",
          Map.of("batch", batch));

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS cnt")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(4L);
      }
    }

    @Test
    void unwindSimpleListWithInlineFilter() {
      try (final ResultSet rs = database.query("opencypher",
          "UNWIND [0, 1, 2] AS id " +
              "MATCH (a:Person {id: id}) " +
              "RETURN a.name AS name ORDER BY name")) {
        final List<String> names = new ArrayList<>();
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
        assertThat(names).containsExactly("Person0", "Person1", "Person2");
      }
    }
  }
}
