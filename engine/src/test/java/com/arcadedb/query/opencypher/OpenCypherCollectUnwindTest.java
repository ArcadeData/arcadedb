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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
        "UNWIND [[1, 2], [3, 4]] AS innerList " +
            "UNWIND innerList AS num " +
            "RETURN num");

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
}
