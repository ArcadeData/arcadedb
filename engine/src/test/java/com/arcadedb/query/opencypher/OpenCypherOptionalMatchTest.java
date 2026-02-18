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

/**
 * Tests for OPTIONAL MATCH clause functionality.
 */
class OpenCypherOptionalMatchTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopenopencypher-optional").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      // Create test data: Person nodes, some with KNOWS relationships
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 35})");

      // Alice knows Bob
      database.command("opencypher", """
          MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
          CREATE (a)-[:KNOWS]->(b)""");

      // Charlie has no KNOWS relationships
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
  void optionalMatchWithExistingRelationship() {
    // Alice has a KNOWS relationship, should return Alice and Bob
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Alice'}) \
        OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) \
        RETURN a.name AS person, b.name AS knows""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("person")).isEqualTo("Alice");
    assertThat(row.<String>getProperty("knows")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void matchCharlieAlone() {
    // First test that basic MATCH with property filter works
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Charlie'}) RETURN a.name AS person");

    final List<Result> allResults = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      allResults.add(row);
      //System.out.println("DEBUG testMatchCharlieAlone: person=" + row.getProperty("person"));
    }
    result.close();

    assertThat(allResults.size()).as("Expected exactly 1 result").isEqualTo(1);
    assertThat(allResults.getFirst().<String>getProperty("person")).isEqualTo("Charlie");
  }

  @Test
  void optionalMatchWithoutRelationship() {
    // Charlie has no KNOWS relationship, should return Charlie with NULL for b
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person {name: 'Charlie'}) \
        OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) \
        RETURN a.name AS person, b.name AS knows""");

    // Debug: print all results
    final List<Result> allResults = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      allResults.add(row);
      //System.out.println("DEBUG testOptionalMatchWithoutRelationship: person=" + row.getProperty("person") + ", knows=" + row.getProperty("knows"));
    }
    result.close();

    assertThat(allResults.size()).as("Expected exactly 1 result").isEqualTo(1);
    final Result row = allResults.getFirst();
    assertThat(row.<String>getProperty("person")).isEqualTo("Charlie");
    assertThat(row.<String>getProperty("knows")).as("Expected NULL for knows when no relationship exists").isNull();
  }

  @Test
  void optionalMatchStandalone() {
    // OPTIONAL MATCH without preceding MATCH
    // Should return all Person nodes or NULL if no matches
    final ResultSet result = database.query("opencypher",
        """
        OPTIONAL MATCH (n:Person {name: 'NonExistent'}) \
        RETURN n.name AS name""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("name")).as("Expected NULL when OPTIONAL MATCH finds nothing").isNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void multiplePeopleWithOptionalMatch() {
    // All people, with optional KNOWS relationships
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person) \
        OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) \
        RETURN a.name AS person, b.name AS knows \
        ORDER BY a.name""");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Alice knows Bob
    // Bob knows nobody -> NULL
    // Charlie knows nobody -> NULL
    assertThat(results.size()).as("Expected 3 results (one per person)").isEqualTo(3);

    // Alice -> Bob

    assertThat(results.get(0).<String>getProperty("person")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("knows")).isEqualTo("Bob");

    // Bob -> NULL
    assertThat(results.get(1).<String>getProperty("person")).isEqualTo("Bob");
    assertThat(results.get(1).<String>getProperty("knows")).isNull();

    // Charlie -> NULL
    assertThat(results.get(2).<String>getProperty("person")).isEqualTo("Charlie");
    assertThat(results.get(2).<String>getProperty("knows")).isNull();
  }

  @Test
  void optionalMatchWithWhere() {
    // WHERE clause is now correctly scoped to OPTIONAL MATCH
    // It filters the optional match results but keeps rows where the match failed

    // Query: MATCH all people, try to find their KNOWS relationships with WHERE filter
    // WHERE filters within OPTIONAL MATCH, so people without matches still appear
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person)
            OPTIONAL MATCH (a)-[:KNOWS]->(b:Person)
            WHERE b.age > 20
            RETURN a.name AS person, b.name AS knows
            ORDER BY a.name""");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Correct behavior: WHERE filters within OPTIONAL MATCH
    // All people are returned, but only matches passing the filter are shown
    assertThat(results.size()).as("All people should be returned").isEqualTo(3);

    // Alice -> Bob (matched and passed filter: age 25 > 20)
    assertThat(results.get(0).<String>getProperty("person")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("knows")).isEqualTo("Bob");

    // Bob -> NULL (no outgoing relationships)
    assertThat(results.get(1).<String>getProperty("person")).isEqualTo("Bob");
    assertThat(results.get(1).<String>getProperty("knows")).isNull();

    // Charlie -> NULL (no outgoing relationships)
    assertThat(results.get(2).<String>getProperty("person")).isEqualTo("Charlie");
    assertThat(results.get(2).<String>getProperty("knows")).isNull();
  }

  /**
   * Test for GitHub issue #3360:
   * OPTIONAL MATCH with pattern predicate in WHERE clause should only return
   * nodes that satisfy the pattern, not all nodes of the label.
   */
  @Test
  void optionalMatchWithPatternPredicateInWhere() {
    // Create additional types for this test
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createEdgeType("RELATED_TO");

    database.transaction(() -> {
      // Create the target Document
      database.command("opencypher", "CREATE (d:DOCUMENT {name: 'MyTargetDoc', type: 'target'})");

      // Create a linked Chunk
      database.command("opencypher", "CREATE (c1:CHUNK {name: 'LinkedChunk_1'})");

      // Create unrelated Chunks (Noise)
      database.command("opencypher", "CREATE (c2:CHUNK {name: 'UnrelatedChunk_A'})");
      database.command("opencypher", "CREATE (c3:CHUNK {name: 'UnrelatedChunk_B'})");

      // Link only c1 to d
      database.command("opencypher",
          """
          MATCH (d:DOCUMENT), (c1:CHUNK) \
          WHERE d.name = 'MyTargetDoc' AND c1.name = 'LinkedChunk_1' \
          CREATE (c1)-[:RELATED_TO]->(d)""");
    });

    // This query should return only LinkedChunk_1 because only it has a relationship to doc
    final ResultSet result = database.query("opencypher",
        """
        MATCH (doc:DOCUMENT) WHERE doc.name = 'MyTargetDoc' \
        OPTIONAL MATCH (c:CHUNK) WHERE (c)-->(doc) \
        RETURN doc.name AS docName, c.name AS chunkName""");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return exactly 1 row: doc=MyTargetDoc, c=LinkedChunk_1
    assertThat(results).hasSize(1);
    assertThat(results.get(0).<String>getProperty("docName")).isEqualTo("MyTargetDoc");
    assertThat(results.get(0).<String>getProperty("chunkName")).isEqualTo("LinkedChunk_1");
  }
}
