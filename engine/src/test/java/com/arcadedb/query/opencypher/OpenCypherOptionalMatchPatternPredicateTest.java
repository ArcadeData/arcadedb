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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for OPTIONAL MATCH with pattern predicates in WHERE clause.
 * Reproduces issue where OPTIONAL MATCH with pattern predicate returns unrelated nodes.
 */
class OpenCypherOptionalMatchPatternPredicateTest {
  private Database database;
  private RID documentRID;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopenopencypher-optional-pattern").create();

    // Create schema
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createEdgeType("CONTAINS");

    database.transaction(() -> {
      // Create one specific document
      final MutableVertex document = database.newVertex("DOCUMENT");
      document.set("name", "MainDoc");
      document.save();
      documentRID = document.getIdentity();

      // Create chunks - some connected to the document, some not
      final MutableVertex connectedChunk1 = database.newVertex("CHUNK");
      connectedChunk1.set("name", "ConnectedChunk1");
      connectedChunk1.save();

      final MutableVertex connectedChunk2 = database.newVertex("CHUNK");
      connectedChunk2.set("name", "ConnectedChunk2");
      connectedChunk2.save();

      final MutableVertex unconnectedChunk1 = database.newVertex("CHUNK");
      unconnectedChunk1.set("name", "UnconnectedChunk1");
      unconnectedChunk1.save();

      final MutableVertex unconnectedChunk2 = database.newVertex("CHUNK");
      unconnectedChunk2.set("name", "UnconnectedChunk2");
      unconnectedChunk2.save();

      final MutableVertex unconnectedChunk3 = database.newVertex("CHUNK");
      unconnectedChunk3.set("name", "UnconnectedChunk3");
      unconnectedChunk3.save();

      // Connect only ConnectedChunk1 and ConnectedChunk2 to the document
      connectedChunk1.newEdge("CONTAINS", document, true, (Object[]) null);
      connectedChunk2.newEdge("CONTAINS", document, true, (Object[]) null);
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
  void testOptionalMatchWithPatternPredicateByID() {
    // This is the core issue: OPTIONAL MATCH with pattern predicate in WHERE
    // Should only return chunks that have an edge to the specific document
    final String query = String.format(
        "MATCH (doc:DOCUMENT) WHERE ID(doc) = '%s' " +
            "OPTIONAL MATCH (c:CHUNK) WHERE (c)-->(doc) " +
            "RETURN doc, c",
        documentRID);

    final ResultSet result = database.query("opencypher", query);

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Expected: Should return exactly 2 rows (one for each connected chunk)
    // Bug: Returns all chunks including unconnected ones
    assertThat(results.size()).as("Should only return connected chunks").isEqualTo(2);

    // Verify that only connected chunks are returned
    for (final Result row : results) {
      assertThat(row.getProperty("doc") != null).as("Document should not be null").isTrue();
      assertThat(row.getProperty("c") != null).as("Chunk should not be null when pattern matches").isTrue();

      final Vertex chunk = (Vertex) row.getProperty("c");
      final String chunkName = (String) chunk.get("name");
      assertThat(chunkName).as("Only connected chunks should be returned")
          .isIn("ConnectedChunk1", "ConnectedChunk2");
    }
  }

  @Test
  void testOptionalMatchWithPatternPredicateByProperty() {
    // Similar test using property filter instead of ID
    final ResultSet result = database.query("opencypher",
        "MATCH (doc:DOCUMENT {name: 'MainDoc'}) " +
            "OPTIONAL MATCH (c:CHUNK) WHERE (c)-->(doc) " +
            "RETURN doc.name AS docName, c.name AS chunkName " +
            "ORDER BY chunkName");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Expected: Should return exactly 2 rows
    assertThat(results.size()).as("Should only return connected chunks").isEqualTo(2);

    assertThat(results.get(0).<String>getProperty("docName")).isEqualTo("MainDoc");
    assertThat(results.get(0).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk1");

    assertThat(results.get(1).<String>getProperty("docName")).isEqualTo("MainDoc");
    assertThat(results.get(1).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk2");
  }

  @Test
  void testOptionalMatchWithDirectedPatternPredicate() {
    // Test with explicit direction in pattern predicate
    final ResultSet result = database.query("opencypher",
        "MATCH (doc:DOCUMENT {name: 'MainDoc'}) " +
            "OPTIONAL MATCH (c:CHUNK) WHERE (c)-[:CONTAINS]->(doc) " +
            "RETURN doc.name AS docName, c.name AS chunkName " +
            "ORDER BY chunkName");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Expected: Should return exactly 2 rows
    assertThat(results.size()).as("Should only return connected chunks").isEqualTo(2);

    assertThat(results.get(0).<String>getProperty("docName")).isEqualTo("MainDoc");
    assertThat(results.get(0).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk1");

    assertThat(results.get(1).<String>getProperty("docName")).isEqualTo("MainDoc");
    assertThat(results.get(1).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk2");
  }

  @Test
  void testOptionalMatchWithPatternPredicateNoMatches() {
    // Test behavior when there are no matching chunks for a document
    database.transaction(() -> {
      final MutableVertex lonelyDoc = database.newVertex("DOCUMENT");
      lonelyDoc.set("name", "LonelyDoc");
      lonelyDoc.save();
    });

    final ResultSet result = database.query("opencypher",
        "MATCH (doc:DOCUMENT {name: 'LonelyDoc'}) " +
            "OPTIONAL MATCH (c:CHUNK) WHERE (c)-->(doc) " +
            "RETURN doc.name AS docName, c.name AS chunkName");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("docName")).isEqualTo("LonelyDoc");
    assertThat(row.<String>getProperty("chunkName")).as("Should be null when no chunks match").isNull();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void testPatternPredicateInRegularMatch() {
    // Test if pattern predicates work at all in regular MATCH (not OPTIONAL)
    final ResultSet result = database.query("opencypher",
        "MATCH (doc:DOCUMENT {name: 'MainDoc'}), (c:CHUNK) " +
            "WHERE (c)-->(doc) " +
            "RETURN doc.name AS docName, c.name AS chunkName " +
            "ORDER BY chunkName");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return only the 2 connected chunks
    assertThat(results.size()).as("Pattern predicate should filter in regular MATCH").isEqualTo(2);
    assertThat(results.get(0).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk1");
    assertThat(results.get(1).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk2");
  }

  @Test
  void testSimplifiedOptionalMatchWithoutWhere() {
    // Test OPTIONAL MATCH without WHERE to see if base functionality works
    final ResultSet result = database.query("opencypher",
        "MATCH (doc:DOCUMENT {name: 'MainDoc'}) " +
            "OPTIONAL MATCH (c:CHUNK)-[:CONTAINS]->(doc) " +
            "RETURN doc.name AS docName, c.name AS chunkName " +
            "ORDER BY chunkName");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Should return the 2 connected chunks
    assertThat(results.size()).as("OPTIONAL MATCH with relationship should work").isEqualTo(2);
    assertThat(results.get(0).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk1");
    assertThat(results.get(1).<String>getProperty("chunkName")).isEqualTo("ConnectedChunk2");
  }
}
