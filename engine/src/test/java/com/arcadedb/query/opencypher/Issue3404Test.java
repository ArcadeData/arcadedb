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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3404: COLLECT(rel) returns a count instead of a list of edge objects.
 * Tests that COLLECT on relationship variables returns a list of edge objects, not a count.
 */
class Issue3404Test {
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
  void testCollectRelationshipsReturnsListNotCount() {
    // Query that collects relationships
    final String query = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                         "RETURN ID(nodeDOc), COLLECT(ID(chunk)), COLLECT(rel)";

    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();

        final Object collectedChunks = row.getProperty("COLLECT(ID(chunk))");
        final Object collectedRels = row.getProperty("COLLECT(rel)");

        System.out.println("Document: " + row.getProperty("ID(nodeDOc)"));
        System.out.println("  Collected chunks: " + collectedChunks);
        System.out.println("  Collected rels: " + collectedRels);
        System.out.println("  Collected rels type: " + (collectedRels != null ? collectedRels.getClass().getName() : "null"));

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
  void testCollectRelationshipsWithIdStillWorks() {
    // Verify that the workaround (wrapping in ID()) still works
    final String query = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                         "RETURN ID(nodeDOc), COLLECT(ID(rel))";

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
  void testCollectWithNoGrouping() {
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
  void testCollectNodesStillWorks() {
    // Verify that COLLECT still works correctly for nodes
    final String query = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                         "RETURN ID(nodeDOc), COLLECT(chunk) AS chunks";

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
  void testJsonSerializationForStudio() {
    // Test that JSON serialization (as used by Studio) correctly serializes COLLECT(rel) as a list, not a count
    final JsonGraphSerializer serializer = JsonGraphSerializer.createJsonGraphSerializer()
        .setExpandVertexEdges(false);
    serializer.setUseCollectionSize(false).setUseCollectionSizeForEdges(false);

    final String query = "MATCH (nodeDOc:DOCUMENT)<-[rel:in]-(chunk:CHUNK) " +
                         "RETURN ID(nodeDOc) AS docId, COLLECT(rel) AS rels";

    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();

      while (rs.hasNext()) {
        final Result row = rs.next();
        final JSONObject json = serializer.serializeResult(database, row);

        System.out.println("JSON for document " + json.get("docId") + ": " + json);

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
