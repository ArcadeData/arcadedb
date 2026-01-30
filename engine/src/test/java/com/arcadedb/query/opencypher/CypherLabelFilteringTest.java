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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for correct label filtering in Cypher MATCH and OPTIONAL MATCH clauses.
 * Specifically tests that:
 * 1. Target vertices in relationship patterns are filtered by label
 * 2. Already-bound variables with labels in subsequent MATCH clauses work correctly
 * 3. The query from GitHub issue (multiple OPTIONAL MATCH with labels) returns correct data
 */
class CypherLabelFilteringTest {
  private Database database;
  private String chunkId;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-label-filtering").create();
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("NER");
    database.getSchema().createVertexType("THEME");
    database.getSchema().createEdgeType("in");
    database.getSchema().createEdgeType("topic");
    database.getSchema().createEdgeType("related");

    database.transaction(() -> {
      // Create graph: CHUNK <- in - DOCUMENT
      //               CHUNK <- in - NER (x3)
      //               CHUNK <- topic - THEME (x2)
      //               NER -> related -> NER (one connection between nerOne and nerTwo)
      MutableVertex chunk = database.newVertex("CHUNK");
      chunk.set("name", "chunk1");
      chunk.save();
      chunkId = chunk.getIdentity().toString();

      MutableVertex doc = database.newVertex("DOCUMENT");
      doc.set("name", "doc1");
      doc.save();
      chunk.newEdge("in", doc, true, (Object[]) null);

      MutableVertex ner1 = database.newVertex("NER");
      ner1.set("name", "ner_1");
      ner1.save();
      ner1.newEdge("in", chunk, true, (Object[]) null);

      MutableVertex ner2 = database.newVertex("NER");
      ner2.set("name", "ner_2");
      ner2.save();
      ner2.newEdge("in", chunk, true, (Object[]) null);

      MutableVertex ner3 = database.newVertex("NER");
      ner3.set("name", "ner_3");
      ner3.save();
      ner3.newEdge("in", chunk, true, (Object[]) null);

      // ner1 -> related -> ner2, and ner2 connects back to chunk via "in"
      ner1.newEdge("related", ner2, true, (Object[]) null);

      MutableVertex theme1 = database.newVertex("THEME");
      theme1.set("name", "theme_1");
      theme1.save();
      theme1.newEdge("topic", chunk, true, (Object[]) null);

      MutableVertex theme2 = database.newVertex("THEME");
      theme2.set("name", "theme_2");
      theme2.save();
      theme2.newEdge("topic", chunk, true, (Object[]) null);
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
   * Tests that target node label filtering works in MatchRelationshipStep.
   * The pattern (a:CHUNK)<-[r:in]-(b:NER) should only return NER vertices as b,
   * not DOCUMENT vertices (which are also connected via "in" edges).
   */
  @Test
  void targetNodeLabelFiltering() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (chunk:CHUNK) WHERE ID(chunk) = $_id " +
              "OPTIONAL MATCH (chunk:CHUNK)<-[r:in]-(target:NER) " +
              "RETURN chunk.name AS chunkName, collect(DISTINCT target) AS targets",
          Map.of("_id", chunkId));

      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();

      assertThat(result.<String>getProperty("chunkName")).isEqualTo("chunk1");
      List<?> targets = (List<?>) result.getProperty("targets");
      // Should only get NER vertices (3), not DOCUMENT vertices
      assertThat(targets.size()).as("Expected 3 NER targets, got " + targets.size()).isEqualTo(3);

      // Verify all targets are NER type
      for (Object target : targets) {
        assertThat(target).isInstanceOf(Vertex.class);
        assertThat(((Vertex) target).getTypeName()).as("Target should be NER, not " + ((Vertex) target).getTypeName()).isEqualTo("NER");
      }

      assertThat(rs.hasNext()).isFalse();
    });
  }

  /**
   * Tests that already-bound variables with labels in subsequent MATCH clauses
   * don't cause Cartesian products or incorrect data.
   * The variable searchedChunk is bound in the first MATCH, and reused with :CHUNK label
   * in the second MATCH - should use the already-bound value.
   */
  @Test
  void boundVariableWithLabelInSubsequentMatch() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (searchedChunk:CHUNK) WHERE ID(searchedChunk) = $_id " +
              "MATCH (sourceDoc:DOCUMENT)<-[chunkDocRel:in]-(searchedChunk:CHUNK) " +
              "RETURN searchedChunk.name AS chunkName, sourceDoc.name AS docName",
          Map.of("_id", chunkId));

      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();

      assertThat(result.<String>getProperty("chunkName")).isEqualTo("chunk1");
      assertThat(result.<String>getProperty("docName")).isEqualTo("doc1");

      // Should be exactly 1 result, not a Cartesian product
      assertThat(rs.hasNext()).isFalse();
    });
  }

  /**
   * Tests the full query pattern from the user's bug report.
   * This is the exact query structure that was returning incorrect results:
   * - searchedChunks should only contain CHUNK vertices
   * - nerOnes should contain NER vertices
   * - nerTwos should contain the NER vertices connected from nerOne
   * - themes should contain THEME vertices
   */
  @Test
  void fullQueryWithLabelsOnBoundVariables() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (searchedChunk:CHUNK) WHERE ID(searchedChunk) IN $_ids " +
              "MATCH (sourceDoc:DOCUMENT)<-[chunkDocRel:in]-(searchedChunk:CHUNK) " +
              "OPTIONAL MATCH (searchedChunk:CHUNK)<-[chunkNerOneRel:in]-(nerOne:NER) " +
              "OPTIONAL MATCH (nerOne:NER)-[nerOneNerTwoRel:related]->(nerTwo:NER)-[chunkNerTwoRel:in]->(searchedChunk:CHUNK) " +
              "OPTIONAL MATCH (searchedChunk:CHUNK)<-[themeToChunkRel:topic]-(theme:THEME) " +
              "RETURN " +
              "  collect(DISTINCT searchedChunk) AS searchedChunks, " +
              "  collect(DISTINCT sourceDoc) AS sourceDocs, " +
              "  collect(DISTINCT nerOne) AS nerOnes, " +
              "  collect(DISTINCT nerTwo) AS nerTwos, " +
              "  collect(DISTINCT theme) AS themes",
          Map.of("_ids", List.of(chunkId)));

      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();

      List<?> searchedChunks = (List<?>) result.getProperty("searchedChunks");
      List<?> sourceDocs = (List<?>) result.getProperty("sourceDocs");
      List<?> nerOnes = (List<?>) result.getProperty("nerOnes");
      List<?> nerTwos = (List<?>) result.getProperty("nerTwos");
      List<?> themes = (List<?>) result.getProperty("themes");

      // searchedChunks should contain exactly 1 CHUNK vertex
      assertThat(searchedChunks.size()).as("Expected 1 CHUNK in searchedChunks").isEqualTo(1);
      assertThat(((Vertex) searchedChunks.get(0)).getTypeName()).isEqualTo("CHUNK");

      // sourceDocs should contain exactly 1 DOCUMENT vertex
      assertThat(sourceDocs.size()).as("Expected 1 DOCUMENT in sourceDocs").isEqualTo(1);

      // nerOnes should contain 3 NER vertices
      assertThat(nerOnes.size()).as("Expected 3 NER in nerOnes").isEqualTo(3);
      for (Object ner : nerOnes) {
        assertThat(((Vertex) ner).getTypeName()).isEqualTo("NER");
      }

      // nerTwos should contain 1 NER vertex (ner2, connected from ner1 via "related")
      assertThat(nerTwos.size()).as("Expected 1 NER in nerTwos").isEqualTo(1);
      assertThat(((Vertex) nerTwos.get(0)).getTypeName()).isEqualTo("NER");

      // themes should contain 2 THEME vertices
      assertThat(themes.size()).as("Expected 2 THEME in themes").isEqualTo(2);

      assertThat(rs.hasNext()).isFalse();
    });
  }

  /**
   * Tests that searchedChunks does NOT contain NER nodes.
   * This was the specific bug reported: searchedChunks contained NER nodes
   * because target label filtering was missing.
   */
  @Test
  void searchedChunksDoNotContainNERNodes() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (searchedChunk:CHUNK) WHERE ID(searchedChunk) IN $_ids " +
              "MATCH (sourceDoc:DOCUMENT)<-[chunkDocRel:in]-(searchedChunk:CHUNK) " +
              "OPTIONAL MATCH (searchedChunk:CHUNK)<-[chunkNerOneRel:in]-(nerOne:NER) " +
              "RETURN " +
              "  collect(DISTINCT searchedChunk) AS searchedChunks, " +
              "  collect(DISTINCT nerOne) AS nerOnes",
          Map.of("_ids", List.of(chunkId)));

      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();

      List<?> searchedChunks = (List<?>) result.getProperty("searchedChunks");
      List<?> nerOnes = (List<?>) result.getProperty("nerOnes");

      // searchedChunks must ONLY contain CHUNK vertices - never NER nodes
      assertThat(searchedChunks.size()).as("Expected 1 CHUNK in searchedChunks").isEqualTo(1);
      for (Object obj : searchedChunks) {
        assertThat(obj).isInstanceOf(Vertex.class);
        assertThat(((Vertex) obj).getTypeName()).as("searchedChunks should only contain CHUNK vertices, not " + ((Vertex) obj).getTypeName()).isEqualTo("CHUNK");
      }

      // nerOnes should contain exactly 3 NER vertices
      assertThat(nerOnes.size()).as("Expected 3 NER in nerOnes").isEqualTo(3);
    });
  }
}
