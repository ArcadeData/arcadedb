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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for GitHub issue #3218.
 * Tests memory explosion issue with multiple OPTIONAL MATCH clauses and collect(DISTINCT ...).
 * <p>
 * The problem: Multiple OPTIONAL MATCH clauses create Cartesian products of matched results
 * before aggregation, causing massive intermediate result sets that consume gigabytes of RAM.
 * <p>
 * Example query pattern:
 * MATCH (searchedChunk:CHUNK) WHERE ID(searchedChunk) IN $_ids
 * MATCH (sourceDoc:DOCUMENT)<-[chunkDocRel:in]-(searchedChunk:CHUNK)
 * OPTIONAL MATCH (searchedChunk:CHUNK)<-[chunkNerOneRel:in]-(nerOne:NER)
 * OPTIONAL MATCH (nerOne:NER)-[nerOneNerTwoRel]->(nerTwo:NER)-[chunkNerTwoRel:in]->(searchedChunk:CHUNK)
 * OPTIONAL MATCH (searchedChunk:CHUNK)<-[themeToChunkRel:topic]-(theme:THEME)
 * RETURN collect(DISTINCT searchedChunk) AS searchedChunks, ...
 * <p>
 * If searchedChunk has:
 * - 1000 NER relationships (nerOne)
 * - Each nerOne has 100 relationships to nerTwo
 * - 10 THEME relationships
 * Then the Cartesian product before aggregation is: 1 * 1000 * 100000 * 10 = 1 billion rows!
 */
class Issue3218Test {
  private Database database;
  private String chunkId;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3218").create();
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("NER");
    database.getSchema().createVertexType("THEME");
    database.getSchema().createEdgeType("in");
    database.getSchema().createEdgeType("topic");
    database.getSchema().createEdgeType("related");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Test the problematic query pattern from issue #3218.
   * This creates a graph structure that demonstrates Cartesian product with OPTIONAL MATCH.
   * Focus is on verifying collect(DISTINCT ...) works correctly.
   */
  @Test
  void optionalMatchCartesianExplosion() {
    // Create a simpler graph structure that clearly demonstrates the feature
    database.transaction(() -> {
      // Create one CHUNK vertex
      MutableVertex chunk = database.newVertex("CHUNK");
      chunk.set("name", "chunk1");
      chunk.save();
      chunkId = chunk.getIdentity().toString();

      // Create one DOCUMENT and connect to CHUNK
      MutableVertex doc = database.newVertex("DOCUMENT");
      doc.set("name", "doc1");
      doc.save();
      chunk.newEdge("in", doc, true, (Object[]) null);

      // Create multiple NER vertices connected to the CHUNK
      for (int i = 0; i < 10; i++) {
        MutableVertex ner = database.newVertex("NER");
        ner.set("name", "ner_" + i);
        ner.save();
        ner.newEdge("in", chunk, true, (Object[]) null);
      }

      // Create THEME vertices connected to CHUNK
      for (int i = 0; i < 5; i++) {
        MutableVertex theme = database.newVertex("THEME");
        theme.set("name", "theme_" + i);
        theme.save();
        theme.newEdge("topic", chunk, true, (Object[]) null);
      }
    });

    long startTime = System.currentTimeMillis();
    long startMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

    database.transaction(() -> {
      // Query with multiple OPTIONAL MATCH - this creates Cartesian product
      // Without DISTINCT: 10 NER × 5 THEME = 50 intermediate rows
      // With collect(DISTINCT ...): should get 1 chunk, 1 doc, 10 NERs, 5 themes
      // Note: Labels can now be repeated on bound variables (bug fix for label filtering)
      final ResultSet rs = database.command("opencypher",
          """
          MATCH (searchedChunk:CHUNK) WHERE ID(searchedChunk) IN $_ids \
          MATCH (sourceDoc:DOCUMENT)<-[chunkDocRel:in]-(searchedChunk:CHUNK) \
          OPTIONAL MATCH (searchedChunk:CHUNK)<-[chunkNerOneRel:in]-(nerOne:NER) \
          OPTIONAL MATCH (searchedChunk:CHUNK)<-[themeToChunkRel:topic]-(theme:THEME) \
          RETURN \
            collect(DISTINCT searchedChunk) AS searchedChunks, \
            collect(DISTINCT sourceDoc) AS sourceDocs, \
            collect(DISTINCT nerOne) AS nerOnes, \
            collect(DISTINCT theme) AS themes""",
          Map.of("_ids", List.of(chunkId)));

      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();

      // Verify we got the correct distinct counts
      List<?> searchedChunks = (List<?>) result.getProperty("searchedChunks");
      List<?> sourceDocs = (List<?>) result.getProperty("sourceDocs");
      List<?> nerOnes = (List<?>) result.getProperty("nerOnes");
      List<?> themes = (List<?>) result.getProperty("themes");

      assertThat(searchedChunks).hasSize(1);
      assertThat(sourceDocs).hasSize(1);
      assertThat(nerOnes).hasSize(10);
      assertThat(themes).hasSize(5);

      assertThat(rs.hasNext()).isFalse();
    });

    long duration = System.currentTimeMillis() - startTime;
    long endMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long memoryUsed = endMemory - startMemory;

    //System.out.println("Query execution time: " + duration + "ms");
    //System.out.println("Memory used: " + (memoryUsed / 1024 / 1024) + "MB");

    // The query should complete in reasonable time and memory
    assertThat(duration)
        .as("Query should not take excessive time due to Cartesian product")
        .isLessThan(5000); // 5 seconds max
  }

  /**
   * Test a simpler case with just one OPTIONAL MATCH to verify basic functionality.
   */
  @Test
  void singleOptionalMatch() {
    database.transaction(() -> {
      MutableVertex chunk = database.newVertex("CHUNK");
      chunk.set("name", "chunk1");
      chunk.save();
      chunkId = chunk.getIdentity().toString();

      MutableVertex doc = database.newVertex("DOCUMENT");
      doc.set("name", "doc1");
      doc.save();
      chunk.newEdge("in", doc, true, (Object[]) null);

      for (int i = 0; i < 10; i++) {
        MutableVertex ner = database.newVertex("NER");
        ner.set("name", "ner_" + i);
        ner.save();
        ner.newEdge("in", chunk, true, (Object[]) null);
      }
    });

    database.transaction(() -> {
      // Labels can now be repeated on bound variables (bug fix for label filtering)
      final ResultSet rs = database.command("opencypher",
          """
          MATCH (searchedChunk:CHUNK) WHERE ID(searchedChunk) = $_id \
          MATCH (sourceDoc:DOCUMENT)<-[chunkDocRel:in]-(searchedChunk:CHUNK) \
          OPTIONAL MATCH (searchedChunk:CHUNK)<-[chunkNerOneRel:in]-(nerOne:NER) \
          RETURN \
            collect(DISTINCT searchedChunk) AS searchedChunks, \
            collect(DISTINCT sourceDoc) AS sourceDocs, \
            collect(DISTINCT nerOne) AS nerOnes""",
          Map.of("_id", chunkId));

      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();

      List<?> searchedChunks = (List<?>) result.getProperty("searchedChunks");
      List<?> sourceDocs = (List<?>) result.getProperty("sourceDocs");
      List<?> nerOnes = (List<?>) result.getProperty("nerOnes");

      assertThat(searchedChunks).hasSize(1);
      assertThat(sourceDocs).hasSize(1);
      assertThat(nerOnes).hasSize(10);
    });
  }

  /**
   * Test to demonstrate the Cartesian product size without DISTINCT.
   * This shows how many intermediate rows are created.
   */
  @Test
  void cartesianProductSize() {
    database.transaction(() -> {
      MutableVertex chunk = database.newVertex("CHUNK");
      chunk.set("name", "chunk1");
      chunk.save();
      chunkId = chunk.getIdentity().toString();

      MutableVertex doc = database.newVertex("DOCUMENT");
      doc.set("name", "doc1");
      doc.save();
      chunk.newEdge("in", doc, true, (Object[]) null);

      // Create 5 NER vertices
      for (int i = 0; i < 5; i++) {
        MutableVertex ner = database.newVertex("NER");
        ner.set("name", "ner_" + i);
        ner.save();
        ner.newEdge("in", chunk, true, (Object[]) null);
      }

      // Create 3 THEME vertices
      for (int i = 0; i < 3; i++) {
        MutableVertex theme = database.newVertex("THEME");
        theme.set("name", "theme_" + i);
        theme.save();
        theme.newEdge("topic", chunk, true, (Object[]) null);
      }
    });

    database.transaction(() -> {
      // Without DISTINCT, we should see the Cartesian product: 5 * 3 = 15 rows
      // Labels can now be repeated on bound variables (bug fix for label filtering)
      final ResultSet rs = database.query("opencypher",
          """
          MATCH (searchedChunk:CHUNK) WHERE ID(searchedChunk) = $_id \
          MATCH (sourceDoc:DOCUMENT)<-[chunkDocRel:in]-(searchedChunk:CHUNK) \
          OPTIONAL MATCH (searchedChunk:CHUNK)<-[chunkNerOneRel:in]-(nerOne:NER) \
          OPTIONAL MATCH (searchedChunk:CHUNK)<-[themeToChunkRel:topic]-(theme:THEME) \
          RETURN count(*) as rowCount""",
          Map.of("_id", chunkId));

      assertThat(rs.hasNext()).isTrue();
      Result result = rs.next();
      long rowCount = ((Number) result.getProperty("rowCount")).longValue();

      //System.out.println("Cartesian product size: " + rowCount + " rows");

      // Expected: 1 (chunk) * 1 (doc) * 5 (ners) * 3 (themes) = 15 rows
      assertThat(rowCount).isEqualTo(15);
    });
  }
}
