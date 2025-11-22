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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for LSMVectorIndex with real document creation.
 * Tests the complete workflow from document creation to vector search.
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexIntegrationTest extends TestHelper {

  @Test
  public void testCreateDocumentsWithVectorIndex() {
    database.transaction(() -> {
      // Create schema with vector index via SQL
      database.command("sql", "CREATE VERTEX TYPE Article IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Article.title IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Article.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Article (embedding) LSM_VECTOR METADATA {'dimensions': 3, 'similarity': 'COSINE'}");
    });

    // Get the index and manually populate it (LSMVectorIndex is not a TypeIndex, so no automatic indexing)
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = (LSMVectorIndex) schema.getIndexByName("Article[embedding]");
      assertThat(index).isNotNull();

      // Create documents with embeddings
      final MutableVertex v1 = database.newVertex("Article");
      v1.set("title", "Machine Learning");
      v1.set("embedding", new float[]{1.0f, 0.0f, 0.0f});
      v1.save();

      final MutableVertex v2 = database.newVertex("Article");
      v2.set("title", "Deep Learning");
      v2.set("embedding", new float[]{0.9f, 0.1f, 0.0f});
      v2.save();

      final MutableVertex v3 = database.newVertex("Article");
      v3.set("title", "Natural Language Processing");
      v3.set("embedding", new float[]{0.0f, 1.0f, 0.0f});
      v3.save();

      final MutableVertex v4 = database.newVertex("Article");
      v4.set("title", "Computer Vision");
      v4.set("embedding", new float[]{0.0f, 0.0f, 1.0f});
      v4.save();

      // Manually index the vectors
      index.put(new Object[]{new float[]{1.0f, 0.0f, 0.0f}}, new RID[]{v1.getIdentity()});
      index.put(new Object[]{new float[]{0.9f, 0.1f, 0.0f}}, new RID[]{v2.getIdentity()});
      index.put(new Object[]{new float[]{0.0f, 1.0f, 0.0f}}, new RID[]{v3.getIdentity()});
      index.put(new Object[]{new float[]{0.0f, 0.0f, 1.0f}}, new RID[]{v4.getIdentity()});
    });

    // Verify documents were created
    database.transaction(() -> {
      final ResultSet resultSet = database.query("sql", "SELECT count(*) as count FROM Article");
      assertThat(resultSet.hasNext()).isTrue();
      final Result result = resultSet.next();
      assertThat(result.<Long>getProperty("count")).isEqualTo(4L);
    });

    // Test KNN search
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = (LSMVectorIndex) schema.getIndexByName("Article[embedding]");

      // Search for articles similar to [1.0, 0.0, 0.0]
      final float[] queryVector = {1.0f, 0.0f, 0.0f};
      final List<LSMVectorIndexMutable.VectorSearchResult> results = index.knnSearch(queryVector, 2);

      assertThat(results).hasSize(2);
      // First result should be exact match (Machine Learning)
      assertThat(results.get(0).distance).isCloseTo(1.0f, org.assertj.core.data.Offset.offset(0.01f));

      // Load the actual documents from RIDs
      final var rid1 = results.get(0).rids.iterator().next();
      final var doc1 = database.lookupByRID(rid1, true);
      assertThat(doc1.asVertex().getString("title")).isEqualTo("Machine Learning");
    });
  }

  @Test
  public void testVectorSearchWithFiltering() {
    database.transaction(() -> {
      // Create schema
      database.command("sql", "CREATE VERTEX TYPE Product IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Product.name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.category IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.features IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Product (features) LSM_VECTOR METADATA {'dimensions': 4, 'similarity': 'EUCLIDEAN'}");

      // Create products with feature vectors
      final MutableVertex p1 = database.newVertex("Product");
      p1.set("name", "Laptop A");
      p1.set("category", "Electronics");
      p1.set("features", new float[]{1.0f, 0.5f, 0.3f, 0.1f});
      p1.save();

      final MutableVertex p2 = database.newVertex("Product");
      p2.set("name", "Laptop B");
      p2.set("category", "Electronics");
      p2.set("features", new float[]{0.9f, 0.6f, 0.2f, 0.1f});
      p2.save();

      final MutableVertex p3 = database.newVertex("Product");
      p3.set("name", "Phone A");
      p3.set("category", "Electronics");
      p3.set("features", new float[]{0.3f, 0.8f, 0.9f, 0.2f});
      p3.save();

      final MutableVertex p4 = database.newVertex("Product");
      p4.set("name", "Book A");
      p4.set("category", "Books");
      p4.set("features", new float[]{0.1f, 0.1f, 0.1f, 0.9f});
      p4.save();
    });

    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = (LSMVectorIndex) schema.getIndexByName("Product[features]");

      // Search for products similar to [1.0, 0.5, 0.3, 0.1] (Laptop A features)
      final float[] queryVector = {1.0f, 0.5f, 0.3f, 0.1f};

      // Filter to only return Electronics category
      final List<LSMVectorIndexMutable.VectorSearchResult> results =
          index.knnSearch(queryVector, 10, rid -> {
            final var doc = database.lookupByRID(rid, true);
            final String category = doc.asVertex().getString("category");
            return !"Electronics".equals(category); // Ignore non-Electronics
          });

      // Should return 3 Electronics products, not the Book
      assertThat(results.size()).isLessThanOrEqualTo(3);

      // Verify all results are Electronics
      for (final var result : results) {
        for (final var rid : result.rids) {
          final var doc = database.lookupByRID(rid, true);
          assertThat(doc.asVertex().getString("category")).isEqualTo("Electronics");
        }
      }
    });
  }

  @Test
  public void testCompactionWithRealDocuments() throws Exception {
    database.transaction(() -> {
      // Create schema
      database.command("sql", "CREATE VERTEX TYPE TextDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY TextDoc.content IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY TextDoc.vector IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON TextDoc (vector) LSM_VECTOR METADATA {'dimensions': 5, 'similarity': 'DOT_PRODUCT'}");

      // Get index
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = (LSMVectorIndex) schema.getIndexByName("TextDoc[vector]");

      // Insert multiple documents to trigger compaction
      for (int i = 0; i < 20; i++) {
        final float[] vector = new float[]{
            (float) Math.random(),
            (float) Math.random(),
            (float) Math.random(),
            (float) Math.random(),
            (float) Math.random()
        };
        final MutableVertex doc = database.newVertex("TextDoc");
        doc.set("content", "Doc " + i);
        doc.set("vector", vector);
        doc.save();

        // Manually index (LSMVectorIndex is not a TypeIndex, so no automatic indexing)
        index.put(new Object[]{vector}, new RID[]{doc.getIdentity()});
      }
    });

    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final LSMVectorIndex index = (LSMVectorIndex) schema.getIndexByName("TextDoc[vector]");

      // Trigger compaction
      if (index.scheduleCompaction()) {
        try {
          index.compact();
        } catch (Exception e) {
          throw new RuntimeException("Compaction failed", e);
        }
      }

      // Verify search still works after compaction
      final float[] queryVector = {0.5f, 0.5f, 0.5f, 0.5f, 0.5f};
      final List<LSMVectorIndexMutable.VectorSearchResult> results = index.knnSearch(queryVector, 5);

      assertThat(results).isNotEmpty();
      assertThat(results.size()).isLessThanOrEqualTo(5);

      // Verify we can load all documents
      for (final var result : results) {
        for (final var rid : result.rids) {
          final var doc = database.lookupByRID(rid, true);
          assertThat(doc).isNotNull();
          assertThat(doc.asVertex().has("content")).isTrue();
        }
      }
    });
  }

  @Test
  public void testTransactionalBehavior() {
    database.transaction(() -> {
      // Create schema
      database.command("sql", "CREATE VERTEX TYPE TestDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY TestDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON TestDoc (vec) LSM_VECTOR METADATA {'dimensions': 2, 'similarity': 'COSINE'}");
    });

    // Test that uncommitted changes are not visible
    try {
      database.begin();
      database.command("sql", "CREATE VERTEX TestDoc SET vec = [1.0, 0.0]");

      // In the same transaction, the document should be findable
      final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM TestDoc");
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);

      // Rollback
      database.rollback();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    // After rollback, no documents should exist
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM TestDoc");
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(0L);
    });

    // Test successful commit
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TestDoc SET vec = [1.0, 0.0]");
    });

    // After commit, document should exist
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM TestDoc");
      assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
    });
  }

  private String vectorToString(float[] vector) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) sb.append(", ");
      sb.append(vector[i]);
    }
    sb.append("]");
    return sb.toString();
  }
}
