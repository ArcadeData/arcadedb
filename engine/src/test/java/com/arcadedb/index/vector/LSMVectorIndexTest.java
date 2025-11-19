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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for LSMVectorIndex using JVector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexTest extends TestHelper {

  @Test
  public void testCreateIndexViaSQLAndQuery() {
    database.transaction(() -> {
      // Create the schema
      database.command("sql", "CREATE VERTEX TYPE VectorDocument IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorDocument.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorDocument.title IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorDocument.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY VectorDocument.category IF NOT EXISTS STRING");

      // Create the LSM_VECTOR index
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON VectorDocument (embedding) LSM_VECTOR " +
          "METADATA {" +
          "  \"dimensions\" : 5," +
          "  \"similarity\" : \"COSINE\"," +
          "  \"maxConnections\" : 16," +
          "  \"beamWidth\" : 100" +
          "}");
    });

    // Verify the index was created
    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("VectorDocument[embedding]");
    Assertions.assertNotNull(typeIndex, "Index should be created");

    // Get one of the underlying LSMVectorIndex instances to verify configuration
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    Assertions.assertEquals(5, index.getDimensions(), "Dimensions should be 5");
    Assertions.assertEquals("COSINE", index.getSimilarityFunction().name(), "Similarity should be COSINE");

    // Insert test data
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var doc = database.newVertex("VectorDocument");
        doc.set("id", "doc" + i);
        doc.set("title", "Document " + i);

        // Create a random 5-dimensional vector
        final float[] vector = new float[5];
        for (int j = 0; j < 5; j++)
          vector[j] = (float) Math.random();

        doc.set("embedding", vector);
        doc.set("category", "category" + (i % 3));
        doc.save();
      }
    });

    // Query the index using TypeIndex
    database.transaction(() -> {
      final float[] queryVector = {0.5f, 0.5f, 0.5f, 0.5f, 0.5f};
      final IndexCursor cursor = typeIndex.get(new Object[]{queryVector}, 10);

      int count = 0;
      while (cursor.hasNext()) {
        Assertions.assertNotNull(cursor.next());
        count++;
      }

      Assertions.assertTrue(count > 0, "Should find at least one result");
      Assertions.assertTrue(count <= 10, "Should return at most 10 results");
    });
  }

  @Test
  public void testCreateIndexProgrammatically() {
    database.transaction(() -> {
      // Create document type
      final DocumentType docType = database.getSchema().createDocumentType("VectorDoc");
      docType.createProperty("id", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Create LSM_VECTOR index programmatically
      database.getSchema()
          .buildLSMVectorIndex("VectorDoc", new String[]{"embedding"})
          .withIndexName("VectorDoc_embedding_idx")
          .withDimensions(3)
          .withSimilarity("EUCLIDEAN")
          .withMaxConnections(8)
          .withBeamWidth(50)
          .create();
    });

    // Verify index was created
    // Note: TypeIndex is created with default name based on type and properties
    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("VectorDoc[embedding]");
    Assertions.assertNotNull(typeIndex);

    // Get one of the underlying LSMVectorIndex instances to verify configuration
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    Assertions.assertEquals(3, index.getDimensions());
    Assertions.assertEquals("EUCLIDEAN", index.getSimilarityFunction().name());
    Assertions.assertEquals(8, index.getMaxConnections());
    Assertions.assertEquals(50, index.getBeamWidth());

    // Insert and query
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var doc = database.newDocument("VectorDoc");
        doc.set("id", "doc" + i);
        doc.set("embedding", new float[]{(float) i, (float) i + 1, (float) i + 2});
        doc.save();
      }
    });

    database.transaction(() -> {
      final float[] queryVector = {5.0f, 6.0f, 7.0f};
      final IndexCursor cursor = typeIndex.get(new Object[]{queryVector}, 5);

      int count = 0;
      while (cursor.hasNext()) {
        count++;
        cursor.next();
      }

      Assertions.assertTrue(count > 0);
      Assertions.assertTrue(count <= 5);
    });
  }

  @Test
  public void testIndexEntryCount() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestDoc");
      database.command("sql", "CREATE PROPERTY TestDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON TestDoc (vec) LSM_VECTOR " +
          "METADATA {\"dimensions\": 2, \"similarity\": \"DOT_PRODUCT\", \"maxConnections\": 4, \"beamWidth\": 10}");
    });

    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final var doc = database.newDocument("TestDoc");
        doc.set("vec", new float[]{(float) i, (float) i * 2});
        doc.save();
      }
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("TestDoc[vec]");
    Assertions.assertEquals(50, typeIndex.countEntries());
  }

  @Test
  public void testTransactionalIsolation() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TxDoc");
      database.command("sql", "CREATE PROPERTY TxDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON TxDoc (vec) LSM_VECTOR " +
          "METADATA {\"dimensions\": 2, \"similarity\": \"COSINE\", \"maxConnections\": 4, \"beamWidth\": 10}");
    });

    // Insert in transaction 1
    database.transaction(() -> {
      final var doc = database.newDocument("TxDoc");
      doc.set("vec", new float[]{1.0f, 2.0f});
      doc.save();
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("TxDoc[vec]");
    Assertions.assertEquals(1, typeIndex.countEntries());

    // Verify transaction rollback doesn't affect committed data
    try {
      database.transaction(() -> {
        final var doc = database.newDocument("TxDoc");
        doc.set("vec", new float[]{3.0f, 4.0f});
        doc.save();

        // Force rollback
        throw new RuntimeException("Test rollback");
      });
    } catch (final RuntimeException e) {
      // Expected
    }

    // Should still have only 1 entry
    Assertions.assertEquals(1, typeIndex.countEntries());
  }
}
