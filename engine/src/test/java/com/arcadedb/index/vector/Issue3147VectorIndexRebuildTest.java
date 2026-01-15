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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3147: REBUILD INDEX fails for vector indexes because it recreates them without vector metadata.
 * <p>
 * The bug causes vector indexes to be recreated with dimensions=0 after REBUILD INDEX,
 * losing all vector-specific configuration (dimensions, similarity, maxConnections, beamWidth, etc.).
 */
class Issue3147VectorIndexRebuildTest {
  private static final String DB_PATH = "databases/test-issue-3147-vector-rebuild";
  private static final int    DIMENSIONS = 128;
  private static final int    MAX_CONNECTIONS = 32;
  private static final int    BEAM_WIDTH = 200;

  private DatabaseFactory factory;

  @BeforeEach
  void setup() {
    factory = new DatabaseFactory(DB_PATH);
    if (factory.exists()) {
      factory.open().drop();
    }
  }

  @AfterEach
  void cleanup() {
    if (factory.exists()) {
      factory.open().drop();
    }
  }

  @Test
  void rebuildIndexPreservesVectorMetadata() {
    // Create database with vector index having custom metadata
    try (Database database = factory.create()) {
      // Create type
      DocumentType type = database.getSchema().createDocumentType("Embedding");
      type.createProperty("name", String.class);
      type.createProperty("vector", float[].class);

      // Create vector index with custom metadata
      database.command("sql",
          "CREATE INDEX ON Embedding (vector) LSM_VECTOR METADATA " +
              "{dimensions: " + DIMENSIONS + ", similarity: 'DOT_PRODUCT', " +
              "maxConnections: " + MAX_CONNECTIONS + ", beamWidth: " + BEAM_WIDTH + ", " +
              "idPropertyName: 'name'}");

      // Add test data
      database.begin();
      for (int i = 0; i < 20; i++) {
        float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++) {
          vector[j] = (float) Math.random();
        }
        database.newDocument("Embedding")
            .set("name", "embedding" + i)
            .set("vector", vector)
            .save();
      }
      database.commit();

      // Verify index exists with correct metadata before rebuild
      Index index = database.getSchema().getIndexByName("Embedding[vector]");
      assertThat(index).as("Index should exist").isNotNull();
      assertThat(index.getType().toString()).isEqualTo("LSM_VECTOR");

      LSMVectorIndex vectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
          .filter(i -> i instanceof LSMVectorIndex)
          .findFirst()
          .orElseThrow();

      LSMVectorIndexMetadata metadataBefore = vectorIndex.getMetadata();
      assertThat(metadataBefore.dimensions).isEqualTo(DIMENSIONS);
      assertThat(metadataBefore.similarityFunction).isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
      assertThat(metadataBefore.maxConnections).isEqualTo(MAX_CONNECTIONS);
      assertThat(metadataBefore.beamWidth).isEqualTo(BEAM_WIDTH);

      // Execute REBUILD INDEX
      database.command("sql", "REBUILD INDEX `Embedding[vector]`");

      // Verify index still exists with same metadata after rebuild
      Index rebuiltTypeIndex = database.getSchema().getIndexByName("Embedding[vector]");
      assertThat(rebuiltTypeIndex).as("Index should exist after rebuild").isNotNull();
      assertThat(rebuiltTypeIndex.getType().toString()).isEqualTo("LSM_VECTOR");

      // Get the underlying bucket index to check metadata
      LSMVectorIndex rebuiltVectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
          .filter(i -> i instanceof LSMVectorIndex)
          .findFirst()
          .orElseThrow();

      LSMVectorIndexMetadata metadataAfter = rebuiltVectorIndex.getMetadata();

      // These assertions will fail due to bug #3147 - metadata is lost during rebuild
      assertThat(metadataAfter.dimensions)
          .as("Dimensions should be preserved after rebuild")
          .isEqualTo(DIMENSIONS);
      assertThat(metadataAfter.similarityFunction)
          .as("Similarity function should be preserved after rebuild")
          .isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
      assertThat(metadataAfter.maxConnections)
          .as("Max connections should be preserved after rebuild")
          .isEqualTo(MAX_CONNECTIONS);
      assertThat(metadataAfter.beamWidth)
          .as("Beam width should be preserved after rebuild")
          .isEqualTo(BEAM_WIDTH);
      assertThat(metadataAfter.idPropertyName)
          .as("ID property name should be preserved after rebuild")
          .isEqualTo("name");

      // Verify index is functional after rebuild
      assertThat(rebuiltTypeIndex.countEntries())
          .as("Index should have all entries after rebuild")
          .isEqualTo(20);

      // Verify vector search still works
      float[] queryVector = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++) {
        queryVector[i] = (float) Math.random();
      }

      IndexCursor cursor = rebuiltVectorIndex.get(new Object[] { queryVector }, 5);
      int resultCount = 0;
      while (cursor.hasNext()) {
        cursor.next();
        resultCount++;
      }
      assertThat(resultCount).as("Vector search should return results after rebuild").isGreaterThan(0);
    }
  }

  @Test
  void rebuildIndexPreservesQuantizationSettings() {
    // Test that quantization settings are preserved during rebuild
    try (Database database = factory.create()) {
      // Create type
      DocumentType type = database.getSchema().createDocumentType("QuantizedEmbedding");
      type.createProperty("name", String.class);
      type.createProperty("vector", float[].class);

      // Create vector index with INT8 quantization
      database.command("sql",
          "CREATE INDEX ON QuantizedEmbedding (vector) LSM_VECTOR METADATA " +
              "{dimensions: 64, similarity: 'EUCLIDEAN', quantization: 'INT8', " +
              "maxConnections: 24, beamWidth: 150}");

      // Add test data
      database.begin();
      for (int i = 0; i < 10; i++) {
        float[] vector = new float[64];
        for (int j = 0; j < 64; j++) {
          vector[j] = (float) Math.random();
        }
        database.newDocument("QuantizedEmbedding")
            .set("name", "qembed" + i)
            .set("vector", vector)
            .save();
      }
      database.commit();

      // Get metadata before rebuild
      LSMVectorIndex vectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
          .filter(i -> i instanceof LSMVectorIndex)
          .findFirst()
          .orElseThrow();

      LSMVectorIndexMetadata metadataBefore = vectorIndex.getMetadata();
      assertThat(metadataBefore.quantizationType).isEqualTo(VectorQuantizationType.INT8);

      // Execute REBUILD INDEX
      database.command("sql", "REBUILD INDEX `QuantizedEmbedding[vector]`");

      // Verify quantization is preserved after rebuild
      LSMVectorIndex rebuiltVectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
          .filter(i -> i instanceof LSMVectorIndex)
          .findFirst()
          .orElseThrow();

      LSMVectorIndexMetadata metadataAfter = rebuiltVectorIndex.getMetadata();
      assertThat(metadataAfter.quantizationType)
          .as("Quantization type should be preserved after rebuild")
          .isEqualTo(VectorQuantizationType.INT8);
      assertThat(metadataAfter.similarityFunction)
          .as("Similarity function should be preserved after rebuild")
          .isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
    }
  }

  @Test
  void rebuildAllIndexesPreservesVectorMetadata() {
    // Test that REBUILD INDEX * also preserves vector metadata
    try (Database database = factory.create()) {
      // Create type
      DocumentType type = database.getSchema().createDocumentType("VectorDoc");
      type.createProperty("name", String.class);
      type.createProperty("vector", float[].class);

      // Create vector index
      database.command("sql",
          "CREATE INDEX ON VectorDoc (vector) LSM_VECTOR METADATA " +
              "{dimensions: 32, similarity: 'COSINE', maxConnections: 20, beamWidth: 80}");

      // Add test data
      database.begin();
      for (int i = 0; i < 5; i++) {
        float[] vector = new float[32];
        for (int j = 0; j < 32; j++) {
          vector[j] = (float) Math.random();
        }
        database.newDocument("VectorDoc")
            .set("name", "doc" + i)
            .set("vector", vector)
            .save();
      }
      database.commit();

      // Get metadata before rebuild
      LSMVectorIndex vectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
          .filter(i -> i instanceof LSMVectorIndex)
          .findFirst()
          .orElseThrow();

      assertThat(vectorIndex.getMetadata().dimensions).isEqualTo(32);

      // Execute REBUILD INDEX *
      database.command("sql", "REBUILD INDEX *");

      // Verify metadata is preserved after rebuild all
      LSMVectorIndex rebuiltVectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
          .filter(i -> i instanceof LSMVectorIndex)
          .findFirst()
          .orElseThrow();

      LSMVectorIndexMetadata metadataAfter = rebuiltVectorIndex.getMetadata();
      assertThat(metadataAfter.dimensions)
          .as("Dimensions should be preserved after REBUILD INDEX *")
          .isEqualTo(32);
      assertThat(metadataAfter.maxConnections)
          .as("Max connections should be preserved after REBUILD INDEX *")
          .isEqualTo(20);
      assertThat(metadataAfter.beamWidth)
          .as("Beam width should be preserved after REBUILD INDEX *")
          .isEqualTo(80);
    }
  }
}
