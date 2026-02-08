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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Extended tests for LSMVectorIndex to increase coverage.
 * Tests edge cases, error handling, and additional functionality.
 */
@Tag("slow")
class LSMVectorIndexExtendedTest extends TestHelper {

  private static final int DIMENSIONS = 64;

  @Test
  void shouldHandleEmptyIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorVertex IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 64,
            "similarity" : "COSINE"
          }""");
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("VectorVertex[embedding]");
    assertThat(typeIndex).isNotNull();

    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final float[] queryVector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++) {
      queryVector[i] = (float) Math.random();
    }

    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isEmpty();
  }

  @Test
  void shouldSupportMultipleSimilarityFunctions() {
    // Test EUCLIDEAN
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorVertex1 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex1.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex1 (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 32,
            "similarity" : "EUCLIDEAN"
          }""");
    });

    final TypeIndex idx1 = (TypeIndex) database.getSchema().getIndexByName("VectorVertex1[embedding]");
    final LSMVectorIndex index1 = (LSMVectorIndex) idx1.getIndexesOnBuckets()[0];
    assertThat(index1.getSimilarityFunction().name()).isEqualTo("EUCLIDEAN");

    // Test DOT_PRODUCT
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorVertex2 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex2.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex2 (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 32,
            "similarity" : "DOT_PRODUCT"
          }""");
    });

    final TypeIndex idx2 = (TypeIndex) database.getSchema().getIndexByName("VectorVertex2[embedding]");
    final LSMVectorIndex index2 = (LSMVectorIndex) idx2.getIndexesOnBuckets()[0];
    assertThat(index2.getSimilarityFunction().name()).isEqualTo("DOT_PRODUCT");
  }

  @Test
  void shouldRespectTopKLimit() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorVertex3 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex3.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex3 (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 16,
            "similarity" : "COSINE"
          }""");
    });

    // Insert 20 vectors
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final var doc = database.newVertex("VectorVertex3");
        final float[] vector = new float[16];
        for (int j = 0; j < 16; j++) {
          vector[j] = (float) (Math.random() * 2 - 1);
        }
        doc.set("embedding", vector);
        doc.save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("VectorVertex3[embedding]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final float[] queryVector = new float[16];
    for (int i = 0; i < 16; i++) {
      queryVector[i] = (float) Math.random();
    }

    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 5);
    assertThat(results.size()).isLessThanOrEqualTo(5);
  }

  @Test
  void shouldProvideMetadata() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorVertex4 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex4.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex4 (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 128,
            "similarity" : "COSINE",
            "maxConnections" : 24,
            "beamWidth" : 150
          }""");
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("VectorVertex4[embedding]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(index.getDimensions()).isEqualTo(128);
    assertThat(index.getSimilarityFunction()).isNotNull();
    assertThat(index.getMetadata()).isNotNull();
    assertThat(index.getMetadata().maxConnections).isEqualTo(24);
    assertThat(index.getMetadata().beamWidth).isEqualTo(150);
  }

  @Test
  void shouldValidateDimensions() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorVertex5 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex5.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex5 (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 32,
            "similarity" : "COSINE"
          }""");
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("VectorVertex5[embedding]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // Try to insert vector with wrong dimensions
    database.transaction(() -> {
      final var doc = database.newVertex("VectorVertex5");
      final float[] wrongVector = new float[16]; // Wrong: should be 32
      doc.set("embedding", wrongVector);

      assertThatThrownBy(doc::save)
          .hasMessageContaining("dimension");
    });
  }

  @Test
  void shouldHandleApproximateSearch() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE VectorVertex6 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex6.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex6 (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 32,
            "similarity" : "COSINE"
          }""");
    });

    // Insert vectors
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var doc = database.newVertex("VectorVertex6");
        final float[] vector = new float[32];
        for (int j = 0; j < 32; j++) {
          vector[j] = (float) Math.random();
        }
        doc.set("embedding", vector);
        doc.save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("VectorVertex6[embedding]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final float[] queryVector = new float[32];
    for (int i = 0; i < 32; i++) {
      queryVector[i] = (float) Math.random();
    }

    // Test approximate search
    final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(queryVector, 5);
    assertThat(results.size()).isLessThanOrEqualTo(5);
  }
}
