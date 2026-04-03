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
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for PR #3775 / issue #3722: ensures that the fallback document scan during
 * vector graph rebuild is scoped to the specific bucket associated with the vector
 * index, not the entire type. In a multi-bucket scenario, scanning all buckets
 * would inflate the document count and incorrectly trigger the fallback, potentially
 * adding documents from unrelated buckets into the vector index.
 *
 * @author lekmaneb (contributor)
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3722VectorFallbackBucketScopeTest extends TestHelper {

  private static final int DIMENSIONS = 32;
  private static final int VECTORS_PER_BUCKET = 50;

  @Test
  void fallbackScanShouldBeScopedToAssociatedBucket() {
    // Create type with two buckets so we can verify bucket-scoped behavior
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("MultiBucketVector");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      // Add a second bucket to the type
      final Bucket extraBucket = database.getSchema().createBucket("MultiBucketVector_extra");
      type.addBucket(extraBucket);

      // Create vector index (will create one sub-index per bucket)
      database.getSchema().buildTypeIndex("MultiBucketVector", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Insert vectors - distribute across both buckets
    database.transaction(() -> {
      final List<Bucket> buckets = database.getSchema().getType("MultiBucketVector").getBuckets(false);
      assertThat(buckets.size()).isGreaterThanOrEqualTo(2);

      for (int i = 0; i < VECTORS_PER_BUCKET * 2; i++) {
        final var vertex = database.newVertex("MultiBucketVector");
        vertex.set("name", "vec" + i);
        vertex.set("vector", createDeterministicVector(i));
        vertex.save();
      }
    });

    // Build vector graph
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("MultiBucketVector[vector]");
    assertThat(typeIndex.getSubIndexes().size()).isGreaterThanOrEqualTo(2);

    // Force build on each sub-index
    for (final var subIndex : typeIndex.getSubIndexes()) {
      final LSMVectorIndex vectorIndex = (LSMVectorIndex) subIndex;
      vectorIndex.buildVectorGraphNow();

      // Verify associated bucket ID is set
      assertThat(vectorIndex.getAssociatedBucketId()).isNotEqualTo(-1);

      final Map<String, Long> stats = vectorIndex.getStats();
      final long totalVectors = stats.get("totalVectors");

      // Each sub-index should only have vectors from its associated bucket,
      // not from all buckets of the type. With the old code using countType/scanType,
      // the document count would be inflated and vectors from wrong buckets could leak in.
      final Bucket associatedBucket = database.getSchema().getBucketById(vectorIndex.getAssociatedBucketId());
      final long bucketDocCount = database.countBucket(associatedBucket.getName());
      assertThat(totalVectors)
          .as("Sub-index for bucket '%s' should have vectors only from that bucket", associatedBucket.getName())
          .isLessThanOrEqualTo(bucketDocCount);
    }

    // Reopen and verify search works correctly
    reopenDatabase();

    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('MultiBucketVector[vector]', ?, ?) AS neighbors",
          queryVector, 10);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      assertThat(neighbors.size()).isGreaterThan(0);
      rs.close();
    });
  }

  private float[] createDeterministicVector(final int index) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(index * 0.1 + j * 0.3) * 0.5f + 0.5f;
    return vector;
  }
}
