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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for delta scan: newly inserted vectors are visible to search before the next graph rebuild.
 * <p>
 * Issue: https://github.com/ArcadeData/arcadedb/issues/3679
 */
class DeltaScanVectorSearchTest extends TestHelper {

  private static final int EMBEDDING_DIM = 8;

  @Test
  void insertAfterGraphBuildFindsNewVector() {
    // High threshold so no rebuild is triggered when we insert one more
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1000);

    database.transaction(() -> {
      database.getSchema().createVertexType("Item");
      database.getSchema().getType("Item").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Item (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert 20 vectors and trigger initial graph build via search
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // First search triggers graph build
    final float[] queryVector = new float[EMBEDDING_DIM];
    queryVector[0] = 999.0f; // distinctive
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Now insert a vector very close to queryVector (should be nearest neighbor)
    final float[] nearVector = new float[EMBEDDING_DIM];
    nearVector[0] = 998.0f; // very close to query
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Item SET vector = ?", (Object) nearVector);
    });

    // Verify the new vector is tracked (either in delta buffer or live graph)
    final Map<String, Long> stats = lsmIndex.getStats();
    // With live builder: vector goes directly to graph (deltaVectorsCount may be 0)
    // Without live builder: vector goes to delta buffer (deltaVectorsCount >= 1)
    // Either way, the vector should be findable in the next search

    // Search again — the new vector should appear (via delta scan or live graph)
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // The closest result should be the nearVector (distance ~1.0 in euclidean)
    final RID closestRID = results.get(0).getFirst();
    // Verify by loading the record and checking its vector
    final Object closestVectorObj = database.lookupByRID(closestRID, true).asDocument().get("vector");
    assertThat(((float[]) closestVectorObj)[0]).isEqualTo(998.0f);
  }

  @Test
  void deleteFromDeltaRemovesFromSearchResults() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1000);

    database.transaction(() -> {
      database.getSchema().createVertexType("Item");
      database.getSchema().getType("Item").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Item (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert initial vectors and trigger graph build
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final float[] queryVector = new float[EMBEDDING_DIM];
    queryVector[0] = 999.0f;

    // Trigger initial graph build
    lsmIndex.findNeighborsFromVector(queryVector, 10);

    // Insert a distinctive vector
    final float[] nearVector = new float[EMBEDDING_DIM];
    nearVector[0] = 998.0f;
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Item SET vector = ?", (Object) nearVector);
    });

    // Verify it appears in results
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    final RID nearRID = results.get(0).getFirst();
    final Object foundVectorObj = database.lookupByRID(nearRID, true).asDocument().get("vector");
    assertThat(((float[]) foundVectorObj)[0]).isEqualTo(998.0f);

    // Now delete that record
    database.transaction(() -> {
      database.command("sql", "DELETE FROM " + nearRID);
    });

    // Search again — the deleted vector should NOT appear
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    for (final Pair<RID, Float> r : results)
      assertThat(r.getFirst()).isNotEqualTo(nearRID);
  }

  @Test
  void deltaClearedAfterRebuild() {
    // Low threshold so rebuild triggers quickly
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 2);

    database.transaction(() -> {
      database.getSchema().createVertexType("Item");
      database.getSchema().getType("Item").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Item (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert initial vectors and trigger graph build
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final float[] queryVector = generateRandomVector(random);

    // Trigger initial graph build
    lsmIndex.findNeighborsFromVector(queryVector, 5);

    // Insert vectors to exceed threshold (threshold=2, so insert 3)
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) generateRandomVector(random));
    });

    // With live builder: vectors go directly to graph (delta may be 0)
    // Without live builder: delta >= 3, then cleared after rebuild

    // Search triggers rebuild (small graph < 1000 → synchronous) if using old path
    lsmIndex.findNeighborsFromVector(queryVector, 5);

    // After rebuild or with live builder, delta should be empty (vectors are in the graph)
    assertThat(lsmIndex.getStats().get("deltaVectorsCount")).isEqualTo(0L);
  }

  @Test
  void ridFilterAppliesToDelta() {
    // High threshold to prevent automatic rebuild which changes ordinal mapping
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

    database.transaction(() -> {
      database.getSchema().createVertexType("Item");
      database.getSchema().getType("Item").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Item (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert initial vectors and trigger graph build
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final float[] queryVector = new float[EMBEDDING_DIM];
    queryVector[0] = 999.0f;

    // Trigger initial graph build
    lsmIndex.findNeighborsFromVector(queryVector, 10);

    // Insert a distinctive vector
    final float[] nearVector = new float[EMBEDDING_DIM];
    nearVector[0] = 998.0f;
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Item SET vector = ?", (Object) nearVector);
    });

    // Find the nearVector's RID by looking for vector[0] == 998.0
    List<Pair<RID, Float>> allResults = lsmIndex.findNeighborsFromVector(queryVector, 11);
    RID nearRID = null;
    for (final Pair<RID, Float> r : allResults) {
      final Object v = database.lookupByRID(r.getFirst(), true).asDocument().get("vector");
      if (v instanceof float[] fv && fv[0] == 998.0f) {
        nearRID = r.getFirst();
        break;
      }
    }
    assertThat(nearRID).as("Should find the nearVector in results").isNotNull();

    // Build allowed set of ALL RIDs from the initial 10 vectors (excluding nearVector)
    final Set<RID> allowedRIDs = new HashSet<>();
    for (final Pair<RID, Float> r : allResults)
      if (!r.getFirst().equals(nearRID))
        allowedRIDs.add(r.getFirst());

    // Search with filter — nearVector should be excluded
    final List<Pair<RID, Float>> filteredResults = lsmIndex.findNeighborsFromVector(queryVector, 10, allowedRIDs);
    for (final Pair<RID, Float> r : filteredResults)
      assertThat(r.getFirst()).as("Filtered results should not contain nearVector").isNotEqualTo(nearRID);
  }

  @Test
  void deltaOnlySearchWithNoGraph() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1000);

    database.transaction(() -> {
      database.getSchema().createVertexType("Item");
      database.getSchema().getType("Item").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Item (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // Insert 1 vector — no graph build yet
    final float[] insertedVector = new float[EMBEDDING_DIM];
    insertedVector[0] = 5.0f;
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Item SET vector = ?", (Object) insertedVector);
    });

    // Search should return the delta vector even without a graph
    final float[] queryVector = new float[EMBEDDING_DIM];
    queryVector[0] = 5.0f;
    final List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 5);
    assertThat(results).hasSize(1);

    // For EUCLIDEAN, JVector returns similarity = 1/(1+dist²), so identical vectors → score 1.0
    assertThat(results.get(0).getSecond()).isEqualTo(1.0f);
  }

  @Test
  void getMethodIncludesDeltaVectors() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1000);

    database.transaction(() -> {
      database.getSchema().createVertexType("Item");
      database.getSchema().getType("Item").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Item (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert initial vectors and trigger graph build
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final float[] queryVector = new float[EMBEDDING_DIM];
    queryVector[0] = 999.0f;

    // Trigger initial graph build via get()
    IndexCursor cursor = lsmIndex.get(new Object[] { queryVector }, 10);
    final List<RID> initialRIDs = new ArrayList<>();
    while (cursor.hasNext())
      initialRIDs.add(cursor.next().getIdentity());
    assertThat(initialRIDs).isNotEmpty();

    // Insert a distinctive vector
    final float[] nearVector = new float[EMBEDDING_DIM];
    nearVector[0] = 998.0f;
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Item SET vector = ?", (Object) nearVector);
    });

    // get() should include the delta vector
    cursor = lsmIndex.get(new Object[] { queryVector }, 10);
    final List<RID> updatedRIDs = new ArrayList<>();
    while (cursor.hasNext())
      updatedRIDs.add(cursor.next().getIdentity());

    // Should have at least as many results, and the new one should be first (closest)
    assertThat(updatedRIDs).isNotEmpty();
    final Object closestVecObj = database.lookupByRID(updatedRIDs.get(0), true).asDocument().get("vector");
    assertThat(((float[]) closestVecObj)[0]).isEqualTo(998.0f);
  }

  private float[] generateRandomVector(final Random random) {
    final float[] vector = new float[EMBEDDING_DIM];
    for (int i = 0; i < EMBEDDING_DIM; i++)
      vector[i] = random.nextFloat() * 2 - 1;
    return vector;
  }
}
