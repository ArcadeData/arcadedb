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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A graph under 1000 vectors was rebuilt on the calling thread by the first search after ANY write, whatever the mutation threshold
 * said. Every bucket of a type has its own graph, so a type with many buckets has many such graphs, and each search that followed
 * a write paid a build under the graph build lock that the other searches of the bucket queued behind. A resident small graph is
 * now rebuilt once the threshold is reached, as a large one is; before that the new vectors are searched through the delta buffer.
 */
class LSMVectorIndexSmallGraphRebuildTest extends TestHelper {

  private static final int DIM = 8;

  @Test
  void aWriteBelowTheThresholdIsSearchedThroughTheDeltaWithoutARebuild() {
    final LSMVectorIndex index = indexWithGraphOf(30, 50);

    final float[] near = new float[DIM];
    near[0] = 998.0f;
    database.transaction(() -> database.command("sql", "INSERT INTO Item SET vector = ?", (Object) near));
    assertThat(mutations(index)).isEqualTo(1L);

    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(distinctiveQuery(), 5);

    assertThat(((float[]) database.lookupByRID(results.get(0).getFirst(), true).asDocument().get("vector"))[0]).isEqualTo(998.0f);
    assertThat(mutations(index)).as("the search must not have rebuilt the graph").isEqualTo(1L);
  }

  @Test
  void theThresholdStillTriggersTheRebuild() {
    final LSMVectorIndex index = indexWithGraphOf(30, 50);
    final Random random = new Random(7);

    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) randomVector(random));
    });
    assertThat(mutations(index)).isGreaterThanOrEqualTo(50L);

    index.findNeighborsFromVector(distinctiveQuery(), 5);

    assertThat(mutations(index)).as("a search past the threshold rebuilds a small graph").isZero();
  }

  @Test
  void theFirstSearchStillBuildsTheGraphWhateverTheThreshold() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1000);
    createIndex();
    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) randomVector(random));
    });
    final LSMVectorIndex index = lsmIndex();

    assertThat(index.findNeighborsFromVector(distinctiveQuery(), 5)).hasSize(5);
    assertThat(mutations(index)).as("the first build cannot wait for a threshold").isZero();
  }

  private LSMVectorIndex indexWithGraphOf(final int vectors, final int threshold) {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
    createIndex();
    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < vectors; i++)
        database.command("sql", "INSERT INTO Item SET vector = ?", (Object) randomVector(random));
    });
    final LSMVectorIndex index = lsmIndex();
    index.findNeighborsFromVector(distinctiveQuery(), 5); // builds the graph
    assertThat(mutations(index)).isZero();
    return index;
  }

  private void createIndex() {
    database.transaction(() -> {
      // one bucket, so the one graph the test watches takes every vector
      database.getSchema().createVertexType("Item", 1);
      database.getSchema().getType("Item").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON Item (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(DIM));
    });
  }

  private LSMVectorIndex lsmIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Item[vector]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static long mutations(final LSMVectorIndex index) {
    return index.getStats().get("mutationsSinceRebuild");
  }

  private static float[] distinctiveQuery() {
    final float[] query = new float[DIM];
    query[0] = 999.0f;
    return query;
  }

  private static float[] randomVector(final Random random) {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
