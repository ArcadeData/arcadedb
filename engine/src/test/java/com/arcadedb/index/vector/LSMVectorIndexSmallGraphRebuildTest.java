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
  void everySearchAfterAWriteBelowTheThresholdReusesTheGraphAndFindsTheWrite() {
    final LSMVectorIndex index = indexWithGraphOf(30, 50);
    final long generation = generation(index);
    final Random random = new Random(11);

    // The shape of the issue: a write, then a search, over and over. None of the ten searches may build, and each must
    // find the vector the write before it left in the delta buffer
    for (int round = 1; round <= 10; round++) {
      final float[] written = randomVector(random);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newVertex("Item").set("vector", written).save().getIdentity());

      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(written, 5);

      assertThat(results).hasSize(5);
      assertThat(results.get(0).getFirst()).as("round %d: the vector just written is its own nearest neighbour", round)
          .isEqualTo(rid[0]);
      assertThat(mutations(index)).as("round %d: the search must not have rebuilt the graph", round).isEqualTo((long) round);
    }
    assertThat(generation(index)).as("no search published a new graph").isEqualTo(generation);
  }

  @Test
  void aDeleteBelowTheThresholdIsHiddenWithoutARebuild() {
    final LSMVectorIndex index = indexWithGraphOf(30, 50);
    final long generation = generation(index);

    final List<Pair<RID, Float>> before = index.findNeighborsFromVector(distinctiveQuery(), 5);
    final RID nearest = before.get(0).getFirst();
    database.transaction(() -> nearest.asVertex().delete());
    assertThat(mutations(index)).isEqualTo(1L);

    final List<Pair<RID, Float>> after = index.findNeighborsFromVector(distinctiveQuery(), 5);

    assertThat(after).as("the deleted vector is still in the graph, and must not be answered").hasSize(5)
        .extracting(Pair::getFirst).doesNotContain(nearest);
    assertThat(after.get(0).getFirst()).as("the runner-up takes its place").isEqualTo(before.get(1).getFirst());
    assertThat(mutations(index)).as("the search must not have rebuilt the graph").isEqualTo(1L);
    assertThat(generation(index)).isEqualTo(generation);
  }

  @Test
  void anUpdateBelowTheThresholdIsFoundOnlyAtItsNewPositionWithoutARebuild() {
    final LSMVectorIndex index = indexWithGraphOf(30, 50);
    final long generation = generation(index);

    final float[] origin = new float[DIM];
    final RID moved = index.findNeighborsFromVector(origin, 1).get(0).getFirst();
    final float[] far = new float[DIM];
    far[0] = 998.0f;
    database.transaction(() -> moved.asVertex().modify().set("vector", far).save());
    assertThat(mutations(index)).as("an update is a delete and an insert").isEqualTo(2L);

    assertThat(index.findNeighborsFromVector(distinctiveQuery(), 5).get(0).getFirst()).as("found at its new position")
        .isEqualTo(moved);
    assertThat(index.findNeighborsFromVector(origin, 5)).as("and no longer at the old one").hasSize(5)
        .extracting(Pair::getFirst).doesNotContain(moved);
    assertThat(mutations(index)).as("no search rebuilt the graph").isEqualTo(2L);
    assertThat(generation(index)).isEqualTo(generation);
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

  private static long generation(final LSMVectorIndex index) {
    return index.getStats().get("rebuildSnapshotGeneration");
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
