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
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A committed vector stays reachable while several indexes rebuild concurrently (issue #5615).
 * <p>
 * Every rebuild in the JVM serializes on one permit of {@code REBUILD_SEMAPHORE}, so a single index in isolation
 * never experiences the contention the Python bindings suite does, where many indexes live in one JVM. This drives
 * several indexes at once.
 * <p>
 * The defect this guards against is a graph build that emits a node with a full set of outgoing edges and no
 * incoming ones. Beam search only ever walks edges forward from the entry node, so such a node can never be
 * returned however good its score - the miss is stable across retries and survives any {@code efSearch}. On a miss
 * the test therefore reports whether the vector is reachable from the entry node at all, which is what separates
 * this from ordinary ANN recall.
 */
@Tag("slow")
class LSMVectorIndexConcurrentRebuildVisibilityTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  private static final int INDEXES       = 4;
  private static final int BATCHES       = 20;
  private static final int BATCH         = 100;

  @Test
  void committedVectorsStayReachableWhileIndexesRebuild() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 8);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 1);

    for (int t = 0; t < INDEXES; t++) {
      final String typeName = "Vec" + t;
      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName);
        database.getSchema().getType(typeName).createProperty("vector", Type.ARRAY_OF_FLOATS);
        database.command("sql", """
            CREATE INDEX ON %s (vector) LSM_VECTOR
            METADATA {
                "dimensions": %d,
                "similarity": "EUCLIDEAN"
            }""".formatted(typeName, EMBEDDING_DIM));
      });
    }

    final List<String> misses = new CopyOnWriteArrayList<>();
    final ExecutorService executor = Executors.newFixedThreadPool(INDEXES);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(INDEXES);

    for (int t = 0; t < INDEXES; t++) {
      final String typeName = "Vec" + t;
      final long seed = 1000L + t;
      executor.submit(() -> {
        try {
          start.await();
          driveOneIndex(typeName, seed, misses);
        } catch (final Exception e) {
          misses.add(typeName + " thread failed: " + e);
        } finally {
          done.countDown();
        }
      });
    }

    start.countDown();
    assertThat(done.await(10, TimeUnit.MINUTES)).as("writers finished").isTrue();
    executor.shutdownNow();

    assertThat(misses).as("every committed vector must stay reachable while other indexes rebuild").isEmpty();
  }

  private void driveOneIndex(final String typeName, final long seed, final List<String> misses) {
    final Random random = new Random(seed);
    final Map<RID, float[]> committed = new LinkedHashMap<>();

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(typeName + "[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    for (int batch = 0; batch < BATCHES && misses.isEmpty(); batch++) {
      final List<float[]> pending = new ArrayList<>(BATCH);
      for (int i = 0; i < BATCH; i++)
        pending.add(randomVector(random));

      database.transaction(() -> {
        for (final float[] vector : pending) {
          final var doc = database.newDocument(typeName);
          doc.set("vector", vector);
          doc.save();
          committed.put(doc.getIdentity(), vector);
        }
      });

      for (final Map.Entry<RID, float[]> e : committed.entrySet()) {
        final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(e.getValue(), 1);
        if (hits.isEmpty() || !hits.getFirst().getFirst().equals(e.getKey())) {
          misses.add(typeName + " batch " + batch + ": " + e.getKey() + " -> "
              + (hits.isEmpty() ? "no hit" : hits.getFirst().getFirst().toString()) + " | " + diagnose(index, e.getKey()));
          return;
        }
      }
    }
  }

  /**
   * Where the engine believes this RID lives, and whether the graph can reach it at all. A miss reported with
   * {@code reachableFromEntry=false} is the orphaned-node defect; one reported with {@code true} is something new
   * and should not be assumed to be the same bug.
   */
  private static String diagnose(final LSMVectorIndex index, final RID rid) {
    try {
      final VectorLocationIndex locations = readField(index, "vectorIndex");
      final int[] vectorIds = locations.getVectorIdsForRid(rid);

      // DeltaVectorEntry is a private nested class, so its elements are inspected reflectively too.
      final List<?> delta = readField(index, "deltaVectors");
      boolean inDelta = false;
      for (final Object entry : delta) {
        final Field ridField = entry.getClass().getDeclaredField("rid");
        ridField.setAccessible(true);
        if (rid.equals(ridField.get(entry))) {
          inDelta = true;
          break;
        }
      }

      final int[] ordinalMap = readField(index, "ordinalToVectorId");
      int ordinal = -1;
      if (ordinalMap != null)
        for (int o = 0; o < ordinalMap.length && ordinal < 0; o++)
          for (final int vectorId : vectorIds)
            if (ordinalMap[o] == vectorId) {
              ordinal = o;
              break;
            }

      final ImmutableGraphIndex graph = readField(index, "graphIndex");

      return "vectorIds=" + java.util.Arrays.toString(vectorIds) + " inDelta=" + inDelta + " ordinal=" + ordinal
          + " deltaSize=" + delta.size() + " ordinalMapSize=" + (ordinalMap == null ? -1 : ordinalMap.length)
          + " graphSize=" + (graph == null ? -1 : graph.size())
          + " reachableFromEntry=" + (graph == null || ordinal < 0 ? "n/a" : reachable(graph, ordinal));
    } catch (final Exception e) {
      return "diagnostics unavailable: " + e;
    }
  }

  /**
   * Walks every edge from the graph entry node. Beam search can only ever return what this walk visits.
   */
  private static boolean reachable(final ImmutableGraphIndex graph, final int target) {
    try (final ImmutableGraphIndex.View view = graph.getView()) {
      final int upper = graph.getIdUpperBound();
      if (target < 0 || target >= upper)
        return false;

      final boolean[] seen = new boolean[upper];
      final ArrayDeque<Integer> queue = new ArrayDeque<>();
      final int entry = view.entryNode().node;
      seen[entry] = true;
      queue.add(entry);

      while (!queue.isEmpty()) {
        final int node = queue.poll();
        for (int level = 0; level <= graph.getMaxLevel(); level++) {
          final NodesIterator neighbors;
          try {
            neighbors = view.getNeighborsIterator(level, node);
          } catch (final Exception ignored) {
            continue;
          }
          while (neighbors.hasNext()) {
            final int next = neighbors.nextInt();
            if (next >= 0 && next < upper && !seen[next]) {
              seen[next] = true;
              queue.add(next);
            }
          }
        }
      }
      return seen[target];
    } catch (final Exception e) {
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(final LSMVectorIndex index, final String name) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField(name);
    field.setAccessible(true);
    return (T) field.get(index);
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[EMBEDDING_DIM];
    for (int i = 0; i < EMBEDDING_DIM; i++)
      vector[i] = random.nextFloat() * 100.0f;
    return vector;
  }
}
