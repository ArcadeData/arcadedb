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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11747: the graph a build publishes must not keep the vectors the build read. jvector's on-heap graph keeps the
 * score provider it was built with (through the diversity provider of every layer), and with it the build's vector reader:
 * its cache held every vector of the index, a second full-precision copy beside the search cache, for as long as the graph
 * stayed the live one.
 */
class GraphBuildReleasesBuildVectorsIssue11747Test extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int VECTORS    = 2_000;

  @BeforeEach
  void onlyTheExplicitBuild() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void restoreTheRebuildTriggers() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  @Test
  void thePublishedGraphKeepsNoVectorOfItsBuildReachable() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V BUCKETS 1");
      database.command("sql", "CREATE PROPERTY V.emb ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON V (emb) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\"}");
    });
    final Random random = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < VECTORS; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian();
        database.newVertex("V").set("emb", vector).save();
      }
    });

    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("V[emb]"))
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
    assertThat(index.getGraphIndex()).as("the build must have published a graph").isNotNull();
    assertThat(index.getGraphIndex().size()).isEqualTo(VECTORS);

    // The one vector a released build reader still holds is its placeholder for an unreadable ordinal
    assertThat(vectorsReachableFrom(index.getGraphIndex()))
        .as("vectors of %d dimensions reachable from the published graph", DIMENSIONS).isLessThanOrEqualTo(1);
  }

  /**
   * Counts the vectors of {@link #DIMENSIONS} floats reachable from {@code root}, walking the fields of the engine's and
   * jvector's objects and the contents of the JDK's containers. The walk stops at the index, the database and the location
   * map: the graph's reader refers to them legitimately, and they lead to everything else the index holds, the search cache
   * included.
   */
  private static int vectorsReachableFrom(final Object root) {
    final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    final ArrayDeque<Object> pending = new ArrayDeque<>();
    pending.add(root);
    int vectors = 0;
    while (!pending.isEmpty()) {
      final Object current = pending.poll();
      if (!seen.add(current) || isBoundary(current))
        continue;
      if (current instanceof VectorFloat<?> vector) {
        if (vector.length() == DIMENSIONS)
          vectors++;
        continue;
      }
      final Class<?> type = current.getClass();
      if (type.isArray()) {
        if (!type.getComponentType().isPrimitive())
          for (int i = 0; i < Array.getLength(current); i++)
            enqueue(pending, Array.get(current, i));
      } else if (current instanceof Collection<?> collection)
        for (final Object element : collection)
          enqueue(pending, element);
      else if (current instanceof Map<?, ?> map)
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
          enqueue(pending, entry.getKey());
          enqueue(pending, entry.getValue());
        }
      else if (current instanceof AtomicReference<?> reference)
        enqueue(pending, reference.get());
      else if (current instanceof AtomicReferenceArray<?> references)
        for (int i = 0; i < references.length(); i++)
          enqueue(pending, references.get(i));
      else if (!isJdk(type))
        for (Class<?> c = type; c != null && !isJdk(c); c = c.getSuperclass())
          for (final Field field : c.getDeclaredFields())
            if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
              field.setAccessible(true);
              try {
                enqueue(pending, field.get(current));
              } catch (final IllegalAccessException e) {
                throw new IllegalStateException(e);
              }
            }
    }
    return vectors;
  }

  private static void enqueue(final ArrayDeque<Object> pending, final Object value) {
    if (value != null)
      pending.add(value);
  }

  private static boolean isBoundary(final Object object) {
    return object instanceof LSMVectorIndex || object instanceof DatabaseInternal || object instanceof VectorLocationIndex
        || object instanceof Thread || object instanceof Class<?> || object instanceof ClassLoader;
  }

  private static boolean isJdk(final Class<?> type) {
    final String name = type.getName();
    return name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jdk.") || name.startsWith("sun.");
  }
}
