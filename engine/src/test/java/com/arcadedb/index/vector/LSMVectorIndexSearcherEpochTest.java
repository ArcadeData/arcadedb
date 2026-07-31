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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The searcher-pool epoch must never repeat a value it has already handed out.
 * <p>
 * A pooled {@code GraphSearcher} holds a graph {@code View} that can pin snapshot state, so it may only be recycled
 * while nothing a search depends on has moved. The pool decides that from the pair (graph identity, epoch). The graph
 * identity alone is not enough: the live-builder path keeps searching the same graph instance while it grows, so the
 * epoch is the only signal that the contents changed. An epoch that returns to an earlier value therefore makes a
 * stale view look reusable.
 */
class LSMVectorIndexSearcherEpochTest extends TestHelper {

  private static final int EMBEDDING_DIM = 8;

  @Test
  void epochNeverRepeatsAcrossRebuilds() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 1);

    database.transaction(() -> {
      database.getSchema().createDocumentType("Epoch");
      database.getSchema().getType("Epoch").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON Epoch (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Epoch[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final Method epoch = LSMVectorIndex.class.getDeclaredMethod("searcherPoolEpoch");
    epoch.setAccessible(true);

    final Random random = new Random(11);

    // Let the inactivity timer do a real rebuild, which is what drains the mutation counter. Forcing it through
    // compact() does not take that path.
    insert(random, 20);
    awaitSettled(index);
    final long afterFirstRebuild = (long) epoch.invoke(index);

    insert(random, 20);
    awaitSettled(index);
    final long afterSecondRebuild = (long) epoch.invoke(index);

    // Both rebuilds settle the mutation counter back to the same value, so an epoch derived from that counter alone
    // hands out the same number twice with two different graphs behind it.
    assertThat(afterSecondRebuild)
        .as("epoch must differ across a rebuild, otherwise a searcher pooled against the previous graph looks reusable")
        .isNotEqualTo(afterFirstRebuild);
  }

  private static void awaitSettled(final LSMVectorIndex index) {
    Awaitility.await("rebuild drains the mutation counter").atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(50))
        .until(() -> index.getStats().get("mutationsSinceRebuild") <= 0L
            && index.getStats().get("asyncRebuildInProgress") == 0L);
  }

  private void insert(final Random random, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] vector = new float[EMBEDDING_DIM];
        for (int j = 0; j < EMBEDDING_DIM; j++)
          vector[j] = random.nextFloat() * 100.0f;
        final var doc = database.newDocument("Epoch");
        doc.set("vector", vector);
        doc.save();
      }
    });
  }
}
