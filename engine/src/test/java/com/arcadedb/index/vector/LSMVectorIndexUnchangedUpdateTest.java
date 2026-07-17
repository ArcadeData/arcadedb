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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5318: a record update on a vector-indexed type that does NOT touch the
 * indexed embedding must not remove and re-insert the vector into the LSM vector index.
 * <p>
 * Before the fix, {@link com.arcadedb.database.DocumentIndexer#updateDocument} compared the old/new
 * {@code float[]} embeddings with {@code Object.equals} (reference equality), so an unchanged embedding
 * was always seen as modified. Every scalar-only update therefore fired
 * {@code LSMVectorIndex.remove(keys, rid)} (O(index size) per call), degrading bulk updates to O(N^2).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexUnchangedUpdateTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  private static final int RECORDS        = 300;

  @Test
  void scalarOnlyUpdateDoesNotTouchVectorIndex() {
    final Random random = new Random(42);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Question");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("counter", Type.INTEGER);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Question (embedding) LSM_VECTOR
          METADATA { "dimensions": %d, "similarity": "COSINE" }""".formatted(EMBEDDING_DIM));
    });

    // Insert RECORDS vertices, each with a distinct random embedding.
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final MutableDocument v = database.newVertex("Question");
        v.set("id", i);
        v.set("counter", 0);
        v.set("embedding", randomEmbedding(random));
        v.save();
      }
    });

    final long insertOpsAfterLoad = sumStat("insertOperations");
    assertThat(sumStat("activeVectors")).isEqualTo((long) RECORDS);
    assertThat(sumStat("deletedVectors")).isEqualTo(0L);

    // Second pass: bump an unrelated scalar property on every vertex, leaving the embedding untouched.
    database.transaction(() -> {
      database.iterateType("Question", false).forEachRemaining(r -> {
        final MutableDocument v = r.getRecord().asVertex().modify();
        v.set("counter", ((Number) v.get("counter")).intValue() + 1);
        v.save();
      });
    });

    // The unchanged embedding must NOT be tombstoned nor re-inserted.
    assertThat(sumStat("deletedVectors")).as("no vector tombstones for scalar-only updates").isEqualTo(0L);
    assertThat(sumStat("insertOperations")).as("no vector re-insertions for scalar-only updates").isEqualTo(insertOpsAfterLoad);
    assertThat(sumStat("activeVectors")).isEqualTo((long) RECORDS);
  }

  private long sumStat(final String key) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Question[embedding]");
    long total = 0;
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      total += ((LSMVectorIndex) bucketIndex).getStats().get(key);
    return total;
  }

  private static float[] randomEmbedding(final Random random) {
    final float[] v = new float[EMBEDDING_DIM];
    for (int i = 0; i < EMBEDDING_DIM; i++)
      v[i] = random.nextFloat();
    return v;
  }
}
