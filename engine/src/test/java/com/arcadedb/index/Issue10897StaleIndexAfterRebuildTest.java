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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10897: {@code REBUILD INDEX} retires the index object and creates a new one under the same name. A caller holding the
 * bucket-level {@link LSMVectorIndex} from before the rebuild was answered as if the index were empty - 0 neighbours, 0 entries,
 * zeroed stats - with no exception, while the {@link TypeIndex} over it and an LSM_TREE bucket refuse with "not valid. Probably has
 * been drop or rebuilt", which is what tells the caller to fetch the index again. Every kind of bucket index now refuses the same
 * way on its read paths, and the new object fetched by name answers the data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10897StaleIndexAfterRebuildTest extends TestHelper {
  private static final int    COUNT      = 500;
  private static final int    DIMENSIONS = 16;
  private static final String NOT_VALID  = "is not valid. Probably has been drop";

  @Test
  void aDenseVectorBucketIndexHeldAcrossARebuildRefusesInsteadOfAnsweringNothing() {
    database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    final Random random = new Random(7);
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newVertex("Doc").set("embedding", randomVector(random)).save();
    });
    database.command("sql",
        "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\"}");

    final TypeIndex oldType = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    final LSMVectorIndex old = (LSMVectorIndex) oldType.getIndexesOnBuckets()[0];
    final float[] query = randomVector(new Random(11));
    assertThat(old.findNeighborsFromVector(query, 5)).hasSize(5);
    assertThat(old.countEntries()).isEqualTo(COUNT);

    database.command("sql", "REBUILD INDEX `Doc[embedding]`");

    assertThat(old.isValid()).isFalse();
    assertRefused(() -> old.findNeighborsFromVector(query, 5));
    assertRefused(() -> old.findNeighborsFromVector(query, 5, -1, null, WorkGuard.forCommandDeadline(null)));
    assertRefused(() -> old.findNeighborsFromVectorApproximate(query, 5));
    assertRefused(() -> old.findNeighborsFromVectorGrouped(query, 5, 1, -1, null, rid -> rid));
    assertRefused(() -> old.findNeighborsFromVectorForGroups(query, Set.of("g"), 1, -1, null, rid -> "g", Float.POSITIVE_INFINITY));
    assertRefused(() -> old.get(new Object[] { query }, 5));
    assertRefused(old::countEntries);
    // the stats describe the instance itself, and its close-time diagnostics are read after it was released
    assertThat(old.getStats()).isNotNull();

    // the data is intact: the index fetched again by name answers it
    final LSMVectorIndex fresh = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]"))
        .getIndexesOnBuckets()[0];
    assertThat(fresh).isNotSameAs(old);
    assertThat(fresh.findNeighborsFromVector(query, 5)).hasSize(5);
    assertThat(fresh.countEntries()).isEqualTo(COUNT);
    assertThat(fresh.getStats().get("activeVectors")).isEqualTo(COUNT);
  }

  @Test
  void aSparseVectorBucketIndexHeldAcrossARebuildRefusesToo() {
    database.command("sql", "CREATE DOCUMENT TYPE Sparse BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newDocument("Sparse").set("tokens", new int[] { i % 16 }, "weights", new float[] { 1.0f }).save();
    });

    final String name = "Sparse[tokens,weights]";
    final LSMSparseVectorIndex old = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(name))
        .getIndexesOnBuckets()[0];
    final int[] tokens = { 3 };
    final float[] weights = { 1.0f };
    assertThat(old.topK(tokens, weights, 5, null)).hasSize(5);

    database.command("sql", "REBUILD INDEX `" + name + "`");

    assertThat(old.isValid()).isFalse();
    assertRefused(() -> old.topK(tokens, weights, 5, null));
    assertRefused(() -> old.topKGrouped(tokens, weights, 5, 1, null, rid -> rid));
    assertRefused(() -> old.topKForGroups(tokens, weights, Set.of("g"), 1, 0f, null, rid -> "g", null));
    assertRefused(old::countEntries);
    assertThat(old.getStats()).isNotNull();

    final LSMSparseVectorIndex fresh = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(name))
        .getIndexesOnBuckets()[0];
    assertThat(fresh.topK(tokens, weights, 5, null)).hasSize(5);
  }

  /** The contrast the report draws, which the vector indexes now match: a stale LSM_TREE or HASH bucket refuses its reads. */
  @Test
  void lsmTreeAndHashBucketIndexesHeldAcrossARebuildRefuseTheirReads() {
    database.command("sql", "CREATE DOCUMENT TYPE Tag BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Tag.name STRING");
    database.command("sql", "CREATE PROPERTY Tag.code STRING");
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newDocument("Tag").set("name", "t" + i, "code", "c" + i).save();
    });
    database.command("sql", "CREATE INDEX ON Tag (name) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Tag (code) UNIQUE_HASH");

    for (final String name : new String[] { "Tag[name]", "Tag[code]" }) {
      final IndexInternal old = ((TypeIndex) database.getSchema().getIndexByName(name)).getIndexesOnBuckets()[0];
      assertThat(old.countEntries()).isEqualTo(COUNT);

      database.command("sql", "REBUILD INDEX `" + name + "`");

      assertThat(old.isValid()).isFalse();
      assertRefused(old::countEntries);
      assertRefused(() -> old.get(new Object[] { name.equals("Tag[name]") ? "t5" : "c5" }));
      final IndexInternal fresh = ((TypeIndex) database.getSchema().getIndexByName(name)).getIndexesOnBuckets()[0];
      assertThat(fresh.countEntries()).isEqualTo(COUNT);
      assertThat(fresh.getStats()).isNotNull();
    }
  }

  private static void assertRefused(final ThrowingCallable call) {
    assertThatThrownBy(call).isInstanceOf(IndexException.class).hasMessageContaining(NOT_VALID);
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) random.nextGaussian();
    return vector;
  }
}
