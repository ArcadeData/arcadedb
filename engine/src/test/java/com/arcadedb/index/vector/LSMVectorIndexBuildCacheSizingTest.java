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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5412 follow-up: bounding the graph-build cache at a flat 100,000 entries (issue #3144) also applied to
 * document-backed indexes, where a miss costs a record lookup plus a full property deserialization. The
 * validation phase read every vector and then dropped everything past the bound, so a from-scratch build of a
 * corpus larger than the cache re-read almost every vector, hundreds of times each. The cache is now sized from
 * the corpus and the heap for document-backed indexes, and keeps the small bound only where misses are cheap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexBuildCacheSizingTest extends TestHelper {
  private static final int DIMENSIONS  = 32;
  private static final int NUM_VECTORS = 600;

  // With the default configuration a document-backed build must never go back to the documents: the vectors
  // the validation phase already read stay in the cache for the whole build.
  @Test
  void documentBackedBuildKeepsTheWholeCorpusResidentByDefault() {
    createIndex(null);
    final LSMVectorIndex index = index();

    final long before = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L);
    index.buildVectorGraphNow();
    final long fetches = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L) - before;

    assertThat(fetches).as("document reads during a from-scratch build with the default cache size").isZero();
  }

  // The knob still bounds the cache when set explicitly, and a corpus larger than it falls back to re-reading.
  @Test
  void anExplicitCacheSizeStillBoundsTheBuild() {
    final int previous = GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getValueAsInteger();
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(16);
    try {
      createIndex(null);
      final LSMVectorIndex index = index();

      assertThat(index.computeGraphBuildCacheCapacity(NUM_VECTORS, false)).isEqualTo(16);

      final long before = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L);
      index.buildVectorGraphNow();
      final long fetches = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L) - before;

      assertThat(fetches).as("document reads with a 16-entry cache over %d vectors", NUM_VECTORS).isPositive();
    } finally {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(previous);
    }
  }

  // Inline-quantized indexes resolve a miss from an index page, so they keep the small bound instead of
  // spending heap on full residency.
  @Test
  void inlineQuantizedBuildKeepsTheSmallBound() {
    createIndex(VectorQuantizationType.INT8);
    final LSMVectorIndex index = index();

    assertThat(index.computeGraphBuildCacheCapacity(50_000_000, true))
        .isEqualTo(ArcadePageVectorValues.DEFAULT_CACHE_SIZE);
  }

  // A corpus too large for the heap budget must still build: the cache is capped, not the corpus.
  @Test
  void autoSizingIsCappedByTheHeapBudget() {
    createIndex(null);
    final LSMVectorIndex index = index();

    // 50M vectors x 32 dims is ~9.6GB of payload, well past any sane share of a test JVM heap
    final int capacity = index.computeGraphBuildCacheCapacity(50_000_000, false);
    assertThat(capacity).isLessThan(50_000_000);
    assertThat(capacity).isGreaterThanOrEqualTo(ArcadePageVectorValues.DEFAULT_CACHE_SIZE);

    // Disabling the heap share falls back to the flat bound rather than to an unbounded cache
    final int previous = GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.getValueAsInteger();
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(0);
    try {
      assertThat(index.computeGraphBuildCacheCapacity(50_000_000, false))
          .isEqualTo(ArcadePageVectorValues.DEFAULT_CACHE_SIZE);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(previous);
    }
  }

  private void createIndex(final VectorQuantizationType quantization) {
    final Random rng = new Random(13);
    database.transaction(() -> {
      final DocumentType docType = database.getSchema().createDocumentType("Doc");
      docType.createProperty("id", Type.INTEGER);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      final var builder = database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE");
      if (quantization != null)
        builder.withQuantization(quantization);
      builder.create();

      for (int i = 0; i < NUM_VECTORS; i++)
        database.newDocument("Doc").set("id", i).set("embedding", randomNormalized(rng)).save();
    });
  }

  private LSMVectorIndex index() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private static float[] randomNormalized(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    double norm = 0;
    for (int i = 0; i < DIMENSIONS; i++) {
      v[i] = (float) rng.nextGaussian();
      norm += v[i] * v[i];
    }
    norm = Math.sqrt(norm);
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] /= (float) norm;
    return v;
  }
}
