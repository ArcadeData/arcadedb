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
import com.arcadedb.log.WarningCapture;
import com.arcadedb.log.WarningCapture.LogLine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5412 follow-up: bounding the graph-build cache at a flat 100,000 entries (issue #3144) also applied to
 * document-backed indexes, where a miss costs a record lookup plus a full property deserialization. The
 * validation phase read every vector and then dropped everything past the bound, so a from-scratch build of a
 * corpus larger than the cache re-read almost every vector, hundreds of times each. The cache is now sized from
 * the corpus and a share of the heap ceiling (issue #7146) for both document-backed and inline-quantized indexes.
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

      assertThat(index.computeGraphBuildCacheCapacity(NUM_VECTORS)).isEqualTo(16);

      final long before = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L);
      index.buildVectorGraphNow();
      final long fetches = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L) - before;

      assertThat(fetches).as("document reads with a 16-entry cache over %d vectors", NUM_VECTORS).isPositive();
    } finally {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(previous);
    }
  }

  // Inline-quantized indexes resolve a miss from an index page, but the heap-percent knob must still move
  // the cache (issue #7146). Only percent=0 (or an explicit graphBuildCacheSize) keeps the flat bound.
  @Test
  void inlineQuantizedBuildHonoursTheHeapPercentKnob() {
    createIndex(VectorQuantizationType.INT8);
    final LSMVectorIndex index = index();

    final int auto = index.computeGraphBuildCacheCapacity(50_000_000);
    assertThat(auto)
        .as("INT8 must not stay at the flat 100,000 bound while the same corpus in fp32 gets the heap share")
        .isGreaterThan(ArcadePageVectorValues.DEFAULT_CACHE_SIZE);
    assertThat((long) auto).isLessThanOrEqualTo(ceilingShareCapacity());

    final int previous = GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.getValueAsInteger();
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(0);
    try {
      assertThat(index.computeGraphBuildCacheCapacity(50_000_000))
          .isEqualTo(ArcadePageVectorValues.DEFAULT_CACHE_SIZE);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(previous);
    }
  }

  @Test
  void aFromScratchBuildLogsCacheCapacityNextToTheCorpusSize() {
    createIndex(null);
    final LSMVectorIndex index = index();

    final List<LogLine> lines = WarningCapture.capture(Level.INFO, index::buildVectorGraphNow);

    assertThat(lines.stream().map(LogLine::message))
        .as("the chosen cache size next to the corpus is what makes a too-small default a one-line diagnosis")
        .anyMatch(message -> message.contains("cache enabled: size=" + NUM_VECTORS + " of " + NUM_VECTORS));
  }

  // The same line for an INT8 index: before issue #7146 it read "size=100000", whatever the knob said, which is
  // the form the report had to work back from. This also pins the warm-in-place path for inline quantization,
  // whose vectors are read from index pages during validation rather than from documents.
  @Test
  void anInlineQuantizedBuildLogsTheSameCapacityAsADocumentBackedOne() {
    createIndex(VectorQuantizationType.INT8);
    final LSMVectorIndex index = index();

    final List<LogLine> lines = WarningCapture.capture(Level.INFO, index::buildVectorGraphNow);

    assertThat(lines.stream().map(LogLine::message))
        .anyMatch(message -> message.contains("cache enabled: size=" + NUM_VECTORS + " of " + NUM_VECTORS));
  }

  // A corpus too large for the heap budget must still build: the cache is capped, not the corpus.
  @Test
  void autoSizingIsCappedByTheHeapBudget() {
    createIndex(null);
    final LSMVectorIndex index = index();

    // The budget is a share of -Xmx (issue #7146), so a bound derived from the live heap would be the only
    // assertion that holds on a 16 GB CI runner AND on a 256 GB workstation. 1% of any ceiling a JVM can be
    // given is far short of 50M x 32-dim vectors, so the cap provably binds here rather than by luck.
    final int previous = GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.getValueAsInteger();
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(1);
    try {
      final int capacity = index.computeGraphBuildCacheCapacity(50_000_000);
      assertThat(capacity).isLessThan(50_000_000);
      assertThat(capacity).isGreaterThanOrEqualTo(ArcadePageVectorValues.DEFAULT_CACHE_SIZE);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(previous);
    }

    // At the default the share of the ceiling is the ceiling of what may be chosen: the available-heap cap can
    // only lower it. Asserting the upper bound rather than an exact figure keeps this deterministic while a
    // concurrent test is allocating.
    assertThat(index.computeGraphBuildCacheCapacity(50_000_000))
        .isLessThanOrEqualTo((int) Math.min(50_000_000L, ceilingShareCapacity()));

    // Disabling the heap share falls back to the flat bound rather than to an unbounded cache
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(0);
    try {
      assertThat(index.computeGraphBuildCacheCapacity(50_000_000))
          .isEqualTo(ArcadePageVectorValues.DEFAULT_CACHE_SIZE);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(previous);
    }
  }

  /** Vectors the configured share of {@code -Xmx} pays for. Depends on no live-heap reading, so it cannot drift. */
  private static long ceilingShareCapacity() {
    final int percent = Math.min(90,
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.getValueAsInteger());
    return VectorHeapBudget.maxHeapBytes() / 100 * percent / VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);
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
