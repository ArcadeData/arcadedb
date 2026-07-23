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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #3144: the HNSW graph build used to keep a full second on-heap copy of the vector set. The
 * validation phase preloaded every vector into a {@code HashMap} and dumped them all into the
 * build-time cache, ignoring the {@code graphBuildCacheSize} bound. For inline-quantized (INT8)
 * indexes vectors are readable straight from index pages on any thread, so the preload is now
 * capped at the cache size and the remaining vectors are re-read lazily during the build.
 * <p>
 * This test builds an INT8-quantized graph with a cache far smaller than the vector count and
 * asserts the graph is still correct - proving the lazy page-read fallback carries the build.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexBoundedBuildMemoryTest extends TestHelper {
  private static final int DIMENSIONS  = 32;
  private static final int NUM_VECTORS = 400;
  private static final int TINY_CACHE  = 16; // deliberately << NUM_VECTORS to force lazy page reads

  @Test
  void int8GraphBuildsCorrectlyWithCacheSmallerThanVectorSet() {
    final int previousCacheSize = GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getValueAsInteger();
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(TINY_CACHE);
    try {
      final float[][] vectors = new float[NUM_VECTORS][];
      final Random rng = new Random(7);

      database.transaction(() -> {
        final DocumentType docType = database.getSchema().createDocumentType("Doc");
        docType.createProperty("id", Type.INTEGER);
        docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

        database.getSchema()
            .buildTypeIndex("Doc", new String[] { "embedding" })
            .withLSMVectorType()
            .withDimensions(DIMENSIONS)
            .withSimilarity("COSINE")
            .withQuantization(VectorQuantizationType.INT8)
            .create();

        for (int i = 0; i < NUM_VECTORS; i++) {
          final float[] v = randomNormalized(rng);
          vectors[i] = v;
          database.newDocument("Doc").set("id", i).set("embedding", v).save();
        }
      });

      // Force a full graph rebuild (the buildGraphFromScratch path that preloads vectors).
      database.command("sql", "REBUILD INDEX `Doc[embedding]`");

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsm = (LSMVectorIndex) idx.getIndexesOnBuckets()[0];

      // The preload was capped at TINY_CACHE, so most of the graph was built by re-reading vectors
      // lazily from the INT8 index pages. Each vector must still find itself as its own top hit.
      database.transaction(() -> {
        int selfIsTop = 0;
        for (int i = 0; i < NUM_VECTORS; i++) {
          final List<Pair<RID, Float>> neighbors = lsm.findNeighborsFromVector(vectors[i], 5);
          assertThat(neighbors).as("query %d returns results", i).isNotEmpty();

          final RID topRid = neighbors.get(0).getFirst();
          final int topId = database.lookupByRID(topRid, true).asDocument().getInteger("id");
          if (topId == i)
            selfIsTop++;
        }
        // INT8 quantization is lossy, so allow a small slack, but the overwhelming majority of
        // vectors must resolve to themselves - a broken/partial graph would collapse this number.
        assertThat(selfIsTop)
            .as("self-recall of the INT8 graph built with a tiny cache")
            .isGreaterThanOrEqualTo((int) (NUM_VECTORS * 0.9));
      });
    } finally {
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(previousCacheSize);
    }
  }

  private static float[] randomNormalized(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    double norm = 0;
    for (int i = 0; i < DIMENSIONS; i++) {
      v[i] = rng.nextFloat() * 2 - 1;
      norm += v[i] * (double) v[i];
    }
    norm = Math.sqrt(norm);
    if (norm == 0)
      norm = 1;
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] /= (float) norm;
    return v;
  }
}
