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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Random;

/**
 * Isolates the cost of the graph-build vector cache: builds an HNSW graph from scratch with a cache
 * deliberately smaller than the corpus and reports wall time, document reads and GC time.
 * <p>
 * Follow-up to issue #5412: a 10M build reportedly went from 2,796s to 11,126s after the search-cache
 * change, which also swapped the build cache replacement policy from fill-then-freeze to evict-on-collision.
 * <p>
 * Usage:
 * mvn test -pl engine -Dtest=VectorGraphBuildCacheBenchmark -DfailIfNoTests=false \
 * -DexcludedGroups= -Dvector.build.numVectors=100000 -Dvector.build.cacheSize=10000
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class VectorGraphBuildCacheBenchmark {
  private static final String DB_PATH     = "target/test-databases/VectorGraphBuildCacheBenchmark";
  private static final int    NUM_VECTORS = Integer.getInteger("vector.build.numVectors", 100_000);
  private static final int    DIMENSIONS  = Integer.getInteger("vector.build.dimensions", 96);
  private static final int    CACHE_SIZE  = Integer.getInteger("vector.build.cacheSize", 10_000);
  private static final int    BATCH_SIZE  = 10_000;
  private static final long   SEED        = 42L;

  @Test
  void buildGraphWithBoundedCache() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(CACHE_SIZE);

    System.out.printf("=== GRAPH BUILD CACHE BENCHMARK: %,d vectors x %d dims, buildCacheSize=%,d ===%n",
        NUM_VECTORS, DIMENSIONS, CACHE_SIZE);

    final float[][] data = generateClusteredVectors(NUM_VECTORS, DIMENSIONS, new Random(SEED));

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("VectorData");
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON VectorData (vector) LSM_VECTOR METADATA { \"dimensions\": "
              + DIMENSIONS + ", \"similarity\": \"COSINE\" }");
        });

        final long ingestStart = System.nanoTime();
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("VectorData").set("vector", data[i]).save();
          if ((i + 1) % BATCH_SIZE == 0) {
            db.commit();
            db.begin();
          }
        }
        if (db.isTransactionActive())
          db.commit();
        System.out.printf("Ingest: %.1fs%n", (System.nanoTime() - ingestStart) / 1e9);
      }

      // Reopen so the graph is built from scratch off the persisted documents
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("VectorData")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

        final long gcBefore = totalGcMillis();
        final long docsBefore = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L);

        final long buildStart = System.nanoTime();
        index.buildVectorGraphNow();
        final double buildSec = (System.nanoTime() - buildStart) / 1e9;

        final Map<String, Long> stats = index.getStats();
        final long docs = stats.getOrDefault("vectorFetchFromDocuments", 0L) - docsBefore;
        final long gcMs = totalGcMillis() - gcBefore;

        System.out.printf("RESULT buildSeconds=%.2f docReads=%,d docReadsPerVector=%.2f gcMillis=%,d%n",
            buildSec, docs, docs / (double) NUM_VECTORS, gcMs);
      }
    } finally {
      FileUtils.deleteRecursively(new File(DB_PATH));
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.reset();
    }
  }

  private static long totalGcMillis() {
    long total = 0;
    for (final GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans())
      total += Math.max(0, bean.getCollectionTime());
    return total;
  }

  /**
   * Clustered data so beam search behaves like a real corpus instead of degenerating on uniform noise.
   */
  private static float[][] generateClusteredVectors(final int count, final int dims, final Random rng) {
    final int clusters = Math.max(1, count / 1_000);
    final float[][] centroids = new float[clusters][dims];
    for (int c = 0; c < clusters; c++)
      for (int d = 0; d < dims; d++)
        centroids[c][d] = rng.nextFloat() * 2 - 1;

    final float[][] out = new float[count][dims];
    for (int i = 0; i < count; i++) {
      final float[] centroid = centroids[rng.nextInt(clusters)];
      for (int d = 0; d < dims; d++)
        out[i][d] = centroid[d] + (float) rng.nextGaussian() * 0.1f;
    }
    return out;
  }
}
