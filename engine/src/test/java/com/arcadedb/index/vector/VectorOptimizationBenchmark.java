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
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

/**
 * Ingestion, graph build, latency and recall@K of {@code LSM_VECTOR} with and without INT8 quantization. Data and
 * queries come from one set of clusters ({@link ClusteredVectorDataset}, #11711); {@code vector.bench.clusters} and
 * {@code vector.bench.clusterSpread} shape them.
 * <p>
 * Usage:
 *   mvn -pl engine -am test -Dtest=VectorOptimizationBenchmark -DexcludedGroups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false -DenableAssertions=false
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class VectorOptimizationBenchmark {
  private static final String DB_PATH = "target/test-databases/VectorOptimizationBenchmark";

  private static final int NUM_VECTORS = Integer.getInteger("vector.bench.numVectors", 100_000);
  private static final int DIMENSIONS = Integer.getInteger("vector.bench.dimensions", 128);
  private static final int NUM_QUERIES = Integer.getInteger("vector.bench.numQueries", 200);
  private static final int K = Integer.getInteger("vector.bench.k", 10);
  private static final int BATCH_SIZE = Integer.getInteger("vector.bench.batchSize", 5_000);
  private static final int NUM_CLUSTERS = Integer.getInteger("vector.bench.clusters", Math.max(10, NUM_VECTORS / 200));
  private static final float CLUSTER_SPREAD = Float.parseFloat(
      System.getProperty("vector.bench.clusterSpread", Float.toString(ClusteredVectorDataset.defaultSpread(DIMENSIONS))));
  private static final int WARMUP_QUERIES = 20;
  private static final long SEED = 42L;

  @Test
  void runFullBenchmark() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.PROFILE.setValue("high-performance");

    System.out.println("=== VECTOR OPTIMIZATION BENCHMARK ===");
    System.out.printf("Vectors: %,d | Dimensions: %d | Queries: %d | K: %d | Batch: %,d%n",
        NUM_VECTORS, DIMENSIONS, NUM_QUERIES, K, BATCH_SIZE);
    System.out.println();

    System.out.printf("Generating data (%d clusters, spread %.4f per dimension)...", NUM_CLUSTERS, CLUSTER_SPREAD);
    final ClusteredVectorDataset dataset = ClusteredVectorDataset.generate(NUM_VECTORS, NUM_QUERIES, DIMENSIONS, NUM_CLUSTERS,
        CLUSTER_SPREAD, SEED);
    final float[][] dataVectors = dataset.data;
    final float[][] queryVectors = dataset.queries;
    System.out.println(" done.");

    System.out.print("Computing ground truth...");
    final int[][] groundTruth = dataset.exactNeighbors(K);
    System.out.println(" done.");

    System.out.println();
    System.out.println("--- Configuration: No Quantization ---");
    runConfig("NONE", VectorQuantizationType.NONE, false, dataVectors, queryVectors, groundTruth);

    FileUtils.deleteRecursively(new File(DB_PATH));
    System.out.println();
    System.out.println("--- Configuration: INT8 Quantization ---");
    runConfig("INT8", VectorQuantizationType.INT8, false, dataVectors, queryVectors, groundTruth);

    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private void runConfig(final String label, final VectorQuantizationType quant, final boolean approximate,
      final float[][] dataVectors, final float[][] queryVectors, final int[][] groundTruth) {

    final Runtime rt = Runtime.getRuntime();

    rt.gc();
    final long memBefore = rt.totalMemory() - rt.freeMemory();
    final long ingestStart = System.nanoTime();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Vec");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          final String quantStr = quant == VectorQuantizationType.NONE ? "" :
              String.format(", \"quantization\": \"%s\"", quant.name());
          db.command("sql", String.format(
              "CREATE INDEX ON Vec (embedding) LSM_VECTOR METADATA { \"dimensions\": %d, \"similarity\": \"COSINE\"%s }",
              DIMENSIONS, quantStr));
        });

        int inserted = 0;
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Vec").set("id", i).set("embedding", dataVectors[i]).save();
          inserted++;
          if (inserted % BATCH_SIZE == 0) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
      }

      final long ingestEnd = System.nanoTime();
      final double ingestSec = (ingestEnd - ingestStart) / 1e9;
      final double vectorsPerSec = NUM_VECTORS / ingestSec;

      System.out.printf("[%s] Ingestion: %.2fs (%,.0f vectors/sec)%n", label, ingestSec, vectorsPerSec);

      final long reopenStart = System.nanoTime();
      try (final Database db = factory.open()) {
        final long reopenEnd = System.nanoTime();
        final double reopenSec = (reopenEnd - reopenStart) / 1e9;

        final LSMVectorIndex index = getVectorIndex(db, "Vec", "embedding");

        final long buildStart = System.nanoTime();
        index.findNeighborsFromVector(queryVectors[0], K);
        final long buildEnd = System.nanoTime();
        final double buildSec = (buildEnd - buildStart) / 1e9;

        rt.gc();
        final long memAfterBuild = rt.totalMemory() - rt.freeMemory();
        final long memUsedMB = (memAfterBuild - memBefore) / (1024 * 1024);

        System.out.printf("[%s] Reopen: %.2fs | Graph build (first query): %.2fs | Memory delta: %dMB%n",
            label, reopenSec, buildSec, memUsedMB);

        for (int i = 0; i < WARMUP_QUERIES; i++) {
          if (approximate)
            index.findNeighborsFromVectorApproximate(queryVectors[i % queryVectors.length], K);
          else
            index.findNeighborsFromVector(queryVectors[i % queryVectors.length], K);
        }

        final long[] latencies = new long[NUM_QUERIES];
        final List<Set<RID>> resultSets = new ArrayList<>(NUM_QUERIES);

        for (int q = 0; q < NUM_QUERIES; q++) {
          final long qStart = System.nanoTime();
          final List<Pair<RID, Float>> results;
          if (approximate)
            results = index.findNeighborsFromVectorApproximate(queryVectors[q], K);
          else
            results = index.findNeighborsFromVector(queryVectors[q], K);
          latencies[q] = System.nanoTime() - qStart;

          final Set<RID> rids = new HashSet<>();
          for (final Pair<RID, Float> p : results)
            rids.add(p.getFirst());
          resultSets.add(rids);
        }

        Arrays.sort(latencies);
        final double meanUs = Arrays.stream(latencies).average().orElse(0) / 1e3;
        final double p50Us = latencies[NUM_QUERIES / 2] / 1e3;
        final double p95Us = latencies[(int) (NUM_QUERIES * 0.95)] / 1e3;
        final double p99Us = latencies[(int) (NUM_QUERIES * 0.99)] / 1e3;

        System.out.printf("[%s] Search latency: mean=%.0fus  p50=%.0fus  p95=%.0fus  p99=%.0fus%n",
            label, meanUs, p50Us, p95Us, p99Us);

        final RID[] ridsByInsertOrder = new RID[NUM_VECTORS];
        db.iterateType("Vec", false).forEachRemaining(rec -> {
          final int id = rec.asDocument().getInteger("id");
          ridsByInsertOrder[id] = rec.getIdentity();
        });

        // A query that found nothing is a recall of 0, not a query to leave out of the mean
        double totalRecall = 0;
        int validQueries = 0;
        for (int q = 0; q < NUM_QUERIES; q++) {
          final Set<RID> resultRids = resultSets.get(q);

          int hits = 0;
          for (final int gtId : groundTruth[q]) {
            if (gtId >= 0 && gtId < ridsByInsertOrder.length && ridsByInsertOrder[gtId] != null) {
              if (resultRids.contains(ridsByInsertOrder[gtId]))
                hits++;
            }
          }
          totalRecall += (double) hits / groundTruth[q].length;
          validQueries++;
        }

        final double avgRecall = validQueries > 0 ? totalRecall / validQueries : 0;
        System.out.printf("[%s] Recall@%d: %.3f (%d queries)%n", label, K, avgRecall, validQueries);

        final Map<String, Long> stats = index.getStats();
        System.out.printf("[%s] Stats: fetchGraph=%d fetchDoc=%d fetchQuantized=%d%n",
            label,
            stats.getOrDefault("vectorFetchFromGraph", 0L),
            stats.getOrDefault("vectorFetchFromDocuments", 0L),
            stats.getOrDefault("vectorFetchFromQuantized", 0L));
      }
    }
  }

  private LSMVectorIndex getVectorIndex(final Database db, final String typeName, final String propertyName) {
    return (LSMVectorIndex) db.getSchema().getType(typeName)
        .getPolymorphicIndexByProperties(propertyName).getIndexesOnBuckets()[0];
  }
}
