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
package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;

/**
 * Quantifies what the issue #5664 memory bound actually costs when it BINDS - i.e. once the
 * head-chunk RID cache starts evicting and call sites fall back to reading the vertex's head chunk
 * from disk.
 * <p>
 * This is deliberately NOT a before/after comparison against the pre-PR baseline: unbounded is the
 * pre-PR behavior, and the whole point of #5664 is that unbounded consumes 16-18 GB on the reported
 * 663M-vertex load. So the honest question is not "is bounded slower than unbounded" (it is, by
 * construction, on a cache miss) but "how much throughput does the bound cost", measured here by
 * varying only {@code withChunkCacheCapacity} on the same tree.
 * <p>
 * {@code effectivelyUnbounded} sizes the cache above the distinct-vertex count so nothing is ever
 * evicted, reproducing pre-PR cache behavior on post-PR code; the tighter capacities force eviction
 * at increasing rates.
 *
 * @see GraphBatchDrainPerfBenchmark for the before/after regression check against the baseline
 */
@Tag("benchmark")
class GraphBatchEvictionCostBenchmark {

  private static final int VERTEX_COUNT = 100_000;
  private static final int EDGE_COUNT   = 1_000_000;
  private static final int ITERATIONS   = 3;

  private static final String VERTEX_TYPE = "EvictPerson";
  private static final String EDGE_TYPE   = "EVICT_KNOWS";

  @Test
  void benchmarkEvictionCostAcrossCacheCapacities() {
    final Random rng = new Random(4242);
    final int[] edgeSrc = new int[EDGE_COUNT];
    final int[] edgeDst = new int[EDGE_COUNT];
    for (int i = 0; i < EDGE_COUNT; i++) {
      edgeSrc[i] = rng.nextInt(VERTEX_COUNT);
      edgeDst[i] = rng.nextInt(VERTEX_COUNT);
      while (edgeDst[i] == edgeSrc[i])
        edgeDst[i] = rng.nextInt(VERTEX_COUNT);
    }

    // 200_000 > VERTEX_COUNT, so no eviction ever happens: the pre-#5664 cache behavior.
    final int[] capacities = { 200_000, 50_000, 10_000, 1_000 };

    final StringBuilder report = new StringBuilder();
    report.append(String.format("%n=== GraphBatch chunk-cache eviction cost (issue #5664) ===%n"));
    report.append(String.format("Vertices: %d, Edges: %d, iterations: %d, parallelFlush=true%n",
        VERTEX_COUNT, EDGE_COUNT, ITERATIONS));

    long unboundedMedian = -1;
    for (final int capacity : capacities) {
      final long[] samples = new long[ITERATIONS];
      for (int it = 0; it < ITERATIONS; it++)
        samples[it] = runOnce(capacity, edgeSrc, edgeDst, it);

      final long[] sorted = samples.clone();
      java.util.Arrays.sort(sorted);
      final long median = sorted[sorted.length / 2];
      if (unboundedMedian < 0)
        unboundedMedian = median;

      final String note = capacity > VERTEX_COUNT
          ? "no eviction (pre-#5664 behavior)"
          : String.format("%+.1f%% vs no-eviction", 100.0 * (median - unboundedMedian) / unboundedMedian);
      report.append(String.format("chunkCacheCapacity=%7d  median=%5d ms  (%7.0f edges/sec)  %s%n",
          capacity, median, EDGE_COUNT / (median / 1000.0), note));
    }

    System.out.println(report);
  }

  private long runOnce(final int chunkCacheCapacity, final int[] edgeSrc, final int[] edgeDst, final int iteration) {
    final String path = "target/databases/GraphBatchEvictCost_" + chunkCacheCapacity + "_" + iteration;
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType(VERTEX_TYPE);
        db.getSchema().createEdgeType(EDGE_TYPE);
      });

      final long start;
      try (final GraphBatch batch = GraphBatch.builder(db)
          .withBatchSize(100_000)
          .withEdgeListInitialSize(256)
          .withParallelFlush(true)
          .withChunkCacheCapacity(chunkCacheCapacity)
          .build()) {

        final RID[] vertexRIDs = batch.createVertices(VERTEX_TYPE, VERTEX_COUNT);

        start = System.nanoTime();
        for (int i = 0; i < EDGE_COUNT; i++)
          batch.newEdge(vertexRIDs[edgeSrc[i]], EDGE_TYPE, vertexRIDs[edgeDst[i]]);
      }
      return (System.nanoTime() - start) / 1_000_000L;
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }
}
