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
 * Throughput benchmark for the {@link GraphBatch} bulk-load paths that PR #5950 touched, used to
 * answer the perf questions left open by its review: the {@code synchronizedMap(LRUCache)} chunk-RID
 * cache serialization on the parallel-flush hot path (review cycle 1), and the boxed-{@code Long}
 * incoming-head undo log added on the sequential drain path (review cycle 4).
 * <p>
 * <b>This file deliberately uses only builder methods that exist BOTH before and after the PR</b>
 * ({@code withBatchSize}, {@code withCommitEvery}, {@code withParallelFlush},
 * {@code withEdgeListInitialSize}) so the exact same source compiles and runs against the pre-PR
 * baseline for a true before/after comparison. It therefore measures the DEFAULT-configured loader,
 * which is what a real caller gets.
 * <p>
 * Not a correctness test - {@code @Tag("benchmark")} keeps it out of regular CI runs per the repo's
 * convention. Run explicitly with:
 * <pre>
 * mvn -pl engine test -Dtest=GraphBatchDrainPerfBenchmarkTest -DexcludedGroups=
 * </pre>
 * Each configuration runs {@link #ITERATIONS} times; the per-iteration numbers and the median are
 * both reported, because a shared dev machine produces enough noise that a single sample is not
 * meaningful.
 */
@Tag("benchmark")
class GraphBatchDrainPerfBenchmark {

  /** Distinct vertices. Kept under the post-PR default 1M chunk-cache capacity on purpose: the
   *  lock-contention cost under test is paid on every cache get/put, eviction or not, so this
   *  isolates contention from eviction-induced disk re-reads. */
  private static final int VERTEX_COUNT = 100_000;
  private static final int EDGE_COUNT   = 1_000_000;
  private static final int ITERATIONS   = 5;

  private static final String VERTEX_TYPE = "BenchPerson";
  private static final String EDGE_TYPE   = "BENCH_KNOWS";

  @Test
  void benchmarkParallelAndSequentialDrain() {
    // One shared random edge list for every iteration and both configurations, so the two sides
    // differ only in the code under test, never in the data.
    final Random rng = new Random(4242);
    final int[] edgeSrc = new int[EDGE_COUNT];
    final int[] edgeDst = new int[EDGE_COUNT];
    for (int i = 0; i < EDGE_COUNT; i++) {
      edgeSrc[i] = rng.nextInt(VERTEX_COUNT);
      edgeDst[i] = rng.nextInt(VERTEX_COUNT);
      while (edgeDst[i] == edgeSrc[i])
        edgeDst[i] = rng.nextInt(VERTEX_COUNT);
    }

    final StringBuilder report = new StringBuilder();
    report.append(String.format("%n=== GraphBatch drain throughput (PR #5950 perf check) ===%n"));
    report.append(String.format("Vertices: %d, Edges: %d, iterations: %d%n",
        VERTEX_COUNT, EDGE_COUNT, ITERATIONS));

    final long[] parallelMs = new long[ITERATIONS];
    final long[] sequentialMs = new long[ITERATIONS];

    // Alternate parallel/sequential within each iteration rather than running all of one then all of
    // the other, so any slow drift on the host (thermal, a background build) hits both sides evenly.
    for (int it = 0; it < ITERATIONS; it++) {
      parallelMs[it] = runOnce(true, edgeSrc, edgeDst, it);
      sequentialMs[it] = runOnce(false, edgeSrc, edgeDst, it);
    }

    appendResult(report, "parallelFlush=true  (chunk-RID cache contention)", parallelMs);
    appendResult(report, "parallelFlush=false (incoming-head undo log)    ", sequentialMs);

    // Deliberately printed rather than asserted: there is no meaningful absolute threshold to gate
    // on, and a timing assertion on a shared runner would just be flaky. The comparison against the
    // baseline branch is done by running this same test on both and diffing these numbers.
    System.out.println(report);
  }

  private long runOnce(final boolean parallelFlush, final int[] edgeSrc, final int[] edgeDst, final int iteration) {
    final String path = "target/databases/GraphBatchDrainPerf_" + (parallelFlush ? "par" : "seq") + "_" + iteration;
    FileUtils.deleteRecursively(new File(path));

    final Database db = new DatabaseFactory(path).create();
    try {
      db.transaction(() -> {
        db.getSchema().createVertexType(VERTEX_TYPE);
        db.getSchema().createEdgeType(EDGE_TYPE);
      });

      final RID[] vertexRIDs;
      final long start;
      try (final GraphBatch batch = GraphBatch.builder(db)
          .withBatchSize(100_000)
          .withEdgeListInitialSize(256)
          .withParallelFlush(parallelFlush)
          .build()) {

        // Vertex creation is outside the measured window: it is untouched by this PR, and including
        // it would dilute the drain/cache costs actually under test.
        vertexRIDs = batch.createVertices(VERTEX_TYPE, VERTEX_COUNT);

        start = System.nanoTime();
        for (int i = 0; i < EDGE_COUNT; i++)
          batch.newEdge(vertexRIDs[edgeSrc[i]], EDGE_TYPE, vertexRIDs[edgeDst[i]]);
        // close() runs the final flush AND the deferred incoming-edge drain, so it must be inside the
        // measured window - the drain is precisely what this PR changed.
      }
      return (System.nanoTime() - start) / 1_000_000L;
    } finally {
      db.drop();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  private void appendResult(final StringBuilder report, final String label, final long[] samples) {
    final long[] sorted = samples.clone();
    java.util.Arrays.sort(sorted);
    final long median = sorted[sorted.length / 2];
    final StringBuilder raw = new StringBuilder();
    for (int i = 0; i < samples.length; i++) {
      if (i > 0)
        raw.append(", ");
      raw.append(samples[i]);
    }
    report.append(String.format("%s  median=%5d ms  (%.0f edges/sec)  runs=[%s]%n",
        label, median, EDGE_COUNT / (median / 1000.0), raw));
  }
}
