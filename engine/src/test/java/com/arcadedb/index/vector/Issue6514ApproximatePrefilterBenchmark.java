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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Issue #6514 follow-up to the issue #6502 benchmark: checks whether {@code findNeighborsFromVectorApproximate}'s
 * (PQ zero-disk-I/O) pre-filter crossover sits near the same {@link GlobalConfiguration#VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY}
 * default (20%) the plain k-NN path uses. PQ scoring is much cheaper per-candidate than exact scoring, which the
 * issue flagged as likely to shift the crossover - this benchmark is what settles it rather than assuming it over.
 * Same fixture shape as {@link Issue6502PrefilterLatencyBenchmark}: 20,000 x 128, EUCLIDEAN.
 * <p>
 * Usage: {@code mvn test -pl engine -Dtest=Issue6514ApproximatePrefilterBenchmark -DfailIfNoTests=false}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue6514ApproximatePrefilterBenchmark {
  private static final String DB_PATH     = "target/test-databases/Issue6514ApproximatePrefilterBenchmark";
  private static final int    NUM_VECTORS = Integer.getInteger("vector.bench.numVectors", 20_000);
  private static final int    DIMENSIONS  = Integer.getInteger("vector.bench.dimensions", 128);
  private static final int    K           = 10;
  private static final int    NUM_QUERIES = 200;
  private static final int    WARMUP      = 20;
  private static final long   SEED        = 42L;

  @Test
  void measurePrefilterVsGraphWalk() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.PROFILE.setValue("high-performance");

    final Random rng = new Random(SEED);
    final float[][] vectors = randomVectors(NUM_VECTORS, DIMENSIONS, rng);

    final RID[] rids = new RID[NUM_VECTORS];
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Vec");
          type.createProperty("v", Type.ARRAY_OF_FLOATS);
          db.command("sql",
              "CREATE INDEX ON Vec (v) LSM_VECTOR METADATA "
                  + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\", "
                  + "\"maxConnections\": 32, \"beamWidth\": 100, \"quantization\": \"PRODUCT\" }");
        });
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          rids[i] = db.newDocument("Vec").set("v", vectors[i]).save().getIdentity();
          if ((i + 1) % 5_000 == 0) {
            db.commit();
            db.begin();
          }
        }
        if (db.isTransactionActive())
          db.commit();
      }

      try (final Database db = factory.open()) {
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Vec")
            .getPolymorphicIndexByProperties("v").getIndexesOnBuckets()[0];
        // Force the graph build up front so it is not attributed to the first measurement below.
        index.findNeighborsFromVector(vectors[0], K, 100);
        if (!index.isPQSearchAvailable())
          throw new IllegalStateException("fixture must train PQ to exercise the approximate path");

        final float[][] queries = randomVectors(NUM_QUERIES, DIMENSIONS, rng);

        System.out.println("=== Issue #6514: PQ-approximate RID allow-list pre-filter plan ===");
        System.out.printf("Vectors: %,d | Dimensions: %d | K: %d%n%n", NUM_VECTORS, DIMENSIONS, K);

        measure(index, "unfiltered", queries, null);
        // Both plans measured explicitly at every selectivity (selectivity 1.0 forces pre-filter, 0 forces the
        // graph walk), instead of only the as-configured default: PQ scoring is so much cheaper per-candidate than
        // exact scoring that the crossover the issue predicted would move turns out to sit BELOW the #6502 default,
        // not above it, and that is only visible by comparing both plans side by side at the same selectivity.
        for (final int allowedCount : new int[] { NUM_VECTORS / 4, NUM_VECTORS / 5, (NUM_VECTORS * 3) / 20,
            NUM_VECTORS / 10, NUM_VECTORS / 11, NUM_VECTORS / 12, NUM_VECTORS / 14, NUM_VECTORS / 16,
            NUM_VECTORS / 20, NUM_VECTORS / 25, NUM_VECTORS / 33, NUM_VECTORS / 50,
            NUM_VECTORS / 100, NUM_VECTORS / 1000, 5 }) {
          final Set<RID> allowed = randomSubset(rids, allowedCount, rng);
          final String label = allowedCount + " RIDs (" + (100.0 * allowedCount / NUM_VECTORS) + "%)";
          measureBothPlans(index, label, queries, allowed);
        }
      }
    }
  }

  private void measureBothPlans(final LSMVectorIndex index, final String label, final float[][] queries, final Set<RID> allowed) {
    GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.setValue(0f);
    try {
      measure(index, label + " graph-walk", queries, allowed);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.reset();
    }
    GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.setValue(1f);
    try {
      measure(index, label + " pre-filter", queries, allowed);
    } finally {
      GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.reset();
    }
  }

  private void measure(final LSMVectorIndex index, final String label, final float[][] queries, final Set<RID> allowed) {
    for (int i = 0; i < WARMUP; i++)
      index.findNeighborsFromVectorApproximate(queries[i % queries.length], K, allowed);

    final long[] latenciesNs = new long[NUM_QUERIES];
    for (int q = 0; q < NUM_QUERIES; q++) {
      final long start = System.nanoTime();
      final List<Pair<RID, Float>> results = index.findNeighborsFromVectorApproximate(queries[q], K, allowed);
      latenciesNs[q] = System.nanoTime() - start;
      if (results.isEmpty())
        throw new IllegalStateException("fixture produced no results for " + label);
    }

    Arrays.sort(latenciesNs);
    final double p50Ms = latenciesNs[latenciesNs.length / 2] / 1e6;
    final double p95Ms = latenciesNs[(int) (latenciesNs.length * 0.95)] / 1e6;
    System.out.printf("[%-32s] p50=%8.3f ms  p95=%8.3f ms%n", label, p50Ms, p95Ms);
  }

  private Set<RID> randomSubset(final RID[] rids, final int count, final Random rng) {
    final Set<RID> subset = new LinkedHashSet<>();
    while (subset.size() < count)
      subset.add(rids[rng.nextInt(rids.length)]);
    return subset;
  }

  private float[][] randomVectors(final int count, final int dims, final Random rng) {
    final float[][] vectors = new float[count][dims];
    for (int i = 0; i < count; i++)
      for (int d = 0; d < dims; d++)
        vectors[i][d] = (float) rng.nextGaussian();
    return vectors;
  }
}
