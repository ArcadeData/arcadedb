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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Large-corpus throughput benchmark for the {@code LSM_SPARSE_VECTOR} index. Sized to
 * characterize how serial BMW DAAT scales at 1M and 10M sparse vectors so we can see whether
 * per-segment parallel scoring is worth its complexity. Tagged {@code benchmark} so it stays out
 * of default CI runs.
 * <p>
 * 10M brute-force scoring is too slow to run every iteration, so the brute-force comparison is
 * limited to a single iteration at that scale. The index path always runs the full warmup +
 * iterations sweep so the per-query number is statistically meaningful.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class LSMSparseVectorIndexLargeBenchmark extends TestHelper {

  private static final String TYPE_NAME   = "LargeSparseDoc";
  private static final String IDX_NAME    = "LargeSparseDoc[tokens,weights]";
  private static final int    DIMENSIONS  = 30_000;        // SPLADE-scale vocab
  private static final int    NNZ_PER_DOC = 30;
  private static final int    Q_NNZ       = 10;
  private static final int    K           = 10;
  private static final int    WARMUP      = 3;
  private static final int    ITERATIONS  = 5;

  /**
   * 1M sparse vectors. Aim is to see the effect of growing posting lists on BMW skip efficiency
   * vs. a brute-force scan at a corpus size that's large enough to make the difference visible
   * but small enough to fit in working memory comfortably.
   */
  @Test
  void oneMillionDocs() {
    runScale(1_000_000, /* runFullBruteForce */ true);
  }

  /**
   * 10M sparse vectors. Brute force is run for a single iteration only since each scan reads
   * every posting list end-to-end. The index path runs the full warmup + iterations sweep.
   */
  @Test
  void tenMillionDocs() {
    runScale(10_000_000, /* runFullBruteForce */ false);
  }

  // ---------- harness ----------

  private void runScale(final int docCount, final boolean runFullBruteForce) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();
    });

    System.out.printf("%n=== Loading %,d sparse vectors (%d nnz/doc, %,d-dim) ===%n",
        docCount, NNZ_PER_DOC, DIMENSIONS);

    final long loadStart = System.nanoTime();
    final Random rnd = new Random(7L);
    int batchOpen = 0;
    final int batchSize = 5_000;
    database.begin();
    for (int i = 0; i < docCount; i++) {
      final int[] indices = new int[NNZ_PER_DOC];
      final float[] values = new float[NNZ_PER_DOC];
      final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
      for (int j = 0; j < NNZ_PER_DOC; j++) {
        int dim;
        do {
          // Skewed: half the nnz hit a 1000-dim head, half are spread across the full vocab.
          dim = j < NNZ_PER_DOC / 2 ? rnd.nextInt(1000) : rnd.nextInt(DIMENSIONS);
        } while (!picked.add(dim));
        indices[j] = dim;
        values[j] = 0.1f + rnd.nextFloat();
      }

      final MutableDocument doc = database.newDocument(TYPE_NAME);
      doc.set("tokens", indices);
      doc.set("weights", values);
      doc.save();

      if (++batchOpen == batchSize) {
        database.commit();
        database.begin();
        batchOpen = 0;
        if ((i + 1) % (docCount / 10) == 0)
          System.out.printf("  loaded %,d / %,d (%d%%) - %.1fs elapsed%n", i + 1, docCount,
              ((i + 1) * 100) / docCount, (System.nanoTime() - loadStart) / 1e9);
      }
    }
    database.commit();
    final long loadElapsedMs = (System.nanoTime() - loadStart) / 1_000_000;
    System.out.printf("Load: %,d ms (%.0f docs/sec)%n", loadElapsedMs, docCount * 1000.0 / loadElapsedMs);

    final long count = database.countType(TYPE_NAME, false);
    assertThat(count).isEqualTo((long) docCount);

    // Build query workload.
    final int[][] queryIdx = new int[ITERATIONS + WARMUP][];
    final float[][] queryVal = new float[ITERATIONS + WARMUP][];
    final Random qRnd = new Random(13L);
    for (int q = 0; q < queryIdx.length; q++) {
      final int[] qi = new int[Q_NNZ];
      final float[] qv = new float[Q_NNZ];
      final HashSet<Integer> picked = new HashSet<>(Q_NNZ);
      for (int j = 0; j < Q_NNZ; j++) {
        int dim;
        do {
          dim = j < Q_NNZ / 2 ? qRnd.nextInt(1000) : qRnd.nextInt(DIMENSIONS);
        } while (!picked.add(dim));
        qi[j] = dim;
        qv[j] = 0.1f + qRnd.nextFloat();
      }
      queryIdx[q] = qi;
      queryVal[q] = qv;
    }

    System.out.printf("%n=== Index-backed `vector.sparseNeighbors` (K=%d) ===%n", K);
    final long indexNs = benchIndex(queryIdx, queryVal);

    System.out.printf("%n=== Brute-force `vector.sparseDot` over %,d docs (K=%d, %s) ===%n", docCount, K,
        runFullBruteForce ? "warmup+5 iters" : "1 iter only - too slow at this scale");
    final long bfNs = benchBruteForce(queryIdx, queryVal, runFullBruteForce);
    final int bfIters = runFullBruteForce ? ITERATIONS : 1;

    System.out.printf("%nSpeedup: %.1fx (index %.2f ms/query vs brute %.0f ms/query)%n",
        ((double) bfNs / bfIters) / ((double) indexNs / ITERATIONS),
        indexNs / 1_000_000.0 / ITERATIONS,
        bfNs / 1_000_000.0 / bfIters);
  }

  private long benchIndex(final int[][] qi, final float[][] qv) {
    final String sql = "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))";
    for (int q = 0; q < WARMUP; q++)
      runIndexOnce(sql, qi[q], qv[q]);
    long totalNs = 0;
    for (int q = 0; q < ITERATIONS; q++) {
      final long start = System.nanoTime();
      final int seen = runIndexOnce(sql, qi[WARMUP + q], qv[WARMUP + q]);
      totalNs += System.nanoTime() - start;
      assertThat(seen).as("index path must return >= 1 result").isGreaterThan(0);
    }
    System.out.printf("vector.sparseNeighbors: avg %.2f ms/query over %d iterations%n",
        totalNs / 1_000_000.0 / ITERATIONS, ITERATIONS);
    return totalNs;
  }

  private int runIndexOnce(final String sql, final int[] qi, final float[] qv) {
    final ResultSet rs = database.query("sql", sql, IDX_NAME, qi, qv, K);
    int seen = 0;
    while (rs.hasNext()) {
      rs.next();
      seen++;
    }
    return seen;
  }

  private long benchBruteForce(final int[][] qi, final float[][] qv, final boolean runFull) {
    final String sql = "SELECT FROM " + TYPE_NAME
        + " ORDER BY `vector.sparseDot`("
        + "`vector.sparseCreate`(tokens, weights, " + DIMENSIONS + "),"
        + "`vector.sparseCreate`(?, ?, " + DIMENSIONS + ")"
        + ") DESC LIMIT " + K;
    if (runFull) {
      for (int q = 0; q < WARMUP; q++)
        runBruteForceOnce(sql, qi[q], qv[q]);
    }
    final int iters = runFull ? ITERATIONS : 1;
    long totalNs = 0;
    for (int q = 0; q < iters; q++) {
      final long start = System.nanoTime();
      final int seen = runBruteForceOnce(sql, qi[WARMUP + q], qv[WARMUP + q]);
      totalNs += System.nanoTime() - start;
      assertThat(seen).as("brute-force path must return >= 1 result").isGreaterThan(0);
    }
    System.out.printf("brute-force sparseDot: avg %.0f ms/query over %d iteration(s)%n",
        totalNs / 1_000_000.0 / iters, iters);
    return totalNs;
  }

  private int runBruteForceOnce(final String sql, final int[] qi, final float[] qv) {
    final ResultSet rs = database.query("sql", sql, qi, qv);
    int seen = 0;
    while (rs.hasNext()) {
      rs.next();
      seen++;
    }
    return seen;
  }
}
