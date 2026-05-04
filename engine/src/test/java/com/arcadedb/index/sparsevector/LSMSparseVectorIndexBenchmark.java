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
 * Throughput benchmark for the {@code LSM_SPARSE_VECTOR} index, comparing the WAND-based
 * {@code vector.sparseNeighbors} against an indexless brute-force {@code vector.sparseDot} scan
 * over the same corpus. Tagged {@code benchmark} so it stays out of default CI runs (kept under
 * 100k vectors so a single run completes in under a couple of minutes on a developer laptop).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class LSMSparseVectorIndexBenchmark extends TestHelper {

  private static final String TYPE_NAME   = "BenchmarkSparseDoc";
  private static final String IDX_NAME    = "BenchmarkSparseDoc[tokens,weights]";
  private static final int    DIMENSIONS  = 5_000;
  private static final int    DOC_COUNT   = 100_000;
  private static final int    NNZ_PER_DOC = 30;
  private static final int    Q_NNZ       = 10;
  private static final int    K           = 10;
  private static final int    WARMUP      = 3;
  private static final int    ITERATIONS  = 5;

  @Test
  void sparseNeighborsVsBruteForceDot() {
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

    System.out.println("\n=== Loading " + DOC_COUNT + " sparse vectors (" + NNZ_PER_DOC + " nnz/doc, "
        + DIMENSIONS + "-dim) ===");

    final long loadStart = System.nanoTime();
    final Random rnd = new Random(7L);
    int batchOpen = 0;
    database.begin();
    for (int i = 0; i < DOC_COUNT; i++) {
      final int[]   indices = new int[NNZ_PER_DOC];
      final float[] values  = new float[NNZ_PER_DOC];
      final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
      for (int j = 0; j < NNZ_PER_DOC; j++) {
        int dim;
        do {
          // Skewed: half the nnz hit a 100-dim head, half are spread across the full space.
          dim = j < NNZ_PER_DOC / 2 ? rnd.nextInt(100) : rnd.nextInt(DIMENSIONS);
        } while (!picked.add(dim));
        indices[j] = dim;
        values[j] = 0.1f + rnd.nextFloat();
      }

      final MutableDocument doc = database.newDocument(TYPE_NAME);
      doc.set("tokens", indices);
      doc.set("weights", values);
      doc.save();

      if (++batchOpen == 1_000) {
        database.commit();
        database.begin();
        batchOpen = 0;
      }
    }
    database.commit();
    final long loadElapsedMs = (System.nanoTime() - loadStart) / 1_000_000;
    System.out.printf("Load: %,d ms (%.0f docs/sec)%n", loadElapsedMs, DOC_COUNT * 1000.0 / loadElapsedMs);

    final long count = database.countType(TYPE_NAME, false);
    assertThat(count).isEqualTo((long) DOC_COUNT);

    final int[][]   queryIdx = new int[ITERATIONS + WARMUP][];
    final float[][] queryVal = new float[ITERATIONS + WARMUP][];
    final Random qRnd = new Random(13L);
    for (int q = 0; q < queryIdx.length; q++) {
      final int[]   qi = new int[Q_NNZ];
      final float[] qv = new float[Q_NNZ];
      final HashSet<Integer> picked = new HashSet<>(Q_NNZ);
      for (int j = 0; j < Q_NNZ; j++) {
        int dim;
        do {
          dim = j < Q_NNZ / 2 ? qRnd.nextInt(100) : qRnd.nextInt(DIMENSIONS);
        } while (!picked.add(dim));
        qi[j] = dim;
        qv[j] = 0.1f + qRnd.nextFloat();
      }
      queryIdx[q] = qi;
      queryVal[q] = qv;
    }

    System.out.println("\n=== Index-backed `vector.sparseNeighbors` (K=" + K + ") ===");
    final long indexNs = bench("vector.sparseNeighbors",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))", queryIdx, queryVal, K);

    System.out.println("\n=== Brute-force `vector.sparseDot` over " + DOC_COUNT + " docs (K=" + K + ") ===");
    final long bfNs = bench("brute-force sparseDot",
        "SELECT FROM " + TYPE_NAME
            + " ORDER BY `vector.sparseDot`("
            + "`vector.sparseCreate`(tokens, weights, " + DIMENSIONS + "),"
            + "`vector.sparseCreate`(?, ?, " + DIMENSIONS + ")"
            + ") DESC LIMIT " + K,
        queryIdx, queryVal, /* k baked into SQL */ -1);

    System.out.printf("%nSpeedup: %.1fx (index %.2f ms vs brute %.2f ms per query)%n",
        (double) bfNs / indexNs, indexNs / 1_000_000.0 / ITERATIONS, bfNs / 1_000_000.0 / ITERATIONS);
  }

  private long bench(final String label, final String sql,
      final int[][] queryIdx, final float[][] queryVal, final int kArg) {
    // Warm up.
    for (int q = 0; q < WARMUP; q++)
      runOnce(sql, queryIdx[q], queryVal[q], kArg);

    long totalNs = 0;
    for (int q = 0; q < ITERATIONS; q++) {
      final long start = System.nanoTime();
      final int seen = runOnce(sql, queryIdx[WARMUP + q], queryVal[WARMUP + q], kArg);
      totalNs += System.nanoTime() - start;
      assertThat(seen).as(label + " must return at least 1 result").isGreaterThan(0);
    }

    final double avgMs = totalNs / 1_000_000.0 / ITERATIONS;
    System.out.printf("%s: avg %.2f ms/query over %d iterations%n", label, avgMs, ITERATIONS);
    return totalNs;
  }

  private int runOnce(final String sql, final int[] qi, final float[] qv, final int kArg) {
    final ResultSet rs;
    if (kArg > 0)
      rs = database.query("sql", sql, IDX_NAME, qi, qv, kArg);
    else
      rs = database.query("sql", sql, qi, qv);

    int seen = 0;
    while (rs.hasNext()) {
      rs.next();
      seen++;
    }
    return seen;
  }
}
