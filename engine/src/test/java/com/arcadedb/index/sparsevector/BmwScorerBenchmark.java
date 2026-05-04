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

import com.arcadedb.database.RID;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

/**
 * Phase 2 benchmark: BMW DAAT vs brute force at 100k / 1M corpus sizes.
 * <p>
 * Tagged {@code benchmark} so it does not run on regular CI. Measures wall time on
 * representative top-K queries; absolute numbers depend on the host but the BMW/BF ratio
 * should reliably favour BMW at 1M.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class BmwScorerBenchmark {

  @Test
  void compareBmwVsBruteForce100k(@TempDir final Path tmp) throws IOException {
    runScale(tmp, 100_000, 1_000, 30, "100k");
  }

  @Test
  void compareBmwVsBruteForce1M(@TempDir final Path tmp) throws IOException {
    runScale(tmp, 1_000_000, 5_000, 30, "1M");
  }

  // ---------- helpers ----------

  private static void runScale(final Path tmp, final int docCount, final int dims, final int nnzPerDoc, final String label)
      throws IOException {
    final Path file = tmp.resolve("bench.sparseseg");

    // Write segment.
    final long writeStart = System.nanoTime();
    long postings = 0L;
    final TreeMap<Integer, TreeMap<RID, Float>> byDim = new TreeMap<>();
    final Random rnd = new Random(0x4242L ^ docCount);
    for (int d = 0; d < docCount; d++) {
      final RID rid = new RID(0, d * 100L + 1);
      final HashSet<Integer> picked = new HashSet<>(nnzPerDoc * 2);
      while (picked.size() < nnzPerDoc) {
        final int dim = rnd.nextInt(dims);
        if (picked.add(dim)) {
          byDim.computeIfAbsent(dim, k -> new TreeMap<>()).put(rid, 0.1f + rnd.nextFloat());
          postings++;
        }
      }
    }
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      for (final var e : byDim.entrySet()) {
        w.startDim(e.getKey());
        for (final var p : e.getValue().entrySet())
          w.appendPosting(p.getKey(), p.getValue());
        w.endDim();
      }
      w.finish();
    }
    final long writeMs = (System.nanoTime() - writeStart) / 1_000_000L;

    final int[] queryDims = pickRandomDims(dims, 10, 0xBADL);
    final float[] queryWeights = new float[queryDims.length];
    final Random wrnd = new Random(0xFEEDL);
    for (int i = 0; i < queryWeights.length; i++)
      queryWeights[i] = 0.2f + wrnd.nextFloat();
    final int k = 10;

    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      final SparseSegmentReader[] segs = { r };

      // Warmup.
      for (int w = 0; w < 3; w++) {
        runBmw(segs, queryDims, queryWeights, k);
        BruteForceScorer.topK(queryDims, queryWeights, segs, k);
      }

      // BMW.
      final long bmwStart = System.nanoTime();
      List<RidScore> bmwResult = null;
      for (int it = 0; it < 5; it++)
        bmwResult = runBmw(segs, queryDims, queryWeights, k);
      final long bmwMs = (System.nanoTime() - bmwStart) / 1_000_000L / 5L;

      // Brute force.
      final long bfStart = System.nanoTime();
      List<RidScore> bfResult = null;
      for (int it = 0; it < 5; it++)
        bfResult = BruteForceScorer.topK(queryDims, queryWeights, segs, k);
      final long bfMs = (System.nanoTime() - bfStart) / 1_000_000L / 5L;

      System.out.printf(
          "%s corpus: %,d docs, %,d postings, %d query dims, k=%d | write %d ms | BMW %d ms (top score %.3f) | brute force %d ms | speedup %.2fx%n",
          label, docCount, postings, queryDims.length, k, writeMs, bmwMs,
          bmwResult.isEmpty() ? Float.NaN : bmwResult.getFirst().score(), bfMs, (double) bfMs / Math.max(1L, bmwMs));
    }
  }

  private static List<RidScore> runBmw(final SparseSegmentReader[] segs, final int[] queryDims, final float[] queryWeights,
      final int k) throws IOException {
    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = DimCursor.open(queryDims[i], segs);
      return BmwScorer.topK(queryDims, queryWeights, cursors, k);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
  }

  private static int[] pickRandomDims(final int dims, final int n, final long seed) {
    final Random rnd = new Random(seed);
    final HashSet<Integer> seen = new HashSet<>(n * 2);
    while (seen.size() < n)
      seen.add(rnd.nextInt(dims));
    return seen.stream().mapToInt(Integer::intValue).toArray();
  }
}
