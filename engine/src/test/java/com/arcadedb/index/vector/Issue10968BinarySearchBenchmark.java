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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;

/**
 * Issue #10968: per-query cost of a BINARY index against a NONE one on the same high-dimensional corpus, at the beam each
 * needs for the same recall. Run by hand; it logs one line per quantization and
 * beam, at SEVERE so the test logging shows it. {@code -Dissue10968.quantizations=BINARY} runs one arm only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue10968BinarySearchBenchmark extends TestHelper {
  private static final int   DIMENSIONS = 1536;
  private static final int   ROWS       = Integer.getInteger("issue10968.rows", 20_000);
  private static final int   QUERIES    = 200;
  private static final int   K          = 10;
  private static final int   ROUNDS     = Integer.getInteger("issue10968.rounds", 5);
  // Spread of a vector around its cluster centre, against centres of unit spread: the higher, the harder for one-bit codes
  private static final float NOISE      = Float.parseFloat(System.getProperty("issue10968.noise", "1.5"));

  @Test
  void binaryAgainstNone() {
    final Random random = new Random(7);
    final float[][] centroids = new float[100][DIMENSIONS];
    for (final float[] c : centroids)
      for (int d = 0; d < DIMENSIONS; d++)
        c[d] = (float) random.nextGaussian();
    final float[][] rows = new float[ROWS][];
    for (int i = 0; i < ROWS; i++)
      rows[i] = around(centroids[random.nextInt(centroids.length)], random);
    final float[][] queries = new float[QUERIES][];
    for (int i = 0; i < QUERIES; i++)
      queries[i] = around(centroids[random.nextInt(centroids.length)], random);

    final int[][] truth = new int[QUERIES][];
    for (int q = 0; q < QUERIES; q++)
      truth[q] = exactTopK(rows, queries[q]);

    final String[] arms = System.getProperty("issue10968.quantizations", "NONE,BINARY").split(",");
    final LSMVectorIndex[] indexes = new LSMVectorIndex[arms.length];
    final List<List<RID>> rids = new ArrayList<>();
    for (int a = 0; a < arms.length; a++) {
      final String type = "V" + arms[a];
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
      database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"quantization\": \"" + arms[a] + "\" }");
      final List<RID> armRids = new ArrayList<>(ROWS);
      for (int start = 0; start < ROWS; start += 1000) {
        final int from = start;
        database.transaction(() -> {
          for (int i = from; i < Math.min(ROWS, from + 1000); i++)
            armRids.add(database.newDocument(type).set("vector", rows[i]).save().getIdentity());
        });
      }
      rids.add(armRids);
      indexes[a] = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[vector]")).getIndexesOnBuckets()[0];
      indexes[a].findNeighborsFromVector(queries[0], K, 50); // builds the graph
    }

    for (final int ef : new int[] { 10, 20, 50, 100, 200 }) {
      for (final LSMVectorIndex index : indexes)
        for (int round = 0; round < 3; round++) // warm up
          for (final float[] q : queries)
            index.findNeighborsFromVector(q, K, ef);

      final long[] nanos = new long[arms.length];
      final int[] hits = new int[arms.length];
      // The arms take turns, in the opposite order every other round, so drift in clock, caches or heap over the run
      // lands on both of them instead of on whichever runs last
      for (int round = 0; round < ROUNDS; round++)
        for (int turn = 0; turn < arms.length; turn++) {
          final int a = round % 2 == 0 ? turn : arms.length - 1 - turn;
          final long start = System.nanoTime();
          for (int q = 0; q < QUERIES; q++) {
            final List<Pair<RID, Float>> found = indexes[a].findNeighborsFromVector(queries[q], K, ef);
            if (round < 2) {
              final Set<RID> expected = new HashSet<>();
              for (final int t : truth[q])
                expected.add(rids.get(a).get(t));
              for (final Pair<RID, Float> f : found)
                if (expected.contains(f.getFirst()))
                  hits[a]++;
            }
          }
          nanos[a] += System.nanoTime() - start;
        }

      for (int a = 0; a < arms.length; a++)
        LogManager.instance().log(this, Level.SEVERE, "Issue #10968 %s ef=%d recall@10=%.3f latency=%.1f us", arms[a], ef,
            hits[a] / (double) (Math.min(2, ROUNDS) * QUERIES * K), nanos[a] / 1000.0 / (ROUNDS * (double) QUERIES));
    }
  }

  private static float[] around(final float[] centroid, final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = centroid[d] + (float) random.nextGaussian() * NOISE;
    return v;
  }

  private static int[] exactTopK(final float[][] rows, final float[] query) {
    final int[] best = new int[K];
    final double[] bestScore = new double[K];
    Arrays.fill(bestScore, -2);
    for (int i = 0; i < rows.length; i++) {
      final double s = cosine(rows[i], query);
      if (s > bestScore[K - 1]) {
        int p = K - 1;
        while (p > 0 && bestScore[p - 1] < s) {
          bestScore[p] = bestScore[p - 1];
          best[p] = best[p - 1];
          p--;
        }
        bestScore[p] = s;
        best[p] = i;
      }
    }
    return best;
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }
}
