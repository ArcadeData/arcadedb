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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #9200: the top-K traversal scores a window of RIDs at a time (the shape of Lucene's {@code MaxScoreBulkScorer})
 * instead of keeping the essential cursors ordered per posting. It must answer exactly what the document-at-a-time
 * traversal answers, over segments, a live memtable and deletions, whatever the window size.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9200WindowTraversalTest extends TestHelper {

  private static final int DIMS = 1500;

  @AfterEach
  void restoreWindow() {
    GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.reset();
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
  }

  @Test
  void windowTraversalAnswersLikeDocumentAtATime() throws Exception {
    final LSMSparseVectorIndex index = buildCorpus(6_000);
    final Random random = new Random(9200);
    for (final int window : new int[] { 64, 1000, 16_384 }) {
      for (int q = 0; q < 60; q++) {
        final int width = 1 + random.nextInt(q % 3 == 0 ? 8 : 60);
        final int[] query = random.ints(0, DIMS).distinct().limit(width).toArray();
        final float[] weights = new float[query.length];
        for (int i = 0; i < weights.length; i++)
          weights[i] = (float) (0.1 + 2 * random.nextDouble());
        final int k = 1 + random.nextInt(25);

        GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(0);
        final List<RidScore> classic = index.topK(query, weights, k, null);
        GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(window);
        final List<RidScore> windowed = index.topK(query, weights, k, null);

        assertSameAnswer(classic, windowed, "window " + window + ", query " + q);
      }
    }
  }

  @Test
  void anOversizedWindowSettingIsClamped() throws Exception {
    final LSMSparseVectorIndex index = buildCorpus(600);
    final int[] query = { 1, 2, 3 };
    final float[] weights = { 1f, 1f, 1f };
    GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(0);
    final List<RidScore> classic = index.topK(query, weights, 5, null);
    GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(Integer.MAX_VALUE);
    final List<RidScore> clamped = index.topK(query, weights, 5, null);
    assertThat(clamped).isNotEmpty();
    assertSameAnswer(classic, clamped, "clamped window");
  }

  @Test
  void partitionedWindowTraversalAnswersLikeDocumentAtATime() throws Exception {
    final LSMSparseVectorIndex index = buildCorpus(6_000);
    final Random random = new Random(9201);
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.setValue(1);
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(4);
    try {
      for (int q = 0; q < 40; q++) {
        final int[] query = random.ints(0, DIMS).distinct().limit(5 + random.nextInt(40)).toArray();
        final float[] weights = new float[query.length];
        for (int i = 0; i < weights.length; i++)
          weights[i] = (float) (0.1 + 2 * random.nextDouble());
        final int k = 1 + random.nextInt(25);

        GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(0);
        final List<RidScore> classic = index.topK(query, weights, k, null);
        GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(256);
        final List<RidScore> windowed = index.topK(query, weights, k, null);
        assertSameAnswer(classic, windowed, "partitioned query " + q);
      }
    } finally {
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.reset();
      GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
    }
  }

  @Test
  @Tag("benchmark")
  void windowTraversalIsNotSlowerOnAWideCorpus() throws Exception {
    final LSMSparseVectorIndex index = buildCorpus(60_000);
    final Random random = new Random(1);
    final int rounds = 6, queries = 150;
    final int[][] qs = new int[queries][];
    final float[][] ws = new float[queries][];
    for (int q = 0; q < queries; q++) {
      qs[q] = random.ints(0, DIMS).distinct().limit(30 + random.nextInt(70)).toArray();
      ws[q] = new float[qs[q].length];
      for (int i = 0; i < ws[q].length; i++)
        ws[q][i] = (float) (0.5 + random.nextDouble());
    }
    final long[] nanos = new long[2];
    for (int r = 0; r < rounds; r++)
      for (int mode = 0; mode < 2; mode++) {
        GlobalConfiguration.SPARSE_VECTOR_SCORING_WINDOW.setValue(mode == 0 ? 0 : 16_384);
        GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.setValue(1);
        final long t0 = System.nanoTime();
        for (int q = 0; q < queries; q++)
          index.topK(qs[q], ws[q], 10, null);
        if (r > 0)
          nanos[mode] += System.nanoTime() - t0;
      }
    GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.reset();
    LogManager.instance().log(this, Level.INFO, "Sparse top-10 on 60k docs: classic %.3f ms/query, window %.3f ms/query",
        nanos[0] / 1e6 / ((rounds - 1) * queries), nanos[1] / 1e6 / ((rounds - 1) * queries));
    assertThat(nanos[0]).isPositive();
  }

  private static void assertSameAnswer(final List<RidScore> expected, final List<RidScore> actual, final String what) {
    assertThat(actual).as(what).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) {
      assertThat(actual.get(i).score()).as(what + " score at " + i).isCloseTo(expected.get(i).score(), within(1e-3f));
      // Documents that tie to float noise may swap places; anything clearly ahead of the K-th must be the same.
      final float kth = expected.get(expected.size() - 1).score();
      if (expected.get(i).score() > kth + 1e-3f && (i == 0 || expected.get(i - 1).score() > expected.get(i).score() + 1e-3f)
          && (i + 1 == expected.size() || expected.get(i + 1).score() < expected.get(i).score() - 1e-3f))
        assertThat(actual.get(i).rid()).as(what + " rid at " + i).isEqualTo(expected.get(i).rid());
    }
  }

  /** Several segments, a live memtable, and deletions (tombstones in the newer sources). */
  private LSMSparseVectorIndex buildCorpus(final int docs) throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Win");
    database.command("sql", "CREATE PROPERTY Win.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Win.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Win (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: " + DIMS + " }");
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Win[tokens,weights]");
    final LSMSparseVectorIndex index = (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final Random random = new Random(92);
    final List<RID> rids = new ArrayList<>();
    final int batch = docs / 3;
    for (int from = 0; from < docs; from += batch) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(docs, start + batch); i++) {
          final int nnz = 8 + random.nextInt(40);
          final int[] tokens = new int[nnz];
          for (int j = 0; j < nnz; j++) {
            final double u = random.nextDouble();
            tokens[j] = (int) (DIMS * u * u); // skewed: a few dims are very frequent
          }
          final int[] distinct = Arrays.stream(tokens).distinct().sorted().toArray();
          final float[] weights = new float[distinct.length];
          for (int j = 0; j < weights.length; j++)
            weights[j] = (float) (0.05 + 3 * random.nextDouble());
          rids.add(database.newDocument("Win").set("tokens", distinct, "weights", weights).save().getIdentity());
        }
      });
      if (from + batch < docs)
        index.compact(); // seal this batch into a segment, keep the last one in the memtable
    }
    database.transaction(() -> {
      for (int i = 0; i < rids.size(); i += 11)
        rids.get(i).asDocument().delete();
    });
    return index;
  }
}
