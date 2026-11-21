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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The IDF modifier weighs a query dimension by its document frequency in the whole index, whatever bucket a document lives in.
 * <p>
 * Each bucket's sub-index scored with {@code N} the document count of the whole type but {@code df} the postings of ITS OWN
 * bucket, so two documents with the same vector scored differently (here by a factor of 350) just because their buckets held a
 * different number of documents with that dimension, and the merged top-K ranked them by that accident.
 */
class SparseIdfMultiBucketTest extends TestHelper {
  private static final int BIG_BUCKET_DOCS   = 200;
  private static final int SMALL_BUCKET_DOCS = 2;

  @Test
  void identicalVectorsScoreTheSameInEveryBucket() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR "
          + "METADATA {\"dimensions\": 16, \"modifier\": \"IDF\", \"weightQuantization\": \"FP32\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < BIG_BUCKET_DOCS; i++)
        database.command("sql", "INSERT INTO bucket:Doc_0 SET tokens = [5], weights = [1.0]");
      for (int i = 0; i < SMALL_BUCKET_DOCS; i++)
        database.command("sql", "INSERT INTO bucket:Doc_1 SET tokens = [5], weights = [1.0]");
    });

    final int total = BIG_BUCKET_DOCS + SMALL_BUCKET_DOCS;
    final List<Double> scores = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', [5], [1.0], 300))")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.add(((Number) row.getProperty("score")).doubleValue());
      }
    }

    // ln((N - df + 0.5) / (df + 0.5) + 1) with N = df = every document of the type
    final double expected = Math.log(0.5 / (total + 0.5) + 1.0);
    assertThat(scores).hasSize(total);
    assertThat(scores).allSatisfy(score -> assertThat(score).isCloseTo(expected, within(1e-6)));
  }

  private static final int     BUCKETS       = 4;
  private static final int[]   QUERY_DIMS    = { 5, 6, 7 };
  private static final float[] QUERY_VALUES = { 1.0f, 1.0f, 1.0f };

  /**
   * The document frequency is counted over every bucket of the type, so counting it per bucket search would make a query cost
   * the number of buckets times what it has to. A search over the whole type resolves it once: one pass per query dim in each
   * bucket, whatever the number of bucket searches it fans out into.
   */
  @Test
  void aFanOutSearchCountsTheDocumentFrequencyOnce() {
    createTypeWithBuckets();
    final long before = countDimCalls();

    final int rows = countRows("SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', [5, 6, 7], [1.0, 1.0, 1.0], 10))");

    assertThat(rows).isEqualTo(10);
    assertThat(countDimCalls() - before)
        .as("one document-frequency pass per query dim in each bucket, for the whole search and not for each bucket search")
        .isEqualTo((long) QUERY_DIMS.length * BUCKETS);
  }

  @Test
  void aGroupedFanOutSearchCountsTheDocumentFrequencyOnce() {
    createTypeWithBuckets();
    final long before = countDimCalls();

    final int rows = countRows("SELECT expand(`vector.sparseNeighbors`('Doc[tokens,weights]', [5, 6, 7], [1.0, 1.0, 1.0], 4, "
        + "{ groupBy: 'grp', groupSize: 2 }))");

    assertThat(rows).isGreaterThan(0);
    assertThat(countDimCalls() - before)
        .as("the grouped searches and the second phase that tops them up reuse the weights the pass resolved")
        .isEqualTo((long) QUERY_DIMS.length * BUCKETS);
  }

  @Test
  void aDirectSearchCountsTheDocumentFrequencyOncePerCall() {
    createTypeWithBuckets();
    final LSMSparseVectorIndex index = (LSMSparseVectorIndex) typeIndex().getIndexesOnBuckets()[0];

    final long before = countDimCalls();
    final List<RidScore> direct = index.topK(QUERY_DIMS, QUERY_VALUES, 5, null);
    assertThat(countDimCalls() - before).isEqualTo((long) QUERY_DIMS.length * BUCKETS);

    // The weights resolved up front score the same, and a search handed them counts nothing.
    final float[] weights = index.effectiveWeights(QUERY_DIMS, QUERY_VALUES);
    final long afterResolve = countDimCalls();
    final List<RidScore> weighted = index.topKWeighted(QUERY_DIMS, weights, 5, null, null);
    assertThat(countDimCalls()).isEqualTo(afterResolve);
    assertThat(weighted).isEqualTo(direct);

    // N and df are both the 12 documents of the type, whichever bucket index asks.
    final float expectedWeight = (float) Math.log(0.5 / (BUCKETS * 3 + 0.5) + 1.0);
    assertThat(weights).containsOnly(expectedWeight);

    // The grouped direct search counts once per call too.
    final long beforeGrouped = countDimCalls();
    index.topKGrouped(QUERY_DIMS, QUERY_VALUES, 3, 2, null, rid -> rid.getPosition());
    assertThat(countDimCalls() - beforeGrouped).isEqualTo((long) QUERY_DIMS.length * BUCKETS);
  }

  /**
   * A document frequency is kept while the index holds the same live postings, so a repeated query counts nothing, and is
   * counted again after anything that changes them: an insert, a delete, an update that drops a dim, a flush to a segment.
   */
  @Test
  void aCachedDocumentFrequencyIsDroppedWhenTheLivePostingsChange() {
    createTypeWithBuckets();
    final LSMSparseVectorIndex index = (LSMSparseVectorIndex) typeIndex().getIndexesOnBuckets()[0];
    final float[] n12 = index.effectiveWeights(QUERY_DIMS, QUERY_VALUES);
    assertThat(n12).containsOnly(idfWeight(12, 12));

    final long walks = countDimWalks();
    assertThat(index.effectiveWeights(QUERY_DIMS, QUERY_VALUES)).isEqualTo(n12);
    assertThat(countDimWalks()).as("the same live postings, so the counts are reused").isEqualTo(walks);

    // an insert holding only dim 5: N = 13, df(5) = 13, df(6) = df(7) = 12
    database.transaction(() -> database.command("sql", "INSERT INTO bucket:Doc_2 SET tokens = [5], weights = [1.0], grp = 'x'"));
    assertThat(index.effectiveWeights(QUERY_DIMS, QUERY_VALUES))
        .containsExactly(idfWeight(13, 13), idfWeight(13, 12), idfWeight(13, 12));

    // an update dropping dims 6 and 7 from a document of the first bucket: df(6) = df(7) = 11
    database.transaction(() -> database.command("sql",
        "UPDATE bucket:Doc_0 SET tokens = [5], weights = [1.0] LIMIT 1"));
    assertThat(index.effectiveWeights(QUERY_DIMS, QUERY_VALUES))
        .containsExactly(idfWeight(13, 13), idfWeight(13, 11), idfWeight(13, 11));

    // a delete of a document holding all three dims
    database.transaction(() -> database.command("sql", "DELETE FROM bucket:Doc_3 LIMIT 1"));
    assertThat(index.effectiveWeights(QUERY_DIMS, QUERY_VALUES))
        .containsExactly(idfWeight(12, 12), idfWeight(12, 10), idfWeight(12, 10));

    // the same live postings in segments instead of memtables: counted again over the new state, to the same answer
    final long beforeFlush = countDimWalks();
    for (final IndexInternal sub : typeIndex().getIndexesOnBuckets())
      sub.flush();
    assertThat(index.effectiveWeights(QUERY_DIMS, QUERY_VALUES))
        .containsExactly(idfWeight(12, 12), idfWeight(12, 10), idfWeight(12, 10));
    assertThat(countDimWalks()).isGreaterThan(beforeFlush);

    // and a delete after the flush masks a posting of a segment
    database.transaction(() -> database.command("sql", "DELETE FROM bucket:Doc_1 LIMIT 1"));
    assertThat(index.effectiveWeights(QUERY_DIMS, QUERY_VALUES))
        .containsExactly(idfWeight(11, 11), idfWeight(11, 9), idfWeight(11, 9));
  }

  private void createTypeWithBuckets() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY Doc.grp STRING");
      database.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR "
          + "METADATA {\"dimensions\": 16, \"modifier\": \"IDF\", \"weightQuantization\": \"FP32\"}");
    });
    database.transaction(() -> {
      for (int bucket = 0; bucket < BUCKETS; bucket++)
        for (int i = 0; i < 3; i++)
          database.command("sql", "INSERT INTO bucket:Doc_" + bucket + " SET tokens = [5, 6, 7], weights = [1.0, 1.0, 1.0], grp = 'g"
              + (bucket * 3 + i) + "'");
    });
  }

  private TypeIndex typeIndex() {
    return (TypeIndex) database.getSchema().getIndexByName("Doc[tokens,weights]");
  }

  /** Document-frequency passes made so far by every bucket index of the type. */
  private long countDimCalls() {
    long calls = 0L;
    for (final IndexInternal sub : typeIndex().getIndexesOnBuckets())
      calls += ((LSMSparseVectorIndex) sub).getEngine().countDimCallCount();
    return calls;
  }

  /** Document-frequency passes that walked postings, i.e. were not answered from the engines' caches. */
  private long countDimWalks() {
    long walks = 0L;
    for (final IndexInternal sub : typeIndex().getIndexesOnBuckets())
      walks += ((LSMSparseVectorIndex) sub).getEngine().countDimWalkCount();
    return walks;
  }

  /** The weight of a unit query weight on a dim held by {@code df} of the {@code n} documents of the type. */
  private static float idfWeight(final long n, final long df) {
    return (float) Math.log(((double) (n - df) + 0.5) / ((double) df + 0.5) + 1.0);
  }

  private int countRows(final String sql) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }
}
