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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for LSMSparseVectorIndex (issue #4065).
 * <p>
 * Verifies:
 * <ul>
 *   <li>Index creation via Java API and via SQL CREATE INDEX</li>
 *   <li>Persistence of sparse vectors as a posting-list inverted index on the LSM-Tree backbone</li>
 *   <li>Top-K retrieval via the {@code vector.sparseNeighbors} SQL function</li>
 *   <li>Correctness vs a brute-force baseline using {@code vector.sparseDot}</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMSparseVectorIndexTest extends TestHelper {

  private static final int    DIMENSIONS  = 1000;
  private static final int    DOC_COUNT   = 50;
  private static final int    NNZ_PER_DOC = 10;
  private static final long   SEED        = 42L;
  private static final String TYPE_NAME   = "SparseDoc";
  private static final String IDX_NAME    = "SparseDoc[tokens,weights]";

  @Test
  void createIndexViaJavaApiAndQueryTopK() {
    final List<int[]>   docIndices = new ArrayList<>(DOC_COUNT);
    final List<float[]> docValues  = new ArrayList<>(DOC_COUNT);
    final List<RID>     docRids    = new ArrayList<>(DOC_COUNT);

    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      final TypeIndex idx = database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();

      assertThat(idx).isNotNull();
      assertThat(idx.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);

      final Random rnd = new Random(SEED);
      for (int i = 0; i < DOC_COUNT; i++) {
        final int[]   indices = new int[NNZ_PER_DOC];
        final float[] values  = new float[NNZ_PER_DOC];
        final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
        for (int j = 0; j < NNZ_PER_DOC; j++) {
          int dim;
          do {
            dim = rnd.nextInt(DIMENSIONS);
          } while (!picked.add(dim));
          indices[j] = dim;
          values[j]  = 0.1f + rnd.nextFloat();
        }

        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("tokens", indices);
        doc.set("weights", values);
        doc.save();

        docIndices.add(indices);
        docValues.add(values);
        docRids.add(doc.getIdentity());
      }
    });

    final int[]   queryIndices = docIndices.get(0);
    final float[] queryValues  = docValues.get(0);

    final List<Map.Entry<RID, Float>> sortedBF = bruteForceTopK(queryIndices, queryValues, docIndices, docValues, docRids);

    final int K = 5;
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, queryIndices, queryValues, K);

    final List<RID>   indexRids   = new ArrayList<>(K);
    final List<Float> indexScores = new ArrayList<>(K);
    while (rs.hasNext()) {
      final Result row = rs.next();
      indexRids.add((RID) row.getProperty("@rid"));
      indexScores.add(((Number) row.getProperty("score")).floatValue());
    }

    assertThat(indexRids).hasSize(K);
    assertThat(indexRids.get(0)).as("self-query must return self at rank 0").isEqualTo(docRids.get(0));

    for (int i = 0; i < K; i++) {
      assertThat(indexRids.get(i)).as("rank " + i).isEqualTo(sortedBF.get(i).getKey());
      assertThat(indexScores.get(i)).as("score at rank " + i)
          .isCloseTo(sortedBF.get(i).getValue(), Offset.offset(1e-4f));
    }
  }

  @Test
  void persistAcrossReopenAndQueryStillWorks() {
    final List<int[]>   docIndices = new ArrayList<>(DOC_COUNT);
    final List<float[]> docValues  = new ArrayList<>(DOC_COUNT);
    final List<RID>     docRids    = new ArrayList<>(DOC_COUNT);

    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();

      final Random rnd = new Random(SEED);
      for (int i = 0; i < DOC_COUNT; i++) {
        final int[]   indices = new int[NNZ_PER_DOC];
        final float[] values  = new float[NNZ_PER_DOC];
        final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
        for (int j = 0; j < NNZ_PER_DOC; j++) {
          int dim;
          do {
            dim = rnd.nextInt(DIMENSIONS);
          } while (!picked.add(dim));
          indices[j] = dim;
          values[j]  = 0.1f + rnd.nextFloat();
        }
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("tokens", indices);
        doc.set("weights", values);
        doc.save();

        docIndices.add(indices);
        docValues.add(values);
        docRids.add(doc.getIdentity());
      }
    });

    database.close();
    database = factory.open();

    final TypeIndex reopened = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    assertThat(reopened).as("index should exist after reopen").isNotNull();
    assertThat(reopened.getType()).as("index type after reopen").isEqualTo(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);

    final int[]   queryIndices = docIndices.get(7);
    final float[] queryValues  = docValues.get(7);

    final List<Map.Entry<RID, Float>> sortedBF = bruteForceTopK(queryIndices, queryValues, docIndices, docValues, docRids);

    final int K = 5;
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, queryIndices, queryValues, K);

    final List<RID> indexRids = new ArrayList<>(K);
    while (rs.hasNext())
      indexRids.add((RID) rs.next().getProperty("@rid"));

    assertThat(indexRids).hasSize(K);
    assertThat(indexRids.get(0)).isEqualTo(docRids.get(7));
    for (int i = 0; i < K; i++)
      assertThat(indexRids.get(i)).as("rank " + i).isEqualTo(sortedBF.get(i).getKey());
  }

  @Test
  void createIndexViaSqlAndQueryTopK() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".weights ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON %s (tokens, weights) LSM_SPARSE_VECTOR
          METADATA { "dimensions": 100 }
          """.formatted(TYPE_NAME));

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
      assertThat(idx).isNotNull();
      assertThat(idx.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR);

      final MutableDocument d1 = database.newDocument(TYPE_NAME);
      d1.set("tokens", new int[] { 1, 5, 10 });
      d1.set("weights", new float[] { 0.5f, 0.3f, 0.2f });
      d1.save();

      final MutableDocument d2 = database.newDocument(TYPE_NAME);
      d2.set("tokens", new int[] { 2, 5, 11 });
      d2.set("weights", new float[] { 0.4f, 0.6f, 0.1f });
      d2.save();
    });

    // Query (5, 1.0) -> d2 should win because its weight at dim 5 is 0.6 vs d1's 0.3.
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, new int[] { 5 }, new float[] { 1.0f }, 5);
    final List<Float> scores = new ArrayList<>();
    final List<RID> rids = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      rids.add((RID) row.getProperty("@rid"));
      scores.add(((Number) row.getProperty("score")).floatValue());
    }
    assertThat(rids).hasSize(2);
    assertThat(scores.get(0)).isCloseTo(0.6f, Offset.offset(1e-4f));
    assertThat(scores.get(1)).isCloseTo(0.3f, Offset.offset(1e-4f));
  }

  @Test
  void removeWithUnequalLengthArraysThrows() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      final TypeIndex idx = database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(100)
          .create();
      final var bucketIndex = idx.getIndexesOnBuckets()[0];
      final MutableDocument doc = database.newDocument(TYPE_NAME);
      doc.set("tokens", new int[] { 1, 2, 3 });
      doc.set("weights", new float[] { 0.1f, 0.2f, 0.3f });
      doc.save();

      // Mismatched arrays must be rejected by remove just as they are by put. Silent truncation
      // would orphan postings on the longer side. Calls the wrapper directly because the SQL path
      // never produces mismatched arrays (DocumentIndexer reads property values that were already
      // validated at insert time).
      final RID rid = doc.getIdentity();
      final org.assertj.core.api.AbstractThrowableAssert<?, ? extends Throwable> assertion =
          org.assertj.core.api.Assertions.assertThatThrownBy(() ->
              bucketIndex.remove(
                  new Object[] { new int[] { 1, 2, 3 }, new float[] { 0.1f, 0.2f } },
                  rid));
      assertion.hasMessageContaining("same length");
    });
  }

  @Test
  void removeDocumentRemovesFromIndex() {
    final RID[] rids = new RID[2];

    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(100)
          .create();

      final MutableDocument d1 = database.newDocument(TYPE_NAME);
      d1.set("tokens", new int[] { 1, 5, 10 });
      d1.set("weights", new float[] { 0.5f, 0.3f, 0.2f });
      d1.save();

      final MutableDocument d2 = database.newDocument(TYPE_NAME);
      d2.set("tokens", new int[] { 2, 5, 11 });
      d2.set("weights", new float[] { 0.5f, 0.3f, 0.2f });
      d2.save();

      rids[0] = d1.getIdentity();
      rids[1] = d2.getIdentity();
    });

    final ResultSet beforeDelete = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, new int[] { 5 }, new float[] { 1.0f }, 10);
    int countBefore = 0;
    while (beforeDelete.hasNext()) {
      beforeDelete.next();
      countBefore++;
    }
    assertThat(countBefore).isEqualTo(2);

    database.transaction(() -> database.lookupByRID(rids[0], true).asDocument().delete());

    final ResultSet afterDelete = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, new int[] { 5 }, new float[] { 1.0f }, 10);
    final List<RID> remaining = new ArrayList<>();
    while (afterDelete.hasNext())
      remaining.add((RID) afterDelete.next().getProperty("@rid"));
    assertThat(remaining).hasSize(1);
    assertThat(remaining.get(0)).isEqualTo(rids[1]);
  }

  @Test
  void idfModifierDownweightsCommonDimensions() {
    final RID[] rareHit = new RID[1];

    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(100)
          .withModifier("IDF")
          .create();

      // dim 1 is rare: only doc A has it.
      // dim 2 is common: every doc has it.
      // A query that mentions both dims should rank A highest under IDF, because the rare
      // dimension dominates after IDF weighting. Without IDF, B-J all share dim 2 with equal
      // weights so they tie with A on the dim-2 contribution and only A's tiny dim-1 weight
      // breaks the tie.
      final MutableDocument a = database.newDocument(TYPE_NAME);
      a.set("tokens", new int[] { 1, 2 });
      a.set("weights", new float[] { 0.1f, 0.5f });
      a.save();
      rareHit[0] = a.getIdentity();

      for (int i = 0; i < 9; i++) {
        final MutableDocument other = database.newDocument(TYPE_NAME);
        other.set("tokens", new int[] { 2 });
        other.set("weights", new float[] { 0.5f });
        other.save();
      }
    });

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, new int[] { 1, 2 }, new float[] { 1.0f, 1.0f }, 10);

    final List<RID>   rids   = new ArrayList<>();
    final List<Float> scores = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      rids.add((RID) row.getProperty("@rid"));
      scores.add(((Number) row.getProperty("score")).floatValue());
    }

    assertThat(rids).hasSize(10);
    assertThat(rids.get(0)).as("rare-dim doc should rank first under IDF").isEqualTo(rareHit[0]);

    // The rare-dim contribution to A should dominate. Under df(1)=1, N=10, IDF(1) = ln((10-1+0.5)/(1+0.5)+1) ~ 1.96.
    // Under df(2)=10, N=10, IDF(2) = ln((10-10+0.5)/(10+0.5)+1) ~ 0.046.
    // A's score: 1.0 * 0.1 * 1.96 + 1.0 * 0.5 * 0.046 ~= 0.219.
    // Others' score: 1.0 * 0.5 * 0.046 ~= 0.023.
    // So A's score should be at least 5x the next.
    assertThat(scores.get(0)).isGreaterThan(scores.get(1) * 5.0f);
  }

  @Test
  void wandResultsMatchBruteForceOnLargerCorpus() {
    final int largeDocCount = 500;
    final int largeNnz = 8;
    final int largeDim = 3000;

    final List<int[]>   docIndices = new ArrayList<>(largeDocCount);
    final List<float[]> docValues  = new ArrayList<>(largeDocCount);
    final List<RID>     docRids    = new ArrayList<>(largeDocCount);

    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(largeDim)
          .create();

      final Random rnd = new Random(11L);
      for (int i = 0; i < largeDocCount; i++) {
        final int[]   indices = new int[largeNnz];
        final float[] values  = new float[largeNnz];
        final HashSet<Integer> picked = new HashSet<>(largeNnz);
        for (int j = 0; j < largeNnz; j++) {
          int dim;
          do {
            // Skewed distribution: half the nnz hit a "frequent" head of 30 dims, half are random.
            dim = j < largeNnz / 2 ? rnd.nextInt(30) : rnd.nextInt(largeDim);
          } while (!picked.add(dim));
          indices[j] = dim;
          values[j] = 0.1f + rnd.nextFloat();
        }
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("tokens", indices);
        doc.set("weights", values);
        doc.save();

        docIndices.add(indices);
        docValues.add(values);
        docRids.add(doc.getIdentity());
      }
    });

    // Three different query shapes: rare-only, frequent-only, mixed.
    final int[][]   queries     = new int[][] { docIndices.get(17), docIndices.get(123), docIndices.get(400) };
    final float[][] queryWeights = new float[][] { docValues.get(17), docValues.get(123), docValues.get(400) };

    for (int q = 0; q < queries.length; q++) {
      final int[]   qIdx = queries[q];
      final float[] qVal = queryWeights[q];

      final List<Map.Entry<RID, Float>> sortedBF = bruteForceTopK(qIdx, qVal, docIndices, docValues, docRids);

      for (final int kVal : new int[] { 1, 5, 25, 100 }) {
        final ResultSet rs = database.query("sql",
            "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
            IDX_NAME, qIdx, qVal, kVal);

        final List<RID>   indexRids   = new ArrayList<>(kVal);
        final List<Float> indexScores = new ArrayList<>(kVal);
        while (rs.hasNext()) {
          final Result row = rs.next();
          indexRids.add((RID) row.getProperty("@rid"));
          indexScores.add(((Number) row.getProperty("score")).floatValue());
        }

        final int expected = Math.min(kVal, sortedBF.size());
        assertThat(indexRids).as("query %d, k=%d size", q, kVal).hasSize(expected);

        // Top-K identity must match brute force (allowing ties on equal scores).
        for (int i = 0; i < expected; i++) {
          final float expectedScore = sortedBF.get(i).getValue();
          assertThat(indexScores.get(i))
              .as("query %d k=%d rank %d score", q, kVal, i)
              .isCloseTo(expectedScore, Offset.offset(1e-3f));
        }
      }
    }
  }

  private static List<Map.Entry<RID, Float>> bruteForceTopK(final int[] qIdx, final float[] qVal,
      final List<int[]> docIndices, final List<float[]> docValues, final List<RID> docRids) {
    final Map<RID, Float> scores = new HashMap<>(docRids.size());
    for (int i = 0; i < docRids.size(); i++) {
      final float s = sparseDot(qIdx, qVal, docIndices.get(i), docValues.get(i));
      if (s > 0.0f)
        scores.put(docRids.get(i), s);
    }
    final List<Map.Entry<RID, Float>> sorted = new ArrayList<>(scores.entrySet());
    sorted.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
    return sorted;
  }

  private static float sparseDot(final int[] aIdx, final float[] aVal, final int[] bIdx, final float[] bVal) {
    final Map<Integer, Float> map = new HashMap<>(aIdx.length);
    for (int i = 0; i < aIdx.length; i++)
      map.put(aIdx[i], aVal[i]);
    float sum = 0.0f;
    for (int i = 0; i < bIdx.length; i++) {
      final Float v = map.get(bIdx[i]);
      if (v != null)
        sum += v * bVal[i];
    }
    return sum;
  }
}
