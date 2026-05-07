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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.sparsevector.SparseVectorScoringPool;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4085 - per-bucket parallel top-K fan-out for {@code vector.sparseNeighbors}. The function
 * iterates the user-facing TypeIndex's per-bucket {@code LSMSparseVectorIndex} sub-indexes; on a
 * type with multiple physical buckets, those sub-indexes are independent (different RIDs, no
 * coordination) so the fan-out runs on the dedicated {@link SparseVectorScoringPool}.
 * <p>
 * What this pins:
 * <ol>
 *   <li>Multi-bucket dispatch produces the same top-K as the serial baseline (correctness).</li>
 *   <li>The fan-out actually used the pool (some completedTasks delta, not zero).</li>
 *   <li>The single-bucket case takes the in-line path - no pool dispatch overhead.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseVectorParallelFanoutTest extends TestHelper {

  private static final int    BUCKETS    = 16;
  private static final int    DOCS       = 200;
  private static final int    DIMS       = 64;
  private static final int    NNZ_PER_DOC = 6;
  private static final int    K          = 10;
  private static final long   SEED       = 4085L;

  /**
   * Multi-bucket: the fan-out path must (a) match the serial baseline result and (b) leave a
   * non-zero footprint on the scoring pool's {@code completedTasks} counter. The latter is what
   * confirms the dispatch actually ran, vs. silently degrading to the inline path.
   */
  @Test
  void multiBucketTypeFansOutThroughScoringPool() {
    final List<int[]>   docIndices = new ArrayList<>(DOCS);
    final List<float[]> docValues  = new ArrayList<>(DOCS);
    final List<RID>     docRids    = new ArrayList<>(DOCS);

    final String typeName = "FanOutSparseDoc";
    final String idxName  = typeName + "[tokens,weights]";

    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + typeName + ".weights ARRAY_OF_FLOATS");
      database.getSchema()
          .buildTypeIndex(typeName, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMS)
          .create();

      final Random rnd = new Random(SEED);
      for (int i = 0; i < DOCS; i++) {
        final int[]   indices = new int[NNZ_PER_DOC];
        final float[] values  = new float[NNZ_PER_DOC];
        final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
        for (int j = 0; j < NNZ_PER_DOC; j++) {
          int dim;
          do {
            dim = rnd.nextInt(DIMS);
          } while (!picked.add(dim));
          indices[j] = dim;
          values[j]  = 0.1f + rnd.nextFloat();
        }
        final MutableDocument doc = database.newDocument(typeName);
        doc.set("tokens", indices);
        doc.set("weights", values);
        doc.save();
        docIndices.add(indices);
        docValues.add(values);
        docRids.add(doc.getIdentity());
      }
    });

    // Use the corpus's first record as the query so the answer is deterministic and the score is
    // bounded by self-similarity at rank 0.
    final int[]   queryIdx = docIndices.get(0);
    final float[] queryVal = docValues.get(0);

    final long completedBefore = SparseVectorScoringPool.getInstance().getPoolStats().completedTasks();

    final List<RID> indexRids = runQuery(idxName, queryIdx, queryVal, K);
    assertThat(indexRids).hasSize(K);
    assertThat(indexRids.get(0)).as("self-query must return self at rank 0").isEqualTo(docRids.get(0));

    final long completedAfter = SparseVectorScoringPool.getInstance().getPoolStats().completedTasks();
    assertThat(completedAfter)
        .as("multi-bucket fan-out must dispatch at least one task to the scoring pool (had %d before, %d after)",
            completedBefore, completedAfter)
        .isGreaterThan(completedBefore);

    // Correctness: brute-force the same query against the in-memory truth and demand identical
    // top-K identity (within int8 quantization tolerance for the scores - we only check identity
    // here so the test does not depend on the float comparison).
    final List<RID> bf = bruteForceTopK(queryIdx, queryVal, docIndices, docValues, docRids, K);
    final Set<RID> indexSet = new HashSet<>(indexRids);
    final Set<RID> bfSet    = new HashSet<>(bf);
    assertThat(indexSet)
        .as("parallel fan-out must produce the same top-K set as the brute-force reference")
        .isEqualTo(bfSet);
  }

  /**
   * Single-bucket type: the fan-out logic detects the trivial case (size <= 1) and runs inline.
   * The pool's completedTasks counter must NOT advance for this query - otherwise the dispatch
   * is paying scheduler overhead for nothing on every single-bucket call.
   */
  @Test
  void singleBucketTypeUsesInlinePath() {
    final String typeName = "SingleBucketSparseDoc";
    final String idxName  = typeName + "[tokens,weights]";

    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + typeName + ".weights ARRAY_OF_FLOATS");
      database.getSchema()
          .buildTypeIndex(typeName, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(8)
          .create();
      for (int i = 0; i < 4; i++) {
        final MutableDocument doc = database.newDocument(typeName);
        doc.set("tokens", new int[] { i });
        doc.set("weights", new float[] { 0.5f });
        doc.save();
      }
    });

    final long completedBefore = SparseVectorScoringPool.getInstance().getPoolStats().completedTasks();
    runQuery(idxName, new int[] { 0 }, new float[] { 1.0f }, 3);
    final long completedAfter = SparseVectorScoringPool.getInstance().getPoolStats().completedTasks();
    assertThat(completedAfter)
        .as("single-bucket query must not dispatch onto the scoring pool")
        .isEqualTo(completedBefore);
  }

  private List<RID> runQuery(final String idxName, final int[] qIdx, final float[] qVal, final int k) {
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        idxName, qIdx, qVal, k);
    final List<RID> rids = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      rids.add((RID) row.getProperty("@rid"));
    }
    return rids;
  }

  private static List<RID> bruteForceTopK(final int[] qIdx, final float[] qVal,
      final List<int[]> docIndices, final List<float[]> docValues, final List<RID> docRids, final int k) {
    final List<float[]> scored = new ArrayList<>(docRids.size());
    for (int i = 0; i < docRids.size(); i++) {
      final float s = sparseDot(qIdx, qVal, docIndices.get(i), docValues.get(i));
      scored.add(new float[] { i, s });
    }
    scored.sort((a, b) -> Float.compare(b[1], a[1]));
    final List<RID> out = new ArrayList<>(k);
    for (int i = 0; i < Math.min(k, scored.size()); i++)
      out.add(docRids.get((int) scored.get(i)[0]));
    return out;
  }

  private static float sparseDot(final int[] aIdx, final float[] aVal, final int[] bIdx, final float[] bVal) {
    final java.util.Map<Integer, Float> map = new java.util.HashMap<>(aIdx.length);
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
