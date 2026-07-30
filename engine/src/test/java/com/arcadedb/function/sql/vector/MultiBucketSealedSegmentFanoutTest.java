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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.SparseVectorScoringPool;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A multi-bucket sparse-vector query whose postings have been flushed to sealed segments - the
 * combination the per-bucket fan-out was never covered against, since every existing fan-out test
 * queries an index whose postings still sit in the memtable, and an in-memory source reads no pages.
 * <p>
 * What makes it worth pinning down: the fan-out runs each bucket's {@code topK} on a scoring-pool
 * worker, and a worker carries no database context of its own, while the segment read path needs one
 * to borrow its decode buffer. It works today only because {@code LocalDatabase.checkDatabaseIsOpen}
 * creates a context as a side effect for any thread that reaches a checked database method, and the
 * index wrapper happens to call one on the way in. Nothing states that contract, so a future
 * refactor that skips or reorders that call would break segment-backed multi-bucket queries with
 * "Transaction context not found on current thread" and no test would notice. This one would.
 * <p>
 * Written while wiring intra-query parallelism (issue #4085), which hit exactly that wall from the
 * other direction: it reaches the decoder without passing a checked method first, and so establishes
 * its worker context explicitly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MultiBucketSealedSegmentFanoutTest extends TestHelper {

  private static final int  BUCKETS     = 8;
  private static final int  DIMS        = 512;
  private static final int  DOCS        = 400;
  private static final int  NNZ_PER_DOC = 6;
  private static final int  K           = 10;
  private static final long SEED        = 4085L;

  @Test
  void multiBucketQueryOverSealedSegmentsReturnsResults() {
    final String typeName = "SealedFanOutDoc";
    final String idxName = typeName + "[tokens,weights]";

    final List<int[]> docIndices = new ArrayList<>(DOCS);
    final List<float[]> docValues = new ArrayList<>(DOCS);
    final List<RID> docRids = new ArrayList<>(DOCS);

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
        final int[] indices = new int[NNZ_PER_DOC];
        final float[] values = new float[NNZ_PER_DOC];
        final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
        for (int j = 0; j < NNZ_PER_DOC; j++) {
          int dim;
          do {
            dim = rnd.nextInt(DIMS);
          } while (!picked.add(dim));
          indices[j] = dim;
          values[j] = 0.1f + rnd.nextFloat();
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

    // Seal every bucket's memtable into a segment: this is what a settled index looks like, and what
    // the fan-out was never exercised against.
    database.transaction(() -> {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(idxName);
      int sealed = 0;
      for (final Index sub : typeIndex.getIndexesOnBuckets()) {
        final LSMSparseVectorIndex sparse = (LSMSparseVectorIndex) sub;
        sparse.flush();
        sealed += sparse.getEngine().segmentCount();
      }
      // Without this the test would silently prove nothing: an unsealed index answers from the
      // in-memory memtable, which reads no pages and so never needs a database context.
      assertThat(sealed).as("every bucket must have sealed at least one segment").isGreaterThanOrEqualTo(BUCKETS);
    });

    final long completedBefore = SparseVectorScoringPool.getInstance().getPoolStats()
        .completedTasks();
    final int[] queryIdx = docIndices.getFirst();
    final float[] queryVal = docValues.getFirst();

    final List<RID> hits = new ArrayList<>();
    database.transaction(() -> {
      final StringBuilder idx = new StringBuilder("[");
      final StringBuilder val = new StringBuilder("[");
      for (int i = 0; i < queryIdx.length; i++) {
        if (i > 0) {
          idx.append(',');
          val.append(',');
        }
        idx.append(queryIdx[i]);
        val.append(queryVal[i]);
      }
      idx.append(']');
      val.append(']');
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(vectorSparseNeighbors('" + idxName + "', " + idx + ", " + val + ", " + K + "))")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          hits.add(r.getProperty("rid") != null ? r.getProperty("rid") : r.getIdentity().orElse(null));
        }
      }
    });

    final long completedAfter = SparseVectorScoringPool.getInstance().getPoolStats()
        .completedTasks();
    assertThat(completedAfter).as("fan-out must have dispatched (before=%d after=%d)", completedBefore, completedAfter)
        .isGreaterThan(completedBefore);
    assertThat(hits).as("a multi-bucket query over sealed segments must return the top-K").hasSize(K);
    assertThat(hits.getFirst()).as("self-query must return self at rank 0").isEqualTo(docRids.getFirst());

  }
}
