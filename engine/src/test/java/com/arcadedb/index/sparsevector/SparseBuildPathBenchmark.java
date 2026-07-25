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
import com.arcadedb.engine.PageManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Write-path benchmark for {@code LSM_SPARSE_VECTOR} shaped like the Big-ANN sparse 100k tier of
 * issue #5411: 100k documents x 130 non-zero dimensions over a 30k vocabulary, half the postings
 * landing on a 500-dim head. Splits the wall clock into the load (document insert + transaction
 * queueing + memtable + flush-to-segment) and the {@code COMPACT INDEX} settle so a write-path
 * change can be attributed to one of them.
 * <p>
 * Reference numbers on the issue's fix (Apple M-series, INT8 quantization, defaults elsewhere):
 * load 14.8s -> 6.4s, compact ~0.65s. The load half is what the transaction fast lane
 * ({@link com.arcadedb.index.IndexInternal#isTransactionKeyOrderRequired()}) and the merged
 * per-dim memtable entry address.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class SparseBuildPathBenchmark extends TestHelper {

  private static final String TYPE_NAME   = "SpladeDoc";
  private static final String IDX_NAME    = "SpladeDoc[tokens,weights]";
  private static final int    DIMENSIONS  = 30_000;
  private static final int    HEAD_DIMS   = 500;
  private static final int    NNZ_PER_DOC = 130;
  private static final int    DOCS        = 100_000;
  private static final int    BATCH_SIZE  = 2_000;

  @Test
  void buildProfile() {
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

    final PageManager.PPageManagerStats before = PageManager.INSTANCE.getStats();

    final long loadStart = System.nanoTime();
    final Random rnd = new Random(7L);
    int batchOpen = 0;
    database.begin();
    for (int i = 0; i < DOCS; i++) {
      final int[]   indices = new int[NNZ_PER_DOC];
      final float[] values  = new float[NNZ_PER_DOC];
      final HashSet<Integer> picked = new HashSet<>(NNZ_PER_DOC);
      for (int j = 0; j < NNZ_PER_DOC; j++) {
        int dim;
        do {
          // Zipf-ish: half the postings land on a small head, the rest spread over the vocabulary.
          dim = j < NNZ_PER_DOC / 2 ? rnd.nextInt(HEAD_DIMS) : rnd.nextInt(DIMENSIONS);
        } while (!picked.add(dim));
        indices[j] = dim;
        values[j] = 0.1f + rnd.nextFloat();
      }

      final MutableDocument doc = database.newDocument(TYPE_NAME);
      doc.set("tokens", indices);
      doc.set("weights", values);
      doc.save();

      if (++batchOpen == BATCH_SIZE) {
        database.commit();
        database.begin();
        batchOpen = 0;
      }
    }
    database.commit();
    final long loadMs = (System.nanoTime() - loadStart) / 1_000_000;

    final long compactStart = System.nanoTime();
    database.command("sql", "COMPACT INDEX `" + IDX_NAME + "`");
    final long compactMs = (System.nanoTime() - compactStart) / 1_000_000;

    final PageManager.PPageManagerStats after = PageManager.INSTANCE.getStats();

    System.out.printf("%n=== sparse write path: %,d docs x %d nnz over %,d dims ===%n", DOCS, NNZ_PER_DOC, DIMENSIONS);
    System.out.printf("load        : %,d ms (%,d postings, %.0f ns/posting)%n",
        loadMs, (long) DOCS * NNZ_PER_DOC, loadMs * 1_000_000.0 / ((long) DOCS * NNZ_PER_DOC));
    System.out.printf("compact     : %,d ms%n", compactMs);
    System.out.printf("total       : %,d ms%n", loadMs + compactMs);
    System.out.printf("pagesWritten: %,d%n", after.pagesWritten - before.pagesWritten);

    assertThat(database.countType(TYPE_NAME, false)).isEqualTo(DOCS);
  }
}
