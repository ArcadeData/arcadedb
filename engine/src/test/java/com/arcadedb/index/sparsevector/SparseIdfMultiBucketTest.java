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
}
