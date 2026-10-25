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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10704: with no {@code efSearch} on the query or in the index metadata, the beam of an {@code LSM_VECTOR} search was
 * {@code max(k, 100)} up to 10,000 vectors in the graph and {@code max(2k, 20)} from there, so a plain {@code vectorNeighbors(idx, v, 10)}
 * on a larger index walked a beam of 20 and missed neighbours that a beam of 100 finds (SIFT 100k recall@10 0.929 against 0.998).
 * <p>
 * For k = 10 the default must now be the beam of 100, so the answer without the fourth argument is the answer with 100, query by
 * query, and its recall against the exact top 10 is the recall of the beam of 100. A beam of 20 gives a different answer, with
 * a lower recall, for some of the probes at this size, which is what made the test fail before. The test asserts on answers and
 * recall only, never on time.
 */
class Issue10704DefaultSearchBeamTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int VECTORS    = 12_000; // past the 10,000 where the default beam used to narrow
  private static final int PROBES     = 60;
  private static final int K          = 10;

  @Test
  void theDefaultBeamOfALargeIndexIsTheBeamOfOneHundred() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS
        + ", similarity: 'EUCLIDEAN'}");

    final Random random = new Random(10704L);
    final float[][] vectors = new float[VECTORS][DIMENSIONS];
    for (int batch = 0; batch < VECTORS; batch += 1000) {
      final int from = batch;
      database.transaction(() -> {
        for (int i = from; i < from + 1000; i++) {
          for (int d = 0; d < DIMENSIONS; d++)
            vectors[i][d] = (float) random.nextGaussian();
          database.newVertex("Doc").set("id", i).set("embedding", vectors[i]).save();
        }
      });
    }

    int different = 0;
    double recallByDefault = 0;
    double recallWithHundred = 0;
    for (int p = 0; p < PROBES; p++) {
      final float[] query = vectors[random.nextInt(VECTORS)].clone();
      for (int d = 0; d < DIMENSIONS; d++)
        query[d] += (float) (0.5 * random.nextGaussian()); // a probe near, not on, a stored vector
      final List<Integer> byDefault = ids("SELECT expand(vectorNeighbors('Doc[embedding]', ?, " + K + "))", query);
      final List<Integer> withHundred = ids("SELECT expand(vectorNeighbors('Doc[embedding]', ?, " + K + ", 100))", query);
      assertThat(byDefault).hasSize(K);
      if (!byDefault.equals(withHundred))
        different++;
      final Set<Integer> exact = exactNeighbours(vectors, query);
      recallByDefault += recall(byDefault, exact);
      recallWithHundred += recall(withHundred, exact);
    }
    recallByDefault /= PROBES;
    recallWithHundred /= PROBES;

    assertThat(different).as("probes whose default answer differs from the answer of a beam of 100").isZero();
    assertThat(recallByDefault).as("recall@10 of the default beam against the exact top 10").isGreaterThanOrEqualTo(0.99);
  }

  private static Set<Integer> exactNeighbours(final float[][] vectors, final float[] query) {
    final double[] distance = new double[vectors.length];
    final Integer[] order = new Integer[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      double sum = 0;
      for (int d = 0; d < DIMENSIONS; d++) {
        final double delta = vectors[i][d] - query[d];
        sum += delta * delta;
      }
      distance[i] = sum;
      order[i] = i;
    }
    java.util.Arrays.sort(order, (a, b) -> Double.compare(distance[a], distance[b]));
    final Set<Integer> exact = new HashSet<>();
    for (int i = 0; i < K; i++)
      exact.add(order[i]);
    return exact;
  }

  private static double recall(final List<Integer> answer, final Set<Integer> exact) {
    int found = 0;
    for (final Integer id : answer)
      if (exact.contains(id))
        found++;
    return (double) found / K;
  }

  private List<Integer> ids(final String sql, final float[] query) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT record.id AS id FROM (" + sql + ")", query)) {
      while (rs.hasNext())
        out.add(rs.next().<Integer>getProperty("id"));
    }
    return out;
  }
}
