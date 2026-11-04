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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10704, the grouped search: with no {@code efSearch} on the query or in the index metadata, the beam of
 * {@code findNeighborsFromVectorGrouped} was {@code max(rows, 100)} up to 10,000 vectors in the graph and
 * {@code max(2 * rows, 20)} from there, where {@code rows} is {@code limit * groupSize}. It is now {@code max(2 * rows, 100)} from
 * there, the same floor as the ungrouped search ({@link Issue10704DefaultSearchBeamTest}).
 * <p>
 * Every vector here is its own group and {@code groupSize} is 1, so the grouped answer for {@code limit = 10} is the exact top 10.
 * For this {@code rows} the default beam must be the beam of 100: the answer without an {@code efSearch} is the answer with 100,
 * probe by probe, and its recall against the exact top 10 is that of the beam of 100. A beam of 20 gives a different answer, with a
 * lower recall, for some of the probes at this size, which is what made the test fail before. The test asserts on answers and recall
 * only, never on time.
 */
@Tag("slow")
class Issue10704GroupedDefaultSearchBeamTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int VECTORS    = 12_000; // past the 10,000 where the default beam used to narrow
  private static final int PROBES     = 60;
  private static final int LIMIT      = 10;

  @Test
  void theDefaultBeamOfALargeGroupedSearchIsTheBeamOfOneHundred() {
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

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final Function<RID, Object> ownGroup = rid -> ((Document) database.lookupByRID(rid, true)).getInteger("id");

    int different = 0;
    double recallByDefault = 0;
    double recallWithHundred = 0;
    for (int p = 0; p < PROBES; p++) {
      final float[] query = vectors[random.nextInt(VECTORS)].clone();
      for (int d = 0; d < DIMENSIONS; d++)
        query[d] += (float) (0.5 * random.nextGaussian()); // a probe near, not on, a stored vector
      final List<Integer> byDefault = ids(index.findNeighborsFromVectorGrouped(query, LIMIT, 1, -1, null, ownGroup));
      final List<Integer> withHundred = ids(index.findNeighborsFromVectorGrouped(query, LIMIT, 1, 100, null, ownGroup));
      assertThat(byDefault).hasSize(LIMIT);
      if (!byDefault.equals(withHundred))
        different++;
      final Set<Integer> exact = exactNeighbours(vectors, query);
      recallByDefault += recall(byDefault, exact);
      recallWithHundred += recall(withHundred, exact);
    }
    recallByDefault /= PROBES;
    recallWithHundred /= PROBES;

    assertThat(different).as("probes whose default grouped answer differs from the answer of a beam of 100").isZero();
    assertThat(recallByDefault).as("recall@10 of the default grouped beam against the exact top 10").isGreaterThanOrEqualTo(0.99);
    assertThat(recallByDefault).as("the default grouped beam finds what a beam of 100 finds")
        .isEqualTo(recallWithHundred);
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
    Arrays.sort(order, (a, b) -> Double.compare(distance[a], distance[b]));
    final Set<Integer> exact = new HashSet<>();
    for (int i = 0; i < LIMIT; i++)
      exact.add(order[i]);
    return exact;
  }

  private static double recall(final List<Integer> answer, final Set<Integer> exact) {
    int found = 0;
    for (final Integer id : answer)
      if (exact.contains(id))
        found++;
    return (double) found / LIMIT;
  }

  private List<Integer> ids(final List<Pair<RID, Float>> answer) {
    final List<Integer> out = new ArrayList<>();
    for (final Pair<RID, Float> r : answer)
      out.add(((Document) database.lookupByRID(r.getFirst(), true)).getInteger("id"));
    return out;
  }
}
