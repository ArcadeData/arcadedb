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
package com.arcadedb.bench.data;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DatasetGeneratorTest {
  private static List<String> take(final Iterator<String> it, final int n) {
    final List<String> rows = new ArrayList<>();
    for (int i = 0; i < n && it.hasNext(); i++)
      rows.add(it.next());
    return rows;
  }

  @Test
  void sameSeedSameData() {
    final DatasetGenerator a = new DatasetGenerator(42);
    final DatasetGenerator b = new DatasetGenerator(42);
    final DatasetGenerator c = new DatasetGenerator(43);

    final List<String> rowsA = take(a.csvRows(100), 100);
    assertThat(rowsA).hasSize(100).isEqualTo(take(b.csvRows(100), 100));
    assertThat(rowsA).isNotEqualTo(take(c.csvRows(100), 100));

    assertThat(new DatasetGenerator(42).vectors(100, 16)).isDeepEqualTo(new DatasetGenerator(42).vectors(100, 16));
    assertThat(new DatasetGenerator(42).vectors(100, 16)).isNotEqualTo(new DatasetGenerator(43).vectors(100, 16));
    assertThat(new DatasetGenerator(42).text(20)).isEqualTo(new DatasetGenerator(42).text(20));
  }

  @Test
  void csvRowsHaveTheDocumentedColumns() {
    final Iterator<String> it = new DatasetGenerator(1).csvRows(3);
    assertThat(it.next()).isEqualTo("id,name,age,city,score");
    final String row = it.next();
    assertThat(row.split(",")).hasSize(5);
    assertThat(row).startsWith("0,");
  }

  @Test
  void vectorsAreUnitLength() {
    for (final float[] v : new DatasetGenerator(7).vectors(200, 128)) {
      double sum = 0;
      for (final float x : v)
        sum += (double) x * x;
      assertThat(Math.sqrt(sum)).isBetween(1 - 1e-5, 1 + 1e-5);
    }
  }

  @Test
  void embeddingsAreDeterministicAndUnitLength() {
    final DatasetGenerator.EmbeddingSpace space = new DatasetGenerator(9).embeddingSpace(128);
    final float[][] a = new DatasetGenerator(42).embeddings(300, space);
    assertThat(a).isDeepEqualTo(new DatasetGenerator(42).embeddings(300, new DatasetGenerator(9).embeddingSpace(128)));
    assertThat(a).isNotEqualTo(new DatasetGenerator(43).embeddings(300, space));
    for (final float[] v : a) {
      assertThat(v).hasSize(128);
      double sum = 0;
      for (final float x : v)
        sum += (double) x * x;
      assertThat(Math.sqrt(sum)).isBetween(1 - 1e-5, 1 + 1e-5);
    }
  }

  // Uniform directions in 128 dimensions are all nearly orthogonal, so the exact nearest neighbor is barely closer than any
  // other point and approximate search cannot tell them apart (recall 0.24 at scale M). Embeddings carry structure: the
  // nearest neighbor of a query drawn from the same space is clearly closer than a typical point.
  @Test
  void embeddingsHaveNeighborsCloserThanUniformVectors() {
    final DatasetGenerator.EmbeddingSpace space = new DatasetGenerator(9).embeddingSpace(128);
    final float[][] base = new DatasetGenerator(1).embeddings(5_000, space);
    final float[][] queries = new DatasetGenerator(2).embeddings(100, space);
    final float[][] uniformBase = new DatasetGenerator(1).vectors(5_000, 128);
    final float[][] uniformQueries = new DatasetGenerator(2).vectors(100, 128);

    final double embedded = meanNearestCosine(base, queries);
    final double uniform = meanNearestCosine(uniformBase, uniformQueries);

    assertThat(uniform).isLessThan(0.45);
    assertThat(embedded).isGreaterThan(0.8);
  }

  private static double meanNearestCosine(final float[][] base, final float[][] queries) {
    double sum = 0;
    for (final float[] q : queries) {
      double best = -1;
      for (final float[] v : base) {
        double dot = 0;
        for (int j = 0; j < v.length; j++)
          dot += (double) v[j] * q[j];
        best = Math.max(best, dot);
      }
      sum += best;
    }
    return sum / queries.length;
  }

  @Test
  void textHasTheRequestedWordCount() {
    assertThat(new DatasetGenerator(3).text(25).split(" ")).hasSize(25);
  }
}
