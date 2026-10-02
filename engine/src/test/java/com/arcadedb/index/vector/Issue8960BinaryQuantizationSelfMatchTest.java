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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #8960: BINARY quantization reconstructed a vector as {@code bit ? median : 0}, which loses the sign of data
 * centred on zero, so a record queried by its own vector came back on the far side of the sphere (distance 1.46) and
 * behind other records; recall@10 on random vectors was 0.19 where INT8 had 0.98.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8960BinaryQuantizationSelfMatchTest extends TestHelper {

  @Test
  void recordQueriedByItsOwnVectorComesFirstAtZeroDistance() {
    final String type = createType("SmallBinary", 4);
    database.transaction(() -> {
      database.newDocument(type).set("name", "A", "vector", new float[] { 0.9f, -0.1f, -0.2f, -0.8f }).save();
      database.newDocument(type).set("name", "B", "vector", new float[] { 0.9f, 0.5f, 0.1f, 0.2f }).save();
    });

    final List<Pair<RID, Float>> hits = index(type).findNeighborsFromVector(new float[] { 0.9f, -0.1f, -0.2f, -0.8f }, 2, 100);

    assertThat(hits).hasSize(2);
    assertThat(hits.get(0).getFirst().asDocument().getString("name")).isEqualTo("A");
    assertThat(hits.get(0).getSecond()).isCloseTo(0f, within(1e-5f));
    assertThat(hits.get(1).getFirst().asDocument().getString("name")).isEqualTo("B");
    assertThat(hits.get(1).getSecond()).isCloseTo(0.5505f, within(1e-3f));
  }

  @Test
  void ownVectorFirstAndUsableRecallOnRandomVectors() {
    final Random random = new Random(42);
    final List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      final float[] v = new float[64];
      for (int d = 0; d < 64; d++)
        v[d] = random.nextFloat() * 2 - 1;
      vectors.add(v);
    }

    final String type = createType("DocBinary", 64);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (final float[] v : vectors)
        rids.add(database.newDocument(type).set("vector", v).save().getIdentity());
    });

    int selfFirst = 0;
    int hitsInExactTop10 = 0;
    for (int q = 0; q < 100; q++) {
      final float[] query = vectors.get(q * 20);
      final List<Pair<RID, Float>> hits = index(type).findNeighborsFromVector(query, 10, 200);
      if (hits.get(0).getFirst().equals(rids.get(q * 20)))
        selfFirst++;

      final List<Integer> order = new ArrayList<>();
      for (int i = 0; i < vectors.size(); i++)
        order.add(i);
      order.sort(Comparator.comparingDouble(i -> -cosine(query, vectors.get(i))));
      final Set<RID> exactTop10 = new HashSet<>();
      for (int i = 0; i < 10; i++)
        exactTop10.add(rids.get(order.get(i)));
      for (final Pair<RID, Float> hit : hits)
        if (exactTop10.contains(hit.getFirst()))
          hitsInExactTop10++;
    }

    assertThat(selfFirst).isEqualTo(100);
    assertThat(hitsInExactTop10 / 1000.0).as("recall@10").isGreaterThan(0.6);
  }

  private String createType(final String type, final int dimensions) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": " + dimensions
        + ", \"similarity\": \"COSINE\", \"quantization\": \"BINARY\" }");
    return type;
  }

  private LSMVectorIndex index(final String type) {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[vector]")).getIndexesOnBuckets()[0];
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }
}
