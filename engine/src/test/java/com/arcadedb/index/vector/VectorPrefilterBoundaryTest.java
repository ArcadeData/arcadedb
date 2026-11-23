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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The crossover of {@code vector.neighbors} between the pre-filter plan (every member of the allow-list is scored: the exact top-k)
 * and the filtered graph walk, as the {@code prefilterMaxSelectivity} setting documents it: an allow-list covering at most that
 * fraction of the index's live vectors takes the plan, one member more takes the walk.
 */
class VectorPrefilterBoundaryTest extends TestHelper {
  private static final int N   = 400;
  private static final int DIM = 8;

  private final List<RID>     rids    = new ArrayList<>();
  private final List<float[]> vectors = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\": " + DIM + ", \"similarity\": \"COSINE\"}");
    final Random rnd = new Random(21);
    database.transaction(() -> {
      for (int i = 0; i < N; i++) {
        final float[] v = new float[DIM];
        for (int j = 0; j < DIM; j++)
          v[j] = (float) rnd.nextGaussian();
        vectors.add(v);
        rids.add(database.newVertex("Doc").set("vector", v).save().getIdentity());
      }
    });
    subIndex().buildVectorGraphNow();
  }

  @AfterEach
  void restoreSetting() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
        GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
  }

  private LSMVectorIndex subIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
  }

  private long preFilterSearches() {
    return subIndex().getStats().get("preFilterSearches");
  }

  private List<RID> search(final List<RID> allowList, final int k) {
    final List<RID> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid FROM (SELECT expand(vector.neighbors('Doc[vector]', ?, ?, { filter: ?, efSearch: 400 })))", vectors.getFirst(), k,
        allowList)) {
      while (rs.hasNext())
        out.add(((Identifiable) rs.next().getProperty("rid")).getIdentity());
    }
    return out;
  }

  private List<RID> exactTop(final List<RID> allowList, final int k) {
    final float[] q = vectors.getFirst();
    final List<RID> sorted = new ArrayList<>(allowList);
    final double[] distance = new double[N];
    for (int i = 0; i < N; i++) {
      double dot = 0, nq = 0, nv = 0;
      for (int j = 0; j < DIM; j++) {
        dot += (double) q[j] * vectors.get(i)[j];
        nq += (double) q[j] * q[j];
        nv += (double) vectors.get(i)[j] * vectors.get(i)[j];
      }
      distance[i] = 1.0 - dot / Math.sqrt(nq * nv);
    }
    sorted.sort((a, b) -> Double.compare(distance[rids.indexOf(a)], distance[rids.indexOf(b)]));
    return sorted.subList(0, Math.min(k, sorted.size()));
  }

  @Test
  void anAllowListAtExactlyTheSelectivityTakesThePlanAndOneMoreTakesTheWalk() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0.1f);
    final int cap = 40; // 10% of the 400 live vectors
    final List<RID> shuffled = new ArrayList<>(rids);
    Collections.shuffle(shuffled, new Random(4));

    final long before = preFilterSearches();
    final List<RID> atCap = shuffled.subList(0, cap);
    assertThat(search(atCap, 10)).containsExactlyElementsOf(exactTop(atCap, 10));
    assertThat(preFilterSearches()).as("an allow-list of %d is 10%% of the index: the pre-filter plan", cap).isEqualTo(before + 1);

    final List<RID> overCap = shuffled.subList(0, cap + 1);
    assertThat(search(overCap, 10)).hasSize(10);
    assertThat(preFilterSearches()).as("one member more is past the setting: the walk").isEqualTo(before + 1);
  }

  @Test
  void aSelectivityOfOneTakesThePlanForTheWholeIndexAndNotForOneMoreMember() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 1f);
    final long before = preFilterSearches();
    assertThat(search(rids, 400)).containsExactlyElementsOf(exactTop(rids, 400));
    assertThat(preFilterSearches()).as("an allow-list of the whole index is 100%% of it: the pre-filter plan").isEqualTo(before + 1);

    // a RID no record has makes 401 members of a 400-vector index: past the setting, so the walk
    final List<RID> everyoneAndMore = new ArrayList<>(rids);
    everyoneAndMore.add(new RID(rids.getFirst().getBucketId(), 999_999L));
    assertThat(search(everyoneAndMore, 400)).hasSize(N);
    assertThat(preFilterSearches()).as("401 members of a 400-vector index are past the setting").isEqualTo(before + 1);
  }
}
