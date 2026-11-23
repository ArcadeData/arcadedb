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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Oracle test of the pre-filter plan of a search with an allow-list of RIDs (modelhub's {@code {filter: :rids}} probe): the plan is
 * taken exactly when the allow-list covers no more than the configured share of the live vectors, and then the answer is the exact
 * top-k of the allowed live records, whatever the size of the list: at and around the sizes where the list of ordinals the plan
 * resolves it to has to grow, with members that are deleted, never indexed, or only in the delta buffer. A wider list takes the
 * graph walk, which has to answer only allowed live records, each with its own exact distance.
 */
class VectorPrefilterPlanOracleTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int VECTORS    = 1_500; // above the 1000 nodes below which a graph is rebuilt synchronously at each search
  private static final int K          = 10;

  private final Random            random = new Random(0x9F1A7L);
  private final Map<RID, float[]> model  = new LinkedHashMap<>();

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aListNoWiderThanTheSelectivityTakesThePlanAndAnswersTheExactTopK(final String quantization) {
    createIndex(quantization);
    final List<RID> graphRids = insertRows(VECTORS);
    index().buildVectorGraphNow();
    final float tolerance = "NONE".equals(quantization) ? 2e-4f : 0.05f;
    final int maxCandidates = (int) (VECTORS * GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getValueAsFloat());
    assertThat(maxCandidates).isEqualTo(300);

    // Around the first size of the ordinal array (256) the plan resolves a list to, and around the threshold (300)
    for (final int size : new int[] { 1, 2, 10, 255, 256, 257, 299, 300 }) {
      final Set<RID> allowed = pick(graphRids, size);
      assertTakesThePlanAndAnswersTheExactTopK("size " + size, allowed, tolerance, true);
    }
    for (final int size : new int[] { 301, 302, 450 }) {
      final Set<RID> allowed = pick(graphRids, size);
      assertTakesThePlanAndAnswersTheExactTopK("size " + size, allowed, tolerance, false);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aListOfAnySizeTakesThePlanWhenTheSelectivityAllowsAndAnswersTheExactTopK(final String quantization) {
    createIndex(quantization);
    final List<RID> graphRids = insertRows(VECTORS);
    index().buildVectorGraphNow();
    final float tolerance = "NONE".equals(quantization) ? 2e-4f : 0.05f;
    final Object original = GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getValue();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 1f);
    try {
      // The ordinal array grows 256, 512, 1024, 2048: lists at and around each size, and the whole index
      for (final int size : new int[] { 511, 512, 513, 1_023, 1_024, 1_025, VECTORS }) {
        final Set<RID> allowed = pick(graphRids, size);
        assertTakesThePlanAndAnswersTheExactTopK("size " + size, allowed, tolerance, true);
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, original);
    }
  }

  @Test
  void membersThatAreDeletedNeverIndexedOrOnlyBufferedAreAnsweredLikeTheLiveSetSays() {
    createIndex("NONE");
    final List<RID> graphRids = insertRows(VECTORS);
    index().buildVectorGraphNow();
    // Vectors committed after the graph (the delta buffer), and graph vectors deleted
    final List<RID> buffered = insertRows(40);
    final List<RID> deleted = new ArrayList<>(graphRids.subList(100, 130));
    database.transaction(() -> {
      for (final RID rid : deleted) {
        database.lookupByRID(rid, true).delete();
        model.remove(rid);
      }
    });
    // Records of this type that hold no vector, and RIDs of nothing at all
    final List<RID> unindexed = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        unindexed.add(database.newDocument("Doc").set("id", -i).save().getIdentity());
    });
    final Set<RID> allowed = new HashSet<>();
    allowed.addAll(graphRids.subList(0, 150)); // 30 of them deleted
    allowed.addAll(buffered.subList(0, 25));
    allowed.addAll(unindexed);
    for (int i = 0; i < 15; i++)
      allowed.add(new RID(unindexed.get(0).getBucketId(), 1_000_000L + i));
    assertTakesThePlanAndAnswersTheExactTopK("mixed list", allowed, 2e-4f, true);

    // Only deleted and unindexed members: nothing to answer
    final Set<RID> nothing = new HashSet<>(deleted);
    nothing.addAll(unindexed);
    assertThat(index().findNeighborsFromVector(randomVector(), K, -1, nothing)).isEmpty();
  }

  private void assertTakesThePlanAndAnswersTheExactTopK(final String where, final Set<RID> allowed, final float tolerance,
      final boolean takesThePlan) {
    for (int probe = 0; probe < 3; probe++) {
      final float[] q = randomVector();
      final long before = index().getStats().getOrDefault("preFilterSearches", 0L);
      final List<Pair<RID, Float>> answer = index().findNeighborsFromVector(q, K, -1, allowed);
      final long after = index().getStats().getOrDefault("preFilterSearches", 0L);
      assertThat(after - before).as("%s: the pre-filter plan is taken exactly when the list is no wider than the share", where)
          .isEqualTo(takesThePlan ? 1L : 0L);

      final List<Double> exact = new ArrayList<>();
      for (final Map.Entry<RID, float[]> e : model.entrySet())
        if (allowed.contains(e.getKey()))
          exact.add(distance(q, e.getValue()));
      Collections.sort(exact);
      if (takesThePlan)
        assertThat(answer).as("%s: rows", where).hasSize(Math.min(K, exact.size()));
      final Set<RID> seen = new HashSet<>();
      for (int rank = 0; rank < answer.size(); rank++) {
        final RID rid = answer.get(rank).getFirst();
        assertThat(seen.add(rid)).as("%s: %s answers once", where, rid).isTrue();
        assertThat(allowed).as("%s: only a member of the list answers, not %s", where, rid).contains(rid);
        assertThat(model).as("%s: only a live record answers, not %s", where, rid).containsKey(rid);
        assertThat((double) answer.get(rank).getSecond()).as("%s: the distance of %s is its own", where, rid)
            .isCloseTo(distance(q, model.get(rid)), within((double) tolerance));
        if (takesThePlan)
          assertThat(distance(q, model.get(rid))).as("%s: rank %d is at the exact rank-%d distance", where, rank, rank)
              .isCloseTo(exact.get(rank), within((double) tolerance));
        if (rank > 0)
          assertThat(answer.get(rank).getSecond()).as("%s: ascending", where).isGreaterThanOrEqualTo(answer.get(rank - 1).getSecond());
      }
    }
  }

  private Set<RID> pick(final List<RID> rids, final int size) {
    final List<RID> shuffled = new ArrayList<>(rids);
    Collections.shuffle(shuffled, random);
    return new HashSet<>(shuffled.subList(0, size));
  }

  private void createIndex(final String quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
          + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quantization + "\"}");
    });
  }

  private float[] randomVector() {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) random.nextGaussian();
    return v;
  }

  private List<RID> insertRows(final int count) {
    final List<RID> rids = new ArrayList<>(count);
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] v = randomVector();
        final RID rid = database.newDocument("Doc").set("id", model.size()).set("vector", v).save().getIdentity();
        model.put(rid, v);
        rids.add(rid);
      }
    });
    return rids;
  }

  private LSMVectorIndex index() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
  }

  private static double distance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }
}
