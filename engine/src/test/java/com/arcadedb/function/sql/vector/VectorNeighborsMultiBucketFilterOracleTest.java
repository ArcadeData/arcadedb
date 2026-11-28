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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
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
 * Oracle test of {@code vector.neighbors(index, q, k, {filter: :rids})} on a type with several buckets (one LSM_VECTOR index per
 * bucket, merged by the function), the shape of modelhub's chunk probe: for allow-lists of every size and of every composition
 * (one bucket, all of them, a few, most, none, records of another type, deleted records) the answer is the exact top-k of the
 * filtered live set by brute force, each record once, never a record outside the list.
 */
class VectorNeighborsMultiBucketFilterOracleTest extends TestHelper {
  private static final int DIMENSIONS = 12;
  private static final int BUCKETS    = 3;
  private static final int PER_BUCKET = 420; // per bucket: each bucket index builds its own graph at search time

  private final Random            random  = new Random(0x3B0C4E7L);
  private final Map<RID, float[]> model   = new LinkedHashMap<>();
  private final List<RID>         foreign = new ArrayList<>();

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void filteredAnswerIsTheExactTopKOfTheFilteredLiveSet(final String quantization) {
    seed(quantization);
    final List<RID> live = new ArrayList<>(model.keySet());
    final Map<Integer, List<RID>> byBucket = new LinkedHashMap<>();
    for (final RID rid : live)
      byBucket.computeIfAbsent(rid.getBucketId(), b -> new ArrayList<>()).add(rid);
    assertThat(byBucket).as("the records are spread over every bucket").hasSize(BUCKETS);

    final List<List<RID>> filters = new ArrayList<>();
    filters.add(sample(live, 1));
    filters.add(sample(live, 8));
    filters.add(sample(live, 60));
    filters.add(sample(live, live.size() / 3));
    filters.add(sample(live, live.size() - 5));
    filters.add(new ArrayList<>(live));
    for (final List<RID> one : byBucket.values())
      filters.add(new ArrayList<>(one));
    final List<RID> twoBuckets = new ArrayList<>();
    final List<List<RID>> lists = new ArrayList<>(byBucket.values());
    twoBuckets.addAll(lists.get(0));
    twoBuckets.addAll(sample(lists.get(1), 7));
    filters.add(twoBuckets);
    // A few records of each bucket (short lists: scored directly)
    final List<RID> thin = new ArrayList<>();
    for (final List<RID> one : byBucket.values())
      thin.addAll(sample(one, 3));
    filters.add(thin);
    // Records of another type, and a record that was deleted: listed, never answered
    final List<RID> withNoise = new ArrayList<>(sample(live, 40));
    withNoise.addAll(foreign);
    filters.add(withNoise);

    for (final List<RID> filter : filters)
      for (final int k : new int[] { 1, 5, 25, filter.size(), filter.size() + 3 }) {
        if (k < 1)
          continue;
        for (int probe = 0; probe < 2; probe++)
          assertExact(quantization, filter, k, randomVector());
      }

    // The list in the order it came and reversed and with duplicates: the same answer
    final List<RID> messy = new ArrayList<>(sample(live, 50));
    messy.addAll(sample(messy, 20));
    Collections.reverse(messy);
    assertExact(quantization, messy, 10, randomVector());
  }

  @Test
  void emptyAndForeignOnlyFiltersAnswerNothing() {
    seed("NONE");
    assertThat(neighbors(randomVector(), 10, new ArrayList<RID>())).isEmpty();
    assertThat(neighbors(randomVector(), 10, foreign)).isEmpty();
  }

  private void assertExact(final String quantization, final List<RID> filter, final int k, final float[] q) {
    final Set<RID> allowed = new HashSet<>();
    for (final RID rid : filter)
      if (model.containsKey(rid))
        allowed.add(rid);
    final List<Double> exact = new ArrayList<>();
    for (final RID rid : allowed)
      exact.add(distance(q, model.get(rid)));
    Collections.sort(exact);

    final List<RID> answer = neighbors(q, k, filter);
    final String where = "filter of " + filter.size() + " (" + allowed.size() + " live), k=" + k + ", " + quantization;
    assertThat(answer).as("%s: rows", where).hasSize(Math.min(k, allowed.size()));
    assertThat(new HashSet<>(answer)).as("%s: each record once", where).hasSize(answer.size());
    final double tolerance = "NONE".equals(quantization) ? 2e-4 : 0.05;
    for (int rank = 0; rank < answer.size(); rank++) {
      assertThat(allowed).as("%s: %s is in the list and live", where, answer.get(rank)).contains(answer.get(rank));
      assertThat(distance(q, model.get(answer.get(rank)))).as("%s: rank %d", where, rank).isCloseTo(exact.get(rank), within(tolerance));
    }
  }

  private List<RID> neighbors(final float[] q, final int k, final List<RID> filter) {
    final List<RID> out = new ArrayList<>();
    // efSearch 2000 is above the 420 nodes of each bucket's graph: the walk visits every node, so the answer is the exact one
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.neighbors`('Doc[vector]', :q, :k, { filter: :rids, efSearch: 2000 }))",
        Map.of("q", q, "k", k, "rids", filter))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getIdentity().orElseThrow());
      }
    }
    return out;
  }

  private void seed(final String quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
          + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quantization + "\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Other");
    });
    database.transaction(() -> {
      for (int i = 0; i < PER_BUCKET * BUCKETS; i++) {
        final float[] v = randomVector();
        model.put(database.newDocument("Doc").set("vector", v).save().getIdentity(), v);
      }
      for (int i = 0; i < 5; i++)
        foreign.add(database.newDocument("Other").set("n", i).save().getIdentity());
    });
    // A deleted record: its RID is listed below and must never answer
    final RID gone = model.keySet().iterator().next();
    database.transaction(() -> database.lookupByRID(gone, true).delete());
    model.remove(gone);
    foreign.add(gone);
  }

  private List<RID> sample(final List<RID> from, final int n) {
    final List<RID> copy = new ArrayList<>(from);
    Collections.shuffle(copy, random);
    return new ArrayList<>(copy.subList(0, Math.min(n, copy.size())));
  }

  private float[] randomVector() {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) random.nextGaussian();
    return v;
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
