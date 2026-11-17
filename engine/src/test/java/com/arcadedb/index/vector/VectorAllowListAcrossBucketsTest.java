/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12355: a type's vector index is one index per bucket, and every one of them was searched with the whole
 * RID allow-list of {@code vector.neighbors(..., {filter: [...]})}. Each sized the allow-list against its own vectors, so a
 * list spread over 8 buckets looked 8 times wider than it is to every one of them: it missed the pre-filter plan, the
 * filtered graph walk met almost none of its members, and the shortfall fallback scanned every vector of the bucket, once
 * per bucket and per query, with a WARNING that the graph may need rebuilding. Each bucket's index now searches only the
 * members of its own bucket.
 */
class VectorAllowListAcrossBucketsTest extends TestHelper {
  private static final int DIMENSIONS  = 8;
  private static final int BUCKETS     = 8;
  private static final int PER_BUCKET  = 100;
  private static final int ALLOWED_PER = 5;

  @Test
  void anAllowListSpreadOverTheBucketsIsScoredDirectlyInEachBucket() {
    final List<RID> all = createSchemaAndData();
    final List<RID> allowed = firstOfEachBucket(all, ALLOWED_PER);
    assertThat(allowed).as("the allow-list must hold 5 records of every bucket").hasSize(BUCKETS * ALLOWED_PER);
    final float[] query = embedding(7);
    final Map<String, Long> before = totals();
    final List<RID> answer = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT FROM (SELECT expand(vector.neighbors('Doc[embedding]', ?, ?, {filter: ?})))", query, allowed.size(), allowed)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        answer.add(r.getIdentity().orElseThrow());
      }
    }
    final Map<String, Long> after = totals();

    assertThat(answer).as("every allowed record, nearest first").containsExactlyElementsOf(exactOrder(allowed, query));
    assertThat(after.get("bruteForceScans") - before.get("bruteForceScans"))
        .as("no bucket may fall back to scanning all its vectors for an allow-list it holds 5 of").isZero();
    assertThat(after.get("preFilterSearches") - before.get("preFilterSearches"))
        .as("each bucket scores its own members of the allow-list directly").isEqualTo(BUCKETS);
  }

  @Test
  void aBucketWithNoneOfTheAllowListAnswersNothingAndScansNothing() {
    final List<RID> all = createSchemaAndData();
    final LSMVectorIndex first = indexes()[0];
    final int firstBucket = first.getAssociatedBucketId();
    final Set<RID> elsewhere = new HashSet<>();
    for (final RID rid : all)
      if (rid.getBucketId() != firstBucket && elsewhere.size() < 50)
        elsewhere.add(rid);

    final Map<String, Long> before = first.getStats();
    final List<Pair<RID, Float>> found = first.findNeighborsFromVector(embedding(3), 50, -1, elsewhere);
    final Map<String, Long> after = first.getStats();

    assertThat(found).isEmpty();
    assertThat(after.get("bruteForceScans")).isEqualTo(before.getOrDefault("bruteForceScans", 0L));
  }

  private List<RID> createSchemaAndData() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      ((TypeLSMVectorIndexBuilder) database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType())
          .withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("EUCLIDEAN").create();
    });

    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < BUCKETS * PER_BUCKET; i++)
        rids.add(database.newVertex("Doc").set("embedding", embedding(i)).save().getIdentity());
    });
    for (final LSMVectorIndex index : indexes())
      index.buildVectorGraphNow();
    assertThat(bucketsOf(rids)).hasSize(BUCKETS);
    return rids;
  }

  private LSMVectorIndex[] indexes() {
    final IndexInternal[] onBuckets = ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets();
    final LSMVectorIndex[] result = new LSMVectorIndex[onBuckets.length];
    for (int i = 0; i < onBuckets.length; i++)
      result[i] = (LSMVectorIndex) onBuckets[i];
    return result;
  }

  private Map<String, Long> totals() {
    long bruteForce = 0, preFilter = 0;
    for (final LSMVectorIndex index : indexes()) {
      final Map<String, Long> stats = index.getStats();
      bruteForce += stats.getOrDefault("bruteForceScans", 0L);
      preFilter += stats.getOrDefault("preFilterSearches", 0L);
    }
    return Map.of("bruteForceScans", bruteForce, "preFilterSearches", preFilter);
  }

  private List<RID> exactOrder(final List<RID> rids, final float[] query) {
    final List<RID> sorted = new ArrayList<>(rids);
    sorted.sort(Comparator.comparingDouble(rid -> distance(database.lookupByRID(rid, true).asVertex().get("embedding"), query)));
    return sorted;
  }

  private static double distance(final Object stored, final float[] query) {
    final float[] v = (float[]) stored;
    double sum = 0;
    for (int i = 0; i < v.length; i++)
      sum += (v[i] - query[i]) * (v[i] - query[i]);
    return sum;
  }

  private static List<RID> firstOfEachBucket(final List<RID> rids, final int perBucket) {
    final List<RID> result = new ArrayList<>();
    final Map<Integer, Integer> taken = new HashMap<>();
    for (final RID rid : rids)
      if (taken.merge(rid.getBucketId(), 1, Integer::sum) <= perBucket)
        result.add(rid);
    return result;
  }

  private static Set<Integer> bucketsOf(final List<RID> rids) {
    final Set<Integer> buckets = new HashSet<>();
    for (final RID rid : rids)
      buckets.add(rid.getBucketId());
    return buckets;
  }

  /** Deterministic, distinct, non-zero vectors, well spread so no two of them tie on distance. */
  private static float[] embedding(final int i) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(i * 0.37 + j * 0.91) + 2.0f + i * 0.013f;
    return vector;
  }
}
