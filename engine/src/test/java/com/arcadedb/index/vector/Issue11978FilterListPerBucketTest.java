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
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code filter} list on a type with several buckets is handed whole to the index of every bucket (issue #11978). Each
 * index used to reason about all of it as its own: it expected k rows from its walk when only the list's members in its
 * bucket can be answered there, so a list that is larger than k overall but smaller in each bucket sent every bucket to
 * the brute-force fallback with a WARNING blaming the graph; and it measured the selectivity of the whole list against
 * its one bucket, so a list selective in every bucket never took the pre-filter plan.
 */
class Issue11978FilterListPerBucketTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int BUCKETS    = 4;

  private final Random             random  = new Random(11978);
  private final Map<RID, float[]>  vectors = new HashMap<>();

  @Test
  void aWalkThatReturnedEveryMemberOfItsBucketIsNoShortfall() {
    final Map<Integer, List<RID>> byBucket = createDocs(10);
    // 6 members in each bucket: 24 in all, more than k, and fewer than k in every bucket
    final List<RID> filter = membersOfEachBucket(byBucket, 6);
    final float[] query = vector();

    final List<String> warnings = new CopyOnWriteArrayList<>();
    final List<RID> answer = captureWarnings(warnings, () -> neighbors(query, 10, filter));

    assertThat(answer).as("the exact nearest members of the list").containsExactlyElementsOf(exactNearest(query, filter, 10));
    assertThat(stat("bruteForceScans"))
        .as("every bucket's walk returned all the list's members in its bucket: nothing is left to scan for (issue #11978)")
        .isZero();
    assertThat(warnings).as("a list that covers its bucket's share is no sign the graph needs rebuilding").isEmpty();
  }

  @Test
  void aListSelectiveInEveryBucketTakesThePreFilterPlan() {
    final Map<Integer, List<RID>> byBucket = createDocs(100);
    // 10 of 100 in each bucket, under the default selectivity of 0.2, while the 40 of the whole list are over 100 * 0.2
    final List<RID> filter = membersOfEachBucket(byBucket, 10);
    final float[] query = vector();

    final List<RID> answer = neighbors(query, 5, filter);

    assertThat(answer).as("the exact nearest members of the list").containsExactlyElementsOf(exactNearest(query, filter, 5));
    assertThat(stat("preFilterSearches"))
        .as("each bucket measures the list's share of it, not the whole list, against its pre-filter selectivity (issue #11978)")
        .isEqualTo(BUCKETS);
  }

  private Map<Integer, List<RID>> createDocs(final int perBucket) {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", BUCKETS).createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS
          + ", similarity: 'EUCLIDEAN'}");
    });
    final Map<Integer, List<RID>> byBucket = new HashMap<>();
    database.transaction(() -> {
      for (int i = 0; i < perBucket * BUCKETS; i++) {
        final float[] v = vector();
        final RID rid = database.newDocument("Doc").set("vector", v).save().getIdentity();
        vectors.put(rid, v);
        byBucket.computeIfAbsent(rid.getBucketId(), b -> new ArrayList<>()).add(rid);
      }
    });
    assertThat(byBucket).as("precondition: the records are spread over every bucket").hasSize(BUCKETS);
    assertThat(byBucket.values()).allSatisfy(rids -> assertThat(rids).hasSize(perBucket));
    return byBucket;
  }

  private static List<RID> membersOfEachBucket(final Map<Integer, List<RID>> byBucket, final int perBucket) {
    final List<RID> filter = new ArrayList<>();
    for (final List<RID> rids : byBucket.values())
      filter.addAll(rids.subList(0, perBucket));
    return filter;
  }

  @SuppressWarnings("unchecked")
  private List<RID> neighbors(final float[] query, final int k, final List<RID> filter) {
    final List<Map<String, Object>> rows = (List<Map<String, Object>>) database.query("sql",
        "SELECT `vector.neighbors`('Doc[vector]', :query, :k, { filter: :filter }) AS neighbors",
        Map.of("query", query, "k", k, "filter", filter)).next().getProperty("neighbors");
    final List<RID> rids = new ArrayList<>();
    for (final Map<String, Object> row : rows)
      rids.add((RID) row.get("@rid"));
    return rids;
  }

  private List<RID> exactNearest(final float[] query, final List<RID> filter, final int k) {
    return filter.stream().sorted(Comparator.comparingDouble(rid -> distance(query, vectors.get(rid)))).limit(k).toList();
  }

  private static double distance(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++)
      sum += (a[i] - b[i]) * (a[i] - b[i]);
    return sum;
  }

  /** The sum of a counter over the index of every bucket. */
  private long stat(final String name) {
    long total = 0;
    for (final Index index : database.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets())
      total += ((LSMVectorIndex) index).getStats().get(name);
    return total;
  }

  private static <T> T captureWarnings(final List<String> warnings, final Supplier<T> action) {
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        if (requester instanceof LSMVectorIndex && level == Level.WARNING)
          warnings.add(message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (requester instanceof LSMVectorIndex && level == Level.WARNING)
          warnings.add(message);
      }

      @Override
      public void flush() {
      }
    });
    try {
      return action.get();
    } finally {
      LogManager.instance().setLogger(new DefaultLogger());
    }
  }

  private float[] vector() {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat();
    return v;
  }
}
