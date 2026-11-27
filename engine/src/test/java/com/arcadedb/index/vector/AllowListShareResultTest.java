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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a sub-index answers for a RID allow-list, whichever way the list relates to its bucket: every member in the bucket (the
 * list is used as it is), a mix of members and RIDs of other buckets or of no record (the bucket takes its share), no member
 * here (nothing is answered), and a list longer than any plan can use. The rows are checked against a brute-force cosine
 * ranking, so the answer of the share is pinned, not only its size.
 */
class AllowListShareResultTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int VECTORS    = 1600;

  @BeforeEach
  void onlyTheExplicitBuilds() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void restoreTheRebuildTriggers() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  @Test
  void oneBucketTypeAnswersTheListInFull() {
    check(1);
  }

  @Test
  void severalBucketsEachAnswerTheirShare() {
    check(4);
  }

  private void check(final int buckets) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY V.emb ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON V (emb) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\"}");
    });
    final Random random = new Random(11);
    final Map<RID, float[]> vectors = new LinkedHashMap<>();
    database.transaction(() -> {
      for (int i = 0; i < VECTORS; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian();
        vectors.put(database.newVertex("V").set("emb", vector).save().getIdentity(), vector);
      }
    });
    for (final Index index : ((TypeIndex) database.getSchema().getIndexByName("V[emb]")).getIndexesOnBuckets())
      ((LSMVectorIndex) index).buildVectorGraphNow();

    final List<RID> all = new ArrayList<>(vectors.keySet());
    final float[] query = vectors.get(all.get(7)).clone();
    for (int d = 0; d < DIMENSIONS; d++)
      query[d] += 0.3f * (float) random.nextGaussian();

    // every member in the list is a record of the type; 5% of the type, so each bucket takes the pre-filter plan: exact ranking
    final List<RID> members = pick(all, 80, 1);
    assertAnswer(vectors, query, members, members.size());
    assertAnswer(vectors, query, members, 10);

    // the same members, repeated in a list that also holds RIDs of another bucket id and of no record
    final List<Object> mixed = new ArrayList<>(members);
    mixed.add(new RID(4000, 3));
    mixed.add(new RID(0, 99_999_999L));
    mixed.add(members.getFirst().toString());
    assertAnswer(vectors, query, mixed, members.size(), members);

    // no member of the type at all
    try (final ResultSet rows = database.query("sql", "SELECT expand(vector.neighbors('V[emb]', ?, 10, {filter: ?}))", query,
        List.of(new RID(4000, 1), new RID(4000, 2)))) {
      assertThat(rows.hasNext()).isFalse();
    }

    // members of one bucket only
    final int bucketId = members.getFirst().getBucketId();
    final List<RID> oneBucket = new ArrayList<>();
    for (final RID rid : all)
      if (rid.getBucketId() == bucketId && oneBucket.size() < 60)
        oneBucket.add(rid);
    assertAnswer(vectors, query, oneBucket, oneBucket.size());

    // a list longer than any plan can use (60% of the type): the walk answers, every member comes back for k = the length
    final List<RID> most = pick(all, VECTORS * 6 / 10, 3);
    final List<RID> answered = answer(query, most, most.size());
    assertThat(answered).containsExactlyInAnyOrderElementsOf(most);
  }

  private List<RID> pick(final List<RID> all, final int count, final long seed) {
    final List<RID> shuffled = new ArrayList<>(all);
    Collections.shuffle(shuffled, new Random(seed));
    return new ArrayList<>(shuffled.subList(0, count));
  }

  private void assertAnswer(final Map<RID, float[]> vectors, final float[] query, final List<?> filter, final int k) {
    final List<RID> expectedMembers = new ArrayList<>();
    for (final Object item : filter)
      expectedMembers.add(item instanceof RID rid ? rid : new RID((String) item));
    assertAnswer(vectors, query, filter, k, expectedMembers);
  }

  private void assertAnswer(final Map<RID, float[]> vectors, final float[] query, final List<?> filter, final int k,
      final List<RID> members) {
    final List<RID> existing = new ArrayList<>();
    for (final RID rid : members)
      if (vectors.containsKey(rid) && !existing.contains(rid))
        existing.add(rid);
    existing.sort(Comparator.comparingDouble((RID rid) -> -cosine(query, vectors.get(rid))).thenComparing(RID::compareTo));
    final List<RID> expected = existing.subList(0, Math.min(k, existing.size()));
    assertThat(answer(query, filter, k)).containsExactlyElementsOf(expected);
  }

  private List<RID> answer(final float[] query, final List<?> filter, final int k) {
    final List<RID> answered = new ArrayList<>();
    try (final ResultSet rows = database.query("sql", "SELECT expand(vector.neighbors('V[emb]', ?, ?, {filter: ?}))", query, k,
        filter)) {
      while (rows.hasNext())
        answered.add(rows.next().getIdentity().orElseThrow());
    }
    return answered;
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return dot / Math.sqrt(na * nb);
  }
}
