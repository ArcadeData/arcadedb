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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11765: each sub-index of a multi-bucket vector index is searched with the type's whole RID allow-list, and must judge
 * it by the members in its own bucket. Judged by its total size, a list selective in every bucket walked every graph, came up
 * short of the rows in the other buckets, and scanned every bucket after the walk.
 */
class AllowListBucketShareIssue11765Test extends TestHelper {
  private static final int BUCKETS            = 8;
  private static final int VECTORS_PER_BUCKET = 200;
  private static final int DIMENSIONS         = 16;
  // 10 members per bucket: 5% of a bucket, under the 20% pre-filter threshold; 80 in all, 40% of a bucket, over it
  private static final int MEMBERS_PER_BUCKET = 10;

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
  void eachBucketScoresItsShareOfTheListInsteadOfWalkingAndScanning() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V BUCKETS " + BUCKETS);
      database.command("sql", "CREATE PROPERTY V.emb ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON V (emb) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\"}");
    });
    final Random random = new Random(5);
    final List<float[]> vectors = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < BUCKETS * VECTORS_PER_BUCKET; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = (float) random.nextGaussian();
        vectors.add(vector);
        database.newVertex("V").set("emb", vector).save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("V[emb]");
    final List<LSMVectorIndex> subIndexes = new ArrayList<>();
    for (final Index index : typeIndex.getIndexesOnBuckets()) {
      final LSMVectorIndex subIndex = (LSMVectorIndex) index;
      subIndex.buildVectorGraphNow();
      subIndexes.add(subIndex);
    }
    assertThat(subIndexes).hasSize(BUCKETS);

    final List<RID> allowed = new ArrayList<>();
    for (final LSMVectorIndex subIndex : subIndexes)
      try (final ResultSet rows = database.query("sql",
          "SELECT @rid AS rid FROM bucket:" + database.getSchema().getBucketById(subIndex.getAssociatedBucketId()).getName()
              + " LIMIT " + MEMBERS_PER_BUCKET)) {
        rows.stream().forEach(row -> allowed.add(row.getProperty("rid")));
      }
    assertThat(allowed).hasSize(BUCKETS * MEMBERS_PER_BUCKET);

    final long scansBefore = sum(subIndexes, "bruteForceScans");
    final long preFiltersBefore = sum(subIndexes, "preFilterSearches");

    final List<RID> answered = new ArrayList<>();
    try (final ResultSet rows = database.query("sql",
        "SELECT expand(vector.neighbors('V[emb]', ?, ?, {filter: ?}))", vectors.getFirst(), allowed.size(), allowed)) {
      rows.stream().map(Result::getIdentity).forEach(rid -> answered.add(rid.orElseThrow()));
    }

    assertThat(answered).as("every member of the list, nearest first").containsExactlyInAnyOrderElementsOf(allowed);
    assertThat(sum(subIndexes, "preFilterSearches") - preFiltersBefore)
        .as("each bucket scores its share of the list directly").isEqualTo(BUCKETS);
    assertThat(sum(subIndexes, "bruteForceScans") - scansBefore)
        .as("no bucket falls short of rows that are in the other buckets").isZero();
  }

  private static long sum(final List<LSMVectorIndex> subIndexes, final String stat) {
    long total = 0;
    for (final LSMVectorIndex subIndex : subIndexes)
      total += ((Number) subIndex.getStats().get(stat)).longValue();
    return total;
  }
}
