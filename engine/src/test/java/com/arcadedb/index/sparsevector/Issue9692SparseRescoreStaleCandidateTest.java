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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9692 (from #9634): with exact rescoring on - the default - a sparse search fetches {@code k * oversample}
 * candidates and loads each one's record to rescore it. A candidate whose record a concurrent commit deleted between
 * the index answer and the record load was dropped and nothing replaced it, so a commit that removed the whole
 * oversampled window left the answer empty while plenty of matching records were live.
 * <p>
 * The race is reproduced deterministically the way the dense twin's test does it (issue #9368): the records are removed
 * from the bucket only, leaving their index entries behind, which is exactly the state a search sees between a commit's
 * record delete and its index update.

 */
class Issue9692SparseRescoreStaleCandidateTest extends TestHelper {
  private static final int   COUNT  = 60;
  private static final int   STALE  = 20;
  private static final int   GROUPS = 5;
  private static final int[]   QUERY_DIMS    = { 0 };
  private static final float[] QUERY_WEIGHTS = { 1.0f };

  /** RID to pid and to group key, captured before the records are removed: a key resolved before the delete. */
  private final Map<RID, Integer> pidByRid = new HashMap<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.grp INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS");
    // All defaults: INT8 weights and the automatic rescoring oversample (2), so exact rescoring is on.
    database.command("sql", "CREATE INDEX ON Doc (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        database.newDocument("Doc").set("pid", i, "grp", i % GROUPS, "tokens", new int[] { 0 }, "weights",
            new float[] { 1.0f - i * 0.01f }).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, pid FROM Doc")) {
      rs.forEachRemaining(r -> pidByRid.put(r.getProperty("rid"), r.getProperty("pid")));
    }
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the test leaves index entries pointing at records it removed from the bucket, on purpose
    return false;
  }

  @Test
  void topKTopsUpCandidatesWhoseRecordIsGone() {
    removeBestRecordsFromBucketOnly();

    // k = 10 with oversample 2 fetches the best 20 candidates: every one of them is gone.
    assertThat(pids(index().topK(QUERY_DIMS, QUERY_WEIGHTS, 10, null))).containsExactly(20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
  }

  @Test
  void topKWithAWhitelistTopsUpCandidatesWhoseRecordIsGone() {
    removeBestRecordsFromBucketOnly();

    final Set<RID> allowed = new HashSet<>(pidByRid.keySet());
    assertThat(pids(index().topK(QUERY_DIMS, QUERY_WEIGHTS, 10, allowed))).containsExactly(20, 21, 22, 23, 24, 25, 26, 27, 28,
        29);
  }

  @Test
  void topKInsideATransactionTopsUpCandidatesWhoseRecordIsGone() {
    removeBestRecordsFromBucketOnly();

    database.begin();
    try {
      // A pending row below every committed one, so the overlay is in play without taking any of the k slots.
      database.newDocument("Doc").set("pid", 1000, "grp", 0, "tokens", new int[] { 0 }, "weights", new float[] { 0.001f }).save();
      assertThat(pids(index().topK(QUERY_DIMS, QUERY_WEIGHTS, 10, null))).containsExactly(20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
    } finally {
      database.rollback();
    }
  }

  @Test
  void topKGroupedTopsUpCandidatesWhoseRecordIsGone() {
    removeBestRecordsFromBucketOnly();

    // limit 3, groupSize 2, oversample 2: the engine hands over 4 members per group, and the best 4 of every group are gone.
    final List<RidScore> hits = index().topKGrouped(QUERY_DIMS, QUERY_WEIGHTS, 3, 2, null, groupKeyResolver());
    assertThat(pids(hits)).containsExactly(20, 21, 22, 25, 26, 27);
  }

  @Test
  void topKForGroupsTopsUpCandidatesWhoseRecordIsGone() {
    removeBestRecordsFromBucketOnly();

    final List<RidScore> hits = index().topKForGroups(QUERY_DIMS, QUERY_WEIGHTS, Set.of(0, 1), 2, Float.NEGATIVE_INFINITY, null,
        groupKeyResolver(), null);
    assertThat(pids(hits)).containsExactly(20, 21, 25, 26);
  }

  @Test
  void topKStillAnswersShortWhenTheIndexReallyHoldsFewer() {
    // Every record but 3 removed: the widening must stop at what the index holds rather than loop on nothing.
    final List<RID> all = new ArrayList<>(pidByRid.keySet());
    all.removeIf(rid -> pidByRid.get(rid) >= COUNT - 3);
    removeFromBucketOnly(all);

    assertThat(pids(index().topK(QUERY_DIMS, QUERY_WEIGHTS, 10, null))).containsExactly(57, 58, 59);
  }

  private void removeBestRecordsFromBucketOnly() {
    final List<RID> stale = new ArrayList<>();
    for (final Map.Entry<RID, Integer> e : pidByRid.entrySet())
      if (e.getValue() < STALE)
        stale.add(e.getKey());
    assertThat(stale).hasSize(STALE);
    removeFromBucketOnly(stale);

    // Precondition: the index still answers with the removed records, otherwise this test proves nothing.
    final List<RID> fromIndex = new ArrayList<>();
    try {
      fromIndex.addAll(index().getEngine().topK(QUERY_DIMS, QUERY_WEIGHTS, STALE).stream().map(RidScore::rid).toList());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    assertThat(fromIndex).containsExactlyInAnyOrderElementsOf(stale);
  }

  private void removeFromBucketOnly(final List<RID> rids) {
    database.transaction(() -> {
      for (final RID rid : rids)
        ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).deleteRecord(rid);
    });
  }

  private Function<RID, Object> groupKeyResolver() {
    // Resolved from what the record held before it was removed: the key a resolver read just before the delete.
    return rid -> pidByRid.get(rid) % GROUPS;
  }

  private List<Integer> pids(final List<RidScore> hits) {
    final List<Integer> out = new ArrayList<>(hits.size());
    for (final RidScore hit : hits)
      out.add(pidByRid.get(hit.rid()));
    return out;
  }

  private LSMSparseVectorIndex index() {
    return (LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[tokens,weights]")).getIndexesOnBuckets()[0];
  }
}
