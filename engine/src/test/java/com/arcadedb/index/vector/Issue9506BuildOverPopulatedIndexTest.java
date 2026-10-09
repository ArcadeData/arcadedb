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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9506: {@code LSMVectorIndex.build()} over an index that already holds entries appended a second vector id per
 * record instead of replacing the existing one, so {@code countEntries()} doubled, a later delete tombstoned two ids per
 * record, and a search, which builds the graph keyed by RID, then disagreed with the count. A dense vector index keeps
 * ONE live vector per record, and a build over a populated index must preserve that.
 */
class Issue9506BuildOverPopulatedIndexTest extends TestHelper {
  private static final String TYPE    = "Probe";
  private static final int    RECORDS = 10;

  @Test
  void aBuildOverAPopulatedIndexKeepsOneEntryPerRecord() {
    final List<RID> rids = populate();
    final LSMVectorIndex index = bucketIndex();
    assertThat(index.countEntries()).isEqualTo(RECORDS);
    final Set<Integer> beforeBuild = liveIds(index, rids);

    assertThat(index.build(null, null)).isEqualTo(RECORDS);
    final Set<Integer> afterFirstBuild = assertOneLiveEntryPerRecord(index, rids);
    // REPLACED, not kept: every record got a fresh id and the ids it had before the build are tombstoned
    assertThat(afterFirstBuild).doesNotContainAnyElementsOf(beforeBuild);
    for (final int id : beforeBuild)
      assertThat(index.getVectorIndex().isLive(id)).as("vector id %d held before the build", id).isFalse();

    // A second build is no different: the count does not grow with the number of builds
    index.build(null, null);
    assertThat(assertOneLiveEntryPerRecord(index, rids)).doesNotContainAnyElementsOf(afterFirstBuild);
  }

  /**
   * A build sharing a transaction its caller opened (the {@code CREATE INDEX} inside an open transaction path, issue
   * #6324) never commits a chunk of its own, so the replacement has to land in the caller's commit.
   */
  @Test
  void aBuildSharingTheCallersTransactionKeepsOneEntryPerRecord() {
    final List<RID> rids = populate();
    final LSMVectorIndex index = bucketIndex();

    database.transaction(() -> index.build(0, true, null));
    assertOneLiveEntryPerRecord(index, rids);
  }

  @Test
  void aDeleteAfterABuildOverAPopulatedIndexRemovesExactlyOneEntry() {
    final List<RID> rids = populate();
    final LSMVectorIndex index = bucketIndex();
    index.build(null, null);

    database.transaction(() -> rids.getFirst().asDocument().delete());

    assertThat(index.countEntries()).isEqualTo(RECORDS - 1);
    assertThat(index.getVectorIndex().getVectorIdsForRid(rids.getFirst())).isEmpty();
  }

  @Test
  void aSearchAfterABuildOverAPopulatedIndexReturnsEveryRecordOnceAndLeavesTheCountAlone() {
    final List<RID> rids = populate();
    final LSMVectorIndex index = bucketIndex();
    index.build(null, null);
    // Before the search too: the graph a search builds keeps one entry per RID, which used to hide the duplicates
    assertThat(index.countEntries()).isEqualTo(RECORDS);

    final List<Pair<RID, Float>> neighbors = index.findNeighborsFromVector(new float[] { 0.5f, 0.5f }, RECORDS * 3);
    final List<RID> found = new ArrayList<>();
    for (final Pair<RID, Float> neighbor : neighbors)
      found.add(neighbor.getFirst());

    assertThat(found).doesNotHaveDuplicates();
    assertThat(new HashSet<>(found)).isEqualTo(new HashSet<>(rids));
    assertThat(index.countEntries()).isEqualTo(RECORDS);
  }

  private List<RID> populate() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + TYPE + ".vector ARRAY_OF_FLOATS");
    // EUCLIDEAN, so that the all-zero vector below is indexed and returned like any other (issue #8962)
    database.command("sql",
        "CREATE INDEX ON " + TYPE + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");

    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      // The origin included on purpose: a removal keyed by an all-zero placeholder would coincide with its key
      rids.add(database.newDocument(TYPE).set("vector", new float[] { 0f, 0f }).save().getIdentity());
      for (int i = 1; i < RECORDS; i++)
        rids.add(database.newDocument(TYPE).set("vector", new float[] { i / 10f, (RECORDS - i) / 10f }).save().getIdentity());
    });
    return rids;
  }

  private LSMVectorIndex bucketIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(TYPE + "[vector]")).getIndexesOnBuckets()[0];
  }

  private static Set<Integer> assertOneLiveEntryPerRecord(final LSMVectorIndex index, final List<RID> rids) {
    assertThat(index.countEntries()).as("live entries after a build over a populated index").isEqualTo(RECORDS);
    for (final RID rid : rids)
      assertThat(index.getVectorIndex().getVectorIdsForRid(rid)).as("live vector ids of %s", rid).hasSize(1);
    final Set<Integer> ids = liveIds(index, rids);
    assertThat(ids).hasSize(RECORDS);
    return ids;
  }

  private static Set<Integer> liveIds(final LSMVectorIndex index, final List<RID> rids) {
    final Set<Integer> ids = new HashSet<>();
    for (final RID rid : rids)
      for (final int id : index.getVectorIndex().getVectorIdsForRid(rid))
        ids.add(id);
    return ids;
  }
}
