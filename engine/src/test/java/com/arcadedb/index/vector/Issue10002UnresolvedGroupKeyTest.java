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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.index.sparsevector.RidScore;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10002: a group-key resolver answers {@link GroupAdmissionState#UNRESOLVED} for a candidate whose record it
 * cannot read right now, and every grouped search skips such a candidate instead of filing it under a group. Filed under
 * the {@code null} group, as the SQL functions used to do, the nearest unreadable records took one of the search's
 * group slots and kept a real group out of the answer.
 * <p>
 * Records {@code 2g} and {@code 2g + 1} form group {@code g}, nearest first. The resolver reports the three nearest
 * records as unreadable, so {@code limit = 3, groupSize = 2} must answer record 3 (group 1, whose other member is
 * unreadable) and the full groups 2 and 3 - never a group made of the unreadable records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10002UnresolvedGroupKeyTest extends TestHelper {
  private static final int COUNT       = 40;
  private static final int UNREADABLE  = 3;
  private static final int LIMIT       = 3;
  private static final int GROUP_SIZE  = 2;
  private static final List<Integer> EXPECTED_PIDS = List.of(3, 4, 5, 6, 7);

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Dense BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Dense.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Dense.grp INTEGER");
    database.command("sql", "CREATE PROPERTY Dense.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Dense (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");

    database.command("sql", "CREATE DOCUMENT TYPE Sparse BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Sparse.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Sparse.grp INTEGER");
    database.command("sql", "CREATE PROPERTY Sparse.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Sparse.weights ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON Sparse (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16, rescoreOversample: 0 }");

    // The same corpus with exact rescoring on (the default): the group-restricted search then re-resolves and caps the
    // rescored rows itself, which is a second place an unreadable record has to be skipped
    database.command("sql", "CREATE DOCUMENT TYPE Rescored BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Rescored.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Rescored.grp INTEGER");
    database.command("sql", "CREATE PROPERTY Rescored.tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY Rescored.weights ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Rescored (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");

    database.transaction(() -> {
      for (int i = 0; i < COUNT; i++) {
        database.newDocument("Rescored").set("pid", i, "grp", i / 2, "tokens", new int[] { 0 }, "weights",
            new float[] { 1.0f - i * 0.01f }).save();
        database.newDocument("Dense").set("pid", i, "grp", i / 2, "vector", new float[] { i, 0f }).save();
        database.newDocument("Sparse").set("pid", i, "grp", i / 2, "tokens", new int[] { 0 }, "weights",
            new float[] { 1.0f - i * 0.01f }).save();
      }
    });
  }

  @Test
  void denseGroupedSearchSkipsUnresolvedCandidates() {
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Dense[vector]"))
        .getIndexesOnBuckets()[0];
    final List<Pair<RID, Float>> rows = index.findNeighborsFromVectorGrouped(new float[] { 0f, 0f }, LIMIT, GROUP_SIZE, -1,
        null, resolver());
    assertThat(pids(rows.stream().map(Pair::getFirst).toList())).containsExactlyElementsOf(EXPECTED_PIDS);
  }

  @Test
  void denseGroupRestrictedSearchSkipsUnresolvedCandidates() {
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Dense[vector]"))
        .getIndexesOnBuckets()[0];
    final List<Pair<RID, Float>> rows = index.findNeighborsFromVectorForGroups(new float[] { 0f, 0f }, Set.of(0, 1, 2),
        GROUP_SIZE, -1, null, resolver(), Float.POSITIVE_INFINITY);
    // group 0 is wholly unreadable, group 1 keeps its readable member
    assertThat(pids(rows.stream().map(Pair::getFirst).toList())).containsExactly(3, 4, 5);
  }

  @Test
  void sparseGroupedSearchSkipsUnresolvedCandidates() {
    final LSMSparseVectorIndex index = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema()
        .getIndexByName("Sparse[tokens,weights]")).getIndexesOnBuckets()[0];
    final List<RidScore> rows = index.topKGrouped(new int[] { 0 }, new float[] { 1.0f }, LIMIT, GROUP_SIZE, null,
        resolver());
    assertThat(pids(rows.stream().map(RidScore::rid).toList())).containsExactlyElementsOf(EXPECTED_PIDS);
  }

  @Test
  void sparseGroupRestrictedSearchSkipsUnresolvedCandidates() {
    final LSMSparseVectorIndex index = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema()
        .getIndexByName("Sparse[tokens,weights]")).getIndexesOnBuckets()[0];
    final List<RidScore> rows = index.topKForGroups(new int[] { 0 }, new float[] { 1.0f }, Set.of(0, 1, 2), GROUP_SIZE,
        Float.NEGATIVE_INFINITY, null, resolver(), null);
    assertThat(pids(rows.stream().map(RidScore::rid).toList())).containsExactlyInAnyOrder(3, 4, 5);
  }

  @Test
  void rescoredGroupRestrictedSearchSkipsUnresolvedCandidates() {
    final LSMSparseVectorIndex index = (LSMSparseVectorIndex) ((TypeIndex) database.getSchema()
        .getIndexByName("Rescored[tokens,weights]")).getIndexesOnBuckets()[0];
    final List<RidScore> rows = index.topKForGroups(new int[] { 0 }, new float[] { 1.0f }, Set.of(0, 1, 2), GROUP_SIZE,
        Float.NEGATIVE_INFINITY, null, resolver(), null);
    assertThat(pids(rows.stream().map(RidScore::rid).toList())).containsExactlyInAnyOrder(3, 4, 5);
  }

  @Test
  void admissionNeverOpensAGroupForAnUnresolvedKey() {
    final GroupAdmissionState groups = new GroupAdmissionState(1, 1);
    assertThat(groups.admit(GroupAdmissionState.UNRESOLVED)).isFalse();
    assertThat(groups.distinctGroups()).isZero();
    // the one slot is still free for a real group, including the ordinary null group of a readable record
    assertThat(groups.admit(null)).isTrue();
    assertThat(groups.isFull()).isTrue();
  }

  @Test
  void plannerIgnoresUnresolvedRows() {
    // Source 0 holds an unresolved row that would otherwise be the best peak overall and win a group of its own,
    // pushing group "b" out of the winners and asking source 1 for a group no record belongs to.
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 2);
    planner.addSource(List.of(new RID(1, 0), new RID(1, 1), new RID(1, 2)), new float[] { 0.99f, 0.9f, 0.8f },
        listOf(GroupAdmissionState.UNRESOLVED, "b", "b"), true);
    planner.addSource(List.of(new RID(2, 0), new RID(2, 1)), new float[] { 0.5f, 0.4f }, listOf("c", "c"), true);
    // "b" wins with two members at or above source 1's cut-off, so nothing needs a second phase
    assertThat(planner.plan()).isEmpty();
  }

  /** Reads the record's group, but reports the {@link #UNREADABLE} nearest records as unreadable. */
  private Function<RID, Object> resolver() {
    final Set<RID> unreadable = new HashSet<>();
    for (final String type : new String[] { "Dense", "Sparse", "Rescored" })
      database.query("sql", "SELECT @rid AS rid FROM " + type + " WHERE pid < " + UNREADABLE)
          .forEachRemaining(r -> unreadable.add(r.getProperty("rid")));
    assertThat(unreadable).hasSize(UNREADABLE * 3);
    return rid -> unreadable.contains(rid) ?
        GroupAdmissionState.UNRESOLVED :
        ((Document) database.lookupByRID(rid, true)).get("grp");
  }

  private List<Integer> pids(final List<RID> rids) {
    final Map<RID, Integer> cache = new HashMap<>();
    final List<Integer> pids = new ArrayList<>(rids.size());
    for (final RID rid : rids)
      pids.add(cache.computeIfAbsent(rid, r -> ((Document) database.lookupByRID(r, true)).getInteger("pid")));
    return pids;
  }

  private static List<Object> listOf(final Object... keys) {
    final List<Object> list = new ArrayList<>(keys.length);
    for (final Object key : keys)
      list.add(key);
    return list;
  }
}
