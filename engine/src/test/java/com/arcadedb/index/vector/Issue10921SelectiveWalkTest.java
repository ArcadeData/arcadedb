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
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10921: the walk restricted by a predicate an index can list is given up for the listing once it proves the predicate
 * selective, and the plan then scores what the index lists. These tests drive the index with a predicate of their own, so
 * they can tell the two outcomes apart: the listing taken, and the listing refused (more candidates than the walk's cost
 * allows), where the walk has to answer after all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10921SelectiveWalkTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int VERTICES   = 1_000;
  private static final int K          = 3;

  @Test
  void aSelectivePredicateGivesTheWalkUpForTheListing() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();
    final Set<RID> members = ridsOf(3, 420, 917);

    final ListedPredicate predicate = new ListedPredicate(members, true);
    final long plansBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

    assertThat(predicate.listingCaps).as("the walk was given up for the listing").hasSize(1);
    assertThat(predicate.listingCaps.getFirst()).isGreaterThanOrEqualTo(members.size());
    assertThat(index.getStats().get("preFilterSearches")).isEqualTo(plansBefore + 1);
    assertThat(predicate.tested).as("a few dozen candidates walked, not the graph").isLessThan(VERTICES / 4);
    assertThat(ridsIn(results)).containsExactlyElementsOf(ridsIn(index.findNeighborsFromVector(embedding(7), K, -1, members)));
  }

  @Test
  void aRefusedListingLeavesTheAnswerToTheWalk() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();
    final Set<RID> members = ridsOf(3, 420, 917);

    // The index cannot list the candidates under the cap the walk's cost allows: the walk answers, unbounded
    final ListedPredicate predicate = new ListedPredicate(members, false);
    final long plansBefore = index.getStats().getOrDefault("preFilterSearches", 0L);
    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

    assertThat(predicate.listingCaps).hasSize(1);
    assertThat(index.getStats().get("preFilterSearches")).isEqualTo(plansBefore);
    assertThat(ridsIn(results)).containsExactlyInAnyOrderElementsOf(members);
  }

  @Test
  void aBroadPredicateIsWalkedWithoutAskingForTheListing() {
    createSchemaAndData();
    final LSMVectorIndex index = vectorIndex();
    final Set<RID> members = new HashSet<>();
    for (int i = 0; i < VERTICES; i += 2)
      members.add(ridOf("doc" + i));

    final ListedPredicate predicate = new ListedPredicate(members, true);
    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

    assertThat(predicate.listingCaps).as("half the records: the walk is the plan").isEmpty();
    assertThat(results).hasSize(K);
    for (final Pair<RID, Float> r : results)
      assertThat(members).contains(r.getFirst());
  }

  @Test
  void aDisabledPreFilterNeverGivesTheWalkUp() {
    createSchemaAndData();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY, 0f);
    try {
      final LSMVectorIndex index = vectorIndex();
      final Set<RID> members = ridsOf(3, 420, 917);
      final ListedPredicate predicate = new ListedPredicate(members, true);
      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), K, -1, predicate);

      assertThat(predicate.listingCaps).isEmpty();
      assertThat(ridsIn(results)).containsExactlyInAnyOrderElementsOf(members);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY,
          GlobalConfiguration.VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY.getDefValue());
    }
  }

  /**
   * A predicate whose members an index can list: {@code lists} false makes every listing come back over the cap.
   */
  private static final class ListedPredicate extends AbstractSet<RID> implements PredicateRidSet {
    private final Set<RID>      members;
    private final boolean       lists;
    private final List<Integer> listingCaps = new ArrayList<>();
    private       int           tested;

    ListedPredicate(final Set<RID> members, final boolean lists) {
      this.members = members;
      this.lists = lists;
    }

    @Override
    public boolean contains(final Object o) {
      ++tested;
      return members.contains(o);
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public int size() {
      return Integer.MAX_VALUE;
    }

    @Override
    public Iterator<RID> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean listsCandidates(final int bucketId) {
      return true;
    }

    @Override
    public Candidates listCandidates(final int bucketId, final int maxCandidates) {
      listingCaps.add(maxCandidates);
      return lists && members.size() <= maxCandidates ? new Candidates(members, true) : null;
    }
  }

  private void createSchemaAndData() {
    // No rebuild may run mid-test, or the live counts the plan is measured against would move under the test
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withQuantization(VectorQuantizationType.NONE).withSimilarity("EUCLIDEAN").create();
    });

    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
    });

    vectorIndex().buildVectorGraphNow();
  }

  private Set<RID> ridsOf(final int... ids) {
    final Set<RID> rids = new HashSet<>();
    for (final int id : ids)
      rids.add(ridOf("doc" + id));
    return rids;
  }

  private RID ridOf(final String id) {
    return database.query("sql", "SELECT @rid AS rid FROM Doc WHERE id = ?", id).next().getProperty("rid");
  }

  private static List<RID> ridsIn(final List<Pair<RID, Float>> results) {
    final List<RID> rids = new ArrayList<>(results.size());
    for (final Pair<RID, Float> r : results)
      rids.add(r.getFirst());
    return rids;
  }

  private LSMVectorIndex vectorIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  /** Deterministic, distinct, non-zero vectors, well spread so no two of them tie on distance. */
  private static float[] embedding(final int i) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(i * 0.37 + j * 0.91) + 2.0f + i * 0.013f;
    return vector;
  }
}
