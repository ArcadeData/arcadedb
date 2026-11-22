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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A search restricted by an allow-list ({@code filter}, {@code where}, or the {@code allowedRIDs} of the index API) answers
 * {@code min(k, members of the list that match the query)} rows, however far down the unfiltered ranking they sit.
 * <p>
 * The committed side fetched a fixed multiple of {@code k} candidates and dropped the ones outside the list, so a list whose
 * members all rank below that multiple answered short or empty although the index held them (the dense search never does:
 * it keeps walking until it holds {@code k} members).
 */
class SparseFilteredTopKFillTest extends TestHelper {
  private static final String TYPE      = "FilteredDoc";
  private static final String INDEX     = TYPE + "[tokens,weights]";
  private static final int    DOCS      = 600;
  private static final int    FAVOURED  = 4;
  private final List<RID>     rids      = new ArrayList<>();

  @BeforeEach
  void load() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
      database.command("sql", "CREATE PROPERTY " + TYPE + ".tokens ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY " + TYPE + ".weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY " + TYPE + ".tag INTEGER");
      database.command("sql", "CREATE INDEX ON " + TYPE + " (tokens, weights) LSM_SPARSE_VECTOR METADATA {\"dimensions\": 16}");
    });
    // every document holds dim 5; the higher the position, the higher the score, so the first FAVOURED documents rank last
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++)
        rids.add(database.newDocument(TYPE).set("tokens", new int[] { 5 }).set("weights", new float[] { 1f + i * 0.01f })
            .set("tag", i < FAVOURED ? 1 : 0).save().getIdentity());
    });
  }

  private List<RID> neighbors(final int k, final Map<String, Object> options) {
    final List<RID> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?, ?))", INDEX, new int[] { 5 },
        new float[] { 1f }, k, options)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add((RID) row.getProperty("@rid"));
      }
    }
    return out;
  }

  @Test
  void filterOfTheLowestRankedDocumentsIsAnsweredInFull() {
    final List<RID> favoured = rids.subList(0, FAVOURED);
    assertThat(neighbors(FAVOURED, Map.of("filter", favoured))).containsExactlyInAnyOrderElementsOf(favoured);
  }

  @Test
  void filterWithFewerMembersThanKIsAnsweredWithAllOfThem() {
    final List<RID> favoured = rids.subList(0, FAVOURED);
    assertThat(neighbors(10, Map.of("filter", favoured))).containsExactlyInAnyOrderElementsOf(favoured);
  }

  @Test
  void wherePredicateOfTheLowestRankedDocumentsIsAnsweredInFull() {
    final List<RID> favoured = rids.subList(0, FAVOURED);
    assertThat(neighbors(FAVOURED, Map.of("where", Map.of("tag", 1)))).containsExactlyInAnyOrderElementsOf(favoured);
  }

  @Test
  void indexApiAllowedRidsOfTheLowestRankedDocumentsIsAnsweredInFull() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    final Set<RID> favoured = new HashSet<>(rids.subList(0, FAVOURED));
    final List<RID> got = new ArrayList<>();
    for (final var sub : typeIndex.getIndexesOnBuckets())
      for (final RidScore score : ((LSMSparseVectorIndex) sub).topK(new int[] { 5 }, new float[] { 1f }, FAVOURED, favoured))
        got.add(score.rid());
    assertThat(got).containsExactlyInAnyOrderElementsOf(favoured);
  }

  /**
   * The widening is for members the fetch did not reach. A listed allow-list whose members the first fetch already holds has
   * none left to find, and must not walk the rest of the index looking for them: the answer is complete at the first pass.
   */
  @Test
  void listedAllowListFoundInFullByTheFirstFetchIsNotWidened() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(INDEX);
    // the best ranked documents: the first fetch holds them all, and the answer wants more rows (keep) than the list has
    final CountingRidSet best = new CountingRidSet(rids.subList(DOCS - FAVOURED, DOCS));
    final List<RID> got = new ArrayList<>();
    for (final var sub : typeIndex.getIndexesOnBuckets())
      for (final RidScore score : ((LSMSparseVectorIndex) sub).topK(new int[] { 5 }, new float[] { 1f }, 10, best))
        got.add(score.rid());
    final int lookups = best.lookups;

    assertThat(got).containsExactlyInAnyOrderElementsOf(best);
    // one pass over the first budget, not the whole index: widening x8 per step walks every one of the DOCS candidates
    assertThat(lookups).isLessThan(DOCS);
  }

  /** The transaction's own rows of the list are merged with the committed ones the widened fetch found. */
  @Test
  void filteredSearchInsideATransactionReturnsPendingAndLowestRankedCommittedMembers() {
    final List<RID> expected = new ArrayList<>(rids.subList(0, FAVOURED));
    database.begin();
    try {
      expected.add(database.newDocument(TYPE).set("tokens", new int[] { 5 }).set("weights", new float[] { 0.5f }).set("tag", 1)
          .save().getIdentity());
      assertThat(neighbors(10, Map.of("where", Map.of("tag", 1)))).containsExactlyInAnyOrderElementsOf(expected);
    } finally {
      database.rollback();
    }
  }

  /** A listed allow-list that counts the membership tests a search asks of it. */
  private static final class CountingRidSet extends HashSet<RID> {
    private int lookups;

    private CountingRidSet(final List<RID> members) {
      super(members);
    }

    @Override
    public boolean contains(final Object o) {
      ++lookups;
      return super.contains(o);
    }
  }
}
