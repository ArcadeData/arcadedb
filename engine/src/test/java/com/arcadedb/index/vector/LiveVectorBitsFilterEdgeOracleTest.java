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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The edges of the predicate the graph walk of every search asks about a candidate ordinal ({@link LiveVectorBitsFilter#get}):
 * empty allow-lists and superseded sets restrict nothing, an ordinal outside the map or a vector id the location map does not know
 * is never answered, and a walk over a selective listed predicate is given up for the listing on every 32nd call (and only then),
 * unless it is disarmed or has no candidate cap.
 */
class LiveVectorBitsFilterEdgeOracleTest {
  private static final int BUCKET = 7;

  @Test
  void anEmptyAllowListAndAnEmptySupersededSetRestrictNothing() {
    final VectorLocationIndex locations = new VectorLocationIndex();
    for (int id = 0; id < 3; id++)
      locations.addOrUpdate(id, false, 10L + id, new RID(BUCKET, id), false);
    locations.markDeleted(1);
    final int[] map = { 0, 1, 2 };
    final LiveVectorBitsFilter filter = new LiveVectorBitsFilter(new HashSet<>(), map, locations, new HashSet<>());
    assertThat(filter.get(0)).isTrue();
    assertThat(filter.get(1)).as("deleted").isFalse();
    assertThat(filter.get(2)).isTrue();
    assertThat(filter.get(3)).as("past the map").isFalse();
    assertThat(filter.get(-1)).as("before the map").isFalse();
    assertThat(filter.get(Integer.MIN_VALUE)).isFalse();
    assertThat(filter.get(Integer.MAX_VALUE)).isFalse();
    assertThat(filter.givesUpForTheListing()).isFalse();
  }

  @Test
  void aVectorTheLocationMapNoLongerKnowsIsNeverAnswered() {
    final VectorLocationIndex locations = new VectorLocationIndex();
    locations.addOrUpdate(0, false, 10L, new RID(BUCKET, 0), false);
    // Vector id 1 was never added: with an allow-list or superseded set the filter asks for its RID, which it does not have
    final Set<RID> allowed = Set.of(new RID(BUCKET, 0), new RID(BUCKET, 1));
    assertThat(new LiveVectorBitsFilter(allowed, new int[] { 0, 1 }, locations).get(1)).isFalse();
    assertThat(new LiveVectorBitsFilter(null, new int[] { 0, 1 }, locations, Set.of(new RID(BUCKET, 9))).get(1)).isFalse();
    assertThat(new LiveVectorBitsFilter(null, new int[] { 0, 1 }, locations).get(1)).isFalse();
    assertThat(new LiveVectorBitsFilter(null, new int[] { 0, 1 }, locations).get(0)).isTrue();
  }

  @Test
  void aSelectiveWalkIsGivenUpForTheListingAndAnUnselectiveOneIsNot() {
    final int vectors = 4_096;
    final VectorLocationIndex locations = new VectorLocationIndex();
    final int[] map = new int[vectors];
    final Set<RID> allowed = new HashSet<>();
    for (int id = 0; id < vectors; id++) {
      final RID rid = new RID(BUCKET, id);
      locations.addOrUpdate(id, false, id, rid, false);
      map[id] = id;
      if (id % 1024 == 0)
        allowed.add(rid); // 0.1% of the vectors
    }

    // The walk needs 10 acceptable candidates and meets 1 in a thousand: the listing is cheaper, and is asked for
    final LiveVectorBitsFilter selective = new LiveVectorBitsFilter(allowed, map, locations, null, null, 10, vectors, 1_000);
    assertThat(selective.givesUpForTheListing()).isTrue();
    int calls = 0;
    boolean gaveUp = false;
    try {
      for (int ordinal = 1; ordinal < vectors && ordinal < 200; ordinal++) {
        calls++;
        selective.get(ordinal);
      }
    } catch (final LiveVectorBitsFilter.SelectiveWalkException e) {
      gaveUp = true;
      assertThat(e.listingCap).as("the listing is capped at what the plan takes").isBetween(1, 1_000);
    }
    assertThat(gaveUp).as("given up within %d calls", calls).isTrue();
    assertThat(calls % 32).as("the estimate is made every 32nd call, and nowhere else").isZero();

    // A disarmed walk (the resumed pass) is never given up
    final LiveVectorBitsFilter resumed = new LiveVectorBitsFilter(allowed, map, locations, null, null, 10, vectors, 1_000);
    resumed.disarm();
    assertThat(resumed.givesUpForTheListing()).isFalse();
    for (int ordinal = 1; ordinal < 600; ordinal++)
      assertThat(resumed.get(ordinal)).isEqualTo(allowed.contains(new RID(BUCKET, ordinal)));

    // No candidate cap, no listing: never given up
    final LiveVectorBitsFilter noCap = new LiveVectorBitsFilter(allowed, map, locations, null, null, 10, vectors, 0);
    assertThat(noCap.givesUpForTheListing()).isFalse();

    // A predicate that keeps most of what the walk meets is walked to its end
    final Set<RID> broad = new HashSet<>();
    for (int id = 0; id < vectors; id++)
      if (id % 10 != 0)
        broad.add(new RID(BUCKET, id));
    final LiveVectorBitsFilter unselective = new LiveVectorBitsFilter(broad, map, locations, null, null, 10, vectors, 1_000);
    for (int ordinal = 0; ordinal < vectors; ordinal++)
      assertThat(unselective.get(ordinal)).as("ordinal %d: answered, never given up", ordinal)
          .isEqualTo(broad.contains(new RID(BUCKET, ordinal)));
  }

  @Test
  void aRowTheCallingTransactionSupersededIsRefusedWhetherOrNotThereIsAnAllowList() {
    final VectorLocationIndex locations = new VectorLocationIndex();
    for (int id = 0; id < 3; id++)
      locations.addOrUpdate(id, false, 10L + id, new RID(BUCKET, id), false);
    final int[] map = { 0, 1, 2 };
    final Set<RID> superseded = Set.of(new RID(BUCKET, 1));

    final LiveVectorBitsFilter unrestricted = new LiveVectorBitsFilter(null, map, locations, superseded);
    assertThat(unrestricted.get(0)).isTrue();
    assertThat(unrestricted.get(1)).as("live, but superseded").isFalse();
    assertThat(unrestricted.get(2)).isTrue();

    final LiveVectorBitsFilter listed = new LiveVectorBitsFilter(Set.of(new RID(BUCKET, 0), new RID(BUCKET, 1)), map, locations,
        superseded);
    assertThat(listed.get(0)).isTrue();
    assertThat(listed.get(1)).as("allowed, but superseded").isFalse();
    assertThat(listed.get(2)).as("live, but not allowed").isFalse();
  }

  /**
   * At the 32nd call the walk has accepted 15 of 32 candidates: a rate of 1/2. It needs a beam of 16, so it has one candidate left
   * to find, which costs 2 more calls, and the listing is capped at 8 times that: 16, or what the plan takes if that is less. The walk is
   * given up when the {@code vectors} the predicate keeps at that rate, twice, are no more than the cap.
   */
  @Test
  void aWalkIsGivenUpWhenTheListingCostsNoMoreThanHalfOfTheRestOfTheWalk() {
    assertThat(listingCapOfTheWalkAfter32Calls(16, 1_000)).as("2 * (1/2) * 16 = 16, the cap: given up").isEqualTo(16);
    assertThat(listingCapOfTheWalkAfter32Calls(17, 1_000)).as("2 * (1/2) * 17 = 17, past the cap: walked on").isEqualTo(-1);
    assertThat(listingCapOfTheWalkAfter32Calls(100, 1_000)).isEqualTo(-1);
    assertThat(listingCapOfTheWalkAfter32Calls(1, 1_000)).isEqualTo(16);
    assertThat(listingCapOfTheWalkAfter32Calls(10, 10)).as("the plan takes 10 at most: the listing is capped at what it takes").isEqualTo(10);
    assertThat(listingCapOfTheWalkAfter32Calls(11, 10)).isEqualTo(-1);
  }

  /** The cap of the listing the walk asks for, or -1 when 32 calls (15 accepted) do not give the walk up. */
  private static int listingCapOfTheWalkAfter32Calls(final int vectors, final int maxCandidates) {
    final VectorLocationIndex locations = new VectorLocationIndex();
    final int[] map = new int[32];
    final Set<RID> allowed = new HashSet<>();
    for (int id = 0; id < 32; id++) {
      final RID rid = new RID(BUCKET, id);
      locations.addOrUpdate(id, false, id, rid, false);
      map[id] = id;
      if (id < 15)
        allowed.add(rid);
    }
    final LiveVectorBitsFilter filter = new LiveVectorBitsFilter(allowed, map, locations, null, null, 16, vectors, maxCandidates);
    for (int ordinal = 0; ordinal < 31; ordinal++)
      filter.get(ordinal);
    try {
      filter.get(31);
      return -1;
    } catch (final LiveVectorBitsFilter.SelectiveWalkException e) {
      return e.listingCap;
    }
  }
}
