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
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which ordinals of a graph walk may enter its result: an ordinal inside the snapshot of the ordinal map, whose vector id is a live
 * vector (not a hole the translated map left as -1, not a tombstone), whose record is in the allow-list when there is a non-empty one
 * and is not one the calling transaction superseded. A model of those four rules decides every ordinal, including the ones outside
 * the map; a walk over a selective listed predicate is given up for the listing, a wide one is not.
 */
class LiveVectorBitsFilterOracleTest {
  private static final int VECTORS = 60;

  private static RID ridOf(final int vectorId) {
    return new RID(3, 1000L + vectorId);
  }

  @Test
  void everyOrdinalIsAcceptedExactlyWhenItsVectorIsLiveAllowedAndNotSuperseded() {
    final Random rnd = new Random(10921);
    final VectorLocationIndex locations = new VectorLocationIndex();
    for (int id = 0; id < VECTORS; id++)
      locations.addOrUpdate(id, false, 4096L + id * 40L, ridOf(id), false);
    final Set<Integer> deleted = new HashSet<>();
    while (deleted.size() < 10) {
      final int id = rnd.nextInt(VECTORS);
      if (deleted.add(id))
        locations.markDeleted(id);
    }
    // the map a renumbering compaction translated: ascending ids, -1 where the vector is gone
    final int[] map = new int[VECTORS + 6];
    for (int ordinal = 0, next = 0; ordinal < map.length; ordinal++)
      map[ordinal] = ordinal % 11 == 5 ? -1 : next++ % VECTORS;

    int accepted = 0;
    int rejected = 0;
    for (int round = 0; round < 400; round++) {
      final int allowKind = rnd.nextInt(4);
      Set<RID> allowed = null;
      if (allowKind == 1)
        allowed = new HashSet<>();
      else if (allowKind >= 2) {
        allowed = new HashSet<>();
        for (int i = 0; i < 1 + rnd.nextInt(VECTORS); i++)
          allowed.add(ridOf(rnd.nextInt(VECTORS + 10)));
      }
      final int supersedeKind = rnd.nextInt(4);
      Set<RID> superseded = null;
      if (supersedeKind == 1)
        superseded = new HashSet<>();
      else if (supersedeKind >= 2) {
        superseded = new HashSet<>();
        for (int i = 0; i < 1 + rnd.nextInt(15); i++)
          superseded.add(ridOf(rnd.nextInt(VECTORS)));
      }
      final LiveVectorBitsFilter filter = new LiveVectorBitsFilter(allowed, map, locations, superseded);
      for (int ordinal = -3; ordinal < map.length + 3; ordinal++) {
        boolean expected = ordinal >= 0 && ordinal < map.length && map[ordinal] >= 0 && !deleted.contains(map[ordinal]);
        if (expected && allowed != null && !allowed.isEmpty())
          expected = allowed.contains(ridOf(map[ordinal]));
        if (expected && superseded != null && !superseded.isEmpty())
          expected = !superseded.contains(ridOf(map[ordinal]));
        assertThat(filter.get(ordinal)).as("round %d ordinal %d (allow-list %s, superseded %s)", round, ordinal,
            allowed == null ? "none" : allowed.size(), superseded == null ? "none" : superseded.size()).isEqualTo(expected);
        if (expected)
          accepted++;
        else
          rejected++;
      }
    }
    assertThat(accepted).isGreaterThan(1000);
    assertThat(rejected).isGreaterThan(1000);
  }

  @Test
  void aWalkOverASelectivePredicateIsGivenUpForTheListingAndAWideOneIsNot() {
    final int vectors = 10_000;
    final VectorLocationIndex locations = new VectorLocationIndex(vectors);
    final int[] map = new int[vectors];
    for (int id = 0; id < vectors; id++) {
      locations.addOrUpdate(id, false, 4096L + id * 40L, ridOf(id), false);
      map[id] = id;
    }
    final Set<RID> onePercent = new HashSet<>();
    final Set<RID> half = new HashSet<>();
    for (int id = 0; id < vectors; id++) {
      if (id % 100 == 7)
        onePercent.add(ridOf(id));
      if (id % 2 == 0)
        half.add(ridOf(id));
    }

    final LiveVectorBitsFilter selective = new LiveVectorBitsFilter(onePercent, map, locations, null, null, 50, vectors, 1000);
    assertThat(selective.givesUpForTheListing()).isTrue();
    int calls = 0;
    int listingCap = 0;
    try {
      for (; calls < vectors; calls++)
        selective.get(calls);
    } catch (final LiveVectorBitsFilter.SelectiveWalkException e) {
      listingCap = e.listingCap;
    }
    assertThat(calls).as("a filter that keeps one ordinal in a hundred is given up long before the end of the walk").isLessThan(200);
    assertThat(listingCap).as("the listing it asks for holds the whole predicate and no more than the plan takes").isBetween(100, 1000);

    final LiveVectorBitsFilter wide = new LiveVectorBitsFilter(half, map, locations, null, null, 50, vectors, 1000);
    for (int ordinal = 0; ordinal < vectors; ordinal++)
      wide.get(ordinal);

    final LiveVectorBitsFilter disarmed = new LiveVectorBitsFilter(onePercent, map, locations, null, null, 50, vectors, 1000);
    disarmed.disarm();
    assertThat(disarmed.givesUpForTheListing()).isFalse();
    for (int ordinal = 0; ordinal < vectors; ordinal++)
      disarmed.get(ordinal);

    // a walk with no beam to reach is never given up
    final LiveVectorBitsFilter noBeam = new LiveVectorBitsFilter(onePercent, map, locations, null, null, 0, vectors, 1000);
    assertThat(noBeam.givesUpForTheListing()).isFalse();
  }
}
