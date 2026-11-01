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

import com.arcadedb.utility.VectorApiSupport;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11071: the search caches were sized to a power of two, and when the round-up of the id range did not fit the heap
 * budget it was rounded DOWN, so a 100,000-vector index of 1536 dimensions under a budget of about 120,000 entries got 65,536
 * slots and the ids past them evicted the ids they shared a slot with on every search. The caches now take any capacity
 * and the rule cuts a round-up that does not fit to the budget itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11071SearchCacheCapacityTest {
  private static final VectorTypeSupport VTS = VectorApiSupport.jvectorProvider().getVectorTypeSupport();

  @Test
  void aBudgetThatCoversTheIdRangeButNotItsRoundUpGivesEveryIdASlot() {
    // The reporter's numbers: 100,000 ids, 6,152 bytes an entry, 25% of about 3 GB available
    final long affordable = 750_000_000L / FlatVectorCache.bytesPerVector(1536);
    assertThat(affordable).isBetween(100_000L, 131_071L);

    final int capacity = LSMVectorIndex.searchCacheCapacity(100_000, affordable, 0);
    assertThat(capacity).isGreaterThanOrEqualTo(100_000);
    assertThat((long) capacity).isLessThanOrEqualTo(affordable);
  }

  @Test
  void aBudgetThatCoversTheRoundUpTakesItForGrowth() {
    assertThat(LSMVectorIndex.searchCacheCapacity(100_000, 1_000_000, 0)).isEqualTo(131_072);
    assertThat(LSMVectorIndex.searchCacheCapacity(131_072, 1_000_000, 0)).isEqualTo(131_072);
    // The cache that covered 65,536 ids doubles when the range passes it
    assertThat(LSMVectorIndex.searchCacheCapacity(70_000, 1_000_000, 65_536)).isEqualTo(131_072);
  }

  @Test
  void aBudgetSmallerThanTheIdRangeIsUsedWholeNotHalved() {
    // Was 65,536: the power of two at or below the budget
    assertThat(LSMVectorIndex.searchCacheCapacity(200_000, 122_000, 0)).isEqualTo(122_000);
  }

  @Test
  void theCapacityNeverShrinksAndDoesNotGrowByAFewSlotsAtATime() {
    // A heap reading that moved by a few hundred entries does not start a cut cache over
    assertThat(LSMVectorIndex.searchCacheCapacity(200_000, 122_500, 122_000)).isEqualTo(122_000);
    // ... unless the growth makes the cache cover every id
    assertThat(LSMVectorIndex.searchCacheCapacity(122_300, 122_500, 122_000)).isEqualTo(122_500);
    // ... or is worth it: an eighth or more
    assertThat(LSMVectorIndex.searchCacheCapacity(300_000, 140_000, 122_000)).isEqualTo(140_000);
    // A budget that shrank keeps the cache as it is
    assertThat(LSMVectorIndex.searchCacheCapacity(300_000, 50_000, 122_000)).isEqualTo(122_000);
    // A cache that covers the range is answered as it is
    assertThat(LSMVectorIndex.searchCacheCapacity(100_000, 50_000, 122_000)).isEqualTo(122_000);
  }

  @Test
  void aFlatCacheOfAnyCapacityKeepsEveryIdBelowItInASlotOfItsOwn() {
    final int capacity = 100;
    // 4 dimensions in chunks of 64 floats: 16 slots a chunk, the last chunk cut to the 4 slots the capacity leaves it
    final FlatVectorCache cache = new FlatVectorCache(4, capacity, 64);
    assertThat(cache.capacity()).isEqualTo(capacity);

    for (int id = 0; id < capacity; id++)
      cache.put(id, vector(id, 1f, 2f, 3f));
    assertThat(cache.size()).isEqualTo(capacity);
    for (int id = 0; id < capacity; id++)
      assertThat(cache.get(id).get(0)).isEqualTo(id);
    assertThat(cache.allocatedBytes()).isEqualTo((long) capacity * 4 * Float.BYTES);

    final VectorFloat<?> query = vector(0f, 0f, 0f, 0f);
    assertThat(cache.similarity(99, query, VectorSimilarityFunction.EUCLIDEAN))
        .isEqualTo(VectorSimilarityFunction.EUCLIDEAN.compare(query, vector(99f, 1f, 2f, 3f)));

    // Past the capacity an id takes the slot of the remainder: 105 that of 5
    cache.put(105, vector(105f, 0f, 0f, 0f));
    assertThat(cache.get(5)).isNull();
    assertThat(cache.get(105).get(0)).isEqualTo(105f);
    assertThat(cache.get(6).get(0)).isEqualTo(6f);
    assertThat(cache.size()).isEqualTo(capacity);
    cache.remove(105);
    assertThat(cache.size()).isEqualTo(capacity - 1);
  }

  @Test
  void aBinaryCodeCacheOfAnyCapacityKeepsEveryIdBelowItInASlotOfItsOwn() {
    final int capacity = 100;
    final BinaryCodeCache cache = new BinaryCodeCache(64, capacity);
    assertThat(cache.capacity()).isEqualTo(capacity);

    final long[] code = new long[1];
    for (int id = 0; id < capacity; id++) {
      code[0] = id;
      cache.put(id, code, 0);
    }
    assertThat(cache.size()).isEqualTo(capacity);
    final long[] zero = new long[1];
    for (int id = 0; id < capacity; id++)
      assertThat(cache.hammingDistance(id, zero)).isEqualTo(Long.bitCount(id));

    code[0] = -1L;
    cache.put(105, code, 0);
    assertThat(cache.hammingDistance(5, zero)).isEqualTo(-1);
    assertThat(cache.hammingDistance(105, zero)).isEqualTo(64);
    assertThat(cache.size()).isEqualTo(capacity);
  }

  private static VectorFloat<?> vector(final float... components) {
    return VTS.createFloatVector(components);
  }
}
