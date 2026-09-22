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
package com.arcadedb.engine.timeseries;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The budget arithmetic of {@link TimeSeriesDecodedColumnCache}, driven directly rather than through a shard
 * (code review on PR #8194).
 * <p>
 * {@code Issue8179DecodedBlockCacheTest} covers what the cache does for a QUERY, and its budget assertions held
 * because nothing it decodes comes close to the budget - so the two branches that only fire under budget pressure,
 * refusing an oversized column and evicting least-recently-used ones, were never reached by it. Reaching them through
 * the engine would mean sealing blocks near {@code SEALED_BLOCK_SIZE} against a budget sized just under one of their
 * columns, which is a slow and indirect way to assert arithmetic. Synthetic arrays sized around the budget say the
 * same thing exactly.
 * <p>
 * Sizes here are derived from the charge the cache applies - 16 bytes of array header plus 8 per primitive element -
 * rather than hardcoded, so a change to the estimate moves the tests with it instead of breaking them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesDecodedColumnCacheTest {

  private static final int SHAPE = TimeSeriesDecodedColumnCache.SHAPE_RAW;

  /** What the cache charges for a {@code long[]} of this length. */
  private static long charge(final int length) {
    return 16L + 8L * length;
  }

  @Test
  void aColumnLargerThanTheWholeBudgetIsNotAdmitted() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100));

    cache.put(1L, 0, SHAPE, new long[200]);

    assertThat(cache.get(1L, 0, SHAPE))
        .as("admitting it would evict everything to make room for an entry the next admission evicts in turn")
        .isNull();
    assertThat(cache.getHeldBytes()).isZero();
  }

  @Test
  void aColumnThatExactlyFitsTheBudgetIsAdmitted() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100));

    cache.put(1L, 0, SHAPE, new long[100]);

    assertThat(cache.get(1L, 0, SHAPE)).isNotNull();
    assertThat(cache.getHeldBytes()).isEqualTo(charge(100));
  }

  @Test
  void admittingPastTheBudgetEvictsUntilItFitsAgain() {
    // Room for two columns of 100 elements, not three.
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100) * 2 + 8);

    cache.put(1L, 0, SHAPE, new long[100]);
    cache.put(2L, 0, SHAPE, new long[100]);
    assertThat(cache.getHeldBytes()).isEqualTo(charge(100) * 2);

    cache.put(3L, 0, SHAPE, new long[100]);

    assertThat(cache.getHeldBytes())
        .as("the budget is met again after the admission, not merely approached")
        .isLessThanOrEqualTo(charge(100) * 2 + 8);
    assertThat(cache.get(1L, 0, SHAPE)).as("the least recently used column is the one that went").isNull();
    assertThat(cache.get(2L, 0, SHAPE)).isNotNull();
    assertThat(cache.get(3L, 0, SHAPE)).isNotNull();
  }

  /**
   * Eviction order follows USE and not admission, which is the whole difference between an LRU and a queue: the block
   * a dashboard keeps polling has to survive the blocks admitted around it.
   */
  @Test
  void readingAnEntryKeepsItAheadOfAnOlderNeighbour() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100) * 2 + 8);

    cache.put(1L, 0, SHAPE, new long[100]);
    cache.put(2L, 0, SHAPE, new long[100]);

    // Touch the FIRST one, so the second becomes the least recently used.
    assertThat(cache.get(1L, 0, SHAPE)).isNotNull();

    cache.put(3L, 0, SHAPE, new long[100]);

    assertThat(cache.get(1L, 0, SHAPE)).as("read since it was admitted, so it outlives its neighbour").isNotNull();
    assertThat(cache.get(2L, 0, SHAPE)).isNull();
  }

  @Test
  void aDisabledCacheHoldsNothingAndReportsItself() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(0);

    cache.put(1L, 0, SHAPE, new long[10]);

    assertThat(cache.isEnabled()).isFalse();
    assertThat(cache.get(1L, 0, SHAPE)).isNull();
    assertThat(cache.getHeldBytes()).isZero();
  }

  /**
   * A block written before issue #8043 carries no id, and {@code NO_BLOCK_ID} is not an identity: caching under it
   * would serve one such block's column for another's.
   */
  @Test
  void aBlockWithoutAnIdIsNeverCached() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(1000));

    cache.put(TimeSeriesSealedStore.BlockEntry.NO_BLOCK_ID, 0, SHAPE, new long[10]);

    assertThat(cache.get(TimeSeriesSealedStore.BlockEntry.NO_BLOCK_ID, 0, SHAPE)).isNull();
    assertThat(cache.getHeldBytes()).isZero();
  }

  /**
   * Re-admitting the same key replaces the entry rather than double-charging for it, which a budget kept as a running
   * total gets wrong if the previous charge is not backed out.
   */
  @Test
  void readmittingTheSameColumnDoesNotDoubleCharge() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(1000));

    cache.put(1L, 0, SHAPE, new long[100]);
    cache.put(1L, 0, SHAPE, new long[100]);

    assertThat(cache.getHeldBytes()).isEqualTo(charge(100));
  }

  /**
   * The shape is part of the key, so the same column read raw and boxed is two entries and the budget counts both.
   */
  @Test
  void theSameColumnInTwoShapesIsTwoEntries() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(1000));

    cache.put(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_RAW, new long[100]);
    cache.put(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_DOUBLE, new double[100]);

    assertThat(cache.get(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_RAW)).isNotNull();
    assertThat(cache.get(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_DOUBLE)).isNotNull();
    assertThat(cache.getHeldBytes()).isEqualTo(charge(100) * 2);
  }

  /**
   * A dictionary column is charged for the DISTINCT strings it keeps alive on top of its slots, so the same row count
   * costs more when it retains more of them - the accounting CodeRabbit caught on PR #8194.
   */
  @Test
  void distinctStringsAreChargedOnceEach() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(1024L * 1024L);

    final String shared = "a".repeat(100);
    final String[] repeated = new String[500];
    Arrays.fill(repeated, shared);

    final String[] distinct = new String[500];
    for (int i = 0; i < distinct.length; i++)
      distinct[i] = i + "a".repeat(100);

    cache.put(1L, 0, SHAPE, repeated);
    final long repeatedBytes = cache.getHeldBytes();

    cache.put(2L, 0, SHAPE, distinct);
    final long distinctBytes = cache.getHeldBytes() - repeatedBytes;

    assertThat(distinctBytes)
        .as("500 distinct 100-char strings retain far more than 500 references to one")
        .isGreaterThan(repeatedBytes * 2);
  }

  /**
   * Boxed columns whose elements are shared instances - a STRING column handed back unchanged, a BOOLEAN autoboxed to
   * the JVM's cached singletons - are charged as references, while a column that really did allocate per row is not.
   */
  @Test
  void onlyBoxedColumnsThatAllocatedPerRowAreChargedPerRow() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(1024L * 1024L);

    final Object[] booleans = new Object[500];
    for (int i = 0; i < booleans.length; i++)
      booleans[i] = i % 2 == 0;

    final Object[] doubles = new Object[500];
    for (int i = 0; i < doubles.length; i++)
      doubles[i] = (double) i;

    cache.put(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_BOXED, booleans);
    final long booleanBytes = cache.getHeldBytes();

    cache.put(2L, 0, TimeSeriesDecodedColumnCache.SHAPE_BOXED, doubles);
    final long doubleBytes = cache.getHeldBytes() - booleanBytes;

    assertThat(booleanBytes).as("Boolean.TRUE/FALSE are singletons, so only the references are held").isEqualTo(
        16L + 8L * booleans.length);
    assertThat(doubleBytes).as("a boxed Double per row really was allocated per row").isEqualTo(
        16L + 24L * doubles.length);
  }
}
