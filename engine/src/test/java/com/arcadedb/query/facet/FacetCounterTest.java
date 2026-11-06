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
package com.arcadedb.query.facet;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class FacetCounterTest {
  @Test
  void denseWhenMatchesNearCardinality() {
    assertThat(FacetCounter.choose(125, 1000)).isInstanceOf(DenseFacetCounter.class);
    assertThat(FacetCounter.choose(124, 1000)).isInstanceOf(HashFacetCounter.class);
    assertThat(FacetCounter.choose(200_000, 100_001)).isInstanceOf(HashFacetCounter.class);
    assertThat(FacetCounter.choose(Long.MAX_VALUE, 100_000)).isInstanceOf(DenseFacetCounter.class);
  }

  @Test
  void denseAndHashAgree() {
    final Random random = new Random(42);
    final DenseFacetCounter dense = new DenseFacetCounter(5000);
    final HashFacetCounter hash = new HashFacetCounter(16);
    for (int i = 0; i < 100_000; i++) {
      final int ordinal = random.nextInt(5000);
      dense.increment(ordinal);
      hash.increment(ordinal);
    }
    final long[] fromDense = new long[5000];
    final long[] fromHash = new long[5000];
    dense.forEachNonZero((o, n) -> fromDense[o] = n);
    hash.forEachNonZero((o, n) -> fromHash[o] = n);
    assertThat(fromHash).isEqualTo(fromDense);
    long total = 0;
    for (final long n : fromDense)
      total += n;
    assertThat(total).isEqualTo(100_000);
    assertThat(dense.get(7)).isEqualTo(hash.get(7));
  }

  @Test
  void forEachNonZeroSkipsZeros() {
    final FacetCounter[] counters = { new DenseFacetCounter(10), new HashFacetCounter(4) };
    for (final FacetCounter c : counters) {
      c.add(3, 5);
      c.add(7, 0);
      final int[] visited = { 0 };
      c.forEachNonZero((o, n) -> {
        visited[0]++;
        assertThat(o).isEqualTo(3);
        assertThat(n).isEqualTo(5);
      });
      assertThat(visited[0]).isEqualTo(1);
      assertThat(c.get(9)).isZero();
    }
  }

  @Test
  void addSaturatesAtIntegerMax() {
    final FacetCounter[] counters = { new DenseFacetCounter(4), new HashFacetCounter(4) };
    for (final FacetCounter c : counters) {
      c.add(1, Integer.MAX_VALUE - 1L);
      c.add(1, 5);
      assertThat(c.get(1)).isEqualTo(Integer.MAX_VALUE);
      c.add(2, Long.MAX_VALUE);
      assertThat(c.get(2)).isEqualTo(Integer.MAX_VALUE);
    }
  }

  @Test
  void estimatedBytesGrowsWithTheHashMap() {
    final HashFacetCounter hash = new HashFacetCounter(16);
    final long before = hash.estimatedBytes();
    for (int i = 0; i < 10_000; i++)
      hash.increment(i);
    assertThat(hash.estimatedBytes()).isGreaterThan(before);
    assertThat(hash.estimatedBytes()).isGreaterThanOrEqualTo(10_000L * 8);
    assertThat(new DenseFacetCounter(1000).estimatedBytes()).isGreaterThanOrEqualTo(4000);
  }

  @Test
  void longTermsCounterKeepsRawBitsIncludingTheMapSentinel() {
    final LongTermsCounter counter = new LongTermsCounter(4);
    counter.increment(5L);
    counter.add(5L, 2);
    counter.increment(Long.MIN_VALUE);
    counter.add(Long.MIN_VALUE, 4);
    counter.increment(Double.doubleToRawLongBits(-0.0d));
    assertThat(counter.get(5L)).isEqualTo(3);
    assertThat(counter.get(Long.MIN_VALUE)).isEqualTo(6);
    assertThat(counter.get(99L)).isZero();
    final long[] sum = { 0 };
    final int[] entries = { 0 };
    counter.forEachNonZero((k, n) -> {
      sum[0] += n;
      entries[0]++;
    });
    assertThat(entries[0]).isEqualTo(2);
    assertThat(sum[0]).isEqualTo(9);
    assertThat(counter.estimatedBytes()).isPositive();
  }

  @Test
  void hashCounterNeverGrowsWithinTheExpectedBound() {
    for (final int expected : new int[] { 1, 12, 13, 16, 100, 1000, 3072, 5000 }) {
      final HashFacetCounter hash = new HashFacetCounter(expected);
      final long before = hash.estimatedBytes();
      assertThat(before).isEqualTo(HashFacetCounter.bytesFor(expected));
      for (int i = 0; i < expected; i++)
        hash.increment(i);
      assertThat(hash.estimatedBytes()).as("expected=%d", expected).isEqualTo(before);
    }
    final LongTermsCounter terms = new LongTermsCounter(3000);
    final long before = terms.estimatedBytes();
    assertThat(before).isEqualTo(LongTermsCounter.bytesFor(3000));
    for (int i = 0; i < 3000; i++)
      terms.increment(i);
    assertThat(terms.estimatedBytes()).isEqualTo(before);
    assertThat(DenseFacetCounter.bytesFor(1000)).isEqualTo(new DenseFacetCounter(1000).estimatedBytes());
  }

  @Test
  void addNearLongMaxOnANonZeroCountSaturates() {
    final FacetCounter[] counters = { new DenseFacetCounter(4), new HashFacetCounter(4) };
    for (final FacetCounter c : counters) {
      c.add(1, 10);
      c.add(1, Long.MAX_VALUE - 5);
      assertThat(c.get(1)).isEqualTo(Integer.MAX_VALUE);
    }
  }
}
