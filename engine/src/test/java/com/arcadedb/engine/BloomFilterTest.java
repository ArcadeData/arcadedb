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
package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a bloom filter is FOR: never missing a key it holds, and rejecting almost everything it does not. The first
 * is a hard guarantee, the second a rate - and a filter whose rate is never measured is indistinguishable from one
 * that answers "maybe" to everything, which is what a saturated filter silently becomes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BloomFilterTest {

  @Test
  void neverMissesAKeyItHolds() {
    final int slots = 1 << 16;
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 23, 4);

    for (int i = 0; i < 5_000; i++)
      bf.add(i * 31 + 7);

    for (int i = 0; i < 5_000; i++)
      assertThat(bf.mightContain(i * 31 + 7)).as("value %d", i * 31 + 7).isTrue();
  }

  /**
   * The rate the sizing helpers promise has to be the rate the filter delivers, otherwise every caller that sizes a
   * filter from them is paying for lookups it believed it had avoided.
   */
  @Test
  void staysWithinThePromisedFalsePositiveRate() {
    for (final double target : new double[] { 0.05, 0.01, 0.001 }) {
      final int entries = 20_000;
      final int slots = BufferBloomFilter.slotsFor(entries, target);
      final int probes = BufferBloomFilter.probesFor(slots, entries);
      final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 0x9747b28c, probes);

      for (int i = 0; i < entries; i++)
        bf.add(i);

      final double measured = measureFalsePositives(bf, entries);

      assertThat(measured).as("target %s with %d slots and %d probes", target, slots, probes).isLessThan(target * 2);
      assertThat(bf.expectedFalsePositiveRate(entries)).isLessThan(target * 1.1);
    }
  }

  /**
   * Two probes were the hardcoded choice before the count became configurable; at the density an index would use
   * they cost several times the false positives of the optimal count, for exactly the same memory.
   */
  @Test
  void theOptimalProbeCountBeatsTheHistoricalTwo() {
    final int entries = 20_000;
    final int slots = BufferBloomFilter.slotsFor(entries, 0.01);

    final double twoProbes = measureFalsePositives(fill(slots, 2, entries), entries);
    final double optimal = measureFalsePositives(fill(slots, BufferBloomFilter.probesFor(slots, entries), entries),
        entries);

    assertThat(optimal).as("optimal probes must beat two at %d slots", slots).isLessThan(twoProbes);
  }

  /**
   * The keys an index has to filter are bytes, not ints.
   */
  @Test
  void filtersKeysOfArbitraryBytes() {
    final int entries = 10_000;
    final int slots = BufferBloomFilter.slotsFor(entries, 0.01);
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 23,
        BufferBloomFilter.probesFor(slots, entries));

    for (int i = 0; i < entries; i++) {
      final byte[] key = key(i);
      bf.add(key, key.length);
    }

    for (int i = 0; i < entries; i++) {
      final byte[] key = key(i);
      assertThat(bf.mightContain(key, key.length)).as("key %d", i).isTrue();
    }

    int falsePositives = 0;
    for (int i = entries; i < entries * 3; i++) {
      final byte[] key = key(i);
      if (bf.mightContain(key, key.length))
        ++falsePositives;
    }
    assertThat((double) falsePositives / (entries * 2)).isLessThan(0.02);
  }

  /**
   * Random keys must behave like sequential ones: a rate that only holds for tidy input would be an artefact of the
   * hash rather than a property of the filter.
   */
  @Test
  void theRateHoldsForRandomKeysToo() {
    final int entries = 20_000;
    final int slots = BufferBloomFilter.slotsFor(entries, 0.01);
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 23,
        BufferBloomFilter.probesFor(slots, entries));

    final Random random = new Random(42);
    final int[] added = new int[entries];
    for (int i = 0; i < entries; i++) {
      added[i] = random.nextInt();
      bf.add(added[i]);
    }

    for (final int value : added)
      assertThat(bf.mightContain(value)).isTrue();

    int falsePositives = 0;
    final int probed = 200_000;
    for (int i = 0; i < probed; i++)
      if (bf.mightContain(random.nextInt()))
        ++falsePositives;

    assertThat((double) falsePositives / probed).isLessThan(0.02);
  }

  @Test
  void sizingHelpersRejectImpossibleRequests() {
    assertThat(BufferBloomFilter.slotsFor(1_000, 0.01) % 8).isZero();
    assertThat(BufferBloomFilter.slotsFor(1_000, 0.001)).isGreaterThan(BufferBloomFilter.slotsFor(1_000, 0.01));
    assertThat(BufferBloomFilter.probesFor(BufferBloomFilter.slotsFor(1_000, 0.01), 1_000)).isGreaterThan(2);
    assertThat(BufferBloomFilter.probesFor(8, 1_000_000)).isEqualTo(1);

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> BufferBloomFilter.slotsFor(0, 0.01))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> BufferBloomFilter.slotsFor(1_000, 0))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> BufferBloomFilter.slotsFor(1_000, 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static byte[] key(final int i) {
    return ("__dk/address/" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static BufferBloomFilter fill(final int slots, final int probes, final int entries) {
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 0x9747b28c, probes);
    for (int i = 0; i < entries; i++)
      bf.add(i);
    return bf;
  }

  private static double measureFalsePositives(final BufferBloomFilter bf, final int entries) {
    int falsePositives = 0;
    final int probed = 200_000;
    for (int i = entries; i < entries + probed; i++)
      if (bf.mightContain(i))
        ++falsePositives;

    return (double) falsePositives / probed;
  }
}
