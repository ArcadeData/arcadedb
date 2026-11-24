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
package com.arcadedb.postgres;

import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The key a catalog row is told apart from the others by: equal exactly when SQL calls two rows the same, with an order that agrees
 * with that equality.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresCatalogRowKeyTest {

  @Test
  void numbersAreTheSameKeyWhateverTypeCarriesThem() {
    final PostgresCatalogRowKey one = PostgresCatalogRowKey.of(List.of(1L));

    assertThat(PostgresCatalogRowKey.of(List.of(1))).isEqualTo(one).hasSameHashCodeAs(one);
    assertThat(PostgresCatalogRowKey.of(List.of((short) 1))).isEqualTo(one).hasSameHashCodeAs(one);
    assertThat(PostgresCatalogRowKey.of(List.of(1.0d))).isEqualTo(one).hasSameHashCodeAs(one);
    assertThat(PostgresCatalogRowKey.of(List.of(1.0f))).isEqualTo(one).hasSameHashCodeAs(one);
    assertThat(PostgresCatalogRowKey.of(List.of(new BigDecimal("1.00")))).isEqualTo(one).hasSameHashCodeAs(one);
    assertThat(PostgresCatalogRowKey.ofValue(1)).isEqualTo(one).hasSameHashCodeAs(one);

    assertThat(PostgresCatalogRowKey.of(List.of(1.5d))).isEqualTo(PostgresCatalogRowKey.of(List.of(1.5f)));
    assertThat(PostgresCatalogRowKey.of(List.of(new BigDecimal("2.50")))).isEqualTo(PostgresCatalogRowKey.of(List.of(new BigDecimal("2.5"))));
    final BigInteger beyondLong = BigInteger.TWO.pow(80);
    assertThat(PostgresCatalogRowKey.of(List.of(beyondLong))).isEqualTo(PostgresCatalogRowKey.of(List.of(new BigDecimal(beyondLong))));
    assertThat(PostgresCatalogRowKey.of(List.of(beyondLong))).isNotEqualTo(PostgresCatalogRowKey.of(List.of(beyondLong.add(BigInteger.ONE))));
  }

  @Test
  void valuesOfDifferentKindsAreDifferentKeys() {
    final List<Object> values = Arrays.asList(null, true, 1L, 0.5d, "1", "", new BigDecimal("0.25"), List.of(1L), new UUID(1, 2));

    for (int i = 0; i < values.size(); i++)
      for (int j = 0; j < values.size(); j++)
        if (i != j)
          assertThat(PostgresCatalogRowKey.ofValue(values.get(i))).as("%s and %s", values.get(i), values.get(j))
              .isNotEqualTo(PostgresCatalogRowKey.ofValue(values.get(j)));
  }

  @Test
  void aRowIsTheSameKeyOnlyWithTheSameValuesInTheSameOrder() {
    assertThat(PostgresCatalogRowKey.of(Arrays.asList(1L, "a", null))).isEqualTo(PostgresCatalogRowKey.of(Arrays.asList(1, "a", null)));
    assertThat(PostgresCatalogRowKey.of(Arrays.asList(1L, "a"))).isNotEqualTo(PostgresCatalogRowKey.of(Arrays.asList("a", 1L)));
    assertThat(PostgresCatalogRowKey.of(Arrays.asList(1L, "a"))).isNotEqualTo(PostgresCatalogRowKey.of(Arrays.asList(1L, "a", null)));
    assertThat(PostgresCatalogRowKey.of(List.of())).isEqualTo(PostgresCatalogRowKey.of(List.of())).isNotEqualTo(PostgresCatalogRowKey.ofValue(null));
  }

  @Test
  void anArrayIsTheSameKeyWhenItsElementsAre() {
    assertThat(PostgresCatalogRowKey.ofValue(List.of(1, 2))).isEqualTo(PostgresCatalogRowKey.ofValue(List.of(1L, 2L)));
    assertThat(PostgresCatalogRowKey.ofValue(new PostgresCatalogIndexes.Vector(1, 2))).isEqualTo(PostgresCatalogRowKey.ofValue(List.of(1L, 2L)));
    assertThat(PostgresCatalogRowKey.ofValue(List.of(1, 2))).isNotEqualTo(PostgresCatalogRowKey.ofValue(List.of(2, 1)));
    assertThat(PostgresCatalogRowKey.ofValue(List.of(List.of(1), List.of("a"))))
        .isEqualTo(PostgresCatalogRowKey.ofValue(List.of(List.of(1L), List.of("a"))));
    assertThat(PostgresCatalogRowKey.ofValue(List.of(1))).isNotEqualTo(PostgresCatalogRowKey.ofValue(List.of(1, 1)));
  }

  @Test
  void theOrderAgreesWithEqualityAndIsTotal() {
    final List<Object> pool = Arrays.asList(null, true, false, 0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 0.5d, -0.5d, Double.NaN,
        Double.POSITIVE_INFINITY, new BigDecimal("1.5"), new BigDecimal("1e30"), "", "a", "b", "Aa", "BB", List.of(), List.of(1),
        List.of(1, "a"), List.of(List.of(2)), new UUID(0, 1), new UUID(0, 2), 'x');

    final List<PostgresCatalogRowKey> keys = new ArrayList<>();
    for (final Object value : pool)
      keys.add(PostgresCatalogRowKey.ofValue(value));
    for (final Object first : pool)
      for (final Object second : pool)
        keys.add(PostgresCatalogRowKey.of(Arrays.asList(first, second)));

    final List<String> violations = new ArrayList<>();

    // pairs: equals is the same thing as comparing as 0, equal keys have the same hash, and the order is antisymmetric
    for (final PostgresCatalogRowKey a : keys)
      for (final PostgresCatalogRowKey b : keys) {
        if (a.equals(b) != (a.compareTo(b) == 0))
          violations.add("equals and compareTo disagree on " + a + " and " + b);
        if (a.equals(b) && a.hashCode() != b.hashCode())
          violations.add("equal keys with another hash " + a + " and " + b);
        if (Integer.signum(a.compareTo(b)) != -Integer.signum(b.compareTo(a)))
          violations.add("the order is not antisymmetric on " + a + " and " + b);
      }

    // triples over the single-value keys: the order is transitive
    final List<PostgresCatalogRowKey> singles = keys.subList(0, pool.size());
    for (final PostgresCatalogRowKey a : singles)
      for (final PostgresCatalogRowKey b : singles)
        for (final PostgresCatalogRowKey c : singles)
          if (a.compareTo(b) <= 0 && b.compareTo(c) <= 0 && a.compareTo(c) > 0)
            violations.add("the order is not transitive on " + a + ", " + b + " and " + c);

    assertThat(violations).isEmpty();
  }

  @Test
  void aKeyLooksUpTheSameInAHashSetAsItsEqualKey() {
    final Set<PostgresCatalogRowKey> keys = new HashSet<>();
    keys.add(PostgresCatalogRowKey.of(Arrays.asList(1, "a", 2.0d)));

    assertThat(keys).contains(PostgresCatalogRowKey.of(Arrays.asList(1L, "a", 2L)));
    assertThat(keys).doesNotContain(PostgresCatalogRowKey.of(Arrays.asList(1L, "a", 3L)));
  }

  @Test
  void aLargeSetOfTextRowsIsCombinedWithinTheBound() {
    final List<List<Object>> texts = textRows(16);
    final List<List<Object>> sameTexts = new ArrayList<>(texts);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final List<List<Object>> union = PostgresCatalogRowSets.combine(List.of("UNION"), List.of(false), List.of(texts, sameTexts));
    final List<List<Object>> intersection = PostgresCatalogRowSets.combine(List.of("INTERSECT"), List.of(false), List.of(texts, sameTexts));
    final List<List<Object>> difference = PostgresCatalogRowSets.combine(List.of("EXCEPT"), List.of(false), List.of(texts, sameTexts));
    stopwatch.assertGaveUpWithin(10_000, "a pass that does a bounded amount of work per row from one that compares each row with all the others");

    assertThat(union).hasSize(texts.size());
    assertThat(intersection).hasSize(texts.size());
    assertThat(difference).isEmpty();
  }

  @Test
  void aLargeSetOfKeysIsHeldInAMapWithinTheBound() {
    final List<List<Object>> texts = textRows(16);
    final List<List<Object>> numbers = new ArrayList<>();
    for (long i = 0; i < 1 << 16; i++)
      numbers.add(List.of(i * 4_294_967_297L));

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final Map<PostgresCatalogRowKey, Integer> counts = new HashMap<>();
    for (final List<List<Object>> rows : List.of(texts, numbers, texts, numbers))
      for (final List<Object> row : rows)
        counts.merge(PostgresCatalogRowKey.of(row), 1, Integer::sum);
    stopwatch.assertGaveUpWithin(10_000, "a pass that does a bounded amount of work per row from one that compares each row with all the others");

    assertThat(counts).hasSize(texts.size() + numbers.size());
    assertThat(counts.values()).allMatch(count -> count == 2);
  }

  /** One row of one text for each way to write {@code blocks} blocks of two characters, a block being either "Aa" or "BB". */
  private static List<List<Object>> textRows(final int blocks) {
    final List<List<Object>> rows = new ArrayList<>(1 << blocks);
    for (int bits = 0; bits < 1 << blocks; bits++) {
      final StringBuilder text = new StringBuilder(blocks * 2);
      for (int block = 0; block < blocks; block++)
        text.append((bits >> block & 1) == 0 ? "Aa" : "BB");
      rows.add(List.of(text.toString()));
    }
    return rows;
  }
}
