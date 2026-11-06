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

import com.arcadedb.index.columnar.GlobalDictionary;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class FacetRankerTest {
  // ordinal order is NOT value order, so a ranker that ignores valueOrder cannot pass by ordinal tie-breaks
  private static final String[] NAMES = { "c", "a", "d", "b", "e" };

  private static DenseFacetCounter counts(final int... perOrdinal) {
    final DenseFacetCounter c = new DenseFacetCounter(perOrdinal.length);
    for (int i = 0; i < perOrdinal.length; i++)
      c.add(i, perOrdinal[i]);
    return c;
  }

  private static String[] names(final int[] ordinals) {
    final String[] result = new String[ordinals.length];
    for (int i = 0; i < ordinals.length; i++)
      result[i] = NAMES[ordinals[i]];
    return result;
  }

  private static int byName(final int a, final int b) {
    return NAMES[a].compareTo(NAMES[b]);
  }

  private static Map<Object, long[]> map(final Object... keyThenCount) {
    final Map<Object, long[]> m = new LinkedHashMap<>();
    for (int i = 0; i < keyThenCount.length; i += 2)
      m.put(keyThenCount[i], new long[] { ((Number) keyThenCount[i + 1]).longValue() });
    return m;
  }

  private static List<Object> keys(final List<Map.Entry<Object, Long>> entries) {
    final List<Object> keys = new ArrayList<>();
    for (final Map.Entry<Object, Long> e : entries)
      keys.add(e.getKey());
    return keys;
  }

  @Test
  void tieBreakByValueAscending() {
    // c:5 a:5 d:5 b:9 -> b first, then the three 5s by name
    final int[] top = FacetRanker.topOrdinals(counts(5, 5, 5, 9), 3, 1, FacetOrder.COUNT_DESC, FacetRankerTest::byName);
    assertThat(names(top)).containsExactly("b", "a", "c");
    // the same counts with the opposite value order must flip the tie
    final int[] reversed = FacetRanker.topOrdinals(counts(5, 5, 5, 9), 3, 1, FacetOrder.COUNT_DESC, (x, y) -> byName(y, x));
    assertThat(names(reversed)).containsExactly("b", "d", "c");
  }

  @Test
  void orderByCountAsc() {
    // c:5 a:2 d:5 b:9 e:2
    final int[] top = FacetRanker.topOrdinals(counts(5, 2, 5, 9, 2), 4, 1, FacetOrder.COUNT_ASC, FacetRankerTest::byName);
    assertThat(names(top)).containsExactly("a", "e", "c", "d");
  }

  @Test
  void orderByValueIgnoresCounts() {
    final int[] asc = FacetRanker.topOrdinals(counts(1, 9, 5, 3, 7), 3, 1, FacetOrder.VALUE_ASC, FacetRankerTest::byName);
    assertThat(names(asc)).containsExactly("a", "b", "c");
    final int[] desc = FacetRanker.topOrdinals(counts(1, 9, 5, 3, 7), 3, 1, FacetOrder.VALUE_DESC, FacetRankerTest::byName);
    assertThat(names(desc)).containsExactly("e", "d", "c");
  }

  @Test
  void minCountDropsBuckets() {
    final int[] top = FacetRanker.topOrdinals(counts(1, 2, 3, 4, 5), 10, 3, FacetOrder.COUNT_DESC, FacetRankerTest::byName);
    assertThat(names(top)).containsExactly("e", "b", "d");
    assertThat(FacetRanker.topOrdinals(counts(1, 2), 10, 3, FacetOrder.COUNT_DESC, FacetRankerTest::byName)).isEmpty();
    assertThat(FacetRanker.topOrdinals(counts(1, 2), 0, 1, FacetOrder.COUNT_DESC, FacetRankerTest::byName)).isEmpty();
  }

  @Test
  void topIsBoundedAndKeepsTheBest() {
    final DenseFacetCounter c = new DenseFacetCounter(2000);
    for (int i = 0; i < 2000; i++)
      c.add(i, (i * 7919L) % 1000 + 1);
    final int[] top = FacetRanker.topOrdinals(c, 5, 1, FacetOrder.COUNT_DESC, Integer::compare);
    assertThat(top).hasSize(5);
    for (int i = 1; i < top.length; i++)
      assertThat(c.get(top[i - 1])).isGreaterThanOrEqualTo(c.get(top[i]));
    final long[] all = new long[2000];
    for (int i = 0; i < 2000; i++)
      all[i] = c.get(i);
    Arrays.sort(all);
    assertThat(c.get(top[4])).isEqualTo(all[1995]);
  }

  @Test
  void topValuesRanksByCountThenValue() {
    final Map<Object, long[]> counts = map("c", 5, "a", 5, "b", 5, "d", 9, "z", 1);
    assertThat(keys(FacetRanker.topValues(counts, 3, 1, FacetOrder.COUNT_DESC))).containsExactly("d", "a", "b");
    assertThat(keys(FacetRanker.topValues(counts, 5, 1, FacetOrder.COUNT_ASC))).containsExactly("z", "a", "b", "c", "d");
    assertThat(keys(FacetRanker.topValues(counts, 5, 5, FacetOrder.COUNT_DESC))).containsExactly("d", "a", "b", "c");
    assertThat(keys(FacetRanker.topValues(counts, 2, 1, FacetOrder.VALUE_DESC))).containsExactly("z", "d");
    assertThat(FacetRanker.topValues(counts, 1, 1, FacetOrder.COUNT_DESC).get(0).getValue()).isEqualTo(9L);
  }

  @Test
  void topValuesKeepsNullLastInEveryOrder() {
    final Map<Object, long[]> counts = map("a", 1, "b", 1);
    counts.put(null, new long[] { 1 });
    assertThat(keys(FacetRanker.topValues(counts, 3, 1, FacetOrder.VALUE_ASC))).containsExactly("a", "b", null);
    assertThat(keys(FacetRanker.topValues(counts, 3, 1, FacetOrder.VALUE_DESC))).containsExactly("b", "a", null);
    assertThat(keys(FacetRanker.topValues(counts, 3, 1, FacetOrder.COUNT_DESC))).containsExactly("a", "b", null);
  }

  @Test
  void mixedTypesAreDeterministic() {
    final List<Object> values = new ArrayList<>(Arrays.asList("x", 2L, 10, null));
    final List<Object> expected = Arrays.asList("x", 2L, 10, null);
    for (int round = 0; round < 6; round++) {
      Collections.shuffle(values, new Random(round));
      values.sort(FacetValueComparator.INSTANCE);
      assertThat(values).containsExactlyElementsOf(expected);
    }
    final Map<Object, long[]> counts = new LinkedHashMap<>();
    for (final Object v : values)
      counts.put(v, new long[] { 3 });
    assertThat(keys(FacetRanker.topValues(counts, 10, 1, FacetOrder.COUNT_DESC))).containsExactly("x", 2L, 10, null);
  }

  @Test
  void unrankedTailTiesBreakByValue() {
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    for (int i = 0; i < 1100; i++)
      dictionary.getOrAdd(String.format("k%04d", i));
    dictionary.rebuildRankView();
    final int zz = dictionary.getOrAdd("zz");
    final int aa = dictionary.getOrAdd("aa");
    final int mm = dictionary.getOrAdd("mm");
    assertThat(dictionary.rankViewSize()).isEqualTo(1100);
    assertThat(dictionary.rank(zz)).isEqualTo(dictionary.rank(mm));
    final DenseFacetCounter c = new DenseFacetCounter(dictionary.size());
    c.add(zz, 4);
    c.add(aa, 4);
    c.add(mm, 4);
    final int[] top = FacetRanker.topOrdinals(c, 3, 1, FacetOrder.COUNT_DESC, dictionary::compare);
    assertThat(top).containsExactly(aa, mm, zz);
  }

  @Test
  void comparatorMatchesDictionaryOrder() {
    final FacetValueComparator cmp = FacetValueComparator.INSTANCE;
    assertThat(cmp.compare("zzz", 1L)).isNegative();
    assertThat(cmp.compare(2L, new BigDecimal("2.5"))).isNegative();
    assertThat(cmp.compare(new BigDecimal("2.5"), 2L)).isPositive();
    assertThat(cmp.compare(Double.POSITIVE_INFINITY, Double.NaN)).isNegative();
    assertThat(cmp.compare(Double.NaN, Long.MAX_VALUE)).isPositive();
    assertThat(cmp.compare(Long.MAX_VALUE, "a")).isPositive();
    assertThat(cmp.compare(false, true)).isNegative();
    assertThat(cmp.compare(true, null)).isNegative();
    assertThat(cmp.compare(null, "a")).isPositive();
    assertThat(cmp.compare(null, null)).isZero();
    assertThat(cmp.compare(10, 10L)).isZero();
    assertThat(cmp.compare(1.5f, 1.5d)).isZero();

    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final Object[] input = { "b", 5L, new BigDecimal("2.5"), Double.NaN, true, "a", -1L, false, Double.NEGATIVE_INFINITY };
    for (final Object v : input)
      dictionary.getOrAdd(v);
    dictionary.rebuildRankView();
    for (int a = 0; a < input.length; a++)
      for (int b = 0; b < input.length; b++)
        assertThat(Integer.signum(cmp.compare(dictionary.valueOf(a), dictionary.valueOf(b)))).isEqualTo(Integer.signum(dictionary.compare(a, b)));
  }

  @Test
  void unsupportedClassesOrderByClassNameThenToString() {
    final FacetValueComparator cmp = FacetValueComparator.INSTANCE;
    assertThat(cmp.compare(new StringBuilder("a"), "zzz")).isPositive();
    assertThat(cmp.compare(new StringBuilder("a"), true)).isPositive();
    assertThat(cmp.compare(new StringBuilder("a"), new StringBuilder("b"))).isNegative();
    assertThat(cmp.compare(new StringBuilder("a"), null)).isNegative();
  }

  @Test
  void datesOfOneClassOrderInTime() {
    final FacetValueComparator cmp = FacetValueComparator.INSTANCE;
    // Date.toString() is not chronological ("Mon ..." before "Sat ..."): the class's own order must win
    assertThat(cmp.compare(new Date(0L), new Date(86_400_000L * 5))).isNegative();
    assertThat(cmp.compare(LocalDate.of(2024, 1, 31), LocalDate.of(2024, 2, 1))).isNegative();
    assertThat(cmp.compare(LocalDate.of(2024, 1, 31), LocalDate.of(2024, 1, 31))).isZero();
  }

  @Test
  void nonFiniteFloatOrdersAsItsDouble() {
    final FacetValueComparator cmp = FacetValueComparator.INSTANCE;
    assertThat(FacetValueComparator.normalize(Float.NaN)).isEqualTo(Double.NaN);
    assertThat(cmp.compare(Float.NaN, Double.NaN)).isZero();
    assertThat(cmp.compare(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY)).isZero();
    assertThat(cmp.compare(Float.NEGATIVE_INFINITY, -1L)).isNegative();
    // a number, not an unsupported class: it sorts before a value of a class the dictionary cannot hold
    assertThat(cmp.compare(Float.POSITIVE_INFINITY, new StringBuilder("a"))).isNegative();
  }

  @Test
  void zeroInEveryRepresentationIsOneClassAndTransitive() {
    final Object[] zeros = { 0L, BigDecimal.ZERO, -0.0d, 0.0d, 0, new BigDecimal("0.000"), 0.0f, -0.0f };
    final FacetValueComparator cmp = FacetValueComparator.INSTANCE;
    for (final Object a : zeros)
      for (final Object b : zeros)
        assertThat(cmp.compare(a, b)).as("%s vs %s", a, b).isZero();
    final Object[] mixed = { 1L, -0.0d, BigDecimal.ZERO, 0L, -1L, 0.5d, new BigDecimal("0.5") };
    for (final Object a : mixed)
      for (final Object b : mixed)
        for (final Object c : mixed)
          if (cmp.compare(a, b) <= 0 && cmp.compare(b, c) <= 0)
            assertThat(cmp.compare(a, c)).as("%s <= %s <= %s", a, b, c).isLessThanOrEqualTo(0);
    final List<Object> sorted = new ArrayList<>(Arrays.asList(mixed));
    sorted.sort(cmp);
    assertThat(sorted.get(0)).isEqualTo(-1L);
    assertThat(sorted.get(sorted.size() - 1)).isEqualTo(1L);
  }

  @Test
  void rawValuesOrderAsTheDictionaryOrdersTheirNormalizedForm() {
    final FacetValueComparator cmp = FacetValueComparator.INSTANCE;
    // 0.05f is keyed through its decimal form (0.05), not 0.05000000074505806
    assertThat(cmp.compare(0.05f, new BigDecimal("0.05"))).isZero();
    assertThat(cmp.compare(0.05f, 0.05d)).isZero();
    assertThat(cmp.compare(2.0d, 2L)).isZero();
    final GlobalDictionary dictionary = GlobalDictionary.createInMemory();
    final Object[] raw = { 0.05f, 2.0d, -0.0d, 7, new BigDecimal("0.049"), new BigDecimal("0.051"), Double.NaN, 1.5f };
    final int[] ordinals = new int[raw.length];
    for (int i = 0; i < raw.length; i++)
      ordinals[i] = dictionary.getOrAdd(FacetValueComparator.normalize(raw[i]));
    dictionary.rebuildRankView();
    for (int a = 0; a < raw.length; a++)
      for (int b = 0; b < raw.length; b++)
        assertThat(Integer.signum(cmp.compare(raw[a], raw[b]))).as("%s vs %s", raw[a], raw[b])
            .isEqualTo(Integer.signum(dictionary.compare(ordinals[a], ordinals[b])));
  }

  /** The bounded heap of {@code topValues} answers exactly what a full sort of every candidate answers, for every order and size. */
  @Test
  void topValuesHeapEqualsAFullSort() {
    final Random random = new Random(41);
    for (int round = 0; round < 50; round++) {
      final Map<Object, long[]> counts = new LinkedHashMap<>();
      final int n = random.nextInt(300);
      for (int i = 0; i < n; i++) {
        final Object key = switch (random.nextInt(3)) {
          case 0 -> "v-" + random.nextInt(500);
          case 1 -> (long) random.nextInt(500);
          default -> random.nextInt(10) == 0 ? null : random.nextDouble() * 100;
        };
        // few distinct counts: most ranks are decided by the value tie-break
        counts.put(key, new long[] { 1 + random.nextInt(4) });
      }
      for (final FacetOrder order : FacetOrder.values())
        for (final int top : new int[] { 1, 3, 10, 1_000 })
          for (final int minCount : new int[] { 1, 3 })
            assertThat(FacetRanker.topValues(counts, top, minCount, order)).as(order + " top " + top + " minCount " + minCount)
                .isEqualTo(fullSort(counts, top, minCount, order));
    }
  }

  private static List<Map.Entry<Object, Long>> fullSort(final Map<Object, long[]> counts, final int top, final int minCount,
      final FacetOrder order) {
    final List<Map.Entry<Object, Long>> all = new ArrayList<>();
    for (final Map.Entry<Object, long[]> e : counts.entrySet())
      if (e.getValue()[0] >= minCount)
        all.add(new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue()[0]));
    final FacetValueComparator values = FacetValueComparator.INSTANCE;
    all.sort((a, b) -> {
      final Object x = a.getKey();
      final Object y = b.getKey();
      int cmp = switch (order) {
        case COUNT_DESC -> Long.compare(b.getValue(), a.getValue());
        case COUNT_ASC -> Long.compare(a.getValue(), b.getValue());
        default -> 0;
      };
      if (cmp == 0)
        cmp = order == FacetOrder.VALUE_DESC && x != null && y != null ? values.compare(y, x) : values.compare(x, y);
      return cmp;
    });
    return new ArrayList<>(all.subList(0, Math.min(top, all.size())));
  }
}
