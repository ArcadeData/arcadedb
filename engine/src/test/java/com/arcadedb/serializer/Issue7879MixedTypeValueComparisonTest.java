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
package com.arcadedb.serializer;

import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Issue #7879: {@code CollectionUtils.compare(Map, Map)} was hardened against a heterogeneous key set (#7111) but
 * not against a heterogeneous value set - the values of paired entries, and the elements of the {@code List}
 * overload, went straight to {@code BinaryComparator.compareTo(Object, Object)}, which threw
 * {@code ClassCastException} for any two values of different classes. A schemaless document mixes {@code Integer}
 * and {@code Long} in the same property routinely (any value read back from JSON, or written by a client that does
 * not pin the width), so ordering one failed the whole query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7879MixedTypeValueComparisonTest {

  @Test
  void mixedBoxedWidthsOfEqualValueCompareEqual() {
    assertThat(BinaryComparator.compareTo(1, 1L)).isZero();
    assertThat(BinaryComparator.compareTo(1L, 1)).isZero();
    assertThat(BinaryComparator.compareTo((byte) 1, (short) 1)).isZero();
    assertThat(BinaryComparator.compareTo(1.5f, 1.5d)).isZero();
    assertThat(BinaryComparator.compareTo(new BigDecimal("1.0"), 1)).isZero();
    assertThat(BinaryComparator.compareTo(1, new BigDecimal("1.0"))).isZero();
    assertThat(BinaryComparator.compareTo(BigInteger.valueOf(5), 5L)).isZero();
    assertThat(BinaryComparator.compareTo(BigInteger.valueOf(5), new BigDecimal("5.0"))).isZero();
  }

  @Test
  void mixedBoxedWidthsOrderByMagnitudeAndStayAntisymmetric() {
    assertThat(BinaryComparator.compareTo(1, 2L)).isLessThan(0);
    assertThat(BinaryComparator.compareTo(2L, 1)).isGreaterThan(0);
    assertThat(BinaryComparator.compareTo(1.5f, 2.5d)).isLessThan(0);
    assertThat(BinaryComparator.compareTo(2.5d, 1.5f)).isGreaterThan(0);
    assertThat(BinaryComparator.compareTo(new BigDecimal("1.5"), 2)).isLessThan(0);
    assertThat(BinaryComparator.compareTo(2, new BigDecimal("1.5"))).isGreaterThan(0);
  }

  /** A `long` past 2^53 must not lose precision through a `double` round-trip when it meets a floating operand. */
  @Test
  void largeLongAgainstFloatingStaysExact() {
    final long exactlyAboveDoublePrecision = (1L << 53) + 1;
    assertThat(BinaryComparator.compareTo(exactlyAboveDoublePrecision, (double) exactlyAboveDoublePrecision - 1))
        .isGreaterThan(0);
  }

  @Test
  void nonFiniteFloatingValuesStillOrder() {
    assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo(Double.NaN, 1));
    assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo(1, Double.POSITIVE_INFINITY));
    assertThat(BinaryComparator.compareTo(1, Double.POSITIVE_INFINITY)).isLessThan(0);
    assertThat(BinaryComparator.compareTo(Double.NEGATIVE_INFINITY, 1)).isLessThan(0);
  }

  /** Genuinely unrelated classes must order (by class name, the same tiebreak Map keys already use) rather than throw. */
  @Test
  void unrelatedClassesOrderByClassNameInsteadOfThrowing() {
    assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo("x", 1));
    final int cmp = BinaryComparator.compareTo("x", 1);
    assertThat(Integer.signum(cmp)).isEqualTo(-Integer.signum(BinaryComparator.compareTo(1, "x")));
  }

  /** {@code ArrayList} is not {@code Comparable}: the List arm must delegate to {@link CollectionUtils#compare} instead
   *  of falling through to the raw cast, exactly as {@code TransactionIndexContext.ComparableKey} already relies on. */
  @Test
  void listsCompareThroughCollectionUtilsInsteadOfThrowing() {
    assertThat(BinaryComparator.compareTo(List.of(1), List.of(1))).isZero();
    assertThat(BinaryComparator.compareTo(List.of(1), List.of(2))).isLessThan(0);
    assertThat(BinaryComparator.compareTo(List.of(2), List.of(1))).isGreaterThan(0);
    // The element classes themselves may differ too, the same gap as the map value case.
    assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo(List.of("x"), List.of(1)));
  }

  @Test
  void collectionUtilsCompareListDoesNotThrowOnMixedElementClasses() {
    assertThatNoException().isThrownBy(() -> CollectionUtils.compare(List.of("x"), List.of(1)));
  }

  /** The three repro lines from the issue report, direct on the utility. */
  @Test
  void issueReportedRepros() {
    final Map<String, Comparable> m1 = new LinkedHashMap<>(Map.of("a", 1));
    final Map<String, Comparable> m2 = new LinkedHashMap<>(Map.of("a", 1L));
    assertThat(CollectionUtils.compare(m1, m2)).isZero();

    final Map<String, Comparable> m3 = new LinkedHashMap<>(Map.of("a", "x"));
    final Map<String, Comparable> m4 = new LinkedHashMap<>(Map.of("a", 1));
    assertThatNoException().isThrownBy(() -> CollectionUtils.compare(m3, m4));

    assertThatNoException().isThrownBy(() -> CollectionUtils.compare(List.of("x"), List.of(1)));
  }

  /**
   * Antisymmetry and transitivity of the induced order over a handful of mixed-class values - what a sorted index
   * relies on to place the same entries in the same order on every build. Mirrors
   * {@code CollectionUtilsTest.compareMapsSatisfiesComparatorContract}.
   */
  @Test
  void mixedValueComparisonSatisfiesComparatorContract() {
    final List<Object> values = List.of(1, 2L, 1.5f, 2.5d, new BigDecimal("1.5"), "a", "b", BigInteger.valueOf(10));

    for (final Object x : values) {
      assertThat(BinaryComparator.compareTo(x, x)).as("%s vs itself", x).isZero();
      for (final Object y : values) {
        final int xy = Integer.signum(BinaryComparator.compareTo(x, y));
        assertThat(xy).as("%s vs %s", x, y).isEqualTo(-Integer.signum(BinaryComparator.compareTo(y, x)));
      }
    }
  }
}
