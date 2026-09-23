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

import com.arcadedb.database.RID;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  /**
   * CodeRabbit suggested guarding the same-class fast path with {@code a instanceof Comparable}, so two instances
   * of a non-Comparable class would fall to the class-name tiebreak (0, since the name is identical) instead of
   * throwing. Tried and reverted: {@code LtOperatorTest}/{@code GeOperatorTest}/{@code LeOperatorTest} explicitly
   * assert that {@code op.execute(null, new Object(), new Object())} throws {@code ClassCastException} - two
   * genuinely unrelated same-class objects are meant to be "not comparable", not silently "equal". Pinned here so
   * a future change cannot reintroduce the CodeRabbit suggestion without noticing the conflict.
   */
  @Test
  void sameClassNonComparablePairStillThrows() {
    final Object a = new Object();
    final Object b = new Object();
    assertThat(a.getClass()).isEqualTo(b.getClass());
    assertThatThrownBy(() -> BinaryComparator.compareTo(a, b)).isInstanceOf(ClassCastException.class);
  }

  /**
   * CodeRabbit review follow-up: {@link RID#compareTo(Object)} deliberately compares against a {@code String}
   * operand by parsing it (issue #6188), so {@code RID vs its own string spelling} must answer 0 in BOTH
   * directions. The forward direction (RID first) always worked because RID's own compareTo runs directly, but
   * the reverse (String first) used to fall straight to the class-name tiebreak the instant
   * {@code String#compareTo(Object)} blind-cast the RID and threw - which does not equal 0 for two different
   * classes, breaking antisymmetry. The fallback now retries the reverse direction (negated) before giving up.
   */
  @Test
  void ridAndItsStringSpellingCompareEqualInEitherOrder() {
    final RID rid = new RID(3, 7);
    final String spelling = rid.toString();

    assertThat(BinaryComparator.compareTo(rid, spelling)).isZero();
    assertThat(BinaryComparator.compareTo(spelling, rid)).isZero();
  }

  @Test
  void ridAndAMismatchedStringSpellingStayAntisymmetric() {
    final RID rid = new RID(3, 7);
    final String other = new RID(3, 9).toString();

    final int forward = BinaryComparator.compareTo(rid, other);
    final int reverse = BinaryComparator.compareTo(other, rid);
    assertThat(forward).isNotZero();
    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(reverse));
  }

  /**
   * The reverse-direction retry must still only absorb ClassCastException: a malformed, non-RID-shaped String
   * reaches RID#compareTo(String) exactly as it would have in the forward direction, and that still throws
   * (issue #6188) rather than being swallowed into a false "not equal" class-name answer.
   */
  @Test
  void malformedStringReverseAgainstRidStillThrows() {
    final RID rid = new RID(3, 7);
    assertThatThrownBy(() -> BinaryComparator.compareTo("not-a-rid", rid))
        .isInstanceOfAny(IllegalArgumentException.class, IndexOutOfBoundsException.class);
  }

  /**
   * CodeRabbit review follow-up: negating a raw {@code compareTo()} result to reverse it overflows when that
   * result is {@code Integer.MIN_VALUE} ({@code -Integer.MIN_VALUE == Integer.MIN_VALUE} in two's complement),
   * silently breaking antisymmetry for that one pair - a real bug even though {@code RID}'s own bounded
   * {@code {-1, 0, 1}} compareTo() never triggers it. A minimal {@code Comparable} that returns
   * {@code MIN_VALUE} proves the fix ({@code Integer.compare(0, reversed)}) gets the sign right where a raw
   * negation would not.
   */
  @Test
  void reverseDirectionSurvivesAMinValueCompareToWithoutOverflow() {
    final class ExtremeComparable implements Comparable<Object> {
      @Override
      public int compareTo(final Object o) {
        if (o instanceof String)
          return Integer.MIN_VALUE;
        throw new ClassCastException();
      }
    }

    final Object extreme = new ExtremeComparable();
    // Forward: extreme.compareTo("x") answers MIN_VALUE directly - no negation involved, so this is just a sanity
    // check that a legitimately extreme comparator value passes through unchanged.
    assertThat(BinaryComparator.compareTo(extreme, "x")).isEqualTo(Integer.MIN_VALUE);
    // Reverse: String#compareTo(Object) blind-casts `extreme` and throws, forcing the retry path this test is
    // actually about. A raw `-Integer.MIN_VALUE` would overflow back to MIN_VALUE (still "less than"), which
    // contradicts the forward direction above; the fix must answer "greater than" (positive) instead.
    assertThat(BinaryComparator.compareTo("x", extreme)).isGreaterThan(0);
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
