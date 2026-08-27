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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link InListMembership} is an accelerator, never a second definition of membership: its answer has to be
 * the one {@link InCondition#evaluateExpressionThreeValued} would have given, for every left value, whether
 * the hash fast path applies or declines. This pins that equivalence across the shapes where the two could
 * plausibly drift - cross-type numbers, {@code null} elements, mixed-kind lists, values no fast path indexes -
 * so a future widening of the fast path cannot quietly change what {@code IN} means (#6796).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InListMembershipTest {

  @Test
  void homogeneousStringListAgreesWithTheLinearEvaluator() {
    final List<Object> list = List.of("a", "b", "c");
    assertAgreesOn(list, "a", "c", "z", "", 1, 1L, null, new RID(1, 1), List.of("a"));
  }

  @Test
  void homogeneousNumberListAgreesAcrossNumericTypes() {
    // The literal list is parsed into one numeric type and the stored property may hold another: the hash key
    // has to normalize both onto the same value or an indexed long would stop matching a literal int.
    final List<Object> list = List.of(1, 2L, (short) 3, 4.0d, 5.5f, (byte) 6);
    assertAgreesOn(list, 1, 1L, 1.0d, 1.0f, (byte) 1, 2, 3L, 4, 4L, 5.5f, 6, 7, 0, -1, null, "1", "x");
  }

  @Test
  void aBigDecimalAnywhereDeclinesTheNumericFastPath() {
    // BigDecimal.equals is scale-sensitive, so the linear path calls 5.5d and 5.50 different numbers. No
    // canonical key can reproduce that - the list has to fall back wholesale rather than answer it its own way.
    assertAgreesOn(List.of(1, new BigDecimal("5.50")), 1, 1L, 5.5d, new BigDecimal("5.50"), new BigDecimal("5.5"), null);
    assertAgreesOn(List.of(BigInteger.valueOf(6)), 6, 6L, BigInteger.valueOf(6), null);
  }

  @Test
  void listWithANullElementMakesAMissUnknown() {
    final List<Object> list = Arrays.asList("a", null, "b");
    assertAgreesOn(list, "a", "b", "z", null, 1);
  }

  @Test
  void mixedKindListDeclinesTheFastPathAndKeepsTheCoercions() {
    // '1' equals 1 under QueryOperatorEquals - a coercion no hash set reproduces - so a list mixing the two
    // kinds must fall back wholesale rather than answer half of it from a set.
    final List<Object> list = List.of("1", 2);
    assertAgreesOn(list, "1", 1, 1L, 2, "2", "z", null);
  }

  @Test
  void valuesNoFastPathIndexesStillAnswerExactly() {
    final Date now = new Date();
    assertAgreesOn(Arrays.asList(now, new Date(0)), now, new Date(0), new Date(1), null);
    assertAgreesOn(List.of(new RID(1, 1)), new RID(1, 1), new RID(1, 2), "#1:1", null);
    assertAgreesOn(List.of(true, false), true, false, "true", null);
  }

  @Test
  void emptyAndAllNullListsAgree() {
    assertAgreesOn(new ArrayList<>(), "a", 1, null);
    assertAgreesOn(Arrays.asList((Object) null, null), "a", 1, null);
  }

  @Test
  void arrayRightHandSideIsIndexedLikeAList() {
    final Object[] array = { "a", "b" };
    final InListMembership membership = InListMembership.build(array);
    assertThat(membership.evaluate("a")).isTrue();
    assertThat(membership.evaluate("z")).isFalse();
    assertThat(membership.evaluate(null)).isNull();
  }

  @Test
  void nonIndexableRightHandSidesAreCarriedThroughUntouched() {
    // A scalar degrades to an equality test, exactly as the linear evaluator does.
    assertThat(InListMembership.build("a").evaluate("a")).isTrue();
    assertThat(InListMembership.build("a").evaluate("b")).isFalse();
    // A null right-hand side is the caller's to turn into UNKNOWN, so it has to survive the build intact.
    assertThat(InListMembership.build(null).getRightValue()).isNull();
  }

  private void assertAgreesOn(final Object list, final Object... leftValues) {
    final InListMembership membership = InListMembership.build(list);
    for (final Object left : leftValues)
      assertThat(membership.evaluate(left))
          .as("membership of %s (%s) in %s", left, left == null ? "null" : left.getClass().getSimpleName(), list)
          .isEqualTo(InCondition.evaluateExpressionThreeValued(left, list));
  }
}
