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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the lazy range list introduced with advisory GHSA-xmjm-8q85-g778.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LongRangeListTest {

  @Test
  void elementsAreComputedFromStartAndStep() {
    assertThat(new LongRangeList(0L, 1L, 5)).containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat(new LongRangeList(10L, -3L, 4)).containsExactly(10L, 7L, 4L, 1L);
    assertThat(new LongRangeList(0L, 1L, 0)).isEmpty();
  }

  @Test
  void randomAccessRespectsBounds() {
    final LongRangeList list = new LongRangeList(100L, 5L, 3);
    assertThat(list.get(0)).isEqualTo(100L);
    assertThat(list.get(2)).isEqualTo(110L);
    assertThatThrownBy(() -> list.get(3)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> list.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  void cardinalityMatchesTheGeneratedElements() {
    assertThat(LongRangeList.cardinality(0L, 4L, 1L)).isEqualTo(5L);
    assertThat(LongRangeList.cardinality(0L, 10L, 3L)).isEqualTo(4L);
    assertThat(LongRangeList.cardinality(5L, 1L, 1L)).isZero();
    assertThat(LongRangeList.cardinality(1L, 5L, -1L)).isZero();
    assertThat(LongRangeList.cardinality(10L, 1L, -3L)).isEqualTo(4L);
    assertThat(LongRangeList.cardinality(7L, 7L, 1L)).isEqualTo(1L);
  }

  /** end - start overflows a long here: the count must still be exact, not wrapped around. */
  @Test
  void cardinalityDoesNotOverflow() {
    assertThat(LongRangeList.cardinality(Long.MIN_VALUE, Long.MAX_VALUE, 1L)).isEqualTo(Long.MAX_VALUE);
    assertThat(LongRangeList.cardinality(Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)).isEqualTo(3L);
    assertThat(LongRangeList.cardinality(0L, Long.MAX_VALUE, Long.MAX_VALUE)).isEqualTo(2L);
    assertThat(LongRangeList.cardinality(0L, Long.MIN_VALUE, Long.MIN_VALUE)).isEqualTo(2L);
  }

  /**
   * A billion-element range must cost nothing: it is only start, step and size. Building the same list eagerly
   * would need tens of GB of heap, so returning quickly is the proof that nothing is materialised.
   * <p>
   * That claim is asserted on the stall-discounted clock rather than by the {@code @Timeout} (issue #6270): every
   * operation here is O(1) arithmetic, so the honest budget is microseconds and a 5 s discounted bound is three
   * orders of magnitude tighter than the 10 s annotation it replaces - while being immune to the stop-the-world
   * pause that made that annotation a coin flip late in a shared-JVM run. The annotation stays behind it as the
   * hang detector, because a materialised range does not merely take longer, it never finishes.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void hugeRangeIsFreeOfHeap() {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    final LongRangeList list = new LongRangeList(0L, 1L, 1_000_000_000);
    assertThat(list.size()).isEqualTo(1_000_000_000);
    assertThat(list.get(999_999_999)).isEqualTo(999_999_999L);
    assertThat(list.contains(123_456_789L)).isTrue();
    assertThat(list.indexOf(123_456_789L)).isEqualTo(123_456_789);

    stopwatch.assertStayedUnder(5_000L, "a constant-cost lazy range, not a materialised billion-element list");
  }

  @Test
  void containsAndIndexOfAreExact() {
    final LongRangeList list = new LongRangeList(0L, 3L, 5); // 0, 3, 6, 9, 12
    assertThat(list.contains(0L)).isTrue();
    assertThat(list.contains(12L)).isTrue();
    assertThat(list.contains(4L)).isFalse();
    assertThat(list.contains(15L)).isFalse();
    assertThat(list.contains(-3L)).isFalse();
    assertThat(list.contains("6")).isFalse();
    assertThat(list.contains(null)).isFalse();
    assertThat(list.contains(6.0d)).isFalse();
    assertThat(list.indexOf(9L)).isEqualTo(3);
    assertThat(list.indexOf(9)).isEqualTo(3);
    assertThat(list.indexOf(10L)).isEqualTo(-1);
    assertThat(list.lastIndexOf(9L)).isEqualTo(3);
  }

  /**
   * {@code indexOf} answers the {@link List} contract, which is {@code equals()}, and no {@code Long} equals a
   * {@code BigInteger} or a {@code BigDecimal}. Truncating them to a long said otherwise, and said it wrongly:
   * every value congruent to an element modulo 2^64 answered as that element.
   */
  @Test
  void indexOfRejectsTheTypesThatNoElementCanEqual() {
    final LongRangeList list = new LongRangeList(0L, 3L, 5); // 0, 3, 6, 9, 12
    assertThat(list.indexOf(BigInteger.valueOf(9))).isEqualTo(-1);
    assertThat(list.indexOf(new BigDecimal("9"))).isEqualTo(-1);
    assertThat(list.indexOf(BigInteger.ONE.shiftLeft(64).add(BigInteger.valueOf(9)))).isEqualTo(-1);
    assertThat(list.contains(BigInteger.valueOf(9))).isFalse();
  }

  /** Membership by value, which is what a caller whose own equality coerces numerically needs (issue #6323). */
  @Test
  void containsLongAnswersByValue() {
    final LongRangeList list = new LongRangeList(0L, 3L, 5); // 0, 3, 6, 9, 12
    assertThat(list.containsLong(9L)).isTrue();
    assertThat(list.containsLong(12L)).isTrue();
    assertThat(list.containsLong(10L)).isFalse();
    assertThat(list.containsLong(15L)).isFalse();
    assertThat(list.containsLong(-3L)).isFalse();
    assertThat(new LongRangeList(10L, -2L, 6).containsLong(0L)).isTrue();  // 10, 8, 6, 4, 2, 0
    assertThat(new LongRangeList(10L, -2L, 6).containsLong(-2L)).isFalse();
    assertThat(new LongRangeList(Long.MIN_VALUE, 1L, 10).containsLong(Long.MAX_VALUE)).isFalse();
  }

  /** The distance from the start overflows a long: the lookup must not wrap around into a false positive. */
  @Test
  void containsHandlesOverflowingDistance() {
    final LongRangeList list = new LongRangeList(Long.MIN_VALUE, 1L, 10);
    assertThat(list.contains(Long.MAX_VALUE)).isFalse();
    assertThat(list.contains(Long.MIN_VALUE + 9L)).isTrue();
  }

  @Test
  void subListStaysLazy() {
    final List<Long> sub = new LongRangeList(0L, 2L, 100).subList(10, 13);
    assertThat(sub).isInstanceOf(LongRangeList.class).containsExactly(20L, 22L, 24L);
    assertThat(new LongRangeList(0L, 1L, 5).subList(2, 2)).isEmpty();
  }

  /**
   * A range read backwards is still an arithmetic progression, so it must come back as one: copying it is what
   * put the heap exhaustion back into {@code reverse()} (issue #6353).
   */
  @Test
  void reversedIsStillALazyRange() {
    assertThat(new LongRangeList(0L, 1L, 5).reversed()).isInstanceOf(LongRangeList.class)
        .containsExactly(4L, 3L, 2L, 1L, 0L);
    assertThat(new LongRangeList(0L, 3L, 4).reversed()).isInstanceOf(LongRangeList.class)
        .containsExactly(9L, 6L, 3L, 0L);
    assertThat(new LongRangeList(10L, -3L, 4).reversed()).isInstanceOf(LongRangeList.class)
        .containsExactly(1L, 4L, 7L, 10L);
    // Reversing twice is the identity, and the empty and single-element cases are their own reverse.
    assertThat(((LongRangeList) new LongRangeList(10L, -3L, 4).reversed()).reversed()).containsExactly(10L, 7L, 4L, 1L);
    assertThat(new LongRangeList(0L, 1L, 0).reversed()).isInstanceOf(LongRangeList.class).isEmpty();
    assertThat(new LongRangeList(7L, 2L, 1).reversed()).isInstanceOf(LongRangeList.class).containsExactly(7L);
  }

  /**
   * {@code Long.MIN_VALUE} has no positive counterpart, so the reversed step is not representable. Only a
   * two-element range can carry that step - a third element would need {@code 2 * |step|} to fit in a long - so
   * the answer is materialised rather than wrapped around into a wrong range.
   */
  @Test
  void reversedFallsBackWhenTheStepCannotBeNegated() {
    final LongRangeList list = new LongRangeList(Long.MAX_VALUE, Long.MIN_VALUE, 2);
    assertThat(list).containsExactly(Long.MAX_VALUE, -1L);
    assertThat(list.reversed()).containsExactly(-1L, Long.MAX_VALUE);
  }

  @Test
  void equalsAndHashCodeFollowTheListContract() {
    assertThat(new LongRangeList(0L, 1L, 3)).isEqualTo(List.of(0L, 1L, 2L));
    assertThat(new LongRangeList(0L, 1L, 3).hashCode()).isEqualTo(List.of(0L, 1L, 2L).hashCode());
  }

  @Test
  void isImmutable() {
    final LongRangeList list = new LongRangeList(0L, 1L, 3);
    assertThatThrownBy(() -> list.add(4L)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> list.set(0, 4L)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> list.remove(0)).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void invalidDefinitionIsRejected() {
    assertThatThrownBy(() -> new LongRangeList(0L, 0L, 3)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new LongRangeList(0L, 1L, -1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LongRangeList.cardinality(0L, 10L, 0L)).isInstanceOf(IllegalArgumentException.class);
  }
}
