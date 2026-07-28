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
   * would need tens of GB of heap, so completing within the timeout is the proof that nothing is materialised.
   */
  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void hugeRangeIsFreeOfHeap() {
    final LongRangeList list = new LongRangeList(0L, 1L, 1_000_000_000);
    assertThat(list.size()).isEqualTo(1_000_000_000);
    assertThat(list.get(999_999_999)).isEqualTo(999_999_999L);
    assertThat(list.contains(123_456_789L)).isTrue();
    assertThat(list.indexOf(123_456_789L)).isEqualTo(123_456_789);
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
