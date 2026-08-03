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
package com.arcadedb.function.coll;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.utility.LongRangeList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("unchecked")
class RangeFunctionTest {

  private final RangeFunction fn = new RangeFunction();

  /** Reported case: step skips past Long.MAX_VALUE - must return only the start element. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void positiveStepOverflowReturnsOnlyFittingElements() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ Long.MAX_VALUE - 7L, Long.MAX_VALUE, 1000L }, null);
    assertThat(result).containsExactly(Long.MAX_VALUE - 7L);
  }

  /** Step of 1 right up to Long.MAX_VALUE must return all 4 values. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void positiveStepUpToMaxValue() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ Long.MAX_VALUE - 3L, Long.MAX_VALUE, 1L }, null);
    assertThat(result).containsExactly(
        Long.MAX_VALUE - 3L,
        Long.MAX_VALUE - 2L,
        Long.MAX_VALUE - 1L,
        Long.MAX_VALUE);
  }

  /** Single-element range exactly at Long.MAX_VALUE. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void singleElementAtMaxValue() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ Long.MAX_VALUE, Long.MAX_VALUE, 1L }, null);
    assertThat(result).containsExactly(Long.MAX_VALUE);
  }

  /** Negative step that skips past Long.MIN_VALUE - must return only the start element. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void negativeStepUnderflowReturnsOnlyFittingElements() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ Long.MIN_VALUE + 7L, Long.MIN_VALUE, -1000L }, null);
    assertThat(result).containsExactly(Long.MIN_VALUE + 7L);
  }

  /** Step of -1 right down to Long.MIN_VALUE must return all 4 values. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void negativeStepDownToMinValue() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ Long.MIN_VALUE + 3L, Long.MIN_VALUE, -1L }, null);
    assertThat(result).containsExactly(
        Long.MIN_VALUE + 3L,
        Long.MIN_VALUE + 2L,
        Long.MIN_VALUE + 1L,
        Long.MIN_VALUE);
  }

  /** Single-element range exactly at Long.MIN_VALUE. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void singleElementAtMinValue() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ Long.MIN_VALUE, Long.MIN_VALUE, -1L }, null);
    assertThat(result).containsExactly(Long.MIN_VALUE);
  }

  /** Extreme: step = Long.MIN_VALUE; guard evaluates to 0 (MIN - MIN = 0 in two's-complement). */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void negativeStepLongMinValue() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ 0L, Long.MIN_VALUE, Long.MIN_VALUE }, null);
    assertThat(result).containsExactly(0L, Long.MIN_VALUE);
  }

  /** Symmetric to negativeStepLongMinValue: step = Long.MAX_VALUE on the positive branch. */
  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void positiveStepLongMaxValue() {
    @SuppressWarnings("unchecked")
    final List<Long> result = (List<Long>) fn.execute(
        new Object[]{ 0L, Long.MAX_VALUE, Long.MAX_VALUE }, null);
    assertThat(result).containsExactly(0L, Long.MAX_VALUE);
  }

  @Test
  void plainRangesKeepTheirValues() {
    assertThat((List<Long>) fn.execute(new Object[]{ 0L, 4L }, null)).containsExactly(0L, 1L, 2L, 3L, 4L);
    assertThat((List<Long>) fn.execute(new Object[]{ 0L, 10L, 3L }, null)).containsExactly(0L, 3L, 6L, 9L);
    assertThat((List<Long>) fn.execute(new Object[]{ 10L, 1L, -3L }, null)).containsExactly(10L, 7L, 4L, 1L);
    assertThat((List<Long>) fn.execute(new Object[]{ 5L, 1L }, null)).isEmpty();
    assertThat((List<Long>) fn.execute(new Object[]{ 1L, 5L, -1L }, null)).isEmpty();
  }

  /** Advisory GHSA-xmjm-8q85-g778: the range is lazy, so a huge one costs a constant amount of heap. */
  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void largeRangeIsLazy() {
    final long previous = GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(-1L);
      final List<Long> result = (List<Long>) fn.execute(new Object[]{ 0L, 999_999_999L }, null);
      assertThat(result).isInstanceOf(LongRangeList.class);
      assertThat(result.size()).isEqualTo(1_000_000_000);
      assertThat(result.get(999_999_999)).isEqualTo(999_999_999L);
    } finally {
      GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(previous);
    }
  }

  /** Advisory GHSA-xmjm-8q85-g778: the reported PoC must be rejected as a client error, not attempted. */
  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void oversizedRangeIsRejected() {
    assertThatThrownBy(() -> fn.execute(new Object[]{ 0L, 9_999_999_999L }, null))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("10000000000")
        .hasMessageContaining(GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getKey());
  }

  /** Even with the configured limit disabled, a range cannot exceed the maximum size of a Java list. */
  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void rangeBiggerThanAListIsRejectedEvenWithoutLimit() {
    final long previous = GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(-1L);
      assertThatThrownBy(() -> fn.execute(new Object[]{ 0L, 9_999_999_999L }, null))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining(String.valueOf(Integer.MAX_VALUE));
    } finally {
      GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(previous);
    }
  }

  @Test
  void configuredLimitIsEnforcedOnTheExactCardinality() {
    final long previous = GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(10L);
      assertThat((List<Long>) fn.execute(new Object[]{ 1L, 10L }, null)).hasSize(10);
      assertThatThrownBy(() -> fn.execute(new Object[]{ 1L, 11L }, null))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("11 elements");
    } finally {
      GlobalConfiguration.QUERY_MAX_RANGE_SIZE.setValue(previous);
    }
  }

  @Test
  void stepZeroIsStillRejected() {
    assertThatThrownBy(() -> fn.execute(new Object[]{ 0L, 10L, 0L }, null))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("step cannot be zero");
  }
}
