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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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
}
