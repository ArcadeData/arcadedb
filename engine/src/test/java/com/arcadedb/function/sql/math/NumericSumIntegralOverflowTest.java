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
package com.arcadedb.function.sql.math;

import com.arcadedb.exception.ArithmeticErrorException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link NumericSum} with {@code integralOverflowFails}, the openCypher {@code sum()} (issue #12993): whatever the order of the
 * values and however they are split in partials merged in any order, an all-integral total is the exact {@code long} or the
 * {@code long overflow} ArithmeticError, and a total with a {@code Double} in it is the exact sum correctly rounded. The default
 * mode keeps widening to a {@code BigDecimal}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class NumericSumIntegralOverflowTest {
  private static final Object OVERFLOW = new Object();

  @Test
  void theDefaultModeStillWidensToDecimal() {
    final NumericSum sum = new NumericSum();
    sum.addLong(Long.MAX_VALUE);
    sum.addLong(1L);
    assertThat(sum.get()).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(sum.isDecimal()).isTrue();
  }

  @Test
  void anIntegralTotalPastTheLongRangeIsALongOverflow() {
    final NumericSum sum = new NumericSum(true);
    sum.addLong(Long.MAX_VALUE);
    sum.addInt(1);
    assertThat(sum.isDecimal()).isFalse();
    assertThat(sum.doubleValue()).isEqualTo(9.223372036854775808E18);
    assertThatThrownBy(sum::get).isInstanceOf(ArithmeticErrorException.class).hasMessageContaining("long overflow");
    // BACK IN RANGE
    sum.addShort((short) -1);
    assertThat(sum.get()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void aFloatingPointValueMakesItTheFloatingPointTotal() {
    final NumericSum doubles = new NumericSum(true);
    doubles.addLong(1L << 62);
    doubles.addLong(1L << 62);
    doubles.addDouble(0.5);
    assertThat(doubles.get()).isEqualTo(9.223372036854776E18);

    final NumericSum floats = new NumericSum(true);
    floats.addLong(Long.MAX_VALUE);
    floats.addLong(Long.MAX_VALUE);
    floats.addFloat(1.5f);
    assertThat(floats.get()).isEqualTo(1.8446744E19f);
  }

  @Test
  void anyOrderAndAnySplitAnswersTheExactTotal() {
    final Random random = new Random(12993);
    final long[] magnitudes = { Long.MAX_VALUE, Long.MIN_VALUE, 1L << 62, -(1L << 62), 1L, -1L, 0L, 123_456_789L };
    for (int round = 0; round < 3_000; round++) {
      final List<Number> values = new ArrayList<>();
      final int count = 1 + random.nextInt(8);
      for (int i = 0; i < count; i++) {
        final int pick = random.nextInt(10);
        if (pick == 0)
          values.add(random.nextInt(5) - 2 + 0.5);
        else if (pick == 1)
          values.add(random.nextInt());
        else
          values.add(magnitudes[random.nextInt(magnitudes.length)]);
      }
      final Object expected = reference(values);

      for (int shuffle = 0; shuffle < 4; shuffle++) {
        Collections.shuffle(values, random);
        assertThat(answer(sequential(values))).as("%s", values).isEqualTo(expected);

        final int cut = random.nextInt(values.size() + 1);
        final NumericSum left = sequential(values.subList(0, cut));
        final NumericSum right = sequential(values.subList(cut, values.size()));
        if (random.nextBoolean()) {
          left.merge(right);
          assertThat(answer(left)).as("%s split at %d", values, cut).isEqualTo(expected);
        } else {
          right.merge(left);
          assertThat(answer(right)).as("%s split at %d, merged right", values, cut).isEqualTo(expected);
        }
      }
    }
  }

  private static NumericSum sequential(final List<Number> values) {
    final NumericSum sum = new NumericSum(true);
    for (final Number value : values)
      sum.add(value);
    return sum;
  }

  /** The total, an Integer one read as the Long it equals: whether a sum of Integers stays an Integer is not the point here. */
  private static Object answer(final NumericSum sum) {
    try {
      final Number total = sum.get();
      return total instanceof Integer i ? (Object) i.longValue() : total;
    } catch (final ArithmeticErrorException e) {
      return OVERFLOW;
    }
  }

  /** The exact total: a Double correctly rounded when a Double is among the values, else the Long, or OVERFLOW past its range. */
  private static Object reference(final List<Number> values) {
    BigDecimal exact = BigDecimal.ZERO;
    boolean floating = false;
    for (final Number value : values) {
      if (value instanceof Double d) {
        floating = true;
        exact = exact.add(new BigDecimal(d));
      } else
        exact = exact.add(BigDecimal.valueOf(value.longValue()));
    }
    if (floating)
      return exact.doubleValue();
    final long total = exact.longValue();
    return exact.compareTo(BigDecimal.valueOf(total)) != 0 ? OVERFLOW : (Object) total;
  }
}
