/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import com.arcadedb.function.sql.SQLAggregatedFunction;
import org.junit.jupiter.api.Test;

import java.math.MathContext;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Model test of the SQL aggregations whose partial states merge (sum, avg, min, max, count): over random streams of integers,
 * longs close to the limits of a long, doubles and decimals, with nulls, the result is the one an exact {@link BigDecimal}
 * computation gives, and the merge of the states of two parts of the stream is the state of the whole stream, whatever the cut.
 */
class SQLAggregateMergeModelTest {
  private final Random random = new Random(0x9496L);

  private Object[] stream(final boolean decimals, final boolean doubles) {
    final int size = random.nextInt(9);
    final Object[] values = new Object[size];
    for (int i = 0; i < size; i++)
      values[i] = switch (random.nextInt(decimals || doubles ? 8 : 6)) {
        case 0 -> null;
        case 1 -> random.nextInt(2001) - 1000;
        case 2 -> (long) random.nextInt(2001) - 1000L;
        // the limits of a long only without a double or a decimal: a double total next to them cannot be as exact as the model
        case 3 -> decimals || doubles ? (long) random.nextInt(50) : Long.MAX_VALUE - random.nextInt(3);
        case 4 -> decimals || doubles ? (long) -random.nextInt(50) : Long.MIN_VALUE + random.nextInt(3);
        case 5 -> random.nextBoolean() && !decimals && !doubles ? Long.MAX_VALUE : 1L + random.nextInt(5);
        case 6 -> doubles ? (Object) (random.nextInt(1000) / 8.0) : (Object) BigDecimal.valueOf(random.nextInt(100000), 2);
        default -> decimals ? (Object) BigDecimal.valueOf(random.nextInt(100000), 3) : (Object) (random.nextInt(1000) / 4.0);
      };
    return values;
  }

  private static SQLAggregatedFunction fed(final Supplier<SQLAggregatedFunction> factory, final Object[] values, final int from, final int to) {
    final SQLAggregatedFunction function = factory.get();
    function.config(new Object[] { "x" });
    for (int i = from; i < to; i++)
      function.execute(null, null, null, new Object[] { values[i] }, null);
    return function;
  }

  private static BigDecimal exact(final Number number) {
    return number instanceof BigDecimal decimal ? decimal : number instanceof Double || number instanceof Float ?
        BigDecimal.valueOf(number.doubleValue()) : BigDecimal.valueOf(number.longValue());
  }

  private static void assertSame(final Object actual, final Object expected, final String what) {
    if (expected == null) {
      assertThat(actual).as(what).isNull();
      return;
    }
    assertThat(actual).as(what).isInstanceOf(Number.class);
    final BigDecimal a = exact((Number) actual);
    final BigDecimal e = exact((Number) expected);
    final BigDecimal tolerance = e.abs().multiply(BigDecimal.valueOf(1e-12)).add(BigDecimal.valueOf(1e-9));
    assertThat(a.subtract(e).abs()).as(what + ": " + actual + " against " + expected).isLessThanOrEqualTo(tolerance);
  }

  private void check(final Supplier<SQLAggregatedFunction> factory, final boolean decimals, final boolean doubles, final Reference reference,
      final String name) {
    for (int round = 0; round < 800; round++) {
      final Object[] values = stream(decimals, doubles);
      final String what = name + " over " + Arrays.toString(values);

      final SQLAggregatedFunction whole = fed(factory, values, 0, values.length);
      assertSame(whole.getResult(), reference.of(values, 0, values.length), what);

      final int cut = random.nextInt(values.length + 1);
      final SQLAggregatedFunction left = fed(factory, values, 0, cut);
      final SQLAggregatedFunction right = fed(factory, values, cut, values.length);
      assertThat(left.canMergePartials()).as(name + " merges").isTrue();
      left.mergePartial(right);
      assertSame(left.getResult(), reference.of(values, 0, values.length), what + " merged at " + cut);
      // the part merged in is left as it was
      assertSame(right.getResult(), reference.of(values, cut, values.length), what + " right part after the merge");
    }
  }

  @FunctionalInterface
  private interface Reference {
    Object of(Object[] values, int from, int to);
  }

  private static List<BigDecimal> numbers(final Object[] values, final int from, final int to) {
    final List<BigDecimal> numbers = new ArrayList<>();
    for (int i = from; i < to; i++)
      if (values[i] != null)
        numbers.add(exact((Number) values[i]));
    return numbers;
  }

  @Test
  void sumIsTheExactSumOfTheNumbersNullsAreSkippedAndAnOverflowOfALongWidens() {
    check(SQLFunctionSum::new, true, true, (values, from, to) -> {
      final List<BigDecimal> numbers = numbers(values, from, to);
      return numbers.isEmpty() ? null : numbers.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
    }, "sum");
  }

  @Test
  void sumOfIntegersAloneIsExactEvenPastTheLimitOfALong() {
    check(SQLFunctionSum::new, false, false, (values, from, to) -> {
      final List<BigDecimal> numbers = numbers(values, from, to);
      return numbers.isEmpty() ? null : numbers.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
    }, "sum of integers");
    final SQLAggregatedFunction sum = fed(SQLFunctionSum::new, new Object[] { Long.MAX_VALUE, 1L }, 0, 2);
    assertThat(sum.getResult()).isEqualTo(new BigDecimal("9223372036854775808"));
    final SQLAggregatedFunction negative = fed(SQLFunctionSum::new, new Object[] { Long.MIN_VALUE, -1L }, 0, 2);
    assertThat(negative.getResult()).isEqualTo(new BigDecimal("-9223372036854775809"));
  }

  @Test
  void avgIsTheMeanOfTheNumbersAndNullIsNotCounted() {
    check(SQLFunctionAverage::new, true, true, (values, from, to) -> {
      final List<BigDecimal> numbers = numbers(values, from, to);
      return numbers.isEmpty() ? null :
          numbers.stream().reduce(BigDecimal.ZERO, BigDecimal::add).divide(BigDecimal.valueOf(numbers.size()), new MathContext(30));
    }, "avg");
  }

  @Test
  void minAndMaxAreTheLeastAndTheGreatestNumber() {
    check(SQLFunctionMin::new, true, true, (values, from, to) -> numbers(values, from, to).stream().min(BigDecimal::compareTo).orElse(null), "min");
    check(SQLFunctionMax::new, true, true, (values, from, to) -> numbers(values, from, to).stream().max(BigDecimal::compareTo).orElse(null), "max");
  }

  @Test
  void countCountsTheValuesThatAreNotNull() {
    check(SQLFunctionCount::new, true, true, (values, from, to) -> (long) numbers(values, from, to).size(), "count");
  }
}
