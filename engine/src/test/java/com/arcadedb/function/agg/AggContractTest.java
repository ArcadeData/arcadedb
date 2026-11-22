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
package com.arcadedb.function.agg;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Contract of the Cypher aggregation functions that the openCypher end-to-end tests do not pin: the shape of
 * {@code agg.statistics} against an independent computation, and the numbers-or-durations rule of {@code avg()}.
 */
class AggContractTest {

  @Test
  @SuppressWarnings("unchecked")
  void statisticsAgreeWithAnIndependentComputationOverRandomLists() {
    final AggStatistics statistics = new AggStatistics();
    final Random random = new Random(0x5747L);
    for (int round = 0; round < 200; round++) {
      final int size = 1 + random.nextInt(12);
      final List<Object> input = new ArrayList<>();
      final List<Double> numbers = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        final double value = random.nextInt(2001) - 1000 + (random.nextBoolean() ? 0.25 : 0.0);
        numbers.add(value);
        // an integer or a double, and a null or a string the function skips
        input.add(random.nextInt(3) == 0 && value == Math.rint(value) ? (Object) (long) value : (Object) value);
        if (random.nextInt(4) == 0)
          input.add(random.nextBoolean() ? null : "skipped");
      }

      double sum = 0;
      for (final double number : numbers)
        sum += number;
      final double mean = sum / numbers.size();
      double squares = 0;
      for (final double number : numbers)
        squares += (number - mean) * (number - mean);
      final List<Double> sorted = new ArrayList<>(numbers);
      Collections.sort(sorted);
      final int middle = sorted.size() / 2;
      final double median = sorted.size() % 2 == 1 ? sorted.get(middle) : (sorted.get(middle - 1) + sorted.get(middle)) / 2.0;

      final Map<String, Object> result = (Map<String, Object>) statistics.execute(new Object[] { input }, null);
      final String what = "round " + round + " over " + input;
      assertThat(result.get("count")).as(what).isEqualTo((long) numbers.size());
      assertThat(result.get("min")).as(what).isEqualTo(sorted.get(0));
      assertThat(result.get("max")).as(what).isEqualTo(sorted.get(sorted.size() - 1));
      assertThat((Double) result.get("sum")).as(what).isCloseTo(sum, within(1e-9));
      assertThat((Double) result.get("mean")).as(what).isCloseTo(mean, within(1e-9));
      // the standard deviation of the population the list is, not of a sample of it
      assertThat((Double) result.get("stdev")).as(what).isCloseTo(Math.sqrt(squares / numbers.size()), within(1e-9));
      assertThat((Double) result.get("median")).as(what).isCloseTo(median, within(1e-9));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void statisticsOfOneValueHaveNoSpread() {
    final Map<String, Object> result = (Map<String, Object>) new AggStatistics().execute(new Object[] { List.of(7) }, null);
    assertThat(result.get("count")).isEqualTo(1L);
    assertThat(result.get("stdev")).isEqualTo(0.0);
    assertThat(result.get("median")).isEqualTo(7.0);
  }

  @Test
  void avgOfDurationsIsTheirComponentWiseSumDividedByTheirCount() {
    final CypherAvgFunction avg = new CypherAvgFunction();
    avg.execute(new Object[] { CypherDuration.parse("PT1H") }, null);
    avg.execute(new Object[] { null }, null);
    avg.execute(new Object[] { CypherDuration.parse("PT3H") }, null);
    assertThat(avg.getAggregatedResult()).isEqualTo(CypherDuration.parse("PT2H"));
  }

  @Test
  void avgRefusesToMixNumbersAndDurationsInEitherOrder() {
    final CypherAvgFunction numbersFirst = new CypherAvgFunction();
    numbersFirst.execute(new Object[] { 1 }, null);
    assertThatThrownBy(() -> numbersFirst.execute(new Object[] { CypherDuration.parse("PT1H") }, null))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot mix numbers and durations");

    final CypherAvgFunction durationsFirst = new CypherAvgFunction();
    durationsFirst.execute(new Object[] { CypherDuration.parse("PT1H") }, null);
    assertThatThrownBy(() -> durationsFirst.execute(new Object[] { 1 }, null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot mix numbers and durations");
  }

  @Test
  void avgRefusesAValueThatIsNeitherANumberNorADurationAndTakesOneArgument() {
    final CypherAvgFunction avg = new CypherAvgFunction();
    assertThatThrownBy(() -> avg.execute(new Object[] { "a" }, null)).hasMessageContaining("avg()");
    assertThatThrownBy(() -> avg.execute(new Object[] { 1, 2 }, null)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> avg.execute(new Object[0], null)).isInstanceOf(RuntimeException.class);
    assertThat(avg.getName()).isEqualTo("avg");
    assertThat(avg.aggregateResults()).isTrue();
    assertThat(avg.canMergePartials()).isTrue();
  }

  @Test
  void avgPartialsOfDurationsMergeAndPartialsOfDifferentKindsDoNot() {
    final CypherAvgFunction first = new CypherAvgFunction();
    first.execute(new Object[] { CypherDuration.parse("PT1H") }, null);
    final CypherAvgFunction second = new CypherAvgFunction();
    second.execute(new Object[] { CypherDuration.parse("PT3H") }, null);
    second.execute(new Object[] { CypherDuration.parse("PT5H") }, null);
    first.mergePartial(second);
    assertThat(first.getAggregatedResult()).isEqualTo(CypherDuration.parse("PT3H"));

    // an empty partial is the identity whatever kind this one holds
    first.mergePartial(new CypherAvgFunction());
    assertThat(first.getAggregatedResult()).isEqualTo(CypherDuration.parse("PT3H"));

    // a partial that holds durations merged into an empty one carries them over
    final CypherAvgFunction empty = new CypherAvgFunction();
    empty.mergePartial(second);
    assertThat(empty.getAggregatedResult()).isEqualTo(CypherDuration.parse("PT4H"));

    final CypherAvgFunction numbers = new CypherAvgFunction();
    numbers.execute(new Object[] { 2 }, null);
    assertThatThrownBy(() -> numbers.mergePartial(second)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot mix numbers and durations");
    assertThatThrownBy(() -> second.mergePartial(numbers)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot mix numbers and durations");
    // the refused merge changed nothing
    assertThat(numbers.getAggregatedResult()).isEqualTo(2.0);
    assertThat(second.getAggregatedResult()).isEqualTo(CypherDuration.parse("PT4H"));
  }

  @Test
  void everyAggFunctionFamilyDeclaresItsOwnNameAndArgumentBounds() {
    final List<StatelessFunction> functions = Arrays.asList(new AggFirst(), new AggLast(), new AggNth(), new AggMedian(), new AggProduct(),
        new AggSlice(), new AggStatistics(), new AggMaxItems(), new AggMinItems());
    for (final StatelessFunction function : functions) {
      assertThat(function.getName()).startsWith("agg.");
      assertThat(function.getMinArgs()).as(function.getName()).isBetween(1, 2);
      assertThat(function.getMaxArgs()).as(function.getName()).isGreaterThanOrEqualTo(function.getMinArgs());
      assertThat(function.getDescription()).as(function.getName()).isNotBlank();
    }
    assertThat(new AggFirst().execute(new Object[] { Arrays.asList(null, "x", "y") }, null)).isEqualTo("x");
    assertThat(new AggFirst().execute(new Object[] { Arrays.asList(null, null) }, null)).isNull();
    assertThat(new AggFirst().execute(new Object[] { null }, null)).isNull();
    assertThat(new AggProduct().execute(new Object[] { List.of(2, 3, 4.0) }, null)).isEqualTo(24.0);
    assertThat(new AggProduct().execute(new Object[] { List.of() }, null)).isNull();
    assertThat(new AggMedian().execute(new Object[] { List.of(5, 1, 3) }, null)).isEqualTo(3.0);
    assertThat(new AggMedian().execute(new Object[] { List.of(4, 1, 3, 2) }, null)).isEqualTo(2.5);
  }
}
