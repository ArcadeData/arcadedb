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
package com.arcadedb.query.sql.function.math;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class SQLFunctionVarianceTest {

  private SQLFunctionVariance variance;

  @BeforeEach
  void setup() {
    variance = new SQLFunctionVariance();
  }

  @Test
  void empty() {
    final Object result = variance.getResult();
    assertThat(result).isNull();
  }

  @Test
  void singleValue() {
    variance.execute(null, null, null, new Object[] { 5 }, null);
    assertThat((Double) variance.getResult()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void variance() {
    // Sample variance of [4, 7, 15, 3]: mean=7.25, sum_sq_diff=88.75, sample=88.75/3
    final Integer[] scores = { 4, 7, 15, 3 };

    for (final Integer s : scores)
      variance.execute(null, null, null, new Object[] { s }, null);

    assertThat((Double) variance.getResult()).isCloseTo(88.75 / 3.0, within(0.0001));
  }

  @Test
  void variance1() {
    // Sample variance of [4, 7]: mean=5.5, sum_sq_diff=4.5, sample=4.5/1
    final Integer[] scores = { 4, 7 };

    for (final Integer s : scores)
      variance.execute(null, null, null, new Object[] { s }, null);

    assertThat((Double) variance.getResult()).isCloseTo(4.5, within(0.0001));
  }

  @Test
  void variance2() {
    // Sample variance of [15, 3]: mean=9, sum_sq_diff=72, sample=72/1
    final Integer[] scores = { 15, 3 };

    for (final Integer s : scores)
      variance.execute(null, null, null, new Object[] { s }, null);

    assertThat((Double) variance.getResult()).isCloseTo(72.0, within(0.0001));
  }

  @Test
  void populationVariance() {
    // Population variance of [4, 7, 15, 3]: mean=7.25, sum_sq_diff=88.75, pop=88.75/4
    final SQLFunctionVarianceP varianceP = new SQLFunctionVarianceP();
    final Integer[] scores = { 4, 7, 15, 3 };

    for (final Integer s : scores)
      varianceP.execute(null, null, null, new Object[] { s }, null);

    assertThat((Double) varianceP.getResult()).isCloseTo(22.1875, within(0.0001));
  }

  @Test
  void populationVarianceTwoValues() {
    // Population variance of [4, 7]: mean=5.5, sum_sq_diff=4.5, pop=4.5/2
    final SQLFunctionVarianceP varianceP = new SQLFunctionVarianceP();
    final Integer[] scores = { 4, 7 };

    for (final Integer s : scores)
      varianceP.execute(null, null, null, new Object[] { s }, null);

    assertThat((Double) varianceP.getResult()).isCloseTo(2.25, within(0.0001));
  }
}
