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
package com.arcadedb.bench.workload.soak;

import org.junit.jupiter.api.Test;

import java.util.OptionalDouble;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class SlopeTest {
  @Test
  void aStraightLineGivesItsSlope() {
    final double[] hours = new double[11];
    final double[] values = new double[11];
    for (int i = 0; i <= 10; i++) {
      hours[i] = i * 0.1;
      values[i] = 100 + 20 * hours[i];
    }
    assertThat(Slope.perHour(hours, values).getAsDouble()).isCloseTo(20.0, within(1e-9));
  }

  @Test
  void fewerThanSixSamplesHaveNoSlope() {
    assertThat(Slope.perHour(new double[] { 0, 1, 2, 3, 4 }, new double[] { 1, 2, 3, 4, 5 })).isEmpty();
    assertThat(Slope.perHour(new double[] { 0, 1, 2, 3, 4, 5 }, new double[] { 1, 2, 3, 4, 5, 6 })).isPresent();
  }

  @Test
  void flatNoiseIsNearZero() {
    final double[] hours = new double[24];
    final double[] values = new double[24];
    for (int i = 0; i < 24; i++) {
      hours[i] = i / 12.0;
      values[i] = 100 + ((i & 1) == 0 ? 1 : -1);
    }
    final OptionalDouble slope = Slope.perHour(hours, values);
    assertThat(slope).isPresent();
    assertThat(Math.abs(slope.getAsDouble())).isLessThan(1.0);
  }

  @Test
  void samplesAllAtOneHourHaveNoSlope() {
    assertThat(Slope.perHour(new double[6], new double[] { 1, 2, 3, 4, 5, 6 })).isEmpty();
  }
}
