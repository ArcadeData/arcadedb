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
package com.arcadedb.bench.runner;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StatsTest {
  @Test
  void medianOddAndEven() {
    assertThat(Stats.median(new double[] { 3, 1, 2 })).isEqualTo(2.0);
    assertThat(Stats.median(new double[] { 4, 1, 3, 2 })).isEqualTo(2.5);
  }

  @Test
  void percentilesOnKnownSet() {
    final LatencyRecorder r = new LatencyRecorder(4);
    for (int i = 100; i >= 1; i--)
      r.record(i);
    assertThat(r.count()).isEqualTo(100);
    assertThat(r.percentile(50)).isEqualTo(50L);
    assertThat(r.percentile(95)).isEqualTo(95L);
    assertThat(r.percentile(99)).isEqualTo(99L);
  }
}
