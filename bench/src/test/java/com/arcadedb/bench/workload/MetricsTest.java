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
package com.arcadedb.bench.workload;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsTest {
  @Test
  void settlesOnTheFirstRepeatedRead() {
    final long[] sequence = { 10, 20, 30, 30, 40 };
    final AtomicInteger i = new AtomicInteger();
    assertThat(Metrics.settle(() -> sequence[i.getAndIncrement()], 1, 5_000)).isEqualTo(30);
    assertThat(i.get()).isEqualTo(4);
  }

  @Test
  void neverSettlingReturnsTheLastReadAfterTheBound() {
    final AtomicInteger i = new AtomicInteger();
    final long result = Metrics.settle(() -> i.incrementAndGet(), 5, 100);
    assertThat(result).isEqualTo(i.get());
    assertThat(i.get()).isGreaterThan(2);
  }
}
