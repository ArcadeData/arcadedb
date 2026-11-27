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
package com.arcadedb.bench.workload.issues;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

class IssueSamplerTest {
  @Test
  void samplesAlternateForms() {
    final AtomicLong clock = new AtomicLong();
    final List<String> order = new ArrayList<>();
    final IssueSampler sampler = new IssueSampler(clock::get);
    // no minimum time: one round of 2 executions of each form per sample
    final IssueSampler.Samples r = sampler.alternateFor(() -> {
      order.add("S");
      clock.addAndGet(2_000_000L);
    }, () -> {
      order.add("R");
      clock.addAndGet(500_000L);
    }, 4, 2, 0.0);
    assertThat(order).containsExactly("S", "S", "R", "R", "R", "R", "S", "S", "S", "S", "R", "R", "R", "R", "S", "S");
    assertThat(r.medianMs()[0]).isEqualTo(2.0);
    assertThat(r.medianMs()[1]).isEqualTo(0.5);
  }

  @Test
  void rotateTimesEveryFormInTurn() {
    final AtomicLong clock = new AtomicLong();
    final List<String> order = new ArrayList<>();
    final IssueSampler sampler = new IssueSampler(clock::get);
    final Runnable a = () -> {
      order.add("A");
      clock.addAndGet(1_000_000L);
    };
    final Runnable b = () -> {
      order.add("B");
      clock.addAndGet(2_000_000L);
    };
    final Runnable c = () -> {
      order.add("C");
      clock.addAndGet(4_000_000L);
    };
    // no minimum time: one round per sample
    final IssueSampler.Samples r = sampler.rotateFor(new Runnable[] { a, b, c }, 3, new int[] { 1, 2, 1 }, 0.0);
    // each sample starts one form later than the one before, and each form runs its own batch
    assertThat(order).containsExactly("A", "B", "B", "C", "B", "B", "C", "A", "C", "A", "B", "B");
    assertThat(r.medianMs()).containsExactly(1.0, 2.0, 4.0);
    assertThat(r.elapsedMs()).isEqualTo(27.0);
  }

  @Test
  void aSampleLastsItsTargetWhateverTheBatch() {
    // a batch sized from a cold estimate (1 execution) must not make the samples shorter than their target
    final AtomicLong clock = new AtomicLong();
    final int[] runs = new int[2];
    final IssueSampler sampler = new IssueSampler(clock::get);
    final IssueSampler.Samples r = sampler.alternateFor(() -> {
      runs[0]++;
      clock.addAndGet(200_000L);
    }, () -> {
      runs[1]++;
      clock.addAndGet(100_000L);
    }, 5, 1, 10.0);
    assertThat(r.elapsedMs()).isGreaterThanOrEqualTo(50.0);
    assertThat(r.medianMs()[0]).isCloseTo(0.2, offset(1e-9));
    assertThat(r.medianMs()[1]).isCloseTo(0.1, offset(1e-9));
    // 0.3 ms per round, 10 ms per sample: 34 rounds of each form per sample
    assertThat(runs[0]).isEqualTo(5 * 34);
    assertThat(runs[1]).isEqualTo(5 * 34);
  }

  @Test
  void batchForTargetsTheSampleTime() {
    assertThat(IssueSampler.batchFor(0.5, 50.0)).isEqualTo(100);
    assertThat(IssueSampler.batchFor(100.0, 50.0)).isEqualTo(1);
    assertThat(IssueSampler.batchFor(0.0, 50.0)).isEqualTo(1);
  }
}
