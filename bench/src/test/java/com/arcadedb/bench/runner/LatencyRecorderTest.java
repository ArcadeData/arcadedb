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

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LatencyRecorderTest {
  @Test
  void encodedHistogramRoundTripsCountAndP99() {
    final LatencyRecorder recorder = new LatencyRecorder(16);
    for (int ms = 1; ms <= 1000; ms++)
      recorder.record(ms * 1_000_000L);
    final Histogram h = LatencyRecorder.decode(recorder.encodedHistogram());
    assertThat(h.getTotalCount()).isEqualTo(1000L);
    assertThat((double) h.getValueAtPercentile(99)).isBetween(990_000 * 0.99, 990_000 * 1.01);
  }

  @Test
  void emptyRecorderEncodesAnEmptyHistogram() {
    final Histogram h = LatencyRecorder.decode(new LatencyRecorder(16).encodedHistogram());
    assertThat(h.getTotalCount()).isZero();
  }

  @Test
  void aSampleAboveTheHighestTrackableValueIsClamped() {
    final LatencyRecorder recorder = new LatencyRecorder(16);
    recorder.record(2L * 3_600_000_000_000L);
    final Histogram h = LatencyRecorder.decode(recorder.encodedHistogram());
    assertThat(h.getTotalCount()).isEqualTo(1L);
  }
}
