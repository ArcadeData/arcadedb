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

  @Test
  void addAllKeepsEverySample() {
    final LatencyRecorder a = new LatencyRecorder(16);
    a.record(5L);
    a.record(70L);
    a.record(3L);
    final LatencyRecorder b = new LatencyRecorder(16);
    b.record(9L);
    b.record(90L);
    b.record(1L);
    b.record(40L);
    a.addAll(b);
    assertThat(a.count()).isEqualTo(7);
    assertThat(a.percentile(100)).isEqualTo(90L);
    assertThat(a.percentile(1)).isEqualTo(1L);
    assertThat(a.totalNanos()).isEqualTo(5L + 70 + 3 + 9 + 90 + 1 + 40);
    // the source is left as it was
    assertThat(b.count()).isEqualTo(4);
  }

  @Test
  void addAllGrowsPastTheCapacity() {
    final LatencyRecorder a = new LatencyRecorder(16);
    final LatencyRecorder b = new LatencyRecorder(16);
    for (int i = 0; i < 100; i++) {
      a.record(i);
      b.record(1000 + i);
    }
    a.addAll(b);
    assertThat(a.count()).isEqualTo(200);
    assertThat(a.percentile(100)).isEqualTo(1099L);
  }

  @Test
  void everySampleIsProgressForTheStallWatchdog() {
    final LatencyRecorder recorder = new LatencyRecorder(16);
    final long before = Progress.count();
    recorder.record(1_000L);
    recorder.record(2_000L);
    // other tests may record meanwhile: at least these two
    assertThat(Progress.count() - before).isGreaterThanOrEqualTo(2L);
  }
}
