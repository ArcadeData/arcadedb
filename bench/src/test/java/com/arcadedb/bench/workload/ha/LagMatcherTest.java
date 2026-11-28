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
package com.arcadedb.bench.workload.ha;

import com.arcadedb.bench.runner.LatencyRecorder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class LagMatcherTest {
  private static final long MS = 1_000_000L;

  private static long[] acks(final long... nanos) {
    return nanos;
  }

  @Test
  void lagIsFromTheAckClampedAtZero() {
    final LagMatcher matcher = new LagMatcher(acks(100 * MS, 200 * MS));
    // the follower showed id 1 before the leader's answer reached the client: a lag of 0, not a negative one
    matcher.seen(1, 150 * MS);
    final LatencyRecorder lags = matcher.lags();
    assertThat(lags.count()).isEqualTo(2);
    assertThat(lags.percentile(100)).isEqualTo(50 * MS);
    assertThat(lags.percentile(50)).isZero();
  }

  @Test
  void anIdIsCountedOncePerFollower() {
    final LagMatcher matcher = new LagMatcher(acks(0, 10 * MS, 20 * MS));
    matcher.seen(1, 30 * MS);
    matcher.seen(1, 90 * MS);
    // a poll answering an older maximum (an earlier read) does not take back what was seen
    matcher.seen(0, 95 * MS);
    LatencyRecorder lags = matcher.lags();
    assertThat(lags.count()).isEqualTo(2);
    assertThat(lags.percentile(100)).isEqualTo(30 * MS);
    matcher.seen(2, 100 * MS);
    lags = matcher.lags();
    assertThat(lags.count()).isEqualTo(3);
    assertThat(lags.percentile(100)).isEqualTo(80 * MS);
  }

  @Test
  void aJumpRecordsEveryIdUpToIt() {
    final long[] acks = new long[5];
    for (int i = 0; i < acks.length; i++)
      acks[i] = i * 10 * MS;
    final LagMatcher matcher = new LagMatcher(acks);
    matcher.seen(-1, 1 * MS);
    assertThat(matcher.lags().count()).isZero();
    matcher.seen(4, 50 * MS);
    final LatencyRecorder lags = matcher.lags();
    assertThat(lags.count()).isEqualTo(5);
    assertThat(lags.percentile(100)).isEqualTo(50 * MS);
    assertThat(lags.percentile(1)).isEqualTo(10 * MS);
    // past the last id written: bounded by the ids the writer issued
    matcher.seen(99, 60 * MS);
    assertThat(matcher.lags().count()).isEqualTo(5);
  }

  @Test
  void anIdNeverAcknowledgedHasNoLag() {
    final long[] acks = new long[3];
    Arrays.fill(acks, LagMatcher.NOT_ACKED);
    acks[0] = 0;
    acks[2] = 0;
    final LagMatcher matcher = new LagMatcher(acks);
    matcher.seen(2, 5 * MS);
    assertThat(matcher.lags().count()).isEqualTo(2);
  }
}
