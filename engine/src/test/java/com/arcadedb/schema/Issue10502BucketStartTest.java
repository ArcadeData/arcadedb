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
package com.arcadedb.schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10502: the start of the bucket a timestamp is in, on the grid of fixed-width buckets a watermark is a start of.
 */
class Issue10502BucketStartTest {
  private static final long HOUR = 3_600_000L;

  @Test
  void aTimestampOnTheEpochGridFallsInTheBucketStartingAtOrBeforeIt() {
    assertThat(ContinuousAggregateImpl.bucketStart(10 * HOUR + 5, 20 * HOUR, HOUR)).isEqualTo(10 * HOUR);
    assertThat(ContinuousAggregateImpl.bucketStart(10 * HOUR, 20 * HOUR, HOUR)).isEqualTo(10 * HOUR);
    assertThat(ContinuousAggregateImpl.bucketStart(11 * HOUR - 1, 20 * HOUR, HOUR)).isEqualTo(10 * HOUR);
  }

  @Test
  void theGridIsTheOneTheAnchorIsOn() {
    // buckets of a day starting at 08:00: the anchor is one of their starts
    final long day = 24 * HOUR;
    final long anchor = 100 * day + 8 * HOUR;
    assertThat(ContinuousAggregateImpl.bucketStart(50 * day + 6 * HOUR, anchor, day)).isEqualTo(49 * day + 8 * HOUR);
    assertThat(ContinuousAggregateImpl.bucketStart(50 * day + 8 * HOUR, anchor, day)).isEqualTo(50 * day + 8 * HOUR);
    // a timestamp after the anchor is on the same grid
    assertThat(ContinuousAggregateImpl.bucketStart(anchor + day + 1, anchor, day)).isEqualTo(anchor + day);
  }

  @Test
  void negativeTimestampsAndAnchorsAreFloored() {
    assertThat(ContinuousAggregateImpl.bucketStart(-1, 0, HOUR)).isEqualTo(-HOUR);
    assertThat(ContinuousAggregateImpl.bucketStart(-HOUR, 0, HOUR)).isEqualTo(-HOUR);
    assertThat(ContinuousAggregateImpl.bucketStart(-HOUR - 1, -30 * 60_000L, HOUR)).isEqualTo(-HOUR - 30 * 60_000L);
  }

  @Test
  void anIntervalOfOneIsTheTimestampItself() {
    assertThat(ContinuousAggregateImpl.bucketStart(12_345, 7, 1)).isEqualTo(12_345);
    assertThat(ContinuousAggregateImpl.bucketStart(-12_345, 7, 1)).isEqualTo(-12_345);
  }

  @Test
  void aStartBeforeTheFirstRepresentableInstantIsClampedToIt() {
    // Long.MIN_VALUE is not on the grid anchored at 0 with this interval: its bucket starts before it
    assertThat(Long.MIN_VALUE % HOUR).isNotZero();
    assertThat(ContinuousAggregateImpl.bucketStart(Long.MIN_VALUE, 0, HOUR)).isEqualTo(Long.MIN_VALUE);
    assertThat(ContinuousAggregateImpl.bucketStart(Long.MIN_VALUE + 1, 0, HOUR)).isEqualTo(Long.MIN_VALUE);
  }
}
