/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotDownloadProgressMeterTest {

  @Test
  void firstCallNeverLogsAndEstablishesBaseline() {
    final SnapshotDownloadProgressMeter meter = new SnapshotDownloadProgressMeter("foo", 5000L);
    assertThat(meter.lineIfDue(0L, 0L)).isNull();
  }

  @Test
  void logsOnlyAfterTheInterval() {
    final SnapshotDownloadProgressMeter meter = new SnapshotDownloadProgressMeter("foo", 5000L);
    meter.lineIfDue(0L, 0L);                       // baseline
    assertThat(meter.lineIfDue(10_000_000L, 1000L)).isNull();    // too soon
    final String line = meter.lineIfDue(40_000_000L, 6000L);     // interval elapsed
    assertThat(line).isNotNull();
    assertThat(line).contains("db=foo").contains("MB").contains("MB/s");
  }

  @Test
  void reThrottlesAfterEachEmittedLine() {
    final SnapshotDownloadProgressMeter meter = new SnapshotDownloadProgressMeter("foo", 5000L);
    meter.lineIfDue(0L, 0L);
    assertThat(meter.lineIfDue(40_000_000L, 6000L)).isNotNull();
    assertThat(meter.lineIfDue(50_000_000L, 7000L)).isNull();    // within interval of last emit
    assertThat(meter.lineIfDue(80_000_000L, 12_000L)).isNotNull();
  }
}
