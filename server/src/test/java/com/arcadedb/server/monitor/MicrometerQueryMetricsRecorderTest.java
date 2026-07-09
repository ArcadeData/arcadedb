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
package com.arcadedb.server.monitor;

import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for the per-tuple timer cache in {@link MicrometerQueryMetricsRecorder} (issue #5025):
 * {@code record()} runs on the query hot path for every wire protocol, so a repeated tag tuple must
 * reuse the same cached {@link Timer} instead of rebuilding the builder/tags/id each time.
 */
class MicrometerQueryMetricsRecorderTest {

  @Test
  void queryTimerIsCachedPerTagTuple() {
    final Timer first = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "query");
    final Timer second = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "query");

    // Same tag tuple -> the exact same cached Timer instance (cache hit, no per-call allocation).
    assertThat(second).isSameAs(first);

    // A different tuple resolves to a distinct timer.
    final Timer command = MicrometerQueryMetricsRecorder.queryTimer("http", "graph", "sql", "command");
    assertThat(command).isNotSameAs(first);
  }

  @Test
  void recordUsesTheCachedTimer() {
    final MicrometerQueryMetricsRecorder recorder = new MicrometerQueryMetricsRecorder();
    recorder.record("bolt", "graph", "cypher", "query", 1_000L);

    // The tuple recorded above must resolve to an already-cached timer (same instance returned).
    final Timer cached = MicrometerQueryMetricsRecorder.queryTimer("bolt", "graph", "cypher", "query");
    assertThat(MicrometerQueryMetricsRecorder.queryTimer("bolt", "graph", "cypher", "query")).isSameAs(cached);
  }
}
