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

import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.mixed.MixedPhase;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("slow")
class MixedWorkloadIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(MixedWorkloadIT.class);

  private static ServerUnderTest server;

  @BeforeAll
  static void start() {
    server = ITSupport.startServer();
  }

  @AfterAll
  static void stop() {
    if (server != null)
      server.close();
  }

  @Test
  void aRepetitionRunsAllSixLoopsAndPassesItsCheck() throws Exception {
    final WorkloadContext ctx = new WorkloadContext(server, Scale.XS, 42L, new JSONObject());
    MixedWorkload.prepareDatabase(ctx);
    final MixedPhase phase = new MixedPhase(server, ctx.seed(), ctx.scale());
    final MixedPhase.Result result = phase.run(MixedWorkload.window(Scale.XS));

    for (final Map.Entry<String, LatencyRecorder> e : result.latencies().entrySet())
      LOGGER.info("mixed IT loop {}: {} operations, p99 {} ms", e.getKey(), e.getValue().count(), e.getValue().percentile(99) / 1e6);
    LOGGER.info("mixed IT: {} operations in {} s, longest commit gap {} ms, {} documents, {} points", result.operations(),
        result.seconds(), result.longestCommitGapNanos() / 1e6, phase.expectedDocs(), phase.expectedPoints());

    assertThat(result.failures()).isEmpty();
    assertThat(result.latencies().keySet()).containsExactlyElementsOf(MixedPhase.LOOPS);
    for (final Map.Entry<String, LatencyRecorder> e : result.latencies().entrySet())
      assertThat(e.getValue().count()).as(e.getKey()).isPositive();
    // something was ingested on both kinds of data, so the check below compares real counts
    assertThat(phase.expectedDocs()).isGreaterThan(MixedWorkload.baseDocs(Scale.XS));
    assertThat(phase.expectedPoints()).isPositive();
    phase.check();
  }
}
