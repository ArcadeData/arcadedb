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

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RampWorkloadIT {
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
  void aShortRampFindsAKnee() {
    // 3 s steps after 0.5 s of warm traffic, at most 4 coarse steps (100/s to 195/s): the whole case, 1 warm-up and 3 reps of
    // one ramp each, ends in about a minute. Steps shorter than this make the client_limited rule (1% of the knee step's
    // arrivals more than 1 ms late) trip on a single client GC pause: 10 ms of pause is 1% of a 1 s step. The late-start limit
    // is 5% here: a shared laptop or CI host alone makes 0.3-0.9% of the arrivals late, close to the spec's 1%, which
    // RampWorkloadTest pins; this IT pins the wiring
    final RampWorkload workload = new RampWorkload(Duration.ofSeconds(3), Duration.ofMillis(500), 4, 0.05);
    // the run asks for 2 reps and NO warm-up: the workload's own 3 and 1 must win, or the recorded values below differ
    final List<CaseResult> results = ITSupport.run(workload, server, new JSONObject(), 2, 0);
    assertThat(results).extracting(CaseResult::caseName).containsExactly("knee-point-lookup-cypher");
    final CaseResult r = results.get(0);
    assertThat(r.status()).as(r.error()).isEqualTo(CaseResult.Status.OK);
    // the workload's own repetitions, not the run's 2/0
    assertThat(r.reps()).isEqualTo(3);
    assertThat(r.warmup()).isEqualTo(1);
    assertThat(ITSupport.metric(r, "knee_per_s").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(r, "knee_per_s").values()).hasSize(3);
    assertThat(ITSupport.metric(r, "steps").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(r, "knee_p99_ms").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(r, "client_limited").median()).isZero();
    // 195/s is far below the server's knee (thousands per second): every ramp reaches the ceiling, so the ceiling path is
    // pinned end to end, the knee is the last coarse step and nothing broke
    assertThat(ITSupport.metric(r, "ceiling_reached").values()).containsExactly(1, 1, 1);
    assertThat(ITSupport.metric(r, "knee_per_s").values()).containsExactly(195.3125, 195.3125, 195.3125);
    assertThat(ITSupport.metric(r, "steps").values()).containsExactly(4, 4, 4);
    for (final String flag : List.of("break_drop", "break_p99", "break_errors", "break_unfinished"))
      assertThat(ITSupport.metric(r, flag).median()).as(flag).isZero();
  }
}
