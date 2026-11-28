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
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

@Tag("slow")
class HybridRagWorkloadIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(HybridRagWorkloadIT.class);

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

  // every query shape is accepted by the real server, every row of every query passes the visibility check (a leak fails the
  // case), and the vector cases report a recall over the floor
  @Test
  void everyCaseRunsAndReturnsOnlyVisibleRows() {
    final List<CaseResult> results = ITSupport.run(new HybridRagWorkload(), server, new JSONObject(), 3, 0);
    assertThat(results).extracting(CaseResult::caseName)
        .containsExactly("vector-k100-filtered", "fulltext-filtered", "hop2-filtered", "rag-pipeline");
    for (final CaseResult result : results) {
      for (final Metric m : result.metrics())
        LOGGER.info("{} {} = {} {}", result.caseName(), m.name(), Arrays.toString(m.values()), m.unit());
      assertThat(result.status()).as(result.caseName() + ": " + result.error()).isEqualTo(CaseResult.Status.OK);
      assertThat(ITSupport.metric(result, "latency_p99_ms")).as(result.caseName()).isNotNull();
    }
    for (final String vectorCase : List.of("vector-k100-filtered", "rag-pipeline")) {
      final CaseResult result = results.stream().filter(r -> r.caseName().equals(vectorCase)).findFirst().orElseThrow();
      final Metric recall = ITSupport.metric(result, "recall_at_100");
      assertThat(recall).as(vectorCase).isNotNull();
      assertThat(recall.median()).as(vectorCase).isGreaterThan(0).isGreaterThanOrEqualTo(HybridRagWorkload.MIN_RECALL_K100);
    }
    assertThat(ITSupport.metric(results.get(0), "queries_per_s").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(results.get(3), "pipelines_per_s").median()).isGreaterThan(0);
  }

  // the check can fail on the real data: the same rows without the server-side filter hold restricted chunks and other
  // tenants' private ones, and the check stops at the first
  @Test
  void theCheckCatchesTheRowsTheFilterRemoves() {
    final HybridRagWorkload workload = new HybridRagWorkload();
    workload.prepare(new WorkloadContext(server, Scale.XS, 42L, new JSONObject()), "vector-k100-filtered");
    try (final RemoteDatabase db = RemoteClients.open(server, HybridRagWorkload.DATABASE)) {
      final IllegalStateException e = catchThrowableOfType(IllegalStateException.class,
          () -> HybridRagWorkload.run(db, "unfiltered", 3, "sql", "SELECT id, tenant, visibility FROM Chunk LIMIT 100", Map.of()));
      assertThat(e).isNotNull();
      assertThat(e.getMessage()).startsWith("unfiltered: tenant 3 received chunk");
      // and the filtered form of the same scan passes
      assertThat(HybridRagWorkload.run(db, "filtered", 3, "sql", "SELECT id, tenant, visibility FROM Chunk WHERE "
          + "(visibility = 'public' OR (visibility = 'private' AND tenant = :t)) LIMIT 100", Map.of("t", 3))).hasSize(100);
    }
  }
}
