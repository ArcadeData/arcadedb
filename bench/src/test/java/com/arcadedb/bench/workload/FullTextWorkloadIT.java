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
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FullTextWorkloadIT {
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
  void allCasesOk() {
    final List<CaseResult> results = ITSupport.run(new FullTextWorkload(), server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName)
        .containsExactly("build", "bm25-single-term", "bm25-multi-term", "bm25-phrase", "hybrid", "bm25-broad-or",
            "bm25-broad-or-filtered");
    for (final CaseResult r : results)
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
    assertThat(ITSupport.metric(results.get(0), "build_s")).isNotNull();
    assertThat(ITSupport.metric(results.get(0), "disk_bytes").median()).isGreaterThan(0);
    for (final CaseResult r : results.subList(1, results.size())) {
      assertThat(ITSupport.metric(r, "queries_per_s").median()).as(r.caseName()).isGreaterThan(0);
      assertThat(ITSupport.metric(r, "latency_p99_ms")).as(r.caseName()).isNotNull();
    }
  }

  @Test
  void queriesReturnHits() {
    final FullTextWorkload workload = new FullTextWorkload();
    final WorkloadContext ctx = new WorkloadContext(server, Scale.XS, 42L, new JSONObject());
    workload.prepare(ctx, "bm25-single-term");
    try {
      final int[] hits = workload.hitCounts(ctx, "bm25-single-term", 50);
      assertThat(hits).hasSize(50);
      final long withHits = Arrays.stream(hits).filter(h -> h >= 1).count();
      assertThat(withHits).as("queries with at least one hit out of 50").isGreaterThanOrEqualTo(45);
    } finally {
      workload.teardown(ctx);
    }
  }

  @Test
  void broadQueriesReturnFiftyRows() {
    for (final String caseName : List.of("bm25-broad-or", "bm25-broad-or-filtered")) {
      final FullTextWorkload workload = new FullTextWorkload();
      final WorkloadContext ctx = new WorkloadContext(server, Scale.XS, 42L, new JSONObject());
      workload.prepare(ctx, caseName);
      try {
        // hitCounts fails on a short answer or on a filtered row with deleted_at set
        assertThat(workload.hitCounts(ctx, caseName, 20)).as(caseName).containsOnly(50);
        // the plain case must see rows with deleted_at set, or the filtered guard proves nothing
        if ("bm25-broad-or".equals(caseName))
          assertThat(workload.deletedRowsSeen()).as("plain rows with deleted_at set").isGreaterThan(0);
      } finally {
        workload.teardown(ctx);
      }
    }
  }
}
