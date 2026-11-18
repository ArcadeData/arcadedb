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

class GraphOltpWorkloadIT {
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
    final List<CaseResult> results = ITSupport.run(new GraphOltpWorkload(), server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName)
        .containsExactly("point-lookup-cypher", "point-lookup-sql", "hop1-cypher", "hop1-sql", "hop2-cypher", "hop2-sql", "hop3-cypher",
            "hop3-sql", "pattern-like-cypher", "pattern-like-sql");
    for (final CaseResult r : results) {
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
      assertThat(ITSupport.metric(r, "queries_per_s").median()).isGreaterThan(0);
      assertThat(ITSupport.metric(r, "latency_p50_ms")).isNotNull();
      assertThat(ITSupport.metric(r, "latency_p95_ms")).isNotNull();
      assertThat(ITSupport.metric(r, "latency_p99_ms")).isNotNull();
    }
  }

  @Test
  void cypherAndSqlAgree() {
    final GraphOltpWorkload workload = new GraphOltpWorkload();
    final WorkloadContext ctx = new WorkloadContext(server, Scale.XS, 42L, new JSONObject());
    workload.prepare(ctx, "hop2-cypher");
    try {
      // hop3 is left out: Cypher applies relationship uniqueness to a path (a trail), SQL out().out().out() does not, so
      // the hop3 counts may differ slightly
      for (final String base : List.of("hop1", "hop2", "pattern-like")) {
        final int[] cypher = workload.distinctCounts(ctx, base + "-cypher", 10);
        final int[] sql = workload.distinctCounts(ctx, base + "-sql", 10);
        assertThat(cypher).as(base).hasSize(10);
        assertThat(cypher).as(base).isEqualTo(sql);
        // two engines agreeing on all zeros would prove nothing
        assertThat(Arrays.stream(cypher).sum()).as(base + " counts summed over the sample ids").isPositive();
      }
    } finally {
      workload.teardown(ctx);
    }
  }
}
