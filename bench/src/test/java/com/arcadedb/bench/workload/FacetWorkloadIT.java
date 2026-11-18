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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FacetWorkloadIT {
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
  void bothCasesOkAndLagRecorded() {
    final List<CaseResult> results = ITSupport.run(new FacetWorkload(), server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName).containsExactly("facet-query-after-load", "facet-under-ingest");
    for (final CaseResult r : results) {
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
      assertThat(ITSupport.metric(r, "latency_p99_ms")).as(r.caseName()).isNotNull();
    }
    assertThat(ITSupport.metric(results.get(0), "queries_per_s").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(results.get(1), "ingest_records_per_s").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(results.get(1), "facet_lag_ms")).isNotNull();
    assertThat(ITSupport.metric(results.get(1), "facet_lag_ms").median()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void ingestCaseResetsTheBaselineAfterEveryRepetition() {
    final FacetWorkload workload = new FacetWorkload();
    final WorkloadContext ctx = new WorkloadContext(server, Scale.XS, 42L, new JSONObject());
    try {
      workload.prepare(ctx, "facet-under-ingest");
      assertThat(workload.assetCount(ctx)).isEqualTo(FacetWorkload.records(Scale.XS));
      for (int rep = 0; rep < 2; rep++) {
        workload.measure(ctx, "facet-under-ingest");
        assertThat(workload.assetCount(ctx)).as("count after repetition " + rep).isEqualTo(FacetWorkload.records(Scale.XS));
      }
    } catch (final Exception e) {
      throw new AssertionError(e);
    } finally {
      workload.teardown(ctx);
    }
  }
}
