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
import com.arcadedb.bench.workload.issues.ExponentScenario;
import com.arcadedb.bench.workload.issues.IssueScenario;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IssuesWorkloadIT {
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
    final IssuesWorkload workload = new IssuesWorkload();
    final List<CaseResult> results = ITSupport.run(workload, server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName).containsExactlyElementsOf(workload.cases());
    final List<IssueScenario> scenarios = IssuesWorkload.scenarios();
    for (int i = 0; i < results.size(); i++) {
      final CaseResult r = results.get(i);
      // an ok case passed its checkForms in prepare: a failed check fails the case before any repetition
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
      if (scenarios.get(i) instanceof ExponentScenario) {
        final Metric exponent = ITSupport.metric(r, "exponent");
        assertThat(exponent).as(r.caseName()).isNotNull();
        assertThat(Double.isFinite(exponent.median())).as(r.caseName() + " exponent " + exponent.median()).isTrue();
        assertThat(ITSupport.metric(r, "size_3_ms").median()).as(r.caseName()).isGreaterThan(0);
      } else {
        final Metric factor = ITSupport.metric(r, "factor");
        assertThat(factor).as(r.caseName()).isNotNull();
        assertThat(factor.median()).as(r.caseName()).isGreaterThan(0);
        assertThat(ITSupport.metric(r, "slow_ms").median()).as(r.caseName()).isGreaterThan(0);
        assertThat(ITSupport.metric(r, "reference_ms").median()).as(r.caseName()).isGreaterThan(0);
      }
    }
  }

  @Test
  void aSecondPrepareKeepsTheDatasetAndChecksTheFormsAgain() {
    final IssuesWorkload workload = new IssuesWorkload();
    final WorkloadContext ctx = new WorkloadContext(server, Scale.XS, 42L, new JSONObject());
    workload.prepare(ctx, "issue-12281");
    final long before = rows(workload);
    workload.prepare(ctx, "issue-12281");
    assertThat(rows(workload)).isEqualTo(before).isEqualTo(2_000L);
  }

  private static long rows(final IssuesWorkload workload) {
    try (final RemoteDatabase db = RemoteClients.open(server, "bench_issue_12281")) {
      return RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM T");
    }
  }
}
