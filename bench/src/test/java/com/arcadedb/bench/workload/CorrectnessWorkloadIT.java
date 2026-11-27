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
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CorrectnessWorkloadIT {
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
  void everyCaseChecksItsAnswerAndReportsItsDuration() {
    final List<CaseResult> results = ITSupport.run(new CorrectnessWorkload(), server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName)
        .containsExactly("vector-recall-after-churn", "index-vs-scan-count", "count-star-vs-iterated");
    for (final CaseResult result : results) {
      assertThat(result.status()).as(result.caseName() + ": " + result.error()).isEqualTo(CaseResult.Status.OK);
      final Metric check = ITSupport.metric(result, "check_s");
      assertThat(check).as(result.caseName()).isNotNull();
      assertThat(check.median()).isGreaterThan(0);
      assertThat(ITSupport.metric(result, "peak_rss_bytes")).as(result.caseName()).isNotNull();
    }
    final Metric recall = ITSupport.metric(results.get(0), "recall_at_10");
    assertThat(recall).isNotNull();
    assertThat(recall.median()).isGreaterThanOrEqualTo(VectorWorkload.MIN_RECALL);
  }

  // the premise of index-vs-scan: v is answered through its index, v_copy by a scan; with no index on v the check must fail
  @Test
  void planCheckSeesTheIndexAndFailsWithoutIt() {
    RemoteClients.ensureDatabase(server, "bench_plan_check");
    try (final RemoteDatabase db = RemoteClients.open(server, "bench_plan_check")) {
      db.command("sql", "CREATE DOCUMENT TYPE Item").close();
      db.command("sql", "CREATE PROPERTY Item.v LONG").close();
      db.command("sql", "CREATE PROPERTY Item.v_copy LONG").close();
      assertThatThrownBy(() -> CorrectnessWorkload.checkPlans(server, "bench_plan_check")).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("does not use its index");
      db.command("sql", "CREATE INDEX ON Item (v) NOTUNIQUE").close();
      assertThatCode(() -> CorrectnessWorkload.checkPlans(server, "bench_plan_check")).doesNotThrowAnyException();
      db.command("sql", "CREATE INDEX ON Item (v_copy) NOTUNIQUE").close();
      assertThatThrownBy(() -> CorrectnessWorkload.checkPlans(server, "bench_plan_check")).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("is not a scan");
    }
  }
}
