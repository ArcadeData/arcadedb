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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class VectorWorkloadIT {
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
  void recallIsAlwaysReported() {
    final List<CaseResult> results = ITSupport.run(new VectorWorkload(), server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName).containsExactly("build", "knn-k10");
    final CaseResult build = results.get(0);
    assertThat(build.status()).as("build: " + build.error()).isEqualTo(CaseResult.Status.OK);
    assertThat(ITSupport.metric(build, "build_s")).isNotNull();
    assertThat(ITSupport.metric(build, "disk_bytes")).isNotNull();
    assertThat(ITSupport.metric(build, "peak_rss_bytes")).isNotNull();

    final CaseResult knn = results.get(1);
    assertThat(knn.status()).as("knn-k10: " + knn.error()).isEqualTo(CaseResult.Status.OK);
    final Metric recall = ITSupport.metric(knn, "recall_at_10");
    assertThat(recall).isNotNull();
    assertThat(recall.median()).isGreaterThanOrEqualTo(VectorWorkload.MIN_RECALL);
    assertThat(ITSupport.metric(knn, "queries_per_s").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(knn, "latency_p95_ms")).isNotNull();
  }

  @Test
  void lowRecallFailsCase() {
    final List<CaseResult> results = ITSupport.run(new VectorWorkload(), server, new JSONObject().put("efSearch", 1));
    final CaseResult knn = results.stream().filter(r -> r.caseName().equals("knn-k10")).findFirst().orElseThrow();
    assertThat(knn.status()).isEqualTo(CaseResult.Status.FAILED);
    assertThat(knn.error()).contains("recall");
    assertThat(ITSupport.metric(knn, "recall_at_10")).isNotNull();
  }
}
