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

class IngestionWorkloadIT {
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
  void allCasesOkAtXS() {
    final List<CaseResult> results = ITSupport.run(new IngestionWorkload(), server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName).containsExactly("bulk-import-csv", "tx-insert-1t", "tx-insert-8t");
    for (final CaseResult r : results) {
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
      final Metric rate = ITSupport.metric(r, "records_per_s");
      assertThat(rate).isNotNull();
      assertThat(rate.median()).isGreaterThan(0);
    }
    final CaseResult bulk = results.get(0);
    assertThat(ITSupport.metric(bulk, "total_s")).isNotNull();
    assertThat(ITSupport.metric(bulk, "disk_bytes").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(bulk, "peak_rss_bytes").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(results.get(1), "latency_p99_ms")).isNotNull();
  }
}
