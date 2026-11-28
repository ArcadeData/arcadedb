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

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.util.List;

import static com.arcadedb.test.support.ContainersTestTemplate.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

class TimeSeriesWorkloadIT {
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
  void everyCaseOkWithItsMetrics() {
    final List<CaseResult> results = ITSupport.run(new TimeSeriesWorkload(), server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName)
        .containsExactly("ingest-line-protocol", "ingest-sql", "range-query", "aggregate-query", "aggregate-continuous");
    for (final CaseResult r : results) {
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
      assertThat(ITSupport.metric(r, "latency_p50_ms")).as(r.caseName()).isNotNull();
      assertThat(ITSupport.metric(r, "latency_p95_ms")).as(r.caseName()).isNotNull();
      assertThat(ITSupport.metric(r, "latency_p99_ms")).as(r.caseName()).isNotNull();
    }
    assertThat(ITSupport.metric(results.get(0), "points_per_s").median()).isGreaterThan(0);
    assertThat(ITSupport.metric(results.get(1), "points_per_s").median()).isGreaterThan(0);
    for (int i = 2; i < 5; i++)
      assertThat(ITSupport.metric(results.get(i), "queries_per_s").median()).as(results.get(i).caseName()).isGreaterThan(0);
    assertThat(ITSupport.metric(results.get(4), "ingest_points_per_s_maintained").median()).isGreaterThan(0);
    for (final CaseResult r : results)
      assertThat(ITSupport.metric(r, "peak_rss_bytes")).as(r.caseName()).isNotNull();
    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      final long from = 1_699_999_200_000L;
      try (final ResultSet rs = db.query("sql", "SELECT ts, temperature FROM sensor WHERE sensor_id = ? AND ts BETWEEN ? AND ?", "s-0",
          from, from + 3_600_000L)) {
        assertThat(rs.hasNext()).as("range query rows").isTrue();
      }
      try (final ResultSet rs = db.query("sql", "SELECT hr FROM sensor_hourly WHERE sensor_id = ? AND hr BETWEEN ? AND ?", "s-0", from,
          from + 3_600_000L)) {
        assertThat(rs.hasNext()).as("continuous aggregate rows").isTrue();
      }
    }
  }
}
