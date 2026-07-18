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
package com.arcadedb.server.monitor;

import com.arcadedb.server.BaseGraphServerTest;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the always-on HTTP RED timer {@code arcadedb.http.requests} records every request
 * served by the universal handler entry, with a bounded path-template tag (never the raw URI).
 */
class HttpRedMetricsIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void httpRequestsTimerRecordsRequestsWithBoundedPathTag() throws Exception {
    // Issue a request against a database-parameterized route so the path tag would carry the
    // database name unless it is collapsed to a template.
    issueReady();
    issueQuery();

    final Timer anyTimer = Metrics.globalRegistry.find("arcadedb.http.requests").timer();
    assertThat(anyTimer).isNotNull();

    // The query route must be tagged with the template, not the concrete database name "graph".
    // Every tag of the tuple this test drove is pinned: the ITs share one reused fork, so meters from
    // earlier test classes are still registered under this name and a path-only lookup returns an
    // arbitrary one of them. Those stale meters record nothing for this test, so asserting a count on
    // one is a false failure that depends purely on test ordering.
    final Timer queryTimer = Metrics.globalRegistry.find("arcadedb.http.requests")
        .tag("path", "/query/{database}").tag("db", "graph").tag("method", "POST").tag("status", "200").timer();
    assertThat(queryTimer).isNotNull();
    assertThat(queryTimer.count()).isGreaterThanOrEqualTo(1L);

    // Cardinality guard: no timer should carry the raw database name in its path tag.
    final Timer rawPathTimer = Metrics.globalRegistry.find("arcadedb.http.requests").tag("path", "/query/graph").timer();
    assertThat(rawPathTimer).isNull();
  }

  @Test
  void httpRequestsTimerRecordsErrorStatus() throws Exception {
    // An unauthenticated request to a protected endpoint returns 401, exercising the error path.
    final HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:2480/api/v1/command/graph").openConnection();
    c.setRequestMethod("POST");
    c.setDoOutput(true);
    c.setRequestProperty("Content-Type", "application/json");
    c.getOutputStream().write("{\"language\":\"sql\",\"command\":\"select 1\"}".getBytes(StandardCharsets.UTF_8));
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(401);
    c.disconnect();

    // RED metrics must record errors too, tagged with the failing status code.
    final Timer errorTimer = Metrics.globalRegistry.find("arcadedb.http.requests")
        .tag("path", "/command/{database}").tag("status", "401").timer();
    assertThat(errorTimer).isNotNull();
    assertThat(errorTimer.count()).isGreaterThanOrEqualTo(1L);
  }

  @Test
  void unmatchedUrisCollapseToBoundedPathTag() throws Exception {
    // The Studio "/" fallback route handled every distinct, client-controlled URI by tagging the RED
    // timer with the raw path. Each unique path registered a permanent percentile-histogram Timer that
    // was never evicted: an unbounded, client-driven heap leak (issue #5025).
    for (int i = 0; i < 50; i++)
      hitGet("/unmatched/" + i);

    // No timer may carry a raw client URI in its path tag.
    final long rawUnmatchedMeters = Metrics.globalRegistry.find("arcadedb.http.requests").timers().stream()
        .map(t -> t.getId().getTag("path"))
        .filter(p -> p != null && p.startsWith("/unmatched"))
        .count();
    assertThat(rawUnmatchedMeters).isZero();

    // All unmatched traffic collapses onto a single bounded path tag.
    final Timer collapsed = Metrics.globalRegistry.find("arcadedb.http.requests").tag("path", "unmatched").timer();
    assertThat(collapsed).isNotNull();
    assertThat(collapsed.count()).isGreaterThanOrEqualTo(50L);
  }

  private void hitGet(final String path) throws Exception {
    final HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:2480" + path).openConnection();
    c.setRequestMethod("GET");
    c.connect();
    c.getResponseCode();
    c.disconnect();
  }

  private void issueReady() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:2480/api/v1/ready").openConnection();
    connection.setRequestMethod("GET");
    connection.connect();
    connection.getResponseCode();
    connection.disconnect();
  }

  private void issueQuery() throws Exception {
    final HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:2480/api/v1/query/graph").openConnection();
    c.setRequestMethod("POST");
    c.setDoOutput(true);
    c.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    c.setRequestProperty("Content-Type", "application/json");
    c.getOutputStream().write("{\"language\":\"sql\",\"command\":\"select 1 as one\"}".getBytes(StandardCharsets.UTF_8));
    c.connect();
    c.getResponseCode();
    c.disconnect();
  }
}
