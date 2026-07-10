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
package com.arcadedb.metrics.prometheus;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Retro-compatibility guard: {@code /prometheus} still serves the pre-existing series and now also
 * renders the additive {@link com.arcadedb.server.monitor.EngineMetricsBinder} engine gauges.
 */
class PrometheusEngineMetricsIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    System.setProperty("arcadedb.serverMetrics.prometheus.requireAuthentication", "false");
    GlobalConfiguration.SERVER_PLUGINS.setValue("PrometheusMetricsPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    System.clearProperty("arcadedb.serverMetrics.prometheus.requireAuthentication");
    super.endTest();
  }

  @Test
  void prometheusStillServesAndExposesEngineGauges() throws Exception {
    // Exercise the engine so non-zero engine counters exist.
    getServerDatabase(0, getDatabaseName()).query("sql", "select 1 as one");

    final HttpClient client = HttpClient.newHttpClient();
    // Warm-up HTTP request so the always-on arcadedb.http.requests timer is registered before scraping.
    client.send(HttpRequest.newBuilder().uri(URI.create("http://localhost:2480/api/v1/ready")).GET().build(),
        HttpResponse.BodyHandlers.ofString());

    final HttpResponse<String> response = client.send(
        HttpRequest.newBuilder().uri(URI.create("http://localhost:2480/prometheus")).GET().build(),
        HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    // Pre-existing series still present (retro-compat):
    assertThat(response.body()).contains("system_cpu_usage");
    // New engine gauge present:
    assertThat(response.body()).contains("arcadedb_engine_page_cache_hits");
    // New RED timer series present (HTTP request timer is always-on):
    assertThat(response.body()).contains("arcadedb_http_requests");
  }
}
