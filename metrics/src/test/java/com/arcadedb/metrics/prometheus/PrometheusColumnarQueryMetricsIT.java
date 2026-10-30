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
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** The FACETS query counters ({@code arcadedb.columnar.query.*}) render on {@code /prometheus} with their database tag and move with the queries. */
class PrometheusColumnarQueryMetricsIT extends BaseGraphServerTest {
  private static final List<String> NAMES = List.of("patched", "budget_waits", "patch_overflows", "fallback_scans", "filter_fallbacks");

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

  private String scrape(final HttpClient client) throws Exception {
    final HttpResponse<String> response = client.send(
        HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl(0, "/prometheus"))).GET().build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return response.body();
  }

  @Test
  void prometheusExposesTaggedCounters() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    db.command("sql", "CREATE PROPERTY Asset.brand STRING");
    db.command("sql", "CREATE PROPERTY Asset.author STRING");
    db.transaction(() -> {
      for (int i = 0; i < 50; i++)
        db.newDocument("Asset").set("brand", "b" + i % 5).set("author", "a" + i % 7).save();
    });
    db.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    // author has no COLUMNAR index: its facet is counted by a fallback scan
    try (final ResultSet rs = db.query("sql", "SELECT FROM Asset FACETS author TERMS")) {
      assertThat(rs.getFacets().get("author").source()).isEqualTo("scan");
    }

    final HttpClient client = HttpClient.newHttpClient();
    await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
      final String body = scrape(client);
      for (final String name : NAMES)
        assertThat(body).as(name).contains("arcadedb_columnar_query_" + name + "_total{database=\"" + getDatabaseName() + "\"");
      // the scan is counted under its reason; every reason has its series, the others at 0
      assertThat(body).contains("arcadedb_columnar_query_fallback_scans_total{database=\"" + getDatabaseName() + "\",reason=\"no_index\"} 1.0");
      assertThat(body).contains("arcadedb_columnar_query_fallback_scans_total{database=\"" + getDatabaseName() + "\",reason=\"not_published\"} 0.0");
      assertThat(body).containsPattern("arcadedb_columnar_query_patched_total\\{database=\"" + getDatabaseName() + "\"[^}]*\\} 0\\.0");
      for (final String name : NAMES)
        assertThat(Metrics.globalRegistry.find("arcadedb.columnar.query." + name.replace('_', '.')).tag("database", getDatabaseName())
            .functionCounter()).as(name).isNotNull();
      final FunctionCounter patched = Metrics.globalRegistry.find("arcadedb.columnar.query.patched").tag("database", getDatabaseName())
          .functionCounter();
      assertThat(patched).isNotNull();
      assertThat(patched.count()).isZero();
    });
  }
}
