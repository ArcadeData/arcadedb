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
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** The columnar meters of the server's {@code ColumnarMetrics} render on {@code /prometheus} with their database tag. */
class PrometheusColumnarMetricsIT extends BaseGraphServerTest {

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
  void prometheusEndpointExposesColumnarMeters() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    db.command("sql", "CREATE PROPERTY Asset.brand STRING");
    db.transaction(() -> {
      for (int i = 0; i < 50; i++)
        db.newDocument("Asset").set("brand", "b" + i % 5).save();
    });
    db.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    // the binder registers a database's series on its 5 s refresh
    final HttpClient client = HttpClient.newHttpClient();
    await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
      final HttpResponse<String> response = client.send(
          HttpRequest.newBuilder().uri(URI.create(getServerHttpUrl(0, "/prometheus"))).GET().build(),
          HttpResponse.BodyHandlers.ofString());

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.body()).contains("# TYPE arcadedb_columnar_rebuilds_total counter");
      assertThat(response.body()).contains("arcadedb_columnar_rebuilds_total{");
      assertThat(response.body()).contains("database=\"" + getDatabaseName() + "\"");
      assertThat(response.body()).contains("reason=\"create\"");
      assertThat(response.body()).contains("arcadedb_columnar_cache_used_bytes{database=\"*\"");
    });
  }
}
