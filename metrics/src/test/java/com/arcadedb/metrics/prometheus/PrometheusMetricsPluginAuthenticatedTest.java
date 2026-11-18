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
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusMetricsPluginAuthenticatedTest extends BaseGraphServerTest {
  private static final String REQUIRE_AUTHENTICATION = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION.getKey();

  private String previousRequireAuthentication;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Restored in endTest(): ContextConfiguration falls back to this system property, so leaving it set changes
    // what every later test in the same JVM reads for the setting (issue #8736).
    previousRequireAuthentication = System.getProperty(REQUIRE_AUTHENTICATION);
    System.setProperty(REQUIRE_AUTHENTICATION, "true");
    GlobalConfiguration.SERVER_PLUGINS.setValue("Prometheus:com.arcadedb.metrics.prometheus.PrometheusMetricsPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      super.endTest();
    } finally {
      if (previousRequireAuthentication != null)
        System.setProperty(REQUIRE_AUTHENTICATION, previousRequireAuthentication);
      else
        System.clearProperty(REQUIRE_AUTHENTICATION);
    }
  }

  @Test
  void prometheusEndpoint() throws Exception {
    // THE CREDENTIALS GO IN THE REQUEST: the 401 of this API carries no challenge (issue #12408), and the JDK's
    // Authenticator needs one to know the scheme to answer with, so it fails the request instead of authenticating it
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl("/prometheus")))
        .header("Authorization", basicCredentials())
        .GET()
        .build();

    HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.headers().firstValue("Content-Type").get()).isEqualTo("text/plain");
    assertThat(response.body()).contains("system_cpu_usage");
  }

  /** A request without credentials is refused with a 401 that carries no challenge (issue #12408). */
  @Test
  void prometheusEndpointWithoutCredentialsIsRefusedWithoutAChallenge() throws Exception {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl("/prometheus")))
        .GET()
        .build();

    HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.headers().firstValue("WWW-Authenticate")).isEmpty();
  }

  private static String basicCredentials() {
    final String pair = "root:" + DEFAULT_PASSWORD_FOR_TESTS;
    return "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8));
  }
}
