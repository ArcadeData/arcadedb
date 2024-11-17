package com.arcadedb.metrics.prometheus;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

class PrometheusMetricsPluginNotAuthenticatedTest extends BaseGraphServerTest {
  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    System.setProperty("arcadedb.serverMetrics.prometheus.requireAuthentication", "false");
    GlobalConfiguration.SERVER_PLUGINS.setValue("Prometheus:com.arcadedb.metrics.prometheus.PrometheusMetricsPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void testPrometheusEndpointWithoutAuth() throws IOException, InterruptedException {
    HttpClient client = HttpClient.newBuilder()
        .build();

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:2480/prometheus"))
        .GET()
        .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("system_cpu_usage");
  }
}
