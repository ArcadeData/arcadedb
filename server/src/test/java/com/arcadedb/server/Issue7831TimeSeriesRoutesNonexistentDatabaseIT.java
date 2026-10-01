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
package com.arcadedb.server;

import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7831: {@code POST /api/v1/ts/{database}/prom/write} against a database that does not exist answers
 * {@code 404}, and that is the contract, not a regression.
 * <p>
 * Since issue #7681 reparented {@code AbstractBinaryHttpHandler} onto {@code DatabaseAbstractHandler}, the
 * database is resolved (with {@code allowLoad=false}) before {@code PostPrometheusWriteHandler.execute} runs, so
 * the handler's own empty-body {@code 400} is never reached for a database that is not there. The resolution
 * raises {@code DatabaseNotAvailableException}, which {@code AbstractServerHttpHandler} answers {@code 404 Database
 * not found} since issue #6778. That is the same answer every other database-scoped route gives, and it is the
 * accurate one: the body is irrelevant when the target does not exist.
 * <p>
 * What the HA-side regression test ({@code RaftTimeSeriesWriteReadYourWritesIT}) was written to prevent is a
 * {@code 500}; it pinned {@code 400} only because that was the status before #7681. This class pins the actual
 * contract on every {@code /api/v1/ts/{database}/...} route, on a single server, so a later reordering that lets
 * any of them reach an engine call against a missing database and surface a {@code 500} fails here and not only
 * in the HA lane.
 */
class Issue7831TimeSeriesRoutesNonexistentDatabaseIT extends BaseGraphServerTest {

  // Per-request hang detector, not a latency bound.
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
  private static final String BOGUS_DB = "issue7831_database_does_not_exist";

  private final HttpClient client = HttpClient.newHttpClient();

  @Test
  void prometheusWriteWithEmptyBodyOnANonexistentDatabaseAnswers404() throws Exception {
    final HttpResponse<String> response = post("/prom/write", new byte[0], "application/x-protobuf");
    assertThat(response.statusCode()).as("empty body, nonexistent database: " + response.body()).isEqualTo(404);
    assertThat(response.body()).contains("Database not found");
  }

  @Test
  void prometheusWriteWithZeroSeriesOnANonexistentDatabaseAnswers404() throws Exception {
    final byte[] body = Snappy.compress(new WriteRequest(List.of()).encode());
    final HttpResponse<String> response = post("/prom/write", body, "application/x-protobuf");
    assertThat(response.statusCode()).as("zero-series body, nonexistent database: " + response.body()).isEqualTo(404);
  }

  @Test
  void prometheusWriteWithSamplesOnANonexistentDatabaseAnswers404() throws Exception {
    final byte[] body = Snappy.compress(new WriteRequest(List.of(new TimeSeries(
        List.of(new Label("__name__", "issue7831_metric"), new Label("host", "h1")),
        List.of(new Sample(1.0, 1000))))).encode());
    final HttpResponse<String> response = post("/prom/write", body, "application/x-protobuf");
    assertThat(response.statusCode()).as("real samples, nonexistent database: " + response.body()).isEqualTo(404);
  }

  @Test
  void prometheusWriteWithEmptyBodyOnAnExistingDatabaseStillAnswers400() throws Exception {
    // The handler's own empty-body check is still the answer when the database IS there: the 404 above comes
    // from the missing database, not from the handler losing its body validation.
    final HttpResponse<String> response = client.send(HttpRequest.newBuilder(
            URI.create(getServerHttpUrl("/api/v1/ts/" + getDatabaseName() + "/prom/write")))
        .timeout(REQUEST_TIMEOUT)
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/x-protobuf")
        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
        .build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(400);
    assertThat(response.body()).contains("Request body is empty");
  }

  @Test
  void everyTimeSeriesRouteAnswers404ForANonexistentDatabase() throws Exception {
    final byte[] promRead = Snappy.compress(new byte[0]);
    final byte[] lineProtocol = "cpu,host=h1 value=1.0 1000".getBytes(StandardCharsets.UTF_8);
    final byte[] json = "{\"type\":\"cpu\"}".getBytes(StandardCharsets.UTF_8);

    // The bodies only matter if a route ever validates them before resolving the database. Should that happen
    // it answers 400 here and this fails: that is the regression, do not "fix" it by loosening to 4xx.
    // Soft: one regressed route must not hide the state of the others.
    final SoftAssertions soft = new SoftAssertions();
    assertNotFound(soft, post("/prom/read", promRead, "application/x-protobuf"), "POST prom/read");
    assertNotFound(soft, post("/write", lineProtocol, "text/plain"), "POST write");
    assertNotFound(soft, post("/query", json, "application/json"), "POST query");
    assertNotFound(soft, post("/grafana/query", json, "application/json"), "POST grafana/query");
    assertNotFound(soft, get("/latest?type=cpu"), "GET latest");
    assertNotFound(soft, get("/grafana/health"), "GET grafana/health");
    assertNotFound(soft, get("/grafana/metadata"), "GET grafana/metadata");
    assertNotFound(soft, get("/prom/api/v1/query?query=cpu"), "GET prom query");
    assertNotFound(soft, get("/prom/api/v1/query_range?query=cpu&start=0&end=10&step=1"), "GET prom query_range");
    assertNotFound(soft, get("/prom/api/v1/labels"), "GET prom labels");
    assertNotFound(soft, get("/prom/api/v1/label/host/values"), "GET prom label values");
    assertNotFound(soft, get("/prom/api/v1/series?match%5B%5D=cpu"), "GET prom series");

    soft.assertAll();
  }

  private static void assertNotFound(final SoftAssertions soft, final HttpResponse<String> response, final String route) {
    soft.assertThat(response.statusCode()).as(route + " on a nonexistent database: " + response.body()).isEqualTo(404);
  }

  private HttpResponse<String> post(final String suffix, final byte[] body, final String contentType) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl("/api/v1/ts/" + BOGUS_DB + suffix)))
        .timeout(REQUEST_TIMEOUT)
        .header("Authorization", basicAuth())
        .header("Content-Type", contentType)
        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> get(final String suffix) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl("/api/v1/ts/" + BOGUS_DB + suffix)))
        .timeout(REQUEST_TIMEOUT)
        .header("Authorization", basicAuth())
        .GET()
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
