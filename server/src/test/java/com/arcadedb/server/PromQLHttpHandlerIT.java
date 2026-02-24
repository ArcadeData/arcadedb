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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for PromQL HTTP API endpoints.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PromQLHttpHandlerIT extends BaseGraphServerTest {

  private void ingestTestData(final int serverIndex) throws Exception {
    // Metric: prom_cpu with host label
    postPromWrite(serverIndex, new WriteRequest(List.of(
        new TimeSeries(
            List.of(new Label("__name__", "prom_cpu"), new Label("host", "server1")),
            List.of(new Sample(10.0, 1000), new Sample(20.0, 2000), new Sample(30.0, 3000),
                new Sample(40.0, 4000), new Sample(50.0, 5000))
        ),
        new TimeSeries(
            List.of(new Label("__name__", "prom_cpu"), new Label("host", "server2")),
            List.of(new Sample(60.0, 1000), new Sample(70.0, 2000), new Sample(80.0, 3000))
        )
    )));

    // Metric: prom_http_total (counter-like) with method label
    postPromWrite(serverIndex, new WriteRequest(List.of(
        new TimeSeries(
            List.of(new Label("__name__", "prom_http_total"), new Label("method", "GET")),
            List.of(new Sample(100.0, 1000), new Sample(150.0, 2000), new Sample(200.0, 3000),
                new Sample(250.0, 4000), new Sample(300.0, 5000))
        )
    )));
  }

  @Test
  void testInstantQuery() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "query", "query=" + encode("prom_cpu") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONObject data = result.getJSONObject("data");
      assertThat(data.getString("resultType")).isEqualTo("vector");
      final JSONArray resultArr = data.getJSONArray("result");
      assertThat(resultArr.length()).isGreaterThan(0);
    });
  }

  @Test
  void testInstantQueryWithLabelFilter() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("prom_cpu{host=\"server1\"}") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray resultArr = result.getJSONObject("data").getJSONArray("result");
      assertThat(resultArr.length()).isEqualTo(1);
      assertThat(resultArr.getJSONObject(0).getJSONObject("metric").getString("host")).isEqualTo("server1");
    });
  }

  @Test
  void testRangeQuery() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "query_range",
          "query=" + encode("prom_cpu") + "&start=1&end=5&step=1");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONObject data = result.getJSONObject("data");
      assertThat(data.getString("resultType")).isEqualTo("matrix");
      final JSONArray resultArr = data.getJSONArray("result");
      assertThat(resultArr.length()).isGreaterThan(0);
      // Each series should have "values" array
      assertThat(resultArr.getJSONObject(0).has("values")).isTrue();
    });
  }

  @Test
  void testRangeQueryWithRate() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "query_range",
          "query=" + encode("rate(prom_http_total[5s])") + "&start=1&end=5&step=1");
      assertThat(result.getString("status")).isEqualTo("success");
      assertThat(result.getJSONObject("data").getString("resultType")).isEqualTo("matrix");
    });
  }

  @Test
  void testLabels() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "labels", "");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray data = result.getJSONArray("data");
      boolean hasName = false;
      for (int i = 0; i < data.length(); i++)
        if ("__name__".equals(data.getString(i)))
          hasName = true;
      assertThat(hasName).isTrue();
    });
  }

  @Test
  void testLabelValuesForName() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQLLabelValues(serverIndex, "__name__");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray data = result.getJSONArray("data");
      assertThat(data.length()).isGreaterThanOrEqualTo(2);
      boolean hasCpu = false;
      boolean hasHttp = false;
      for (int i = 0; i < data.length(); i++) {
        if ("prom_cpu".equals(data.getString(i)))
          hasCpu = true;
        if ("prom_http_total".equals(data.getString(i)))
          hasHttp = true;
      }
      assertThat(hasCpu).isTrue();
      assertThat(hasHttp).isTrue();
    });
  }

  @Test
  void testLabelValuesForHost() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQLLabelValues(serverIndex, "host");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray data = result.getJSONArray("data");
      assertThat(data.length()).isGreaterThanOrEqualTo(2);
    });
  }

  @Test
  void testSeries() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "series",
          "match[]=" + encode("prom_cpu") + "&start=0&end=10");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray data = result.getJSONArray("data");
      assertThat(data.length()).isGreaterThan(0);
      assertThat(data.getJSONObject(0).getString("__name__")).isEqualTo("prom_cpu");
    });
  }

  @Test
  void testErrorMissingQuery() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject result = getPromQL(serverIndex, "query", "time=5");
      assertThat(result.getString("status")).isEqualTo("error");
      assertThat(result.getString("errorType")).isEqualTo("bad_data");
    });
  }

  @Test
  void testErrorNonexistentMetric() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("nonexistent_metric") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      assertThat(result.getJSONObject("data").getJSONArray("result").length()).isEqualTo(0);
    });
  }

  @Test
  void testErrorMalformedPromQL() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("sum(") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("error");
    });
  }

  @Test
  void testBinaryExpression() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("prom_cpu * 2") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray resultArr = result.getJSONObject("data").getJSONArray("result");
      assertThat(resultArr.length()).isGreaterThan(0);
    });
  }

  @Test
  void testAggregationSum() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("sum(prom_cpu)") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray resultArr = result.getJSONObject("data").getJSONArray("result");
      assertThat(resultArr.length()).isEqualTo(1);
    });
  }

  @Test
  void testEmptyResultSet() throws Exception {
    testEachServer((serverIndex) -> {
      // Query a metric that has no data ingested
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("completely_nonexistent_metric") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray resultArr = result.getJSONObject("data").getJSONArray("result");
      assertThat(resultArr.length()).isEqualTo(0);
    });
  }

  @Test
  void testTopkWithKGreaterThanSeriesCount() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      // prom_cpu has 2 series (server1, server2); ask for topk(10) — should return all 2
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("topk(10, prom_cpu)") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray resultArr = result.getJSONObject("data").getJSONArray("result");
      // Should return all available series (2), not error
      assertThat(resultArr.length()).isLessThanOrEqualTo(2);
      assertThat(resultArr.length()).isGreaterThan(0);
    });
  }

  @Test
  void testBottomkWithKGreaterThanSeriesCount() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      // Ask for bottomk(100) — should return all available series
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("bottomk(100, prom_cpu)") + "&time=5");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONArray resultArr = result.getJSONObject("data").getJSONArray("result");
      assertThat(resultArr.length()).isGreaterThan(0);
    });
  }

  @Test
  void testRangeQueryEmptyResult() throws Exception {
    testEachServer((serverIndex) -> {
      // Range query on nonexistent metric
      final JSONObject result = getPromQL(serverIndex, "query_range",
          "query=" + encode("nonexistent_range_metric") + "&start=1&end=5&step=1");
      assertThat(result.getString("status")).isEqualTo("success");
      final JSONObject data = result.getJSONObject("data");
      assertThat(data.getString("resultType")).isEqualTo("matrix");
      assertThat(data.getJSONArray("result").length()).isEqualTo(0);
    });
  }

  @Test
  void testLookbackDeltaParameter() throws Exception {
    testEachServer((serverIndex) -> {
      ingestTestData(serverIndex);
      // Query with a custom lookback_delta
      final JSONObject result = getPromQL(serverIndex, "query",
          "query=" + encode("prom_cpu") + "&time=5&lookback_delta=10m");
      assertThat(result.getString("status")).isEqualTo("success");
      assertThat(result.getJSONObject("data").getJSONArray("result").length()).isGreaterThan(0);
    });
  }

  // --- Helpers ---

  private JSONObject getPromQL(final int serverIndex, final String endpoint, final String queryString) throws Exception {
    final String separator = queryString.isEmpty() ? "" : "?";
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/prom/api/v1/" + endpoint + separator + queryString)
        .toURL()
        .openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final int responseCode = connection.getResponseCode();
    final InputStream is = responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream();
    final String body = readResponse(is);
    return new JSONObject(body);
  }

  private JSONObject getPromQLLabelValues(final int serverIndex, final String labelName) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/prom/api/v1/label/" + labelName + "/values")
        .toURL()
        .openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final int responseCode = connection.getResponseCode();
    final InputStream is = responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream();
    final String body = readResponse(is);
    return new JSONObject(body);
  }

  private void postPromWrite(final int serverIndex, final WriteRequest writeRequest) throws Exception {
    final byte[] protobufBytes = writeRequest.encode();
    final byte[] compressed = Snappy.compress(protobufBytes);

    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/prom/write")
        .toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(compressed);
      os.flush();
    }
    assertThat(connection.getResponseCode()).isEqualTo(204);
  }

  private String readResponse(final InputStream is) throws Exception {
    if (is == null)
      return "{}";
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final byte[] buf = new byte[4096];
    int n;
    while ((n = is.read(buf)) != -1)
      baos.write(buf, 0, n);
    return baos.toString(StandardCharsets.UTF_8);
  }

  private String encode(final String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
