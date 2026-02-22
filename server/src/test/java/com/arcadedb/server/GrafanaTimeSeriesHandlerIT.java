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
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Grafana-compatible TimeSeries HTTP endpoints.
 */
class GrafanaTimeSeriesHandlerIT extends BaseGraphServerTest {

  @Test
  void testHealthEndpoint() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = openGet(serverIndex, "/api/v1/ts/graph/grafana/health");
      assertThat(connection.getResponseCode()).isEqualTo(200);

      final JSONObject result = readJson(connection);
      assertThat(result.getString("status")).isEqualTo("ok");
      assertThat(result.getString("database")).isEqualTo("graph");
    });
  }

  @Test
  void testMetadataEndpoint() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final HttpURLConnection connection = openGet(serverIndex, "/api/v1/ts/graph/grafana/metadata");
      assertThat(connection.getResponseCode()).isEqualTo(200);

      final JSONObject result = readJson(connection);
      assertThat(result.has("types")).isTrue();
      assertThat(result.has("aggregationTypes")).isTrue();

      final JSONArray types = result.getJSONArray("types");
      assertThat(types.length()).isGreaterThanOrEqualTo(1);

      // Find the weather type
      JSONObject weatherType = null;
      for (int i = 0; i < types.length(); i++) {
        if ("weather".equals(types.getJSONObject(i).getString("name"))) {
          weatherType = types.getJSONObject(i);
          break;
        }
      }
      assertThat(weatherType).isNotNull();
      assertThat(weatherType.getJSONArray("fields").length()).isEqualTo(1); // temperature
      assertThat(weatherType.getJSONArray("tags").length()).isEqualTo(1);   // location

      final JSONArray aggTypes = result.getJSONArray("aggregationTypes");
      assertThat(aggTypes.length()).isGreaterThanOrEqualTo(5); // SUM, AVG, MIN, MAX, COUNT
    });
  }

  @Test
  void testRawGrafanaQuery() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("from", 1000L);
      request.put("to", 3000L);

      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", "weather");
      targets.put(target);
      request.put("targets", targets);

      final JSONObject result = postGrafanaQuery(serverIndex, request);
      assertThat(result.has("results")).isTrue();

      final JSONObject results = result.getJSONObject("results");
      assertThat(results.has("A")).isTrue();

      final JSONObject refA = results.getJSONObject("A");
      final JSONArray frames = refA.getJSONArray("frames");
      assertThat(frames.length()).isEqualTo(1);

      final JSONObject frame = frames.getJSONObject(0);
      assertThat(frame.has("schema")).isTrue();
      assertThat(frame.has("data")).isTrue();

      final JSONArray fields = frame.getJSONObject("schema").getJSONArray("fields");
      assertThat(fields.length()).isGreaterThanOrEqualTo(3); // ts, location, temperature

      final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
      assertThat(values.length()).isEqualTo(fields.length()); // columnar: one array per field

      // 3 data points
      assertThat(values.getJSONArray(0).length()).isEqualTo(3);
    });
  }

  @Test
  void testAggregatedGrafanaQuery() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("from", 1000L);
      request.put("to", 3000L);

      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", "weather");

      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", 5000L);

      final JSONArray aggRequests = new JSONArray();
      final JSONObject avgReq = new JSONObject();
      avgReq.put("field", "temperature");
      avgReq.put("type", "AVG");
      avgReq.put("alias", "avg_temp");
      aggRequests.put(avgReq);
      aggregation.put("requests", aggRequests);
      target.put("aggregation", aggregation);

      targets.put(target);
      request.put("targets", targets);

      final JSONObject result = postGrafanaQuery(serverIndex, request);
      final JSONObject refA = result.getJSONObject("results").getJSONObject("A");
      final JSONArray frames = refA.getJSONArray("frames");
      assertThat(frames.length()).isEqualTo(1);

      final JSONObject frame = frames.getJSONObject(0);
      final JSONArray fields = frame.getJSONObject("schema").getJSONArray("fields");

      // Should have "time" + "avg_temp"
      assertThat(fields.length()).isEqualTo(2);
      assertThat(fields.getJSONObject(0).getString("name")).isEqualTo("time");
      assertThat(fields.getJSONObject(0).getString("type")).isEqualTo("time");
      assertThat(fields.getJSONObject(1).getString("name")).isEqualTo("avg_temp");
      assertThat(fields.getJSONObject(1).getString("type")).isEqualTo("number");

      final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
      assertThat(values.length()).isEqualTo(2); // time column + avg_temp column
      assertThat(values.getJSONArray(0).length()).isGreaterThan(0); // at least one bucket
    });
  }

  @Test
  void testMultiTargetQuery() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      // Create a second TS type
      command(serverIndex,
          "CREATE TIMESERIES TYPE pressure TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");
      final String lineProtocol = "pressure,sensor=s1 value=1013.25 1500\n";
      postLineProtocol(serverIndex, lineProtocol, "ms");

      final JSONObject request = new JSONObject();
      request.put("from", 1000L);
      request.put("to", 3000L);

      final JSONArray targets = new JSONArray();

      final JSONObject targetA = new JSONObject();
      targetA.put("refId", "A");
      targetA.put("type", "weather");
      targets.put(targetA);

      final JSONObject targetB = new JSONObject();
      targetB.put("refId", "B");
      targetB.put("type", "pressure");
      targets.put(targetB);

      request.put("targets", targets);

      final JSONObject result = postGrafanaQuery(serverIndex, request);
      final JSONObject results = result.getJSONObject("results");
      assertThat(results.has("A")).isTrue();
      assertThat(results.has("B")).isTrue();

      // Both should have frames
      assertThat(results.getJSONObject("A").getJSONArray("frames").length()).isEqualTo(1);
      assertThat(results.getJSONObject("B").getJSONArray("frames").length()).isEqualTo(1);
    });
  }

  @Test
  void testTagFilterInGrafanaQuery() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("from", 1000L);
      request.put("to", 3000L);

      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", "weather");

      final JSONObject tags = new JSONObject();
      tags.put("location", "us-east");
      target.put("tags", tags);

      targets.put(target);
      request.put("targets", targets);

      final JSONObject result = postGrafanaQuery(serverIndex, request);
      final JSONObject frame = result.getJSONObject("results").getJSONObject("A")
          .getJSONArray("frames").getJSONObject(0);

      // us-east has 2 entries (timestamps 1000, 3000)
      final JSONArray timeValues = frame.getJSONObject("data").getJSONArray("values").getJSONArray(0);
      assertThat(timeValues.length()).isEqualTo(2);
    });
  }

  @Test
  void testAutoMaxDataPoints() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("from", 1000L);
      request.put("to", 3000L);
      request.put("maxDataPoints", 2);

      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", "weather");

      final JSONObject aggregation = new JSONObject();
      // No bucketInterval — should auto-calculate from maxDataPoints: (3000-1000)/2 = 1000
      final JSONArray aggRequests = new JSONArray();
      final JSONObject avgReq = new JSONObject();
      avgReq.put("field", "temperature");
      avgReq.put("type", "AVG");
      avgReq.put("alias", "avg_temp");
      aggRequests.put(avgReq);
      aggregation.put("requests", aggRequests);
      target.put("aggregation", aggregation);

      targets.put(target);
      request.put("targets", targets);

      final JSONObject result = postGrafanaQuery(serverIndex, request);
      final JSONObject frame = result.getJSONObject("results").getJSONObject("A")
          .getJSONArray("frames").getJSONObject(0);

      // Should have produced buckets (auto-calculated interval)
      final JSONArray timeValues = frame.getJSONObject("data").getJSONArray("values").getJSONArray(0);
      assertThat(timeValues.length()).isGreaterThan(0);
    });
  }

  @Test
  void testMissingTargets() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject request = new JSONObject();
      // No "targets"

      final int statusCode = postGrafanaQueryRaw(serverIndex, request);
      assertThat(statusCode).isEqualTo(400);
    });
  }

  // --- helper methods ---

  private void createTypeAndIngestData(final int serverIndex) throws Exception {
    command(serverIndex,
        "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");

    final String lineProtocol = """
        weather,location=us-east temperature=22.5 1000
        weather,location=us-west temperature=18.3 2000
        weather,location=us-east temperature=23.1 3000
        """;

    final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ms");
    assertThat(statusCode).isEqualTo(204);
  }

  private int postLineProtocol(final int serverIndex, final String body, final String precision) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/write?precision=" + precision)
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection.getResponseCode();
  }

  private JSONObject postGrafanaQuery(final int serverIndex, final JSONObject request) throws Exception {
    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/ts/graph/grafana/query", request);
    assertThat(connection.getResponseCode()).isEqualTo(200);
    return readJson(connection);
  }

  private int postGrafanaQueryRaw(final int serverIndex, final JSONObject request) throws Exception {
    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/ts/graph/grafana/query", request);
    return connection.getResponseCode();
  }

  private HttpURLConnection openGet(final int serverIndex, final String path) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + path)
        .toURL()
        .openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", basicAuth());
    return connection;
  }

  private HttpURLConnection openPost(final int serverIndex, final String path, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + path)
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection;
  }

  private JSONObject readJson(final HttpURLConnection connection) throws Exception {
    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
