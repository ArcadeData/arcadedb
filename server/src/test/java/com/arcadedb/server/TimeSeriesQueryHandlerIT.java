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
 * Integration tests for TimeSeries query and latest HTTP endpoints.
 */
class TimeSeriesQueryHandlerIT extends BaseGraphServerTest {

  @Test
  void testRawQuery() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");
      request.put("from", 1000L);
      request.put("to", 3000L);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();
      assertThat(result.getString("type")).isEqualTo("weather");
      assertThat(result.getInt("count")).isEqualTo(3);

      final JSONArray columns = result.getJSONArray("columns");
      assertThat(columns.length()).isGreaterThanOrEqualTo(3);

      final JSONArray rows = result.getJSONArray("rows");
      assertThat(rows.length()).isEqualTo(3);
    });
  }

  @Test
  void testAggregatedQuery() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");

      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", 5000L);

      final JSONArray requests = new JSONArray();
      final JSONObject avgReq = new JSONObject();
      avgReq.put("field", "temperature");
      avgReq.put("type", "AVG");
      avgReq.put("alias", "avg_temp");
      requests.put(avgReq);
      aggregation.put("requests", requests);

      request.put("aggregation", aggregation);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();
      assertThat(result.getString("type")).isEqualTo("weather");
      assertThat(result.getInt("count")).isGreaterThan(0);

      final JSONArray aggregations = result.getJSONArray("aggregations");
      assertThat(aggregations.getString(0)).isEqualTo("avg_temp");

      final JSONArray buckets = result.getJSONArray("buckets");
      assertThat(buckets.length()).isGreaterThan(0);

      final JSONObject firstBucket = buckets.getJSONObject(0);
      assertThat(firstBucket.has("timestamp")).isTrue();
      assertThat(firstBucket.has("values")).isTrue();
    });
  }

  @Test
  void testQueryWithTagFilter() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");

      final JSONObject tags = new JSONObject();
      tags.put("location", "us-east");
      request.put("tags", tags);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();

      final JSONArray rows = result.getJSONArray("rows");
      assertThat(rows.length()).isEqualTo(2);
    });
  }

  @Test
  void testQueryWithFieldProjection() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");

      final JSONArray fields = new JSONArray();
      fields.put("temperature");
      request.put("fields", fields);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();

      final JSONArray columns = result.getJSONArray("columns");
      // Should have timestamp + temperature only
      assertThat(columns.length()).isEqualTo(2);
      assertThat(columns.getString(0)).isEqualTo("ts");
      assertThat(columns.getString(1)).isEqualTo("temperature");
    });
  }

  @Test
  void testQueryMissingType() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject request = new JSONObject();
      // No "type" field

      final int statusCode = postTsQueryRaw(serverIndex, request);
      assertThat(statusCode).isEqualTo(400);
    });
  }

  @Test
  void testQueryNonTimeSeriesType() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE DOCUMENT TYPE notts");

      final JSONObject request = new JSONObject();
      request.put("type", "notts");

      final int statusCode = postTsQueryRaw(serverIndex, request);
      assertThat(statusCode).isEqualTo(400);
    });
  }

  @Test
  void testLatestValue() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject result = getTsLatest(serverIndex, "weather", null);
      assertThat(result).isNotNull();
      assertThat(result.getString("type")).isEqualTo("weather");

      final JSONArray latest = result.getJSONArray("latest");
      assertThat(latest).isNotNull();
      // Latest timestamp should be 3000
      assertThat(latest.getLong(0)).isEqualTo(3000L);
    });
  }

  @Test
  void testLatestWithTagFilter() throws Exception {
    testEachServer((serverIndex) -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject result = getTsLatest(serverIndex, "weather", "location:us-west");
      assertThat(result).isNotNull();

      final JSONArray latest = result.getJSONArray("latest");
      assertThat(latest).isNotNull();
      // us-west has only one entry at timestamp 2000
      assertThat(latest.getLong(0)).isEqualTo(2000L);
    });
  }

  @Test
  void testLatestEmptyType() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE emptyts TIMESTAMP ts TAGS (tag1 STRING) FIELDS (value DOUBLE)");

      final JSONObject result = getTsLatest(serverIndex, "emptyts", null);
      assertThat(result).isNotNull();
      assertThat(result.isNull("latest")).isTrue();
    });
  }

  @Test
  void testLatestMissingType() throws Exception {
    testEachServer((serverIndex) -> {
      final int statusCode = getTsLatestRaw(serverIndex, null, null);
      assertThat(statusCode).isEqualTo(400);
    });
  }

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
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection.getResponseCode();
  }

  private JSONObject postTsQuery(final int serverIndex, final JSONObject request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/query")
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private int postTsQueryRaw(final int serverIndex, final JSONObject request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/query")
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection.getResponseCode();
  }

  private JSONObject getTsLatest(final int serverIndex, final String type, final String tag) throws Exception {
    final StringBuilder url = new StringBuilder("http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/latest?type=" + type);
    if (tag != null)
      url.append("&tag=").append(tag);

    final HttpURLConnection connection = (HttpURLConnection) new URI(url.toString())
        .toURL()
        .openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private int getTsLatestRaw(final int serverIndex, final String type, final String tag) throws Exception {
    final StringBuilder url = new StringBuilder("http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/latest");
    if (type != null)
      url.append("?type=").append(type);
    if (tag != null)
      url.append(type != null ? "&" : "?").append("tag=").append(tag);

    final HttpURLConnection connection = (HttpURLConnection) new URI(url.toString())
        .toURL()
        .openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    return connection.getResponseCode();
  }
}
