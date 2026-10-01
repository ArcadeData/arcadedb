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
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8798: the native aggregation endpoint buckets from the Unix epoch, a Thursday, so a one-week bucket started on
 * Thursday and a one-day bucket at 08:00 in UTC+8. {@code aggregation.bucketOrigin} moves the grid, and it must be the
 * grid {@code ts.timeBucket} uses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8798BucketOriginIT extends BaseGraphServerTest {
  private static final long DAY  = 86_400_000L;
  private static final long WEEK = 7 * DAY;

  private static long ms(final String iso) {
    return Instant.parse(iso).toEpochMilli();
  }

  @Test
  void bucketOriginMovesTheGrid() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex, "CREATE TIMESERIES TYPE energy TIMESTAMP ts TAGS (site STRING) FIELDS (kwh DOUBLE)");

      // Sun 2025-06-15 18:30 UTC (a Thursday-grid week of 2025-06-12), Mon 2025-06-16 01:00, Tue 2025-06-10 12:00
      final String lineProtocol = "energy,site=a kwh=1.0 " + ms("2025-06-15T18:30:00Z") + "\n"
          + "energy,site=a kwh=2.0 " + ms("2025-06-16T01:00:00Z") + "\n"
          + "energy,site=a kwh=4.0 " + ms("2025-06-10T12:00:00Z") + "\n";
      assertThat(postLineProtocol(serverIndex, lineProtocol)).isEqualTo(204);

      // default grid: Thursday weeks
      JSONArray buckets = aggregate(serverIndex, WEEK, null).getJSONArray("buckets");
      assertThat(buckets.length()).isEqualTo(2);
      assertThat(buckets.getJSONObject(0).getLong("timestamp")).isEqualTo(ms("2025-06-05T00:00:00Z"));
      assertThat(buckets.getJSONObject(1).getLong("timestamp")).isEqualTo(ms("2025-06-12T00:00:00Z"));

      // Monday weeks
      buckets = aggregate(serverIndex, WEEK, ms("2024-01-01T00:00:00Z")).getJSONArray("buckets");
      assertThat(buckets.length()).isEqualTo(2);
      assertThat(buckets.getJSONObject(0).getLong("timestamp")).isEqualTo(ms("2025-06-09T00:00:00Z"));
      assertThat(buckets.getJSONObject(1).getLong("timestamp")).isEqualTo(ms("2025-06-16T00:00:00Z"));
      assertThat(buckets.getJSONObject(0).getJSONArray("values").getDouble(0)).isEqualTo(5.0);
      assertThat(buckets.getJSONObject(1).getJSONArray("values").getDouble(0)).isEqualTo(2.0);

      // local days in UTC+8: the origin is local midnight, which is 16:00 UTC of the day before
      buckets = aggregate(serverIndex, DAY, ms("2025-06-14T16:00:00Z")).getJSONArray("buckets");
      assertThat(buckets.getJSONObject(buckets.length() - 1).getLong("timestamp")).isEqualTo(ms("2025-06-15T16:00:00Z"));

      // the same query through SQL lands in the same buckets
      final JSONObject sql = new JSONObject(command(serverIndex,
          "SELECT ts.timeBucket('1w', ts, {'origin': '2024-01-01T00:00:00Z'}) AS wk, sum(kwh) AS s FROM energy GROUP BY wk ORDER BY wk"));
      assertThat(sql.getJSONArray("result").length()).isEqualTo(2);
    });
  }

  @Test
  void bucketOriginMustBeANumber() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex, "CREATE TIMESERIES TYPE energy TIMESTAMP ts TAGS (site STRING) FIELDS (kwh DOUBLE)");
      final JSONObject request = request(WEEK, null);
      request.getJSONObject("aggregation").put("bucketOrigin", "monday");
      assertThat(post(serverIndex, request).getResponseCode()).isEqualTo(400);
    });
  }

  private JSONObject request(final long interval, final Long origin) {
    final JSONObject req = new JSONObject();
    req.put("type", "energy");
    final JSONObject aggregation = new JSONObject();
    aggregation.put("bucketInterval", interval);
    if (origin != null)
      aggregation.put("bucketOrigin", origin);
    final JSONArray requests = new JSONArray();
    final JSONObject sum = new JSONObject();
    sum.put("field", "kwh");
    sum.put("type", "SUM");
    requests.put(sum);
    aggregation.put("requests", requests);
    req.put("aggregation", aggregation);
    return req;
  }

  private JSONObject aggregate(final int serverIndex, final long interval, final Long origin) throws Exception {
    final HttpURLConnection connection = post(serverIndex, request(interval, origin));
    assertThat(connection.getResponseCode()).isEqualTo(200);
    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private HttpURLConnection post(final int serverIndex, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/ts/graph/query").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.toString().getBytes(StandardCharsets.UTF_8));
    }
    return connection;
  }

  private int postLineProtocol(final int serverIndex, final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/ts/graph/write?precision=ms").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
    }
    return connection.getResponseCode();
  }
}
