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

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for InfluxDB Line Protocol ingestion via HTTP.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostTimeSeriesWriteHandlerIT extends BaseGraphServerTest {

  @Test
  void testIngestLineProtocol() throws Exception {
    testEachServer((serverIndex) -> {
      // Create a TimeSeries type
      command(serverIndex,
          "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");

      // Post InfluxDB Line Protocol data
      final String lineProtocol = """
          weather,location=us-east temperature=22.5 1000
          weather,location=us-west temperature=18.3 2000
          weather,location=us-east temperature=23.1 3000
          """;

      final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ms");
      assertThat(statusCode).isEqualTo(204);

      // Verify data was inserted
      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM weather");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(3);
    });
  }

  @Test
  void testIngestWithNanoPrecision() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE cpu TIMESTAMP ts TAGS (host STRING) FIELDS (usage DOUBLE)");

      // Nanosecond timestamps
      final String lineProtocol = "cpu,host=server1 usage=55.3 1000000000\ncpu,host=server2 usage=72.1 2000000000\n";

      final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ns");
      assertThat(statusCode).isEqualTo(204);

      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM cpu");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(2);
    });
  }

  @Test
  void testEmptyBody() throws Exception {
    testEachServer((serverIndex) -> {
      final int statusCode = postLineProtocol(serverIndex, "", "ms");
      assertThat(statusCode).isEqualTo(400);
    });
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
}
