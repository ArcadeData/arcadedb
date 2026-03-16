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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3567
 * When the request body exceeds the configured limit, the server should return
 * HTTP 413 with a descriptive JSON error body (not a silent 400 with no content).
 */
class HttpBodySizeLimitTest extends BaseGraphServerTest {

  private static final long SMALL_LIMIT_BYTES = 1024L; // 1KB for testing

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE, SMALL_LIMIT_BYTES);
  }

  @Test
  void requestExceedingBodyLimitReturns413WithJsonError() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "application/json");

      // Body exceeds the 1KB limit
      final byte[] largeBody = new byte[(int) (SMALL_LIMIT_BYTES * 2)];
      Arrays.fill(largeBody, (byte) 'x');
      connection.setRequestProperty("Content-Length", Integer.toString(largeBody.length));
      connection.setDoOutput(true);

      try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
        wr.write(largeBody);
      } catch (IOException ignored) {
        // Server may close connection early after sending 413; this is expected
      }

      try {
        final int responseCode = connection.getResponseCode();
        assertThat(responseCode).isEqualTo(413);

        final String errorBody = readErrorStream(connection);
        assertThat(errorBody).contains("error");
        assertThat(errorBody).contains("arcadedb.server.httpBodyContentMaxSize");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void requestWithinBodyLimitSucceeds() throws Exception {
    testEachServer((serverIndex) -> {
      // A small query well within the 1KB limit should succeed normally
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final String payload = "{\"language\":\"sql\",\"command\":\"SELECT 1\"}";
      final byte[] data = payload.getBytes(StandardCharsets.UTF_8);
      connection.setRequestProperty("Content-Length", Integer.toString(data.length));
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);

      try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
        wr.write(data);
      }
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }
    });
  }

  private String readErrorStream(final HttpURLConnection connection) {
    try {
      final InputStream errorStream = connection.getErrorStream();
      if (errorStream == null)
        return "";
      return new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "";
    }
  }
}
