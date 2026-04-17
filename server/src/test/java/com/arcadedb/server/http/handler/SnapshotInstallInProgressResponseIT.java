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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the base HTTP handler translates 500-class failures into 503 + Retry-After while
 * {@link ArcadeDBServer#isSnapshotInstallInProgress()} is {@code true}, so a snapshot install
 * running on a follower produces a retryable error to the client instead of an opaque 500.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotInstallInProgressResponseIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void failingRequestReturns500WhenFlagClear() throws Exception {
    final int status = postMalformedSqlAndReadStatus();
    assertThat(status).isEqualTo(500);
  }

  @Test
  void failingRequestReturns503WithRetryAfterWhenFlagSet() throws Exception {
    final ArcadeDBServer server = getServer(0);
    server.setSnapshotInstallInProgress(true);
    try {
      final int port = 2480;
      final HttpURLConnection conn = (HttpURLConnection) new URI(
          "http://localhost:" + port + "/api/v1/command/" + getDatabaseName()).toURL().openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      // Malformed SQL triggers CommandExecutionException → 500 in the normal path.
      conn.getOutputStream().write(
          "{\"language\":\"sql\",\"command\":\"SELECT FROM DoesNotExist_xyz123\"}".getBytes(StandardCharsets.UTF_8));

      final int status = conn.getResponseCode();
      final String retryAfter = conn.getHeaderField("Retry-After");

      assertThat(status).isEqualTo(503);
      assertThat(retryAfter).isEqualTo("5");
    } finally {
      server.setSnapshotInstallInProgress(false);
    }
  }

  @Test
  void successfulRequestUnaffectedByFlag() throws Exception {
    final ArcadeDBServer server = getServer(0);
    server.setSnapshotInstallInProgress(true);
    try {
      final int port = 2480;
      final HttpURLConnection conn = (HttpURLConnection) new URI(
          "http://localhost:" + port + "/api/v1/command/" + getDatabaseName()).toURL().openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      conn.getOutputStream().write(
          "{\"language\":\"sql\",\"command\":\"SELECT 1 AS n\"}".getBytes(StandardCharsets.UTF_8));

      assertThat(conn.getResponseCode()).isEqualTo(200);
    } finally {
      server.setSnapshotInstallInProgress(false);
    }
  }

  private int postMalformedSqlAndReadStatus() throws Exception {
    final int port = 2480;
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + port + "/api/v1/command/" + getDatabaseName()).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    conn.getOutputStream().write(
        "{\"language\":\"sql\",\"command\":\"SELECT FROM DoesNotExist_xyz123\"}".getBytes(StandardCharsets.UTF_8));
    return conn.getResponseCode();
  }
}
