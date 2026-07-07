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

import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for issue #5037 item 2: POST /begin must return HTTP 409 (not 401) when a transaction is
 * already started for the session, and {@code isolationLevel} must be optional (an empty JSON body still
 * begins a transaction with the default isolation instead of failing with 400).
 */
class Issue5037BeginHandlerIT extends BaseGraphServerTest {

  private String beginUrl() {
    return "http://127.0.0.1:2480/api/v1/begin/" + getDatabaseName();
  }

  @Test
  void secondBeginOnSameSessionReturnsConflict() throws Exception {
    // First begin: obtain a session id.
    HttpURLConnection connection = open(beginUrl());
    connection.setRequestMethod("POST");
    authorize(connection);
    connection.connect();

    final String sessionId;
    try {
      assertThat(connection.getResponseCode()).isEqualTo(204);
      sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID).trim();
      assertThat(sessionId).isNotNull();
    } finally {
      connection.disconnect();
    }

    // Second begin re-using the same session: it is a state conflict, not an authentication failure.
    connection = open(beginUrl());
    connection.setRequestMethod("POST");
    authorize(connection);
    connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(409);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void beginWithEmptyPayloadDoesNotRequireIsolationLevel() throws Exception {
    final HttpURLConnection connection = open(beginUrl());
    connection.setRequestMethod("POST");
    authorize(connection);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    final byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(body.length));
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(body);
    }
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(204);
      assertThat(connection.getHeaderField(ARCADEDB_SESSION_ID)).isNotNull();
    } finally {
      connection.disconnect();
    }
  }

  private static HttpURLConnection open(final String url) throws Exception {
    return (HttpURLConnection) new URI(url).toURL().openConnection();
  }

  private static void authorize(final HttpURLConnection connection) {
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
  }
}
