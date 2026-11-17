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

import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12408: a request that carried no credentials at all was answered 401 with an EMPTY error message and a
 * {@code WWW-Authenticate: Basic} challenge. The HTTP API reference says that no challenge is sent, so that a browser
 * does not open its native login dialog over the login form of the client, and that the 401 of this API answers
 * "Authentication failed".
 */
class Issue12408RequestWithoutCredentialsAnswers401WithAMessageTest extends BaseGraphServerTest {

  @Test
  void aRequestWithoutCredentialsIs401WithAMessage() throws Exception {
    final Response response = send("GET", "/api/v1/databases", null);

    assertThat(response.status).as(response.body).isEqualTo(401);
    assertThat(response.body).as("the answer names the cause").contains("\"error\":\"Authentication failed\"");
    assertThat(response.body).as("the answer is not empty").doesNotContain("\"error\":\"\"");
  }

  @Test
  void aRequestWithoutCredentialsGetsNoChallenge() throws Exception {
    final Response response = send("GET", "/api/v1/databases", null);

    assertThat(response.status).as(response.body).isEqualTo(401);
    assertThat(response.challenge).as("no challenge is sent").isNull();
  }

  @Test
  void theOtherCredentialFailuresKeepTheirAnswer() throws Exception {
    final Response wrong = send("GET", "/api/v1/databases", basic("root:wrong"));
    assertThat(wrong.status).as(wrong.body).isEqualTo(401);
    assertThat(wrong.body).as("a wrong password names the cause").contains("Authentication failed");

    final Response malformed = send("GET", "/api/v1/databases", "Basic !!!not-base64!!!");
    assertThat(malformed.status).as(malformed.body).isEqualTo(401);
    assertThat(malformed.body).as("a malformed header names the cause").contains("Basic authentication error");
  }

  @Test
  void correctCredentialsStillAuthenticate() throws Exception {
    final Response response = send("GET", "/api/v1/databases", basic("root:" + StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS));

    assertThat(response.status).as(response.body).isEqualTo(200);
  }

  private static String basic(final String credential) {
    return "Basic " + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
  }

  private Response send(final String method, final String path, final String authorization) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(path)).toURL().openConnection();
    connection.setRequestMethod(method);
    if (authorization != null)
      connection.setRequestProperty("Authorization", authorization);
    try {
      connection.connect();
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body, connection.getHeaderField("WWW-Authenticate"));
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body, String challenge) {
  }
}
