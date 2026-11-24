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
 * Issue #12408: a request that carried no credentials at all was answered 401 with an EMPTY error message, where every
 * other 401 of the API names its cause and the HTTP API reference says the 401 of this API answers "Authentication
 * failed".
 * <p>
 * The {@code WWW-Authenticate: Basic} challenge of that same answer is NOT part of the defect and stays: a request with no
 * credentials is exactly the one a browser sends when a person opens the Swagger UI page at {@code /api/v1/docs}, which has
 * no login form of its own, so the challenge is what makes the browser ask for the credentials (RFC 9110 15.5.2: a 401 has
 * to carry one). The answers to credentials that WERE sent carry none, so a browser does not open its native login dialog
 * over the login form of Studio, whose every request carries credentials.
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
  void aRequestWithoutCredentialsKeepsItsChallenge() throws Exception {
    final Response response = send("GET", "/api/v1/databases", null);

    assertThat(response.status).as(response.body).isEqualTo(401);
    assertThat(response.challenge).as("a client that answers a challenge (a browser, the JDK Authenticator, curl) needs it").isEqualTo("Basic");
  }

  @Test
  void theSwaggerUiPageWithoutCredentialsIsChallenged() throws Exception {
    // the page has no login form: the challenge of this answer is the only way a person can sign in to it from a browser
    final Response response = send("GET", "/api/v1/docs", null);

    assertThat(response.status).as(response.body).isEqualTo(401);
    assertThat(response.challenge).as("the browser opens its login dialog on this answer").isEqualTo("Basic");
  }

  @Test
  void theOtherCredentialFailuresKeepTheirAnswerAndSendNoChallenge() throws Exception {
    final Response wrong = send("GET", "/api/v1/databases", basic("root:wrong"));
    assertThat(wrong.status).as(wrong.body).isEqualTo(401);
    assertThat(wrong.body).as("a wrong password names the cause").contains("Authentication failed");
    assertThat(wrong.challenge).as("a challenge here would open the native login dialog over the login form of Studio").isNull();

    final Response malformed = send("GET", "/api/v1/databases", "Basic !!!not-base64!!!");
    assertThat(malformed.status).as(malformed.body).isEqualTo(401);
    assertThat(malformed.body).as("a malformed header names the cause").contains("Basic authentication error");
    assertThat(malformed.challenge).as("no challenge for a malformed header").isNull();

    final Response unsupported = send("GET", "/api/v1/databases", "Weird abcdef");
    assertThat(unsupported.status).as(unsupported.body).isEqualTo(401);
    assertThat(unsupported.body).as("an unsupported scheme names the cause").contains("Authentication not supported");
    assertThat(unsupported.challenge).as("no challenge for an unsupported scheme").isNull();

    final Response token = send("GET", "/api/v1/databases", "Bearer garbage");
    assertThat(token.status).as(token.body).isEqualTo(401);
    assertThat(token.body).as("a bad token names the cause").contains("Invalid or expired authentication token");
    assertThat(token.challenge).as("no challenge for a bad token").isNull();
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
