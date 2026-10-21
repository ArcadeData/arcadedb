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

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10109: a request whose HTTP Basic credentials fail to authenticate was answered 403 "Security error", the
 * same answer as an authenticated caller asking for something it is not allowed. RFC 9110 15.5.2 makes it a 401
 * ("lacks valid authentication credentials"), which is also what an invalid bearer or API token already got. 403
 * stays reserved for an authenticated principal that is refused.
 */
class Issue10109WrongBasicCredentialsAnswer401Test extends BaseGraphServerTest {

  private static final String USER     = "user10109";
  private static final String PASSWORD = "Passw0rd10109!";
  private static final String LOCKED   = "locked10109";

  @BeforeEach
  void createUsers() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (!security.existsUser(USER))
      security.createUser(USER, PASSWORD);
    if (!security.existsUser(LOCKED))
      security.createUser(LOCKED, PASSWORD);
  }

  @AfterEach
  void dropUsers() {
    getServer(0).getSecurity().dropUser(USER);
    getServer(0).getSecurity().dropUser(LOCKED);
  }

  @Test
  void loginWithAWrongPasswordIs401() throws Exception {
    final Response response = send("POST", "/api/v1/login", basic(USER + ":wrong"));
    assertThat(response.status).as(response.body).isEqualTo(401);
    assertThat(response.body).contains(ServerSecurityException.class.getName());
    assertThat(response.body).contains("User/Password not valid");
  }

  @Test
  void aWrongPasswordOnAnyEndpointIs401() throws Exception {
    final Response response = send("GET", "/api/v1/databases", basic(USER + ":wrong"));
    assertThat(response.status).as(response.body).isEqualTo(401);
  }

  @Test
  void anUnknownUserIs401() throws Exception {
    final Response response = send("GET", "/api/v1/databases", basic("nobody10109:whatever"));
    assertThat(response.status).as(response.body).isEqualTo(401);
  }

  @Test
  void aLockedOutPrincipalIs401() throws Exception {
    // ServerSecurity locks a principal out after 5 failures; the refusal is still a credential failure, answered as
    // the API-token lockout already is
    for (int i = 0; i < 5; i++)
      assertThat(send("GET", "/api/v1/databases", basic(LOCKED + ":wrong")).status).isEqualTo(401);

    final Response response = send("GET", "/api/v1/databases", basic(LOCKED + ":" + PASSWORD));
    assertThat(response.status).as(response.body).isEqualTo(401);
    assertThat(response.body).contains("Too many failed authentication attempts");
  }

  @Test
  void aMalformedBasicHeaderIs401() throws Exception {
    assertThat(send("GET", "/api/v1/databases", "Basic !!!not-base64!!!").status).isEqualTo(401);
    assertThat(send("GET", "/api/v1/databases", basic("no-colon-at-all")).status).isEqualTo(401);
  }

  @Test
  void anUnsupportedSchemeIs401() throws Exception {
    final Response response = send("GET", "/api/v1/databases", "Digest username=\"root\"");
    assertThat(response.status).as(response.body).isEqualTo(401);
    assertThat(response.body).contains("Authentication not supported");
  }

  @Test
  void anAuthenticatedButForbiddenCallerIsStill403() throws Exception {
    final Response response = send("GET", "/api/v1/server/users", basic(USER + ":" + PASSWORD));
    assertThat(response.status).as(response.body).isEqualTo(403);
  }

  @Test
  void correctCredentialsStillAuthenticate() throws Exception {
    final Response response = send("POST", "/api/v1/login", basic(USER + ":" + PASSWORD));
    assertThat(response.status).as(response.body).isEqualTo(200);
  }

  /**
   * The Java client rebuilds the typed exception from the body's exception class name, not from the status, so the
   * caller still gets a SecurityException.
   */
  @Test
  void theJavaClientStillRaisesASecurityException() {
    assertThatThrownBy(() -> {
      try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), USER,
          "wrong")) {
        database.query("sql", "select 1").close();
      }
    }).isInstanceOf(SecurityException.class);
  }

  private static String basic(final String credential) {
    return "Basic " + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
  }

  private Response send(final String method, final String path, final String authorization) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(path)).toURL().openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", authorization);
    try {
      connection.connect();
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body) {
  }
}
