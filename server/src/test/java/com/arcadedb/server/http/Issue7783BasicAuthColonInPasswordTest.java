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

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7783: RFC 7617 defines the Basic credential as {@code user-id ":" password}, where only the user-id is
 * forbidden a colon. The HTTP handler split the decoded pair on EVERY colon and demanded exactly two parts, so a
 * password containing ':' was refused with "Basic authentication error" before it ever reached the credential
 * check, and so was an empty password ({@code String.split} drops the trailing empty string).
 */
class Issue7783BasicAuthColonInPasswordTest extends BaseGraphServerTest {

  private static final String USER     = "colonuser7783";
  private static final String PASSWORD = "Pa:ss:word123!";

  @BeforeEach
  void createColonUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (!security.existsUser(USER))
      security.createUser(USER, PASSWORD);
  }

  @AfterEach
  void dropColonUser() {
    getServer(0).getSecurity().dropUser(USER);
  }

  @Test
  void aPasswordWithColonsAuthenticatesOverHttp() throws Exception {
    assertThat(getServer(0).getSecurity().authenticate(USER, PASSWORD, null).getName())
        .as("precondition: the stored credential is valid at the engine level").isEqualTo(USER);

    final Response response = get(USER + ":" + PASSWORD);
    assertThat(response.status).as(response.body).isEqualTo(200);
  }

  @Test
  void aWrongPasswordWithColonsReachesTheCredentialCheck() throws Exception {
    final Response response = get(USER + ":Pa:ss:WRONG");
    assertThat(response.status).isEqualTo(403);
    assertThat(response.body).doesNotContain("Basic authentication error");
    assertThat(response.body).contains("User/Password not valid");
  }

  @Test
  void anEmptyPasswordReachesTheCredentialCheck() throws Exception {
    final Response response = get(USER + ":");
    assertThat(response.status).isEqualTo(403);
    assertThat(response.body).doesNotContain("Basic authentication error");
    assertThat(response.body).contains("User/Password not valid");
  }

  @Test
  void aCredentialWithoutAnyColonIsStillMalformed() throws Exception {
    final Response response = get(USER + PASSWORD.replace(":", ""));
    assertThat(response.status).isEqualTo(403);
    assertThat(response.body).contains("Basic authentication error");
  }

  /**
   * The official Java client builds the header as {@code user + ":" + password}, so it is the other entry point
   * that carried a colon password into the broken split.
   */
  @Test
  void theJavaClientAuthenticatesWithAPasswordContainingColons() {
    try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), USER,
        PASSWORD)) {
      try (final ResultSet rs = database.query("sql", "select 1 as one")) {
        assertThat(rs.next().<Integer>getProperty("one")).isEqualTo(1);
      }
    }
  }

  private Response get(final String credential) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl("/api/v1/databases")).toURL()
        .openConnection();
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8)));
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
