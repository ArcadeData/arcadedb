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

import com.arcadedb.remote.RemoteAlreadyExistsException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10075: the server answers a taken user, API token or database name as a conflict (HTTP 409), but the Java HTTP
 * client had no type for it: it fell through to a generic {@link RemoteException} that only message text could tell
 * apart. {@link RemoteServer} now raises {@link RemoteAlreadyExistsException} for all of them. Driven against a real
 * server, one test per way the conflict reaches the client.
 */
class Issue10075RemoteAlreadyExistsIT extends BaseGraphServerTest {

  private static final String PASSWORD = "a-long-password";

  @Test
  void aTakenUserNameIsATypedAlreadyExistsException() {
    final RemoteServer remote = remote();
    remote.createUser("dup_typed_user", PASSWORD, List.of());

    assertThatThrownBy(() -> remote.createUser("dup_typed_user", PASSWORD, List.of()))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .isInstanceOf(RemoteException.class)
        .isNotInstanceOf(SecurityException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining("dup_typed_user");
  }

  @Test
  void aTakenDatabaseNameIsATypedAlreadyExistsException() {
    final RemoteServer remote = remote();
    final String name = "dup_typed_db_10075";
    remote.create(name);
    try {
      assertThatThrownBy(() -> remote.create(name))
          .isInstanceOf(RemoteAlreadyExistsException.class)
          .hasMessageContaining("already exists")
          .hasMessageContaining(name);
    } finally {
      remote.drop(name);
    }
  }

  @Test
  void aTakenApiTokenNameIsATypedAlreadyExistsException() {
    final RemoteServer remote = remote();
    remote.createApiToken("dup_typed_token", "graph", 0, new JSONObject());

    assertThatThrownBy(() -> remote.createApiToken("dup_typed_token", "graph", 0, new JSONObject()))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining("dup_typed_token");
  }

  /**
   * The token route answered 409 with only {@code {"error": ...}}: no exception name, so a client could not tell a taken
   * token name from any other conflict. The body now names the exception, like every other error the server sends.
   */
  @Test
  void theTokenConflictBodyNamesItsException() throws Exception {
    remote().createApiToken("dup_body_token", "graph", 0, new JSONObject());

    final JSONObject body = new JSONObject().put("name", "dup_body_token").put("database", "graph").put("expiresAt", 0)
        .put("permissions", new JSONObject());
    final HttpURLConnection connection = (HttpURLConnection) URI.create(getServerHttpUrl(0, "/api/v1/server/api-tokens"))
        .toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);
      try (final OutputStream out = connection.getOutputStream()) {
        out.write(body.toString().getBytes(StandardCharsets.UTF_8));
      }

      assertThat(connection.getResponseCode()).isEqualTo(409);
      try (final InputStream in = connection.getErrorStream()) {
        final JSONObject answer = new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        assertThat(answer.getString("exception", "")).isEqualTo("com.arcadedb.server.ServerControlPlane$AlreadyExistsException");
        assertThat(answer.getString("detail", "")).contains("already exists").contains("dup_body_token");
        assertThat(answer.getString("error", "")).contains("already exists");
      }
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void aDifferentTokenNameIsStillCreated() {
    final RemoteServer remote = remote();
    remote.createApiToken("dup_ok_token_a", "graph", 0, new JSONObject());

    assertThat(remote.createApiToken("dup_ok_token_b", "graph", 0, new JSONObject()).getString("token", "")).isNotEmpty();
  }

  private RemoteServer remote() {
    return new RemoteServer("127.0.0.1", getServerHttpPort(0), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }
}
