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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the revocation half of issue #6808: dropping a user, or changing its password, has to
 * revoke the {@code AU-} login tokens already minted for it, not only stop future logins.
 * <p>
 * The bearer branch of {@code AbstractServerHttpHandler} used to take the principal straight from the cached
 * {@code HttpAuthSession}, which captured a {@code ServerSecurityUser} at login time, and
 * {@code ServerSecurity.invalidateHttpSessions} only reached the transaction-session manager. A revoked
 * principal therefore kept authenticating - with its login-time grants - until the token idle-expired, up to
 * 30 minutes after an operator believed the credentials were gone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6808LoginTokenRevocationIT extends BaseGraphServerTest {

  private static final String USER = "issue6808tokenuser";
  private static final String PWD  = "issue6808password";

  @Test
  void droppingAUserRevokesItsLoginTokenImmediately() throws Exception {
    testEachServer(serverIndex -> {
      createUser(serverIndex, PWD);
      try {
        final String token = login(serverIndex, PWD);
        assertThat(queryWithToken(serverIndex, token)).as("a fresh token must work").isEqualTo(200);

        dropUser(serverIndex);

        assertThat(queryWithToken(serverIndex, token))
            .as("the token of a dropped user must stop working at once, not when it idle-expires")
            .isEqualTo(401);
      } finally {
        dropUser(serverIndex);
      }
    });
  }

  @Test
  void changingThePasswordRevokesTheLoginTokenIssuedBeforeIt() throws Exception {
    testEachServer(serverIndex -> {
      createUser(serverIndex, PWD);
      try {
        final String token = login(serverIndex, PWD);
        assertThat(queryWithToken(serverIndex, token)).isEqualTo(200);

        updatePassword(serverIndex, PWD + "-rotated");

        assertThat(queryWithToken(serverIndex, token))
            .as("a password rotation must not leave the previous credentials usable through a token")
            .isEqualTo(401);

        // The new password still mints a working token, so the revocation is not a lockout.
        assertThat(queryWithToken(serverIndex, login(serverIndex, PWD + "-rotated"))).isEqualTo(200);
      } finally {
        dropUser(serverIndex);
      }
    });
  }

  @Test
  void reloadingTheUserFileWithARotatedPasswordRevokesTheTokenToo() throws Exception {
    testEachServer(serverIndex -> {
      createUser(serverIndex, PWD);
      try {
        final String token = login(serverIndex, PWD);
        assertThat(queryWithToken(serverIndex, token)).isEqualTo(200);

        // Third way a password can change: an operator edits server-users.jsonl and the file is re-read.
        // The bearer path only requires the principal to still resolve by name, and it does, so the reload
        // has to compare the stored hashes and revoke on its own.
        final ServerSecurity security = getServer(serverIndex).getSecurity();
        for (final JSONObject user : security.usersToJSON())
          if (USER.equals(user.getString("name")))
            user.put("password", security.encodePassword(PWD + "-from-file"));
        security.saveUsers();
        security.loadUsers();

        assertThat(queryWithToken(serverIndex, token))
            .as("a password rotated through the user file must revoke the tokens issued before it")
            .isEqualTo(401);
      } finally {
        dropUser(serverIndex);
      }
    });
  }

  private String login(final int serverIndex, final String password) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/login", "POST", basicAuth(USER, password));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String token = new JSONObject(readResponse(connection)).getString("token");
      assertThat(token).startsWith("AU-");
      return token;
    } finally {
      connection.disconnect();
    }
  }

  private int queryWithToken(final int serverIndex, final String token) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/query/" + getDatabaseName(), "POST",
        "Bearer " + token);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(new JSONObject().put("language", "sql").put("command", "select 1 as one")
        .toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private void createUser(final int serverIndex, final String password) throws Exception {
    if (getServer(serverIndex).getSecurity().existsUser(USER))
      getServer(serverIndex).getSecurity().dropUser(USER);

    final JSONObject payload = new JSONObject()
        .put("name", USER)
        .put("password", password)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", "POST", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void updatePassword(final int serverIndex, final String password) throws Exception {
    final HttpURLConnection connection = open(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(USER, StandardCharsets.UTF_8), "PUT", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream()
        .write(new JSONObject().put("password", password).toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private void dropUser(final int serverIndex) throws Exception {
    final HttpURLConnection connection = open(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(USER, StandardCharsets.UTF_8), "DELETE", basicAuth());
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String method, final String auth)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth() {
    return basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
