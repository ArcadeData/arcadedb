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
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A security change takes effect on a running server without a restart, end to end over HTTP: revoked or granted
 * group permissions, and login tokens of a dropped user or a changed password.
 */
class LiveSecurityChangeIT {
  /**
   * Regression test for issue #6806: a group's <b>database-level</b> grants ({@code updateSchema},
   * {@code updateSecurity}, {@code updateDatabaseSettings}) were frozen at the values they had when the user
   * first touched the database.
   * <p>
   * {@code ServerSecurityDatabaseUser} keeps two independent permission maps refreshed by two different
   * methods, and the server's only refresh path ({@code ServerSecurity.updateSchema}) called just the
   * per-type/per-bucket one. So a revoked {@code updateSchema} kept working until restart, and - symmetrically -
   * a newly granted one was ignored until restart. {@code GroupManagementIT.updateGroup} only asserts that the
   * group JSON round-trips through the group file, so nothing pinned the effective permission.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GroupPermissionRefresh extends BaseGraphServerTest {

    private static final String GROUP = "issue6806editor";
    private static final String USER  = "issue6806alice";
    private static final String PWD   = "issue6806pwd";

    @Test
    void revokedAndRegrantedDatabaseGrantTakeEffectWithoutRestart() throws Exception {
      testEachServer(serverIndex -> {
        saveGroup(serverIndex, new JSONArray().put("updateSchema"));
        createUser(serverIndex);

        try {
          // The first command caches alice's ServerSecurityDatabaseUser for this database, with UPDATE_SCHEMA=true.
          assertThat(userCommand(serverIndex, "CREATE VERTEX TYPE Issue6806A"))
              .as("granted updateSchema must let the group member create a type").isEqualTo(200);

          // REVOKE: the group keeps its per-type grants but loses every database-level one.
          saveGroup(serverIndex, new JSONArray());
          assertThat(userCommand(serverIndex, "CREATE VERTEX TYPE Issue6806B"))
              .as("revoked updateSchema must take effect immediately, not at the next restart").isEqualTo(403);

          // RE-GRANT: the same gap broke this direction too - a newly granted permission was ignored.
          saveGroup(serverIndex, new JSONArray().put("updateSchema"));
          assertThat(userCommand(serverIndex, "CREATE VERTEX TYPE Issue6806C"))
              .as("re-granted updateSchema must take effect immediately").isEqualTo(200);
        } finally {
          deleteGroup(serverIndex);
          dropUser(serverIndex);
          adminCommand(serverIndex, "DROP TYPE Issue6806A IF EXISTS");
          adminCommand(serverIndex, "DROP TYPE Issue6806B IF EXISTS");
          adminCommand(serverIndex, "DROP TYPE Issue6806C IF EXISTS");
        }
      });
    }

    /**
     * The quotas live in the same never-refreshed map as the database-level grants, so a group whose
     * {@code resultSetLimit} is tightened after the user has touched the database used to keep serving the old
     * (looser) limit until restart.
     */
    @Test
    void groupResultSetLimitIsRefreshed() throws Exception {
      testEachServer(serverIndex -> {
        saveGroup(serverIndex, new JSONArray(), -1L);
        createUser(serverIndex);

        try {
          // Caches alice's database user with resultSetLimit = -1 (unlimited).
          assertThat(userCommand(serverIndex, "SELECT FROM V1 LIMIT 1")).isEqualTo(200);
          assertThat(effectiveResultSetLimit(serverIndex)).isEqualTo(-1L);

          saveGroup(serverIndex, new JSONArray(), 7L);
          assertThat(effectiveResultSetLimit(serverIndex))
              .as("a tightened group quota must be visible without a restart").isEqualTo(7L);
        } finally {
          deleteGroup(serverIndex);
          dropUser(serverIndex);
        }
      });
    }

    private long effectiveResultSetLimit(final int serverIndex) {
      return getServer(serverIndex).getSecurity().getUser(USER)
          .getDatabaseUser(getServer(serverIndex).getDatabaseForOperator(getDatabaseName())).getResultSetLimit();
    }

    private void saveGroup(final int serverIndex, final JSONArray databaseAccess) throws Exception {
      saveGroup(serverIndex, databaseAccess, -1L);
    }

    private void saveGroup(final int serverIndex, final JSONArray databaseAccess, final long resultSetLimit)
        throws Exception {
      final JSONObject payload = new JSONObject()
          .put("database", getDatabaseName())
          .put("name", GROUP)
          .put("resultSetLimit", resultSetLimit)
          .put("readTimeout", -1L)
          .put("access", databaseAccess)
          .put("types", new JSONObject().put("*", new JSONObject().put("access",
              new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))));

      final HttpURLConnection connection = open(serverIndex, "/api/v1/server/groups", "POST", basicAuth());
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
      connection.connect();
      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }
    }

    private void deleteGroup(final int serverIndex) throws Exception {
      final HttpURLConnection connection = open(serverIndex,
          "/api/v1/server/groups?database=" + getDatabaseName() + "&name=" + GROUP, "DELETE", basicAuth());
      connection.connect();
      try {
        connection.getResponseCode();
      } finally {
        connection.disconnect();
      }
    }

    private void createUser(final int serverIndex) throws Exception {
      if (getServer(serverIndex).getSecurity().existsUser(USER))
        getServer(serverIndex).getSecurity().dropUser(USER);

      final JSONObject payload = new JSONObject()
          .put("name", USER)
          .put("password", PWD)
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP)));

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

    private int adminCommand(final int serverIndex, final String sql) throws Exception {
      return command(serverIndex, basicAuth(), sql);
    }

    private int userCommand(final int serverIndex, final String sql) throws Exception {
      return command(serverIndex, "Basic " + Base64.getEncoder()
          .encodeToString((USER + ":" + PWD).getBytes(StandardCharsets.UTF_8)), sql);
    }

    private int command(final int serverIndex, final String auth, final String sql) throws Exception {
      final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + getDatabaseName(), "POST", auth);
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.getOutputStream()
          .write(new JSONObject().put("language", "sql").put("command", sql).toString().getBytes(StandardCharsets.UTF_8));
      connection.connect();
      try {
        return connection.getResponseCode();
      } finally {
        connection.disconnect();
      }
    }

    private HttpURLConnection open(final int serverIndex, final String path, final String method, final String auth)
        throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:" + getServerHttpPort(serverIndex) + path).openConnection();
      connection.setRequestMethod(method);
      connection.setRequestProperty("Authorization", auth);
      return connection;
    }

    private String basicAuth() {
      return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
    }
  }

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
  @Nested
  class LoginTokenRevocation extends BaseGraphServerTest {

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
          final List<JSONObject> fromFile = security.usersToJSON();
          for (final JSONObject user : fromFile)
            if (USER.equals(user.getString("name")))
              user.put("password", security.encodePassword(PWD + "-from-file"));
          security.saveUsers(fromFile);
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
          "http://127.0.0.1:" + getServerHttpPort(serverIndex) + path).openConnection();
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
}
