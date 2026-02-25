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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class UserManagementIT extends BaseGraphServerTest {

  @Test
  void updateUserDatabases() throws Exception {
    testEachServer((serverIndex) -> {
      // Create user via POST
      createUser(serverIndex, "dbuser", "password1234",
          new JSONObject().put("*", new JSONArray().put("admin")));

      // PUT new databases
      final JSONObject updatePayload = new JSONObject();
      updatePayload.put("databases", new JSONObject()
          .put("graph", new JSONArray().put("admin")));

      final HttpURLConnection putConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users?name=dbuser").openConnection();
      putConn.setRequestMethod("PUT");
      putConn.setRequestProperty("Authorization", basicAuth());
      putConn.setDoOutput(true);
      putConn.setRequestProperty("Content-Type", "application/json");
      putConn.getOutputStream().write(updatePayload.toString().getBytes());
      putConn.connect();

      try {
        assertThat(putConn.getResponseCode()).isEqualTo(200);
      } finally {
        putConn.disconnect();
      }

      // GET users and verify change
      final HttpURLConnection getConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users").openConnection();
      getConn.setRequestMethod("GET");
      getConn.setRequestProperty("Authorization", basicAuth());
      getConn.connect();

      try {
        assertThat(getConn.getResponseCode()).isEqualTo(200);
        final JSONObject response = new JSONObject(readResponse(getConn));
        final JSONArray users = response.getJSONArray("result");
        boolean found = false;
        for (int i = 0; i < users.length(); i++) {
          final JSONObject u = users.getJSONObject(i);
          if ("dbuser".equals(u.getString("name"))) {
            found = true;
            assertThat(u.getJSONObject("databases").has("graph")).isTrue();
            assertThat(u.getJSONObject("databases").has("*")).isFalse();
            break;
          }
        }
        assertThat(found).isTrue();
      } finally {
        getConn.disconnect();
      }

      // Cleanup
      deleteUser(serverIndex, "dbuser");
    });
  }

  @Test
  void updateUserPassword() throws Exception {
    testEachServer((serverIndex) -> {
      createUser(serverIndex, "pwduser", "oldpassword1",
          new JSONObject().put("*", new JSONArray().put("admin")));

      // PUT new password
      final JSONObject updatePayload = new JSONObject();
      updatePayload.put("password", "newpassword2");

      final HttpURLConnection putConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users?name=pwduser").openConnection();
      putConn.setRequestMethod("PUT");
      putConn.setRequestProperty("Authorization", basicAuth());
      putConn.setDoOutput(true);
      putConn.setRequestProperty("Content-Type", "application/json");
      putConn.getOutputStream().write(updatePayload.toString().getBytes());
      putConn.connect();

      try {
        assertThat(putConn.getResponseCode()).isEqualTo(200);
      } finally {
        putConn.disconnect();
      }

      // Verify auth with new password works
      final String newAuth = "Basic " + Base64.getEncoder()
          .encodeToString("pwduser:newpassword2".getBytes());
      final HttpURLConnection authConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201%20as%20value").openConnection();
      authConn.setRequestMethod("GET");
      authConn.setRequestProperty("Authorization", newAuth);
      authConn.connect();

      try {
        assertThat(authConn.getResponseCode()).isEqualTo(200);
      } finally {
        authConn.disconnect();
      }

      // Verify old password no longer works
      final String oldAuth = "Basic " + Base64.getEncoder()
          .encodeToString("pwduser:oldpassword1".getBytes());
      final HttpURLConnection oldAuthConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201%20as%20value").openConnection();
      oldAuthConn.setRequestMethod("GET");
      oldAuthConn.setRequestProperty("Authorization", oldAuth);
      oldAuthConn.connect();

      try {
        assertThat(oldAuthConn.getResponseCode()).isEqualTo(403);
      } finally {
        oldAuthConn.disconnect();
      }

      // Cleanup
      deleteUser(serverIndex, "pwduser");
    });
  }

  @Test
  void createUserWithApitokenPrefixReturns400() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject payload = new JSONObject();
      payload.put("name", "apitoken:hack");
      payload.put("password", "password1234");

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.getOutputStream().write(payload.toString().getBytes());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(400);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void updateUserWithShortPasswordReturns400() throws Exception {
    testEachServer((serverIndex) -> {
      createUser(serverIndex, "shortpwduser", "validpassword1",
          new JSONObject().put("*", new JSONArray().put("admin")));

      final JSONObject updatePayload = new JSONObject().put("password", "short");

      final HttpURLConnection putConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users?name=shortpwduser").openConnection();
      putConn.setRequestMethod("PUT");
      putConn.setRequestProperty("Authorization", basicAuth());
      putConn.setDoOutput(true);
      putConn.setRequestProperty("Content-Type", "application/json");
      putConn.getOutputStream().write(updatePayload.toString().getBytes());
      putConn.connect();

      try {
        assertThat(putConn.getResponseCode()).isEqualTo(400);
      } finally {
        putConn.disconnect();
      }

      // Cleanup
      deleteUser(serverIndex, "shortpwduser");
    });
  }

  @Test
  void updateUserWithTooLongPasswordReturns400() throws Exception {
    testEachServer((serverIndex) -> {
      createUser(serverIndex, "longpwduser", "validpassword1",
          new JSONObject().put("*", new JSONArray().put("admin")));

      final String tooLongPassword = "a".repeat(257);
      final JSONObject updatePayload = new JSONObject().put("password", tooLongPassword);

      final HttpURLConnection putConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users?name=longpwduser").openConnection();
      putConn.setRequestMethod("PUT");
      putConn.setRequestProperty("Authorization", basicAuth());
      putConn.setDoOutput(true);
      putConn.setRequestProperty("Content-Type", "application/json");
      putConn.getOutputStream().write(updatePayload.toString().getBytes());
      putConn.connect();

      try {
        assertThat(putConn.getResponseCode()).isEqualTo(400);
      } finally {
        putConn.disconnect();
      }

      // Cleanup
      deleteUser(serverIndex, "longpwduser");
    });
  }

  @Test
  void updateNonExistentUserReturns404() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject updatePayload = new JSONObject();
      updatePayload.put("password", "doesntmatter");

      final HttpURLConnection putConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users?name=nonexistent").openConnection();
      putConn.setRequestMethod("PUT");
      putConn.setRequestProperty("Authorization", basicAuth());
      putConn.setDoOutput(true);
      putConn.setRequestProperty("Content-Type", "application/json");
      putConn.getOutputStream().write(updatePayload.toString().getBytes());
      putConn.connect();

      try {
        assertThat(putConn.getResponseCode()).isEqualTo(404);
      } finally {
        putConn.disconnect();
      }
    });
  }

  private void createUser(final int serverIndex, final String name, final String password,
      final JSONObject databases) throws Exception {
    // Delete user first if it already exists (cleanup from previous test runs)
    if (getServer(serverIndex).getSecurity().existsUser(name))
      getServer(serverIndex).getSecurity().dropUser(name);

    final JSONObject payload = new JSONObject();
    payload.put("name", name);
    payload.put("password", password);
    payload.put("databases", databases);

    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void deleteUser(final int serverIndex, final String name) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server/users?name=" +
            URLEncoder.encode(name, "UTF-8")).openConnection();
    connection.setRequestMethod("DELETE");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
