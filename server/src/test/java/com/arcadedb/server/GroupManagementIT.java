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

import java.io.*;
import java.net.*;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GroupManagementIT extends BaseGraphServerTest {

  @Test
  void listDefaultGroups() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final JSONObject response = new JSONObject(readResponse(connection));
        final JSONObject result = response.getJSONObject("result");
        assertThat(result.has("databases")).isTrue();

        final JSONObject databases = result.getJSONObject("databases");
        assertThat(databases.has("*")).isTrue();

        final JSONObject defaultDb = databases.getJSONObject("*");
        assertThat(defaultDb.has("groups")).isTrue();

        final JSONObject groups = defaultDb.getJSONObject("groups");
        assertThat(groups.has("admin")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void createGroup() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject payload = new JSONObject();
      payload.put("database", "*");
      payload.put("name", "reader");
      payload.put("resultSetLimit", 100);
      payload.put("readTimeout", 5000);
      payload.put("access", new JSONArray());
      payload.put("types", new JSONObject()
          .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.getOutputStream().write(payload.toString().getBytes());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }

      // Verify group was created
      final HttpURLConnection getConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
      getConn.setRequestMethod("GET");
      getConn.setRequestProperty("Authorization", basicAuth());
      getConn.connect();

      try {
        assertThat(getConn.getResponseCode()).isEqualTo(200);
        final JSONObject response = new JSONObject(readResponse(getConn));
        final JSONObject groups = response.getJSONObject("result")
            .getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
        assertThat(groups.has("reader")).isTrue();

        final JSONObject reader = groups.getJSONObject("reader");
        assertThat(reader.getLong("resultSetLimit")).isEqualTo(100);
        assertThat(reader.getLong("readTimeout")).isEqualTo(5000);
      } finally {
        getConn.disconnect();
      }
    });
  }

  @Test
  void updateGroup() throws Exception {
    testEachServer((serverIndex) -> {
      // First create the group
      final JSONObject createPayload = new JSONObject();
      createPayload.put("database", "*");
      createPayload.put("name", "writer");
      createPayload.put("resultSetLimit", -1);
      createPayload.put("readTimeout", -1);
      createPayload.put("access", new JSONArray());
      createPayload.put("types", new JSONObject()
          .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));

      postGroup(serverIndex, createPayload);

      // Now update it with more permissions
      final JSONObject updatePayload = new JSONObject();
      updatePayload.put("database", "*");
      updatePayload.put("name", "writer");
      updatePayload.put("resultSetLimit", 500);
      updatePayload.put("readTimeout", 10000);
      updatePayload.put("access", new JSONArray().put("updateSchema"));
      updatePayload.put("types", new JSONObject()
          .put("*", new JSONObject().put("access",
              new JSONArray().put("createRecord").put("readRecord").put("updateRecord"))));

      postGroup(serverIndex, updatePayload);

      // Verify updated
      final HttpURLConnection getConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
      getConn.setRequestMethod("GET");
      getConn.setRequestProperty("Authorization", basicAuth());
      getConn.connect();

      try {
        assertThat(getConn.getResponseCode()).isEqualTo(200);
        final JSONObject response = new JSONObject(readResponse(getConn));
        final JSONObject writer = response.getJSONObject("result")
            .getJSONObject("databases").getJSONObject("*").getJSONObject("groups").getJSONObject("writer");
        assertThat(writer.getLong("resultSetLimit")).isEqualTo(500);
        assertThat(writer.getLong("readTimeout")).isEqualTo(10000);

        final JSONArray access = writer.getJSONArray("access");
        assertThat(access.length()).isEqualTo(1);
        assertThat(access.getString(0)).isEqualTo("updateSchema");
      } finally {
        getConn.disconnect();
      }
    });
  }

  @Test
  void deleteGroup() throws Exception {
    testEachServer((serverIndex) -> {
      // Create group first
      final JSONObject payload = new JSONObject();
      payload.put("database", "*");
      payload.put("name", "todelete");
      payload.put("resultSetLimit", -1);
      payload.put("readTimeout", -1);
      payload.put("access", new JSONArray());
      payload.put("types", new JSONObject());

      postGroup(serverIndex, payload);

      // Delete it
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups?database=*&name=todelete").openConnection();
      connection.setRequestMethod("DELETE");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }

      // Verify it's gone
      final HttpURLConnection getConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
      getConn.setRequestMethod("GET");
      getConn.setRequestProperty("Authorization", basicAuth());
      getConn.connect();

      try {
        assertThat(getConn.getResponseCode()).isEqualTo(200);
        final JSONObject response = new JSONObject(readResponse(getConn));
        final JSONObject groups = response.getJSONObject("result")
            .getJSONObject("databases").getJSONObject("*").getJSONObject("groups");
        assertThat(groups.has("todelete")).isFalse();
      } finally {
        getConn.disconnect();
      }
    });
  }

  @Test
  void cannotDeleteAdminFromWildcard() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups?database=*&name=admin").openConnection();
      connection.setRequestMethod("DELETE");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(400);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void nonRootCannotManageGroups() throws Exception {
    testEachServer((serverIndex) -> {
      if (!getServer(serverIndex).getSecurity().existsUser("testuser"))
        getServer(serverIndex).getSecurity().createUser("testuser", "testpass");

      final String nonRootAuth = "Basic " + Base64.getEncoder()
          .encodeToString("testuser:testpass".getBytes());

      // GET should fail
      final HttpURLConnection getConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
      getConn.setRequestMethod("GET");
      getConn.setRequestProperty("Authorization", nonRootAuth);
      getConn.connect();

      try {
        assertThat(getConn.getResponseCode()).isEqualTo(403);
      } finally {
        getConn.disconnect();
      }

      // POST should fail
      final HttpURLConnection postConn = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
      postConn.setRequestMethod("POST");
      postConn.setRequestProperty("Authorization", nonRootAuth);
      postConn.setDoOutput(true);
      postConn.setRequestProperty("Content-Type", "application/json");
      postConn.getOutputStream().write("{}".getBytes());
      postConn.connect();

      try {
        assertThat(postConn.getResponseCode()).isEqualTo(403);
      } finally {
        postConn.disconnect();
      }
    });
  }

  @Test
  void databaseSpecificGroupDoesNotBreakWildcardGroups() throws Exception {
    testEachServer((serverIndex) -> {
      // Create a test database
      final HttpURLConnection createDb = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      createDb.setRequestMethod("POST");
      createDb.setRequestProperty("Authorization", basicAuth());
      createDb.setDoOutput(true);
      createDb.setRequestProperty("Content-Type", "application/json");
      createDb.getOutputStream().write("{\"command\":\"create database grouptest\"}".getBytes());
      createDb.connect();
      try {
        assertThat(createDb.getResponseCode()).isEqualTo(200);
      } finally {
        createDb.disconnect();
      }

      try {
        // Create a type in the database
        final HttpURLConnection createType = (HttpURLConnection) new URL(
            "http://127.0.0.1:248" + serverIndex + "/api/v1/command/grouptest").openConnection();
        createType.setRequestMethod("POST");
        createType.setRequestProperty("Authorization", basicAuth());
        createType.setDoOutput(true);
        createType.setRequestProperty("Content-Type", "application/json");
        createType.getOutputStream().write("{\"language\":\"sql\",\"command\":\"CREATE DOCUMENT TYPE TestDoc\"}".getBytes());
        createType.connect();
        try {
          assertThat(createType.getResponseCode()).isEqualTo(200);
        } finally {
          createType.disconnect();
        }

        // Add a database-specific group (not admin)
        final JSONObject payload = new JSONObject();
        payload.put("database", "grouptest");
        payload.put("name", "customreader");
        payload.put("resultSetLimit", 50);
        payload.put("readTimeout", 3000);
        payload.put("access", new JSONArray());
        payload.put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));

        postGroup(serverIndex, payload);

        // Root user (in admin group from *) should still be able to query this database
        final HttpURLConnection query = (HttpURLConnection) new URL(
            "http://127.0.0.1:248" + serverIndex + "/api/v1/query/grouptest").openConnection();
        query.setRequestMethod("POST");
        query.setRequestProperty("Authorization", basicAuth());
        query.setDoOutput(true);
        query.setRequestProperty("Content-Type", "application/json");
        query.getOutputStream().write("{\"language\":\"sql\",\"command\":\"SELECT FROM TestDoc\"}".getBytes());
        query.connect();

        try {
          assertThat(query.getResponseCode()).isEqualTo(200);
        } finally {
          query.disconnect();
        }
      } finally {
        // Cleanup: drop the database and remove the group
        final HttpURLConnection dropDb = (HttpURLConnection) new URL(
            "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
        dropDb.setRequestMethod("POST");
        dropDb.setRequestProperty("Authorization", basicAuth());
        dropDb.setDoOutput(true);
        dropDb.setRequestProperty("Content-Type", "application/json");
        dropDb.getOutputStream().write("{\"command\":\"drop database grouptest\"}".getBytes());
        dropDb.connect();
        try {
          dropDb.getResponseCode();
        } finally {
          dropDb.disconnect();
        }

        // Clean up the database-specific group entry
        final HttpURLConnection deleteGroup = (HttpURLConnection) new URL(
            "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups?database=grouptest&name=customreader").openConnection();
        deleteGroup.setRequestMethod("DELETE");
        deleteGroup.setRequestProperty("Authorization", basicAuth());
        deleteGroup.connect();
        try {
          deleteGroup.getResponseCode();
        } finally {
          deleteGroup.disconnect();
        }
      }
    });
  }

  private void postGroup(final int serverIndex, final JSONObject payload) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server/groups").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
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
