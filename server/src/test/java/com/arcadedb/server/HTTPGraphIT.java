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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class HTTPGraphIT extends BaseGraphServerTest {
  @Test
  public void checkAuthenticationError() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:wrong".getBytes()));
      try {
        connection.connect();
        readResponse(connection);
        fail("Authentication was bypassed!");
      } catch (final IOException e) {
        assertThat(e.toString()).contains("403");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkNoAuthentication() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      try {
        connection.connect();
        readResponse(connection);
        fail("Authentication was bypassed!");
      } catch (final IOException e) {
        assertThat(e.toString()).contains("401");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkQueryInGet() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("V1")).isTrue();

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkQueryInPost() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "select from V1 limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("V1")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommand() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "select from V1 limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("V1")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommandLoadByRIDWithParameters() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM :rid", null, Map.of("rid", "#1:0"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("V1")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommandLoadByRIDInWhereWithParameters() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " where @rid = :rid", null,
          Map.of("rid", "#1:0"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("V1")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/discussions/468
   */
  @Test
  public void checkCommandLoadByRIDIn() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " where @rid in (#1:0)", null, Collections.emptyMap());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("V1")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/discussions/468
   */
  @Test
  public void checkCommandLet() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql",
          "SELECT $p from " + VERTEX1_TYPE_NAME + " let pid = @rid, p = (select from " + VERTEX1_TYPE_NAME
              + " where @rid = $parent.pid)", null, Collections.emptyMap());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response).contains("#1:0");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommandNoDuplication() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject responseAsJson = executeCommand(serverIndex, "sql", "SELECT FROM E1");

      final List<Object> vertices = responseAsJson.getJSONObject("result").getJSONArray("vertices").toList();
      assertThat(vertices).hasSize(2);
      for (final Object o : vertices)
        assertThat(((Map) o).get("t").equals("V1") || ((Map) o).get("t").equals("V2")).isTrue();

      final List<Object> records = responseAsJson.getJSONObject("result").getJSONArray("records").toList();
      assertThat(records).hasSize(1);
      for (final Object o : records)
//        Assertions.assertTrue(
//            ((Map) o).get("@type").equals("V1") || ((Map) o).get("@type").equals("V2") || ((Map) o).get("@type").equals("E1"));

        assertThat(((Map) o).get("@type").equals("V1") || ((Map) o).get("@type").equals("V2") || ((Map) o).get("@type")
            .equals("E1")).isTrue();
      final List<Object> edges = responseAsJson.getJSONObject("result").getJSONArray("edges").toList();
      assertThat(edges).hasSize(1);
      for (final Object o : edges)
        assertThat(((Map) o).get("t")).isEqualTo("E1");
    });
  }

  @Test
  public void checkDatabaseExists() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/exists/graph/").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getBoolean("result")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkDatabaseList() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/databases").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        final JSONArray databases = new JSONObject(response).getJSONArray("result");
        assertThat(databases.length()).isEqualTo(1).withFailMessage("Found the following databases: " + databases);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void createAndDropDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE DATABASE 'JUSTFORFUN'
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "create database justforfun"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getString("result")).isEqualTo("ok");

      } finally {
        connection.disconnect();
      }

      // CHECK EXISTENCE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/exists/justforfun").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getBoolean("result")).isTrue();

      } finally {
        connection.disconnect();
      }

      // DROP DATABASE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "drop database justforfun"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getString("result")).isEqualTo("ok");

      } finally {
        connection.disconnect();
      }

      // CHECK NOT EXISTENCE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/exists/justforfun").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getBoolean("result")).isFalse();

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void closeAndReopenDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE DATABASE 'JUSTFORFUN'
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "create database closeAndReopen"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getString("result")).isEqualTo("ok");

      } finally {
        connection.disconnect();
      }

      // CLOSE DATABASE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "close database closeAndReopen"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getString("result")).isEqualTo("ok");

      } finally {
        connection.disconnect();
      }

      // RE-OPEN DATABASE
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "open database closeAndReopen"));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getString("result")).isEqualTo("ok");

      } finally {
        connection.disconnect();
      }

      // CHECK EXISTENCE
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/exists/closeAndReopen").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(new JSONObject(response).getBoolean("result")).isTrue();

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void testEmptyDatabaseName() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE DATABASE ''
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, new JSONObject().put("command", "create database "));
      connection.connect();

      try {
        readResponse(connection);
        fail("Empty database should be an error");
      } catch (final Exception e) {
        assertThat(connection.getResponseCode()).isEqualTo(400);

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void testOneEdgePerTx() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sqlscript",
          """
              CREATE VERTEX TYPE Photos;
              CREATE VERTEX TYPE Users;
              CREATE EDGE TYPE HasUploaded;""");

      executeCommand(serverIndex, "sql", "CREATE VERTEX Users SET id = 'u1111'");

      executeCommand(serverIndex, "sqlscript",
          """
              BEGIN;
              LET photo = CREATE VERTEX Photos SET id = "p12345", name = "download1.jpg";
              LET user = SELECT FROM Users WHERE id = "u1111";
              LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET type = "User_Photos";
              COMMIT RETRY 30;
              RETURN $photo;""");

      executeCommand(serverIndex, "sqlscript",
          """
              BEGIN;
              LET photo = CREATE VERTEX Photos SET id = "p2222", name = "download2.jpg";
              LET user = SELECT FROM Users WHERE id = "u1111";
              LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET type = "User_Photos";
              COMMIT RETRY 30;
              RETURN $photo;""");

      executeCommand(serverIndex, "sqlscript",
          """
              BEGIN;LET photo = CREATE VERTEX Photos SET id = "p5555", name = "download3.jpg";
              LET user = SELECT FROM Users WHERE id = "u1111";
              LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET type = "User_Photos";
              COMMIT RETRY 30;
              RETURN $photo;""");

      final JSONObject responseAsJsonSelect = executeCommand(serverIndex, "sql",
          """
              SELECT expand( outE('HasUploaded') ) FROM Users WHERE id = "u1111"
              """);

      String response = responseAsJsonSelect.toString();
      assertThat(JsonPath.<Integer>read(response, "$..records.length()")).isEqualTo(3);
      assertThat(JsonPath.<String>read(response, "$.user")).isEqualTo("root");
      assertThat(JsonPath.<String>read(response, "$.result.vertices[0].p.@type")).isEqualTo("Photos");
      assertThat(JsonPath.<String>read(response, "$.result.vertices[0].p.@cat")).isEqualTo("v");
      assertThat(JsonPath.<String>read(response, "$.result.vertices[0].t")).isEqualTo("Photos");

      assertThat(JsonPath.<String>read(response, "$.result.edges[0].p.@type")).isEqualTo("HasUploaded");
      assertThat(JsonPath.<String>read(response, "$.result.edges[0].p.@cat")).isEqualTo("e");
      assertThat(JsonPath.<String>read(response, "$.result.edges[0].p.@in")).isNotEmpty();
      assertThat(JsonPath.<String>read(response, "$.result.edges[0].p.@out")).isNotEmpty();

      assertThat(JsonPath.<String>read(response, "$.result.records[0].@type")).isEqualTo("HasUploaded");
      assertThat(JsonPath.<String>read(response, "$.result.records[0].@cat")).isEqualTo("e");
      assertThat(JsonPath.<String>read(response, "$.result.records[0].@in")).isNotEmpty();
      assertThat(JsonPath.<String>read(response, "$.result.records[0].@out")).isNotEmpty();
    });
  }

  @Test
  public void testOneEdgePerTxMultiThreads() throws Exception {
    testEachServer((serverIndex) -> {
      executeCommand(serverIndex, "sqlscript", "create vertex type Photos;create vertex type Users;create edge type HasUploaded;");

      executeCommand(serverIndex, "sql", "create vertex Users set id = 'u1111'");

      final int THREADS = 4;
      final int SCRIPTS = 100;
      final AtomicInteger atomic = new AtomicInteger();

      final Thread[] threads = new Thread[THREADS];
      for (int i = 0; i < THREADS; i++) {
        threads[i] = new Thread(() -> {
          for (int j = 0; j < SCRIPTS; j++) {
            try {
              final JSONObject responseAsJson = executeCommand(serverIndex, "sqlscript",
                  """
                      BEGIN ISOLATION REPEATABLE_READ;
                      LET photo = CREATE vertex Photos SET id = uuid(), name = "downloadX.jpg";
                      LET user = SELECT FROM Users WHERE id = "u1111";
                      LET userEdge = Create edge HasUploaded FROM $user to $photo set type = "User_Photos";
                      commit retry 100;return $photo;""");

              atomic.incrementAndGet();

              if (responseAsJson == null) {
                LogManager.instance().log(this, Level.SEVERE, "Error on execution from thread %d", Thread.currentThread().threadId());
                continue;
              }

              assertThat(responseAsJson.getJSONObject("result").getJSONArray("records")).isNotNull();

            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        });
        threads[i].start();
      }

      for (int i = 0; i < THREADS; i++)
        threads[i].join(60 * 1_000);

      assertThat(atomic.get()).isEqualTo(THREADS * SCRIPTS);

      final JSONObject responseAsJsonSelect = executeCommand(serverIndex, "sql",
          "SELECT id FROM ( SELECT expand( outE('HasUploaded') ) FROM Users WHERE id = \"u1111\" )");

      assertThat(responseAsJsonSelect.getJSONObject("result").getJSONArray("records").length()).isEqualTo(THREADS * SCRIPTS);
    });
  }
}
