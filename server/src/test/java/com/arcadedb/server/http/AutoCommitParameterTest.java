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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for the autoCommit parameter in HTTP query/command endpoints.
 */
public class AutoCommitParameterTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  @Test
  void testExplicitAutoCommitTrueWithCommand() throws Exception {
    testEachServer((serverIndex) -> {
      // Create test document type
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE TestDoc");

      // Execute INSERT with explicit autoCommit=true
      HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "INSERT INTO TestDoc SET name='test1', value=42");
      payload.put("autoCommit", true);

      formatPayload(connection, payload);
      connection.connect();

      try {
        String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        // Verify data was committed
        final Database database = getServerDatabase(serverIndex, DATABASE_NAME);
        final ResultSet result = database.query("sql", "SELECT FROM TestDoc WHERE name='test1'");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().<Integer>getProperty("value")).isEqualTo(42);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void testExplicitAutoCommitFalseWithCommand() throws Exception {
    testEachServer((serverIndex) -> {
      // Create test document type if not exists
      try {
        executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE TestDoc2");
      } catch (Exception e) {
        // Type may already exist
      }

      // Execute INSERT with explicit autoCommit=false (no transaction)
      HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "INSERT INTO TestDoc2 SET name='test2', value=99");
      payload.put("autoCommit", false);

      formatPayload(connection, payload);
      connection.connect();

      try {
        String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        // Verify data exists (even without transaction, single command should work)
        final Database database = getServerDatabase(serverIndex, DATABASE_NAME);
        final ResultSet result = database.query("sql", "SELECT FROM TestDoc2 WHERE name='test2'");
        assertThat(result.hasNext()).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void testDefaultBehaviorWithCommand() throws Exception {
    testEachServer((serverIndex) -> {
      // Create test document type
      try {
        executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE TestDoc3");
      } catch (Exception e) {
        // Type may already exist
      }

      // Execute INSERT without autoCommit parameter (default behavior)
      HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "INSERT INTO TestDoc3 SET name='test3', value=123");
      // No autoCommit parameter - should use default behavior

      formatPayload(connection, payload);
      connection.connect();

      try {
        String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        // Verify data was committed (default behavior)
        final Database database = getServerDatabase(serverIndex, DATABASE_NAME);
        final ResultSet result = database.query("sql", "SELECT FROM TestDoc3 WHERE name='test3'");
        assertThat(result.hasNext()).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void testAutoCommitTrueWithQuery() throws Exception {
    testEachServer((serverIndex) -> {
      // Create test data
      executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE TestDoc4");
      executeCommand(serverIndex, "sql", "INSERT INTO TestDoc4 SET name='query_test', value=555");

      // Execute query with autoCommit=true
      HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "SELECT FROM TestDoc4 WHERE name='query_test'");
      payload.put("autoCommit", true);

      formatPayload(connection, payload);
      connection.connect();

      try {
        String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.has("result")).isTrue();
        assertThat(responseJson.getJSONArray("result").length()).isEqualTo(1);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void testAutoCommitParameterWithSessionId() throws Exception {
    testEachServer((serverIndex) -> {
      // Create test document type
      try {
        executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE TestDoc5");
      } catch (Exception e) {
        // Type may already exist
      }

      // BEGIN transaction
      HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/begin/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      String sessionId;
      try {
        readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(204);
        sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID).trim();
        assertThat(sessionId).isNotNull();
      } finally {
        connection.disconnect();
      }

      // Execute with session + autoCommit parameter (should be ignored with warning)
      connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "INSERT INTO TestDoc5 SET name='session_test', value=777");
      payload.put("autoCommit", true); // Should be ignored

      formatPayload(connection, payload);
      connection.connect();

      try {
        String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }

      // Commit the session transaction
      connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/commit/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }

      // Verify data was committed
      final Database database = getServerDatabase(serverIndex, DATABASE_NAME);
      final ResultSet result = database.query("sql", "SELECT FROM TestDoc5 WHERE name='session_test'");
      assertThat(result.hasNext()).isTrue();
    });
  }

  @Test
  void testAutoCommitTrueWithRetries() throws Exception {
    testEachServer((serverIndex) -> {
      // Create test document type
      try {
        executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE TestDoc6");
      } catch (Exception e) {
        // Type may already exist
      }

      // Execute INSERT with autoCommit=true and retries
      HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "INSERT INTO TestDoc6 SET name='retry_test', value=888");
      payload.put("autoCommit", true);
      payload.put("retries", 3);

      formatPayload(connection, payload);
      connection.connect();

      try {
        String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        // Verify data was committed
        final Database database = getServerDatabase(serverIndex, DATABASE_NAME);
        final ResultSet result = database.query("sql", "SELECT FROM TestDoc6 WHERE name='retry_test'");
        assertThat(result.hasNext()).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void testAutoCommitFalseForReadOperation() throws Exception {
    testEachServer((serverIndex) -> {
      // Create test data
      try {
        executeCommand(serverIndex, "sql", "CREATE DOCUMENT TYPE TestDoc7");
        executeCommand(serverIndex, "sql", "INSERT INTO TestDoc7 SET name='read_test', value=999");
      } catch (Exception e) {
        // Data may already exist
      }

      // Execute query with autoCommit=false (read without transaction)
      HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "SELECT FROM TestDoc7 WHERE name='read_test'");
      payload.put("autoCommit", false);

      formatPayload(connection, payload);
      connection.connect();

      try {
        String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.has("result")).isTrue();
        assertThat(responseJson.getJSONArray("result").length()).isGreaterThan(0);
      } finally {
        connection.disconnect();
      }
    });
  }
}
