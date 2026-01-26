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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import com.arcadedb.server.http.handler.AbstractQueryHandler;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.*;

class HTTPDocumentIT extends BaseGraphServerTest {
  private final static String DATABASE_NAME = "httpDocument";
  private final        int    TOTAL         = AbstractQueryHandler.DEFAULT_LIMIT + 2;

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Test
  void serverInfo() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/server").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        final String response = readResponse(connection);

        System.out.println("response = " + response);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void serverClusterInfo() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/server?mode=cluster").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");

        final JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.getString("user")).isEqualTo("root");
        assertThat(responseJson.getString("version")).isEqualTo(Constants.getVersion());
        assertThat(responseJson.getString("serverName")).isEqualTo(getServer(serverIndex).getServerName());

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void serverReady() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/ready").openConnection();
      connection.setRequestMethod("GET");
      try {
        connection.connect();
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkAuthenticationError() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME
              + "/sql/select%20from%20Person%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:wrong".getBytes()));
      try {
        connection.connect();
        readResponse(connection);
        fail("Authentication was bypassed!");
      } catch (final IOException e) {
        assertThat(e.toString().contains("403")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkQueryInGet() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME
              + "/sql/select%20from%20Person%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");

        final JSONArray result = new JSONObject(response).getJSONArray("result");
        assertThat(result.length()).isEqualTo(1);

        assertThat(response.contains("Person")).isTrue();

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkQueryInGetWithLimitAboveDefaultCut() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME
              + "/sql/select%20from%20Person%20limit%20" + (TOTAL - 1)).openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");

        final JSONArray result = new JSONObject(response).getJSONArray("result");
        assertThat(result.length()).isEqualTo(TOTAL - 1);

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkQueryInGetWithDefaultLimit() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME
              + "/sql/select%20from%20Person").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");

        final JSONArray result = new JSONObject(response).getJSONArray("result");
        assertThat(result.length()).isEqualTo(AbstractQueryHandler.DEFAULT_LIMIT);

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkQueryInGetWithSqlScript() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME
              + "/sqlscript/select%20from%20Person%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("Person")).isTrue();

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkQueryCommandEncoding() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME
              + "/sql/select%201%20%2B%201%20as%20result").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "TEST: Response: %s", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("result")).isTrue();

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkQueryInPost() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "select from Person limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("Person")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkCommand() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "select from Person limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("Person")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkAsyncCommand() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection post = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

      post.setRequestMethod("POST");
      post.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(post, new JSONObject()
          .put("language", "sql")
          .put("command", "create document type doc;")
          .put("awaitResponse", false));
      post.connect();

      try {
        assertThat(post.getResponseCode()).isEqualTo(202);
      } finally {
        post.disconnect();
      }

      final HttpURLConnection get = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME
              + "/sql/select%20name%20from%20schema%3Atypes").openConnection();
      get.setRequestMethod("GET");
      get.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      get.connect();

      try {
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
          get.connect();
          return get.getResponseCode() == 200;
        });
        final String response = readResponse(get);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(get.getResponseCode()).isEqualTo(200);
        assertThat(get.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("doc")).isTrue();
      } finally {
        get.disconnect();
      }
    });
  }

  @Test
  void checkCommandNoDuplication() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM Person", "studio", Collections.emptyMap());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");

        final JSONObject responseAsJson = new JSONObject(response);

        final List<Object> records = responseAsJson.getJSONObject("result").getJSONArray("records").toList();
        assertThat(records).hasSize(AbstractQueryHandler.DEFAULT_LIMIT);
        for (final Object o : records)
          assertThat(((Map) o).get("@type")).isEqualTo("Person");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void checkRecordCreate() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE DOCUMENT
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject("{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}");
      formatPayload(connection, "sql", "insert into Person content " + payload, null, new HashMap<>());
      connection.connect();

      final String rid;
      try {
        final String response = readResponse(connection);

        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        final JSONObject responseAsJson = new JSONObject(response);
        assertThat(responseAsJson.has("result")).isTrue();
        rid = responseAsJson.getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);
        assertThat(rid.contains("#")).isTrue();
      } finally {
        connection.disconnect();
      }

      HTTPTransactionIT.checkDocumentWasCreated(DATABASE_NAME, serverIndex, payload, rid, null);

    });
  }

  /**
   * Test for GitHub issue #1602: Unable to add Data with Special Characters via The Studio
   * <p>
   * Verifies that special characters (like &, <, >, ", ') are correctly:
   * 1. Stored when sent via the Studio API (with HTML-escaped command)
   * 2. Returned correctly in JSON response
   * 3. Properly escaped for HTML display
   */
  @Test
  void checkSpecialCharactersInData() throws Exception {
    testEachServer((serverIndex) -> {
      // Test value with special characters as described in issue #1602
      final String testValue = "LdhgfdY&hgff2&a";

      // First, create the document type via command (ignore error if already exists)
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "CREATE DOCUMENT TYPE Field", null, new HashMap<>());
      connection.connect();
      try {
        // Ignore error if type already exists
        connection.getResponseCode();
      } finally {
        connection.disconnect();
      }

      // Insert data with special characters using Studio serializer
      // The Studio frontend escapes HTML before sending, so we simulate that behavior
      final String escapedCommand = "INSERT INTO Field SET value = &#039;LdhgfdY&amp;hgff2&amp;a&#039;";

      connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", escapedCommand, "studio", new HashMap<>());
      connection.connect();

      final String rid;
      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final JSONObject responseAsJson = new JSONObject(response);
        assertThat(responseAsJson.has("result")).isTrue();

        final JSONArray records = responseAsJson.getJSONObject("result").getJSONArray("records");
        assertThat(records.length()).isEqualTo(1);

        // The value should be stored correctly (without HTML entities)
        final JSONObject record = records.getJSONObject(0);
        rid = record.getString(RID_PROPERTY);

        // Check the value is correct in the response
        final String returnedValue = record.getString("value");
        assertThat(returnedValue)
            .as("Special characters should be preserved in the JSON response")
            .isEqualTo(testValue);
      } finally {
        connection.disconnect();
      }

      // Query the record back to verify the data is stored correctly
      connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "SELECT FROM " + rid, "studio", new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        final JSONObject responseAsJson = new JSONObject(response);
        final JSONArray records = responseAsJson.getJSONObject("result").getJSONArray("records");
        assertThat(records.length()).isEqualTo(1);

        final JSONObject record = records.getJSONObject(0);
        final String returnedValue = record.getString("value");

        // The value should still contain the special characters
        assertThat(returnedValue)
            .as("Special characters should be preserved when querying the record")
            .isEqualTo(testValue);

        // Also verify the raw JSON contains the properly encoded value
        // (Gson may use \u0026 for & which is valid JSON)
        assertThat(response)
            .as("JSON response should contain the value (possibly with unicode escapes)")
            .containsPattern("LdhgfdY.*hgff2.*a");

      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Test that various special characters are handled correctly through the HTTP API.
   */
  @Test
  void checkVariousSpecialCharacters() throws Exception {
    testEachServer((serverIndex) -> {
      // Create the document type (ignore error if already exists)
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "CREATE DOCUMENT TYPE SpecialChars", null, new HashMap<>());
      connection.connect();
      try {
        // Ignore error if type already exists
        connection.getResponseCode();
      } finally {
        connection.disconnect();
      }

      // Test various special characters - send without HTML escaping (direct JSON)
      final String[] testValues = {
          "Hello & World",
          "Less < Greater >",
          "Quote \" Test",
          "Single ' Quote",
          "All together: & < > \" '",
          "URL encoded: foo%20bar",
          "Unicode: \u00e9\u00e8\u00ea",
          "Multiple &&& ampersands &&&"
      };

      for (final String testValue : testValues) {
        // Insert using parameterized query (cleaner approach without HTML escaping)
        connection = (HttpURLConnection) new URL(
            "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

        final JSONObject payload = new JSONObject();
        payload.put("language", "sql");
        payload.put("command", "INSERT INTO SpecialChars SET value = :value");
        payload.put("serializer", "studio");
        final JSONObject params = new JSONObject();
        params.put("value", testValue);
        payload.put("params", params);

        formatPayload(connection, payload);
        connection.connect();

        try {
          final String response = readResponse(connection);
          assertThat(connection.getResponseCode()).isEqualTo(200);

          final JSONObject responseAsJson = new JSONObject(response);
          final JSONArray records = responseAsJson.getJSONObject("result").getJSONArray("records");
          assertThat(records.length()).isEqualTo(1);

          final String returnedValue = records.getJSONObject(0).getString("value");
          assertThat(returnedValue)
              .as("Special characters should be preserved for value: " + testValue)
              .isEqualTo(testValue);

        } finally {
          connection.disconnect();
        }
      }
    });
  }

  @Override
  protected void populateDatabase() {
    final Database database = getDatabase(0);
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      assertThat(schema.existsType("Person")).isFalse();
      final DocumentType v = schema.buildDocumentType().withName("Person").withTotalBuckets(3).create();
      v.createProperty("id", Long.class);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");

      for (int i = 0; i < TOTAL; i++)
        database.newDocument("Person").set("id", i).set("name", "John" + i).save();
    });
  }
}
