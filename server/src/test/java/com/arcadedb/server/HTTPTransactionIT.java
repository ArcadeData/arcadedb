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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;

public class HTTPTransactionIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  @Test
  public void simpleTx() throws Exception {
    testEachServer((serverIndex) -> {
      // BEGIN
      HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/begin/" + DATABASE_NAME).toURL().openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      String sessionId;
      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(204, connection.getResponseCode());
        sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID).trim();

        Assertions.assertNotNull(sessionId);

      } finally {
        connection.disconnect();
      }

      final JSONObject payload = new JSONObject("{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}");

      // CREATE DOCUMENT
      connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").toURL().openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "insert into Person content " + payload, null, new HashMap<>());
      connection.connect();

      final String rid;
      try {
        final String response = readResponse(connection);

        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        final JSONObject responseAsJson = new JSONObject(response);
        Assertions.assertTrue(responseAsJson.has("result"));
        rid = responseAsJson.getJSONArray("result").getJSONObject(0).getString("@rid");
        Assertions.assertTrue(rid.contains("#"));
      } finally {
        connection.disconnect();
      }

      // CANNOT RETRIEVE DOCUMENT OUTSIDE A TX
      try {
        checkDocumentWasCreated(DATABASE_NAME, serverIndex, payload, rid, null);
        Assertions.fail();
      } catch (final Exception e) {
        // EXPECTED
      }

      // RETRIEVE DOCUMENT
      checkDocumentWasCreated(DATABASE_NAME, serverIndex, payload, rid, sessionId);

      // QUERY IN GET
      connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20Person%20limit%201").toURL().openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("Person"));

      } finally {
        connection.disconnect();
      }

      // QUERY IN POST
      connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph").toURL().openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, "sql", "select from Person limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("Person"));
      } finally {
        connection.disconnect();
      }

      // COMMIT
      connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/commit/graph").toURL().openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(204, connection.getResponseCode());
        Assertions.assertNull(connection.getHeaderField(ARCADEDB_SESSION_ID));

      } finally {
        connection.disconnect();
      }

      // RETRIEVE DOCUMENT
      checkDocumentWasCreated(DATABASE_NAME, serverIndex, payload, rid, sessionId);
    });
  }

  @Test
  public void checkUnique() throws Exception {
    testEachServer((serverIndex) -> {
      // BEGIN
      HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/begin/graph").toURL().openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      String sessionId;
      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(204, connection.getResponseCode());
        sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID).trim();

        Assertions.assertNotNull(sessionId);

      } finally {
        connection.disconnect();
      }

      // CREATE DOCUMENT
      connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").toURL().openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject("{\"language\":\"sqlScript\", \"command\":\"" +//
          "CREATE VERTEX TYPE eltdev;" +//
          "CREATE PROPERTY eltdev.SN string;" +//
          "CREATE INDEX ON eltdev (SN) UNIQUE;" +//
          "CREATE VERTEX eltdev SET SN='bubu';" +//
          "CREATE VERTEX eltdev SET SN='bubu';" +//
          "\"}");

      connection.setRequestMethod("POST");
      connection.setDoOutput(true);

      connection.connect();

      final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()));
      pw.write(payload.toString());
      pw.close();

      String response = null;
      try {
        response = readResponse(connection);
        Assertions.fail();
      } catch (final IOException e) {
        response = readError(connection);
        Assertions.assertEquals(503, connection.getResponseCode());
        connection.disconnect();
        Assertions.assertTrue(response.contains("DuplicatedKeyException"));
      }
    });
  }

  public static void checkDocumentWasCreated(final String databaseName, final int serverIndex, final JSONObject payload,
      final String rid, final String sessionId) throws IOException {

    // QUERY IN GET
    final HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + databaseName + "/sql/select%20from%20%23" + rid.substring(
      1)).toURL().openConnection();

    connection.setRequestMethod("GET");
    if (sessionId != null)
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.connect();

    try {
      final String response = readResponse(connection);
      final JSONObject responseAsJson = new JSONObject(response);
      Assertions.assertTrue(responseAsJson.has("result"));
      final JSONObject object = responseAsJson.getJSONArray("result").getJSONObject(0);
      Assertions.assertEquals(200, connection.getResponseCode());
      Assertions.assertEquals("OK", connection.getResponseMessage());
      Assertions.assertEquals(rid, object.remove("@rid").toString());
      Assertions.assertEquals("d", object.remove("@cat"));
      Assertions.assertEquals(payload.toMap(), object.toMap());

    } finally {
      connection.disconnect();
    }
  }
}
