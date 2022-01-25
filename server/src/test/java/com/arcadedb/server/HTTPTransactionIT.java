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

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;

import com.arcadedb.log.LogManager;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;

public class HTTPTransactionIT extends BaseGraphServerTest {

  @Test
  public void simpleTx() throws Exception {
    testEachServer((serverIndex) -> {
      // BEGIN
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/begin/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      String sessionId;
      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);
        Assertions.assertEquals(204, connection.getResponseCode());
        sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID).trim();

        Assertions.assertNotNull(sessionId);

      } finally {
        connection.disconnect();
      }

      // CREATE DOCUMENT
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/document/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      final JSONObject payload = new JSONObject("{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}");

      connection.setRequestMethod("POST");
      connection.setDoOutput(true);

      connection.connect();

      PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()));
      pw.write(payload.toString());
      pw.close();

      final String rid;
      try {
        final String response = readResponse(connection);

        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);
        final JSONObject responseAsJson = new JSONObject(response);
        Assertions.assertTrue(responseAsJson.has("result"));
        rid = responseAsJson.getString("result");
        Assertions.assertTrue(rid.contains("#"));
      } finally {
        connection.disconnect();
      }

      // CANNOT RETRIEVE DOCUMENT OUTSIDE A TX
      try {
        checkDocumentWasCreated(serverIndex, payload, rid, null);
        Assertions.fail();
      } catch (Exception e) {
        // EXPECTED
      }

      // RETRIEVE DOCUMENT
      checkDocumentWasCreated(serverIndex, payload, rid, sessionId);

      // QUERY IN GET
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20Person%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("Person"));

      } finally {
        connection.disconnect();
      }

      // QUERY IN POST
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPost(connection, "sql", "select from Person limit 1", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
        Assertions.assertTrue(response.contains("Person"));
      } finally {
        connection.disconnect();
      }

      // COMMIT
      connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/commit/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);
        Assertions.assertEquals(204, connection.getResponseCode());
        Assertions.assertNull(connection.getHeaderField(ARCADEDB_SESSION_ID));

      } finally {
        connection.disconnect();
      }

      // RETRIEVE DOCUMENT
      checkDocumentWasCreated(serverIndex, payload, rid, sessionId);
    });
  }

  private void checkDocumentWasCreated(int serverIndex, JSONObject payload, String rid, String sessionId) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/document/graph/" + rid.substring(1)).openConnection();

    connection.setRequestMethod("GET");
    if (sessionId != null)
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.connect();

    try {
      final String response = readResponse(connection);
      LogManager.instance().log(this, Level.INFO, "Response: ", null, response);
      final JSONObject responseAsJson = new JSONObject(response);
      Assertions.assertTrue(responseAsJson.has("result"));
      final JSONObject object = responseAsJson.getJSONObject("result");
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
