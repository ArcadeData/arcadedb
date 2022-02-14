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

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

public class HTTPDocumentIT extends BaseGraphServerTest {
  private final static String DATABASE_NAME = "httpDocument";

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    final Database database = getDatabase(0);
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      Assertions.assertFalse(schema.existsType("Person"));
      DocumentType v = schema.createDocumentType("Person", 3);
      v.createProperty("id", Long.class);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id");

      for (int i = 0; i < 100; i++)
        database.newDocument("Person").set("id", i).set("name", "Elon" + i).save();
    });
  }

  @Test
  public void checkAuthenticationError() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME + "/sql/select%20from%20Person%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:wrong".getBytes()));
      try {
        connection.connect();
        readResponse(connection);
        Assertions.fail("Authentication was bypassed!");
      } catch (IOException e) {
        Assertions.assertTrue(e.toString().contains("403"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkQueryInGet() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME + "/sql/select%20from%20Person%20limit%201").openConnection();

      connection.setRequestMethod("GET");
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
    });
  }

  @Test
  public void checkQueryInPost() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPost(connection, "sql", "select from Person limit 1", null, new HashMap<>());
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
    });
  }

  @Test
  public void checkCommand() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPost(connection, "sql", "select from Person limit 1", null, new HashMap<>());
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
    });
  }

  @Test
  public void checkCommandNoDuplication() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPost(connection, "sql", "SELECT FROM Person", "graph", Collections.emptyMap());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());

        final JSONObject responseAsJson = new JSONObject(response);

        final List<Object> records = responseAsJson.getJSONObject("result").getJSONArray("records").toList();
        Assertions.assertEquals(100, records.size());
        for (Object o : records)
          Assertions.assertTrue(((Map) o).get("@type").equals("Person"));
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkRecordCreate() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/document/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
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
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        final JSONObject responseAsJson = new JSONObject(response);
        Assertions.assertTrue(responseAsJson.has("result"));
        rid = responseAsJson.getString("result");
        Assertions.assertTrue(rid.contains("#"));
      } finally {
        connection.disconnect();
      }

      HttpURLConnection connection2 = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/document/" + DATABASE_NAME + "/" + rid.substring(1)).openConnection();

      connection2.setRequestMethod("GET");
      connection2.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection2.connect();

      try {
        final String response = readResponse(connection2);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        final JSONObject responseAsJson = new JSONObject(response);
        Assertions.assertTrue(responseAsJson.has("result"));
        final JSONObject object = responseAsJson.getJSONObject("result");
        Assertions.assertEquals(200, connection2.getResponseCode());
        Assertions.assertEquals("OK", connection2.getResponseMessage());
        Assertions.assertEquals(rid, object.remove("@rid").toString());
        Assertions.assertEquals("d", object.remove("@cat"));
        Assertions.assertEquals(payload.toMap(), object.toMap());

      } finally {
        connection2.disconnect();
      }
    });
  }

}
