/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server;

import com.arcadedb.log.LogManager;
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

public class HTTPGraphIT extends BaseGraphServerTest {
  @Test
  public void checkAuthenticationError() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

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
  public void checkNoAuthentication() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");

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
          "http://127.0.0.1:248" + serverIndex + "/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);

        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);

        Assertions.assertEquals(200, connection.getResponseCode());

        Assertions.assertEquals("OK", connection.getResponseMessage());

        Assertions.assertTrue(response.contains("V1"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkQueryInPost() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/query/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
      formatPost(connection, "sql", "select from V1 limit 1", new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);

        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);

        Assertions.assertEquals(200, connection.getResponseCode());

        Assertions.assertEquals("OK", connection.getResponseMessage());

        Assertions.assertTrue(response.contains("V1"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkCommand() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
      formatPost(connection, "sql", "select from V1 limit 1", new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);

        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);

        Assertions.assertEquals(200, connection.getResponseCode());

        Assertions.assertEquals("OK", connection.getResponseMessage());

        Assertions.assertTrue(response.contains("V1"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkRecordLoading() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/document/graph/" + BaseGraphServerTest.root.getIdentity().toString().substring(1)).openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);

        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);

        Assertions.assertEquals(200, connection.getResponseCode());

        Assertions.assertEquals("OK", connection.getResponseMessage());

        Assertions.assertTrue(response.contains("V1"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkRecordCreate() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/document/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));

      final String payload = "{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}";

      connection.setRequestMethod("POST");
      connection.setDoOutput(true);

      connection.connect();

      PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()));
      pw.write(payload);
      pw.close();

      try {
        final String response = readResponse(connection);

        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());

        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);

        Assertions.assertTrue(response.contains("#"));

      } finally {
        connection.disconnect();
      }
    });
  }
}
