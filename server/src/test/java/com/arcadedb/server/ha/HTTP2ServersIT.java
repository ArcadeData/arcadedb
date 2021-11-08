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
 */
package com.arcadedb.server.ha;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

public class HTTP2ServersIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  public void propagationOfSchema() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE SCHEMA ON BOTH SERVER, ONE TYPE PER SERVER
      String response = command(serverIndex, "create vertex type VertexType" + serverIndex);
      Assertions.assertTrue(response.contains("VertexType" + serverIndex), "Type " + (("VertexType" + serverIndex) + " not found on server " + serverIndex));
    });

    Thread.sleep(1000);

    // CHECK THE SCHEMA HAS BEEN PROPAGATED
    testEachServer((serverIndex) -> {
      command(serverIndex, "select from VertexType" + serverIndex);
    });
  }

  @Test
  public void checkQuery() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + +serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.INFO, "TEST: Response: %s", null, response);
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
          "http://127.0.0.1:248" + serverIndex + "/api/v1/document/graph/" + BaseGraphServerTest.root.getIdentity().toString().substring(1)).openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.INFO, "TEST: Response: %s", null, response);
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
      createRecord(serverIndex, "{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}");
    });
  }

  @Test
  public void checkDeleteGraphElements() throws Exception {
    //testEachServer((serverIndex) -> {
    final int serverIndex = 0;
      String v1 = new JSONObject(createRecord(serverIndex, "{\"@type\":\"V1\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}")).getString("result");
      String v2 = new JSONObject(createRecord(serverIndex, "{\"@type\":\"V1\",\"name\":\"Elon\",\"surname\":\"Musk\",\"age\":50}")).getString("result");
      String e1 = new JSONObject(command(serverIndex, "create edge E1 from " + v1 + " to " + v2)).getJSONArray("result").getJSONObject(0).getString("@rid");
      String v3 = new JSONObject(createRecord(serverIndex, "{\"@type\":\"V1\",\"name\":\"Nikola\",\"surname\":\"Tesla\",\"age\":150}")).getString("result");
      String e2 = new JSONObject(command(serverIndex, "create edge E2 from " + v2 + " to " + v3)).getJSONArray("result").getJSONObject(0).getString("@rid");

      command(serverIndex, "delete from " + v1);

      testEachServer((checkServer) -> {
        Assertions.assertTrue(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result").isEmpty());
        Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + v2)).getJSONArray("result").isEmpty());
        Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + v3)).getJSONArray("result").isEmpty());
        Assertions.assertTrue(new JSONObject(command(checkServer, "select from " + e1)).getJSONArray("result").isEmpty());
        Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + e2)).getJSONArray("result").isEmpty());
      });
//    });
  }

  private String createRecord(final int serverIndex, final String payload) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/document/graph").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);

    connection.connect();

    PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()));
    pw.write(payload);
    pw.close();

    try {
      final String response = readResponse(connection);

      Assertions.assertEquals(200, connection.getResponseCode());
      Assertions.assertEquals("OK", connection.getResponseMessage());
      LogManager.instance().log(this, Level.INFO, "TEST: Response: %s", null, response);
      Assertions.assertTrue(response.contains("#"));

      return response;

    } finally {
      connection.disconnect();
    }
  }

  private String command(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection initialConnection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
    try {

      initialConnection.setRequestMethod("POST");
      initialConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPost(initialConnection, "sql", command, new HashMap<>());
      initialConnection.connect();

      final String response = readResponse(initialConnection);

      LogManager.instance().log(this, Level.INFO, "Response: %s", null, response);
      Assertions.assertEquals(200, initialConnection.getResponseCode());
      Assertions.assertEquals("OK", initialConnection.getResponseMessage());
      return response;

    } finally {
      initialConnection.disconnect();
    }
  }
}
