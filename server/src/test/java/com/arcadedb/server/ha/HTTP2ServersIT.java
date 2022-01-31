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
package com.arcadedb.server.ha;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.logging.Level;

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

    Thread.sleep(300);

    // CHECK THE SCHEMA HAS BEEN PROPAGATED
    testEachServer((serverIndex) -> command(serverIndex, "select from VertexType" + serverIndex));
  }

  @Test
  public void checkQuery() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "TEST: Response: %s", null, response);
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
        LogManager.instance().log(this, Level.FINE, "TEST: Response: %s", null, response);
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
    testEachServer((serverIndex) -> createRecord(serverIndex, "{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}"));
  }

  @Test
  public void checkDeleteGraphElements() throws Exception {
    testEachServer((serverIndex) -> {
      LogManager.instance().log(this, Level.FINE, "TESTS SERVER " + serverIndex);

      String v1 = new JSONObject(createRecord(serverIndex, "{\"@type\":\"V1\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}")).getString("result");

      if (!getServer(serverIndex).getHA().isLeader())
        Thread.sleep(300);

      testEachServer((checkServer) -> {
        try {
          Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result").isEmpty(), "server " + serverIndex);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for V1 on server " + checkServer);
          throw e;
        }
      });

      String v2 = new JSONObject(createRecord(serverIndex, "{\"@type\":\"V1\",\"name\":\"Elon\",\"surname\":\"Musk\",\"age\":50}")).getString("result");

      if (!getServer(serverIndex).getHA().isLeader())
        Thread.sleep(300);

      testEachServer((checkServer) -> {
        try {
          Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + v2)).getJSONArray("result").isEmpty(), "server " + serverIndex);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for V2 on server " + checkServer);
          throw e;
        }
      });

      String e1 = new JSONObject(command(serverIndex, "create edge E1 from " + v1 + " to " + v2)).getJSONArray("result").getJSONObject(0).getString("@rid");

      if (!getServer(serverIndex).getHA().isLeader())
        Thread.sleep(300);

      testEachServer((checkServer) -> {
        try {
          Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + e1)).getJSONArray("result").isEmpty(), "server " + serverIndex);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking on E1 on server " + checkServer);
          throw e;
        }
      });

      String v3 = new JSONObject(createRecord(serverIndex, "{\"@type\":\"V1\",\"name\":\"Nikola\",\"surname\":\"Tesla\",\"age\":150}")).getString("result");

      if (!getServer(serverIndex).getHA().isLeader())
        Thread.sleep(300);

      testEachServer((checkServer) -> {
        try {
          Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + v3)).getJSONArray("result").isEmpty(), "server " + serverIndex);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for V3 on server " + checkServer);
          throw e;
        }
      });

      String e2 = new JSONObject(command(serverIndex, "create edge E2 from " + v2 + " to " + v3)).getJSONArray("result").getJSONObject(0).getString("@rid");

      if (!getServer(serverIndex).getHA().isLeader())
        Thread.sleep(300);

      testEachServer((checkServer) -> {
        try {
          Assertions.assertFalse(new JSONObject(command(checkServer, "select from " + e2)).getJSONArray("result").isEmpty(), "server " + serverIndex);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for E2 on server " + checkServer);
          throw e;
        }
      });

      command(serverIndex, "delete from " + v1);

      if (!getServer(serverIndex).getHA().isLeader())
        Thread.sleep(300);

      testEachServer((checkServer) -> {
        try {
          Assertions.assertTrue(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result").isEmpty(), "server " + serverIndex);
          Assertions.assertTrue(new JSONObject(command(checkServer, "select from " + e1)).getJSONArray("result").isEmpty(), "server " + serverIndex);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for right deletion on server " + checkServer);
          throw e;
        }
      });
    });
  }
}
