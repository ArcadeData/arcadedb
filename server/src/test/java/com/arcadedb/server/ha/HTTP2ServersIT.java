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
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

public class HTTP2ServersIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  public void testServerInfo() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server?mode=cluster").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        connection.connect();
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void propagationOfSchema() throws Exception {
    testEachServer((serverIndex) -> {
      // CREATE THE SCHEMA ON BOTH SERVER, ONE TYPE PER SERVER
      final String response = command(serverIndex, "create vertex type VertexType" + serverIndex);
      assertThat(response).contains("VertexType" + serverIndex)
          .withFailMessage("Type " + (("VertexType" + serverIndex) + " not found on server " + serverIndex));

    });

    // Wait for schema propagation using awaitility
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> {
          // CHECK THE SCHEMA HAS BEEN PROPAGATED to both servers
          for (int i = 0; i < getServerCount(); i++) {
            final int serverIndex = i;
            try {
              command(serverIndex, "select from VertexType" + serverIndex);
            } catch (Exception e) {
              LogManager.instance().log(this, Level.FINE, "Schema not yet propagated to server " + serverIndex, e);
              return false;
            }
          }
          return true;
        });
  }

  @Test
  public void checkQuery() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "TEST: Response: %s", null, response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");
        assertThat(response.contains("V1")).isTrue();

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkDeleteGraphElements() throws Exception {

    // Wait for initial synchronization
    waitForReplicationIsCompleted(0);

    testEachServer((serverIndex) -> {
      LogManager.instance().log(this, Level.FINE, "TESTS SERVER " + serverIndex);

      final String v1 = new JSONObject(
          command(serverIndex, "create vertex V1 content {\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}")).getJSONArray(
          "result").getJSONObject(0).getString(RID_PROPERTY);

      if (!getServer(serverIndex).getHA().isLeader())
        waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) -> {
        try {
          assertThat(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result")).isNotEmpty().
              withFailMessage("executed on server " + serverIndex + " checking on server " + serverIndex);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for V1 on server " + checkServer);
          throw e;
        }
      });

      final String v2 = new JSONObject(
          command(serverIndex, "create vertex V1 content {\"name\":\"John\",\"surname\":\"Red\",\"age\":50}")).getJSONArray(
          "result").getJSONObject(0).getString(RID_PROPERTY);

      if (!getServer(serverIndex).getHA().isLeader())
        waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) -> {
        try {

          assertThat(new JSONObject(command(checkServer, "select from " + v2)).getJSONArray("result")).isNotEmpty()
              .withFailMessage("executed on server " + serverIndex + " checking on server " + serverIndex);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for V2 on server " + checkServer);
          throw e;
        }
      });

      final String e1 = new JSONObject(command(serverIndex, "create edge E1 from " + v1 + " to " + v2)).getJSONArray("result")
          .getJSONObject(0).getString(RID_PROPERTY);

      if (!getServer(serverIndex).getHA().isLeader())
        waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) -> {
        try {
          assertThat(new JSONObject(command(checkServer, "select from " + e1)).getJSONArray("result")).isNotEmpty()
              .withFailMessage("executed on server " + serverIndex + " checking on server " + serverIndex);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking on E1 on server " + checkServer);
          throw e;
        }
      });

      final String v3 = new JSONObject(
          command(serverIndex, "create vertex V1 content {\"name\":\"Nikola\",\"surname\":\"Tesla\",\"age\":150}")).getJSONArray(
          "result").getJSONObject(0).getString(RID_PROPERTY);

      if (!getServer(serverIndex).getHA().isLeader())
        waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) -> {
        try {
          assertThat(new JSONObject(command(checkServer, "select from " + v3)).getJSONArray("result")).isNotEmpty()
              .withFailMessage("executed on server " + serverIndex + " checking on server " + serverIndex);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for V3 on server " + checkServer);
          throw e;
        }
      });

      final String e2 = new JSONObject(command(serverIndex, "create edge E2 from " + v2 + " to " + v3)).getJSONArray("result")
          .getJSONObject(0).getString(RID_PROPERTY);

      if (!getServer(serverIndex).getHA().isLeader())
        waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) -> {
        try {
          assertThat(new JSONObject(command(checkServer, "select from " + e2)).getJSONArray("result")).isNotEmpty()
              .withFailMessage("executed on server " + serverIndex + " checking on server " + serverIndex);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on checking for E2 on server " + checkServer);
          throw e;
        }
      });

      command(serverIndex, "delete from " + v1);

      if (!getServer(serverIndex).getHA().isLeader())
        waitForReplicationIsCompleted(serverIndex);

      testEachServer((checkServer) -> {
        try {
          new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result");
          fail("executed on server " + serverIndex + " checking on server " + serverIndex);
        } catch (FileNotFoundException e) {
          //  EXPECTED
        }

        try {
          new JSONObject(command(checkServer, "select from " + e1)).getJSONArray("result");
          fail("executed on server " + serverIndex + " checking on server " + serverIndex);
        } catch (FileNotFoundException e) {
          //  EXPECTED
        }
      });
    });
  }

  @Test
  public void testHAConfiguration() {
    for (ArcadeDBServer server : getServers()) {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(database.getLeaderAddress()).isNotNull();
      assertThat(database.getReplicaAddresses().isEmpty()).isFalse();
    }
  }
}
