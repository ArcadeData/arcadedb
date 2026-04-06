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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

class HTTP2ServersCreateReplicatedDatabaseIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  @org.junit.jupiter.api.Disabled("Database creation via HTTP is not replicated to followers with Ratis. See TODO in arcadedb-ha-26.4.1.md")
  void createReplicatedDatabase() throws Exception {
    // CREATE DATABASE ON THE LEADER (database creation is a server-level op, not replicated via Ratis).
    // With Ratis, the leader creates the DB locally and followers auto-create it when the first
    // replicated transaction arrives.
    final int leaderPort = getLeaderServer().getHttpServer().getPort();
    final HttpURLConnection dbConn = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + leaderPort + "/api/v1/server").openConnection();
    dbConn.setRequestMethod("POST");
    dbConn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(dbConn, new JSONObject().put("command", "create database " + getDatabaseName()));
      dbConn.connect();
      readResponse(dbConn);
      assertThat(dbConn.getResponseCode()).isEqualTo(200);
    } finally {
      dbConn.disconnect();
    }

    // CREATE THE SCHEMA ON THE LEADER (with Ratis, DDL must go through leader)
    final int leaderIdx = getLeaderIndex();
    for (int s = 0; s < getServerCount(); s++) {
      final String response = command(leaderIdx, "create vertex type VertexType" + s);
      assertThat(response).contains("VertexType" + s)
          .withFailMessage("Type VertexType" + s + " not found on leader");
    }

    // Wait for schema propagation
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

    // CREATE SOME VERTICES ON BOTH SERVERS
    testEachServer((serverIndex) -> {
      for (int i = 0; i < 100; i++) {
        final String v1 = new JSONObject(
            command(serverIndex, "create vertex VertexType" + serverIndex
                + " content {\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}")).getJSONArray(
            "result").getJSONObject(0).getString(RID_PROPERTY);

        testEachServer((checkServer) -> {
          try {
            assertThat(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result")).isNotEmpty().
                withFailMessage("executed on server " + serverIndex + " checking on server " + serverIndex);
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on checking for V1 on server " + checkServer);
            throw e;
          }
        });
      }
    });
  }
}
