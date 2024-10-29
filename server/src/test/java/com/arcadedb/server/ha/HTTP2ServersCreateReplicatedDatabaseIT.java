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
import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

public class HTTP2ServersCreateReplicatedDatabaseIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void testCreateReplicatedDatabase() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + 0 + "/api/v1/server").openConnection();

    // CREATE DATABASE ON THE LEADER
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", "create database " + getDatabaseName()));
      connection.connect();
      final String response = readResponse(connection);
      LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      assertThat(connection.getResponseMessage()).isEqualTo("OK");
    } finally {
      connection.disconnect();
    }

    // CREATE THE SCHEMA ON BOTH SERVER, ONE TYPE PER SERVER
    testEachServer((serverIndex) -> {
      final String response = command(serverIndex, "create vertex type VertexType" + serverIndex);
      assertThat(response).contains("VertexType" + serverIndex)
          .withFailMessage("Type " + (("VertexType" + serverIndex) + " not found on server " + serverIndex));
    });

    Thread.sleep(300);

    // CHECK THE SCHEMA HAS BEEN PROPAGATED
    testEachServer((serverIndex) -> command(serverIndex, "select from VertexType" + serverIndex));

    // CREATE SOME VERTICES ON BOTH SERVERS
    testEachServer((serverIndex) -> {
      for (int i = 0; i < 100; i++) {
        final String v1 = new JSONObject(
            command(serverIndex, "create vertex VertexType" + serverIndex
                + " content {\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}")).getJSONArray(
            "result").getJSONObject(0).getString("@rid");

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
