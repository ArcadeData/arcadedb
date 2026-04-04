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
package com.arcadedb.server.ha.raft;

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
import static org.assertj.core.api.Assertions.*;

class RaftHTTP2ServersCreateReplicatedDatabaseIT extends BaseRaftHATest {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void createReplicatedDatabase() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + 0 + "/api/v1/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", "create database " + getDatabaseName()));
      connection.connect();
      final String response = readResponse(connection);
      LogManager.instance().log(this, Level.FINE, "Response: %s", null, response);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      assertThat(connection.getResponseMessage()).isEqualTo("OK");
    } finally {
      connection.disconnect();
    }

    testEachServer((serverIndex) -> {
      final String response = command(serverIndex, "create vertex type RaftCreateVertex" + serverIndex);
      assertThat(response).contains("RaftCreateVertex" + serverIndex);
    });

    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++) {
            try {
              command(i, "select from RaftCreateVertex" + i);
            } catch (final Exception e) {
              return false;
            }
          }
          return true;
        });

    testEachServer((serverIndex) -> {
      for (int i = 0; i < 100; i++) {
        final String v1 = new JSONObject(
            command(serverIndex, "create vertex RaftCreateVertex" + serverIndex
                + " content {\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}"))
            .getJSONArray("result").getJSONObject(0).getString(RID_PROPERTY);

        waitForReplicationIsCompleted(serverIndex);

        testEachServer((checkServer) ->
            assertThat(new JSONObject(command(checkServer, "select from " + v1)).getJSONArray("result"))
                .withFailMessage("executed on server " + serverIndex + " checking on server " + checkServer)
                .isNotEmpty());
      }
    });
  }
}
