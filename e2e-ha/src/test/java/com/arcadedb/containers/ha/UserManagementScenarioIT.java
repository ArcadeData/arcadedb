/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.containers.ha;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class UserManagementScenarioIT extends ContainersTestTemplate {

  private static final String SERVER_LIST    = "arcadedb-0:2434:2480,arcadedb-1:2434:2480,arcadedb-2:2434:2480";
  private static final String ALICE_PASSWORD = "alicepw1234";

  @AfterEach
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Three-node Raft HA: create/drop user replicates login authorization to every peer")
  void userCreateAndDropReplicatedAcrossCluster() throws Exception {
    createArcadeContainer("arcadedb-0", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-1", SERVER_LIST, "majority", network);
    createArcadeContainer("arcadedb-2", SERVER_LIST, "majority", network);

    logger.info("Starting cluster");
    final List<ServerWrapper> servers = startCluster();

    // Step A: create alice on the leader (node 0)
    final JSONObject alice = new JSONObject()
        .put("name", "alice")
        .put("password", ALICE_PASSWORD)
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    logger.info("Creating user alice on leader");
    final int createStatus = postServerCommandAsRoot(servers.get(0), "create user " + alice.toString(), 30_000);
    assertThat(createStatus).as("create user HTTP status").isEqualTo(200);

    // Step B: await until alice can log in on every node
    logger.info("Awaiting alice login propagation");
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> loginOk(servers.get(0), "alice", ALICE_PASSWORD)
            && loginOk(servers.get(1), "alice", ALICE_PASSWORD)
            && loginOk(servers.get(2), "alice", ALICE_PASSWORD));

    // Step C: drop alice on the leader
    logger.info("Dropping user alice on leader");
    final int dropStatus = postServerCommandAsRoot(servers.get(0), "drop user alice", 30_000);
    assertThat(dropStatus).as("drop user HTTP status").isEqualTo(200);

    // Step D: await until alice can no longer log in on any node
    logger.info("Awaiting alice login rejection propagation");
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> !loginOk(servers.get(0), "alice", ALICE_PASSWORD)
            && !loginOk(servers.get(1), "alice", ALICE_PASSWORD)
            && !loginOk(servers.get(2), "alice", ALICE_PASSWORD));
  }

  /**
   * POSTs a server command to /api/v1/server as root and returns the HTTP status code.
   */
  private int postServerCommandAsRoot(final ServerWrapper server, final String command, final int readTimeoutMs) throws Exception {
    return postServerCommand(server, command, "root", PASSWORD, readTimeoutMs);
  }

  /**
   * POSTs a server command to /api/v1/server with the given credentials and returns the HTTP status code.
   * Does not throw on non-200 responses; the caller is expected to inspect the return value.
   */
  private int postServerCommand(final ServerWrapper server, final String command,
      final String user, final String password, final int readTimeoutMs) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://" + server.host() + ":" + server.httpPort() + "/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(readTimeoutMs);
    connection.setDoOutput(true);
    try {
      connection.getOutputStream().write(
          new JSONObject().put("command", command).toString().getBytes());
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Returns true if the given user can successfully issue {@code list databases} on the given server.
   * Uses HTTP 200 as the success indicator; 401/403 means auth failure; any other status or exception
   * is treated as failure.
   */
  private boolean loginOk(final ServerWrapper server, final String user, final String password) {
    try {
      return postServerCommand(server, "list databases", user, password, 5000) == 200;
    } catch (final Exception e) {
      return false;
    }
  }
}
