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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code create user} and {@code drop user} submitted to a Raft cluster propagate
 * to every replica via the SECURITY_USERS_ENTRY log entry. Covers login verification
 * on every node (HTTP 200 on success, 403 on rejected credentials) and byte-level equality
 * of the server-users.jsonl files after concurrent mutations.
 */
class RaftUserManagement3NodesIT extends BaseRaftHATest {

  RaftUserManagement3NodesIT() {
    // Each server gets its own root path so they don't share config files.
    // In production, servers run in separate processes; in tests, all servers
    // run in-process and would race on a shared server-users.jsonl file.
    for (int i = 0; i < 3; i++)
      FileUtils.deleteRecursively(new File("./target/server" + i));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    for (int i = 0; i < 3; i++)
      FileUtils.deleteRecursively(new File("./target/server" + i));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");

    // Per-server root path to isolate config directories (see constructor comment).
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target/server" + index);
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // No test database is created (isCreateDatabases=false) and users are server-wide state,
    // not per-database. Nothing to compare at the database page level.
  }

  @Test
  void createDropUserReplicatedAcrossCluster() throws Exception {
    assertThat(findLeaderIndex()).isGreaterThanOrEqualTo(0);

    // Trigger user creation. On the first commit after leader election the Raft gRPC reply can
    // be delayed (the entry IS committed but the client-side acknowledgment arrives late), so we
    // accept 200 (instant ACK) and 503 (transient quorum timeout that still committed). In either
    // case we wait below for the user to appear on every node before proceeding.
    final JSONObject alice = new JSONObject()
        .put("name", "alice")
        .put("password", "alicepw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    postServerCommandRetryOn503(0, "create user " + alice.toString());

    // Await: every peer's in-memory users map contains alice
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (getServer(i).getSecurity().getUser("alice") == null)
              return false;
          return true;
        });

    // Login probe: alice:alicepw1234 should succeed on every peer
    for (int i = 0; i < getServerCount(); i++) {
      final int status = postServerCommandReturnStatus(i, "list databases", "alice", "alicepw1234");
      assertThat(status).as("server %d login as alice", i).isEqualTo(200);
    }

    // Drop alice via the server command endpoint. Same retry-on-503 logic applies.
    postServerCommandRetryOn503(0, "drop user alice");

    // Await: every peer's in-memory users map no longer contains alice
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (getServer(i).getSecurity().getUser("alice") != null)
              return false;
          return true;
        });

    // Login probe: alice should be rejected on every peer (server returns 403 for bad credentials)
    for (int i = 0; i < getServerCount(); i++) {
      final int status = postServerCommandReturnStatus(i, "list databases", "alice", "alicepw1234");
      assertThat(status).as("server %d login as alice after drop", i).isEqualTo(403);
    }
  }

  @Test
  void concurrentUserCreationsConvergeOnAllNodes() throws Exception {
    assertThat(findLeaderIndex()).isGreaterThanOrEqualTo(0);

    // Warm up the Raft gRPC channel with a single user creation before launching concurrent
    // requests. The very first Raft commit after leader election can encounter a gRPC stream
    // warm-up delay; subsequent commits use already-established streams and are reliable.
    postServerCommandRetryOn503(0, "create user " + new JSONObject()
        .put("name", "warmup")
        .put("password", "warmup12345")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> getServer(0).getSecurity().getUser("warmup") != null);

    // Submit concurrent creates via server 0 (forwarded to leader if needed).
    // Use postServerCommandRetryOn503 so transient Raft-client warm-up failures are handled.
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(5);
    for (int n = 0; n < 5; n++) {
      final int userIndex = n;
      pool.submit(() -> {
        start.await();
        final JSONObject user = new JSONObject()
            .put("name", "u" + userIndex)
            .put("password", "pw12345678")
            .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
        postServerCommandRetryOn503(0, "create user " + user.toString());
        return null;
      });
    }
    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();

    // Await: every peer's in-memory users map contains all five users
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            for (int n = 0; n < 5; n++)
              if (getServer(i).getSecurity().getUser("u" + n) == null)
                return false;
          return true;
        });

    // Byte-level equality: every server's users file should be identical because all nodes
    // apply the same ordered sequence of Raft log entries, so the final file content is the same.
    final byte[] reference = Files.readAllBytes(
        Paths.get(getServer(0).getRootPath(), "config", "server-users.jsonl"));
    for (int i = 1; i < getServerCount(); i++) {
      final byte[] other = Files.readAllBytes(
          Paths.get(getServer(i).getRootPath(), "config", "server-users.jsonl"));
      assertThat(other).as("server %d users file byte equality", i).isEqualTo(reference);
    }
  }

  /**
   * POSTs a server command using root credentials, retrying up to 8 times on 503.
   * A 503 can occur transiently right after leader election while the Raft gRPC channel
   * is warming up: the entry IS committed on the cluster but the gRPC acknowledgment
   * arrives after the client-side timeout. In that case a retry of {@code create user}
   * returns 403 (user already exists) and a retry of {@code drop user} returns 400
   * (user already gone). We therefore do not assert any particular status here;
   * callers use {@code Awaitility} to verify the actual cluster state.
   */
  private void postServerCommandRetryOn503(final int serverIndex, final String command) throws Exception {
    int status = 503;
    for (int attempt = 0; attempt < 8 && status == 503; attempt++) {
      if (attempt > 0)
        Thread.sleep(2_000);
      status = postServerCommandReturnStatus(serverIndex, command, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }
  }

  /**
   * POSTs a server command and returns the HTTP status code.
   */
  private int postServerCommandReturnStatus(final int serverIndex, final String command,
      final String user, final String password) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
