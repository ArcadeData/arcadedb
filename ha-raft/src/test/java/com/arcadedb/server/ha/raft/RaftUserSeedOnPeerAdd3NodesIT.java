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
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that when a new peer joins a Raft cluster, the leader emits a one-shot
 * SECURITY_USERS_ENTRY that brings the new peer's users file up to date.
 * <p>
 * The test simulates the "newly joined peer with stale users" scenario by manually
 * corrupting a non-leader server's users state after the cluster is up, then invoking
 * the seed path that {@link PostAddPeerHandler} would call after a successful addPeer,
 * and asserts the corrupted state is restored.
 */
class RaftUserSeedOnPeerAdd3NodesIT extends BaseRaftHATest {

  RaftUserSeedOnPeerAdd3NodesIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
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
  }

  @Override
  protected void checkDatabasesAreIdentical() {
    // No test database is created (isCreateDatabases=false); users are server-wide state.
  }

  @Test
  void peerAddSeedRestoresCorruptedUsersOnTarget() throws Exception {
    // Step A: create alice via the normal replicated path
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);

    final JSONObject alice = new JSONObject()
        .put("name", "alice")
        .put("password", "alicepw1234")
        .put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    postServerCommandRetryOn503(leader, "create user " + alice.toString());

    // Await: every peer's in-memory users map contains alice
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (getServer(i).getSecurity().getUser("alice") == null)
              return false;
          return true;
        });

    // Step B: pick a non-leader target and corrupt its users state
    final int target = (leader + 1) % getServerCount();
    final Path targetUsersFile = Paths.get(getServer(target).getRootPath(), "config", "server-users.jsonl");
    assertThat(Files.exists(targetUsersFile)).isTrue();

    // Preserve root's existing password hash so root login still works after the corruption
    final ServerSecurityUser rootUser = getServer(target).getSecurity().getUser("root");
    assertThat(rootUser).as("root user must exist on target").isNotNull();
    final JSONObject rootJson = rootUser.toJSON();

    // Write a minimal users file with only root, then force an in-memory reload
    Files.writeString(targetUsersFile, rootJson.toString() + "\n", StandardCharsets.UTF_8);
    getServer(target).getSecurity().loadUsers();

    assertThat(getServer(target).getSecurity().getUser("alice"))
        .as("target server should no longer have alice after corruption")
        .isNull();
    assertThat(getServer(target).getSecurity().getUser("root"))
        .as("target server must still have root after corruption")
        .isNotNull();

    // Step C: invoke the seed path on the leader (same call PostAddPeerHandler makes after addPeer)
    final String currentUsers = getServer(leader).getSecurity().getUsersJsonPayload();
    getRaftPlugin(leader).replicateSecurityUsers(currentUsers);

    // Step D: wait until the target has alice back
    Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> getServer(target).getSecurity().getUser("alice") != null);

    // Step E: verify the file on disk was rewritten
    final String restored = Files.readString(targetUsersFile, StandardCharsets.UTF_8);
    assertThat(restored).as("target's users file should contain alice after seed").contains("\"alice\"");
  }

  /**
   * POSTs a server command using root credentials, retrying up to 8 times on 503.
   * A 503 can occur transiently right after leader election while the Raft gRPC channel
   * is warming up.
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
