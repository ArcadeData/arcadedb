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
import com.arcadedb.database.Database;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that `import database` on a Raft leader creates the database cluster-wide
 * via the INSTALL_DATABASE_ENTRY Raft log entry, and the importer's subsequent
 * transactions replicate to every peer as normal TX_ENTRY stream. At the end, every
 * peer should have the same type set and matching record counts.
 */
class RaftImportDatabase3NodesIT extends BaseRaftHATest {

  private static final String DB_NAME = "RaftImportTest";

  RaftImportDatabase3NodesIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    for (int i = 0; i < 3; i++)
      FileUtils.deleteRecursively(new File("./target/databases" + i + "/" + DB_NAME));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    for (int i = 0; i < 3; i++)
      FileUtils.deleteRecursively(new File("./target/databases" + i + "/" + DB_NAME));
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
    // The default "graph" database is never created in this test (isCreateDatabases=false).
    // Page-level comparison of the imported database is not meaningful because bucket
    // file IDs are assigned independently by the importer on each node - logical counts
    // match but page contents may differ. The test asserts logical equality explicitly below.
  }

  @Test
  void importDatabasePropagatesAcrossCluster() throws Exception {
    final File fixture = new File("src/test/resources/raft-import-fixture.jsonl.tgz");
    assertThat(fixture.exists()).as("import fixture should be present at %s", fixture.getAbsolutePath()).isTrue();
    final String fixtureUrl = "file://" + fixture.getAbsolutePath();

    // Step A: issue `import database` via HTTP on the leader
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    postServerCommand(leader, "import database " + DB_NAME + " " + fixtureUrl);

    // Step B: wait until every peer sees the new database
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    // Step C: wait for replication to catch up on every peer
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Step D: compute a reference snapshot of (type name, count) pairs from the leader
    final Database leaderDb = getServer(leader).getDatabase(DB_NAME);
    final Map<String, Long> leaderCounts = new HashMap<>();
    for (final DocumentType type : leaderDb.getSchema().getTypes())
      leaderCounts.put(type.getName(), leaderDb.countType(type.getName(), false));

    assertThat(leaderCounts).as("leader should have at least one imported type").isNotEmpty();
    assertThat(leaderCounts.values())
        .as("every type should have a non-negative record count")
        .allMatch(n -> n >= 0);
    final long totalRecords = leaderCounts.values().stream().mapToLong(Long::longValue).sum();
    assertThat(totalRecords).as("imported database should have at least one record").isGreaterThan(0L);

    // Step E: assert every other peer has the same type set and matching counts
    for (int i = 0; i < getServerCount(); i++) {
      if (i == leader)
        continue;
      final Database peerDb = getServer(i).getDatabase(DB_NAME);
      for (final Map.Entry<String, Long> entry : leaderCounts.entrySet()) {
        final String typeName = entry.getKey();
        final long expected = entry.getValue();
        assertThat(peerDb.getSchema().existsType(typeName))
            .as("server %d should have type '%s'", i, typeName)
            .isTrue();
        final long actual = peerDb.countType(typeName, false);
        assertThat(actual)
            .as("server %d count of type '%s'", i, typeName)
            .isEqualTo(expected);
      }
    }
  }

  /**
   * POSTs a server command against /api/v1/server and asserts HTTP 200.
   */
  private void postServerCommand(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(
            ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    try {
      formatPayload(connection, new JSONObject().put("command", command));
      connection.connect();
      assertThat(connection.getResponseCode())
          .as("server command '%s' on server %d", command, serverIndex)
          .isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }
}
