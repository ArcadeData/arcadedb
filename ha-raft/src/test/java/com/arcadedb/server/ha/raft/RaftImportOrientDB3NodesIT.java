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
 * Regression test for issue #3887 "Import Database issues with HA".
 * <p>
 * Imports an OrientDB-format export (Person + Friend, ~10k edges) into an empty database
 * on the Raft leader. The user report was that a 10k-record Whisky import against a 3-node
 * HA cluster timed out at the Raft quorum after minutes, while the same import on a
 * single node completed in seconds.
 * <p>
 * This test exercises the full OrientDB importer path through Raft: schema creation,
 * batch record inserts, edge creation, and index creation. Expected completion well under
 * one minute on a 3-node in-process cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftImportOrientDB3NodesIT extends BaseRaftHATest {

  private static final String DB_NAME             = "RaftImportOrientDBTest";
  // Issue #3887 reported a 10k-record import taking 10+ minutes on HA while a single-node
  // import completed in 2 seconds. A 60-second budget on a 3-node in-process cluster is
  // well above the ~2 second steady-state baseline and still catches any severe regression.
  private static final long   IMPORT_TIMEOUT_SECS = 60;

  RaftImportOrientDB3NodesIT() {
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
    // Page-level comparison across nodes is not meaningful for the imported database because
    // bucket file IDs are assigned per node. The test asserts logical equality (type set + counts).
  }

  @Test
  void importOrientDBPropagatesAcrossCluster() throws Exception {
    final File fixture = new File("src/test/resources/orientdb-export-small.gz");
    assertThat(fixture.exists()).as("orientdb fixture should be present at %s", fixture.getAbsolutePath()).isTrue();
    final String fixtureUrl = "file://" + fixture.getAbsolutePath();

    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);

    final long start = System.currentTimeMillis();
    postServerCommand(leader, "import database " + DB_NAME + " " + fixtureUrl);
    final long elapsedMs = System.currentTimeMillis() - start;

    assertThat(elapsedMs)
        .as("IMPORT DATABASE should complete in under %d seconds on a 3-node Raft cluster (actual: %d ms)",
            IMPORT_TIMEOUT_SECS, elapsedMs)
        .isLessThan(IMPORT_TIMEOUT_SECS * 1000);

    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(DB_NAME))
              return false;
          return true;
        });

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final Database leaderDb = getServer(leader).getDatabase(DB_NAME);
    final Map<String, Long> leaderCounts = new HashMap<>();
    for (final DocumentType type : leaderDb.getSchema().getTypes())
      leaderCounts.put(type.getName(), leaderDb.countType(type.getName(), false));

    assertThat(leaderCounts.get("Person"))
        .as("Person vertex count on the leader")
        .isEqualTo(500L);
    assertThat(leaderCounts.get("Friend"))
        .as("Friend edge count on the leader")
        .isEqualTo(10_000L);

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
