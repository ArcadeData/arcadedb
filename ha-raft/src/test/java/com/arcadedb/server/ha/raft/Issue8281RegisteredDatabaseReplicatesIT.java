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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.ServerDatabase;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8281: {@link ArcadeDBServer#registerDatabase} registered a caller-supplied database exactly as given, so on a
 * node with high availability active a plain {@link LocalDatabase} registered that way served every protocol and
 * committed locally without replication. It now goes through the HA wrapper like every database the server opens or
 * creates: a write through the returned handle reaches the other nodes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8281RegisteredDatabaseReplicatesIT extends BaseRaftHATest {
  private static final String REGISTERED = "registered8281";

  @Test
  void aWriteThroughARegisteredDatabaseReachesTheFollowers() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final ArcadeDBServer leader = getServer(leaderIndex);

    final String path = leader.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + REGISTERED;
    final LocalDatabase local = (LocalDatabase) new DatabaseFactory(path).create();

    final ServerDatabase registered = leader.registerDatabase(REGISTERED, local);

    assertThat(registered.getWrappedDatabaseInstance())
        .as("a plain database registered on an HA node must be the replicated wrapper, not the local instance")
        .isInstanceOf(RaftReplicatedDatabase.class);

    // Registering does not create the database on the other nodes, exactly as createDatabase() does not.
    ((HAReplicatedDatabase) registered.getWrappedDatabaseInstance()).createInReplicas();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    Awaitility.await("database created on the follower").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
        .until(() -> getServer(followerIndex).existsDatabase(REGISTERED));

    registered.command("sql", "CREATE DOCUMENT TYPE Doc");
    // An embedder's database, opened without the server's auto-transaction: the write runs in an explicit one.
    registered.transaction(() -> registered.command("sql", "INSERT INTO Doc SET n = 1"));
    waitForAllServers();

    Awaitility.await("write replicated to the follower").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
        .ignoreExceptions()
        .until(() -> getServer(followerIndex).getDatabase(REGISTERED).countType("Doc", true) == 1L);
  }
}
