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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11848: {@code create database} created the database on the node that ran it and only then replicated the
 * install-database entry. When that entry never committed (the followers unreachable, then the leader lost before a quorum
 * appended it) the old leader kept a database no other node had: the cluster reported itself converged while that node
 * served reads of it and the others answered "database not found". The create now goes through the log first, and the
 * apply of the entry creates the database on every node, the one that ran the command included.
 * <p>
 * Three servers with persistent Raft storage, so the old leader can come back after the two others elected a leader whose
 * log has no install entry.
 */
class Issue11848CreateDatabaseUncommittedIT extends BaseRaftHATest {

  private static final String NAME = "uncommitted11848";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // How long the create waits for a quorum that cannot form here: the default 10 s, twice (wait and grace), per create
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 4_000L);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @AfterEach
  void clearHook() {
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = null;
  }

  @Test
  void aCreateWhoseEntryNeverCommitsLeavesTheDatabaseOnNoNode() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower1 = (leader + 1) % 3;
    final int follower2 = (leader + 2) % 3;

    getServer(follower1).stop();
    getServer(follower2).stop();

    assertThatThrownBy(() -> new ServerControlPlane(getServer(leader)).createDatabase(NAME))
        .as("a create no quorum can commit fails").isInstanceOf(RuntimeException.class);
    assertNoDatabase(leader, "the leader right after its create failed");

    // The two others elect a leader whose log has no install entry; the old leader then rejoins and its entry is truncated
    getServer(leader).stop();
    Thread.sleep(2_000);
    startServer(follower1);
    startServer(follower2);
    final int newLeader = awaitLeaderAmong(follower1, follower2);
    startServer(leader);
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++)
      assertNoDatabase(i, "server " + i + " once the cluster settled");

    // The name is free everywhere: a create through the new leader installs it on every node, the old leader included
    new ServerControlPlane(getServer(newLeader)).createDatabase(NAME);
    awaitDatabaseOnEveryNode();
  }

  @Test
  void aCreateWhoseEntryCommitsAfterTheWaitGaveUpCreatesTheDatabaseOnEveryNode() {
    final int leader = requireLeaderIndex();
    final AtomicBoolean fired = new AtomicBoolean();
    RaftGroupCommitter.TEST_FORCE_DISPATCHED_TIMEOUT = entry -> isInstallEntryOf(entry) && fired.compareAndSet(false, true);

    assertThatThrownBy(() -> new ServerControlPlane(getServer(leader)).createDatabase(NAME))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class);
    assertThat(fired.get()).as("the dispatched-timeout fault fired").isTrue();

    // The entry was dispatched and commits: its apply creates the database on every node, the leader included
    awaitDatabaseOnEveryNode();
  }

  private void assertNoDatabase(final int serverIndex, final String where) {
    final ArcadeDBServer server = getServer(serverIndex);
    assertThat(server.existsDatabase(NAME)).as("%s registers no '%s'", where, NAME).isFalse();
    assertThat(server.getAvailableDatabaseNames()).as("%s lists no '%s'", where, NAME).doesNotContain(NAME);
    assertThat(new File(databaseDirectory(server), NAME)).as("%s has no directory '%s'", where, NAME).doesNotExist();
  }

  private void awaitDatabaseOnEveryNode() {
    Awaitility.await("'" + NAME + "' on every node").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
        .until(() -> {
          for (int i = 0; i < getServerCount(); i++)
            if (!getServer(i).existsDatabase(NAME))
              return false;
          return true;
        });
  }

  private int awaitLeaderAmong(final int first, final int second) {
    final int[] leader = { -1 };
    Awaitility.await("a leader among servers " + first + " and " + second).atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(250)).until(() -> {
          for (final int i : new int[] { first, second }) {
            final RaftHAPlugin plugin = getRaftPlugin(i);
            if (plugin != null && plugin.isLeader()) {
              leader[0] = i;
              return true;
            }
          }
          return false;
        });
    LogManager.instance().log(this, Level.INFO, "TEST: server %d leads without the install entry", leader[0]);
    return leader[0];
  }

  private static String databaseDirectory(final ArcadeDBServer server) {
    return server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
  }

  private static boolean isInstallEntryOf(final byte[] entry) {
    try {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(ByteString.copyFrom(entry));
      return decoded.type() == RaftLogEntryType.INSTALL_DATABASE_ENTRY && NAME.equals(decoded.databaseName());
    } catch (final Exception e) {
      return false;
    }
  }
}
