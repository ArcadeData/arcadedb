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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.Date;
import java.util.Set;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9763: under Raft HA, {@code ALTER DATABASE} for a setting kept in the database's {@code configuration.json}
 * was written on the node that ran it only - {@code RaftReplicatedDatabase.saveConfiguration()} proxied to the local
 * save and a {@code SCHEMA_ENTRY} carries only {@code schema.json}. The followers, and whichever of them became leader
 * after a failover, kept the previous value; for {@code arcadedb.dateImplementation} that meant a different answer to
 * the same read.
 * <p>
 * Every node is checked three ways: its in-memory configuration (what that node runs with now), its own
 * {@code configuration.json} (what it reopens with, e.g. as the leader after a failover), and for the date
 * implementation the class a read actually returns there.
 */
class Issue9763DatabaseConfigurationReplicationIT extends BaseRaftHATest {

  /** How long a node is given to observe a peer's advertisement change (a few refresh rounds). */
  private static final long CAPABILITY_CONVERGENCE_TIMEOUT_MS = 30_000L;

  @Test
  void alterDatabaseOnTheLeaderReachesEveryNode() throws Exception {
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE D");
    leader.command("sql", "CREATE PROPERTY D.day DATE");
    leader.transaction(() -> leader.newDocument("D").set("day", LocalDate.of(2026, 10, 13)).save());
    assertClusterConsistency();

    // java.util.Date, not the default java.time.LocalDate: a node that kept the default must read differently
    leader.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'");
    leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 5");

    assertOnEveryNode((index, database) -> {
      assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.DATE_IMPLEMENTATION))
          .as("server %d in memory", index).isEqualTo("java.util.Date");
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("server %d in memory", index).isEqualTo(5);
      assertThat(database.getSerializer().getDateImplementation())
          .as("server %d serializer", index).isEqualTo(Date.class);
      try (final ResultSet rs = database.query("sql", "SELECT day FROM D")) {
        assertThat(rs.next().<Object>getProperty("day"))
            .as("server %d answers a read with the replicated implementation", index).isInstanceOf(Date.class);
      }
    }, (index, file) -> {
      assertThat(file.getString("dateImplementation")).as("server %d configuration.json", index)
          .isEqualTo("java.util.Date");
      assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index).isEqualTo(5);
    });
  }

  /**
   * The path a user takes: the statement sent over HTTP to a FOLLOWER, which forwards it to the leader, where the
   * HTTP handler runs it inside a transaction of its own.
   */
  @Test
  void alterDatabaseSentOverHttpToAFollowerReachesEveryNode() throws Exception {
    final int followerIndex = (findLeaderIndex() + 1) % getServerCount();

    assertThat(executeCommand(followerIndex, "sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 7")).isNotNull();

    assertOnEveryNode((index, database) -> assertThat(
            database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
            .as("server %d in memory", index).isEqualTo(7),
        (index, file) -> assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index)
            .isEqualTo(7));
  }

  /**
   * The second writer: the "set database setting" server command (HTTP and gRPC), which does not forward and so runs
   * on whichever node the client reached - here a follower.
   */
  @Test
  void setDatabaseSettingOnAFollowerReachesEveryNode() throws Exception {
    final int followerIndex = (findLeaderIndex() + 1) % getServerCount();

    new ServerControlPlane(getServer(followerIndex)).setDatabaseSetting(getDatabaseName(),
        GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey(), "9");

    assertOnEveryNode((index, database) -> assertThat(
            database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
            .as("server %d in memory", index).isEqualTo(9),
        (index, file) -> assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index)
            .isEqualTo(9));
  }

  /**
   * Only the changed settings travel, so a change replaces nothing else on any node: a value one node holds in its own
   * {@code configuration.json} only (from before this replication existed) survives a change to another setting made
   * elsewhere, and so do changes to different settings made on different nodes.
   */
  @Test
  void aChangeOnOneNodeKeepsEverySettingItDoesNotName() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final LocalDatabase followerLocal = (LocalDatabase) ((DatabaseInternal) getServerDatabase(followerIndex,
        getDatabaseName())).getEmbedded();
    // Written past the replication, as a node upgraded from a build without it can hold
    followerLocal.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
    followerLocal.saveConfiguration();

    leader().command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 6");
    new ServerControlPlane(getServer(followerIndex)).setDatabaseSetting(getDatabaseName(),
        GlobalConfiguration.TX_RETRIES.getKey(), "7");

    assertOnEveryNode((index, database) -> {
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("server %d in memory", index).isEqualTo(6);
      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES))
          .as("server %d in memory", index).isEqualTo(7);
    }, (index, file) -> {
      assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index).isEqualTo(6);
      assertThat(file.getInt("txRetries")).as("server %d configuration.json", index).isEqualTo(7);
    });

    final DatabaseInternal follower = (DatabaseInternal) getServerDatabase(followerIndex, getDatabaseName());
    assertThat(follower.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH))
        .as("the follower's own value is not replaced by changes to other settings").isEqualTo(2);
    assertThat(readConfigurationFile(follower).getInt("txWalFlush")).isEqualTo(2);
  }

  /**
   * A peer that cannot decode the new entry type would halt on it, so the change is refused before anything is
   * submitted - and the node that ran it must not keep the setting in force on its own, which is the divergence the
   * replication exists to end.
   */
  @Test
  void aChangeIsRefusedAndRolledBackWhileAPeerCannotDecodeTheEntry() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final int laggingIndex = (leaderIndex + 1) % getServerCount();
    final String laggingPeerId = peerIdForIndex(laggingIndex);
    final RaftHAServer lagging = getRaftPlugin(laggingIndex).getRaftHAServer();
    final Database leader = leader();
    final DatabaseInternal leaderDb = (DatabaseInternal) leader;
    final int bucketsBefore = leaderDb.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    final Class<?> dateImplementationBefore = leaderDb.getSerializer().getDateImplementation();
    assertThat(dateImplementationBefore).as("precondition: the change below must be a change").isNotEqualTo(Date.class);

    lagging.setAdvertisedCapabilities(Set.of(PeerCapabilities.SCHEMA_DELTA));
    try {
      awaitObserved(leaderIndex, laggingPeerId,
          capabilities -> capabilities != null && !capabilities.contains(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY));

      assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 11"))
          .hasMessageContaining(laggingPeerId)
          .hasMessageContaining(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);
      assertThatThrownBy(() -> leader.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
          .hasMessageContaining(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY);

      assertThat(leaderDb.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("the refused setting must not stay in force on the leader alone").isEqualTo(bucketsBefore);
      assertThat(leaderDb.getSerializer().getDateImplementation())
          .as("nor its per-database side effect").isEqualTo(dateImplementationBefore);
      assertThat(getServer(laggingIndex).isStarted()).as("nothing was submitted, so nothing halted").isTrue();
    } finally {
      lagging.setAdvertisedCapabilities(PeerCapabilities.LOCAL);
    }

    awaitObserved(leaderIndex, laggingPeerId,
        capabilities -> capabilities != null && capabilities.contains(PeerCapabilities.DATABASE_CONFIGURATION_ENTRY));
    leader.command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 11");
    assertOnEveryNode((index, database) -> assertThat(
            database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
            .as("server %d once the cluster is uniform", index).isEqualTo(11),
        (index, file) -> assertThat(file.getInt("typeDefaultBuckets")).isEqualTo(11));
  }

  /**
   * Issue #9959: the entry a change submits names the setting it changed and nothing else. A value another statement on
   * the same node has put in force and not submitted yet - its save still ahead of it, and possibly refused - used to
   * travel with this change's entry and commit on every node. Here that other statement is the in-memory store each
   * writer makes before its save, left pending on the node that then runs a change to a different setting: through
   * {@code ALTER DATABASE} on the leader and through the "set database setting" command on a follower.
   */
  @Test
  void aChangeDoesNotCarryAnotherPendingSettingOfItsNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String walFlushKey = GlobalConfiguration.TX_WAL_FLUSH.getKey();
    final LocalDatabase leaderLocal = (LocalDatabase) ((DatabaseInternal) leader()).getEmbedded();
    final LocalDatabase followerLocal = (LocalDatabase) ((DatabaseInternal) getServerDatabase(followerIndex,
        getDatabaseName())).getEmbedded();
    final int[] walFlushBefore = new int[getServerCount()];
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
      walFlushBefore[i] = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH);
      assertThat(readConfigurationFile(database).has("txWalFlush")).as("precondition: server %d", i).isFalse();
    }

    // The other statements: in force on their node, not submitted
    leaderLocal.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);
    followerLocal.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
    try {
      leader().command("sql", "ALTER DATABASE `arcadedb.typeDefaultBuckets` 4");
      new ServerControlPlane(getServer(followerIndex)).setDatabaseSetting(getDatabaseName(),
          GlobalConfiguration.TX_RETRIES.getKey(), "8");

      assertOnEveryNode((index, database) -> {
        assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
            .as("server %d in memory", index).isEqualTo(4);
        assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES))
            .as("server %d in memory", index).isEqualTo(8);
        final int expectedWalFlush = index == leaderIndex ? 2 : index == followerIndex ? 1 : walFlushBefore[index];
        assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH))
            .as("server %d runs with its own pending value, or none, never another node's", index)
            .isEqualTo(expectedWalFlush);
      }, (index, file) -> {
        assertThat(file.getInt("typeDefaultBuckets")).as("server %d configuration.json", index).isEqualTo(4);
        assertThat(file.getInt("txRetries")).as("server %d configuration.json", index).isEqualTo(8);
        assertThat(file.has("txWalFlush")).as("server %d: no entry carried the pending setting", index).isFalse();
      });
    } finally {
      try {
        leaderLocal.getConfiguration().setValue(walFlushKey, null);
      } finally {
        followerLocal.getConfiguration().setValue(walFlushKey, null);
      }
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  @FunctionalInterface
  private interface NodeCheck<T> {
    void accept(int serverIndex, T value) throws Exception;
  }

  private Database leader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  /** Waits for every node to apply what the leader committed, then checks each node in memory and on disk. */
  private void assertOnEveryNode(final NodeCheck<DatabaseInternal> inMemory, final NodeCheck<JSONObject> onDisk)
      throws Exception {
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(i, getDatabaseName());
      inMemory.accept(i, database);
      onDisk.accept(i, readConfigurationFile(database));
    }
  }

  private static JSONObject readConfigurationFile(final DatabaseInternal database) throws IOException {
    final File file = new File(database.getDatabasePath(), LocalDatabase.CONFIGURATION_FILE_NAME);
    return new JSONObject(Files.readString(file.toPath(), StandardCharsets.UTF_8)).getJSONObject("configuration");
  }

  /** Waits until the leader has PROBED the peer and recorded an advertisement satisfying {@code settled}. */
  private void awaitObserved(final int observerIndex, final String peerId, final Predicate<Set<String>> settled)
      throws InterruptedException {
    final RaftHAServer observer = getRaftPlugin(observerIndex).getRaftHAServer();
    final long deadline = System.currentTimeMillis() + CAPABILITY_CONVERGENCE_TIMEOUT_MS;
    Set<String> observed = observedCapabilities(observer, peerId);
    while (!settled.test(observed) && System.currentTimeMillis() < deadline) {
      Thread.sleep(200);
      observed = observedCapabilities(observer, peerId);
    }
    // A hang detector, not a latency assertion: the refresh runs every few seconds
    assertThat(settled.test(observed)).as("advertisement of %s seen by server %d: %s", peerId, observerIndex, observed)
        .isTrue();
  }

  private static Set<String> observedCapabilities(final RaftHAServer observer, final String peerId) {
    final PeerCapabilityRegistry.Advertisement advertisement =
        observer.getPeerCapabilityRegistry().freshAdvertisementOf(peerId);
    return advertisement != null ? advertisement.capabilities() : null;
  }
}
