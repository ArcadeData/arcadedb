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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.protocol.RaftPeer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9510, against real Ratis: a node removed from the Raft configuration ({@code leaveCluster()}, i.e. a
 * {@code setConfiguration} without it) keeps an open division and state machine that no longer receive appends. Its
 * client still reaches the cluster's leader, which accepts a request from a non-member, so before the fix a commit on
 * such a node was committed cluster-wide and then waited the whole quorum timeout for a local apply that never comes.
 * It is now refused, retryably, before anything is prepared or submitted.
 */
@Tag("slow")
class Issue9510NodeRemovedFromConfigurationRefusesWritesIT extends BaseRaftHATest {

  private static final String TYPE_NAME      = "Issue9510";
  private static final int    RECORDS_BEFORE = 5;
  private static final long   QUORUM_TIMEOUT = 10_000L;

  /** The server that left the cluster: excluded from the end-of-test comparison, it diverges by design. */
  private volatile int leftIndex = -1;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, QUORUM_TIMEOUT);
  }

  @Override
  protected int[] getServerToCheck() {
    return serversMatching(i -> i != leftIndex && getServer(i) != null && getServer(i).isStarted());
  }

  @Test
  void aFollowerThatLeftTheClusterRefusesCommitsInsteadOfStalling() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex);
    assertCommitRefusedAndNotReplicated(followerIndex);
  }

  @Test
  void aLeaderThatLeftTheClusterRefusesCommitsInsteadOfStalling() {
    final int leaderIndex = prepareType();
    leaveAndAwaitRemoval(leaderIndex);
    assertThat(getRaftPlugin(leaderIndex).getRaftHAServer().isLeader()).as("the leader stepped down on leaving").isFalse();
    assertCommitRefusedAndNotReplicated(leaderIndex);
  }

  @Test
  void aNodeThatLeftTheClusterStillCommitsAReadOnlyTransaction() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex);

    // Every HTTP query runs in a transaction: a removed node submits nothing for it, so it has nothing to refuse.
    final Database removedDb = getServerDatabase(followerIndex, getDatabaseName());
    removedDb.begin();
    try (final ResultSet rs = removedDb.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(RECORDS_BEFORE);
    }
    removedDb.commit();
    assertThat(removedDb.isTransactionActive()).isFalse();
  }

  @Test
  void aNodeThatLeftTheClusterRefusesAnInstall() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex);

    final HAReplicatedDatabase removedDb = (HAReplicatedDatabase) getServer(followerIndex).getDatabase(getDatabaseName())
        .getWrappedDatabaseInstance();
    assertThatThrownBy(removedDb::createInReplicas).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member");
    assertThatThrownBy(() -> removedDb.createInReplicas(true)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member");
  }

  @Test
  void aNodeThatLeftTheClusterRefusesADropInsteadOfDroppingTheDatabaseClusterWide() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex);

    final HAReplicatedDatabase removedDb = (HAReplicatedDatabase) getServer(followerIndex).getDatabase(getDatabaseName())
        .getWrappedDatabaseInstance();
    assertThatThrownBy(removedDb::dropInReplicas).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member");

    // The members still hold the database: the drop never reached the log.
    for (final int index : getServerToCheck())
      assertThat(getServer(index).existsDatabase(getDatabaseName())).as("database on server %d", index).isTrue();
  }

  private void assertCommitRefusedAndNotReplicated(final int removedIndex) {
    final Database removedDb = getServerDatabase(removedIndex, getDatabaseName());

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    removedDb.begin();
    removedDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE).save();
    assertThatThrownBy(removedDb::commit).isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member");
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a refusal before replication, rather than a wait for an apply this removed node will never run");
    assertThat(removedDb.isTransactionActive()).as("the refused transaction is rolled back").isFalse();

    // The refused write reached no member of the cluster.
    final int member = getServerToCheck()[0];
    final Database memberDb = getServerDatabase(member, getDatabaseName());
    memberDb.transaction(() -> memberDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE + 1).save());
    for (final int index : getServerToCheck()) {
      waitForReplicationIsCompleted(index);
      try (final ResultSet rs = getServerDatabase(index, getDatabaseName()).query("sql",
          "SELECT count(*) AS cnt FROM " + TYPE_NAME + " WHERE id = " + RECORDS_BEFORE)) {
        assertThat(rs.next().<Number>getProperty("cnt").longValue()).as("refused record on server %d", index).isZero();
      }
    }
  }

  /** Removes {@code index} from the configuration and waits until its own division has seen the new configuration. */
  private void leaveAndAwaitRemoval(final int index) {
    leftIndex = index;
    final RaftHAServer raft = getRaftPlugin(index).getRaftHAServer();
    final String peerId = raft.getLocalPeerId().toString();
    raft.leaveCluster(false);
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> !containsPeer(raft.getCommittedPeersOrNull(), peerId) && !raft.isLeader());
  }

  private static boolean containsPeer(final Collection<RaftPeer> peers, final String peerId) {
    if (peers == null)
      return true;
    for (final RaftPeer peer : peers)
      if (peer.getId().toString().equals(peerId))
        return true;
    return false;
  }

  /** Creates the type and commits {@link #RECORDS_BEFORE} records on the leader; returns the leader's index. */
  private int prepareType() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().createDocumentType(TYPE_NAME));
    for (int i = 0; i < RECORDS_BEFORE; i++) {
      final int id = i;
      leaderDb.transaction(() -> leaderDb.newDocument(TYPE_NAME).set("id", id).save());
    }
    waitForAllServers();
    return leaderIndex;
  }
}
