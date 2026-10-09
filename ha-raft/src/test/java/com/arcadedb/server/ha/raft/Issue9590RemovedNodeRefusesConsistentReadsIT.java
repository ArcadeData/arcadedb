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
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.protocol.RaftPeer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9590, against real Ratis: a node removed from the Raft configuration keeps an open division that receives no
 * more appends. Before the fix a READ_YOUR_WRITES read carrying a bookmark the node never reaches waited the whole
 * quorum timeout and then degraded to EVENTUAL, serving data missing the write; a LINEARIZABLE read waited for a read
 * index the division never applies and failed only at the timeout. Both are now refused at once, retryably, the way
 * #9510 refuses writes. EVENTUAL reads, and a READ_YOUR_WRITES read whose bookmark the node already applied, are served.
 */
@Tag("slow")
class Issue9590RemovedNodeRefusesConsistentReadsIT extends BaseRaftHATest {

  private static final String TYPE_NAME      = "Issue9590";
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

  @AfterEach
  void clearContext() {
    RaftReplicatedDatabase.removeReadConsistencyContext();
  }

  @Test
  void aReadYourWritesReadWithABookmarkTheRemovedNodeNeverReachesIsRefusedAtOnce() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex);

    // A write the removed node never receives: its bookmark is past anything it will ever apply.
    final long bookmark = writeOnMemberAndGetBookmark();
    assertThat(getRaftPlugin(followerIndex).getRaftHAServer().getLastAppliedIndex())
        .as("the removed node did not apply the new write").isLessThan(bookmark);

    assertConsistentReadRefused(followerIndex, Database.READ_CONSISTENCY.READ_YOUR_WRITES, bookmark);
  }

  @Test
  void aLinearizableReadOnARemovedNodeIsRefusedAtOnce() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex);
    writeOnMemberAndGetBookmark();

    assertConsistentReadRefused(followerIndex, Database.READ_CONSISTENCY.LINEARIZABLE, -1L);
  }

  @Test
  void aLeaderThatLeftTheClusterRefusesALinearizableRead() {
    final int leaderIndex = prepareType();
    leaveAndAwaitRemoval(leaderIndex);
    writeOnMemberAndGetBookmark();

    assertConsistentReadRefused(leaderIndex, Database.READ_CONSISTENCY.LINEARIZABLE, -1L);
  }

  @Test
  void aRemovedNodeStillServesEventualReadsAndBookmarksItAlreadyApplied() {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    leaveAndAwaitRemoval(followerIndex);
    writeOnMemberAndGetBookmark();

    final Database removedDb = getServerDatabase(followerIndex, getDatabaseName());

    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.EVENTUAL, -1L);
    assertThat(countBefore(removedDb)).isEqualTo(RECORDS_BEFORE);

    // A bookmark this node has already applied needs no wait, so membership does not matter to it.
    final long applied = getRaftPlugin(followerIndex).getRaftHAServer().getTrustedAppliedIndex(getDatabaseName());
    assertThat(applied).isPositive();
    RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, applied);
    assertThat(countBefore(removedDb)).isEqualTo(RECORDS_BEFORE);
  }

  private void assertConsistentReadRefused(final int removedIndex, final Database.READ_CONSISTENCY consistency,
      final long bookmark) {
    final Database removedDb = getServerDatabase(removedIndex, getDatabaseName());
    RaftReplicatedDatabase.applyReadConsistencyContext(consistency, bookmark);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> countBefore(removedDb)).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("not a member").hasMessageContaining(consistency.name());
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a refusal on a removed node, rather than a wait for an index its division will never apply");
  }

  private static long countBefore(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME + " WHERE id < " + RECORDS_BEFORE)) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }

  /** Commits one record on a member of the cluster and returns the leader's commit index, a bookmark past it. */
  private long writeOnMemberAndGetBookmark() {
    final int newLeader = awaitLeaderAmong(getServerToCheck());
    final Database memberDb = getServerDatabase(newLeader, getDatabaseName());
    memberDb.transaction(() -> memberDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE).save());
    return getRaftPlugin(newLeader).getRaftHAServer().getCommitIndex();
  }

  private int awaitLeaderAmong(final int[] candidates) {
    final int[] found = { -1 };
    Awaitility.await("a leader among the remaining members").atMost(60, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS).until(() -> {
          for (final int i : candidates)
            if (getRaftPlugin(i).getRaftHAServer().isLeader()) {
              found[0] = i;
              return true;
            }
          return false;
        });
    return found[0];
  }

  /** Removes {@code index} from the configuration and waits until its own division has seen the new configuration. */
  private void leaveAndAwaitRemoval(final int index) {
    leftIndex = index;
    final RaftHAServer raft = getRaftPlugin(index).getRaftHAServer();
    final String peerId = raft.getLocalPeerId().toString();
    raft.leaveCluster(false);
    Awaitility.await("server " + index + " sees its own removal from the Raft configuration and is not the leader")
        .atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> !containsPeer(raft.getCommittedPeersOrNull(), peerId) && !raft.isLeader()
            && raft.isRemovedFromConfiguration());
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
