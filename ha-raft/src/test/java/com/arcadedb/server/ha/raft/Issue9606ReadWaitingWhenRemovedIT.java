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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9606, against real Ratis: a READ_YOUR_WRITES read already waiting on a follower for a bookmark ahead of it, when
 * that follower is removed from the Raft configuration, used to keep waiting for the whole quorum timeout before #9590's
 * re-check refused it. It is now refused as soon as the follower learns of its removal: at once when it removed itself,
 * and within one recheck interval when the leader removed it, since a removed node is cut off before it applies the
 * entry that removes it and nothing tells its state machine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue9606ReadWaitingWhenRemovedIT extends BaseRaftHATest {

  private static final String TYPE_NAME      = "Issue9606";
  private static final long   QUORUM_TIMEOUT = 10_000L;

  /** The server that left the cluster: excluded from the end-of-test comparison, it diverges by design. */
  private volatile int removedIndex = -1;

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
    return serversMatching(i -> i != removedIndex && getServer(i) != null && getServer(i).isStarted());
  }

  @Test
  void aWaitingReadIsRefusedPromptlyWhenTheFollowerLeavesTheCluster() throws Exception {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final CompletableFuture<Throwable> read = readWaitingOn(followerIndex, leaderIndex);

    removedIndex = followerIndex;
    getRaftPlugin(followerIndex).getRaftHAServer().leaveCluster(false);
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertRefused(read.get(60, TimeUnit.SECONDS));
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a read refused once its node removed itself, rather than one waiting out the quorum timeout");
  }

  @Test
  void aWaitingReadIsRefusedPromptlyWhenTheLeaderRemovesTheFollower() throws Exception {
    final int leaderIndex = prepareType();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final CompletableFuture<Throwable> read = readWaitingOn(followerIndex, leaderIndex);

    removedIndex = followerIndex;
    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();
    getRaftPlugin(leaderIndex).getRaftHAServer().removePeer(follower.getLocalPeerId().toString());
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertRefused(read.get(60, TimeUnit.SECONDS));
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a read refused within a membership recheck of its node's removal, rather than one waiting out the quorum timeout");
  }

  /**
   * Starts a READ_YOUR_WRITES read on the follower with a bookmark far past the leader's commit index, so it waits, and
   * returns once it is waiting: still running while the follower is a member and the bookmark is ahead.
   */
  private CompletableFuture<Throwable> readWaitingOn(final int followerIndex, final int leaderIndex) {
    final long bookmark = getRaftPlugin(leaderIndex).getRaftHAServer().getCommitIndex() + 1_000_000L;
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final CompletableFuture<Throwable> read = CompletableFuture.supplyAsync(() -> {
      RaftReplicatedDatabase.applyReadConsistencyContext(Database.READ_CONSISTENCY.READ_YOUR_WRITES, bookmark);
      try (final ResultSet rs = followerDb.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
        rs.next();
        return null;
      } catch (final Throwable t) {
        return t;
      } finally {
        RaftReplicatedDatabase.removeReadConsistencyContext();
      }
    });
    // Waiting rather than refused or served: the follower is a member and the bookmark is unreachable
    Awaitility.await().pollDelay(500, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> true);
    assertThat(read).as("the read is waiting for its bookmark").isNotDone();
    return read;
  }

  private static void assertRefused(final Throwable failure) {
    assertThat(failure).isInstanceOf(NeedRetryException.class).hasMessageContaining("not a member")
        .hasMessageContaining(Database.READ_CONSISTENCY.READ_YOUR_WRITES.name());
  }

  /** Creates the type on the leader and waits for every server; returns the leader's index. */
  private int prepareType() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().createDocumentType(TYPE_NAME));
    leaderDb.transaction(() -> leaderDb.newDocument(TYPE_NAME).set("id", 0).save());
    waitForAllServers();
    return leaderIndex;
  }
}
