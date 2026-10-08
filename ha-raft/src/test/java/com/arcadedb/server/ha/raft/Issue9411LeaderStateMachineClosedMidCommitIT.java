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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9411, against real Ratis: what happens to a leader commit when the leader's state machine closes under it.
 * <ul>
 *   <li>An in-place Ratis restart between the registration of the prepared transaction and the dispatch of its entry
 *   (the race of #8785): the replacement state machine applies the entry from its WAL bytes, and the committing thread
 *   must not publish it a second time - the bucket counters would count the record twice.</li>
 *   <li>A leader division closed for good, without a shutdown or a restart: Ratis keeps its LEADER role, so before
 *   #9411 every commit on it blocked for the whole quorum timeout and was released unpublished. It is refused,
 *   retryably, before anything is prepared.</li>
 * </ul>
 * The health monitor is off in this fixture, so a closed division stays closed until the test restarts it.
 */
@Tag("slow")
class Issue9411LeaderStateMachineClosedMidCommitIT extends BaseRaftHATest {

  private static final String TYPE_NAME       = "Issue9411";
  private static final int    RECORDS_BEFORE  = 5;
  private static final long   QUORUM_TIMEOUT  = 10_000L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, QUORUM_TIMEOUT);
  }

  @AfterEach
  void clearHook() {
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = null;
  }

  @Test
  void anInPlaceRestartBetweenRegistrationAndDispatchCountsTheRecordOnce() {
    final int leaderIndex = prepareType();
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    final AtomicBoolean fired = new AtomicBoolean();
    final AtomicInteger registeredWithTheOldMachine = new AtomicInteger(-1);
    final AtomicReference<ArcadeStateMachine> oldMachine = new AtomicReference<>();
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = databaseName -> {
      if (!getDatabaseName().equals(databaseName) || !fired.compareAndSet(false, true))
        return;
      final ArcadeStateMachine before = leaderRaft.getStateMachine();
      oldMachine.set(before);
      registeredWithTheOldMachine.set(before.pendingLocalCommits());
      // The in-place restart the health monitor performs on a dead division: the old machine is closed with the
      // transaction registered and unclaimed, and the entry is then dispatched through the restarted server.
      restartInPlaceAndAwaitLeader(leaderRaft);
    };

    leaderDb.begin();
    leaderDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE).save();
    leaderDb.commit();
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = null;

    // The scenario must actually have happened, or the counts below prove nothing.
    assertThat(fired.get()).as("the pre-dispatch hook must have run").isTrue();
    assertThat(registeredWithTheOldMachine.get()).as("the commit was registered with the machine the restart closed")
        .isEqualTo(1);
    assertThat(oldMachine.get().isClosed()).as("the restart closed the machine the commit registered with").isTrue();
    assertThat(leaderRaft.getStateMachine()).as("the restart replaced the state machine").isNotSameAs(oldMachine.get());

    assertCountedOnceEverywhere(RECORDS_BEFORE + 1);
  }

  @Test
  void aLeaderWhoseDivisionClosedForGoodRefusesCommitsInsteadOfStalling() throws Exception {
    final int leaderIndex = prepareType();
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // A dead division nobody restarts: the health monitor is off here.
    leaderRaft.getRaftDivision().close();
    assertThat(leaderRaft.getStateMachine().isClosed()).isTrue();
    // The question #9411 asked: can such a node still take writes as leader? Ratis keeps the role of a closed division.
    assertThat(leaderRaft.isLeader()).as("a closed division keeps reporting the LEADER role").isTrue();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    leaderDb.begin();
    leaderDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE).save();
    assertThatThrownBy(leaderDb::commit).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("state machine is closed");
    stopwatch.assertGaveUpWithin(QUORUM_TIMEOUT / 2,
        "a refusal before replication, rather than a wait for an apply that no local state machine will run");
    assertThat(leaderDb.isTransactionActive()).as("the refused transaction is rolled back").isFalse();

    // Once the division is restarted in place the node takes writes again, through whichever node leads now.
    restartInPlaceAndAwaitLeader(leaderRaft);
    leaderDb.begin();
    leaderDb.newDocument(TYPE_NAME).set("id", RECORDS_BEFORE + 1).save();
    leaderDb.commit();

    assertCountedOnceEverywhere(RECORDS_BEFORE + 1);
  }

  /**
   * Restarts the division in place, then waits for the election it causes to settle on this node: a leader known and
   * the Raft client rebuilt for it. The rebuild interrupts whatever the old client was waiting for, which turns a commit
   * dispatched before it into an indeterminate outcome rather than into the acknowledged one under test.
   */
  private void restartInPlaceAndAwaitLeader(final RaftHAServer raft) {
    raft.restartRatisIfNeeded();
    final RaftTransactionBroker restarted = raft.getTransactionBroker();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> raft.getLeaderId() != null && raft.getTransactionBroker() != restarted && findLeaderIndex() >= 0);
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

  private void assertCountedOnceEverywhere(final long expected) {
    waitForAllServers();
    for (int serverIndex = 0; serverIndex < getServerCount(); serverIndex++) {
      final int index = serverIndex;
      final long scanned = withResyncRetry(index, Issue9411LeaderStateMachineClosedMidCommitIT::scanCount);
      final long stored = withResyncRetry(index, Issue9411LeaderStateMachineClosedMidCommitIT::storedCount);
      assertThat(scanned).as("records on server %d", index).isEqualTo(expected);
      assertThat(stored).as("stored count(*) on server %d must match a scan of the same type", index).isEqualTo(scanned);
    }
  }

  /** {@code count(*)} over the type, answered from the stored per-bucket counters. */
  private static long storedCount(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME)) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }

  /** The same count by a scan of the records, which never reads the stored counters. */
  private static long scanCount(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME + " WHERE id IS NOT NULL")) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }
}
