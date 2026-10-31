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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The in-place Ratis restart ({@link RaftHAServer#restartRatisIfNeeded()}) driven by the test itself, against real
 * Ratis, with the health monitor OFF (the base default): a closed division stays closed until the test restarts it.
 * Merged from {@code Issue4839RecoveryRewiresStateMachineIT}, {@code Issue8901ReplicationPathAfterInPlaceRestartIT} and
 * {@code Issue9411LeaderStateMachineClosedMidCommitIT}, which ran on the same fixture: three servers, persistent Raft
 * storage, {@code majority} quorum and a 10 s quorum timeout (both the defaults; #9411 set the timeout explicitly, the
 * other two relied on it). The in-place restarts that the HEALTH MONITOR drives live in
 * {@link HaInPlaceRestartRecoveryHealthMonitorIT} (volatile storage) and {@code Issue8900ClosedServerInPlaceRestartIT}
 * (persistent storage).
 * <ul>
 * <li><b>#4839</b>: the restart builds a brand-new {@link ArcadeStateMachine}, and before the fix the recovery path
 * called only {@code setServer} and forgot {@code setRaftHAServer(this)}. A node recovered through that path could no
 * longer install snapshots from the leader (NPE), never marked its locally-originated transactions (a future leader
 * re-applies its own committed txns, divergence), and silently skipped leader-change handling. The rebuilt machine must
 * be fully wired and the follower must still replicate.</li>
 * <li><b>#8901</b>: the signal that holds the stale-term reformat back is armed by the in-place restart and cleared by
 * the first replicated entry the restarted division takes. A node never restarted in place never holds the reformat back
 * (#4741). The dead path itself (the leader's log stream still bound to the closed instance) is timing-dependent (#8898,
 * #8900), so the hold it triggers is covered by {@code Issue8901DeadReplicationPathNoReformatTest}.</li>
 * <li><b>#9411</b>: a leader commit whose leader state machine closes under it. An in-place restart between the
 * registration of the prepared transaction and the dispatch of its entry (the race of #8785) must not publish the entry a
 * second time (the bucket counters would count the record twice); a leader division closed for good keeps its LEADER
 * role, so every commit on it blocked for the whole quorum timeout before #9411; it is now refused, retryably, before
 * anything is prepared.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class HaInPlaceRestartRecoveryIT extends BaseRaftHATest {

  private static final String TYPE_NAME      = "Issue9411";
  private static final int    RECORDS_BEFORE = 5;
  private static final long   QUORUM_TIMEOUT = 10_000L;

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

  /**
   * Issue #4839, from {@code Issue4839RecoveryRewiresStateMachineIT}: triggers the restart the HealthMonitor would and
   * asserts the freshly built state machine is wired ({@code getRaftHAServer() != null}) and the recovered follower still
   * replicates writes from the leader.
   */
  @Test
  void restartRewiresStateMachineRaftHAServer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    assertThat(replicaIndex).as("a replica must exist").isGreaterThanOrEqualTo(0);

    final RaftHAServer follower = getRaftPlugin(replicaIndex).getRaftHAServer();

    // Sanity: at startup the state machine is fully wired.
    final ArcadeStateMachine before = follower.getStateMachine();
    assertThat(before.getRaftHAServer())
        .as("state machine built at startup must be wired to the owning RaftHAServer")
        .isSameAs(follower);

    // Trigger the in-place HealthMonitor recovery path (CLOSED/EXCEPTION restart). It replaces the
    // state machine with a fresh instance.
    LogManager.instance().log(this, Level.INFO, "TEST: triggering Ratis restart on replica %d", replicaIndex);
    follower.restartRatisIfNeeded();

    final ArcadeStateMachine after = follower.getStateMachine();
    assertThat(after).as("recovery must build a new state machine instance").isNotSameAs(before);

    // The regression: before the fix this field was null on the recovery-built machine.
    assertThat(after.getRaftHAServer())
        .as("recovery-built state machine must be wired to its RaftHAServer (issue #4839)")
        .isSameAs(follower);

    // The recovered follower must remain a working cluster member: a write on the leader must still
    // replicate to it. (With a null raftHAServer the snapshot-install path would NPE.)
    waitForReplicationIsCompleted(replicaIndex);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("PostRecovery"))
        leaderDb.getSchema().createVertexType("PostRecovery");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final MutableVertex v = leaderDb.newVertex("PostRecovery");
        v.set("name", "post-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    final var followerDb = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(followerDb.countType("PostRecovery", true))
        .as("recovered follower must receive writes replicated after the restart")
        .isEqualTo(20);
  }

  /** Issue #8901, from {@code Issue8901ReplicationPathAfterInPlaceRestartIT}. */
  @Test
  void anInPlaceRestartHoldsTheReformatUntilAnEntryArrives() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer follower = getRaftPlugin(replicaIndex).getRaftHAServer();

    assertThat(follower.isReplicationPathUnprovenSinceRestart())
        .as("a division never restarted in place has no old server instance that could hold the leader's stream")
        .isFalse();

    waitForReplicationIsCompleted(replicaIndex);
    follower.restartRatisIfNeeded();

    assertThat(follower.isReplicationPathUnprovenSinceRestart())
        .as("right after the in-place restart no entry has reached the new division yet")
        .isTrue();
    // Issue #9013: the same state, machine-readable in the follower's own status document.
    assertThat(queryClusterEndpoint(replicaIndex).getBoolean("localReplicationPathUnproven")).isTrue();

    // Issue #8953: an idle cluster sends the restarted follower no entry either, so "unproven" alone must not read as an
    // unreachable leader. The health monitor is off in this fixture, so its two hooks are driven here, for longer than
    // the grace, on the real division: the leader's heartbeat makes it known and its commit index is what we hold.
    final ContextConfiguration config = getServer(replicaIndex).getConfiguration();
    final long grace = 2L * RaftPropertiesBuilder.electionTimeoutMaxFor(
        config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN),
        config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX));
    Awaitility.await().during(grace + 2_000L, TimeUnit.MILLISECONDS).atMost(grace + 30_000L, TimeUnit.MILLISECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
          follower.trackLeaderReachSinceRestart();
          follower.refreshLeaderCommitIndex();
          return follower.getLeaderUnreachableSinceRestart() == null;
        });
    final JSONObject idle = queryClusterEndpoint(replicaIndex);
    assertThat(idle.getBoolean("localLeaderUnreachableSinceRestart")).isFalse();
    assertThat(idle.getJSONArray("alerts").toString()).doesNotContain("follower-leader-unreachable-since-restart");

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Issue8901"))
        leaderDb.getSchema().createVertexType("Issue8901");
    });
    leaderDb.transaction(() -> {
      final MutableVertex v = leaderDb.newVertex("Issue8901");
      v.set("name", "after-restart");
      v.save();
    });

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> !follower.isReplicationPathUnprovenSinceRestart());

    waitForReplicationIsCompleted(replicaIndex);
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("Issue8901", true)).isEqualTo(1L);
    assertThat(queryClusterEndpoint(replicaIndex).getBoolean("localReplicationPathUnproven")).isFalse();
  }

  /** Issue #9411, from {@code Issue9411LeaderStateMachineClosedMidCommitIT}. */
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

  /** Issue #9411, from {@code Issue9411LeaderStateMachineClosedMidCommitIT}. */
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

  private JSONObject queryClusterEndpoint(final int serverIndex) throws Exception {
    final URL url = URI.create("http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/cluster").toURL();
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
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
      final long scanned = withResyncRetry(index, HaInPlaceRestartRecoveryIT::scanCount);
      final long stored = withResyncRetry(index, HaInPlaceRestartRecoveryIT::storedCount);
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
