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
import org.apache.ratis.server.raftlog.RaftLogBase;
import org.apache.ratis.util.LifeCycle;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8652, against a real follower division: the two shapes of a node whose Raft machinery died
 * under a division the lifecycle checks could not condemn.
 * <ul>
 *   <li>the {@code StateMachineUpdater} thread ends (any throwable that escapes it): the node stops applying and Ratis
 *       closes the division from the dying thread;</li>
 *   <li>the Raft log is closed under a division that reports {@code RUNNING}: every append is rejected with
 *       {@code SegmentedRaftLog: Failed to append}, yet Ratis never calls {@code notifyLogFailed}, so the #7037 mark
 *       stayed empty.</li>
 * </ul>
 * In both, the health monitor must restart the node in place and it must replicate again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8652DeadUpdaterRecoveryIT extends BaseRaftHATest {

  private static final long HEALTH_CHECK_INTERVAL_MS = 500L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, HEALTH_CHECK_INTERVAL_MS);
  }

  @Test
  void aFollowerWhoseUpdaterDiedIsRestartedAndReplicatesAgain() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    writeOnLeader(leaderIndex, "UpdaterDied", 1);
    waitForFollowerToApply(followerIndex, leaderIndex);

    final ArcadeStateMachine before = followerRaft.getStateMachine();
    final Thread updater = before.getApplyThreadForTesting();
    assertThat(updater).as("the follower applied an entry, so its updater was seen").isNotNull();
    assertThat(before.describeDeadApplyThread()).isNull();

    // Ratis reads an interrupt of a RUNNING updater as a failure: it logs "caught a Throwable" and closes the division
    // from that thread, the exact end of the chaos run's zombie.
    updater.interrupt();
    updater.join(10_000L);

    // The state machine says so directly, whatever the division reports about itself.
    assertThat(before.describeDeadApplyThread()).contains("terminated");

    awaitRecoveredAndReplicating(leaderIndex, followerIndex, "UpdaterDied");
    assertThat(followerRaft.getStateMachine()).as("a fresh state machine replaced the one whose updater died")
        .isNotSameAs(before);
  }

  @Test
  void aFollowerWhoseRaftLogWasClosedUnderARunningDivisionIsRestartedAndReplicatesAgain() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    writeOnLeader(leaderIndex, "LogClosed", 1);
    waitForFollowerToApply(followerIndex, leaderIndex);

    assertThat(followerRaft.isRaftLogClosed()).as("healthy log").isFalse();
    assertThat(followerRaft.getRaftLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);

    final ArcadeStateMachine before = followerRaft.getStateMachine();
    // Closes the log only, leaving the division's own lifecycle untouched.
    ((RaftLogBase) followerRaft.getRaftDivision().getRaftLog()).close();

    // The blind spot: not a writer failure Ratis reported, so the #7037 mark stays empty; the closed log is what shows.
    // The health monitor may already have restarted the node
    Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> followerRaft.isRaftLogClosed() || followerRaft.getStateMachine() != before);

    awaitRecoveredAndReplicating(leaderIndex, followerIndex, "LogClosed");
  }

  private void writeOnLeader(final int leaderIndex, final String type, final int value) {
    final var db = getServerDatabase(leaderIndex, getDatabaseName());
    db.transaction(() -> {
      if (!db.getSchema().existsType(type))
        db.getSchema().createVertexType(type);
      db.newVertex(type).set("v", value).save();
    });
  }

  private void waitForFollowerToApply(final int followerIndex, final int leaderIndex) {
    final long leaderApplied = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(getRaftPlugin(followerIndex).getRaftHAServer().getLastAppliedIndex())
            .isGreaterThanOrEqualTo(leaderApplied));
  }

  private void awaitRecoveredAndReplicating(final int leaderIndex, final int followerIndex, final String type) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          final RaftHAServer recovered = getRaftPlugin(followerIndex).getRaftHAServer();
          assertThat(recovered.getRaftLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);
          assertThat(recovered.isRaftLogClosed()).isFalse();
          assertThat(recovered.getDeadStateMachineUpdater()).isNull();
        });

    writeOnLeader(leaderIndex, type, 2);
    final long leaderApplied = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(getRaftPlugin(followerIndex).getRaftHAServer().getLastAppliedIndex())
            .as("the restarted follower replicates again").isGreaterThanOrEqualTo(leaderApplied));
  }
}
