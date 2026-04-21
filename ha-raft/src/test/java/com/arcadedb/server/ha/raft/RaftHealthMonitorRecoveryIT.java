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
import com.arcadedb.log.LogManager;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: verifies that the HealthMonitor detects a follower whose Ratis server
 * appears to be in CLOSED state and triggers in-place recovery within a 3-node cluster.
 * <p>
 * The test injects a simulated CLOSED state via the test hook
 * {@link RaftHAServer#forceRaftStateForTesting(LifeCycle.State)}, waits for the
 * HealthMonitor to invoke {@link RaftHAServer#restartRatisIfNeeded()}, and then verifies
 * that the follower successfully replicates subsequent writes from the leader.
 * <p>
 * Note: HealthMonitor is disabled in {@link BaseRaftHATest#onServerConfiguration} by default.
 * This test re-enables it with a short 500 ms interval so recovery happens quickly.
 */
class RaftHealthMonitorRecoveryIT extends BaseRaftHATest {

  private static final long HEALTH_CHECK_INTERVAL_MS = 500L;
  private static final long RECOVERY_TIMEOUT_MS      = 20_000L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Re-enable HealthMonitor with a short interval so the test runs quickly.
    // The base class sets this to 0 (disabled) to avoid thread exhaustion during teardown;
    // we accept that risk here because this test exercises the monitor explicitly.
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, HEALTH_CHECK_INTERVAL_MS);
  }

  @Test
  void healthMonitorRecoversFollowerFromClosedState() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // Pick the first follower that is not the leader
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();

    // Phase 1: write initial data to confirm cluster is fully live, then wait
    // for the follower to apply those log entries (not a full page-level comparison
    // since the HealthMonitor may have restarted Ratis during setup)
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("HealthRecovery"))
        leaderDb.getSchema().createVertexType("HealthRecovery");
    });

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++)
        leaderDb.newVertex("HealthRecovery").set("phase", 1).set("idx", i).save();
    });

    final long indexAfterPhase1 = leaderRaft.getLastAppliedIndex();
    // Wait for the follower to catch up to the leader's current index
    followerRaft.waitForAppliedIndex(indexAfterPhase1);

    // Sanity: follower should see all 50 records from phase 1
    assertThat(getServerDatabase(followerIndex, getDatabaseName()).countType("HealthRecovery", true))
        .as("Follower must see 50 phase-1 records before fault injection").isEqualTo(50);

    // Record the follower's applied index after phase-1 writes are stable
    final long indexBeforeFault = followerRaft.getLastAppliedIndex();
    assertThat(indexBeforeFault).as("Follower must have applied at least one log entry").isGreaterThan(0);

    LogManager.instance().log(this, Level.INFO,
        "TEST: Follower %d lastAppliedIndex before fault injection = %d", followerIndex, indexBeforeFault);

    // Phase 2: inject CLOSED state into the follower to simulate a Ratis network-partition crash.
    // forceRaftStateForTesting causes the next getRaftLifeCycleState() call to return CLOSED,
    // which will trigger the HealthMonitor's tick to call restartRatisIfNeeded().
    followerRaft.forceRaftStateForTesting(LifeCycle.State.CLOSED);

    LogManager.instance().log(this, Level.INFO,
        "TEST: CLOSED state injected into follower %d, waiting for HealthMonitor to recover", followerIndex);

    // Phase 3: write more data on the leader while the follower has the injected fault.
    // The HealthMonitor will detect the CLOSED state and call restartRatisIfNeeded() on the follower.
    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++)
        leaderDb.newVertex("HealthRecovery").set("phase", 2).set("idx", i).save();
    });

    final long indexAfterLeaderWrite = leaderRaft.getLastAppliedIndex();
    assertThat(indexAfterLeaderWrite).as("Leader must have advanced its applied index after phase-2 write")
        .isGreaterThan(indexBeforeFault);

    // Phase 4: wait for the HealthMonitor to fire (CLOSED state was injected above), trigger
    // restartRatisIfNeeded(), and for the follower to replicate the phase-2 writes.
    // The HealthMonitor grace period is 2 * intervalMs from start, so by now the first
    // scheduled tick has already fired. The injected CLOSED state is consumed on the next tick.
    final long deadline = System.currentTimeMillis() + RECOVERY_TIMEOUT_MS;
    long followerAppliedIndex = followerRaft.getLastAppliedIndex();

    while (System.currentTimeMillis() < deadline && followerAppliedIndex < indexAfterLeaderWrite) {
      Thread.sleep(200);
      followerAppliedIndex = followerRaft.getLastAppliedIndex();
    }

    LogManager.instance().log(this, Level.INFO,
        "TEST: Follower %d lastAppliedIndex after recovery wait = %d (leader = %d)",
        followerIndex, followerAppliedIndex, indexAfterLeaderWrite);

    assertThat(followerAppliedIndex)
        .as("Follower must have caught up to the leader's applied index after HealthMonitor recovery")
        .isGreaterThanOrEqualTo(indexAfterLeaderWrite);

    // Phase 5: confirm data integrity - recovered follower sees all 100 records
    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    final long count = followerDb.countType("HealthRecovery", true);
    assertThat(count)
        .as("Recovered follower must see all 100 HealthRecovery records (50 per phase)")
        .isEqualTo(100);
  }
}
