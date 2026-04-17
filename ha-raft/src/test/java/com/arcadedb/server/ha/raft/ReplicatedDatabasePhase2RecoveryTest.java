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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAPlugin;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the post-phase-2 recovery policy in {@link ReplicatedDatabase}:
 * <ul>
 *   <li>A successful first step-down short-circuits the retry loop.</li>
 *   <li>Step-down retries are bounded by {@code STEP_DOWN_MAX_ATTEMPTS}.</li>
 *   <li>After the retries are exhausted, the server stays up by default (the legacy fail-stop
 *       default on a transient CME was too aggressive).</li>
 *   <li>With {@code arcadedb.ha.stopServerOnReplicationFailure=true}, {@code server.stop()} runs
 *       after the retries are exhausted so an orchestrator can restart the node and let Raft log
 *       replay reconcile its state.</li>
 * </ul>
 * <p>
 * Uses lightweight test stubs rather than Mockito to match the project's testing conventions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReplicatedDatabasePhase2RecoveryTest {

  private static final String DB_NAME = "phase2RecoveryTestDB";
  private static final String TX_ID   = "tx-1234";

  @Test
  void successfulFirstStepDownSkipsRetriesAndDoesNotStopServer() throws Exception {
    final TestHAPlugin ha = new TestHAPlugin(0);
    final TestArcadeDBServer server = new TestArcadeDBServer(ha, false);

    joinRecovery(server);

    assertThat(ha.stepDownAttempts.get()).isEqualTo(1);
    assertThat(server.stopCalls.get()).isZero();
  }

  @Test
  void noLongerLeaderShortCircuits() throws Exception {
    final TestHAPlugin ha = new TestHAPlugin(0);
    ha.leader = false; // we already stepped down via another path; nothing to do
    final TestArcadeDBServer server = new TestArcadeDBServer(ha, false);

    joinRecovery(server);

    assertThat(ha.stepDownAttempts.get()).isZero();
    assertThat(server.stopCalls.get()).isZero();
  }

  @Test
  void stepDownFailuresExhaustRetriesAndLeaveServerUpByDefault() throws Exception {
    // stepDown throws every time (failuresBeforeSuccess > max attempts)
    final TestHAPlugin ha = new TestHAPlugin(10);
    final TestArcadeDBServer server = new TestArcadeDBServer(ha, false);

    joinRecovery(server);

    assertThat(ha.stepDownAttempts.get()).isEqualTo(3); // STEP_DOWN_MAX_ATTEMPTS
    assertThat(server.stopCalls.get()).as("Default policy: server must stay up after retries fail").isZero();
  }

  @Test
  void stepDownFailuresTriggerStopWhenOptInFlagSet() throws Exception {
    final TestHAPlugin ha = new TestHAPlugin(10);
    final TestArcadeDBServer server = new TestArcadeDBServer(ha, true);

    joinRecovery(server);

    assertThat(ha.stepDownAttempts.get()).isEqualTo(3);
    assertThat(server.stopCalls.get()).as("Opt-in policy: server.stop() called exactly once").isEqualTo(1);
  }

  @Test
  void serverStopFailureIsSwallowed() throws Exception {
    final TestHAPlugin ha = new TestHAPlugin(10);
    final TestArcadeDBServer server = new TestArcadeDBServer(ha, true);
    server.stopThrows = true;

    // Must not propagate - recovery thread must terminate cleanly so the JVM is not left hanging.
    joinRecovery(server);

    assertThat(ha.stepDownAttempts.get()).isEqualTo(3);
    assertThat(server.stopCalls.get()).isEqualTo(1);
  }

  private static void joinRecovery(final TestArcadeDBServer server) throws InterruptedException {
    final Thread t = ReplicatedDatabase.recoverLeadershipAfterPhase2Failure(server, DB_NAME, TX_ID);
    // 3 retries * 250 ms delay ≈ 750 ms; generous margin for slow CI.
    t.join(TimeUnit.SECONDS.toMillis(10));
    assertThat(t.isAlive()).as("Recovery thread must terminate within 10 s").isFalse();
  }

  /** Minimal {@link ArcadeDBServer} subclass that overrides only what the recovery path touches. */
  private static final class TestArcadeDBServer extends ArcadeDBServer {
    private final HAPlugin            ha;
    private final ContextConfiguration cfg;
    final AtomicInteger               stopCalls = new AtomicInteger();
    volatile boolean                  stopThrows;

    TestArcadeDBServer(final HAPlugin ha, final boolean stopOnFailure) {
      super(buildConfig(stopOnFailure));
      this.ha = ha;
      this.cfg = buildConfig(stopOnFailure);
    }

    private static ContextConfiguration buildConfig(final boolean stopOnFailure) {
      final ContextConfiguration c = new ContextConfiguration();
      c.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, stopOnFailure);
      return c;
    }

    @Override
    public ContextConfiguration getConfiguration() {
      return cfg;
    }

    @Override
    public HAPlugin getHA() {
      return ha;
    }

    @Override
    public synchronized void stop() {
      stopCalls.incrementAndGet();
      if (stopThrows)
        throw new RuntimeException("simulated server.stop() failure");
    }
  }

  /**
   * {@link HAPlugin} stub that fails {@code stepDown()} the first {@code failuresBeforeSuccess}
   * times. The default {@link #leader} is {@code true} so the recovery path enters the retry loop.
   */
  private static final class TestHAPlugin implements HAPlugin {
    volatile boolean       leader              = true;
    final    int           failuresBeforeSuccess;
    final    AtomicInteger stepDownAttempts    = new AtomicInteger();

    TestHAPlugin(final int failuresBeforeSuccess) {
      this.failuresBeforeSuccess = failuresBeforeSuccess;
    }

    @Override
    public void configure(final com.arcadedb.server.ArcadeDBServer server, final ContextConfiguration configuration) {
    }

    @Override
    public void startService() {
    }

    @Override
    public boolean isActive() {
      return true;
    }

    @Override
    public boolean isLeader() {
      return leader;
    }

    @Override
    public void stepDown() {
      final int attempt = stepDownAttempts.incrementAndGet();
      if (attempt <= failuresBeforeSuccess)
        throw new RuntimeException("simulated step-down failure on attempt " + attempt);
    }

    @Override
    public String getClusterToken() {
      return null;
    }

    @Override
    public long getCommitIndex() {
      return 0;
    }

    @Override
    public String getLeaderHTTPAddress() {
      return null;
    }

    @Override
    public String getLeaderName() {
      return null;
    }

    @Override
    public String getElectionStatus() {
      return "test";
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public int getConfiguredServers() {
      return 1;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public String getServerName() {
      return "test";
    }

    @Override
    public long getLastAppliedIndex() {
      return 0;
    }

    @Override
    public JSONObject exportClusterStatus() {
      return new JSONObject();
    }

    @Override
    public void replicateCreateDatabase(final String databaseName) {
    }

    @Override
    public void replicateDropDatabase(final String databaseName) {
    }

    @Override
    public void replicateCreateUser(final String userJson) {
    }

    @Override
    public void replicateUpdateUser(final String userJson) {
    }

    @Override
    public void replicateDropUser(final String userName) {
    }
  }
}
