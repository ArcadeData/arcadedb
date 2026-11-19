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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How a leader steps down after a failed commit (phase 2): transfer attempts, the fallback, the stop-on-failure setting and the retry loop.
 */
class StepDownFailureHandlingTest {
  // ---- #7127 ----
  // Issue #7127: exercise the real step-down loop, including recovery through a real database commit.
  // Only the replication/leadership-transfer boundary and process stop are simulated, so failures are
  // deterministic without waiting for a cluster's network timeouts.

  private final ContextConfiguration config = configuration();
  private final FakeArcadeDBServer server = FakeArcadeDBServer.create();
  private final CountDownLatch stopped = new CountDownLatch(1);
  private ControlledTransfers raft;

  @BeforeEach
  void setUp() {
    server.returns("getServerName", "ArcadeDB_0");
    server.returns("getConfiguration", config);
    server.on("stop", args -> {
      stopped.countDown();
      return null;
    });
    raft = new ControlledTransfers(server, config);
  }

  @AfterEach
  void tearDown() {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
    raft.stop();
  }

  // Regression for #7127: exhausted transfers must not report successful step down
  @Test
  void exhaustedTransfersMustNotReportSuccessfulStepDown() {
    assertThatThrownBy(raft::stepDown)
        .as("a leader that could not transfer leadership must report failure to phase-2 recovery")
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("Cannot step down");
    assertThat(raft.targetedAttempts).isEqualTo(2);
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  // Regression for #7127: leadership lost during fallback must not be reported as transfer failure
  @Test
  void leadershipLostDuringFallbackMustNotBeReportedAsTransferFailure() {
    raft.loseLeadershipOnFallbackAttempt = 1;

    assertThatThrownBy(raft::stepDown)
        .as("phase-2 recovery must stop retrying when this node is already a follower")
        .isInstanceOf(NotTheLeaderRefusalException.class);
    assertThat(raft.targetedAttempts).isEqualTo(2);
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  // Regression for #7127: no eligible candidates and failed fallback must report failure
  @Test
  void noEligibleCandidatesAndFailedFallbackMustReportFailure() {
    raft.stop();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    raft = new ControlledTransfers(server, config);

    assertThatThrownBy(raft::stepDown).isInstanceOf(ReplicationException.class);
    assertThat(raft.targetedAttempts).isZero();
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  // Regression for #7127: successful targeted transfer must not try the fallback
  @Test
  void successfulTargetedTransferMustNotTryTheFallback() {
    raft.targetedTransferSucceeds = true;

    assertThatCode(raft::stepDown).doesNotThrowAnyException();
    assertThat(raft.targetedAttempts).isEqualTo(1);
    assertThat(raft.fallbackAttempts).isZero();
  }

  // Regression for #7127: successful fallback must not report failure
  @Test
  void successfulFallbackMustNotReportFailure() {
    raft.successfulFallbackAttempt = 1;

    assertThatCode(raft::stepDown).doesNotThrowAnyException();
    assertThat(raft.targetedAttempts).isEqualTo(2);
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  // Regression for #7127: booleans
  @ParameterizedTest
  @ValueSource(booleans = { true, false }
)
  void phase2FailureRetriesRealStepDownAndHonorsStopSetting(final boolean stopOnFailure) throws InterruptedException {
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, stopOnFailure);

    commitWithPhase2Failure();

    assertThat(raft.fallbackAttempts).as("all three real step-down attempts must be exhausted").isEqualTo(3);
    assertThat(raft.targetedAttempts).isEqualTo(6);
    if (stopOnFailure) {
      // recoverLeadershipAfterPhase2Failure() runs server.stop() on its own daemon thread, so the latch is the
      // only completion signal available here. The bound is a HANG DETECTOR, not a latency claim - it separates
      // "the emergency-stop branch was reached" from "it was never reached at all", and the work behind it is a
      // single call on a mock. Generous on purpose: a wider bound cannot turn a passing run red, while a tight
      // one turns a full-suite stop-the-world pause into a false failure (see CLAUDE.md on wall-clock bounds).
      assertThat(stopped.await(60, TimeUnit.SECONDS)).as("emergency server stop must be reached").isTrue();
      assertThat(server.calls("stop")).hasSize(1);
    } else
      assertThat(server.calls("stop")).isEmpty();
  }

  // Regression for #7127: phase2 recovery stops retrying after successful transfer
  @Test
  void phase2RecoveryStopsRetryingAfterSuccessfulTransfer() {
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, true);
    raft.successfulFallbackAttempt = 2;

    commitWithPhase2Failure();

    assertThat(raft.fallbackAttempts).isEqualTo(2);
    assertThat(raft.targetedAttempts).isEqualTo(4);
    assertThat(server.calls("stop")).isEmpty();
  }

  // Regression for #7127: ints
  @ParameterizedTest
  @ValueSource(ints = { 1, 3 }
)
  void phase2RecoveryMustNotStopANodeThatLostLeadership(final int demotionAttempt) {
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, true);
    raft.loseLeadershipOnFallbackAttempt = demotionAttempt;

    commitWithPhase2Failure();

    assertThat(raft.fallbackAttempts).isEqualTo(demotionAttempt);
    assertThat(raft.targetedAttempts).isEqualTo(2 * demotionAttempt);
    assertThat(raft.isLeader()).isFalse();
    assertThat(server.calls("stop")).isEmpty();
  }

  private void commitWithPhase2Failure() {
    final String path = "target/databases/issue7127-" + UUID.randomUUID();
    final LocalDatabase local = (LocalDatabase) new DatabaseFactory(path).create();
    try {
      local.getSchema().createDocumentType("Item", 1);
      local.transaction(() -> local.newDocument("Item").set("value", 0).save());

      final RaftReplicatedDatabase database = new RaftReplicatedDatabase(server, local, raft);
      database.begin();
      final MutableDocument document = database.iterateType("Item", false).next().asDocument().modify();
      document.set("value", 1).save();

      final IllegalStateException fault = new IllegalStateException("simulated local phase-2 apply failure");
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = dbName -> {
        throw fault;
      };
      assertThatThrownBy(database::commit)
          .isInstanceOf(TransactionCommittedRemotelyException.class)
          .hasMessageContaining("Do NOT retry")
          .hasCause(fault);
      assertThat(raft.broker.calls("replicateTransaction")).hasSize(1);
    } finally {
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
      local.setWrappedDatabaseInstance(local);
      local.rollbackAllNested();
      local.drop();
    }
  }

  private static ContextConfiguration configuration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    return config;
  }

  private static class ControlledTransfers extends RaftHAServer {
    // Every configured peer answers (issue #8556): these tests are about the step-down loop, not reachability.
    @Override
    Set<String> handoffReachablePeers() {
      return getLivePeers().stream().map(peer -> peer.getId().toString()).collect(Collectors.toSet());
    }

    private final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    private int targetedAttempts;
    private int fallbackAttempts;
    private boolean leader = true;
    private boolean targetedTransferSucceeds;
    private int successfulFallbackAttempt = Integer.MAX_VALUE;
    private int loseLeadershipOnFallbackAttempt = Integer.MAX_VALUE;

    private ControlledTransfers(final ArcadeDBServer server, final ContextConfiguration config) {
      super(server, config);
      broker.returns("replicateTransaction", 1L);
    }

    @Override
    public RaftTransactionBroker getTransactionBroker() {
      return broker;
    }

    // Since issue #8781 a leader with a live state machine never publishes pages on the committing thread: it waits for
    // the apply, which nothing here performs. Phase 2 runs on the committing thread - where the fault is injected and the
    // step-down recovery under test starts - only when no state machine is wired, the Raft server's startup window.
    @Override
    public ArcadeStateMachine getStateMachine() {
      return null;
    }

    @Override
    public boolean isLeader() {
      return leader;
    }

    @Override
    public void transferLeadership(final String targetPeerId, final long timeoutMs) {
      targetedAttempts++;
      if (targetedTransferSucceeds) {
        leader = false;
        return;
      }
      throw new ReplicationException("Transfer timed out: " + targetPeerId);
    }

    // stepDown()'s fallback is the bare step-down, not the no-target transferLeadership(long): since issue #8480 that
    // one tries the same explicit candidates first, which the step-down loop has just tried.
    @Override
    boolean stepDownWithoutTarget(final long timeoutMs) {
      fallbackAttempts++;
      if (fallbackAttempts == successfulFallbackAttempt) {
        leader = false;
        return true;
      }
      if (fallbackAttempts == loseLeadershipOnFallbackAttempt)
        leader = false;
      return false;
    }
  }
  /**
   * Regression test for the retry half of issue #7134.
   *
   * The phase-2 failure recovery re-checks {@code isLeader()} and then calls {@code stepDown()} up to three times
   * with 500ms between attempts. Once {@code stepDown()} refuses because this node is no longer the leader, that
   * refusal is not a transient failure - leadership has already moved, there is nothing left to hand off, and
   * every remaining attempt is guaranteed to refuse identically. Retrying burns ~1.5s on the commit path and,
   * worse, ends at the "all step-down attempts failed" branch, which under
   * {@code arcadedb.ha.stopServerOnReplicationFailure} stops a server whose leadership problem has already
   * resolved itself.
   */
  @Nested
  class RefusalStopsTheRetry {

    private static final String NEW_LEADER = "peer-b_2435";

    /**
     * Drives the private recovery directly: reaching it through a real phase-2 failure would need a live cluster,
     * and the behaviour under test is entirely local to the retry loop.
     */
    private static void recover(final RaftReplicatedDatabase db) throws Exception {
      final Method m = RaftReplicatedDatabase.class
          .getDeclaredMethod("recoverLeadershipAfterPhase2Failure", String.class);
      m.setAccessible(true);
      m.invoke(db, "tx-1");
    }

    private static RaftReplicatedDatabase databaseWith(final RaftHAServer raft, final ArcadeDBServer server) {
      return new RaftReplicatedDatabase(server, mock(LocalDatabase.class), raft);
    }

    // Regression for #7134: a refusal stops the retry loop immediately and never stops the server
    @Test
    void aRefusalStopsTheRetryLoopImmediatelyAndNeverStopsTheServer() throws Exception {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(true); // true when recovery starts, then leadership moves
      raft.fails("stepDown", new NotTheLeaderRefusalException("Refusing to step down",
          RaftPeerId.valueOf(NEW_LEADER)));

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, true);
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getConfiguration", config);

      recover(databaseWith(raft, server));

      assertThat(raft.calls("stepDown")).hasSize(1);
      assertThat(server.calls("stop")).isEmpty();
    }

    // Regression for #7134: an ordinary step down failure is still retried
    /**
     * Control: any OTHER step-down failure is still retried the full three times, because that one can succeed on
     * a later attempt. Without this the test above would also pass against a loop that never retries at all.
     */
    @Test
    void anOrdinaryStepDownFailureIsStillRetried() throws Exception {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(true);
      raft.fails("stepDown", new IllegalStateException("transfer timed out"));

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, false);
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getConfiguration", config);

      recover(databaseWith(raft, server));

      assertThat(raft.calls("stepDown")).hasSize(3);
    }

    // Regression for #7134: step down aborts when leadership moves while candidates are tried
    /**
     * The same shape one level up, in {@code stepDown()}'s own candidate loop. Leadership moving after the entry
     * guard used to be swallowed per candidate: every remaining peer refused identically, the loop fell through to
     * a redundant no-target attempt and a misleading "no other peer available", and stepDown() RETURNED NORMALLY -
     * which the HTTP handler reports as 200 for a step-down that never happened.
     */
    @Test
    void stepDownAbortsWhenLeadershipMovesWhileCandidatesAreTried() {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getServerName", "ArcadeDB_0");

      // isLeader() is true (the entry guard passes) but every transfer refuses, as it does once leadership moved.
      final RaftHAServer raft = new RaftHAServer(server, config) {
        // Every configured peer answers (issue #8556): these tests are about the step-down loop, not reachability.
        @Override
        Set<String> handoffReachablePeers() {
          return getLivePeers().stream().map(peer -> peer.getId().toString()).collect(Collectors.toSet());
        }

        @Override
        public boolean isLeader() {
          return true;
        }

        @Override
        public void transferLeadership(final String targetPeerId, final long timeoutMs) {
          transferAttempts++;
          throw new NotTheLeaderRefusalException("Refusing to transfer leadership to " + targetPeerId,
              RaftPeerId.valueOf(NEW_LEADER));
        }
      };

      assertThatThrownBy(raft::stepDown)
          .as("a step-down that transferred nothing must not return normally")
          .isInstanceOf(NotTheLeaderRefusalException.class)
          .hasMessageContaining(NEW_LEADER);
      assertThat(transferAttempts).as("the remaining candidates all refuse identically; do not try them").isEqualTo(1);
    }

    private int transferAttempts;

    // Regression for #7134: a non leader never attempts A step down
    /** And a node that is not the leader when recovery starts never attempts a step-down at all. */
    @Test
    void aNonLeaderNeverAttemptsAStepDown() throws Exception {
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(false);

      final FakeArcadeDBServer server = FakeArcadeDBServer.create();

      recover(databaseWith(raft, server));

      assertThat(raft.calls("stepDown")).isEmpty();
    }

  }
}
