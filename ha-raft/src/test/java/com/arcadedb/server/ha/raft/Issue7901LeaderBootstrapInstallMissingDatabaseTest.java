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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7901: {@code installFromLeaderForBootstrap}'s leader short-circuit was written for
 * one caller and inherited by another with the opposite premise.
 * <p>
 * The original caller is the fingerprint-mismatch arm, where this node holds a copy that differs from the cluster
 * baseline. There "the leader is the bootstrap source, so it already holds the chosen baseline" is true by
 * construction and returning is right. Issue #7298 gave the method a second caller - the replay-skip arm, when the
 * database the skip is about is not on this node any more - for which it is false in the only way that matters:
 * the database is absent, and this node being the Raft leader does not make it appear.
 * <p>
 * Because the method returned NORMALLY there, at TRACE level, the whole of #7298's recovery was skipped: no retry
 * was scheduled, {@code markBootstrapUnreconciled} was never reached (it lives in the catch), and the node ran on
 * as a cluster member permanently short of a database the cluster believes it has, with one TRACE line as the
 * entire record. The wide window is not the synchronous apply - the comments there say leader discovery has not
 * happened yet - but the asynchronous retry, deliberately deferred until a leader is reachable, by which time a
 * restarting node with election priority has ordinarily become the leader itself.
 * <p>
 * The fix asserts the premise instead of assuming it. "Present" means registered OR on disk, which is what keeps
 * the leader whose copy is merely CLOSED on the fast path: it is still the source every peer installs from, and
 * throwing for it would mark it unreconciled and pull a download over perfectly good files.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7901LeaderBootstrapInstallMissingDatabaseTest {

  private static final String DB_NAME     = "gone-on-the-leader";
  private static final long   ENTRY_INDEX = 50L;

  @TempDir
  private Path serverDir;

  private final List<ArcadeStateMachine> stateMachines = new ArrayList<>();

  /**
   * A state machine owns two executors and these tests drive installs that fail, leaving work queued on one of
   * them. Left running, those threads outlive the test method and race JUnit's {@code @TempDir} cleanup for
   * {@code serverDir/.raft} - see the same teardown in {@code Issue7298BootstrapReplaySkipMissingDatabaseTest}.
   */
  @AfterEach
  void closeStateMachines() {
    for (final ArcadeStateMachine sm : stateMachines)
      try {
        sm.close();
      } catch (final IOException e) {
        // Teardown of a unit-test fixture: a close that fails must not replace the test's own verdict.
      }
    stateMachines.clear();
  }

  private ArcadeDBServer mockServerWithDatabaseRegistered(final boolean registered) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    // 0 retries with no backoff: no leader is reachable from a unit test, so an install that IS attempted fails
    // at once instead of sleeping through an exponential-backoff budget.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(DB_NAME)).thenReturn(registered);
    return server;
  }

  /**
   * A state machine that reports this node as the Raft leader - the configuration none of the six #7298 tests
   * drives, which is why the suite pinned the follower half of that fix and could not observe the leader half.
   */
  private ArcadeStateMachine leaderStateMachineOn(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    sm.setRaftHAServer(raft);
    stateMachines.add(sm);
    return sm;
  }

  private static RaftLogEntryCodec.DecodedEntry bootstrapEntry() throws Exception {
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), 7L);
    return RaftLogEntryCodec.decode(encoded);
  }

  /**
   * The defect. The applied index says a previous session applied this very entry, the database is gone, and this
   * node is the leader. Before the fix the install returned normally, so nothing was recorded anywhere a machine
   * could read - which is exactly what #7298's commit message ("a missing database that could not be pulled back
   * must survive as state, not as a log line") was written to prevent.
   * <p>
   * Awaited because the mark is written by the one-shot retry on the lifecycleExecutor, so that a failing install
   * never blocks the Raft apply thread.
   */
  @Test
  void aMissingDatabaseIsRecordedEvenWhenThisNodeIsTheLeader() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = leaderStateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    // The failure must stay contained: this runs on the Raft StateMachineUpdater thread, where an escaping
    // exception trips the critical-error halt - the outcome installFromLeaderForBootstrapWithRetry exists to
    // prevent.
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX));

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(sm.getBootstrapUnreconciledDatabases())
            .as("a leader missing a database the cluster has must record it, not log one TRACE line")
            .contains(DB_NAME));
  }

  /**
   * And it is reported as MISSING rather than as a kept copy, which is the #7902 half of the same story: the node
   * has nothing to keep.
   */
  @Test
  void theLeadersMarkIsClassifiedAsMissingRatherThanAsAKeptCopy() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = leaderStateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      final ArcadeStateMachine.BootstrapUnreconciled unreconciled = sm.getBootstrapUnreconciled(null);
      assertThat(unreconciled.missingLocally()).containsExactly(DB_NAME);
      assertThat(unreconciled.keptLocalCopy()).isEmpty();
    });
  }

  /**
   * The counter-case that keeps the assertion above from being "the leader always throws". A leader whose copy is
   * merely CLOSED - not registered, but its directory is right there - is still the authoritative source every
   * peer installs from. It must stay on the short-circuit: no install, no mark, no download over files that are
   * perfectly good.
   * <p>
   * This is also why the guard tests the directory and not {@code existsDatabase} alone, which the issue's own
   * suggested fix did: with that predicate this case would throw.
   */
  @Test
  void aLeaderWhoseCopyIsMerelyClosedStaysOnTheShortCircuit() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = leaderStateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);
    Files.createDirectories(serverDir.resolve(DB_NAME));

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    // getBackupCoordinator is SnapshotInstaller.install's first call, so it is the proof the install was reached -
    // independent of the log lines, which is what the original guard got wrong in the first place.
    verify(server, never()).getBackupCoordinator();
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("a leader holding the copy has nothing to reconcile")
        .doesNotContain(DB_NAME);
  }

  /**
   * A state machine with no server wired must not read as a node that is missing every database.
   * <p>
   * Both call sites of the presence check originally spelled it out with a leading {@code server != null}, so an
   * unwired state machine fell through to the previous behaviour: the leader short-circuit returned, and the
   * marked set reported its databases as KEPT. Folding the two into one {@code isDatabasePresentLocally} helper
   * inverted that null case - the helper answered false, which reads as "missing" - and nothing caught it,
   * because no other test here leaves the server unset (review on PR #7953).
   * <p>
   * "Present" is the conservative answer when this node cannot be asked: it is the one that leaves local files
   * alone and recommends nothing, which is the same doctrine {@code databaseDirectoryExists} follows for a path
   * it cannot resolve.
   */
  @Test
  void aStateMachineWithNoServerReportsNothingMissing() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    stateMachines.add(sm);
    sm.markBootstrapUnreconciled(DB_NAME);

    final ArcadeStateMachine.BootstrapUnreconciled unreconciled = sm.getBootstrapUnreconciled(null);

    assertThat(unreconciled.missingLocally())
        .as("a node that cannot be asked has not been shown to be missing anything")
        .isEmpty();
    assertThat(unreconciled.missingCount()).isZero();
    assertThat(unreconciled.keptLocalCopy())
        .as("the mark is still real, it is just not a 'missing database' one")
        .containsExactly(DB_NAME);
  }

  /**
   * The periodic check reaches the same guard by a different road, and must reach the same verdict. A marked
   * database whose directory is gone is retried by {@code reconcileBootstrapDivergence}; on a leader that retry
   * can only fail, and the mark has to survive so the alert stays raised until leadership moves and the follower
   * path can install it.
   * <p>
   * Note this arm is reachable on a leader even though {@code verifyBootstrapDivergence()} returns early on one:
   * the mark is durable, so a node that marks the database while leading and is still leading on the next tick
   * keeps it, and a node that marks it as a follower and then wins an election reaches this through the retry
   * that was already queued.
   */
  @Test
  void thePeriodicRetryOnTheLeaderKeepsTheMarkInsteadOfClearingIt() {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = leaderStateMachineOn(server);
    sm.markBootstrapUnreconciled(DB_NAME);

    assertThatNoException().isThrownBy(() -> sm.reconcileBootstrapDivergence(
        java.util.Map.of(DB_NAME, new ArcadeStateMachine.BootstrapBaseline("0".repeat(64), 7L))));

    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("a leader cannot install a database from itself, so the mark must outlive the attempt")
        .contains(DB_NAME);
    assertThat(sm.getBootstrapUnreconciled(null).missingLocally()).containsExactly(DB_NAME);
  }
}
