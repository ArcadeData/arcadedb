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
import com.arcadedb.schema.LocalSchema;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7298, the sibling of #7221.
 * <p>
 * #7221 fixed the {@code forceSnapshot} arm of {@code applyInstallDatabaseEntry}, which returned on a persisted
 * applied index without asking whether the database was still on disk.
 * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry} had the identical guard 150 lines away in the same
 * class, with its {@code existsDatabase} check sitting one line PAST the early return that skipped it.
 * <p>
 * The input is the same and so is the consequence. {@code .raft/applied-index} is a SIBLING of the per-database
 * directories, so the wipe-and-resync recovery this repo's runbook prescribes - stop the follower, delete the bad
 * copy, start it again - leaves the entry behind while the database it describes is gone. On replay the guard
 * fired, logged that verification had completed, and returned. Nothing else brought the database back either: a
 * bootstrap-baselined database predates the cluster, so there is no {@code INSTALL_DATABASE_ENTRY} in the log for
 * the "late joiner" arm below the guard to be waiting for, and {@code triggerSnapshotDownload} only reinstalls
 * the databases the server already has REGISTERED - which is precisely the one it does not have.
 * <p>
 * The premise is now proved before the skip, and the not-registered case reinstalls from the leader, which is the
 * action #7221's own fix takes in the same situation on the install path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7298BootstrapReplaySkipMissingDatabaseTest {

  private static final String DB_NAME     = "wiped-and-resynced";
  private static final long   ENTRY_INDEX = 50L;

  @TempDir
  private Path serverDir;

  /**
   * {@code SnapshotInstaller.install} asks the server for its backup coordinator before it does anything else, so
   * that call is the proof the install was actually reached - independent of the log lines, which is what the old
   * guard got wrong in the first place.
   */
  private ArcadeDBServer mockServerWithDatabaseRegistered(final boolean registered) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    // 0 retries with no backoff: the leader is unknown in this unit test, so the install fails immediately
    // instead of sleeping through an exponential-backoff budget.
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 0L);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.existsDatabase(DB_NAME)).thenReturn(registered);
    return server;
  }

  /**
   * Every state machine this class builds, closed in {@link #closeStateMachines()}.
   * <p>
   * A state machine owns two executors, and an install that fails - which is what half of these tests drive -
   * leaves work queued on one of them. Left running, those threads outlive the test method and race JUnit's
   * {@code @TempDir} cleanup for {@code serverDir/.raft}, which surfaces as a {@code DirectoryNotEmptyException}
   * out of the extension rather than as any test failing: the CI run of PR #7941 hit exactly that with all six
   * tests green. Closing them is what makes the directory quiet before JUnit deletes it.
   */
  private final List<ArcadeStateMachine> stateMachines = new ArrayList<>();

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

  private ArcadeStateMachine stateMachineOn(final ArcadeDBServer server) {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    stateMachines.add(sm);
    return sm;
  }

  private static RaftLogEntryCodec.DecodedEntry bootstrapEntry() throws Exception {
    final ByteString encoded = RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB_NAME, "0".repeat(64), 7L);
    return RaftLogEntryCodec.decode(encoded);
  }

  /**
   * The defect. The applied index says a previous session applied this very entry, the database is not registered
   * now, and before the fix the guard returned anyway - claiming a completed verification for a database that is
   * not there, and leaving the node running permanently short of it.
   */
  @Test
  void anAppliedEntryWhoseDatabaseIsGoneReinstallsItInsteadOfSkipping() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    // The install fails (no leader is reachable from a unit test) and that failure must stay contained: this runs
    // on the Raft StateMachineUpdater thread, where an escaping exception trips the critical-error halt.
    assertThatNoException().isThrownBy(() -> sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX));

    verify(server, atLeastOnce()).getBackupCoordinator();
    // The failed install hands off to a retry on the lifecycle executor, which persists the unreconciled mark under
    // .raft before it releases its holder. Waiting for the release keeps that write from racing the @TempDir
    // cleanup, which surfaced as "Failed to close extension context" with the test itself green.
    await().atMost(Duration.ofSeconds(30)).until(() -> sm.getBootstrapInstallsInFlight().isEmpty());
  }

  /**
   * The legitimate replay-skip is preserved: with the database registered, the same input still returns without
   * touching the install machinery. This is what #4824's own test asserts, restated against the install path so a
   * fix for #7298 that simply deleted the guard fails here.
   */
  @Test
  void anAppliedEntryWhoseDatabaseIsStillHereStillSkips() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(true);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    verify(server, never()).getBackupCoordinator();
    // The baseline is recorded before the skip either way, as it always was.
    assertThat(sm.getBootstrapBaseline(DB_NAME)).isNotNull();
  }

  /**
   * The failed reinstall must not be a log line and nothing else. {@code applyTransaction} persists this
   * database's applied index whatever happens in this method, so the entry reads as applied while the database is
   * absent, and the replay that would retry it is not guaranteed to survive the next Ratis snapshot. The mark is
   * what outlives that: it is persisted with the baselines, published by {@code ClusterAlerts}, and re-verified
   * on the health tick.
   * <p>
   * Awaited rather than asserted outright: the mark is written by the one-shot retry, which runs on the
   * lifecycleExecutor so that a failing install never blocks the Raft apply thread.
   */
  @Test
  void aFailedReinstallIsRecordedDurablyRatherThanOnlyLogged() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.writePersistedAppliedIndex(ENTRY_INDEX, DB_NAME);

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(sm.getBootstrapUnreconciledDatabases())
            .as("a database this node had, lost, and could not pull back must be visible in the cluster status")
            .contains(DB_NAME));
  }

  /**
   * The bounded retry: the periodic bootstrap-divergence check reinstalls a marked database whose files are gone.
   * Its absent-database arm used to {@code continue} unconditionally, which was right for the #6124 case it was
   * written for - a database that is merely CLOSED still has files worth protecting - and wrong for this one,
   * where there is nothing local to protect and nothing else retries.
   * <p>
   * The mark is set directly rather than by replaying an entry: this test is about what the periodic check does
   * with a marked-and-missing database, and going through the apply path would race its asynchronous retry
   * against the synchronous one being measured.
   */
  @Test
  void thePeriodicCheckRetriesTheInstallWhenTheDirectoryIsGone() {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.markBootstrapUnreconciled(DB_NAME);

    sm.reconcileBootstrapDivergence(Map.of(DB_NAME, new ArcadeStateMachine.BootstrapBaseline("0".repeat(64), 7L)));

    verify(server, atLeastOnce()).getBackupCoordinator();
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("the install failed again, so the mark stays for the next tick")
        .contains(DB_NAME);
  }

  /**
   * The other half of that rule: a marked database whose DIRECTORY is still on disk is only closed, and the check
   * must leave it exactly as it was rather than reinstall over an operator's copy.
   */
  @Test
  void thePeriodicCheckLeavesAClosedButPresentDatabaseAlone() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    sm.markBootstrapUnreconciled(DB_NAME);
    // A closed database, files and all: an EMPTY directory is what a failed install leaves behind, and holds no
    // copy of anything (issue #8045).
    Files.writeString(Files.createDirectories(serverDir.resolve(DB_NAME)).resolve(LocalSchema.SCHEMA_FILE_NAME), "{}");

    sm.reconcileBootstrapDivergence(Map.of(DB_NAME, new ArcadeStateMachine.BootstrapBaseline("0".repeat(64), 7L)));

    verify(server, never()).getBackupCoordinator();
    assertThat(sm.getBootstrapUnreconciledDatabases())
        .as("a closed database keeps its mark: absence from the registry is not evidence of convergence")
        .contains(DB_NAME);
  }

  /**
   * A genuine late joiner - no local copy and NO evidence it ever had one - must keep waiting for the follow-on
   * install entry rather than pulling a snapshot on its own. That is the first-formation path, and turning it
   * into an install would make every peer of a forming cluster race the leader for a database that is about to
   * be shipped to it anyway.
   */
  @Test
  void aGenuineLateJoinerStillWaitsForTheFollowOnInstallEntry() throws Exception {
    final ArcadeDBServer server = mockServerWithDatabaseRegistered(false);
    final ArcadeStateMachine sm = stateMachineOn(server);
    // No persisted applied index for this database at all: readPersistedAppliedIndex(dbName) answers -1.

    sm.applyBootstrapFingerprintEntry(bootstrapEntry(), ENTRY_INDEX);

    verify(server, never()).getBackupCoordinator();
    assertThat(sm.getBootstrapBaseline(DB_NAME)).isNotNull();
  }
}
