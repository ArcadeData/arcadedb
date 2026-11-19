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
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Stale entries replayed from this node's own Raft log after a snapshot install registered a boundary: skipped as no-ops (per-database gate boundary and Raft-wide boundary), driven against a bare {@link ArcadeStateMachine}.
 */
class StaleEntryAfterInstallBoundaryTest {

  // ------------------------------------------------------------------------------------------
  // Issue #8577
  // ------------------------------------------------------------------------------------------
  /*
   * Regression tests for issue #8577: a follower re-applying its own local Raft log after a restart, while a
   * leader-driven snapshot install (issue #8449's notify-install path) lands mid-replay, used to keep applying the
   * stale entries once the install's per-database {@link ArcadeStateMachine.InstallApplyGate} released - onto the
   * copy the install had just put in place. The first such entry whose {@code updateLastAppliedTermIndex} call
   * regressed relative to the position the install had already registered tripped Ratis's monotonic check and halted
   * the node with {@code Failed updateLastAppliedTermIndex}.
   * <p>
   * The fix records the install's boundary index on the database's gate BEFORE releasing it
   * ({@code ArcadeStateMachine.runUnderInstallGate}), and {@code applyTransaction} skips any entry at or below it as
   * a no-op - no apply, no {@code lastAppliedIndex}/Ratis/persisted-index change - instead of re-applying it.
   * <p>
   * Driven directly against a bare {@link ArcadeStateMachine} (no server, no cluster), the same way
   * {@code SnapshotSourceAppliedIndexTest} exercises the install lock: an empty-payload {@code TX_ENTRY}
   * fails {@code applyTxEntry} on its first read and quarantines the database, which - deliberately - is what tells
   * an entry ABOVE the boundary apart from one that was skipped: a skipped entry always completes normally with "OK"
   * (this test's proof that nothing was applied and no exception was thrown), while an entry that actually reached
   * the apply path with this malformed payload fails.
   */

  @AfterEach
  void clearSeam() {
    ArcadeStateMachine.applyWaitsForInstallForTesting = null;
  }

  // Regression for #8577: an entry at or below the installed boundary is skipped as ano op
  @Test
  void anEntryAtOrBelowTheInstalledBoundaryIsSkippedAsANoOp() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // A leader-driven install brings database 'chaos's copy up to index 625716 (a safe lower bound - see
    // ArcadeStateMachine.installSnapshotFromLeader's computedSnapshotIndex, issue #8577), recorded on the gate
    // before it releases (ArcadeStateMachine.runUnderInstallGate).
    sm.runUnderInstallGate("chaos", 625716L, () -> {
      // The download + swap; nothing to do in this server-less harness.
    });

    final long appliedBeforeReplay = sm.readAppliedIndexCounter();

    // Stale local-log entries the apply thread is replaying below the install boundary after a restart (the
    // exact #8577 shape): skipped as a no-op rather than re-applied to the copy the install just put in place.
    final CompletableFuture<Message> belowBoundary = sm.applyTransaction(txEntryForDatabase(sm, "chaos", 12L, 601958L));
    assertThat(belowBoundary.isCompletedExceptionally())
        .as("an entry below the install boundary must not throw - no CRITICAL halt").isFalse();
    assertThat(belowBoundary.get().getContent().toStringUtf8()).isEqualTo("OK");
    assertThat(sm.readAppliedIndexCounter())
        .as("a skipped entry must not move the applied position").isEqualTo(appliedBeforeReplay);

    // An entry AT the boundary is covered too.
    final CompletableFuture<Message> atBoundary = sm.applyTransaction(txEntryForDatabase(sm, "chaos", 12L, 625716L));
    assertThat(atBoundary.isCompletedExceptionally()).isFalse();
    assertThat(sm.readAppliedIndexCounter())
        .as("the last applied position stays at the install boundary").isEqualTo(appliedBeforeReplay);

    // The next entry ABOVE the boundary is not skipped: it reaches the normal apply path. This harness's
    // empty payload fails applyTxEntry and quarantines the database instead of silently succeeding - proof the
    // entry was actually attempted, not treated as already covered.
    final CompletableFuture<Message> aboveBoundary = sm.applyTransaction(txEntryForDatabase(sm, "chaos", 13L, 625717L));
    assertThat(aboveBoundary.isCompletedExceptionally())
        .as("an entry above the boundary must go through the normal apply path, not be skipped").isTrue();
  }

  // Regression for #8577: an entry already waiting on the gate when the install finishes is skipped
  @Test
  @Timeout(60)
  void anEntryAlreadyWaitingOnTheGateWhenTheInstallFinishesIsSkipped() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final long appliedBeforeReplay = sm.readAppliedIndexCounter();

    final CountDownLatch installHoldsGate = new CountDownLatch(1);
    final CountDownLatch releaseInstall = new CountDownLatch(1);
    final CountDownLatch applyParked = new CountDownLatch(1);
    ArcadeStateMachine.applyWaitsForInstallForTesting = name -> {
      if ("chaos".equals(name))
        applyParked.countDown();
    };

    final AtomicReference<Throwable> installFailure = new AtomicReference<>();
    final Thread installer = new Thread(() -> {
      try {
        sm.runUnderInstallGate("chaos", 625716L, () -> {
          installHoldsGate.countDown();
          try {
            releaseInstall.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      } catch (final Throwable t) {
        installFailure.set(t);
      }
    }, "issue8577-install");
    installer.start();
    assertThat(installHoldsGate.await(30, TimeUnit.SECONDS)).isTrue();

    // The #8577 interleaving: the apply thread reaches a stale replayed entry while the install holds the gate, and
    // waits on it; the install then finishes and releases it.
    final AtomicReference<CompletableFuture<Message>> result = new AtomicReference<>();
    final Thread applier = new Thread(() -> result.set(sm.applyTransaction(txEntryForDatabase(sm, "chaos", 12L, 601958L))),
        "issue8577-apply");
    applier.start();
    assertThat(applyParked.await(30, TimeUnit.SECONDS)).as("the stale entry must be parked on the install's gate").isTrue();

    releaseInstall.countDown();
    installer.join(30_000);
    applier.join(30_000);

    assertThat(installFailure.get()).isNull();
    assertThat(result.get().isCompletedExceptionally())
        .as("the parked stale entry must be skipped once the install releases the gate, not applied").isFalse();
    assertThat(result.get().get().getContent().toStringUtf8()).isEqualTo("OK");
    assertThat(sm.readAppliedIndexCounter()).isEqualTo(appliedBeforeReplay);
  }

  // Regression for #8577: a targeted resync records no boundary so waiting entries still apply normally
  @Test
  void aTargetedResyncRecordsNoBoundarySoWaitingEntriesStillApplyNormally() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    // installLeaderCopy's targeted/operator resync (issue #8490 and friends) is not tied to a Raft install
    // boundary: its waiting entries are assumed newer than what gets installed, exactly as before #8577.
    sm.runUnderInstallGate("chaos", () -> {
      // The download + swap; nothing to do in this server-less harness.
    });

    final CompletableFuture<Message> entry = sm.applyTransaction(txEntryForDatabase(sm, "chaos", 1L, 1L));
    assertThat(entry.isCompletedExceptionally())
        .as("with no recorded boundary, the entry reaches the normal apply path and this malformed payload fails")
        .isTrue();
  }

  // Regression for #8577: install apply gate records the highest installed index and never regresses
  @Test
  void installApplyGateRecordsTheHighestInstalledIndexAndNeverRegresses() {
    final ArcadeStateMachine.InstallApplyGate gate = new ArcadeStateMachine.InstallApplyGate();
    assertThat(gate.installedIndex()).as("no install has recorded a boundary yet").isEqualTo(-1L);

    gate.recordInstalled(50L);
    assertThat(gate.installedIndex()).isEqualTo(50L);

    gate.recordInstalled(30L);
    assertThat(gate.installedIndex()).as("a lower value must not regress the recorded boundary").isEqualTo(50L);

    gate.recordInstalled(80L);
    assertThat(gate.installedIndex()).isEqualTo(80L);
  }

  private static TransactionContext txEntryForDatabase(final ArcadeStateMachine sm, final String databaseName,
      final long term, final long index) {
    // An empty payload fails applyTxEntry on its first read, without needing a server (see
    // SnapshotSourceAppliedIndexTest and ArcadeStateMachinePerDatabaseHaltTest).
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, new byte[0], Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  // ------------------------------------------------------------------------------------------
  // Issue #8651
  // ------------------------------------------------------------------------------------------
  /*
   * Regression tests for issue #8651: the #8577 skip only covered entries that name a database, so a stale replayed
   * entry that Ratis applies through {@code notifyTermIndexUpdated} (its own metadata and configuration entries), or
   * one that names no database (the security entries), still regressed the applied position below the boundary a
   * leader-driven install had registered. The Ratis monotonic check then killed the {@code StateMachineUpdater}
   * thread and left the node a zombie. Driven against a bare {@link ArcadeStateMachine}, like
   * {@link StaleEntryAfterInstallBoundaryTest}.
   */

  // Regression for #8651: a stale metadata entry below the install boundary leaves the applied position alone
  @Test
  void aStaleMetadataEntryBelowTheInstallBoundaryLeavesTheAppliedPositionAlone() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.recordInstalledRaftBoundary(1867073L);
    sm.notifyTermIndexUpdated(30L, 1867073L);

    // The exact shape from the chaos run: Ratis applies its own metadata entry from the pre-install local log.
    sm.notifyTermIndexUpdated(30L, 1835784L);

    assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(30L, 1867073L));
  }

  // Regression for #8651: an entry above the install boundary still advances the applied position
  @Test
  void anEntryAboveTheInstallBoundaryStillAdvancesTheAppliedPosition() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.recordInstalledRaftBoundary(1867073L);
    sm.notifyTermIndexUpdated(30L, 1867073L);

    sm.notifyTermIndexUpdated(30L, 1867074L);

    assertThat(sm.getLastAppliedTermIndex()).isEqualTo(TermIndex.valueOf(30L, 1867074L));
  }

  // Regression for #8651: a regression with no recorded boundary still fails loudly
  @Test
  void aRegressionWithNoRecordedBoundaryStillFailsLoudly() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.notifyTermIndexUpdated(30L, 1867073L);

    assertThatThrownBy(() -> sm.notifyTermIndexUpdated(30L, 1835784L))
        .isInstanceOf(IllegalStateException.class);
  }

  // Regression for #8651: a stale entry that names no database is skipped as ano op
  @ParameterizedTest
  @EnumSource(value = RaftLogEntryType.class, names = { "SECURITY_USERS_ENTRY", "SECURITY_GROUPS_ENTRY",
      "SECURITY_API_TOKENS_ENTRY" })
  void aStaleEntryThatNamesNoDatabaseIsSkippedAsANoOp(final RaftLogEntryType type) throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.recordInstalledRaftBoundary(1867073L);
    sm.notifyTermIndexUpdated(30L, 1867073L);
    final long appliedBefore = sm.readAppliedIndexCounter();
    final long persistedBefore = sm.readPersistedAppliedIndex();

    final CompletableFuture<Message> result = sm.applyTransaction(entry(sm, securityPayload(type), 30L, 1835784L));

    assertThat(result.isCompletedExceptionally()).as("a stale security entry must not throw or halt").isFalse();
    assertThat(result.get().getContent().toStringUtf8()).isEqualTo("OK");
    assertThat(sm.readAppliedIndexCounter()).as("nothing moves the applied counter backward").isEqualTo(appliedBefore);
    assertThat(sm.getLastAppliedTermIndex()).as("nor the Ratis applied position")
        .isEqualTo(TermIndex.valueOf(30L, 1867073L));
    assertThat(sm.readPersistedAppliedIndex()).as("nor the persisted global position").isEqualTo(persistedBefore);
  }

  // Regression for #8651: a stale entry for adatabase the install did not cover leaves the global positions alone
  /**
   * A database the install did not cover (not installed, missing on the leader, created since) has no boundary on its
   * gate, so its stale entry is applied to it - but the global positions belong to the install and must not move
   * backward; only that database's own applied position is recorded.
   */
  @Test
  void aStaleEntryForADatabaseTheInstallDidNotCoverLeavesTheGlobalPositionsAlone(@TempDir final Path serverDir)
      throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, serverDir.toString());
    final ArcadeDBServer server = new ArcadeDBServer(config);
    final String dbPath = serverDir.resolve("uncovered").toString();
    final LocalDatabase db = (LocalDatabase) new DatabaseFactory(dbPath).create();
    try {
      server.registerDatabase("uncovered", db);
      final ArcadeStateMachine sm = new ArcadeStateMachine();
      sm.setServer(server);
      final ByteString bootstrap = RaftLogEntryCodec.encodeBootstrapFingerprintEntry("uncovered",
          BootstrapFingerprint.compute(new File(dbPath)), db.getLastTransactionId());

      // The node applied up to 120, then an install registered 100 without touching 'uncovered'.
      assertThat(sm.applyTransaction(entry(sm, bootstrap, 30L, 120L)).isCompletedExceptionally()).isFalse();
      sm.recordInstalledRaftBoundary(100L);

      // A replayed entry for that database, below the boundary.
      final CompletableFuture<Message> stale = sm.applyTransaction(entry(sm, bootstrap, 30L, 50L));

      assertThat(stale.isCompletedExceptionally()).isFalse();
      assertThat(sm.readAppliedIndexCounter()).as("the global counter must not move backward").isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex()).as("the persisted global position must not move backward")
          .isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex("uncovered")).as("the database's own position is recorded")
          .isEqualTo(50L);

      // A stale DROP below the boundary, through applyTransaction: it evicts that database's own entry and still
      // leaves the global positions alone.
      sm.writePersistedDatabaseAppliedIndex(40L, "gone", false);
      final CompletableFuture<Message> drop = sm.applyTransaction(
          entry(sm, RaftLogEntryCodec.encodeDropDatabaseEntry("gone"), 30L, 60L));
      assertThat(drop.isCompletedExceptionally()).isFalse();
      assertThat(sm.readAppliedIndexCounter()).isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex()).isEqualTo(120L);
      assertThat(sm.readPersistedAppliedIndex("gone")).as("evicted by the drop").isEqualTo(-1L);
    } finally {
      db.close();
    }
  }

  // Regression for #8651: the boundary covers the installed index and everything below it
  @Test
  void theBoundaryCoversTheInstalledIndexAndEverythingBelowIt() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.isBelowInstalledRaftBoundary(1L)).as("no install recorded, nothing is covered").isFalse();

    sm.recordInstalledRaftBoundary(100L);
    sm.recordInstalledRaftBoundary(50L);

    assertThat(sm.isBelowInstalledRaftBoundary(99L)).isTrue();
    assertThat(sm.isBelowInstalledRaftBoundary(100L)).as("the boundary index itself is covered").isTrue();
    assertThat(sm.isBelowInstalledRaftBoundary(101L)).isFalse();
  }

  // Regression for #8651: a closing division overrides the running proxy state
  @Test
  void aClosingDivisionOverridesTheRunningProxyState() {
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.CLOSING))
        .as("a division its dying updater closed must not read as RUNNING").isTrue();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.CLOSED)).isTrue();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.EXCEPTION)).isTrue();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.RUNNING)).isFalse();
    assertThat(RaftHAServer.isDivisionStateReported(LifeCycle.State.STARTING)).isFalse();
  }

  // Regression for #8651: a closing division is reported by the health monitor only when it persists
  @Test
  void aClosingDivisionIsReportedByTheHealthMonitorOnlyWhenItPersists() {
    final HealthMonitorTest.FakeHealthTarget fake = new HealthMonitorTest.FakeHealthTarget();
    final HealthMonitor monitor = new HealthMonitor(fake, 0);

    fake.state.set(LifeCycle.State.CLOSING);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("a division that is closing right now may be a normal shutdown").isZero();

    fake.state.set(LifeCycle.State.RUNNING);
    monitor.tick();
    fake.state.set(LifeCycle.State.CLOSING);
    monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("a healthy tick in between resets the streak").isZero();

    monitor.tick();
    assertThat(fake.recoveryCalls.get()).as("a division stuck closing across ticks is recovered like a CLOSED one")
        .isEqualTo(1);
  }

  private static ByteString securityPayload(final RaftLogEntryType type) {
    return switch (type) {
      case SECURITY_USERS_ENTRY -> RaftLogEntryCodec.encodeSecurityUsersEntry("{}");
      case SECURITY_GROUPS_ENTRY -> RaftLogEntryCodec.encodeSecurityGroupsEntry("{}");
      case SECURITY_API_TOKENS_ENTRY -> RaftLogEntryCodec.encodeSecurityApiTokensEntry("{}");
      default -> throw new IllegalArgumentException(type.name());
    };
  }

  private static TransactionContext entry(final ArcadeStateMachine sm, final ByteString payload, final long term,
      final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }
}
