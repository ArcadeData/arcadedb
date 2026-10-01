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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
 * {@code Issue8454SnapshotSourceAppliedIndexTest} exercises the install lock: an empty-payload {@code TX_ENTRY}
 * fails {@code applyTxEntry} on its first read and quarantines the database, which - deliberately - is what tells
 * an entry ABOVE the boundary apart from one that was skipped: a skipped entry always completes normally with "OK"
 * (this test's proof that nothing was applied and no exception was thrown), while an entry that actually reached
 * the apply path with this malformed payload fails.
 */
class Issue8577StaleEntryAfterInstallBoundaryTest {

  @AfterEach
  void clearSeam() {
    ArcadeStateMachine.applyWaitsForInstallForTesting = null;
  }

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
    // Issue8454SnapshotSourceAppliedIndexTest and ArcadeStateMachinePerDatabaseHaltTest).
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, new byte[0], Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }
}
