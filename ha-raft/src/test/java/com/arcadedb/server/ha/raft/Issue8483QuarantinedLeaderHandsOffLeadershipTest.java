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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8483: a leader that quarantined one of its own databases kept leadership, and nothing
 * could heal it. The resync a quarantine triggers is refused on the leader role (it cannot pull from itself), and
 * since #8468 the leader also refuses to serve the quarantined database to its followers, so until an operator moved
 * the leadership by hand the database could be installed nowhere.
 * <p>
 * A leader now hands leadership to a healthy peer: at once from the apply paths that raise the quarantine, and from
 * the health tick for a quarantine or read floor nothing raised in this term (restored from disk, left by an
 * incomplete install).
 */
class Issue8483QuarantinedLeaderHandsOffLeadershipTest {

  // ---------------------------------------------------------------------------------------------
  // The apply paths that raise a quarantine hand leadership off at once
  // ---------------------------------------------------------------------------------------------

  @Test
  void anApplyErrorThatQuarantinesADatabaseOnTheLeaderHandsLeadershipOff() {
    final RaftHAServer raft = raftMock(true);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);

    // An entry this node cannot decode: handleUnexpectedApplyError quarantines db-A (see Issue7495 test).
    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L));

    assertThatThrownBy(future::join).hasMessageContaining("per-database snapshot resync in progress");
    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    verify(raft, times(1)).handOffLeadershipToResync(contains("'db-A'"));
  }

  @Test
  void aSecondErrorOnTheSameQuarantineDoesNotAskAgain() {
    final RaftHAServer raft = raftMock(true);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);

    assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L)).join());
    assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 6L)).join());

    verify(raft, times(1)).handOffLeadershipToResync(anyString());
  }

  @Test
  void anApplyErrorOnAFollowerDoesNotTouchTheLeadership() {
    final RaftHAServer raft = raftMock(false);
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);

    assertThatThrownBy(() -> sm.applyTransaction(txEntry(sm, "db-A", new byte[0], 5L)).join());

    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    verify(raft, never()).handOffLeadershipToResync(anyString());
  }

  @Test
  void aWalVersionGapOnTheLeaderHandsLeadershipOff() {
    final RaftHAServer raft = raftMock(true);
    final TransactionManager txManager = mock(TransactionManager.class);
    when(txManager.applyChanges(any(), anyMap(), anyBoolean())).thenThrow(new WALVersionGapException("gap"));
    final DatabaseInternal db = mock(DatabaseInternal.class);
    when(db.getTransactionManager()).thenReturn(txManager);
    final ArcadeStateMachine sm = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }
    };
    sm.setRaftHAServer(raft);

    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", emptyWalTransaction(), 5L));

    assertThatThrownBy(future::join);
    assertThat(sm.quarantineCause("db-A")).isEqualTo(DivergenceCause.WAL_VERSION_GAP);
    verify(raft, times(1)).handOffLeadershipToResync(contains("'db-A'"));
  }

  // ---------------------------------------------------------------------------------------------
  // The health tick is the backstop for a quarantine nothing raised in this term
  // ---------------------------------------------------------------------------------------------

  @Test
  void theHealthTickHandsLeadershipOffForAQuarantineTheLeaderAlreadyHeld(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = raftMock(true);
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      // What a quarantine restored from disk looks like: recorded, with no apply path raising it in this JVM.
      sm.markStateDiverged("db-A", DivergenceCause.UNDECODABLE_LOG_ENTRY);
      verify(raft, never()).handOffLeadershipToResync(anyString());

      sm.retryUnfilledSnapshotGap();

      verify(raft, times(1)).handOffLeadershipToResync(contains("db-A"));
    } finally {
      sm.close();
    }
  }

  /**
   * A node-wide read floor is the same state for a leader: reinitialize() publishes it when the snapshot marker runs
   * ahead of what was applied, and only a resync from another node clears it.
   */
  @Test
  void theHealthTickHandsLeadershipOffForAReadFloorTheLeaderHolds(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = raftMock(true);
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      final Field f = ArcadeStateMachine.class.getDeclaredField("staleSnapshotAppliedFloor");
      f.setAccessible(true);
      ((AtomicLong) f.get(sm)).set(42L);

      sm.retryUnfilledSnapshotGap();

      verify(raft, times(1)).handOffLeadershipToResync(contains("read floor at 42"));
    } finally {
      sm.close();
    }
  }

  @Test
  void theHealthTickLeavesAHealthyLeaderAlone(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = raftMock(true);
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      sm.retryUnfilledSnapshotGap();

      verify(raft, never()).handOffLeadershipToResync(anyString());
    } finally {
      sm.close();
    }
  }

  @Test
  void theHealthTickNeverHandsOffFromAFollower(@TempDir final Path tempDir) throws Exception {
    final RaftHAServer raft = raftMock(false);
    final ArcadeStateMachine sm = newStateMachine(tempDir);
    sm.setRaftHAServer(raft);
    try {
      sm.markStateDiverged("db-A", DivergenceCause.APPLY_ERROR);
      sm.retryUnfilledSnapshotGap();

      verify(raft, never()).handOffLeadershipToResync(anyString());
    } finally {
      sm.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // RaftHAServer's gates
  // ---------------------------------------------------------------------------------------------

  @Test
  void aHandoffIsAdmittedOncePerCooldown() {
    final AtomicLong last = new AtomicLong();
    final long now = 1_000_000L;

    assertThat(RaftHAServer.admitQuarantineHandoff(last, now)).isTrue();
    assertThat(RaftHAServer.admitQuarantineHandoff(last, now + 1)).isFalse();
    assertThat(RaftHAServer.admitQuarantineHandoff(last, now + 9 * 60_000L)).isFalse();
    assertThat(RaftHAServer.admitQuarantineHandoff(last, now + 10 * 60_000L)).isTrue();
    assertThat(last.get()).isEqualTo(now + 10 * 60_000L);
  }

  /**
   * The window is taken only when a transfer is about to be attempted (code review on PR #8531): a handoff dropped
   * because no other peer is configured yet, or because this node no longer leads, must not suppress the one that
   * becomes possible when a peer joins moments later.
   */
  @Test
  void aHandoffWithNoPeerOrNoLeadershipLeavesTheWindowIntact() {
    final AtomicLong lastHandoff = new AtomicLong();
    final AtomicLong lastNoPeerReport = new AtomicLong();
    final long t0 = 1_000_000L;

    assertThat(RaftHAServer.decideQuarantineHandoff(false, true, lastHandoff, lastNoPeerReport, t0))
        .isEqualTo(RaftHAServer.QuarantineHandoff.NOT_LEADER);
    assertThat(RaftHAServer.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0))
        .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER_REPORT);
    assertThat(lastHandoff.get()).as("neither drop may take the handoff window").isZero();

    // A peer joins 30 s later: the handoff goes ahead at once.
    assertThat(RaftHAServer.decideQuarantineHandoff(true, true, lastHandoff, lastNoPeerReport, t0 + 30_000L))
        .isEqualTo(RaftHAServer.QuarantineHandoff.TRANSFER);
    // ...and the next tick is inside the window.
    assertThat(RaftHAServer.decideQuarantineHandoff(true, true, lastHandoff, lastNoPeerReport, t0 + 33_000L))
        .isEqualTo(RaftHAServer.QuarantineHandoff.COOLDOWN);
  }

  /** A peer-less leader reports the operator action once per window, not on every health tick. */
  @Test
  void theNoPeerReportIsThrottled() {
    final AtomicLong lastHandoff = new AtomicLong();
    final AtomicLong lastNoPeerReport = new AtomicLong();
    final long t0 = 1_000_000L;

    assertThat(RaftHAServer.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0))
        .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER_REPORT);
    assertThat(RaftHAServer.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0 + 3_000L))
        .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER);
    assertThat(RaftHAServer.decideQuarantineHandoff(true, false, lastHandoff, lastNoPeerReport, t0 + 10 * 60_000L))
        .isEqualTo(RaftHAServer.QuarantineHandoff.NO_PEER_REPORT);
  }

  @Test
  void aSingleNodeClusterHasNoPeerToHandLeadershipTo() {
    final RaftPeerId self = RaftPeerId.valueOf("self");

    assertThat(RaftHAServer.hasHandoffTarget(List.of(peer("self", 0)), self, null)).isFalse();
    assertThat(RaftHAServer.hasHandoffTarget(List.of(), self, null)).isFalse();
    assertThat(RaftHAServer.hasHandoffTarget(List.of(peer("self", 0), peer("other", 0)), self, null)).isTrue();
  }

  /**
   * A peer that is present but not eligible is no target either (code review on PR #8531): handing off to a lagging
   * follower falls back to a Ratis step-down that re-elects this node, an election for nothing.
   */
  @Test
  void aLaggingPeerIsNoHandoffTarget() {
    final RaftPeerId self = RaftPeerId.valueOf("self");
    final ClusterMonitor monitor = new ClusterMonitor(10L);
    monitor.updateLeaderCommitIndex(10_000L);
    monitor.updateReplicaMatchIndex("other", 0L, 0L); // lag 10000 > threshold 10

    assertThat(RaftHAServer.hasHandoffTarget(List.of(peer("self", 0), peer("other", 0)), self, monitor)).isFalse();

    monitor.updateReplicaMatchIndex("other", 10_000L, 0L); // caught up
    assertThat(RaftHAServer.hasHandoffTarget(List.of(peer("self", 0), peer("other", 0)), self, monitor)).isTrue();
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static RaftPeer peer(final String id, final int priority) {
    return RaftPeer.newBuilder().setId(RaftPeerId.valueOf(id)).setAddress(id + ":2434").setPriority(priority).build();
  }

  private static RaftHAServer raftMock(final boolean leader) {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(leader);
    final RaftPeerId leaderId = RaftPeerId.valueOf(leader ? "self" : "peer-b");
    when(raft.getLeaderId()).thenReturn(leaderId);
    when(raft.getLocalPeerId()).thenReturn(RaftPeerId.valueOf("self"));
    when(raft.getUnambiguousPeerHttpAddress(leaderId)).thenReturn("peer-b:2480");
    return raft;
  }

  private static ArcadeStateMachine newStateMachine(final Path tempDir) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    return sm;
  }

  /** A well-formed WAL payload of a transaction with no pages, so the apply reaches applyChanges. */
  private static byte[] emptyWalTransaction() {
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
    buf.putLong(1L); // txId
    buf.putLong(0L); // timestamp
    buf.putInt(0);   // pageCount
    buf.putInt(0);   // segmentSize
    return buf.array();
  }

  private static TransactionContext txEntry(final ArcadeStateMachine sm, final String databaseName,
      final byte[] walData, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }
}
