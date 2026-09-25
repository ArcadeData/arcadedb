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
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8360: a follower installing a leader-driven Raft snapshot registered the term of the
 * NEXT log entry ({@code firstTermIndexInLog}) as the snapshot boundary's term, instead of the term of the entry the
 * snapshot actually covers ({@code snapshotIndex = firstTermIndexInLog.getIndex() - 1}). The two differ whenever a
 * term/leadership change lands exactly on that boundary - routine under a rolling restart or a chaos-fault election,
 * not an edge case.
 * <p>
 * The wrong term is not a cosmetic mismatch: Ratis trusts it verbatim as {@code ServerState.latestInstalledSnapshot}
 * and uses an EXACT {@code equals()} check against it ({@code ServerState.containsTermIndex}) to answer the
 * {@code AppendEntries} log-matching consistency check the LEADER runs for the very first replication attempt after
 * this boundary. A wrong term therefore rejects every subsequent {@code AppendEntries} starting right after the
 * snapshot forever; the leader cannot walk back further (that is exactly why it drove a snapshot install here), so it
 * can only re-notify another install - which recomputes the identical wrong term and gets stuck at the identical
 * index on every reformat, matching the chaos-test reproduction in the issue.
 *
 * @see ArcadeStateMachine#resolveInstalledSnapshotTerm(long, long, TermIndex)
 */
class Issue8360InstallSnapshotTermTest {

  private static final String LEADER_PEER_ID  = "peer-b_2434";
  private static final String LOCAL_HTTP      = "localhost:2480";
  /** The first log index AFTER the snapshot, matching the leader's own retained-log start. */
  private static final long   FIRST_LOG_INDEX = 234020L;
  /** {@code FIRST_LOG_INDEX - 1}: the index the install must register as the snapshot boundary. */
  private static final long   SNAPSHOT_INDEX  = FIRST_LOG_INDEX - 1;

  // ---------------------------------------------------------------------------------------------
  // resolveInstalledSnapshotTerm: the pure decision function
  // ---------------------------------------------------------------------------------------------

  @Test
  void usesTheLeaderReportedTermWhenItsIndexMatches() {
    // The exact bug shape: a term boundary lands right at the snapshot index, so the next entry's term (11, the
    // old approximation) differs from the real term of the snapshot boundary itself (10, what the leader reports).
    final TermIndex leaderReported = TermIndex.valueOf(10L, SNAPSHOT_INDEX);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotTerm(SNAPSHOT_INDEX, 11L, leaderReported))
        .as("the leader's own answer for this exact index must win over the next-entry approximation")
        .isEqualTo(10L);
  }

  @Test
  void fallsBackToTheApproximationWhenTheLeaderReportedNothing() {
    // auto-acquire disabled, an unreachable leader, or an older leader build: no leader-reported value at all.
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotTerm(SNAPSHOT_INDEX, 11L, null))
        .isEqualTo(11L);
  }

  @Test
  void fallsBackToTheApproximationWhenTheLeaderReportedADifferentIndex() {
    // A race: the leader's own compaction advanced between answering Ratis's firstAvailableLogIndex and answering
    // this node's bootstrap-state query. Trusting a term reported for a DIFFERENT index would be worse than the
    // approximation, so the fallback applies instead.
    final TermIndex leaderReportedForALaterIndex = TermIndex.valueOf(12L, SNAPSHOT_INDEX + 5_000L);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotTerm(SNAPSHOT_INDEX, 11L, leaderReportedForALaterIndex))
        .isEqualTo(11L);
  }

  @Test
  void agreesWithTheApproximationWhenNoTermBoundaryFallsOnTheSnapshotIndex() {
    // The common case: no election happened right at the boundary, so both sources agree. Locks in that the fix
    // does not change behavior when there is nothing to fix.
    final TermIndex leaderReported = TermIndex.valueOf(11L, SNAPSHOT_INDEX);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotTerm(SNAPSHOT_INDEX, 11L, leaderReported))
        .isEqualTo(11L);
  }

  // ---------------------------------------------------------------------------------------------
  // End-to-end through notifyInstallSnapshotFromLeader: proves the wiring, not just the decision function
  // ---------------------------------------------------------------------------------------------

  /**
   * Drives the real {@code ArcadeStateMachine.notifyInstallSnapshotFromLeader} callback Ratis invokes on a
   * follower, with a stub {@link DatabaseReconciler} standing in for the network round trip to the leader (the
   * established pattern in {@code Issue6202SnapshotInstallGuardTest}/{@code Issue6760PartialSnapshotInstallTest}).
   * The stub reports a {@code leaderSnapshotTermIndex} whose term deliberately differs from
   * {@code firstTermIndexInLog}'s term - the exact shape of #8360 - and the installed {@link TermIndex} Ratis
   * receives back must carry the leader-reported term, not the old next-entry approximation.
   */
  @Test
  void installRegistersTheLeaderReportedTermNotTheNextEntryApproximation(@TempDir final Path tempDir) throws Exception {
    final long nextEntryTerm = 11L; // what the OLD code would have (wrongly) registered
    final long realBoundaryTerm = 10L; // what the leader actually applied at SNAPSHOT_INDEX

    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    replaceReconciler(sm, new StubReconciler(TermIndex.valueOf(realBoundaryTerm, SNAPSHOT_INDEX)));
    sm.setRaftHAServer(followerRaftHAServer());
    try {
      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(
          leaderRoleInfo(), TermIndex.valueOf(nextEntryTerm, FIRST_LOG_INDEX)).get();

      assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);
      assertThat(installed.getTerm())
          .as("must register the term the leader actually applied at the snapshot boundary, not the next entry's")
          .isEqualTo(realBoundaryTerm);

      // What Ratis itself persists as the discoverable marker must carry the same corrected term - this is the
      // value StateMachineUpdater.reload() reads back and ServerState.containsTermIndex() will later match
      // AppendEntries against.
      final var live = sm.getStateMachineStorage().getLatestSnapshot();
      assertThat(live.getIndex()).isEqualTo(SNAPSHOT_INDEX);
      assertThat(live.getTerm()).isEqualTo(realBoundaryTerm);
    } finally {
      sm.close();
    }
  }

  /** When the reconciler cannot report a leader term at all, the install still succeeds via the safe fallback. */
  @Test
  void installFallsBackSafelyWhenTheReconcilerHasNoLeaderTerm(@TempDir final Path tempDir) throws Exception {
    final long nextEntryTerm = 11L;

    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir);
    replaceReconciler(sm, new StubReconciler(null));
    sm.setRaftHAServer(followerRaftHAServer());
    try {
      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(
          leaderRoleInfo(), TermIndex.valueOf(nextEntryTerm, FIRST_LOG_INDEX)).get();

      assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);
      assertThat(installed.getTerm()).isEqualTo(nextEntryTerm);
    } finally {
      sm.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** A reconciler that touches no network and reports a fixed leader snapshot {@link TermIndex}. */
  private static final class StubReconciler extends DatabaseReconciler {
    private final TermIndex leaderSnapshotTermIndex;

    private StubReconciler(final TermIndex leaderSnapshotTermIndex) {
      this.leaderSnapshotTermIndex = leaderSnapshotTermIndex;
    }

    @Override
    ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderHttpAddr, final String leaderHttpsAddr,
        final String clusterToken) {
      return new ReconcileFromLeaderResult(Set.of(), leaderSnapshotTermIndex);
    }
  }

  private static void replaceReconciler(final ArcadeStateMachine sm, final DatabaseReconciler reconciler) throws Exception {
    final Field f = ArcadeStateMachine.class.getDeclaredField("reconciler");
    f.setAccessible(true);
    f.set(sm, reconciler);
  }

  private static RaftHAServer followerRaftHAServer() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLeaderId()).thenReturn(RaftPeerId.valueOf(LEADER_PEER_ID));
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn("peer-b:2480");
    when(raft.getLocalHttpAddress()).thenReturn(LOCAL_HTTP);
    when(raft.getUnambiguousPeerHttpsAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn(null);
    when(raft.getLocalHttpsAddress()).thenReturn(null);
    when(raft.getClusterToken()).thenReturn(null);
    return raft;
  }

  /** The Ratis role info a follower receives, naming {@link #LEADER_PEER_ID} as the installing leader. */
  private static RaftProtos.RoleInfoProto leaderRoleInfo() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
        .build();
  }

  /**
   * A state machine with real Ratis storage (the install registers a real snapshot marker through it) rooted at
   * {@code tempDir}, and auto-acquire off: the stub reconciler bypasses the network path entirely, so this setting
   * only controls which no-op branch of {@code DatabaseReconciler} would have run had the stub not overridden it.
   */
  private static ArcadeStateMachine newInitializedStateMachine(final Path tempDir) throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(new ArcadeDBServer(config));
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()), newFormattedStorage(tempDir.resolve("raft")));
    return sm;
  }

  private static RaftStorage newFormattedStorage(final Path dir) throws IOException {
    return RaftStorage.newBuilder()
        .setDirectory(dir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(
        Issue8360InstallSnapshotTermTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
