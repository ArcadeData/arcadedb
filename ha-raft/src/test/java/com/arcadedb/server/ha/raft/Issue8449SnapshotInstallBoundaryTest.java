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
 * Regression tests for issue #8449: after a leader-driven snapshot install a follower registered
 * {@code firstTermIndexInLog - 1} (S-1) as its boundary even when the leader's latest snapshot marker M was at or
 * past the leader's log start S - the routine case, because Ratis purges whole log segments. S-1 is then neither in
 * the leader's log nor its marker, so {@code LogAppender.getPrevious(S)} is {@code null}, {@code shouldInstallSnapshot}
 * stays true and the leader re-notifies the install forever, each notification answered {@code ALREADY_INSTALLED}.
 * <p>
 * The fix registers S itself, with the term the leader's log holds there ({@code firstTermIndexInLog}): S is in the
 * leader's log, so {@code getPrevious(S + 1)} returns exactly that TermIndex. The marker's own TermIndex is not used
 * for an index inside the log, because {@code getPrevious} reads the log first and a marker term can differ from it.
 *
 * @see ArcadeStateMachine#resolveInstalledSnapshotBoundary(TermIndex, TermIndex)
 */
class Issue8449SnapshotInstallBoundaryTest {

  private static final String LEADER_PEER_ID          = "peer-b_2434";
  private static final String LOCAL_HTTP              = "localhost:2480";
  /** The leader's log start S, as carried by the install notification. */
  private static final long   FIRST_LOG_INDEX         = 234020L;
  /** The term the leader's log holds at S. */
  private static final long   FIRST_LOG_TERM          = 9L;
  /** S-1: what the old code always registered. */
  private static final long   COMPUTED_SNAPSHOT_INDEX = FIRST_LOG_INDEX - 1;
  private static final TermIndex FIRST_LOG            = TermIndex.valueOf(FIRST_LOG_TERM, FIRST_LOG_INDEX);

  // ---------------------------------------------------------------------------------------------
  // resolveInstalledSnapshotBoundary: the pure decision function
  // ---------------------------------------------------------------------------------------------

  @Test
  void registersTheLogStartWhenTheMarkerIsPastIt() {
    // The chaos-run shape: log start 206194, marker 206950 - a partly purged segment is still in the log.
    final TermIndex marker = TermIndex.valueOf(FIRST_LOG_TERM, COMPUTED_SNAPSHOT_INDEX + 2_481L);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotBoundary(FIRST_LOG, marker))
        .as("S-1 is neither in the leader's log nor its marker; S is in the log, so getPrevious(S + 1) resolves")
        .isEqualTo(FIRST_LOG);
  }

  @Test
  void registersTheLogStartWhenTheMarkerIsExactlyAtIt() {
    final TermIndex marker = TermIndex.valueOf(FIRST_LOG_TERM, FIRST_LOG_INDEX);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotBoundary(FIRST_LOG, marker))
        .isEqualTo(FIRST_LOG);
  }

  @Test
  void usesTheLogTermNotTheMarkerTermForAnIndexInsideTheLog() {
    // takeSnapshot() pairs the applied index with the applied term, which a configuration entry can advance on its own,
    // so the marker's term (12) can exceed the log's (9). getPrevious reads the log first, so only the log term matches.
    final TermIndex inflatedMarker = TermIndex.valueOf(12L, COMPUTED_SNAPSHOT_INDEX + 2_481L);
    final TermIndex boundary = ArcadeStateMachine.resolveInstalledSnapshotBoundary(FIRST_LOG,
        inflatedMarker);
    assertThat(boundary.getTerm()).isEqualTo(FIRST_LOG_TERM);
    assertThat(boundary).isNotEqualTo(inflatedMarker);
  }

  @Test
  void registersTheMarkerWhenItEndsRightBeforeTheLogStart() {
    // Unchanged #8360 case: getPrevious(S) reads the marker itself, so its term is the one to match.
    final TermIndex marker = TermIndex.valueOf(8L, COMPUTED_SNAPSHOT_INDEX);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotBoundary(FIRST_LOG, marker))
        .isEqualTo(marker);
  }

  @Test
  void keepsTheApproximationWhenTheMarkerIsOlder() {
    final TermIndex olderMarker = TermIndex.valueOf(7L, COMPUTED_SNAPSHOT_INDEX - 500L);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotBoundary(FIRST_LOG, olderMarker))
        .isEqualTo(TermIndex.valueOf(FIRST_LOG_TERM, COMPUTED_SNAPSHOT_INDEX));
  }

  @Test
  void registersTheLogStartVerbatimEvenWhenTheIndexClampApplies() {
    // S-1 is clamped to 0 for S <= 1. The boundary must still be the log start itself, never a clamped index + 1.
    final TermIndex logStartAtZero = TermIndex.valueOf(FIRST_LOG_TERM, 0L);
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotBoundary(logStartAtZero, TermIndex.valueOf(FIRST_LOG_TERM, 5L)))
        .isEqualTo(logStartAtZero);
  }

  @Test
  void keepsTheApproximationWhenThereIsNoMarker() {
    assertThat(ArcadeStateMachine.resolveInstalledSnapshotBoundary(FIRST_LOG, null))
        .isEqualTo(TermIndex.valueOf(FIRST_LOG_TERM, COMPUTED_SNAPSHOT_INDEX));
  }

  // ---------------------------------------------------------------------------------------------
  // End-to-end through notifyInstallSnapshotFromLeader
  // ---------------------------------------------------------------------------------------------

  /** What Ratis receives back, and the marker it persists, must be the log start when the marker is past it. */
  @Test
  void installRegistersTheLogStartWhenTheMarkerIsPastIt(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir, Set.of());
    replaceReconciler(sm, new StubReconciler(TermIndex.valueOf(12L, COMPUTED_SNAPSHOT_INDEX + 2_481L)));
    sm.setRaftHAServer(followerRaftHAServer());
    try {
      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), FIRST_LOG).get();

      assertThat(installed).isEqualTo(FIRST_LOG);
      final var live = sm.getStateMachineStorage().getLatestSnapshot();
      assertThat(live.getTermIndex()).isEqualTo(FIRST_LOG);
    } finally {
      sm.close();
    }
  }

  /** The per-database applied positions follow the same boundary, so nothing after it is skipped. */
  @Test
  void installRecordsTheSameBoundaryForEveryDatabase(@TempDir final Path tempDir) throws Exception {
    final String dbName = "db-ok";
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir, Set.of(dbName));
    replaceReconciler(sm, new StubReconciler(TermIndex.valueOf(FIRST_LOG_TERM, COMPUTED_SNAPSHOT_INDEX + 2_481L)));
    sm.setRaftHAServer(followerRaftHAServer());
    try {
      sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), FIRST_LOG).get();

      assertThat(sm.readPersistedAppliedIndex(dbName)).isEqualTo(FIRST_LOG_INDEX);
    } finally {
      sm.close();
    }
  }

  /** Control: a marker older than S-1 keeps the old approximation. */
  @Test
  void installKeepsTheApproximationWhenTheMarkerIsOlder(@TempDir final Path tempDir) throws Exception {
    final ArcadeStateMachine sm = newInitializedStateMachine(tempDir, Set.of());
    replaceReconciler(sm, new StubReconciler(TermIndex.valueOf(7L, COMPUTED_SNAPSHOT_INDEX - 500L)));
    sm.setRaftHAServer(followerRaftHAServer());
    try {
      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(), FIRST_LOG).get();

      assertThat(installed).isEqualTo(TermIndex.valueOf(FIRST_LOG_TERM, COMPUTED_SNAPSHOT_INDEX));
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
  private static ArcadeStateMachine newInitializedStateMachine(final Path tempDir, final Set<String> databaseNames)
      throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabaseNames()).thenReturn(databaseNames);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
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
        Issue8449SnapshotInstallBoundaryTest.class.getClassLoader(),
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
