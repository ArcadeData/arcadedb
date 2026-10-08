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
import com.arcadedb.server.FakeArcadeDBServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8432: a STATICALLY configured member removed while down, re-added with its config volume
 * retained and caught up by a leader-driven snapshot install past the leader's compaction point never observes the
 * re-add and is never armed as a runtime joiner (#7819), so #8353's join boundary - a no-op on an unarmed node - did
 * not hold its readiness either.
 * <p>
 * Every leader-driven install on an unarmed node now opens a TRANSIENT hold: the documents must be confirmed after the
 * install - the leader-confirmed match {@code SecurityCatchUp.afterSnapshotInstall} asks for, or a seed applied from
 * the log - before the node counts as converged. The node stays unarmed and no marker is written, so the #7819
 * contract ("armed only on a runtime join") is unchanged.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8432StaticMemberSnapshotInstallHoldTest {

  private static final RaftPeerId SELF            = RaftPeerId.valueOf("arcadedb-3");
  private static final String     LEADER_PEER_ID  = "peer-b_2434";
  private static final long       FIRST_LOG_INDEX = 5_000L;
  private static final long       SNAPSHOT_INDEX  = FIRST_LOG_INDEX - 1;

  @TempDir
  File tempDir;

  // -----------------------------------------------------------------------------------------------
  // The detector
  // -----------------------------------------------------------------------------------------------

  /** The issue's scenario: a static member that converged in its previous membership, then caught up by snapshot. */
  @Test
  void aStaticMemberCaughtUpBySnapshotIsHeldWithoutBeingArmed() {
    final File marker = marker();
    final RuntimeJoinDetector detector = staticMember(marker);
    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall()).as("no install yet").isEmpty();
    assertThat(detector.lastSnapshotInstallIndex()).isEqualTo(-1L);

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .as("held until the cluster's current documents are confirmed")
        .containsExactly("users", "groups", "API tokens");
    assertThat(detector.lastSnapshotInstallIndex()).isEqualTo(SNAPSHOT_INDEX);
    assertThat(detector.hasJoinedAtRuntime()).as("the #7819 contract: not armed").isFalse();
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
    assertThat(marker).as("transient: nothing written").doesNotExist();
  }

  /** What releases it: the catch-up match read at the applied index the install left, or seeds from the log. */
  @Test
  void theCatchUpMatchOrALaterSeedReleasesIt() {
    final RuntimeJoinDetector matched = staticMember(null);
    matched.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    matched.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX);
    assertThat(matched.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();

    final RuntimeJoinDetector seeded = staticMember(null);
    seeded.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    seeded.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, SNAPSHOT_INDEX + 3);
    seeded.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, SNAPSHOT_INDEX + 4);
    assertThat(seeded.securityDocumentsNotConfirmedSinceSnapshotInstall()).containsExactly("API tokens");
    seeded.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, SNAPSHOT_INDEX + 5);
    assertThat(seeded.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
  }

  /** A match read below the snapshot index - the once-per-start catch-up racing the install - does not count. */
  @Test
  void aMatchReadBeforeTheInstallDoesNotCount() {
    final RuntimeJoinDetector detector = staticMember(null);
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    detector.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX - 1);

    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .containsExactly("users", "groups", "API tokens");
  }

  /** An install recorded BEFORE the snapshot does not count whatever its index: it came from a superseded log. */
  @Test
  void anInstallRecordedBeforeTheSnapshotDoesNotCountWhateverItsIndex() {
    final RuntimeJoinDetector detector = staticMember(null);
    installAll(detector, SNAPSHOT_INDEX + 50);

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .containsExactly("users", "groups", "API tokens");
  }

  /** Every install opens a hold of its own, and an older one cannot pull it back. */
  @Test
  void aLaterInstallHoldsAgainAndAnOlderOneIsIgnored() {
    final RuntimeJoinDetector detector = staticMember(null);
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    detector.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX);
    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX + 1_000);
    assertThat(detector.lastSnapshotInstallIndex()).isEqualTo(SNAPSHOT_INDEX + 1_000);
    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .containsExactly("users", "groups", "API tokens");

    detector.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX + 1_000);
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    assertThat(detector.lastSnapshotInstallIndex()).isEqualTo(SNAPSHOT_INDEX + 1_000);
    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
  }

  /** An armed node is judged by the armed gate (#8353); the transient hold is the unarmed node's only. */
  @Test
  void anArmedNodeReportsNoTransientHold() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 98);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 100);

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  /** The Raft server and plugin expose the detector's state; a plugin without a server reports no install. */
  @Test
  void theServerAndPluginExposeTheSignal() {
    final RaftHAServer server = detachedServer();
    final RuntimeJoinDetector detector = server.getStateMachine().getRuntimeJoinDetector();
    assertThat(server.getLastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(server.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(server.getLastSnapshotInstallIndex()).isEqualTo(SNAPSHOT_INDEX);
    assertThat(server.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .containsExactly("users", "groups", "API tokens");

    final RaftHAPlugin plugin = new RaftHAPlugin();
    assertThat(plugin.getLastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(plugin.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .as("an unreadable Raft server is no evidence of an install: nothing held")
        .isEmpty();
  }

  // -----------------------------------------------------------------------------------------------
  // The caller: the leader-driven install itself
  // -----------------------------------------------------------------------------------------------

  /** Drives the real {@code notifyInstallSnapshotFromLeader} body on an unarmed node. */
  @Test
  void aLeaderDrivenInstallOnAStaticMemberOpensTheHold(@TempDir final Path smDir) throws Exception {
    final File marker = marker();
    final RuntimeJoinDetector detector = staticMember(marker);

    final ArcadeStateMachine sm = newStateMachine(smDir);
    setField(ArcadeStateMachine.class, sm, "reconciler", new NoOpReconciler());
    sm.setRaftHAServer(followerRaft());
    sm.setRuntimeJoinDetector(detector);
    try {
      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
      assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);

      assertThat(detector.lastSnapshotInstallIndex()).isEqualTo(SNAPSHOT_INDEX);
      assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall())
          .containsExactly("users", "groups", "API tokens");
      assertThat(detector.hasJoinedAtRuntime()).isFalse();
      assertThat(marker).doesNotExist();
    } finally {
      sm.close();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  private File marker() {
    return new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
  }

  /** A member from its first configuration, which converged during its previous membership. */
  private static RuntimeJoinDetector staticMember(final File marker) {
    final RuntimeJoinDetector detector = marker != null ? new RuntimeJoinDetector(marker, true) : new RuntimeJoinDetector();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 1);
    installAll(detector, 2);
    // Restarted after being removed and re-added while down: every configuration it observes names it.
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 4_200);
    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    return detector;
  }

  private static void installAll(final RuntimeJoinDetector detector, final long index) {
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, index);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, index);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, index);
  }

  private static List<RaftPeerId> peers(final String... ids) {
    return Arrays.stream(ids).map(RaftPeerId::valueOf).toList();
  }

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "arcadedb-0:2434:2480");
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, tempDir.getAbsolutePath());
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
    final FakeArcadeDBServer arcadeServer = FakeArcadeDBServer.create("arcadedb-0", new ContextConfiguration());
    return new RaftHAServer(arcadeServer, config);
  }

  /** A reconciler that touches nothing and installs every database. */
  private static class NoOpReconciler extends DatabaseReconciler {
    @Override
    ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderPeerId, final String leaderHttpAddr, final String leaderHttpsAddr,
        final String clusterToken, final long installedBoundaryIndex) {
      return new ReconcileFromLeaderResult(Set.of(), null);
    }
  }

  private static RaftHAServer followerRaft() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false);
    raft.leaderId(RaftPeerId.valueOf(LEADER_PEER_ID));
    raft.peerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID), "peer-b:2480");
    raft.localHttpAddress("localhost:2480");
    return raft;
  }

  private static RaftProtos.RoleInfoProto leaderRoleInfo() {
    return RaftProtos.RoleInfoProto.newBuilder()
        .setFollowerInfo(RaftProtos.FollowerInfoProto.newBuilder()
            .setLeaderInfo(RaftProtos.ServerRpcProto.newBuilder()
                .setId(RaftProtos.RaftPeerProto.newBuilder()
                    .setId(ByteString.copyFromUtf8(LEADER_PEER_ID)))))
        .build();
  }

  private static ArcadeStateMachine newStateMachine(final Path dir) throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, dir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, config);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
        RaftStorage.newBuilder().setDirectory(dir.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
            .build());
    return sm;
  }

  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(Issue8432StaticMemberSnapshotInstallHoldTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  private static void setField(final Class<?> owner, final Object target, final String name, final Object value)
      throws Exception {
    final Field f = owner.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
