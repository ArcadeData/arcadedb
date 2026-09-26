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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8353: a runtime joiner removed from the cluster WHILE IT WAS DOWN, re-added with its
 * config volume retained, and caught up by a leader-driven snapshot install, never observed the re-add. It did not
 * receive the removal, so every configuration it replayed named it, and the re-add's joint entry lay below the
 * leader's compaction point, so it was never applied. Its join index stayed at the first join, the security documents
 * it installed during its PREVIOUS membership kept counting, and the readiness gate of #8317 reported it converged
 * while it could still hold a user dropped, a group narrowed or a token revoked while it was out.
 * <p>
 * A leader-driven snapshot install on an armed node is now a join boundary of its own: the join index moves to just
 * below the snapshot index and every install recorded before it is forgotten, so only a seed applied from the log
 * after it or the leader-confirmed match read after the install converges the node again.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8353SnapshotInstallIsAJoinBoundaryTest {

  private static final RaftPeerId SELF            = RaftPeerId.valueOf("arcadedb-3");
  private static final String     LEADER_PEER_ID  = "peer-b_2434";
  private static final long       FIRST_LOG_INDEX = 5_000L;
  private static final long       SNAPSHOT_INDEX  = FIRST_LOG_INDEX - 1;

  @TempDir
  File tempDir;

  // -----------------------------------------------------------------------------------------------
  // The detector
  // -----------------------------------------------------------------------------------------------

  /**
   * The issue's scenario end to end on the detector: joined at 100, converged at 101, restarted after being removed
   * and re-added while down, observing only configurations that name it, then caught up by a snapshot install.
   */
  @Test
  void aReAddedNodeCaughtUpBySnapshotNoLongerCountsItsPreviousMembership() {
    final File marker = marker();
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    joinAtRuntime(before, 100);
    installAll(before, 101);
    assertThat(before.securityDocumentsNotInstalledSinceJoin()).isEmpty();

    // Restarted: its own log replays only configurations naming it, and the snapshot install delivers the final
    // configuration after the re-add, which names it too. Neither rule of onConfiguration fires.
    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);
    restarted.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 150);
    restarted.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 4_200);
    assertThat(restarted.joinIndex()).as("the configuration log alone cannot see the re-add").isEqualTo(100L);
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin())
        .as("the defect: the previous membership's installs still count")
        .isEmpty();

    assertThat(restarted.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX)).isTrue();

    assertThat(restarted.joinIndex()).isEqualTo(SNAPSHOT_INDEX - 1);
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin())
        .as("held until the cluster's current documents are confirmed")
        .containsExactly("users", "groups", "API tokens");
  }

  /** What releases it: the catch-up match, read at the applied index the install left, or a later seed. */
  @Test
  void theCatchUpMatchReadAfterTheInstallOrALaterSeedConvergesIt() {
    final RuntimeJoinDetector matched = convergedJoiner();
    matched.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    matched.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX);
    assertThat(matched.securityDocumentsNotInstalledSinceJoin())
        .as("a match read at the snapshot index counts: the boundary is just below it (#8346)")
        .isEmpty();

    final RuntimeJoinDetector seeded = convergedJoiner();
    seeded.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    seeded.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, SNAPSHOT_INDEX + 3);
    seeded.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, SNAPSHOT_INDEX + 4);
    assertThat(seeded.securityDocumentsNotInstalledSinceJoin()).containsExactly("API tokens");
  }

  /**
   * A match read BELOW the snapshot index - the once-per-start catch-up racing the install, which read its applied
   * index before the install advanced it - describes the documents before the install and does not count.
   */
  @Test
  void aMatchReadBeforeTheInstallDoesNotCount() {
    final RuntimeJoinDetector detector = convergedJoiner();
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    detector.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX - 1);
    detector.onSecurityDocumentsMatchedLeader(150);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  /**
   * Installs are forgotten, not only judged by index: an install recorded before the snapshot at an index past it
   * came from a log the install superseded - for one, a log the divergence reformat has since thrown away.
   */
  @Test
  void anInstallRecordedBeforeTheSnapshotIsForgottenWhateverItsIndex() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    joinAtRuntime(detector, 100);
    installAll(detector, SNAPSHOT_INDEX + 50);

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  /** Only forward: a join the log did observe past the snapshot is kept. */
  @Test
  void aLaterJoinIsNotPulledBack() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    joinAtRuntime(detector, SNAPSHOT_INDEX + 10);

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(detector.joinIndex()).isEqualTo(SNAPSHOT_INDEX + 10);
  }

  /**
   * A node that never joined at runtime is not gated (#7819), and a snapshot install is no evidence that it did: a
   * statically configured member lagging past the compaction point must stay unarmed, and keep what it recorded.
   */
  @Test
  void aStaticMemberIsNotArmedByASnapshotInstall() {
    final File marker = marker();
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, true);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 1);
    installAll(detector, 2);

    assertThat(detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX)).isFalse();

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
    assertThat(marker).doesNotExist();
  }

  /** Persisted: a restart right after the install, before the catch-up answered, is still held. */
  @Test
  void theBoundarySurvivesARestart() {
    final File marker = marker();
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    joinAtRuntime(before, 100);
    installAll(before, 101);
    before.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);

    assertThat(restarted.joinIndex()).isEqualTo(SNAPSHOT_INDEX - 1);
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  /** An unknown or empty snapshot index records nothing. */
  @Test
  void aNonPositiveSnapshotIndexRecordsNothing() {
    final RuntimeJoinDetector detector = convergedJoiner();

    assertThat(detector.onSnapshotInstalledFromLeader(0)).isFalse();
    assertThat(detector.onSnapshotInstalledFromLeader(-1)).isFalse();

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

  // -----------------------------------------------------------------------------------------------
  // The caller: the leader-driven install itself
  // -----------------------------------------------------------------------------------------------

  /**
   * Drives the real {@code notifyInstallSnapshotFromLeader} body: without the call from the install, the detector
   * method above would be a capability nothing uses.
   */
  @Test
  void aLeaderDrivenInstallMovesTheJoinBoundaryOfTheDetectorItIsWiredTo(@TempDir final Path smDir) throws Exception {
    final File marker = marker();
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, true);
    joinAtRuntime(detector, 100);
    installAll(detector, 101);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();

    final ArcadeStateMachine sm = newStateMachine(smDir);
    setField(ArcadeStateMachine.class, sm, "reconciler", new NoOpReconciler());
    sm.setRaftHAServer(followerRaft());
    sm.setRuntimeJoinDetector(detector);
    try {
      final TermIndex installed = sm.notifyInstallSnapshotFromLeader(leaderRoleInfo(),
          TermIndex.valueOf(9L, FIRST_LOG_INDEX)).get();
      assertThat(installed.getIndex()).isEqualTo(SNAPSHOT_INDEX);

      assertThat(detector.joinIndex()).isEqualTo(SNAPSHOT_INDEX - 1);
      assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
      assertThat(new RuntimeJoinDetector(marker, true).joinIndex())
          .as("and the boundary is on disk by the time the install returns")
          .isEqualTo(SNAPSHOT_INDEX - 1);
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

  private static RuntimeJoinDetector convergedJoiner() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    joinAtRuntime(detector, 100);
    installAll(detector, 101);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
    return detector;
  }

  private static void joinAtRuntime(final RuntimeJoinDetector detector, final long joinIndex) {
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), joinIndex - 2);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), joinIndex);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(),
        joinIndex + 1);
  }

  private static void installAll(final RuntimeJoinDetector detector, final long index) {
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, index);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, index);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, index);
  }

  private static List<RaftPeerId> peers(final String... ids) {
    return Arrays.stream(ids).map(RaftPeerId::valueOf).toList();
  }

  /** A reconciler that touches nothing and installs every database. */
  private static class NoOpReconciler extends DatabaseReconciler {
    @Override
    ReconcileFromLeaderResult reconcileDatabasesFromLeader(final String leaderHttpAddr, final String leaderHttpsAddr,
        final String clusterToken) {
      return new ReconcileFromLeaderResult(Set.of(), null);
    }
  }

  private static RaftHAServer followerRaft() {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(false);
    when(raft.getLeaderId()).thenReturn(RaftPeerId.valueOf(LEADER_PEER_ID));
    when(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf(LEADER_PEER_ID))).thenReturn("peer-b:2480");
    when(raft.getLocalHttpAddress()).thenReturn("localhost:2480");
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

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(config);
    when(server.getDatabaseNames()).thenReturn(Set.of());

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubRaftServer(), RaftGroupId.valueOf(UUID.randomUUID()),
        RaftStorage.newBuilder().setDirectory(dir.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
            .build());
    return sm;
  }

  private static RaftServer stubRaftServer() {
    return (RaftServer) Proxy.newProxyInstance(Issue8353SnapshotInstallIsAJoinBoundaryTest.class.getClassLoader(),
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
