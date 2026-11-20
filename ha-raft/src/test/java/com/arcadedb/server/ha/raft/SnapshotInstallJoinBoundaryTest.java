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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A snapshot install is a join boundary and holds a static member until it catches up, also across a restart.
 */
class SnapshotInstallJoinBoundaryTest {

  // ------------------------------------------------------------------------------------------
  // Issue #8353
  // ------------------------------------------------------------------------------------------
  /*
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
   */

  private static final RaftPeerId SELF            = RaftPeerId.valueOf("arcadedb-3");

  private static final String     LEADER_PEER_ID  = "peer-b_2434";

  private static final long       FIRST_LOG_INDEX = 5_000L;

  private static final long       SNAPSHOT_INDEX  = FIRST_LOG_INDEX - 1;

  @TempDir
  File tempDir;

  // Regression for #8353: a re added node caught up by snapshot no longer counts its previous membership
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

  // Regression for #8353: the catch up match read after the install or alater seed converges it
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

  // Regression for #8353: a match read before the install does not count
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

  // Regression for #8353: an install recorded before the snapshot is forgotten whatever its index
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

  // Regression for #8353: a later join is not pulled back
  /** Only forward: a join the log did observe past the snapshot is kept. */
  @Test
  void aLaterJoinIsNotPulledBack() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    joinAtRuntime(detector, SNAPSHOT_INDEX + 10);

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(detector.joinIndex()).isEqualTo(SNAPSHOT_INDEX + 10);
  }

  // Regression for #8353: a static member is not armed by asnapshot install
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

  // Regression for #8353: the boundary survives arestart
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

  // Regression for #8353: a non positive snapshot index records nothing
  /** An unknown or empty snapshot index records nothing. */
  @Test
  void aNonPositiveSnapshotIndexRecordsNothing() {
    final RuntimeJoinDetector detector = convergedJoiner();

    assertThat(detector.onSnapshotInstalledFromLeader(0)).isFalse();
    assertThat(detector.onSnapshotInstalledFromLeader(-1)).isFalse();

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

  // Regression for #8353: a leader driven install moves the join boundary of the detector it is wired to
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
    return (RaftServer) Proxy.newProxyInstance(SnapshotInstallJoinBoundaryTest.class.getClassLoader(),
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

  // ------------------------------------------------------------------------------------------
  // Issue #8432
  // ------------------------------------------------------------------------------------------
  /*
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
   */

  // Regression for #8432: a static member caught up by snapshot is held without being armed
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

  // Regression for #8432: the catch up match or alater seed releases it
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

  // Regression for #8432: a match read before the install does not count
  /** A match read below the snapshot index - the once-per-start catch-up racing the install - does not count. */
  @Test
  void aMatchReadBeforeTheInstallDoesNotCountStaticMember() {
    final RuntimeJoinDetector detector = staticMember(null);
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    detector.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX - 1);

    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8432: an install recorded before the snapshot does not count whatever its index
  /** An install recorded BEFORE the snapshot does not count whatever its index: it came from a superseded log. */
  @Test
  void anInstallRecordedBeforeTheSnapshotDoesNotCountWhateverItsIndex() {
    final RuntimeJoinDetector detector = staticMember(null);
    installAll(detector, SNAPSHOT_INDEX + 50);

    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    assertThat(detector.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8432: a later install holds again and an older one is ignored
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

  // Regression for #8432: an armed node reports no transient hold
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

  // Regression for #8432: the server and plugin expose the signal
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

  // Regression for #8432: a leader driven install on astatic member opens the hold
  // -----------------------------------------------------------------------------------------------
  // The caller: the leader-driven install itself
  // -----------------------------------------------------------------------------------------------

  /** Drives the real {@code notifyInstallSnapshotFromLeader} body on an unarmed node. */
  @Test
  void aLeaderDrivenInstallOnAStaticMemberOpensTheHold(@TempDir final Path smDir) throws Exception {
    final File marker = marker();
    final RuntimeJoinDetector detector = staticMember(marker);

    final ArcadeStateMachine sm = newStateMachineStaticMember(smDir);
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

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "arcadedb-0:2434:2480");
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, tempDir.getAbsolutePath());
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
    final FakeArcadeDBServer arcadeServer = FakeArcadeDBServer.create("arcadedb-0", new ContextConfiguration());
    return new RaftHAServer(arcadeServer, config);
  }

  private static ArcadeStateMachine newStateMachineStaticMember(final Path dir) throws IOException {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, dir.resolve("databases").toString());
    config.setValue(GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES, false);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 0);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create((String) null, config);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(server);
    sm.initialize(stubRaftServerStaticMember(), RaftGroupId.valueOf(UUID.randomUUID()),
        RaftStorage.newBuilder().setDirectory(dir.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
            .build());
    return sm;
  }

  private static RaftServer stubRaftServerStaticMember() {
    return (RaftServer) Proxy.newProxyInstance(SnapshotInstallJoinBoundaryTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }

  // ------------------------------------------------------------------------------------------
  // Issue #8465
  // ------------------------------------------------------------------------------------------
  /*
   * Regression test for issue #8465: the snapshot-install hold #8432 opens on a STATIC member (never armed as a runtime
   * joiner, #7819) lived in memory only, so a restart between the install and the leader's confirmation came back
   * unheld - its log starts at the registered snapshot marker, every configuration it replays names it, and nothing else
   * recorded that its security documents were never confirmed.
   * <p>
   * The hold is now kept in a marker of its own, next to the runtime-join marker but never the same file, so reading it
   * back restores the hold without arming the node, and the file is deleted as soon as the hold is released.
   *
   */

  private static final long       SNAPSHOT_INDEX_RESTART = 4_999L;

  // Regression for #8465: a restart before the confirmation restores the hold
  /** The issue's scenario: install, restart before the leader answered, and the restarted node is held again. */
  @Test
  void aRestartBeforeTheConfirmationRestoresTheHold() {
    final RuntimeJoinDetector before = staticMember(true);
    before.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);
    assertThat(holdMarker()).as("written the moment the hold opens").exists();
    assertThat(joinMarker()).as("the #7819 contract: a static member is never armed").doesNotExist();

    final RuntimeJoinDetector after = staticMember(true);

    assertThat(after.hasJoinedAtRuntime()).as("restoring the hold does not arm the node").isFalse();
    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(SNAPSHOT_INDEX_RESTART);
    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .as("the defect: the restarted node came back unheld")
        .containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8465: the catch up after the restart releases it and deletes the marker
  /** The once-per-start catch-up releases the restored hold: its match is read at an index at or past the install. */
  @Test
  void theCatchUpAfterTheRestartReleasesItAndDeletesTheMarker() {
    staticMember(true).onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);

    final RuntimeJoinDetector after = staticMember(true);
    after.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX_RESTART);

    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
    assertThat(holdMarker()).as("released: nothing left to restore").doesNotExist();
    assertThat(staticMember(true).securityDocumentsNotConfirmedSinceSnapshotInstall())
        .as("a further restart is not held").isEmpty();
  }

  // Regression for #8465: a partial confirmation survives the restart
  /** A partial confirmation is persisted too, so a restart waits only for the documents still unconfirmed. */
  @Test
  void aPartialConfirmationSurvivesTheRestart() {
    final RuntimeJoinDetector before = staticMember(true);
    before.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);
    before.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, SNAPSHOT_INDEX_RESTART + 2);
    before.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, SNAPSHOT_INDEX_RESTART + 3);
    assertThat(holdMarker()).exists();

    assertThat(staticMember(true).securityDocumentsNotConfirmedSinceSnapshotInstall()).containsExactly("groups");
  }

  // Regression for #8465: a confirmation before the restart leaves nothing to restore
  /** Confirmed before the restart: the marker was deleted, so the restarted node is not held. */
  @Test
  void aConfirmationBeforeTheRestartLeavesNothingToRestore() {
    final RuntimeJoinDetector before = staticMember(true);
    before.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);
    before.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX_RESTART);
    assertThat(holdMarker()).doesNotExist();

    final RuntimeJoinDetector after = staticMember(true);
    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
  }

  // Regression for #8465: a non persistent storage discards the hold
  /** Raft storage that is not persisted across restarts discards the hold, as it does the runtime-join marker. */
  @Test
  void aNonPersistentStorageDiscardsTheHold() {
    staticMember(true).onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);

    final RuntimeJoinDetector after = staticMember(false);

    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
    assertThat(holdMarker()).doesNotExist();
  }

  // Regression for #8465: arming the node deletes the hold marker
  /** A node armed later is judged by the armed gate: the hold marker is deleted rather than left stale. */
  @Test
  void armingTheNodeDeletesTheHoldMarker() {
    final RuntimeJoinDetector detector = staticMember(true);
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);
    assertThat(holdMarker()).exists();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-4"), SNAPSHOT_INDEX_RESTART + 10);

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
    assertThat(holdMarker()).doesNotExist();
    assertThat(joinMarker()).exists();
  }

  // Regression for #8465: a hold marker left beside ajoin marker is dropped on restart
  /**
   * A crash between arming and deleting the hold marker leaves both markers on disk (review of PR #8477): the restart is
   * armed, and the stale hold marker is dropped rather than leaked for good.
   */
  @Test
  void aHoldMarkerLeftBesideAJoinMarkerIsDroppedOnRestart() throws Exception {
    staticMember(true).onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);
    Files.writeString(joinMarker().toPath(), "peer=arcadedb-3\njoinIndex=10\n");

    final RuntimeJoinDetector after = new RuntimeJoinDetector(joinMarker(), holdMarker(), true);

    assertThat(after.hasJoinedAtRuntime()).isTrue();
    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(holdMarker()).doesNotExist();
  }

  // Regression for #8465: a failed join marker write keeps the hold marker
  /**
   * Review of PR #8477: when the runtime-join marker cannot be written at the moment the node arms, the hold marker is
   * kept, so a restart comes back held rather than neither armed nor held.
   */
  @Test
  void aFailedJoinMarkerWriteKeepsTheHoldMarker() throws Exception {
    final File notADirectory = new File(tempDir, "not-a-directory");
    Files.writeString(notADirectory.toPath(), "x");
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(new File(notADirectory, "join"), holdMarker(), true);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 1);
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX_RESTART);
    assertThat(holdMarker()).exists();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-4"), SNAPSHOT_INDEX_RESTART + 10);

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
    assertThat(holdMarker()).as("the join marker never reached the disk").exists();
  }

  // Regression for #8465: a corrupt hold marker restores nothing
  /** An unreadable hold marker restores nothing: the node behaves as before issue #8465 rather than failing. */
  @Test
  void aCorruptHoldMarkerRestoresNothing() throws Exception {
    Files.writeString(holdMarker().toPath(), "snapshotInstallIndex=not-a-number\n");

    final RuntimeJoinDetector after = staticMember(true);

    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
  }

  // Regression for #8465: the server places the hold marker beside the raft storage
  /** The server places the hold marker next to the Raft storage directory, apart from the runtime-join marker. */
  @Test
  void theServerPlacesTheHoldMarkerBesideTheRaftStorage() {
    final File storageDir = new File(tempDir, "raft-storage-arcadedb-3");
    final File hold = RaftStorageFiles.snapshotInstallHoldMarkerFile(storageDir);

    assertThat(hold.getParentFile()).isEqualTo(tempDir.getAbsoluteFile());
    assertThat(hold.getName()).isEqualTo("raft-storage-arcadedb-3.snapshot-install-hold");
    assertThat(hold).isNotEqualTo(RaftStorageFiles.runtimeJoinMarkerFile(storageDir));
  }

  // -----------------------------------------------------------------------------------------------

  private File joinMarker() {
    return new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
  }

  private File holdMarker() {
    return new File(tempDir, "raft-storage-arcadedb-3.snapshot-install-hold");
  }

  /** A statically configured member, (re)started: every configuration it observes names it. */
  private RuntimeJoinDetector staticMember(final boolean persistentStorage) {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(joinMarker(), holdMarker(), persistentStorage);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 1);
    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    return detector;
  }
}
