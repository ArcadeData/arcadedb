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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Issue #7819: which configuration sequences arm the security-convergence readiness gate, and - the half the issue
 * warned about - which must not. Each sequence is the one a real node applies, in the order Ratis delivers it.
 * <p>
 * Also covers the persisted runtime-join marker that survives a restart (#8329, #8390), the join-relative convergence of the
 * security documents (#8317, #8346) and the join index the Raft server reports from it (#8414).
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RuntimeJoinDetectorTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("arcadedb-3");

  @TempDir
  File tempDir;
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("arcadedb-1");

  /**
   * A peer added by {@code addPeer}, {@code connect cluster} or a {@code KubernetesAutoJoin} self-join: it applies
   * the cluster's existing configuration first, then the joint-consensus entry that adds it, then the final one.
   */
  @Test
  void aPeerAddedByAConfigurationChangeIsArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of());
    assertThat(detector.hasJoinedAtRuntime()).isFalse();

    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"))).as("the joint entry that adds this node arms it").isTrue();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /**
   * The baseline trap the issue named. When the joint entry is the FIRST configuration the joiner observes - its
   * earlier entries were compacted into the snapshot it installed, and the snapshot's configuration equalled the
   * starting group so Ratis never notified it - there is no earlier observation to diff against. The joint entry
   * describes its own change, so it still arms.
   */
  @Test
  void theJointEntryArmsWithoutAnyBaseline() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"));

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /**
   * The fallback: a joiner that fell behind and caught up by a snapshot install carrying the FINAL configuration
   * never applies the joint entry. It did observe a configuration without itself earlier in this process.
   */
  @Test
  void aFinalConfigurationFollowingOneWithoutThisNodeArms() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /**
   * A member of a freshly formed cluster: its first observed configuration is the leader's startup entry, which
   * names it. Getting the baseline backwards - treating "not observed yet" as "not a member" - would arm it here,
   * on every node of every cluster, which is the always-fire failure.
   */
  @Test
  void aMemberOfAFreshlyFormedClusterIsNotArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
    // A later leader's startup entry repeats the same membership.
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * A static member watching SOMEONE ELSE join: the joint entry lists this node on both sides, which is a change
   * to the cluster, not to this node's membership.
   */
  @Test
  void aMemberWatchingAnotherPeerJoinIsNotArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3", "arcadedb-4"), peers("arcadedb-1", "arcadedb-3"));
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3", "arcadedb-4"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * A statically configured node that restarts replays configuration entries, and every one of them names it -
   * including the joint entries of peers added since, which name it on the old side too. Replay must not arm it.
   */
  @Test
  void aStaticMemberReplayingItsLogOnRestartIsNotArmed() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"),
        peers("arcadedb-1", "arcadedb-2", "arcadedb-3"));
    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * Removal does not arm, and removal-then-re-add does: a node taken out while it was down and re-added by
   * {@code KubernetesAutoJoin}'s startup self-check (issue #5275) is exactly as stale as a new peer.
   */
  @Test
  void aRemovalDoesNotArmButARemovalThenReAddDoes() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-1"), peers("arcadedb-1", "arcadedb-3"));
    detector.onConfiguration(SELF, peers("arcadedb-1"), List.of());
    assertThat(detector.hasJoinedAtRuntime()).as("removed, not joined").isFalse();

    detector.onConfiguration(SELF, peers("arcadedb-1", "arcadedb-3"), peers("arcadedb-1"));
    assertThat(detector.hasJoinedAtRuntime()).as("re-added").isTrue();
  }

  /** Once armed, a later configuration does not disarm it: convergence releases the gate, membership does not. */
  @Test
  void armingIsNeverUndoneByALaterConfiguration() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"));
    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), List.of()))
        .as("a later configuration does not re-report the arming").isFalse();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3", "arcadedb-5"), peers("arcadedb-0", "arcadedb-3"));

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  /** A state machine Ratis has not initialized cannot say which peer is itself, and records nothing. */
  @Test
  void anUnknownSelfRecordsNothing() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();

    assertThat(detector.onConfiguration(null, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"))).isFalse();
    // And it did not consume a baseline either: the first real observation still behaves as a first one.
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), List.of());

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
  }

  /**
   * The wiring. The state machine decodes {@code oldPeers} from the configuration proto and hands both lists to
   * the detector - on an unwired state machine, whose {@code getId()} is {@code null}, without throwing and without
   * arming, since it cannot know which peer it is.
   */
  @Test
  void theStateMachineCallbackIsSafeAndInertBeforeRatisInitializesIt() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      assertThatNoException().isThrownBy(() -> sm.notifyConfigurationChanged(1, 11,
          configuration(List.of("arcadedb-0", "arcadedb-3"), List.of("arcadedb-0"))));
      assertThat(sm.getRuntimeJoinDetector().hasJoinedAtRuntime()).isFalse();
    } finally {
      sm.close();
    }
  }

  /** The detector the state machine records into is the one handed to it, so its owner reads what it records. */
  @Test
  void theStateMachineRecordsIntoTheDetectorItIsGiven() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final RuntimeJoinDetector owned = new RuntimeJoinDetector();
      sm.setRuntimeJoinDetector(owned);
      assertThat(sm.getRuntimeJoinDetector()).isSameAs(owned);
    } finally {
      sm.close();
    }
  }

  // -----------------------------------------------------------------------------------------------------------

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> peers = new ArrayList<>(ids.length);
    for (final String id : ids)
      peers.add(RaftPeerId.valueOf(id));
    return peers;
  }

  private static RaftProtos.RaftConfigurationProto configuration(final List<String> peerIds,
      final List<String> oldPeerIds) {
    final RaftProtos.RaftConfigurationProto.Builder builder = RaftProtos.RaftConfigurationProto.newBuilder();
    for (final String peerId : peerIds)
      builder.addPeers(peer(peerId));
    for (final String peerId : oldPeerIds)
      builder.addOldPeers(peer(peerId));
    return builder.build();
  }

  private static RaftProtos.RaftPeerProto peer(final String id) {
    return RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)).setAddress("localhost:2434")
        .build();
  }

  // Regression for #8317 / #8329: a converged joiner restarting after compaction is not held at readiness
  /**
   * Regression test for the interaction of issues #8317 and #8329: the runtime-join marker of #8329 restored only the
   * arm, while #8317 counts a security document as converged only when it was installed by a log entry after the join.
   * A converged joiner restarting after compaction loads its documents from disk rather than replaying them, so it
   * observed no install at all and was held at readiness until the bounded window expired, on every restart, and then
   * reported documents that "never reached this node". The marker now carries the join index and the install indexes.
   */
  @Test
  void aConvergedJoinerRestartingAfterCompactionIsNotHeld() {
    final File marker = marker();
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    joinAtRuntime(before, 100);
    installAll(before, 101);
    assertThat(before.securityDocumentsNotInstalledSinceJoin()).isEmpty();

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);
    observeTheSnapshotOfACompactedLog(restarted, 200);

    assertThat(restarted.hasJoinedAtRuntime()).isTrue();
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).as("converged before the restart").isEmpty();
  }

  // Regression for #8317 / #8329: a converged joiner restarting after compaction is not held at readiness
  @Test
  void aJoinerThatHadNotConvergedIsStillHeldAfterTheRestart() {
    final File marker = marker();
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    joinAtRuntime(before, 100);
    before.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, 101);
    // Installed from the previous membership: before the join, so it does not count.
    before.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, 50);

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);
    observeTheSnapshotOfACompactedLog(restarted, 200);

    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).containsExactly("groups", "API tokens");
  }

  // Regression for #8317 / #8329: a converged joiner restarting after compaction is not held at readiness
  @Test
  void aReAddAfterTheRestartStillMovesTheJoinIndexForward() {
    final File marker = marker();
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    joinAtRuntime(before, 100);
    installAll(before, 101);

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);
    restarted.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 300);
    assertThat(restarted.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 301)).as("re-armed by the re-add").isTrue();

    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");

    // And the moved join index is what the next restart reads back.
    final RuntimeJoinDetector again = new RuntimeJoinDetector(marker, true);
    assertThat(again.joinIndex()).isEqualTo(301);
    assertThat(again.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8317 / #8329: a converged joiner restarting after compaction is not held at readiness
  /** A marker written before the indexes were persisted says only "armed": every document is awaited, safely. */
  @Test
  void aMarkerWithoutIndexesRestoresTheArmAndAwaitsEveryDocument() throws IOException {
    final File marker = marker();
    Files.writeString(marker.toPath(), "peer=arcadedb-3\narmedAt=1\n");

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);

    assertThat(restarted.hasJoinedAtRuntime()).isTrue();
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8317 / #8329: a converged joiner restarting after compaction is not held at readiness
  /** An unreadable marker fails the safe way: armed, nothing counted as installed. */
  @Test
  void aCorruptMarkerRestoresTheArmAndAwaitsEveryDocument() throws IOException {
    final File marker = marker();
    Files.writeString(marker.toPath(), "joinIndex=abc\ninstalled.users=\n");

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);

    assertThat(restarted.hasJoinedAtRuntime()).isTrue();
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8317 / #8329: a converged joiner restarting after compaction is not held at readiness
  /** A static member never arms, so its installs never create a marker. */
  @Test
  void installsOnAStaticMemberWriteNoMarker() {
    final File marker = marker();
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, true);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), 1);
    installAll(detector, 2);

    assertThat(marker).doesNotExist();
  }

  private File marker() {
    return new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
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

  /** After compaction: only the final configuration naming this node, and no security entry replayed. */
  private static void observeTheSnapshotOfACompactedLog(final RuntimeJoinDetector detector, final long index) {
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of(), index);
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  // -----------------------------------------------------------------------------------------------------------
  // The detector
  // -----------------------------------------------------------------------------------------------------------

  /**
   * Regression test for issue #8329: the runtime-join detector of issue #7819 was in memory only, so a peer added at
   * runtime that restarted after the configuration entry adding it had been compacted into a Ratis snapshot observed
   * only configurations that already contained it, came back unarmed, and reported READY while enforcing the
   * credentials, groups and API tokens of its own config directory.
   * <p>
   * The restart is modelled by what a restarted node observes after that compaction: every configuration names it,
   * and no joint entry adds it. Those are exactly the sequences that must not arm a fresh detector (issue #7819's
   * static member), so only persisted state can tell the restarted joiner apart.
   * <p>
   * Without persisted state the restart after compaction is indistinguishable from a static member: the gap.
   */
  @Test
  void aRestartAfterCompactionIsInvisibleToAnInMemoryDetector() {
    final RuntimeJoinDetector restarted = new RuntimeJoinDetector();

    observeTheSnapshotOfACompactedLog(restarted);

    assertThat(restarted.hasJoinedAtRuntime()).as("the premise the marker exists for").isFalse();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  @Test
  void theArmWritesTheMarkerAndARestartReadsItBack() {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    assertThat(marker).doesNotExist();

    joinAtRuntime(before);

    assertThat(before.hasJoinedAtRuntime()).isTrue();
    assertThat(marker).exists();

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);
    assertThat(restarted.hasJoinedAtRuntime()).as("armed before observing anything").isTrue();

    observeTheSnapshotOfACompactedLog(restarted);
    assertThat(restarted.hasJoinedAtRuntime()).as("and still armed after the compacted configurations").isTrue();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  /** A static member never arms, so it never writes a marker for a later run to misread. */
  @Test
  void aStaticMemberWritesNoMarker() {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, true);

    observeTheSnapshotOfACompactedLog(detector);

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    assertThat(marker).doesNotExist();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  /** Not restoring means the previous membership no longer describes this node: the marker is discarded. */
  @Test
  void aDetectorThatDoesNotRestoreDiscardsAStaleMarker() throws IOException {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    Files.writeString(marker.toPath(), "peer=arcadedb-3\n");

    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, false);

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    assertThat(marker).doesNotExist();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  /** A marker that cannot be written is logged: the arm still holds for this process, and nothing throws. */
  @Test
  void aMarkerThatCannotBeWrittenLeavesTheInMemoryArmInPlace() throws IOException {
    final File notADirectory = new File(tempDir, "a-file");
    Files.writeString(notADirectory.toPath(), "x");
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(new File(notADirectory, "marker"), true);

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of());
    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"))).isTrue();

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  // -----------------------------------------------------------------------------------------------------------
  // The wiring in RaftHAServer, which owns the detector across process restarts
  // -----------------------------------------------------------------------------------------------------------

  /** The reported case end to end at the owner: a second RaftHAServer over the same storage starts armed. */
  @Test
  void aRaftHAServerBuiltOverTheStorageOfARuntimeJoinerStartsArmed() {
    final ContextConfiguration config = configuration(true);

    final RaftHAServer first = detachedServer(config);
    assertThat(first.hasJoinedClusterAtRuntime()).isFalse();
    joinAtRuntime(first.getStateMachine().getRuntimeJoinDetector());
    assertThat(first.hasJoinedClusterAtRuntime()).isTrue();

    final RaftHAServer restarted = detachedServer(config);
    assertThat(restarted.hasJoinedClusterAtRuntime())
        .as("the process restart of a runtime joiner, whose admission entry may be compacted away").isTrue();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  /** And a server whose node never joined at runtime starts unarmed, as issue #7819 requires. */
  @Test
  void aRaftHAServerOverTheStorageOfAStaticMemberStartsUnarmed() {
    final ContextConfiguration config = configuration(true);

    observeTheSnapshotOfACompactedLog(detachedServer(config).getStateMachine().getRuntimeJoinDetector());

    assertThat(detachedServer(config).hasJoinedClusterAtRuntime()).isFalse();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  /**
   * The design question the issue raised: the divergence reformat, {@code restartRatis(true)}, deletes the Raft
   * storage directory. The marker is outside it, so the reformat cannot take it along.
   */
  @Test
  void theMarkerSurvivesTheDeletionOfTheRaftStorageDirectory() throws IOException {
    final ContextConfiguration config = configuration(true);
    final File storageDir = new File(tempDir, "raft-storage-" + detachedServer(config).getLocalPeerId());
    assertThat(storageDir.mkdirs()).isTrue();
    Files.writeString(new File(storageDir, "log_inprogress_0").toPath(), "entries");

    joinAtRuntime(detachedServer(config).getStateMachine().getRuntimeJoinDetector());
    final File marker = RaftStorageFiles.runtimeJoinMarkerFile(storageDir);
    assertThat(marker).exists();
    assertThat(marker.getParentFile().getCanonicalFile()).isEqualTo(tempDir.getCanonicalFile());

    FileUtils.deleteRecursively(storageDir);
    assertThat(storageDir).doesNotExist();

    assertThat(marker).exists();
    assertThat(detachedServer(config).hasJoinedClusterAtRuntime()).isTrue();
  }

  // Regression for #8329: the runtime-join arm is persisted in a marker and restored after a restart
  /** A node whose Raft log is wiped at every start rejoins from nothing: its old marker is not restored. */
  @Test
  void aNonPersistentRaftStorageDoesNotRestoreTheMarker() {
    joinAtRuntime(detachedServer(configuration(true)).getStateMachine().getRuntimeJoinDetector());

    final RaftHAServer nonPersistent = detachedServer(configuration(false));

    assertThat(nonPersistent.hasJoinedClusterAtRuntime()).isFalse();
    assertThat(detachedServer(configuration(true)).hasJoinedClusterAtRuntime())
        .as("and the discarded marker is gone for good").isFalse();
  }

  // -----------------------------------------------------------------------------------------------------------

  /** The joiner's own sequence: the cluster's configuration, the joint entry that adds it, the final one. */
  private static void joinAtRuntime(final RuntimeJoinDetector detector) {
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"));
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
  }

  /** What a node restarting over a log compacted past its admission observes: configurations that name it. */
  private static void observeTheSnapshotOfACompactedLog(final RuntimeJoinDetector detector) {
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"), List.of());
  }

  private ContextConfiguration configuration(final boolean persistStorage) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "arcadedb-0:2434:2480");
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, tempDir.getAbsolutePath());
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, persistStorage);
    return config;
  }

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private static RaftHAServer detachedServer(final ContextConfiguration config) {
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("arcadedb-0");
    return new RaftHAServer(arcadeServer, config);
  }

  // Regression for #8390: a concurrent install never persists the armed marker with a null peer
  /**
   * Regression test for issue #8390: {@link RuntimeJoinDetector#onConfiguration} bumped the state version under the
   * monitor but assigned the peer name only after releasing it. A concurrent install on the other Ratis call site could
   * then persist the bumped version with a {@code null} peer, and the arming thread's own write, finding that version
   * already persisted, wrote nothing: the marker named {@code peer=null} until some later change bumped the version.
   * <p>
   * The interleaving is forced deterministically: the arming thread is parked in the INFO log line it emits between
   * leaving the monitor and calling {@code persist()}, while the test thread records an install.
   */
  @Test
  void aConcurrentInstallPersistsTheArmedPeerNotNull() throws Exception {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, true);

    final CountDownLatch armLogged = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Logger original = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new ParkingLogger(original, detector, armLogged, release));
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread arming = new Thread(() -> {
      try {
        detector.onConfiguration(SELF, List.of(LEADER, SELF), List.of(LEADER), 5L);
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "issue8390-arming");
    try {
      arming.start();
      assertThat(armLogged.await(30, TimeUnit.SECONDS)).as("the arming thread reached its log line").isTrue();

      // The other Ratis call site, while the arming thread is between the version bump and its own persist().
      detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, 10L);
    } finally {
      release.countDown();
      arming.join(30_000);
      LogManager.instance().setLogger(original);
    }

    assertThat(arming.isAlive()).isFalse();
    assertThat(failure.get()).isNull();
    assertThat(detector.hasJoinedAtRuntime()).isTrue();
    assertThat(peerLine(marker)).isEqualTo("peer=" + SELF);
  }

  // Regression for #8390: a concurrent install never persists the armed marker with a null peer
  /** A guard around the fix rather than a regression test: it passes before the fix too. */
  @Test
  void aRestartReadsBackThePeerTheArmWrote() throws IOException {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    final RuntimeJoinDetector before = new RuntimeJoinDetector(marker, true);
    before.onConfiguration(SELF, List.of(LEADER, SELF), List.of(LEADER), 5L);
    assertThat(peerLine(marker)).isEqualTo("peer=" + SELF);

    // A restarted node rewrites the marker on its next change: the restored peer must survive it.
    final RuntimeJoinDetector after = new RuntimeJoinDetector(marker, true);
    after.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, 12L);

    assertThat(peerLine(marker)).isEqualTo("peer=" + SELF);
  }

  private static String peerLine(final File marker) throws IOException {
    return Files.readAllLines(marker.toPath(), StandardCharsets.UTF_8).stream()
        .filter(l -> l.startsWith("peer="))
        .findFirst()
        .orElse(null);
  }

  /** Parks the thread that logs the detector's arm message until the test releases it; forwards everything. */
  private static final class ParkingLogger implements Logger {
    private final Logger         delegate;
    private final Object         requester;
    private final CountDownLatch reached;
    private final CountDownLatch release;

    ParkingLogger(final Logger delegate, final Object requester, final CountDownLatch reached,
        final CountDownLatch release) {
      this.delegate = delegate;
      this.requester = requester;
      this.reached = reached;
      this.release = release;
    }

    private void park(final Object iRequester, final String message) {
      // Matches the INFO line RuntimeJoinDetector.onConfiguration logs on the first arm. If that text is reworded,
      // update it here too: otherwise the test fails on the armLogged await instead of reaching the race.
      if (iRequester != requester || message == null || !message.contains("was added to the Raft configuration while running"))
        return;
      reached.countDown();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      park(iRequester, iMessage);
      delegate.log(iRequester, iLevel, iMessage, iException, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException,
        final String context, final Object... args) {
      park(iRequester, iMessage);
      delegate.log(iRequester, iLevel, iMessage, iException, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }

  // Regression for #8346: a security document the leader reports as matching counts as converged
  /**
   * Issue #8346: a node re-added with its config volume retained, whose log is behind the leader's compaction point,
   * catches up by snapshot install PAST the re-admission seed. The seed entries are never applied on it, and when its
   * retained documents already equal the cluster's the leader's fingerprint comparison writes nothing - so no install
   * after the join index ever happened, and the gate held it for the whole readiness window before logging a
   * misleading "never reached this node". A leader-confirmed match now counts as convergence at the applied index the
   * compared documents were read at.
   * <p>
   * The issue's scenario: re-added at 30, snapshot installed at 50, the leader reports every document matching.
   */
  @Test
  void aMatchReadPastTheJoinConvergesEveryDocument() {
    final RuntimeJoinDetector detector = reAddedAt(new RuntimeJoinDetector(), 30);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");

    detector.onSecurityDocumentsMatchedLeader(50);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

  // Regression for #8346: a security document the leader reports as matching counts as converged
  /**
   * A match read at or before the join proves nothing about the membership the join started: the documents compared
   * may be the previous membership's, and the join may still carry a change this node has not applied.
   */
  @Test
  void aMatchReadAtOrBeforeTheJoinDoesNotCount() {
    final RuntimeJoinDetector detector = reAddedAt(new RuntimeJoinDetector(), 30);

    detector.onSecurityDocumentsMatchedLeader(30);
    detector.onSecurityDocumentsMatchedLeader(12);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8346: a security document the leader reports as matching counts as converged
  /** An applied index that could not be read ({@code -1}) records nothing. */
  @Test
  void anUnknownAppliedIndexRecordsNothing() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"));

    detector.onSecurityDocumentsMatchedLeader(-1);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8346: a security document the leader reports as matching counts as converged
  /**
   * A match from BEFORE a later re-add - the "existing member" catch-up of a node that is then removed and added
   * back - must stop counting at the re-add, exactly as an install from the previous membership does (#8317).
   */
  @Test
  void aLaterReAddDiscardsAnEarlierMatch() {
    final RuntimeJoinDetector detector = reAddedAt(new RuntimeJoinDetector(), 30);
    detector.onSecurityDocumentsMatchedLeader(50);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), 60);
    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1"), 70)).isTrue();

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  // Regression for #8346: a security document the leader reports as matching counts as converged
  /**
   * The join can be observed AFTER the match: the snapshot-install callback delivering the configuration and the
   * catch-up task are not ordered. A match read at the snapshot index still follows a join the snapshot contains.
   */
  @Test
  void aJoinObservedAfterTheMatchStillCountsTheMatchWhenTheMatchIsPastIt() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), List.of(), 1);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), 20);

    detector.onSecurityDocumentsMatchedLeader(50);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), List.of(), 30);

    assertThat(detector.joinIndex()).isEqualTo(30L);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

  // Regression for #8346: a security document the leader reports as matching counts as converged
  /** A match never moves an install index backwards. */
  @Test
  void aMatchNeverLowersAnInstallIndex() {
    final RuntimeJoinDetector detector = reAddedAt(new RuntimeJoinDetector(), 30);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, 80);

    detector.onSecurityDocumentsMatchedLeader(50);
    // Re-added at 60: the install at 80 still follows it, the match at 50 does not.
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), 55);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), peers("arcadedb-0", "arcadedb-1"), 60);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("groups", "API tokens");
  }

  // Regression for #8346: a security document the leader reports as matching counts as converged
  /**
   * Persisted like an install (#8329): a restart after compaction replays neither the join nor the catch-up, and
   * without this the converged node would be held for the whole window again on every restart.
   */
  @Test
  void aMatchSurvivesARestart() {
    final File marker = new File(tempDir, "runtime-join.marker");
    final RuntimeJoinDetector before = reAddedAt(new RuntimeJoinDetector(marker, true), 30);
    before.onSecurityDocumentsMatchedLeader(50);

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);

    assertThat(restarted.hasJoinedAtRuntime()).isTrue();
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

  // -----------------------------------------------------------------------------------------------------------

  /** Joined at 10, installed the first membership's documents at 12, removed at 20, re-added at {@code reAdd}. */
  private static RuntimeJoinDetector reAddedAt(final RuntimeJoinDetector detector, final long reAdd) {
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), List.of(), 1);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), peers("arcadedb-0", "arcadedb-1"), 10);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, 12);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, 12);
    detector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, 12);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1"), peers("arcadedb-0", "arcadedb-1", "arcadedb-3"), 20);
    assertThat(detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1"), reAdd)).as("re-added at %d", reAdd).isTrue();
    return detector;
  }

  // Regression for #8414: the Raft server reports the join index that moves forward on a re-add
  /**
   * Issue #8414 (#8382): the security-convergence readiness window is per join, and what tells the gate a new join
   * happened is {@code HAServerPlugin.getRuntimeJoinIndex()}. These tests pin the Raft implementation of that signal:
   * it is the detector's join index, it moves forward on a re-add and on a snapshot install that moves the join
   * boundary, and a plugin without a Raft server reports none.
   * <p>
   * Before any join the server reports none; the join and the re-add each move it forward.
   */
  @Test
  void theServerReportsTheJoinIndexAndARemoveAndReAddMovesItForward() {
    final RaftHAServer server = detachedServer();
    final RuntimeJoinDetector detector = server.getStateMachine().getRuntimeJoinDetector();
    assertThat(server.getRuntimeJoinIndex()).as("no join yet").isEqualTo(-1L);

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 5L);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 10L);
    assertThat(server.getRuntimeJoinIndex()).as("the first join").isEqualTo(10L);

    // Removed, then re-added: the re-add's joint entry moves the join, which is what reopens the gate's window.
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 15L);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 20L);
    assertThat(server.getRuntimeJoinIndex()).as("the re-add").isEqualTo(20L);
  }

  // Regression for #8414: the Raft server reports the join index that moves forward on a re-add
  /** A snapshot install past the join is a join boundary of its own (issue #8353), so it moves the index too. */
  @Test
  void aSnapshotInstallThatMovesTheJoinBoundaryMovesTheIndex() {
    final RaftHAServer server = detachedServer();
    final RuntimeJoinDetector detector = server.getStateMachine().getRuntimeJoinDetector();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), List.of(), 5L);
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2"), 10L);

    detector.onSnapshotInstalledFromLeader(50L);

    assertThat(server.getRuntimeJoinIndex()).isEqualTo(49L);
  }

  // Regression for #8414: the Raft server reports the join index that moves forward on a re-add
  /**
   * No Raft server, no join index. The gate reads this only on an armed reading, and counts only a join index that
   * moves forward, so {@code -1} can never restart its bound.
   */
  @Test
  void aPluginWithoutARaftServerReportsNoJoinIndex() {
    assertThat(new RaftHAPlugin().getRuntimeJoinIndex()).isEqualTo(-1L);
  }

  // -----------------------------------------------------------------------------------------------------------

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "arcadedb-0:2434:2480");
    config.setValue(GlobalConfiguration.HA_RAFT_STORAGE_DIRECTORY, tempDir.getAbsolutePath());
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("arcadedb-0");
    return new RaftHAServer(arcadeServer, config);
  }
}
