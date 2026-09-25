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
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8329: the runtime-join detector of issue #7819 was in memory only, so a peer added at
 * runtime that restarted after the configuration entry adding it had been compacted into a Ratis snapshot observed
 * only configurations that already contained it, came back unarmed, and reported READY while enforcing the
 * credentials, groups and API tokens of its own config directory.
 * <p>
 * The restart is modelled by what a restarted node observes after that compaction: every configuration names it,
 * and no joint entry adds it. Those are exactly the sequences that must not arm a fresh detector (issue #7819's
 * static member), so only persisted state can tell the restarted joiner apart.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8329RuntimeJoinMarkerTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("arcadedb-3");

  @TempDir
  File tempDir;

  // -----------------------------------------------------------------------------------------------------------
  // The detector
  // -----------------------------------------------------------------------------------------------------------

  /** Without persisted state the restart after compaction is indistinguishable from a static member: the gap. */
  @Test
  void aRestartAfterCompactionIsInvisibleToAnInMemoryDetector() {
    final RuntimeJoinDetector restarted = new RuntimeJoinDetector();

    observeTheSnapshotOfACompactedLog(restarted);

    assertThat(restarted.hasJoinedAtRuntime()).as("the premise the marker exists for").isFalse();
  }

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

  /** A static member never arms, so it never writes a marker for a later run to misread. */
  @Test
  void aStaticMemberWritesNoMarker() {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, true);

    observeTheSnapshotOfACompactedLog(detector);

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    assertThat(marker).doesNotExist();
  }

  /** Not restoring means the previous membership no longer describes this node: the marker is discarded. */
  @Test
  void aDetectorThatDoesNotRestoreDiscardsAStaleMarker() throws IOException {
    final File marker = new File(tempDir, "raft-storage-arcadedb-3.joined-at-runtime");
    Files.writeString(marker.toPath(), "peer=arcadedb-3\n");

    final RuntimeJoinDetector detector = new RuntimeJoinDetector(marker, false);

    assertThat(detector.hasJoinedAtRuntime()).isFalse();
    assertThat(marker).doesNotExist();
  }

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

  /** And a server whose node never joined at runtime starts unarmed, as issue #7819 requires. */
  @Test
  void aRaftHAServerOverTheStorageOfAStaticMemberStartsUnarmed() {
    final ContextConfiguration config = configuration(true);

    observeTheSnapshotOfACompactedLog(detachedServer(config).getStateMachine().getRuntimeJoinDetector());

    assertThat(detachedServer(config).hasJoinedClusterAtRuntime()).isFalse();
  }

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
    final File marker = RaftHAServer.runtimeJoinMarkerFile(storageDir);
    assertThat(marker).exists();
    assertThat(marker.getParentFile().getCanonicalFile()).isEqualTo(tempDir.getCanonicalFile());

    FileUtils.deleteRecursively(storageDir);
    assertThat(storageDir).doesNotExist();

    assertThat(marker).exists();
    assertThat(detachedServer(config).hasJoinedClusterAtRuntime()).isTrue();
  }

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
    final ArcadeDBServer arcadeServer = mock(ArcadeDBServer.class);
    when(arcadeServer.getServerName()).thenReturn("arcadedb-0");
    return new RaftHAServer(arcadeServer, config);
  }

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> peers = new ArrayList<>(ids.length);
    for (final String id : ids)
      peers.add(RaftPeerId.valueOf(id));
    return peers;
  }
}
