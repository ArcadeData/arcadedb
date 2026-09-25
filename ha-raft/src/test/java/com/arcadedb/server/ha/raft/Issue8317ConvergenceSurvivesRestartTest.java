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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the interaction of issues #8317 and #8329: the runtime-join marker of #8329 restored only the
 * arm, while #8317 counts a security document as converged only when it was installed by a log entry after the join.
 * A converged joiner restarting after compaction loads its documents from disk rather than replaying them, so it
 * observed no install at all and was held at readiness until the bounded window expired, on every restart, and then
 * reported documents that "never reached this node". The marker now carries the join index and the install indexes.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8317ConvergenceSurvivesRestartTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("arcadedb-3");

  @TempDir
  File tempDir;

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

  /** A marker written before the indexes were persisted says only "armed": every document is awaited, safely. */
  @Test
  void aMarkerWithoutIndexesRestoresTheArmAndAwaitsEveryDocument() throws IOException {
    final File marker = marker();
    Files.writeString(marker.toPath(), "peer=arcadedb-3\narmedAt=1\n");

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);

    assertThat(restarted.hasJoinedAtRuntime()).isTrue();
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

  /** An unreadable marker fails the safe way: armed, nothing counted as installed. */
  @Test
  void aCorruptMarkerRestoresTheArmAndAwaitsEveryDocument() throws IOException {
    final File marker = marker();
    Files.writeString(marker.toPath(), "joinIndex=abc\ninstalled.users=\n");

    final RuntimeJoinDetector restarted = new RuntimeJoinDetector(marker, true);

    assertThat(restarted.hasJoinedAtRuntime()).isTrue();
    assertThat(restarted.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

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

  private static List<RaftPeerId> peers(final String... ids) {
    return Arrays.stream(ids).map(RaftPeerId::valueOf).toList();
  }
}
