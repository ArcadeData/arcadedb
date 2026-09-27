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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8465: the snapshot-install hold #8432 opens on a STATIC member (never armed as a runtime
 * joiner, #7819) lived in memory only, so a restart between the install and the leader's confirmation came back
 * unheld - its log starts at the registered snapshot marker, every configuration it replays names it, and nothing else
 * recorded that its security documents were never confirmed.
 * <p>
 * The hold is now kept in a marker of its own, next to the runtime-join marker but never the same file, so reading it
 * back restores the hold without arming the node, and the file is deleted as soon as the hold is released.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8465SnapshotInstallHoldSurvivesRestartTest {

  private static final RaftPeerId SELF           = RaftPeerId.valueOf("arcadedb-3");
  private static final long       SNAPSHOT_INDEX = 4_999L;

  @TempDir
  File tempDir;

  /** The issue's scenario: install, restart before the leader answered, and the restarted node is held again. */
  @Test
  void aRestartBeforeTheConfirmationRestoresTheHold() {
    final RuntimeJoinDetector before = staticMember(true);
    before.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    assertThat(holdMarker()).as("written the moment the hold opens").exists();
    assertThat(joinMarker()).as("the #7819 contract: a static member is never armed").doesNotExist();

    final RuntimeJoinDetector after = staticMember(true);

    assertThat(after.hasJoinedAtRuntime()).as("restoring the hold does not arm the node").isFalse();
    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(SNAPSHOT_INDEX);
    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall())
        .as("the defect: the restarted node came back unheld")
        .containsExactly("users", "groups", "API tokens");
  }

  /** The once-per-start catch-up releases the restored hold: its match is read at an index at or past the install. */
  @Test
  void theCatchUpAfterTheRestartReleasesItAndDeletesTheMarker() {
    staticMember(true).onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    final RuntimeJoinDetector after = staticMember(true);
    after.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX);

    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
    assertThat(holdMarker()).as("released: nothing left to restore").doesNotExist();
    assertThat(staticMember(true).securityDocumentsNotConfirmedSinceSnapshotInstall())
        .as("a further restart is not held").isEmpty();
  }

  /** A partial confirmation is persisted too, so a restart waits only for the documents still unconfirmed. */
  @Test
  void aPartialConfirmationSurvivesTheRestart() {
    final RuntimeJoinDetector before = staticMember(true);
    before.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    before.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, SNAPSHOT_INDEX + 2);
    before.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, SNAPSHOT_INDEX + 3);
    assertThat(holdMarker()).exists();

    assertThat(staticMember(true).securityDocumentsNotConfirmedSinceSnapshotInstall()).containsExactly("groups");
  }

  /** Confirmed before the restart: the marker was deleted, so the restarted node is not held. */
  @Test
  void aConfirmationBeforeTheRestartLeavesNothingToRestore() {
    final RuntimeJoinDetector before = staticMember(true);
    before.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    before.onSecurityDocumentsMatchedLeader(SNAPSHOT_INDEX);
    assertThat(holdMarker()).doesNotExist();

    final RuntimeJoinDetector after = staticMember(true);
    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
  }

  /** Raft storage that is not persisted across restarts discards the hold, as it does the runtime-join marker. */
  @Test
  void aNonPersistentStorageDiscardsTheHold() {
    staticMember(true).onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);

    final RuntimeJoinDetector after = staticMember(false);

    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
    assertThat(holdMarker()).doesNotExist();
  }

  /** A node armed later is judged by the armed gate: the hold marker is deleted rather than left stale. */
  @Test
  void armingTheNodeDeletesTheHoldMarker() {
    final RuntimeJoinDetector detector = staticMember(true);
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    assertThat(holdMarker()).exists();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-4"), SNAPSHOT_INDEX + 10);

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
    assertThat(holdMarker()).doesNotExist();
    assertThat(joinMarker()).exists();
  }

  /**
   * A crash between arming and deleting the hold marker leaves both markers on disk (review of PR #8477): the restart is
   * armed, and the stale hold marker is dropped rather than leaked for good.
   */
  @Test
  void aHoldMarkerLeftBesideAJoinMarkerIsDroppedOnRestart() throws Exception {
    staticMember(true).onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    Files.writeString(joinMarker().toPath(), "peer=arcadedb-3\njoinIndex=10\n");

    final RuntimeJoinDetector after = new RuntimeJoinDetector(joinMarker(), holdMarker(), true);

    assertThat(after.hasJoinedAtRuntime()).isTrue();
    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(holdMarker()).doesNotExist();
  }

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
    detector.onSnapshotInstalledFromLeader(SNAPSHOT_INDEX);
    assertThat(holdMarker()).exists();

    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-3", "arcadedb-4"),
        peers("arcadedb-0", "arcadedb-1", "arcadedb-2", "arcadedb-4"), SNAPSHOT_INDEX + 10);

    assertThat(detector.hasJoinedAtRuntime()).isTrue();
    assertThat(holdMarker()).as("the join marker never reached the disk").exists();
  }

  /** An unreadable hold marker restores nothing: the node behaves as before issue #8465 rather than failing. */
  @Test
  void aCorruptHoldMarkerRestoresNothing() throws Exception {
    Files.writeString(holdMarker().toPath(), "snapshotInstallIndex=not-a-number\n");

    final RuntimeJoinDetector after = staticMember(true);

    assertThat(after.lastSnapshotInstallIndex()).isEqualTo(-1L);
    assertThat(after.securityDocumentsNotConfirmedSinceSnapshotInstall()).isEmpty();
  }

  /** The server places the hold marker next to the Raft storage directory, apart from the runtime-join marker. */
  @Test
  void theServerPlacesTheHoldMarkerBesideTheRaftStorage() {
    final File storageDir = new File(tempDir, "raft-storage-arcadedb-3");
    final File hold = RaftHAServer.snapshotInstallHoldMarkerFile(storageDir);

    assertThat(hold.getParentFile()).isEqualTo(tempDir.getAbsoluteFile());
    assertThat(hold.getName()).isEqualTo("raft-storage-arcadedb-3.snapshot-install-hold");
    assertThat(hold).isNotEqualTo(RaftHAServer.runtimeJoinMarkerFile(storageDir));
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

  private static List<RaftPeerId> peers(final String... ids) {
    return Arrays.stream(ids).map(RaftPeerId::valueOf).toList();
  }
}
