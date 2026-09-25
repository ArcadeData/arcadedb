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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8346: a node re-added with its config volume retained, whose log is behind the leader's compaction point,
 * catches up by snapshot install PAST the re-admission seed. The seed entries are never applied on it, and when its
 * retained documents already equal the cluster's the leader's fingerprint comparison writes nothing - so no install
 * after the join index ever happened, and the gate held it for the whole readiness window before logging a
 * misleading "never reached this node". A leader-confirmed match now counts as convergence at the applied index the
 * compared documents were read at.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8346MatchedLeaderCountsAsConvergedTest {

  private static final RaftPeerId SELF = RaftPeerId.valueOf("arcadedb-3");

  @TempDir
  File tempDir;

  /** The issue's scenario: re-added at 30, snapshot installed at 50, the leader reports every document matching. */
  @Test
  void aMatchReadPastTheJoinConvergesEveryDocument() {
    final RuntimeJoinDetector detector = reAddedAt(new RuntimeJoinDetector(), 30);
    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");

    detector.onSecurityDocumentsMatchedLeader(50);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).isEmpty();
  }

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

  /** An applied index that could not be read ({@code -1}) records nothing. */
  @Test
  void anUnknownAppliedIndexRecordsNothing() {
    final RuntimeJoinDetector detector = new RuntimeJoinDetector();
    detector.onConfiguration(SELF, peers("arcadedb-0", "arcadedb-3"), peers("arcadedb-0"));

    detector.onSecurityDocumentsMatchedLeader(-1);

    assertThat(detector.securityDocumentsNotInstalledSinceJoin()).containsExactly("users", "groups", "API tokens");
  }

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

  private static List<RaftPeerId> peers(final String... ids) {
    final List<RaftPeerId> peers = new ArrayList<>(ids.length);
    for (final String id : ids)
      peers.add(RaftPeerId.valueOf(id));
    return peers;
  }
}
