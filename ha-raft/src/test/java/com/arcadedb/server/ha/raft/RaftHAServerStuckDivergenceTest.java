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

import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.RaftHAServer.isStuckDivergedState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link RaftHAServer#isStuckDivergedState} predicate behind the issue #4741
 * stuck-divergence detector. The recovery action it gates is destructive (it reformats the local Raft
 * storage), so the predicate is the highest-risk piece of the fix and is exercised here in isolation -
 * in particular the <em>positive</em> case, which the integration test cannot reproduce deterministically.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAServerStuckDivergenceTest {

  // Argument order: leaderPresent, catchingUp, snapshotPending, currentTerm, appliedTerm, appliedIndex, commitIndex

  @Test
  void firesOnlyForTheDivergenceSignature() {
    // Leader present, not catching up / installing, applied everything it could commit (commit==applied)
    // but at a stale term (currentTerm 5 > appliedTerm 4): this is the stuck-diverged follower.
    assertThat(isStuckDivergedState(true, false, false, 5, 4, 10, 10)).isTrue();
  }

  @Test
  void healthyFollowerAtCurrentTermDoesNotFire() {
    // After the post-election no-op commits and applies, appliedTerm == currentTerm.
    assertThat(isStuckDivergedState(true, false, false, 5, 5, 10, 10)).isFalse();
  }

  @Test
  void normallyLaggingFollowerDoesNotFire() {
    // A follower that simply trails the leader has commit > applied (entries committed, not yet applied),
    // even across a term boundary. It is making progress, not stuck.
    assertThat(isStuckDivergedState(true, false, false, 5, 4, 8, 10)).isFalse();
  }

  @Test
  void leaderlessDoesNotFire() {
    assertThat(isStuckDivergedState(false, false, false, 5, 4, 10, 10)).isFalse();
  }

  @Test
  void catchingUpDoesNotFire() {
    assertThat(isStuckDivergedState(true, true, false, 5, 4, 10, 10)).isFalse();
  }

  @Test
  void snapshotPendingDoesNotFire() {
    assertThat(isStuckDivergedState(true, false, true, 5, 4, 10, 10)).isFalse();
  }

  @Test
  void unreadableStateDoesNotFire() {
    // Any negative value means the Raft state could not be read this tick: never act on it.
    assertThat(isStuckDivergedState(true, false, false, -1, 4, 10, 10)).as("negative currentTerm").isFalse();
    assertThat(isStuckDivergedState(true, false, false, 5, -1, 10, 10)).as("negative appliedTerm").isFalse();
    assertThat(isStuckDivergedState(true, false, false, 5, 4, -1, 10)).as("negative appliedIndex").isFalse();
    assertThat(isStuckDivergedState(true, false, false, 5, 4, 10, -1)).as("negative commitIndex").isFalse();
  }

  @Test
  void sameTermButCommitAheadDoesNotFire() {
    // Edge: same term, commit ahead - still just lag, not divergence.
    assertThat(isStuckDivergedState(true, false, false, 5, 5, 9, 10)).isFalse();
  }
}
