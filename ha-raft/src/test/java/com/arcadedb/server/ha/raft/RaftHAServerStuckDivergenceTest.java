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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.RaftHAServer.effectiveDivergedFollowerRecoveryDurationMs;
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

  // --- Effective reformat window (issue #8375) ---

  @Test
  void divergedRecoveryDurationDefaultsBelowTheLagRecoveryOne() {
    assertThat(GlobalConfiguration.HA_DIVERGED_FOLLOWER_RECOVERY_DURATION_MS.getDefValue()).isEqualTo(20_000L);
    assertThat((Long) GlobalConfiguration.HA_DIVERGED_FOLLOWER_RECOVERY_DURATION_MS.getDefValue())
        .isLessThan((Long) GlobalConfiguration.HA_STALE_FOLLOWER_RECOVERY_DURATION_MS.getDefValue());
  }

  @Test
  void divergedRecoveryDurationDefaultSurvivesTheDefaultFloor() {
    // With every default the configured window is exactly the floor: nothing is silently raised.
    final long configured = (Long) GlobalConfiguration.HA_DIVERGED_FOLLOWER_RECOVERY_DURATION_MS.getDefValue();
    final long electionMax = (Integer) GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getDefValue();
    assertThat(effectiveDivergedFollowerRecoveryDurationMs(configured, electionMax)).isEqualTo(configured);
  }

  @Test
  void divergedRecoveryDurationIsFlooredAtTwiceTheElectionTimeout() {
    // A follower that stops hearing its leader clears the signature within one election timeout; a window shorter
    // than twice that could reformat a node that was only waiting for the election.
    assertThat(effectiveDivergedFollowerRecoveryDurationMs(5_000, 10_000)).isEqualTo(20_000);
    assertThat(effectiveDivergedFollowerRecoveryDurationMs(20_000, 30_000)).as("scales with a WAN-tuned timeout")
        .isEqualTo(60_000);
    assertThat(effectiveDivergedFollowerRecoveryDurationMs(90_000, 10_000)).as("a larger value is kept")
        .isEqualTo(90_000);
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
