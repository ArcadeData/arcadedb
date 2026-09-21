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

import com.arcadedb.server.ha.raft.SecurityCatchUp.Outcome;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the once-per-start latch does with each outcome an attempt can have (issue #8034).
 * <p>
 * The latch used to be spent by <b>trying</b>: it was taken when a request was submitted and released only
 * when the dial failed transiently and its own retry budget ran out. Every other exit kept it, including the
 * two that never asked anybody - this node leading by the time the task ran, and the task being interrupted
 * before it dialled. A node that restarted during a failover, queued its catch-up as a replica and then won
 * the election therefore burned its one request on an arm that asks nothing, and stayed out of step for the
 * rest of its life: {@code onFirstLeaderObserved} was a no-op from then on, so the leader change that finally
 * gave it somebody to ask made no request.
 * <p>
 * The leader arm itself is driven end-to-end against a real cluster by {@code Issue8034SecurityCatchUpRearmIT}.
 * What is pinned here is the rule that arm relies on, without a cluster: {@link Outcome#ASKED} is the only
 * outcome that spends the request.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8034SecurityCatchUpRearmTest {

  /**
   * {@code submit} returns without queueing anything when it has no server or no Raft server to work with, so
   * the triggers can be driven here for their effect on the latch alone, with no executor, no jitter and no
   * dial in the way.
   */
  private static void observeALeader(final SecurityCatchUp catchUp) {
    catchUp.onFirstLeaderObserved(null, null);
  }

  @Test
  void theFirstLeaderObservedTakesTheRequestAndTheSecondDoesNot() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      assertThat(catchUp.hasRequestedSinceStart()).as("a fresh node has not asked anybody yet").isFalse();

      observeALeader(catchUp);
      assertThat(catchUp.hasRequestedSinceStart()).isTrue();

      // The whole point of the latch: a re-election on a healthy node must not put an HTTP round trip on a path
      // that already has an election to get through.
      observeALeader(catchUp);
      assertThat(catchUp.hasRequestedSinceStart()).isTrue();
    }
  }

  /** An attempt that reached a peer has made the request, whatever the peer answered. */
  @Test
  void anAttemptThatAskedSpendsTheRequest() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      observeALeader(catchUp);

      assertThat(catchUp.settle(Outcome.ASKED)).as("nothing is left to retry once a peer has answered").isTrue();
      assertThat(catchUp.hasRequestedSinceStart())
          .as("the request was made, so the next leader change must not make it again")
          .isTrue();
    }
  }

  /**
   * The regression. An attempt that found nobody to ask - this node leading, no Raft plugin, or an interrupt
   * before the dial - asked nothing, so it must leave the request unspent.
   */
  @Test
  void anAttemptThatAskedNobodyReleasesTheRequest() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      observeALeader(catchUp);
      assertThat(catchUp.hasRequestedSinceStart()).isTrue();

      assertThat(catchUp.settle(Outcome.NOBODY_TO_ASK)).as("there is nothing to retry while nobody can answer")
          .isTrue();
      assertThat(catchUp.hasRequestedSinceStart())
          .as("nothing was asked, so the once-per-start request must survive")
          .isFalse();
    }
  }

  /** And the release is worth having only because the next leader change then makes the request. */
  @Test
  void theNextLeaderObservedAfterAReleaseAsksAgain() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      observeALeader(catchUp);
      catchUp.settle(Outcome.NOBODY_TO_ASK);

      observeALeader(catchUp);
      assertThat(catchUp.hasRequestedSinceStart())
          .as("the leader change that finally gives this node somebody to ask must not be a no-op")
          .isTrue();
    }
  }

  /**
   * A transient failure is the one outcome that is retried, so it leaves the latch alone: {@code run} owns it
   * across the retry budget and releases it once the budget is spent.
   */
  @Test
  void aTransientFailureIsRetriedAndDoesNotTouchTheRequest() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      observeALeader(catchUp);

      assertThat(catchUp.settle(Outcome.TRANSIENT_FAILURE)).as("a transient failure is retried").isFalse();
      assertThat(catchUp.hasRequestedSinceStart())
          .as("the retry budget still owns the request while it has attempts left")
          .isTrue();
    }
  }

  /**
   * The snapshot-install trigger takes the same latch up front, so it has the same exposure: a follow-up
   * request that lands on an arm which asks nobody must not consume the once-per-start request either.
   */
  @Test
  void theSnapshotInstallTriggerAlsoReleasesTheRequestWhenItAskedNobody() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      catchUp.afterSnapshotInstall(null, null);
      assertThat(catchUp.hasRequestedSinceStart())
          .as("the snapshot-install trigger counts as the once-per-start request")
          .isTrue();

      catchUp.settle(Outcome.NOBODY_TO_ASK);
      assertThat(catchUp.hasRequestedSinceStart())
          .as("a snapshot install whose follow-up request asked nobody must not spend the request")
          .isFalse();
    }
  }

  /** The test seam itself, so a cluster test can start from a known state whatever startup left behind. */
  @Test
  void rearmForTestsPutsTheLatchBackToItsAtStartValue() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      observeALeader(catchUp);
      assertThat(catchUp.hasRequestedSinceStart()).isTrue();

      catchUp.rearmForTests();
      assertThat(catchUp.hasRequestedSinceStart()).isFalse();
    }
  }
}
