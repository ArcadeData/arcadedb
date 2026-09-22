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
 * What the once-per-start latch does with each outcome an attempt can have (issue #8034), and what it does when
 * an attempt is never even queued (issue #8087).
 * <p>
 * The latch used to be spent by <b>trying</b>: it was taken when a request was submitted and released only when
 * the dial failed transiently and its own retry budget ran out. Every other exit kept it, including the two that
 * never asked anybody - this node leading by the time the task ran, and the task being interrupted before it
 * dialled. A node that restarted during a failover, queued its catch-up as a replica and then won the election
 * therefore burned its one request on an arm that asks nothing, and stayed out of step for the rest of its life:
 * {@code onFirstLeaderObserved} was a no-op from then on, so the leader change that finally gave it somebody to
 * ask made no request.
 * <p>
 * Issue #8087 closed the two ways an attempt can be spent without ever running at all: {@link
 * SecurityCatchUp#submit} silently doing nothing when it has no server or no Raft server to work with (below),
 * and the executor's queue having no room for a third task while two are already in flight (not exercised here -
 * it needs the executor genuinely saturated by two blocked attempts, which needs a real cluster the way the
 * leader arm itself does; see {@code Issue8034SecurityCatchUpRearmIT}).
 * <p>
 * The leader arm itself is driven end-to-end against a real cluster by {@code Issue8034SecurityCatchUpRearmIT}.
 * What is pinned here is the rule that arm relies on, without a cluster: {@link Outcome#ASKED} is the only
 * outcome that spends the request, and taking the latch is only ever permanent once a task has actually been
 * queued behind it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8034SecurityCatchUpRearmTest {

  @Test
  void theFirstLeaderObservedTakesTheRequestAndTheSecondDoesNot() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      assertThat(catchUp.hasRequestedSinceStart()).as("a fresh node has not asked anybody yet").isFalse();

      catchUp.takeRequestForTests();
      assertThat(catchUp.hasRequestedSinceStart()).isTrue();

      // The whole point of the latch: a re-election on a healthy node must not put an HTTP round trip on a path
      // that already has an election to get through. With the latch already taken, onFirstLeaderObserved's own
      // compare-and-set fails before it ever reaches submit, so nothing here can release it again.
      catchUp.onFirstLeaderObserved(null, null);
      assertThat(catchUp.hasRequestedSinceStart()).isTrue();
    }
  }

  /** An attempt that reached a peer has made the request, whatever the peer answered. */
  @Test
  void anAttemptThatAskedSpendsTheRequest() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      catchUp.takeRequestForTests();

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
      catchUp.takeRequestForTests();
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
      catchUp.takeRequestForTests();
      catchUp.settle(Outcome.NOBODY_TO_ASK);
      assertThat(catchUp.hasRequestedSinceStart()).isFalse();

      catchUp.takeRequestForTests();
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
      catchUp.takeRequestForTests();

      assertThat(catchUp.settle(Outcome.TRANSIENT_FAILURE)).as("a transient failure is retried").isFalse();
      assertThat(catchUp.hasRequestedSinceStart())
          .as("the retry budget still owns the request while it has attempts left")
          .isTrue();
    }
  }

  /** The test seam itself, so a cluster test can start from a known state whatever startup left behind. */
  @Test
  void rearmForTestsPutsTheLatchBackToItsAtStartValue() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      catchUp.takeRequestForTests();
      assertThat(catchUp.hasRequestedSinceStart()).isTrue();

      catchUp.rearmForTests();
      assertThat(catchUp.hasRequestedSinceStart()).isFalse();
    }
  }

  /**
   * Issue #8087, path 1. {@code onFirstLeaderObserved}'s compare-and-set takes the latch before calling {@code
   * submit}, on the assumption that a task is about to be queued behind it. When {@code submit} finds nothing to
   * work with - {@code ArcadeStateMachine.server} can still be null at a leader change - it used to return
   * without queueing anything, leaving the latch taken for the life of the node with no attempt ever having
   * been made. {@code submit} now releases it itself in that case, so the request survives to be made on a
   * later leader change instead.
   */
  @Test
  void onFirstLeaderObservedWithNoServerToAskReleasesTheRequestInsteadOfLeakingIt() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      assertThat(catchUp.hasRequestedSinceStart()).isFalse();

      catchUp.onFirstLeaderObserved(null, null);

      assertThat(catchUp.hasRequestedSinceStart())
          .as("nothing was queued behind the latch, so it must not be left taken with no attempt in flight")
          .isFalse();
    }
  }

  /**
   * Same defect, the snapshot-install trigger's side: it takes the latch unconditionally, up front, before
   * calling the same {@code submit}.
   */
  @Test
  void afterSnapshotInstallWithNoServerToAskReleasesTheRequestInsteadOfLeakingIt() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      assertThat(catchUp.hasRequestedSinceStart()).isFalse();

      catchUp.afterSnapshotInstall(null, null);

      assertThat(catchUp.hasRequestedSinceStart())
          .as("nothing was queued behind the latch, so it must not be left taken with no attempt in flight")
          .isFalse();
    }
  }

  /** And, as above, the release is worth having only because the next trigger then makes the request. */
  @Test
  void aLeaderObservedAfterANoServerReleaseAsksAgain() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      catchUp.onFirstLeaderObserved(null, null);
      assertThat(catchUp.hasRequestedSinceStart()).isFalse();

      catchUp.takeRequestForTests();
      assertThat(catchUp.hasRequestedSinceStart())
          .as("a later leader change, once there is a server and a Raft server to ask through, must not find the "
              + "request already spent")
          .isTrue();
    }
  }
}
