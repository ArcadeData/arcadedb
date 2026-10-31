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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The security catch-up test of {@link HaSecurityMutationRoutingTwoNodeIT}'s family that moves leadership, on the
 * per-test {@link BaseRaftHATest} fixture (two servers, no configuration override). From
 * {@code Issue8034SecurityCatchUpRearmIT}; its other test is in {@link HaSecurityMutationRoutingTwoNodeIT}.
 * <p>
 * <b>#8034</b>: the security catch-up must not spend its once-per-start request on an attempt that asked nobody.
 * {@code SecurityCatchUp} takes the request when it is submitted, on the REPLICA branch of the leader-change callback,
 * and the task then delays itself (up to three seconds of jitter, plus {@code RaftHAServer.waitForLocalApply()}, plus the
 * transport backoffs). Leadership moving to this node inside that window is the ordinary sequence when the leader it just
 * observed is the one that failed: the node queues its catch-up as a replica, retries into an unreachable leader, wins
 * the election, and finds itself on the arm that answers "I am the reference, so there is nobody to ask". That arm used
 * to answer without releasing the request, and nothing else re-armed it, so when the node later stepped down and
 * observed a different leader - the one moment an answer is available - {@code onFirstLeaderObserved} was a no-op, and
 * the node went on serving requests against a user dropped, a group narrowed or a token revoked while it was away. The
 * three documents live under {@code <server-root>/config/}, which nothing replicates.
 * <p>
 * The two halves of the sequence are asserted: the leader arm must leave the request UNSPENT, and the step-down must
 * then MAKE it. What the leader does with the request is #7833's subject. Keeping the dial's outcome out of the
 * assertions also makes the test insensitive to the leader's address: the request is observable when it is submitted.
 * <p>
 * Why not on the shared cluster: the test pins the leader and then transfers leadership away, and it reads a latch whose
 * value a leader change sets. Tagged {@code slow} as its source class was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class HaSecurityMutationRoutingDisruptiveIT extends BaseRaftHATest {

  /** A hang detector for the polls below, not a latency bound: nothing here is timed. */
  private static final long WAIT_BUDGET_MS = 60_000L;

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * Issue #8034, from {@code Issue8034SecurityCatchUpRearmIT}.
   * <p>
   * The regression. A catch-up that ran while this node led asked nobody, so the request survives - and the leader
   * change that puts the node back under somebody who can answer then makes it, instead of finding it already spent.
   * <p>
   * The two roles are pinned rather than taken from whichever node won the first election: the node under test has to
   * LEAD while its catch-up runs and then step down, so letting the election choose would leave the whole sequence
   * dependent on it.
   */
  @Test
  void aCatchUpThatLedInsteadOfAskingAsksTheNextLeaderItObserves() throws Exception {
    final int nodeUnderTest = 0;
    final int theNextLeader = 1;

    makeLeader(nodeUnderTest);

    final SecurityCatchUp catchUp = getRaftPlugin(nodeUnderTest).getRaftHAServer().getStateMachine().getSecurityCatchUp();

    // Whatever this server's own startup left behind, start from "has not asked anybody yet" - the state the
    // REPLICA branch puts a restarting node in just before it wins the election.
    catchUp.rearmForTests();
    assertThat(catchUp.hasRequestedSinceStart()).isFalse();

    catchUp.onFirstLeaderObserved(getServer(nodeUnderTest), getRaftPlugin(nodeUnderTest).getRaftHAServer());

    HaSecurityMutationRoutingTwoNodeIT.waitUntil(() -> !catchUp.hasRequestedSinceStart(), WAIT_BUDGET_MS,
        "the catch-up ran while this node led, so it asked nobody and must leave the once-per-start request "
            + "unspent");

    // The leader change this node has been waiting for without knowing it: somebody else is now the reference,
    // so the request has an answer available for the first time.
    makeLeader(theNextLeader);

    HaSecurityMutationRoutingTwoNodeIT.waitUntil(catchUp::hasRequestedSinceStart, WAIT_BUDGET_MS,
        "stepping down must make the once-per-start request, not skip it as already made");
  }

  /** Moves leadership to {@code serverIndex}, unless it is already there, and waits for both sides to agree. */
  private void makeLeader(final int serverIndex) throws Exception {
    final int currentLeader = findLeaderIndex();
    assertThat(currentLeader).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);
    if (currentLeader == serverIndex)
      return;

    getRaftPlugin(currentLeader).getRaftHAServer().transferLeadership(peerIdForIndex(serverIndex), 20_000L);
    HaSecurityMutationRoutingTwoNodeIT.waitUntil(
        () -> getRaftPlugin(serverIndex).isLeader() && !getRaftPlugin(currentLeader).isLeader(), WAIT_BUDGET_MS,
        "leadership must move to server " + serverIndex);
  }
}
