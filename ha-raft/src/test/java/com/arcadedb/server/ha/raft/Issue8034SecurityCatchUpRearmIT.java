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

import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8034: the security catch-up must not spend its once-per-start request on an
 * attempt that asked nobody.
 * <p>
 * {@code SecurityCatchUp} takes the request when it is submitted, on the REPLICA branch of the leader-change
 * callback, and the task then delays itself - up to three seconds of jitter, plus
 * {@code RaftHAServer.waitForLocalApply()}, plus the transport backoffs. Leadership moving to this node inside
 * that window is the ordinary sequence when the leader it just observed is the one that failed, which is also
 * why the dial failed: a node that restarts during a failover queues its catch-up as a replica, retries into
 * an unreachable leader, wins the election, and finds itself on the arm that answers "I am the reference, so
 * there is nobody to ask".
 * <p>
 * That arm used to answer without releasing the request, and nothing else re-armed it. So when the node later
 * stepped down and observed a different leader - the one moment at which an answer is actually available -
 * {@code onFirstLeaderObserved} was a no-op, and the node went on serving requests against a user dropped, a
 * group narrowed or a token revoked while it was away. Nothing converges it in the meantime: the three
 * documents live under {@code <server-root>/config/}, outside the database directory, and nothing replicates a
 * leader's copies of them to anybody.
 *
 * <h2>What is asserted, and what is left to its neighbour</h2>
 * The two halves of the sequence, on a real two-node cluster: the leader arm must leave the request UNSPENT,
 * and the step-down must then MAKE it. What the leader does with the request once it arrives - compare
 * fingerprints, seed nothing when they match, seed all three when they do not - is
 * {@code Issue7833SecurityCatchUpIT}'s subject and is not re-asserted here. Keeping the dial's outcome out of
 * the assertions is also what makes this test insensitive to the leader's address: the request is observable
 * at the moment it is submitted.
 * <p>
 * Tagged {@code slow}: it starts a two-node cluster and moves leadership across it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8034SecurityCatchUpRearmIT extends BaseRaftHATest {

  /** A hang detector for the polls below, not a latency bound: nothing here is timed. */
  private static final long WAIT_BUDGET_MS = 60_000L;

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * The regression. A catch-up that ran while this node led asked nobody, so the request survives - and the
   * leader change that puts the node back under somebody who can answer then makes it, instead of finding it
   * already spent.
   * <p>
   * The two roles are pinned rather than taken from whichever node won the first election: the node under test
   * has to LEAD while its catch-up runs and then step down, so letting the election choose would leave the
   * whole sequence dependent on it.
   */
  @Test
  void aCatchUpThatLedInsteadOfAskingAsksTheNextLeaderItObserves() throws Exception {
    final int nodeUnderTest = 0;
    final int theNextLeader = 1;

    makeLeader(nodeUnderTest);

    final SecurityCatchUp catchUp = securityCatchUpOf(nodeUnderTest);

    // Whatever this server's own startup left behind, start from "has not asked anybody yet" - the state the
    // REPLICA branch puts a restarting node in just before it wins the election.
    catchUp.rearmForTests();
    assertThat(catchUp.hasRequestedSinceStart()).isFalse();

    catchUp.onFirstLeaderObserved(getServer(nodeUnderTest), getRaftPlugin(nodeUnderTest).getRaftHAServer());

    waitUntil(() -> !catchUp.hasRequestedSinceStart(),
        "the catch-up ran while this node led, so it asked nobody and must leave the once-per-start request "
            + "unspent");

    // The leader change this node has been waiting for without knowing it: somebody else is now the reference,
    // so the request has an answer available for the first time.
    makeLeader(theNextLeader);

    waitUntil(catchUp::hasRequestedSinceStart,
        "stepping down must make the once-per-start request, not skip it as already made");
  }

  /**
   * The same arm, reached from the other trigger. {@code afterSnapshotInstall} sets the once-per-start request
   * up front and then submits, so a snapshot install whose follow-up request lands on a node that has since
   * won the election used to consume the request as well as its own.
   */
  @Test
  void aSnapshotInstallFollowUpThatLedInsteadOfAskingReleasesTheRequestToo() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);

    final SecurityCatchUp catchUp = securityCatchUpOf(leaderIndex);
    catchUp.rearmForTests();

    catchUp.afterSnapshotInstall(getServer(leaderIndex), getRaftPlugin(leaderIndex).getRaftHAServer());

    waitUntil(() -> !catchUp.hasRequestedSinceStart(),
        "a snapshot-install follow-up that found this node leading asked nobody, so it must not spend the "
            + "once-per-start request either");
  }

  /** Moves leadership to {@code serverIndex}, unless it is already there, and waits for both sides to agree. */
  private void makeLeader(final int serverIndex) throws Exception {
    final int currentLeader = findLeaderIndex();
    assertThat(currentLeader).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);
    if (currentLeader == serverIndex)
      return;

    getRaftPlugin(currentLeader).getRaftHAServer().transferLeadership(peerIdForIndex(serverIndex), 20_000L);
    waitUntil(() -> getRaftPlugin(serverIndex).isLeader() && !getRaftPlugin(currentLeader).isLeader(),
        "leadership must move to server " + serverIndex);
  }

  private SecurityCatchUp securityCatchUpOf(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer().getStateMachine().getSecurityCatchUp();
  }

  /** Polls a condition to a generous deadline; a hang detector, never a latency bound. */
  private static void waitUntil(final BooleanSupplier condition, final String what) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + WAIT_BUDGET_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return;
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting: " + what);
  }
}
