/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.database.Database;

import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7259.
 * <p>
 * A Raft HA test used to be released by {@link BaseRaftHATest#waitAllReplicasAreConnected()} the instant a
 * leader was elected, while the cluster's per-database bootstrap was still ahead of it: {@code
 * BootstrapElection} samples the peers, commits a {@code BOOTSTRAP_FINGERPRINT_ENTRY}, and every peer whose
 * copy does not match the chosen baseline <b>replaces its whole database directory</b> with a full snapshot
 * pulled from the leader. In the run that produced #7259 that sequence started 445 ms after the election and
 * finished 800 ms after it, so {@code BoltFollowerForwardingIT}'s own {@code createVertexType} and its Bolt
 * write both landed inside the window, and one of the two followers came out of it without the type -
 * permanently, and with no error on either side.
 * <p>
 * Each test here pins one half of the gate:
 * <ul>
 *   <li>{@link #bootstrapPassHasReportedAndEveryPeerHasCaughtUpBeforeTheTestBodyRuns()} pins the gate itself,
 *       through the two readings it is built on - the leader's terminal bootstrap outcome, and every started
 *       peer's published applied index. The applied index is the load-bearing one: Ratis publishes it only
 *       after {@code applyTransaction} has RETURNED, so a peer still swapping in a snapshot inside that call
 *       still reads behind.</li>
 *   <li>{@link #aTypeCreatedRightAfterStartupReachesEveryPeer()} pins what the gate buys, which is the shape
 *       {@code BoltFollowerForwardingIT} asserts through Bolt: a type created on the leader immediately after
 *       setup returns is present on every peer, and the databases compare identical.</li>
 * </ul>
 */
class Issue7259ClusterBootstrapSettledIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "Issue7259BootstrapRace";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void bootstrapPassHasReportedAndEveryPeerHasCaughtUpBeforeTheTestBodyRuns() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    assertThat(getRaftPlugin(leaderIndex).getRaftHAServer().getLastBootstrapOutcome())
        .as("""
            The leader's bootstrap pass must have reported a terminal outcome before the test body starts. \
            While it has not, a BOOTSTRAP_FINGERPRINT_ENTRY - and the full-database reinstall applying it can \
            trigger on a follower - is still ahead of anything this test writes (issue #7259)""")
        .isNotNull();

    long highest = -1;
    for (final int i : startedServers()) {
      final TermIndex applied = getRaftPlugin(i).getRaftHAServer().getStateMachine().getLastAppliedTermIndex();
      assertThat(applied).as("Server %d must publish an applied index", i).isNotNull();
      if (applied.getIndex() > highest)
        highest = applied.getIndex();
    }
    for (final int i : startedServers())
      assertThat(getRaftPlugin(i).getRaftHAServer().getStateMachine().getLastAppliedTermIndex().getIndex())
          .as("""
              Server %d must have applied every entry the cluster has applied before the test body starts. \
              Ratis publishes an applied index only once applyTransaction has returned for it, so a peer \
              reading behind here is a peer that may still be replacing its whole database directory from a \
              leader-shipped snapshot (issue #7259)""", i)
          .isGreaterThanOrEqualTo(highest);
  }

  @Test
  void aTypeCreatedRightAfterStartupReachesEveryPeer() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });

    waitForAllServers();

    for (final int i : startedServers())
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(VERTEX_TYPE))
          .as("Server %d (leader=%d) must hold the type created on the leader right after startup", i, leaderIndex)
          .isTrue();

    checkDatabasesAreIdentical();
  }
}
