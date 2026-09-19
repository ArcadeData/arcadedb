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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7549: the peer-capability registry is filled in EVERY role, so a rolling-upgrade readiness check does
 * not have to locate the leader before it can be answered.
 * <p>
 * The monitor was started from {@code startLagMonitor} and stopped from {@code stopLagMonitor}, i.e. it lived
 * exactly as long as a leadership term, because #7219's only consumer - the schema-delta decision - was
 * leader-side. Two consumers have since arrived that are not: the security compare-and-set gate of #7511, which
 * runs on whichever node the client landed on because the group and API-token routes do not forward, and
 * {@code GET /api/v1/cluster}, which is what an operator polls to ask whether the cluster is ready for a group
 * change. On a follower both saw an empty registry - the gate paid a synchronous probe round to work around it,
 * the status document simply omitted the rows.
 * <p>
 * The end-to-end half - a follower's {@code /api/v1/cluster} carrying the {@code capabilities} row of a peer
 * that is not itself - is {@code Issue7549FollowerCapabilityReportingIT}. What is pinned here is the lifecycle
 * it rests on, which needs no cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7549CapabilityMonitorInEveryRoleTest {

  private static final String PEER = "localhost_2435";

  /**
   * The lifecycle change itself: losing leadership ends the lag sampling and does NOT end the capability
   * refresh. Before this, a node that was demoted stopped asking, its answers aged out within
   * {@code ADVERTISEMENT_TTL_MS}, and from then on it could report nothing about any peer but itself.
   */
  @Test
  void losingLeadershipDoesNotStopTheCapabilityRefresh() {
    final RaftHAServer raft = newDetachedServer();
    // Nothing answers, so the rounds this starts cost a failed probe each and record nothing: the assertion is
    // about whether the monitor is still running, not about what it collected.
    raft.setCapabilityProber((peerId, http, https, token) -> {
      throw new IOException("no answer");
    });

    raft.startLagMonitor();
    try {
      assertThat(raft.isCapabilityMonitorRunning()).as("a leader refreshes capabilities").isTrue();

      raft.stopLagMonitor();

      assertThat(raft.isCapabilityMonitorRunning())
          .as("a demoted node keeps refreshing: every role now has a consumer for what it records")
          .isTrue();
    } finally {
      raft.stopCapabilityMonitor();
    }
    assertThat(raft.isCapabilityMonitorRunning()).as("only shutdown ends it").isFalse();
  }

  /**
   * A node that never led records what its peers advertise. Driven through the real
   * {@code refreshPeerCapabilities} round with a stub prober, on a server that is not the leader of anything -
   * {@code configuredPeers()} and the probe are read-only and available in any role, which is the premise the
   * whole change rests on.
   */
  @Test
  void aNodeThatIsNotTheLeaderRecordsWhatItsPeersAdvertise() {
    final RaftHAServer raft = newDetachedServer();
    assertThat(raft.isLeader()).as("nothing here has won an election").isFalse();

    raft.setCapabilityProber((peerId, http, https, token) ->
        new PeerCapabilityQuery.Advertisement(PEER, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA)));

    raft.refreshPeerCapabilities();

    final PeerCapabilityRegistry.Advertisement advertisement = raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER);
    assertThat(advertisement).as("a follower knows what its peer advertises").isNotNull();
    assertThat(advertisement.capabilities()).contains(PeerCapabilities.SCHEMA_DELTA);
    assertThat(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER))
        .as("nothing is unknown about a peer that answered").isNull();
  }

  /**
   * And when the peer does NOT answer, the node that asked has a reason to publish - the field
   * {@code GET /api/v1/cluster} writes since issue #7256, which used to be written only on the leader because
   * only the leader ever had one.
   */
  @Test
  void aNodeThatIsNotTheLeaderAlsoHasAReasonToPublishWhenAPeerDoesNotAnswer() {
    final RaftHAServer raft = newDetachedServer();

    raft.setCapabilityProber((peerId, http, https, token) -> {
      throw new IOException("connection refused");
    });

    raft.refreshPeerCapabilities();

    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER)).isNull();
    assertThat(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER))
        .as("a follower can say WHY a peer is unknown, not merely omit the row")
        .isNotNull()
        .contains("connection refused");
  }

  /**
   * Two callers can start the monitor at once now, and only one executor may result (review of PR #7941).
   * <p>
   * Since the monitor is started from {@code start()} as well as from {@code startLagMonitor}, a node that wins
   * an election immediately - {@code raftServer.start()} does not wait for one, so a single-node bootstrap
   * always does - reaches both from different threads. The guard was a plain check-then-act on a non-volatile
   * field, so both could see null, both create a {@link java.util.concurrent.ScheduledExecutorService}, and the
   * second assignment orphan the first: a daemon thread and its 5-second probe schedule that
   * {@code stopCapabilityMonitor} can no longer reach.
   * <p>
   * Counted in live threads rather than in the field, because the field is exactly what a lost executor is no
   * longer reachable through - the leak is invisible to any assertion made on it.
   */
  @Test
  void concurrentStartsCreateExactlyOneMonitor() throws Exception {
    final RaftHAServer raft = newDetachedServer();
    raft.setCapabilityProber((peerId, http, https, token) -> {
      throw new IOException("no answer");
    });

    final int starters = 8;
    final CountDownLatch go = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(starters);
    for (int i = 0; i < starters; i++) {
      final Thread starter = new Thread(() -> {
        try {
          go.await();
          raft.startCapabilityMonitor();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      }, "issue7549-starter-" + i);
      starter.setDaemon(true);
      starter.start();
    }

    try {
      go.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).as("every starter finished").isTrue();

      assertThat(liveCapabilityMonitorThreads())
          .as("eight concurrent starts must leave one monitor, not eight")
          .isEqualTo(1);
    } finally {
      raft.stopCapabilityMonitor();
    }

    // The one that was created is also the one that can be stopped, which is the half a leaked executor fails.
    Awaitility.await().atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(liveCapabilityMonitorThreads())
            .as("and stopping it leaves none behind").isZero());
  }

  /** How many capability-monitor threads this JVM currently has, by the name the factory gives them. */
  private static long liveCapabilityMonitorThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .filter(t -> "arcadedb-raft-capability-monitor".equals(t.getName()))
        .count();
  }

  private static RaftHAServer newDetachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");
    return new RaftHAServer(mockServer, config);
  }
}
