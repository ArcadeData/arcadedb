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
import com.arcadedb.database.Database;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The measurement issue #7339 asks for before {@code arcadedb.ha.grpcMaxConnectionAgeMs} could ever default to a
 * non-zero value.
 * <p>
 * The age window is the only bound that closes a revoked peer which keeps retrying, because gRPC restarts the idle
 * window on every RPC, refused ones included. Its price is that it is unconditional in the other direction too: it
 * recycles the <i>healthy</i> Raft connections on the same period, and a leader's {@code AppendEntries} to a
 * follower is one long-lived stream that a recycle tears down and forces to re-establish. ArcadeDB does not move the
 * Ratis election timers off their defaults, so "does a re-establish cost less than an election timeout" is a
 * question about a running cluster rather than about the setting.
 * <p>
 * This test answers it at a period far more aggressive than anything an operator would configure: a 3-node cluster
 * with a 3 s age and a 1 s grace, i.e. roughly one recycle per connection every 3 s, writing continuously across at
 * least four recycle periods. It asserts what a recycle must not cost - a lost write, a divergent replica, or a
 * leadership change - and it is a regression test for the wiring as much as a measurement.
 * <p>
 * What it does <b>not</b> establish is the right production value: loopback with three nodes and one database is
 * the friendliest case there is. That is why {@code arcadedb.ha.grpcMaxConnectionAgeMs} still defaults to 0 and the
 * decision is left with the operator, who should start well above their election timeout.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class Issue7339RaftConnectionAgeRecyclingIT extends BaseRaftHATest {

  private static final String TYPE = "Issue7339";

  /** gRPC jitters this by +/-10% per connection, so the recycles do not line up across the cluster. */
  private static final long AGE_MS   = 3_000L;
  private static final long GRACE_MS = 1_000L;

  /** Long enough to span at least four recycle periods on every connection, jitter included. */
  private static final long WRITE_WINDOW_MS = 14_000L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS, AGE_MS);
    config.setValue(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_GRACE_MS, GRACE_MS);
  }

  @Test
  void replicationSurvivesConnectionsBeingRecycledUnderneathIt() throws InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final long termAtStart = raftTerm(leaderIndex);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(TYPE))
        leaderDb.getSchema().createVertexType(TYPE);
    });

    // One transaction at a time, on the clock rather than on a count: the point is to keep the AppendEntries
    // stream busy for longer than several recycle periods, not to move a particular number of records.
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WRITE_WINDOW_MS);
    long written = 0;
    while (System.nanoTime() < deadline) {
      final long id = written++;
      leaderDb.transaction(() -> leaderDb.newVertex(TYPE).set("id", id).save());
      Thread.sleep(20);
    }

    assertThat(written)
        .as("the write window has to have produced enough traffic to span several recycle periods")
        .isGreaterThan(100);

    // Every write the leader accepted has to be on every replica: a connection recycled underneath an in-flight
    // AppendEntries must cost a retry, never an entry.
    for (int i = 0; i < getServerCount(); i++)
      assertThat(awaitCountOn(i, TYPE, written))
          .as("server %d must hold every record written across the recycles", i)
          .isEqualTo(written);

    assertClusterConsistency();

    // The re-establish has to cost less than an election timeout, which is exactly what a term bump would say it
    // did not. This is the assertion that would have to be relaxed before a non-zero default could be considered.
    assertThat(findLeaderIndex())
        .as("recycling connections must not hand leadership around")
        .isEqualTo(leaderIndex);
    assertThat(raftTerm(leaderIndex))
        .as("a recycle that cost an election would show up as a new Raft term")
        .isEqualTo(termAtStart);
  }

  /**
   * The other half of the measurement, and what stops the one above from passing on a cluster where the setting
   * never reached the builder: the listener of a node that is right now leading a 3-node cluster does close a
   * connection at the age window, and the GOAWAY names that window rather than the idle one.
   * <p>
   * The probe connects over loopback, which {@code PeerAddressAllowlistFilter.transportReady} admits without
   * registering a session, so it is a connection to the real listener that cannot disturb the cluster's own.
   */
  @Test
  void theLiveListenerClosesAConnectionAtTheAgeWindow() throws IOException, InterruptedException {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int raftPort = getServer(leaderIndex).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.HA_RAFT_PORT);

    try (final Http2ConnectionProbe probe = Http2ConnectionProbe.connectTo("localhost", raftPort)) {
      // A drumbeat rather than a quiet wait, so this also covers the case the issue is about: the connection is
      // never idle, and only the age window can close it. The default idle window is five minutes, far beyond
      // this budget, so a max_idle here would itself be a finding.
      probe.drumUntilClosed(60_000L, 100L);

      assertThat(probe.beats()).as("the drumbeat has to have actually opened streams").isGreaterThan(10);
      assertThat(probe.closeReason())
          .as("the fully assembled listener of a live cluster node must close a busy connection at "
              + "arcadedb.ha.grpcMaxConnectionAgeMs. %s", probe.timing())
          .isEqualTo("max_age");
    }
  }

  private long raftTerm(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer().getCurrentTerm();
  }
}
