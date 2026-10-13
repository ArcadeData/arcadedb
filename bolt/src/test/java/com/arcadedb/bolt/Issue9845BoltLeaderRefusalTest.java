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
package com.arcadedb.bolt;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9845: a follower's refusal of leader-only work reached a Bolt client as
 * {@code Neo.TransientError.Transaction.DeadlockDetected}, which a managed-transaction driver retries on the same
 * follower until its budget runs out, and the leader's address was dropped. A refusal that names the leader is
 * Neo4j's {@code Neo.ClientError.Cluster.NotALeader}, which a routing driver re-routes on, and the failure text names
 * the leader. One raised mid-election, naming none, keeps today's retryable answer.
 */
class Issue9845BoltLeaderRefusalTest {

  private static final String LEADER = "arcadedb-0:2480";

  private static ServerIsNotTheLeaderException refusal(final String leader) {
    return new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server", leader);
  }

  @Test
  void aRefusalNamingTheLeaderIsNotALeader() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(refusal(LEADER), BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.NOT_A_LEADER_ERROR)
        .isEqualTo("Neo.ClientError.Cluster.NotALeader");
  }

  @Test
  void aWrappedRefusalNamingTheLeaderIsNotALeader() {
    // The commit path wraps what the replicated database raises
    final Throwable wrapped = new TransactionException("Error on commit", refusal(LEADER));
    assertThat(BoltNetworkExecutor.classifyExecutionError(wrapped, BoltErrorCodes.TRANSACTION_ERROR))
        .isEqualTo(BoltErrorCodes.NOT_A_LEADER_ERROR);
  }

  @Test
  void aRefusalNamingNoLeaderStaysATransientConflict() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(refusal(null), BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(refusal("  "), BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
  }

  @Test
  void aTerminationOutranksTheRefusal() {
    final Throwable terminated = new QueryTerminatedException("terminated");
    terminated.initCause(refusal(LEADER));
    assertThat(BoltNetworkExecutor.classifyExecutionError(terminated, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSACTION_TERMINATED_ERROR);
    assertThat(BoltNetworkExecutor.withLeaderRedirect("terminated", terminated, null)).isEqualTo("terminated");
  }

  @Test
  void anOrdinaryConflictIsUnchanged() {
    final Throwable conflict = new ConcurrentModificationException("retry");
    assertThat(BoltNetworkExecutor.classifyExecutionError(conflict, BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.TRANSIENT_CONFLICT_ERROR);
    assertThat(BoltNetworkExecutor.withLeaderRedirect("retry", conflict, null)).isEqualTo("retry");
  }

  @Test
  void selectingADatabaseOnAFollowerThatRefusesIsNotALeader() {
    assertThat(BoltNetworkExecutor.classifyDatabaseSelectionError(new RuntimeException("open failed", refusal(LEADER))))
        .isEqualTo(BoltErrorCodes.NOT_A_LEADER_ERROR);
  }

  @Test
  void theMessageNamesTheLeaderTheRefusalCarries() {
    assertThat(BoltNetworkExecutor.withLeaderRedirect("Commit error", refusal(LEADER), null))
        .isEqualTo("Commit error. The leader is server '" + LEADER + "' (HTTP address; use its Bolt port). Retry there");
  }

  @Test
  void theMessageNamesTheBoltAddressWhenTheClusterResolvesOne() {
    final HAServerPlugin ha = new RoutingHA(new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT,
        "arcadedb-0:7687", List.of("arcadedb-1:7687")));
    assertThat(BoltNetworkExecutor.withLeaderRedirect("Commit error", refusal(LEADER), ha))
        .isEqualTo("Commit error. Reconnect to the leader at 'arcadedb-0:7687' (Bolt address) and retry");
  }

  @Test
  void theRoutingTableAddressIsStrippedOfControlCharacters() {
    final HAServerPlugin ha = new RoutingHA(new HAServerPlugin.RoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT,
        "arcadedb-0\n:7687", List.of()));
    assertThat(BoltNetworkExecutor.withLeaderRedirect("Commit error", refusal(LEADER), ha)).doesNotContain("\n")
        .contains("'arcadedb-0 :7687'");
  }

  @Test
  void theMessageFallsBackToTheRefusalsAddressWhenTheRoutingTableHasNone() {
    assertThat(BoltNetworkExecutor.withLeaderRedirect("Commit error", refusal(LEADER), new RoutingHA(null)))
        .isEqualTo("Commit error. The leader is server '" + LEADER + "' (HTTP address; use its Bolt port). Retry there");
  }

  @Test
  void theAddressSurvivesConcealment() {
    // In production mode the engine text is replaced by the placeholder: the address is appended to whatever is sent
    assertThat(BoltNetworkExecutor.withLeaderRedirect(ArcadeDBServer.CONCEALED_ERROR_MESSAGE, refusal(LEADER), null))
        .startsWith(ArcadeDBServer.CONCEALED_ERROR_MESSAGE)
        .endsWith("The leader is server '" + LEADER + "' (HTTP address; use its Bolt port). Retry there");
  }

  @Test
  void aRefusalNamingNoLeaderAddsNothingToTheMessage() {
    assertThat(BoltNetworkExecutor.withLeaderRedirect("Commit error", refusal(null), null)).isEqualTo("Commit error");
  }

  /** The smallest HA plugin answering one routing table; nothing else is read. */
  private record RoutingHA(HAServerPlugin.RoutingTable routing) implements HAServerPlugin {
    @Override
    public RoutingTable getRoutingTable(final ROUTING_PROTOCOL protocol) {
      return routing != null && routing.protocol() == protocol ? routing : null;
    }

    @Override
    public void startService() {
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public String getLeaderName() {
      return "arcadedb-0";
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Map.of();
    }

    @Override
    public int getConfiguredServers() {
      return 2;
    }

    @Override
    public String getLeaderAddress() {
      return LEADER;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }
  }
}
