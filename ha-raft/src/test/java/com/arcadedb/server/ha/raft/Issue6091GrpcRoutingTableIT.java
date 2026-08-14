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

import com.arcadedb.server.HAServerPlugin;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cluster can name a peer's client-reachable gRPC endpoint (issue #6091). Before this, HA knew a peer's Raft,
 * HTTP and Bolt addresses but not its gRPC one, so a gRPC RPC refusing work that only the leader may run could
 * only point at the leader's HTTP address and tell the caller to work out the port mapping itself.
 * <p>
 * The addresses here are <b>declared</b>, with the object-form {@code grpc:} field, which is the case that has to
 * work for a heterogeneous deployment - and the only one an in-process cluster can exercise honestly, since three
 * nodes sharing localhost cannot share a port. The other half - the derive-from-this-node's-port fallback used when
 * nothing is declared - is covered by {@link Issue6183AmbiguousRoutingTest} for the derive itself and by
 * {@link Issue6183AmbiguousRoutingIT} for what a cluster of same-host peers ends up advertising, which is nothing:
 * a derived address that fits every peer identifies none of them (issue #6183).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6091GrpcRoutingTableIT extends BaseRaftHATest {

  private static final int BASE_RAFT = 2434;
  private static final int BASE_HTTP = 2480;
  private static final int BASE_GRPC = 50071;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    // Object form so each node declares its own gRPC port: the nodes share localhost and differ only by port,
    // which is precisely the shape the derive-from-local-port fallback cannot express.
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(BASE_RAFT + i)
          .append(",http:").append(BASE_HTTP + i)
          .append(",grpc:").append(BASE_GRPC + i).append("}");
    }
    return sb.toString();
  }

  @Test
  void everyNodeNamesTheSameLeaderGrpcAddress() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    waitForAllServers();

    final String leaderGrpc = "localhost:" + (BASE_GRPC + leaderIndex);
    final List<String> followerGrpc = new ArrayList<>();
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        followerGrpc.add("localhost:" + (BASE_GRPC + i));

    // Asked of the leader AND of each follower: routing is a fact about the cluster, not about who is answering.
    for (int i = 0; i < getServerCount(); i++) {
      final HAServerPlugin.RoutingTable table = getRaftPlugin(i)
          .getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC);

      assertThat(table).as("server %d must be able to build a gRPC routing table", i).isNotNull();
      assertThat(table.protocol()).isEqualTo(HAServerPlugin.ROUTING_PROTOCOL.GRPC);
      assertThat(table.writer()).as("server %d (leader=%d) must name the leader's declared gRPC port", i, leaderIndex)
          .isEqualTo(leaderGrpc);
      assertThat(table.readers()).as("server %d must name every follower's declared gRPC port", i)
          .containsExactlyInAnyOrderElementsOf(followerGrpc);
    }
  }

  /**
   * Bolt and gRPC resolve through the same code with different inputs, so a protocol reading the other's map is
   * the one failure a shared resolver can introduce that a one-protocol test cannot see. This cluster declares
   * gRPC ports and no Bolt ones, which separates the two completely: gRPC resolves per peer, while Bolt derives
   * every peer to this node's own Bolt port and is therefore suppressed as ambiguous (issue #6183). A Bolt table
   * that came back at all - let alone one equal to the gRPC table - would mean the maps had been crossed.
   */
  @Test
  void theBoltTableIsNotTheGrpcTable() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    final HAServerPlugin.RoutingTable grpc = getRaftPlugin(leaderIndex)
        .getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC);
    final HAServerPlugin.RoutingTable bolt = getRaftPlugin(leaderIndex)
        .getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.BOLT);

    assertThat(grpc.writer()).isEqualTo("localhost:" + (BASE_GRPC + leaderIndex));
    assertThat(bolt).as("three nodes on one host with no bolt: field declared cannot be told apart").isNull();
  }

  /**
   * The writer and the readers must come from one leader snapshot: a table whose writer also appears among its
   * readers would send a client both to and away from the same node, which is what a mid-flight leader change
   * would produce if each side re-read the leader independently.
   */
  @Test
  void theWriterIsNeverAlsoAReader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    for (int i = 0; i < getServerCount(); i++) {
      final HAServerPlugin.RoutingTable table = getRaftPlugin(i)
          .getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC);
      assertThat(table.readers()).as("server %d", i).doesNotContain(table.writer());
      assertThat(table.readers()).as("server %d", i).hasSize(getServerCount() - 1);
    }
  }
}
