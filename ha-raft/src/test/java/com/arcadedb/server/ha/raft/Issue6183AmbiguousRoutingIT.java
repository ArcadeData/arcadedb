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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.HAServerPlugin;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The deployment the derive-from-this-node's-port fallback cannot express: every node on one host, differing by
 * port, with no {@code bolt:} / {@code grpc:} field declared. Each peer then derives to the same address, so the
 * "leader" a follower would advertise is the follower itself - and since issue #6091 put that address on a gRPC
 * refusal's trailers, a caller redirecting itself automatically would dial the node that just refused it, be
 * refused again, and loop.
 * <p>
 * Nothing is advertised instead (issue #6183). Both callers already handle that: Bolt's ROUTE falls back to
 * advertising this node as READ/ROUTE and never as writer, and a gRPC refusal falls back to the leader's HTTP
 * address. The check has to hold for <b>every</b> routing protocol, so both are asserted here - guarding one and
 * not the other would reintroduce exactly the per-protocol divergence #6091 removed.
 * <p>
 * The positive derive - distinct hosts, one shared port, which is what a Kubernetes StatefulSet looks like - is
 * unit-tested in {@link Issue6183AmbiguousRoutingTest} and {@link RaftHAServerAddressParsingTest}, because an
 * in-process cluster cannot produce distinct hosts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6183AmbiguousRoutingIT extends BaseRaftHATest {

  /** Deliberately not the 50051 default: a test that passes on the default cannot tell it was read at all. */
  private static final int LOCAL_GRPC_PORT = 50081;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // No gRPC or Bolt plugin is started: the routing table answers from configuration, and a listening socket
    // would only add ports to bind. What is under test is what the cluster ADVERTISES.
    config.setValue(GlobalConfiguration.GRPC_PORT, LOCAL_GRPC_PORT);
  }

  @Test
  void peersThatCannotBeToldApartAreNotAdvertised() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++)
      for (final HAServerPlugin.ROUTING_PROTOCOL protocol : HAServerPlugin.ROUTING_PROTOCOL.values())
        assertThat(getRaftPlugin(i).getRoutingTable(protocol))
            .as("server %d must advertise no %s routing table it cannot attribute to distinct peers", i, protocol)
            .isNull();
  }
}
