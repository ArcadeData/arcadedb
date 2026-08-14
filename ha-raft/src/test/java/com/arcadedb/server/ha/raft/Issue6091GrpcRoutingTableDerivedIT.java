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
 * The half of gRPC routing that needs no configuration: with no {@code grpc:} field declared, a peer's gRPC
 * endpoint is its Raft host plus <b>this</b> node's gRPC port (issue #6091). That is correct for a homogeneous
 * deployment - a Kubernetes StatefulSet, where every pod listens on the same port - and knowingly wrong for a
 * heterogeneous one, which is why the field exists and why the fallback logs a one-time WARNING.
 * <p>
 * The assertion below is deliberately written against the port <i>this node</i> was configured with rather than
 * the default, so it fails if the resolver ever stops reading {@link GlobalConfiguration#GRPC_PORT} - the setting
 * the gRPC plugin itself binds. {@link Issue6091GrpcRoutingTableIT} covers the declared case.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6091GrpcRoutingTableDerivedIT extends BaseRaftHATest {

  /** Deliberately not the 50051 default: a test that passes on the default cannot tell it was read at all. */
  private static final int LOCAL_GRPC_PORT = 50081;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // No gRPC plugin is started: the routing table answers from configuration, and a listening socket would only
    // add two ports to bind. What is under test is which port the cluster ADVERTISES.
    config.setValue(GlobalConfiguration.GRPC_PORT, LOCAL_GRPC_PORT);
  }

  @Test
  void undeclaredGrpcEndpointsFallBackToThisNodesGrpcPort() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final HAServerPlugin.RoutingTable table = getRaftPlugin(i)
          .getRoutingTable(HAServerPlugin.ROUTING_PROTOCOL.GRPC);

      assertThat(table).as("server %d must still produce a routing table with nothing declared", i).isNotNull();
      assertThat(table.writer()).as("server %d must derive the leader's endpoint from its own gRPC port", i)
          .isEqualTo("localhost:" + LOCAL_GRPC_PORT);
      assertThat(table.readers()).as("server %d", i).containsExactly("localhost:" + LOCAL_GRPC_PORT);
    }
  }
}
