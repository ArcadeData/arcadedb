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
package com.arcadedb.server.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import com.arcadedb.server.ha.raft.RaftHAServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #10066: the gRPC admin gate ({@code requireLeader}) named the leader by {@code HAServerPlugin.getLeaderAddress()},
 * unvetted, and the redirect trailers the mapper attaches to the answer were read from the same address. On a cluster whose
 * peers share a host and declare no HTTP port (issue #6191) that is the follower's OWN address, so both the text and the trailer
 * sent the client back to the node that had just refused it. They now name the leader the way the schema refusal does (issue
 * #10060): by its HTTP address when that identifies it, by its server name when it does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10066GrpcAdminRefusalNamesTheLeaderIT extends BaseRaftHATest {

  // As in Issue8109GrpcSecurityLeaderRoutingIT: the Raft and HTTP ports are BaseRaftHATest's own, only the gRPC port is this
  // class's to choose.
  private static final int BASE_HTTP_PORT = 2480;
  private final        int[] grpcPorts     = allocateFixturePorts(3);

  private ManagedChannel channel;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(raftPort(i))
          .append(",http:").append(BASE_HTTP_PORT + i)
          .append(",grpc:").append(grpcPorts[i]).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(grpcPorts[index]));
    config.setValue("arcadedb.grpc.host", "localhost");
    config.setValue("arcadedb.grpc.reflection.enabled", "false");
    config.setValue("arcadedb.grpc.health.enabled", "false");

    final String existingPlugins = config.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    final String pluginEntry = "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin";
    if (existingPlugins == null || existingPlugins.isEmpty())
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, pluginEntry);
    else if (!existingPlugins.contains(pluginEntry))
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, existingPlugins + "," + pluginEntry);
  }

  @AfterEach
  void closeChannelAfterEach() {
    closeChannel();
  }

  @Test
  @Timeout(120)
  void anAdminRefusalOnAFollowerThatCannotTellItsPeersApartNamesTheLeaderByName() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final RaftHAServer raft = getRaftPlugin(follower).getRaftHAServer();
    final Runnable restore = refuseForwardedWritesOn(follower);
    try {
      final StatusRuntimeException refusal = createUserRefusedOn(follower, "issue10066grpcambiguous");
      final String leaderName = raft.getLeaderName();
      assertThat(leaderName).as("the follower must know which node leads").isNotBlank();

      final String named = refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS);
      assertThat(named).as("the redirect trailer must not send the client back to the follower that refused it").isNotNull();
      assertThat(raft.isOwnHttpAddress(named)).as("trailer named '%s'", named).isFalse();
      assertThat(named).isEqualTo(leaderName);
    } finally {
      restore.run();
    }
  }

  @Test
  @Timeout(120)
  void anAdminRefusalOnAFollowerOfAWellConfiguredClusterNamesTheLeadersHttpAddress() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final String leaderHttpAddress = getRaftPlugin(follower).getRaftHAServer().getLeaderHttpAddress();

    final StatusRuntimeException refusal = createUserRefusedOn(follower, "issue10066grpcconfigured");
    assertThat(refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isEqualTo(leaderHttpAddress);
  }

  private StatusRuntimeException createUserRefusedOn(final int follower, final String user) {
    closeChannel();
    channel = ManagedChannelBuilder.forTarget("localhost:" + grpcPorts[follower]).usePlaintext().build();
    final ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub stub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel)
        .withDeadlineAfter(30, TimeUnit.SECONDS);
    final DatabaseCredentials root = DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();

    final StatusRuntimeException refusal = catchThrowableOfType(StatusRuntimeException.class,
        () -> stub.createUser(CreateUserRequest.newBuilder().setCredentials(root).setUser(user).setPassword("issue10066password")
            .build()));
    assertThat(refusal).as("the call must have been refused").isNotNull();
    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(refusal.getTrailers()).isNotNull();
    return refusal;
  }

  private void closeChannel() {
    if (channel != null) {
      channel.shutdown();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      channel = null;
    }
  }
}
