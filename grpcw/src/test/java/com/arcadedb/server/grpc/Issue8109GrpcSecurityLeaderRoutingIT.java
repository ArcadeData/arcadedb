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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import com.arcadedb.server.security.ServerSecurity;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #8109 (absorbing #7826): the gRPC {@code CreateUser}, {@code DeleteUser} and {@code UpdateUser} RPCs refuse
 * off-leader ({@code requireLeader}, issue #7304), but {@code SaveGroup}, {@code DeleteGroup}, {@code CreateApiToken}
 * and {@code DeleteApiToken} did not: they ran their cluster-wide security mutation on whichever node the client
 * reached. They are now refused on a follower with the leader's address, like the user RPCs, and the HTTP routes for
 * the same operations forward to the leader.
 */
@Tag("slow")
class Issue8109GrpcSecurityLeaderRoutingIT extends BaseRaftHATest {

  // As in Issue7308GrpcRestoreImportLeaderRoutingIT: the Raft and HTTP ports are BaseRaftHATest's own, only the gRPC
  // port is this class's to choose.
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

  /**
   * All four RPCs in one method, since each method restarts a three-node cluster. The DELETE RPCs aim at state that
   * exists, created through the leader first, so a follower that ran them would visibly remove it.
   */
  @Test
  void groupAndApiTokenRpcsAreRefusedOnAFollowerAndNameTheLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    waitForAllServers();
    final int followerIndex = anyFollowerOf(leaderIndex);

    final String existingGroup = "issue8109grpcgroup";
    adminStubOn(leaderIndex).saveGroup(SaveGroupRequest.newBuilder().setCredentials(root()).setDatabase("*")
        .setName(existingGroup).setGroupJson("{}").build());
    final String existingToken = "issue8109grpctoken";
    assertThat(adminStubOn(leaderIndex).createApiToken(
        CreateApiTokenRequest.newBuilder().setCredentials(root()).setName(existingToken).build()).getToken())
        .isNotBlank();
    final String existingTokenHash = tokenHashOf(getServer(leaderIndex).getSecurity(), existingToken);
    assertThat(existingTokenHash).isNotNull();

    assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).saveGroup(
        SaveGroupRequest.newBuilder().setCredentials(root()).setDatabase("*").setName("issue8109nevergroup")
            .setGroupJson("{}").build())));
    assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).deleteGroup(
        DeleteGroupRequest.newBuilder().setCredentials(root()).setDatabase("*").setName(existingGroup).build())));
    assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).createApiToken(
        CreateApiTokenRequest.newBuilder().setCredentials(root()).setName("issue8109nevertoken").build())));
    assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(followerIndex).deleteApiToken(
        DeleteApiTokenRequest.newBuilder().setCredentials(root()).setTokenHash(existingTokenHash).build())));

    for (int i = 0; i < getServerCount(); i++) {
      final ServerSecurity security = getServer(i).getSecurity();
      assertThat(hasGroup(security, "issue8109nevergroup")).as("server %d must not hold the refused group", i)
          .isFalse();
      assertThat(tokenHashOf(security, "issue8109nevertoken")).as("server %d must not hold the refused token", i)
          .isNull();
    }
    // The two deletes: the state they aimed at is still in force on the leader, the node every follower copies.
    assertThat(hasGroup(getServer(leaderIndex).getSecurity(), existingGroup)).isTrue();
    assertThat(tokenHashOf(getServer(leaderIndex).getSecurity(), existingToken)).isNotNull();
  }

  /** The control: the same RPCs get past the gate on the leader. */
  @Test
  void theSameRpcsSucceedOnTheLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    waitForAllServers();

    assertThat(adminStubOn(leaderIndex).saveGroup(SaveGroupRequest.newBuilder().setCredentials(root())
        .setDatabase("*").setName("issue8109leadergroup").setGroupJson("{}").build()).getSuccess()).isTrue();
    assertThat(adminStubOn(leaderIndex).deleteGroup(DeleteGroupRequest.newBuilder().setCredentials(root())
        .setDatabase("*").setName("issue8109leadergroup").build()).getSuccess()).isTrue();

    assertThat(adminStubOn(leaderIndex).createApiToken(CreateApiTokenRequest.newBuilder().setCredentials(root())
        .setName("issue8109leadertoken").build()).getToken()).isNotBlank();
    final String hash = tokenHashOf(getServer(leaderIndex).getSecurity(), "issue8109leadertoken");
    assertThat(adminStubOn(leaderIndex).deleteApiToken(DeleteApiTokenRequest.newBuilder().setCredentials(root())
        .setTokenHash(hash).build()).getSuccess()).isTrue();
  }

  // ---------------------------------------------------------------------------------------------

  private static boolean hasGroup(final ServerSecurity security, final String name) {
    return security.groupsToJSON().getJSONObject("databases").getJSONObject("*", new JSONObject())
        .getJSONObject("groups", new JSONObject()).has(name);
  }

  private static String tokenHashOf(final ServerSecurity security, final String name) {
    for (final JSONObject token : security.getApiTokenConfiguration().listTokens())
      if (name.equals(token.getString("name", null)))
        return token.getString("tokenHash");
    return null;
  }

  private static void assertRefusedAndNamesTheLeader(final StatusRuntimeException refusal) {
    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(refusal.getTrailers()).isNotNull();
    assertThat(refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS))
        .as("the refusal must name where to go instead").isNotBlank();
  }

  private static StatusRuntimeException catchStatus(final Runnable call) {
    final StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class, call::run);
    assertThat(e).as("the call must have been refused").isNotNull();
    return e;
  }

  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
    closeChannel();
    channel = ManagedChannelBuilder.forTarget("localhost:" + grpcPorts[serverIndex]).usePlaintext().build();
    return ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
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

  private static DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private int anyFollowerOf(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        return i;
    throw new IllegalStateException("At least one follower must exist");
  }
}
