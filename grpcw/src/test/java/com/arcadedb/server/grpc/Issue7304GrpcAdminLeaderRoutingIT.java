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
 * Issue #7304: the HTTP control plane forwards create/drop database and create/drop user to the
 * leader ({@code PostServerCommandHandler.forwardToLeaderIfReplica}), so those commands never run on
 * a follower. gRPC has no request-proxying equivalent, and before this change its admin service ran
 * them wherever the call landed - which would have taken {@code createUserClusterWide} into
 * {@code HAServerPlugin.replicateSecurityUsers} on a follower.
 * <p>
 * The gRPC gate is therefore a refusal that names the leader, the pattern {@code graphBatchLoad}
 * established on this transport (issues #6091, #6183). This test drives it against a real follower:
 * the refusal must be FAILED_PRECONDITION and must carry the leader address in the
 * {@link LeaderRedirectProtocol} trailers, and the same call against the leader must still succeed -
 * without that positive control, a gate that refused everywhere would pass.
 */
@Tag("slow")
class Issue7304GrpcAdminLeaderRoutingIT extends BaseRaftHATest {

  private static final int BASE_RAFT_PORT = 2434;
  private static final int BASE_HTTP_PORT = 2480;
  private static final int BASE_GRPC_PORT = 51141;

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
      sb.append("localhost:{raft:").append(BASE_RAFT_PORT + i)
          .append(",http:").append(BASE_HTTP_PORT + i)
          .append(",grpc:").append(BASE_GRPC_PORT + i).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(BASE_GRPC_PORT + index));
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
   * A blocking admin stub aimed at one node. Every call here is blocking and has returned by the time
   * the next stub is asked for, so the previous channel is closed rather than left behind - a test
   * that dials two nodes would otherwise leak the first one past the {@code @AfterEach}, which can
   * only see the last.
   */
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
    closeChannel();
    channel = ManagedChannelBuilder.forTarget("localhost:" + (BASE_GRPC_PORT + serverIndex)).usePlaintext().build();
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

  /**
   * Both leader-only mutations, in one cluster: gRPC has no request proxy, so a follower answers
   * FAILED_PRECONDITION and names the leader on the {@link LeaderRedirectProtocol} trailers instead
   * of running the work where the call happened to land. Nothing may be written on any node on the
   * way to the refusal.
   * <p>
   * The two RPCs share a test because each method of this class restarts a three-node cluster, and
   * the assertions are the same gate seen through two entry points rather than two behaviours.
   */
  @Test
  void leaderOnlyAdminRpcsAreRefusedOnAFollowerAndNameTheLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    waitForAllServers();

    final int followerIndex = anyFollowerOf(leaderIndex);
    final String user = "issue7304leaderuser";
    final String database = "issue7304_follower_db";

    final StatusRuntimeException userRefusal = catchStatus(() -> adminStubOn(followerIndex).createUser(
        CreateUserRequest.newBuilder().setCredentials(root()).setUser(user).setPassword("issue7304password").build()));

    assertThat(userRefusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(userRefusal.getTrailers()).isNotNull();
    assertThat(userRefusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS))
        .as("the refusal must name where to go instead").isNotBlank();

    final StatusRuntimeException databaseRefusal = catchStatus(() -> adminStubOn(followerIndex).createDatabase(
        CreateDatabaseRequest.newBuilder().setCredentials(root()).setName(database).setType("document").build()));

    assertThat(databaseRefusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(databaseRefusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS)).isNotBlank();

    for (int i = 0; i < getServerCount(); i++) {
      assertThat(getServer(i).getSecurity().existsUser(user))
          .as("server %d must not hold a user the follower refused to create", i).isFalse();
      assertThat(getServer(i).existsDatabase(database))
          .as("server %d must not hold a database the follower refused to create", i).isFalse();
    }
  }

  /**
   * The two controls the refusals need to mean anything: a gate that refused on every node, or one
   * that had widened to reads, would satisfy the test above and fail this one.
   * <p>
   * Reading the control plane is leader-agnostic on both transports - {@code GET /databases} answers
   * from whichever node is asked - so {@code ListDatabases} must still answer on a follower.
   */
  @Test
  void theSameCallSucceedsOnTheLeaderWhileReadsStillAnswerOnAFollower() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    waitForAllServers();

    final String user = "issue7304leaderokuser";
    adminStubOn(leaderIndex).createUser(CreateUserRequest.newBuilder().setCredentials(root())
        .setUser(user).setPassword("issue7304password").build());

    assertThat(getServer(leaderIndex).getSecurity().existsUser(user)).isTrue();

    final ListDatabasesResponse response = adminStubOn(anyFollowerOf(leaderIndex))
        .listDatabases(ListDatabasesRequest.newBuilder().setCredentials(root()).build());

    assertThat(response.getDatabasesList()).contains(getDatabaseName());
  }

  private static StatusRuntimeException catchStatus(final Runnable call) {
    return catchThrowableOfType(StatusRuntimeException.class, call::run);
  }
}
