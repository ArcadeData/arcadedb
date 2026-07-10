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
package com.arcadedb.server.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.server.ha.raft.BaseRaftHATest;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: a gRPC write issued to a follower must be forwarded to the leader. Before the
 * fix, ArcadeDbGrpcService.getDatabase() called DatabaseContext.init() but never propagated the
 * authenticated user, so RaftReplicatedDatabase.forwardCommandToLeaderViaRaft threw
 * "Cannot forward command to leader: no authenticated user in the current security context"
 * and every write hitting a follower was silently dropped on the client side.
 */
class GrpcFollowerForwardingIT extends BaseRaftHATest {

  private static final int    BASE_GRPC_PORT = 51071;
  private static final String VERTEX_TYPE    = "GrpcFollowerWrite";

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;

  private final class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(BASE_GRPC_PORT + index));
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

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  @Test
  void executeCommandThroughFollowerIsForwardedToLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        followerIndex = i;
        break;
      }
    assertThat(followerIndex).as("At least one follower must exist").isGreaterThanOrEqualTo(0);

    // Schema goes through the leader to avoid mixing schema replication with the forwarding under test.
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });
    waitForAllServers();

    // Open the gRPC channel against the follower deliberately.
    channel = ManagedChannelBuilder
        .forAddress("localhost", BASE_GRPC_PORT + followerIndex)
        .usePlaintext()
        .build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);

    final DatabaseCredentials credentials = DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();

    // Write via SQL command - the follower must forward to leader, which requires an authenticated
    // user on the DatabaseContext. Before the fix this surfaced a SecurityException to the caller.
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials)
        .setLanguage("sql")
        .setCommand("CREATE VERTEX " + VERTEX_TYPE + " SET name = 'forwarded'")
        .build();

    final ExecuteCommandResponse response = stub.executeCommand(request);
    assertThat(response).as("Follower-forwarded executeCommand must return a response").isNotNull();

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      assertThat(serverDb.countType(VERTEX_TYPE, true))
          .as("Server %d (leader=%d, write went through follower=%d) must see the forwarded vertex",
              i, leaderIndex, followerIndex)
          .isEqualTo(1);
    }
  }
}
