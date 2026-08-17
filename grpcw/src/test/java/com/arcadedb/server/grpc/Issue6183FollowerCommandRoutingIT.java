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
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a leader-only refusal can actually arise over gRPC, and where it cannot. Issue #6183 asked whether every
 * gRPC RPC that refuses on a follower should name the leader the way {@code graphBatchLoad} does; the answer is
 * that almost none of them ever refuse. {@code RaftReplicatedDatabase.command} <b>forwards</b> a DDL or otherwise
 * non-idempotent statement to the leader instead of rejecting it, so a schema change sent to a follower over
 * {@code executeCommand} succeeds and replicates. Only {@code graphBatchLoad} refuses outright, deliberately:
 * relaying a bulk load through a follower would double the traffic of the transport chosen to avoid exactly that.
 * <p>
 * That is what makes the {@code ServerIsNotTheLeaderException} handling in {@code GrpcErrorMapper} a consistency
 * guarantee rather than a fix for a routine failure - it covers the leadership change that lands between the
 * forwarding decision and the schema write - and this test pins the premise so a future change that turns
 * forwarding into a refusal is noticed here rather than by a user.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6183FollowerCommandRoutingIT extends BaseRaftHATest {

  private static final int    BASE_RAFT_PORT = 2434;
  private static final int    BASE_HTTP_PORT = 2480;
  private static final int    BASE_GRPC_PORT = 51121;
  private static final String VERTEX_TYPE    = "Issue6183ForwardedType";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    // Object form so each node declares the gRPC port it actually binds: three nodes on one host cannot share
    // one, which is also the shape where deriving a peer's port from this node's would be ambiguous (#6183).
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
  void teardownGrpcClient() throws InterruptedException {
    closeChannel();
  }

  @Test
  void aSchemaChangeSentToAFollowerIsForwardedRatherThanRefused() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = anyFollowerOf(leaderIndex);
    waitForAllServers();

    final ExecuteCommandResponse response = execute("localhost:" + (BASE_GRPC_PORT + followerIndex),
        "CREATE VERTEX TYPE " + VERTEX_TYPE);

    assertThat(response.getSuccess()).as("a follower forwards a DDL to the leader instead of refusing it: %s",
        response.getMessage()).isTrue();

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(VERTEX_TYPE))
          .as("server %d must hold the forwarded type", i).isTrue();
  }

  /**
   * A refused DDL needs the trailers to name the node to go to instead, but that is no longer what sets it apart:
   * every {@code executeCommand} failure - including an ordinary one like a SQL syntax error - now surfaces as a
   * gRPC error status through {@code GrpcErrorMapper}, the same as every other RPC (issue #6192). The
   * {@code success=false} in-band envelope this test used to pin is exactly the defect that issue fixed.
   */
  @Test
  void anOrdinaryCommandFailureSurfacesAsAGrpcError() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    assertThatThrownBy(() -> execute("localhost:" + (BASE_GRPC_PORT + leaderIndex), "SELECTT FROM Nothing"))
        .isInstanceOf(StatusRuntimeException.class);
  }

  private int anyFollowerOf(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        return i;
    throw new IllegalStateException("At least one follower must exist");
  }

  /** Runs one SQL command against the given {@code host:port} gRPC target and returns the response as-is. */
  private ExecuteCommandResponse execute(final String target, final String sql) throws InterruptedException {
    channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    try {
      return ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new AuthClientInterceptor()))
          .withDeadlineAfter(30, TimeUnit.SECONDS)
          .executeCommand(ExecuteCommandRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setLanguage("sql")
              .setCommand(sql)
              .setCredentials(DatabaseCredentials.newBuilder()
                  .setUsername("root")
                  .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
                  .build())
              .build());
    } finally {
      closeChannel();
    }
  }

  private void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  private final class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
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
}
