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
import com.arcadedb.database.Database;
import com.arcadedb.server.ha.raft.ApplyStall;
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
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The read-your-writes bookmark of a gRPC commit covers the commit, as the one of an HTTP commit does (issue #11742): a commit
 * the cluster acknowledged at index N whose local apply is still behind when the response is written (the wait for it gave
 * up, or the apply failed) must still answer a bookmark of at least N. A client that reads on another follower with it then
 * waits for N, where a bookmark below N lets that read answer without the commit.
 * <p>
 * The apply of the server the client committed on is held still, so the wait of the commit on its own entry runs out
 * deterministically ({@code arcadedb.ha.quorumTimeout} is lowered for it).
 */
class GrpcCommitBookmarkIT extends BaseRaftHATest {
  private static final String TYPE = "CommitBookmark";

  private final int[] grpcPorts = allocateFixturePorts(3);

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(grpcPorts[index]));
    config.setValue("arcadedb.grpc.host", "localhost");
    config.setValue("arcadedb.grpc.reflection.enabled", "false");
    config.setValue("arcadedb.grpc.health.enabled", "false");
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 2000L);

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
  void closeChannel() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  /** A transaction the client opened on a follower: its commit runs on the transaction's own thread, the response is written by another. */
  @Test
  @Timeout(180)
  void theBookmarkOfACommitCoversItWhenTheLocalApplyIsBehind() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    createType(leader);

    final AtomicReference<Metadata> headers = new AtomicReference<>();
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = connect(follower, headers);

    final String txId = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
    stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setLanguage("sql").setCommand("INSERT INTO " + TYPE + " SET k = 'stalled-tx'").setTransaction(tx).build());

    try (final ApplyStall stall = ApplyStall.on(getRaftPlugin(follower))) {
      final CommitTransactionResponse committed = stub.commitTransaction(
          CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build());
      assertThat(committed.getCommitted()).as("the cluster acknowledged the commit").isTrue();

      assertBookmarkCoversTheCommit(leader, follower, headers);
    }
  }

  /** A write a follower forwards to the leader: the follower waits for the leader's entry, and answers when that wait runs out. */
  @Test
  @Timeout(180)
  void theBookmarkOfAForwardedWriteCoversItWhenTheLocalApplyIsBehind() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    createType(leader);

    final AtomicReference<Metadata> headers = new AtomicReference<>();
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = connect(follower, headers);

    try (final ApplyStall stall = ApplyStall.on(getRaftPlugin(follower))) {
      stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setLanguage("sql").setCommand("INSERT INTO " + TYPE + " SET k = 'stalled-forward'").build());

      assertBookmarkCoversTheCommit(leader, follower, headers);
    }
  }

  private void assertBookmarkCoversTheCommit(final int leader, final int follower, final AtomicReference<Metadata> headers) {
    final long leaderApplied = ApplyStall.appliedCounter(getRaftPlugin(leader));
    assertThat(ApplyStall.appliedCounter(getRaftPlugin(follower))).as("guard: the apply of the answering server is behind the commit")
        .isLessThan(leaderApplied);

    final String bookmark = headers.get() != null ? headers.get().get(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER) : null;
    assertThat(bookmark).as("the commit answers its read-your-writes bookmark").isNotNull();
    assertThat(Long.parseLong(bookmark))
        .as("the bookmark covers the entry the cluster acknowledged the commit at (the leader applied it at %d)", leaderApplied)
        .isGreaterThanOrEqualTo(leaderApplied);
  }

  private void createType(final int leader) {
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(TYPE))
        leaderDb.getSchema().createDocumentType(TYPE);
    });
    waitForAllServers();
  }

  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub connect(final int serverIndex, final AtomicReference<Metadata> headers) {
    channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[serverIndex]).usePlaintext().build();
    return ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new AuthClientInterceptor()))
        .withInterceptors(MetadataUtils.newCaptureMetadataInterceptor(headers, new AtomicReference<>()));
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private final class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions,
        final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          super.start(responseListener, headers);
        }
      };
    }
  }
}
