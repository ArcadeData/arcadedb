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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Read-your-writes on a follower for a gRPC client (issue #11948). A gRPC commit answered no commit index and a gRPC
 * query took no read-after index, so a client that writes on the leader and reads on a follower read its own commit
 * back as missing in about one round in five. The commit now answers {@code x-arcadedb-commit-index}, and a read that
 * sends it back as {@code x-arcadedb-read-after} waits until the follower has applied it, as HTTP does.
 */
@Tag("slow")
class Issue11948GrpcReadYourWritesIT extends BaseRaftHATest {
  private static final String TYPE   = "Issue11948";
  private static final int    ROUNDS = 100;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);

  // One gRPC port per server, from the same ledger as the Raft ports so the two can never coincide (issue #8203)
  private final int[]                grpcPorts = allocateFixturePorts(3);
  private final List<ManagedChannel> channels  = new ArrayList<>();

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
    final String plugins = config.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    final String entry = "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin";
    if (plugins == null || plugins.isEmpty())
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, entry);
    else if (!plugins.contains(entry))
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, plugins + "," + entry);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void closeChannels() throws InterruptedException {
    for (final ManagedChannel channel : channels) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    channels.clear();
  }

  @Test
  void aFollowerReadAfterTheCommitBookmarkSeesTheCommit() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    createType(leaderIndex);

    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub leader = stub(leaderIndex);
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub follower = stub(followerIndex);

    long previous = -1;
    for (int i = 0; i < ROUNDS; i++) {
      final String key = "k" + i;
      final AtomicReference<Metadata> headers = new AtomicReference<>();
      final AtomicReference<Metadata> trailers = new AtomicReference<>();
      commitOne(leader.withInterceptors(MetadataUtils.newCaptureMetadataInterceptor(headers, trailers)), key);

      final String bookmark = headers.get() != null ? headers.get().get(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER) : null;
      assertThat(bookmark).as("the commit of round %d answers its read-your-writes bookmark", i).isNotNull();
      final long index = Long.parseLong(bookmark);
      assertThat(index).as("the bookmark of round %d covers the commits before it", i).isGreaterThan(previous);
      previous = index;

      final Metadata readAfter = new Metadata();
      readAfter.put(GrpcReadConsistencyInterceptor.READ_AFTER_HEADER, bookmark);
      readAfter.put(GrpcReadConsistencyInterceptor.READ_CONSISTENCY_HEADER, "read_your_writes");
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub followerAfter =
          follower.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(readAfter));
      assertThat(count(followerAfter, key))
          .as("a follower read with the bookmark of round %d sees the row committed in it", i).isEqualTo(1);
      // The other two read paths: a streamed query resolves the database on its call's thread, a read inside a client
      // transaction runs on that transaction's own thread, where submitToActiveTransaction applies the consistency
      assertThat(countStreamed(followerAfter, key))
          .as("a streamed follower read with the bookmark of round %d sees the row committed in it", i).isEqualTo(1);
      assertThat(countInTransaction(followerAfter, key))
          .as("a follower read inside a transaction, with the bookmark of round %d, sees the row committed in it", i)
          .isEqualTo(1);
    }
  }

  @Test
  void aBookmarkThatIsNotANumberIsRefused() {
    final int leaderIndex = findLeaderIndex();
    createType(leaderIndex);

    final Metadata readAfter = new Metadata();
    readAfter.put(GrpcReadConsistencyInterceptor.READ_AFTER_HEADER, "not-a-number");
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub follower = stub(leaderIndex == 0 ? 1 : 0)
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(readAfter));

    assertThatThrownBy(() -> count(follower, "any")).isInstanceOfSatisfying(StatusRuntimeException.class,
        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
  }

  @Test
  void theConfiguredConsistencyAppliesToAFollowerReadWithoutHeaders() {
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    createType(leaderIndex);

    // arcadedb.ha.readConsistency is what an HTTP read without headers gets; a gRPC read ignored it
    final ContextConfiguration followerConfig = getServerDatabase(followerIndex, getDatabaseName()).getConfiguration();
    final String before = followerConfig.getValueAsString(GlobalConfiguration.HA_READ_CONSISTENCY);
    followerConfig.setValue(GlobalConfiguration.HA_READ_CONSISTENCY, "linearizable");
    try {
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub leader = stub(leaderIndex);
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub follower = stub(followerIndex);
      for (int i = 0; i < ROUNDS; i++) {
        commitOne(leader, "d" + i);
        assertThat(count(follower, "d" + i))
            .as("a linearizable follower read, the configured default, sees the row committed in round %d", i).isEqualTo(1);
      }
    } finally {
      followerConfig.setValue(GlobalConfiguration.HA_READ_CONSISTENCY, before);
    }
  }

  private void createType(final int leaderIndex) {
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(TYPE))
        leaderDb.getSchema().createDocumentType(TYPE);
    });
    waitForAllServers();
  }

  private void commitOne(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub leader, final String key) {
    final String txId = leader.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
    leader.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setLanguage("sql").setCommand("INSERT INTO " + TYPE + " SET k = '" + key + "'").setTransaction(tx).build());
    assertThat(leader.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build())
        .getCommitted()).isTrue();
  }

  private int count(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String key) {
    final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setLanguage("sql").setQuery("SELECT FROM " + TYPE + " WHERE k = '" + key + "'").build());
    int rows = 0;
    for (final QueryResult result : response.getResultsList())
      rows += result.getRecordsCount();
    return rows;
  }

  private int countStreamed(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String key) {
    final Iterator<QueryResult> results = stub.streamQuery(StreamQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setLanguage("sql").setQuery("SELECT FROM " + TYPE + " WHERE k = '" + key + "'").build());
    int rows = 0;
    while (results.hasNext())
      rows += results.next().getRecordsCount();
    return rows;
  }

  private int countInTransaction(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String key) {
    final String txId = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
    try {
      final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).setLanguage("sql").setQuery("SELECT FROM " + TYPE + " WHERE k = '" + key + "'")
          .setTransaction(tx).build());
      int rows = 0;
      for (final QueryResult result : response.getResultsList())
        rows += result.getRecordsCount();
      return rows;
    } finally {
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build());
    }
  }

  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub(final int serverIndex) {
    final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[serverIndex]).usePlaintext().build();
    channels.add(channel);
    return ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
          final CallOptions callOptions, final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(USER_HEADER, "root");
            headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
            super.start(responseListener, headers);
          }
        };
      }
    }));
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
