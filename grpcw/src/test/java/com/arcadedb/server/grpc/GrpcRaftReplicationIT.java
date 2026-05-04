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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4076: massive bulk insert through gRPC must replicate to all
 * followers in a Raft HA cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrpcRaftReplicationIT extends BaseRaftHATest {

  private static final int    BASE_GRPC_PORT = 51051;
  private static final int    ROW_COUNT      = 500;
  private static final String VERTEX_TYPE    = "GrpcReplicatedVertex";

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
    return 2;
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
  void bulkInsertReplicatesToFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });

    waitForAllServers();

    channel = ManagedChannelBuilder
        .forAddress("localhost", BASE_GRPC_PORT + leaderIndex)
        .usePlaintext()
        .maxInboundMessageSize(64 * 1024 * 1024)
        .build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);

    final DatabaseCredentials credentials = DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();

    final List<GrpcRecord> rows = new ArrayList<>(ROW_COUNT);
    for (int i = 0; i < ROW_COUNT; i++) {
      rows.add(GrpcRecord.newBuilder()
          .putProperties("name", GrpcValue.newBuilder().setStringValue("row-" + i).build())
          .putProperties("idx", GrpcValue.newBuilder().setInt32Value(i).build())
          .build());
    }

    final BulkInsertRequest request = BulkInsertRequest.newBuilder()
        .setOptions(InsertOptions.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials)
            .setTargetClass(VERTEX_TYPE)
            .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
            .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
            .setServerBatchSize(100)
            .build())
        .addAllRows(rows)
        .build();

    final InsertSummary summary = stub.bulkInsert(request);
    assertThat(summary.getInserted()).as("All rows must be inserted on the leader").isEqualTo(ROW_COUNT);
    assertThat(summary.getFailed()).isZero();

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      final long count = serverDb.countType(VERTEX_TYPE, true);
      assertThat(count)
          .as("Server %d (leader=%d) must have replicated all %d rows", i, leaderIndex, ROW_COUNT)
          .isEqualTo(ROW_COUNT);
    }
  }

  @Test
  void graphBatchLoadReplicatesToFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final String typeName = "GraphBatchVertex";
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(typeName))
        leaderDb.getSchema().createVertexType(typeName);
    });

    waitForAllServers();

    channel = ManagedChannelBuilder
        .forAddress("localhost", BASE_GRPC_PORT + leaderIndex)
        .usePlaintext()
        .maxInboundMessageSize(64 * 1024 * 1024)
        .build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    final ArcadeDbServiceGrpc.ArcadeDbServiceStub asyncStub = ArcadeDbServiceGrpc.newStub(authenticated);

    final DatabaseCredentials credentials = DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();

    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<GraphBatchResult> result = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    final StreamObserver<GraphBatchChunk> req = asyncStub.graphBatchLoad(new StreamObserver<>() {
      @Override
      public void onNext(final GraphBatchResult value) {
        result.set(value);
      }

      @Override
      public void onError(final Throwable t) {
        failure.set(t);
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }
    });

    final int rows = 200;
    final GraphBatchChunk.Builder firstChunk = GraphBatchChunk.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials);
    for (int i = 0; i < rows; i++) {
      firstChunk.addRecords(GraphBatchRecord.newBuilder()
          .setKind(GraphBatchRecord.Kind.VERTEX)
          .setTypeName(typeName)
          .setTempId("v" + i)
          .putProperties("idx", GrpcValue.newBuilder().setInt32Value(i).build())
          .build());
    }
    req.onNext(firstChunk.build());
    req.onCompleted();

    assertThat(done.await(60, TimeUnit.SECONDS)).as("graphBatchLoad must complete").isTrue();
    assertThat(failure.get()).as("graphBatchLoad must not fail").isNull();
    assertThat(result.get().getVerticesCreated()).isEqualTo(rows);

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      final long count = serverDb.countType(typeName, true);
      assertThat(count)
          .as("Server %d (leader=%d) must have replicated all %d graph batch rows", i, leaderIndex, rows)
          .isEqualTo(rows);
    }
  }
}
