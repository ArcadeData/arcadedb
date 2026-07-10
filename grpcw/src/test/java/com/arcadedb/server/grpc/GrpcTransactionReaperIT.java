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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4802: an abandoned gRPC transaction (the client begins a transaction and then
 * disconnects without committing or rolling back) must be reclaimed by the idle reaper, releasing the dedicated
 * executor thread, the open ArcadeDB transaction and the database reference.
 */
public class GrpcTransactionReaperIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  // Short reaper windows so the test stays fast and deterministic.
  private static final String MAX_IDLE_MS      = "1500";
  private static final String REAPER_PERIOD_MS = "300";

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                       channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub      authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    // The gRPC plugin reads these via ContextConfiguration, which falls back to system properties.
    System.setProperty("arcadedb.grpc.tx.maxIdleMs", MAX_IDLE_MS);
    System.setProperty("arcadedb.grpc.tx.reaperPeriodMs", REAPER_PERIOD_MS);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    System.clearProperty("arcadedb.grpc.tx.maxIdleMs");
    System.clearProperty("arcadedb.grpc.tx.reaperPeriodMs");
  }

  private class AuthClientInterceptor implements ClientInterceptor {
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

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private ArcadeDbGrpcService grpcService() {
    for (final ServerPlugin plugin : getServer(0).getPlugins()) {
      if (plugin instanceof GrpcServerPlugin grpcPlugin)
        return grpcPlugin.getService();
    }
    throw new IllegalStateException("GrpcServerPlugin not found");
  }

  @Test
  void abandonedTransactionIsReaped() throws Exception {
    final ArcadeDbGrpcService service = grpcService();

    // Begin a transaction the way a client would, then write inside it but never commit/rollback.
    final BeginTransactionRequest beginRequest = BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build();

    final BeginTransactionResponse beginResponse = authenticatedStub.beginTransaction(beginRequest);
    final String txId = beginResponse.getTransactionId();
    assertThat(txId).isNotEmpty();

    final ExecuteCommandRequest insertRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = 'Abandoned " + txId + "'")
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
        .build();
    authenticatedStub.executeCommand(insertRequest);

    // The transaction is now registered and open.
    assertThat(service.getActiveTransactionCount()).isEqualTo(1);

    // Simulate an abandoned client: do not commit, do not rollback, do not touch the transaction again.
    // The reaper must reclaim it once it has been idle past the configured threshold.
    final long deadline = System.currentTimeMillis() + 30_000L;
    while (service.getActiveTransactionCount() > 0 && System.currentTimeMillis() < deadline)
      Thread.sleep(100);

    assertThat(service.getActiveTransactionCount())
        .as("abandoned transaction must be reaped by the idle reaper")
        .isEqualTo(0);

    // A late commit attempt on the reaped id must report it as already gone (idempotent no-op).
    final CommitTransactionRequest commitRequest = CommitTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build())
        .build();
    final CommitTransactionResponse commitResponse = authenticatedStub.commitTransaction(commitRequest);
    assertThat(commitResponse.getCommitted()).isFalse();

    // The uncommitted write must not have been persisted.
    final ExecuteQueryRequest queryRequest = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = 'Abandoned " + txId + "'")
        .build();
    final ExecuteQueryResponse queryResponse = authenticatedStub.executeQuery(queryRequest);
    assertThat(queryResponse.getResultsList().getFirst().getRecordsList()).isEmpty();
  }
}
