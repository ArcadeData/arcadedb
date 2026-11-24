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
 * Issue #13253: the idle reaper reclaims a transaction nobody uses, not one whose statement is still running: a statement that outlasts the
 * idle window is the transaction's use, and the transaction must be there when the statement returns.
 */
public class GrpcTransactionRunningStatementReaperIT extends BaseGrpcServerTest {

  // The idle window is shorter than the statement, and the reaper sweeps several times while the statement runs.
  private static final String MAX_IDLE_MS      = "1000";
  private static final String REAPER_PERIOD_MS = "200";
  private static final long   STATEMENT_MS     = 3_000L;

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    System.setProperty("arcadedb.grpc.tx.maxIdleMs", MAX_IDLE_MS);
    System.setProperty("arcadedb.grpc.tx.reaperPeriodMs", REAPER_PERIOD_MS);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
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
          super.start(responseListener, headers);
        }
      };
    }
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private ArcadeDbGrpcService grpcService() {
    for (final ServerPlugin plugin : getServer(0).getPlugins())
      if (plugin instanceof GrpcServerPlugin grpcPlugin)
        return grpcPlugin.getService();
    throw new IllegalStateException("GrpcServerPlugin not found");
  }

  private ExecuteCommandResponse command(final String text, final String txId) {
    return authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setCommand(text)
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build()).build());
  }

  @Test
  void aStatementRunningLongerThanTheIdleWindowKeepsItsTransaction() {
    final ArcadeDbGrpcService service = grpcService();
    final String txId = authenticatedStub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();

    command("INSERT INTO Person SET name = 'before the long statement'", txId);
    command("SLEEP " + STATEMENT_MS, txId);

    assertThat(service.getActiveTransactionCount()).as("the transaction is still registered when its statement returns").isEqualTo(1);
    // the next statement follows at once: the transaction was in use the whole time, so it is not idle
    command("INSERT INTO Person SET name = 'after the long statement'", txId);

    final CommitTransactionResponse commit = authenticatedStub.commitTransaction(CommitTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build()).build());
    assertThat(commit.getCommitted()).as("the commit publishes both inserts").isTrue();

    final ExecuteQueryResponse rows = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setQuery("SELECT FROM Person WHERE name LIKE '% the long statement'").build());
    assertThat(rows.getResultsList().get(0).getRecordsList()).hasSize(2);
  }
}
