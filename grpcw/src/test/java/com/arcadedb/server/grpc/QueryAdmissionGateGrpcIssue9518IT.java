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
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9518 over gRPC: an RPC the query admission gate does not start is closed with {@code ABORTED}, the status the
 * service gives every retryable failure, before its handler runs; transaction management is never held behind the
 * queries; and every RPC gives its slot back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryAdmissionGateGrpcIssue9518IT extends BaseGrpcServerTest {
  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  private ManagedChannel                                channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
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
    });
    stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void anRpcTheGateDoesNotStartIsAbortedAndTransactionsAreStillManaged() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThatThrownBy(() -> stub.executeQuery(query())).isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.ABORTED));

      // A SERVER-STREAMING RPC IS REFUSED THE SAME WAY, BEFORE ITS FIRST ROW
      final Iterator<QueryResult> stream = stub.streamQuery(StreamQueryRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).setQuery("SELECT 1 AS one").setBatchSize(10).build());
      assertThatThrownBy(stream::hasNext).isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.ABORTED));

      // BEGIN AND ROLLBACK ARE NEVER HELD BEHIND THE QUERIES
      final String txId = stub.beginTransaction(
          BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
      assertThat(txId).isNotEmpty();
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build()).build());
    }

    // EVERY RPC GIVES ITS SLOT BACK: WITH ONE SLOT AND NO WAITING, A LEAKED ONE WOULD REFUSE THE SECOND
    for (int i = 0; i < 2; i++)
      assertThat(stub.executeQuery(query()).getResultsList()).isNotEmpty();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void onlyTheDataRpcsOfTheServiceAreGated() {
    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbServiceGrpc.getExecuteQueryMethod())).isTrue();
    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbServiceGrpc.getExecuteCommandMethod())).isTrue();
    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbServiceGrpc.getStreamQueryMethod())).isTrue();
    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbServiceGrpc.getCommitTransactionMethod())).isTrue();

    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbServiceGrpc.getBeginTransactionMethod())).isFalse();
    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbServiceGrpc.getRollbackTransactionMethod())).isFalse();
    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbServiceGrpc.getInsertStreamMethod())).as("client-streaming").isFalse();
    assertThat(GrpcAdmissionInterceptor.isGated(ArcadeDbAdminServiceGrpc.getPingMethod())).as("admin service").isFalse();
  }

  private ExecuteQueryRequest query() {
    return ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery("SELECT 1 AS one")
        .build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
