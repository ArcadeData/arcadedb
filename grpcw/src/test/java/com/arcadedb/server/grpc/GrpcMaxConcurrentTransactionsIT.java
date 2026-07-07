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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5048 (SEC-7): each open gRPC transaction owns a dedicated single-thread executor, so an
 * authenticated client looping beginTransaction() without committing can exhaust threads/memory. The server must cap
 * the number of concurrently open transactions and reject excess beginTransaction() calls with RESOURCE_EXHAUSTED.
 */
public class GrpcMaxConcurrentTransactionsIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final String MAX_CONCURRENT = "2";

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    // The gRPC plugin reads these via ContextConfiguration, which falls back to system properties.
    System.setProperty("arcadedb.grpc.maxConcurrentTransactions", MAX_CONCURRENT);
    // Disable the idle reaper so the open transactions are not reclaimed under the test's feet.
    System.setProperty("arcadedb.grpc.tx.maxIdleMs", "0");
    System.setProperty("arcadedb.grpc.tx.maxAgeMs", "0");
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
    System.clearProperty("arcadedb.grpc.maxConcurrentTransactions");
    System.clearProperty("arcadedb.grpc.tx.maxIdleMs");
    System.clearProperty("arcadedb.grpc.tx.maxAgeMs");
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

  private String beginTx() {
    final BeginTransactionResponse response = authenticatedStub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build());
    return response.getTransactionId();
  }

  private void rollbackTx(final String txId) {
    authenticatedStub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build())
        .build());
  }

  @Test
  void excessConcurrentTransactionsAreRejectedWithResourceExhausted() {
    final List<String> openTxIds = new ArrayList<>();
    try {
      // Open up to the configured cap.
      openTxIds.add(beginTx());
      openTxIds.add(beginTx());
      assertThat(openTxIds).hasSize(2).doesNotContainNull();

      // The next beginTransaction must be rejected with RESOURCE_EXHAUSTED (was: unbounded growth).
      assertThatThrownBy(this::beginTx)
          .isInstanceOfSatisfying(StatusRuntimeException.class,
              e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));

      // Freeing a slot (rollback) must let a new transaction be opened again.
      final String toRelease = openTxIds.remove(0);
      rollbackTx(toRelease);

      final String reopened = beginTx();
      assertThat(reopened).isNotEmpty();
      openTxIds.add(reopened);
    } finally {
      for (final String txId : openTxIds) {
        try {
          rollbackTx(txId);
        } catch (final Exception ignore) {
          // best-effort cleanup
        }
      }
    }
  }
}
