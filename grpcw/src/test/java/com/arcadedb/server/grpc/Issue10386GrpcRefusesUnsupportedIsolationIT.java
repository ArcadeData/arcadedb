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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10386: {@code beginTransaction} accepted {@code SERIALIZABLE} and answered success at {@code REPEATABLE_READ}, the
 * one failure a client cannot detect: it asked for write-skew prevention and was told its transaction started. HTTP, SQL,
 * openCypher and Postgres all refuse a level the engine cannot provide, so gRPC does too, with {@code INVALID_ARGUMENT} and
 * the message the other surfaces use. A value this server does not know - a newer client's enum constant - is refused for
 * the same reason, where it used to begin silently at {@code READ_COMMITTED}.
 * {@code READ_UNCOMMITTED} stays accepted: it is the proto default, every client that leaves the field unset sends it, and
 * the engine gives it a STRONGER level than asked for, which {@code effective_isolation} reports (issue #10322).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10386GrpcRefusesUnsupportedIsolationIT extends BaseGrpcServerTest {

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new AuthClientInterceptor()));
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aSerializableBeginIsRefusedWithInvalidArgument() {
    assertThatThrownBy(() -> stub.beginTransaction(begin().setIsolation(TransactionIsolation.SERIALIZABLE).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
          // The words the other surfaces use: what was asked for, and what the engine provides
          assertThat(e.getStatus().getDescription()).contains("'SERIALIZABLE'").contains("READ_COMMITTED").contains("REPEATABLE_READ");
        });
  }

  @Test
  void aSerializableBeginLeavesNoTransactionBehind() {
    final int before = grpcService().getActiveTransactionCount();

    assertThatThrownBy(() -> stub.beginTransaction(begin().setIsolation(TransactionIsolation.SERIALIZABLE).build()))
        .isInstanceOf(StatusRuntimeException.class);

    assertThat(grpcService().getActiveTransactionCount()).as("a refused begin holds no transaction slot").isEqualTo(before);
  }

  @Test
  void anIsolationThisServerDoesNotKnowIsRefusedToo() {
    // A newer client's enum constant parses as UNRECOGNIZED: starting it at READ_COMMITTED would be the same silent downgrade
    assertThatThrownBy(() -> stub.beginTransaction(begin().setIsolationValue(99).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
          assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
          assertThat(e.getStatus().getDescription()).contains("99").contains("READ_COMMITTED").contains("REPEATABLE_READ");
        });
  }

  @Test
  void theLevelsTheEngineProvidesStillBeginAndReportTheirOwnLevel() {
    assertThat(beginAndRollback(TransactionIsolation.READ_COMMITTED)).isEqualTo(TransactionIsolation.READ_COMMITTED);
    assertThat(beginAndRollback(TransactionIsolation.REPEATABLE_READ)).isEqualTo(TransactionIsolation.REPEATABLE_READ);
  }

  @Test
  void readUncommittedIsTheProtoDefaultAndRunsAtTheStrongerReadCommitted() {
    assertThat(beginAndRollback(TransactionIsolation.READ_UNCOMMITTED)).isEqualTo(TransactionIsolation.READ_COMMITTED);

    // An unset field is the same value, so a client that never heard of isolation keeps beginning
    final BeginTransactionResponse begun = stub.beginTransaction(begin().build());
    try {
      assertThat(begun.getEffectiveIsolation()).isEqualTo(TransactionIsolation.READ_COMMITTED);
    } finally {
      rollback(begun.getTransactionId());
    }
  }

  private ArcadeDbGrpcService grpcService() {
    for (final ServerPlugin plugin : getServer(0).getPlugins())
      if (plugin instanceof GrpcServerPlugin grpcPlugin)
        return grpcPlugin.getService();
    throw new IllegalStateException("GrpcServerPlugin not found");
  }

  private BeginTransactionRequest.Builder begin() {
    return BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials());
  }

  private TransactionIsolation beginAndRollback(final TransactionIsolation requested) {
    final BeginTransactionResponse begun = stub.beginTransaction(begin().setIsolation(requested).build());
    rollback(begun.getTransactionId());
    return begun.getEffectiveIsolation();
  }

  private void rollback(final String txId) {
    stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build())
        .build());
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
}
