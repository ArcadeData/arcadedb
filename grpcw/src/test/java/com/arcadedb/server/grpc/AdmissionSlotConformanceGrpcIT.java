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
import com.arcadedb.database.Database;
import com.arcadedb.server.AdmissionSlotConformance;
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
 * Issue #10235 over gRPC: a statement that gives its admission slot back before a wait finds the slot on the thread that
 * runs it, with and without a client-managed transaction. Inside one the statement runs on the transaction's own thread,
 * not on the gRPC thread that was admitted (#10140). See {@link AdmissionSlotConformance}.
 */
public class AdmissionSlotConformanceGrpcIT extends BaseGrpcServerTest {
  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final String               TYPE            = "Conformance10235";

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private Database                                        database;

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

    database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateDocumentType(TYPE);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aCommandGivesItsSlotBackFromTheThreadThatRunsIt() throws Exception {
    AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "gRPC executeCommand", () -> {
      assertThat(stub.executeCommand(command(null, "INSERT INTO " + TYPE + " SET id = 1")).getSuccess()).isTrue();
      return null;
    });
  }

  @Test
  void aCommandInsideAClientTransactionGivesItsSlotBackFromTheTransactionThread() throws Exception {
    final String txId = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();

    AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "gRPC executeCommand in a transaction", () -> {
      assertThat(stub.executeCommand(command(txId, "INSERT INTO " + TYPE + " SET id = 2")).getSuccess()).isTrue();
      return null;
    });

    assertThat(stub.commitTransaction(CommitTransactionRequest.newBuilder().setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build()).build())
        .getCommitted()).isTrue();
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  private ExecuteCommandRequest command(final String txId, final String sql) {
    final ExecuteCommandRequest.Builder builder = ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setCommand(sql);
    if (txId != null)
      builder.setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build());
    return builder.build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
