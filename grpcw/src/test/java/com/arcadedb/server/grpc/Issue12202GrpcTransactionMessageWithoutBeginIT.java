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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12202 (review of PR #12230): an ExecuteCommand whose transaction message names no registered transaction and does not set
 * {@code begin} runs in no transaction of the client, so a standalone transaction control is refused there too. It ran without the
 * auto-commit wrapper and was answered OK.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue12202GrpcTransactionMessageWithoutBeginIT extends BaseGrpcServerTest {

  private ManagedChannel                              channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void open() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
          final CallOptions callOptions, final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
            headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
            headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
            super.start(responseListener, headers);
          }
        };
      }
    }));
  }

  @AfterEach
  void close() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @ParameterizedTest
  @CsvSource({ "sql,BEGIN,false,false", "sql,ROLLBACK,false,true", "sql,COMMIT,true,false", "opencypher,START TRANSACTION,false,false" })
  void aTransactionControlUnderATransactionMessageWithoutBeginIsRefused(final String language, final String command,
      final boolean commit, final boolean rollback) {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setLanguage(language)
        .setCommand(command)
        .setTransaction(TransactionContext.newBuilder().setCommit(commit).setRollback(rollback).build())
        .build();

    assertThatThrownBy(() -> stub.executeCommand(request)).isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
        .hasMessageContaining("RemoteDatabase.begin()");
  }
}
