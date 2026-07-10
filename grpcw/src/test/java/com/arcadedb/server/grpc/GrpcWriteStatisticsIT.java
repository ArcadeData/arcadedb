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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code ExecuteCommandResponse.stats} carries Cypher write counters for mutating
 * commands and is absent for read-only commands.
 */
public class GrpcWriteStatisticsIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT)
        .usePlaintext()
        .build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
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

  @Test
  void writeCommandCarriesStatsInResponse() {
    final ExecuteCommandRequest writeRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder()
            .setUsername("root")
            .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
            .build())
        .setLanguage("opencypher")
        .setCommand("CREATE (:GrpcStat {name:'x'})-[:REL]->(:GrpcStat2 {name:'y'})")
        .build();

    final ExecuteCommandResponse write = authenticatedStub.executeCommand(writeRequest);

    assertThat(write.hasStats()).isTrue();
    assertThat(write.getStats().getNodesCreated()).isEqualTo(2);
    assertThat(write.getStats().getRelationshipsCreated()).isEqualTo(1);
    assertThat(write.getStats().getPropertiesSet()).isEqualTo(2);
    assertThat(write.getStats().getContainsUpdates()).isTrue();
  }

  @Test
  void readCommandHasNoStatsInResponse() {
    final ExecuteCommandRequest seedRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder()
            .setUsername("root")
            .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
            .build())
        .setLanguage("opencypher")
        .setCommand("CREATE (:GrpcStat {name:'x'})")
        .build();
    authenticatedStub.executeCommand(seedRequest);

    final ExecuteCommandRequest readRequest = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder()
            .setUsername("root")
            .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
            .build())
        .setLanguage("opencypher")
        .setCommand("MATCH (n:GrpcStat) RETURN n")
        .build();

    final ExecuteCommandResponse read = authenticatedStub.executeCommand(readRequest);

    assertThat(read.hasStats()).isFalse();
  }
}
