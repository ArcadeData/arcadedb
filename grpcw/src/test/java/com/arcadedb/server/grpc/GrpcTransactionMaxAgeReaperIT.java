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
 * Regression test for issue #4802 covering the max-age expiry path: with the idle bound disabled, a transaction that
 * is kept continuously busy must still be reaped once it exceeds the configured maximum age. This guarantees an
 * upper bound on how long any single gRPC transaction can hold resources, independent of client activity.
 */
public class GrpcTransactionMaxAgeReaperIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final String MAX_AGE_MS       = "2000";
  private static final String REAPER_PERIOD_MS = "300";

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
    // Disable the idle bound so only the max-age bound can trigger reaping; keep the period short and fast.
    System.setProperty("arcadedb.grpc.tx.maxIdleMs", "0");
    System.setProperty("arcadedb.grpc.tx.maxAgeMs", MAX_AGE_MS);
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
    System.clearProperty("arcadedb.grpc.tx.maxAgeMs");
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
  void busyTransactionIsReapedOnceItExceedsMaxAge() throws Exception {
    final ArcadeDbGrpcService service = grpcService();

    final BeginTransactionResponse beginResponse = authenticatedStub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build());
    final String txId = beginResponse.getTransactionId();
    assertThat(txId).isNotEmpty();
    assertThat(service.getActiveTransactionCount()).isEqualTo(1);

    // Keep the transaction continuously busy (touching lastAccess every iteration). Because the idle bound is
    // disabled, only the max-age bound can reclaim it - which it must, despite the constant activity.
    final TransactionContext txRef = TransactionContext.newBuilder().setTransactionId(txId).build();
    final long deadline = System.currentTimeMillis() + 30_000L;
    while (service.getActiveTransactionCount() > 0 && System.currentTimeMillis() < deadline) {
      try {
        authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand("INSERT INTO Person SET name = 'Busy " + txId + "'")
            .setTransaction(txRef)
            .build());
      } catch (final Exception ignore) {
        // Once the transaction has been reaped, a command on its id is no longer part of that transaction; ignore.
      }
      Thread.sleep(150);
    }

    assertThat(service.getActiveTransactionCount())
        .as("transaction must be reaped once it exceeds the configured max age, even while busy")
        .isEqualTo(0);
  }
}
