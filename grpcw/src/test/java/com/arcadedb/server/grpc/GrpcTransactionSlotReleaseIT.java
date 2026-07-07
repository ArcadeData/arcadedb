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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5048 (SEC-7, review follow-up): when the idle reaper reclaims an abandoned transaction it
 * must release the per-principal slot exactly once. Otherwise the per-principal counter would drift (either leaking a
 * slot, so the cap is reached prematurely, or double-releasing, so the cap can be exceeded). After a full reap cycle
 * the principal's live count must be zero and new transactions must be openable again.
 */
@Tag("slow")
public class GrpcTransactionSlotReleaseIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT        = 50051;
  private static final String MAX_CONCURRENT   = "2";
  private static final String MAX_IDLE_MS      = "700";
  private static final String REAPER_PERIOD_MS = "200";

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
    System.setProperty("arcadedb.grpc.maxConcurrentTransactions", MAX_CONCURRENT);
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
    System.clearProperty("arcadedb.grpc.maxConcurrentTransactions");
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

  private String beginTx() {
    return authenticatedStub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build())
        .getTransactionId();
  }

  private ArcadeDbGrpcService grpcService() {
    for (final ServerPlugin plugin : getServer(0).getPlugins())
      if (plugin instanceof GrpcServerPlugin grpcPlugin)
        return grpcPlugin.getService();
    throw new IllegalStateException("GrpcServerPlugin not found");
  }

  @Test
  void reaperReleasesPerPrincipalSlotExactlyOnce() throws Exception {
    final ArcadeDbGrpcService service = grpcService();

    // Fill the cap with abandoned transactions (never committed/rolled back).
    beginTx();
    beginTx();
    assertThat(service.getActiveTransactionCount()).isEqualTo(2);
    assertThat(service.getActiveTransactionCountForPrincipal("root")).isEqualTo(2);

    // Wait for the reaper to reclaim both.
    final long deadline = System.currentTimeMillis() + 30_000L;
    while (service.getActiveTransactionCount() > 0 && System.currentTimeMillis() < deadline)
      Thread.sleep(100);

    assertThat(service.getActiveTransactionCount()).isZero();
    // The per-principal counter must be back to exactly zero (not leaked, not driven negative).
    assertThat(service.getActiveTransactionCountForPrincipal("root")).isZero();

    // Because the slots were released, the principal can open transactions up to the cap again.
    final String a = beginTx();
    final String b = beginTx();
    assertThat(a).isNotEmpty();
    assertThat(b).isNotEmpty();
    assertThat(service.getActiveTransactionCountForPrincipal("root")).isEqualTo(2);

    // cleanup
    authenticatedStub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(a).setDatabase(getDatabaseName()).build()).build());
    authenticatedStub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(b).setDatabase(getDatabaseName()).build()).build());
  }
}
