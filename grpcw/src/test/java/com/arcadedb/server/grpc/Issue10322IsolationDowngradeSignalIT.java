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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10322: {@code beginTransaction} maps {@code SERIALIZABLE} to the engine's {@code REPEATABLE_READ} and
 * {@code READ_UNCOMMITTED} to {@code READ_COMMITTED}. The mapping stays (refusing {@code READ_UNCOMMITTED} is not an
 * option, it is the proto default), but it is no longer silent: the response reports the level the transaction really
 * runs at, and a {@code SERIALIZABLE} request - the one that gets a WEAKER guarantee than it asked for - is logged at
 * WARNING, throttled so a client that always asks for it cannot flood the log.
 */
class Issue10322IsolationDowngradeSignalIT extends BaseGrpcServerTest {

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private Logger                                          jul;
  private Level                                           previousLevel;
  private final List<LogRecord>                           published = new CopyOnWriteArrayList<>();
  private final Handler                                   capture   = new Handler() {
    @Override
    public void publish(final LogRecord record) {
      published.add(record);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  };

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new AuthClientInterceptor()));

    // The logger stays at the default INFO: what is asserted is what an operator sees without turning anything on
    jul = Logger.getLogger(ArcadeDbGrpcService.class.getName());
    previousLevel = jul.getLevel();
    jul.setLevel(Level.INFO);
    capture.setLevel(Level.ALL);
    jul.addHandler(capture);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    jul.removeHandler(capture);
    jul.setLevel(previousLevel);
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void theResponseReportsTheLevelTheTransactionReallyRunsAt() {
    assertThat(beginAndRollback(TransactionIsolation.READ_UNCOMMITTED)).isEqualTo(TransactionIsolation.READ_COMMITTED);
    assertThat(beginAndRollback(TransactionIsolation.READ_COMMITTED)).isEqualTo(TransactionIsolation.READ_COMMITTED);
    assertThat(beginAndRollback(TransactionIsolation.REPEATABLE_READ)).isEqualTo(TransactionIsolation.REPEATABLE_READ);
    assertThat(beginAndRollback(TransactionIsolation.SERIALIZABLE)).isEqualTo(TransactionIsolation.REPEATABLE_READ);
  }

  @Test
  void anUnsetIsolationIsReportedAsReadCommitted() {
    final BeginTransactionResponse begun = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build());
    try {
      assertThat(begun.getEffectiveIsolation()).isEqualTo(TransactionIsolation.READ_COMMITTED);
    } finally {
      rollback(begun.getTransactionId());
    }
  }

  @Test
  void aSerializableRequestIsLoggedAsADowngradeAtWarningOncePerWindow() {
    beginAndRollback(TransactionIsolation.SERIALIZABLE);

    final List<LogRecord> first = downgradeWarnings();
    assertThat(first).hasSize(1);
    assertThat(first.getFirst().getMessage()).contains("SERIALIZABLE").contains("REPEATABLE_READ");

    // A client that always asks for SERIALIZABLE must not write one WARNING per transaction
    beginAndRollback(TransactionIsolation.SERIALIZABLE);
    beginAndRollback(TransactionIsolation.SERIALIZABLE);
    assertThat(downgradeWarnings()).hasSize(1);
  }

  @Test
  void aReadUncommittedRequestIsNotLoggedAsADowngrade() {
    // READ_UNCOMMITTED is the proto default, so every client that leaves the field unset sends it, and the engine
    // gives it a STRONGER level than asked for: logging it would be noise on every transaction
    beginAndRollback(TransactionIsolation.READ_UNCOMMITTED);
    beginAndRollback(TransactionIsolation.READ_COMMITTED);
    beginAndRollback(TransactionIsolation.REPEATABLE_READ);

    // Filtered on the downgrade message so an unrelated server WARNING during these begins cannot make this flaky
    assertThat(downgradeWarnings()).isEmpty();
  }

  @Test
  void theDowngradeWarningWindowReopensOnceItHasElapsed() {
    final ArcadeDbGrpcService service = grpcService();
    final long window = ArcadeDbGrpcService.SERIALIZABLE_DOWNGRADE_WARNING_WINDOW_MS;
    final long t0 = 1_000_000L;

    assertThat(service.claimSerializableDowngradeWarning(t0)).isTrue();
    assertThat(service.claimSerializableDowngradeWarning(t0 + 1)).isFalse();
    assertThat(service.claimSerializableDowngradeWarning(t0 + window - 1)).isFalse();
    // Once the window has elapsed the next downgrade is reported at WARNING again, and opens a new window
    assertThat(service.claimSerializableDowngradeWarning(t0 + window)).isTrue();
    assertThat(service.claimSerializableDowngradeWarning(t0 + window + 1)).isFalse();
  }

  @Test
  void concurrentDowngradesInOneWindowLetExactlyOneWarningThrough() throws Exception {
    final ArcadeDbGrpcService service = grpcService();
    final int threads = 16;
    final long now = 5_000_000L;
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicInteger winners = new AtomicInteger();
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++)
        futures.add(pool.submit(() -> {
          start.await();
          if (service.claimSerializableDowngradeWarning(now))
            winners.incrementAndGet();
          return null;
        }));
      start.countDown();
      for (final Future<?> f : futures)
        f.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertThat(winners.get()).isEqualTo(1);
  }

  private ArcadeDbGrpcService grpcService() {
    for (final ServerPlugin plugin : getServer(0).getPlugins())
      if (plugin instanceof GrpcServerPlugin grpcPlugin)
        return grpcPlugin.getService();
    throw new IllegalStateException("GrpcServerPlugin not found");
  }

  private List<LogRecord> downgradeWarnings() {
    return published.stream()
        .filter(r -> r.getLevel() == Level.WARNING && r.getMessage() != null && r.getMessage().contains("SERIALIZABLE"))
        .toList();
  }

  private TransactionIsolation beginAndRollback(final TransactionIsolation requested) {
    final BeginTransactionResponse begun = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
            .setIsolation(requested).build());
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
