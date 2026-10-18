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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #10140: a gRPC statement inside a client-managed transaction runs on the transaction's own thread, not on the gRPC
 * thread that took its admission slot. The slot must go with it, or the statement's release of it before a wait - on an
 * explicit lock here, on the leader for a follower's forward - is a no-op, and the slot stays held for the whole wait.
 * With the lock, that is a livelock: the waiters hold every slot, and the commit of the lock holder, which is gated, queues
 * behind them.
 */
public class Issue10140GrpcTransactionAdmissionSlotIT extends BaseGrpcServerTest {
  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final String               TYPE            = "Issue10140Locked";
  private static final String               LOCK            = "LOCK TYPE " + TYPE;

  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

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

    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType(TYPE))
      database.getSchema().createDocumentType(TYPE);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    getServerDatabase(0, getDatabaseName()).getConfiguration()
        .setValue(GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT, GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT.getDefValue());
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aStatementWaitingOnAnExplicitLockInsideATransactionGivesItsSlotBack() throws Exception {
    // ONE SLOT, AND A COMMIT THAT FINDS IT TAKEN IS REFUSED AFTER A SHORT WAIT INSTEAD OF HANGING THE TEST
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(2_000L);
    // THE WAITER MUST STILL BE WAITING ON THE LOCK WHEN THE HOLDER COMMITS: WIDE ENOUGH FOR A SLOW RUNNER
    getServerDatabase(0, getDatabaseName()).getConfiguration().setValue(GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT, 60_000L);

    final String holder = begin();
    stub.executeCommand(command(holder, LOCK));
    assertThat(gate.getRunning()).as("the holder's statement is over").isZero();

    final String waiter = begin();
    final CompletableFuture<ExecuteCommandResponse> waiting = CompletableFuture.supplyAsync(
        () -> stub.executeCommand(command(waiter, LOCK)));

    // THE WAITER'S STATEMENT IS ADMITTED AND IS WAITING FOR THE LOCK ON ITS TRANSACTION'S THREAD
    await().atMost(Duration.ofSeconds(30)).until(() -> getServer(0).getRunningQueries().getRunning().stream()
        .anyMatch(q -> LOCK.equals(q.getText())));
    assertThat(waiting).isNotDone();

    // IT GAVE ITS SLOT BACK BEFORE WAITING: BEFORE THE FIX IT HELD IT, ON THE gRPC THREAD, FOR THE WHOLE WAIT
    await().atMost(Duration.ofSeconds(10)).until(() -> gate.getRunning() == 0);

    // SO THE HOLDER'S COMMIT, WHICH IS GATED, IS ADMITTED AND RELEASES THE LOCK
    assertThat(stub.commitTransaction(commit(holder)).getCommitted()).isTrue();
    assertThat(waiting.get(60, TimeUnit.SECONDS).getSuccess()).isTrue();
    assertThat(stub.commitTransaction(commit(waiter)).getCommitted()).isTrue();

    await().atMost(Duration.ofSeconds(10)).until(() -> gate.getRunning() == 0);
  }

  private String begin() {
    return stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
  }

  private ExecuteCommandRequest command(final String txId, final String sql) {
    return ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql)
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build()).build();
  }

  private CommitTransactionRequest commit(final String txId) {
    return CommitTransactionRequest.newBuilder().setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build()).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
