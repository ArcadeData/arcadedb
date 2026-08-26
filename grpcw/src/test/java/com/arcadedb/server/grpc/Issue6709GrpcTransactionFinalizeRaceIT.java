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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * End-to-end regression test for issue #6709: a concurrent commitTransaction/rollbackTransaction/idle-reap
 * can finalize a transaction (removing it from the server's active-transaction map and shutting down its
 * dedicated executor) in the window between a transaction-scoped RPC's resolve and its dispatched task
 * actually running on that executor. Unlike {@link Issue5042ReapedTransactionIT} (which covers the transaction
 * already being gone at resolve time), this drives the RPC through the finalize-races-dispatch window itself
 * and asserts the client still sees {@code FAILED_PRECONDITION}, not an opaque {@code INTERNAL}.
 *
 * <p>The race is made deterministic (no real timing dependency on outcome) by occupying the transaction's
 * single dedicated executor thread with a synthetic blocking task before the RPC's own task can run, then
 * finalizing the transaction (via {@link ArcadeDbGrpcService#finalizeTransactionForTesting}) while the RPC's
 * task is still queued behind it, and only then releasing the blocker.
 */
public class Issue6709GrpcTransactionFinalizeRaceIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

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
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
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

  /**
   * Begins a transaction, occupies its dedicated executor with a blocking task, runs {@code rpcCall} (given
   * the transaction id) on a background thread so it queues behind the blocker, finalizes the transaction
   * while that task is still queued, then releases the blocker so the RPC's task runs against the
   * now-finalized transaction. Returns the {@link StatusRuntimeException} the client observed.
   */
  private StatusRuntimeException raceFinalizeAgainstDispatch(final Function<String, StatusRuntimeException> rpcCall)
      throws Exception {
    final ArcadeDbGrpcService service = grpcService();

    final BeginTransactionResponse begin = authenticatedStub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build());
    final String txId = begin.getTransactionId();
    assertThat(txId).isNotEmpty();

    final var txCtx = service.getActiveTransactionForTesting(txId);
    assertThat(txCtx).as("beginTransaction must register the transaction").isNotNull();

    final CountDownLatch blockerRunning = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    // Occupies the transaction's single dedicated thread so the RPC's own dispatched task queues behind it,
    // giving a deterministic window to finalize the transaction before that task actually runs.
    txCtx.executor.submit(() -> {
      blockerRunning.countDown();
      release.await();
      return null;
    });
    assertThat(blockerRunning.await(5, TimeUnit.SECONDS)).as("blocker must start").isTrue();

    final ExecutorService clientExecutor = Executors.newSingleThreadExecutor();
    try {
      final Future<StatusRuntimeException> rpcResult = clientExecutor.submit(() -> rpcCall.apply(txId));

      // Deterministically wait for the RPC's own task to actually be enqueued behind the blocker, rather
      // than a fixed sleep: since the blocker occupies the executor's single core thread, a second submitted
      // task can only land in the queue, so a non-empty queue proves resolveAuthorizedTransaction already
      // succeeded and submitToActiveTransaction was called - i.e. that finalizing now genuinely races the
      // dispatched task's requireTransactionStillActive check, not the earlier isUnknownSuppliedTransaction
      // check at resolve time (cycle-2 review: a fixed sleep could pass without exercising that path at all).
      final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (txCtx.executor.getQueue().isEmpty()) {
        if (System.nanoTime() > deadlineNanos)
          throw new AssertionError("RPC task was never enqueued behind the blocker within 5s");
        Thread.sleep(5);
      }
      service.finalizeTransactionForTesting(txId);
      release.countDown();

      return rpcResult.get(5, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      clientExecutor.shutdownNow();
    }
  }

  @Test
  void lookupByRidFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
    final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
        catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.lookupByRid(
            LookupByRidRequest.newBuilder()
                .setDatabase(getDatabaseName())
                .setCredentials(credentials())
                .setRid("#1:0")
                .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                .build())));

    assertThat(err).as("lookupByRid must reject the finalize-race, not hang or succeed").isNotNull();
    assertThat(err.getStatus().getCode())
        .as("lookupByRid must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
            + "finalized while its dispatched task is queued")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void updateRecordFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
    final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
        catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.updateRecord(
            UpdateRecordRequest.newBuilder()
                .setDatabase(getDatabaseName())
                .setCredentials(credentials())
                .setRid("#1:0")
                .setPartial(PropertiesUpdate.newBuilder()
                    .putProperties("name", GrpcValue.newBuilder().setStringValue("raced-update").build()).build())
                .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                .build())));

    assertThat(err).as("updateRecord must reject the finalize-race, not hang or succeed").isNotNull();
    assertThat(err.getStatus().getCode())
        .as("updateRecord must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
            + "finalized while its dispatched task is queued")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void deleteRecordFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
    final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
        catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.deleteRecord(
            DeleteRecordRequest.newBuilder()
                .setDatabase(getDatabaseName())
                .setCredentials(credentials())
                .setRid("#1:0")
                .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                .build())));

    assertThat(err).as("deleteRecord must reject the finalize-race, not hang or succeed").isNotNull();
    assertThat(err.getStatus().getCode())
        .as("deleteRecord must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
            + "finalized while its dispatched task is queued")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void executeCommandFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
    final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
        catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.executeCommand(
            ExecuteCommandRequest.newBuilder()
                .setDatabase(getDatabaseName())
                .setCredentials(credentials())
                .setCommand("INSERT INTO Person SET name = 'raced-executeCommand'")
                .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                .build())));

    assertThat(err).as("executeCommand must reject the finalize-race, not hang or succeed").isNotNull();
    assertThat(err.getStatus().getCode())
        .as("executeCommand must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
            + "finalized while its dispatched task is queued")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void bulkInsertFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
    final GrpcRecord record = GrpcRecord.newBuilder()
        .setType("Person")
        .putProperties("name", GrpcValue.newBuilder().setStringValue("raced-bulkInsert").build())
        .build();

    final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
        catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.bulkInsert(
            BulkInsertRequest.newBuilder()
                .setOptions(InsertOptions.newBuilder()
                    .setDatabase(getDatabaseName())
                    .setCredentials(credentials())
                    .setTargetClass("Person")
                    .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
                    .build())
                .addRows(record)
                .setCredentials(credentials())
                .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                .build())));

    assertThat(err).as("bulkInsert must reject the finalize-race, not hang or succeed").isNotNull();
    assertThat(err.getStatus().getCode())
        .as("bulkInsert must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
            + "finalized while its dispatched task is queued - the catch block here changed non-trivially in "
            + "this PR")
        .isEqualTo(Status.Code.FAILED_PRECONDITION);
  }
}
