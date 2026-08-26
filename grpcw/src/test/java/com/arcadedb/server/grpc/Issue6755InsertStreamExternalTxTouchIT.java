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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6755: an {@code insertStream} driven inside a caller-created (external)
 * transaction was reaped mid-stream because {@code lastAccessMs} was refreshed only on the first chunk.
 * A stream whose chunks stayed within the idle window individually, but whose total active duration
 * exceeded it, was reclaimed by the idle-transaction reaper while the client was still sending - discarding
 * every row streamed so far, committed or not, because the external transaction is never actually committed
 * until the caller's own {@code commitTransaction} call runs.
 * <p>
 * Uses tiny {@code arcadedb.grpc.tx.maxIdleMs} / {@code arcadedb.grpc.tx.reaperPeriodMs} values so the
 * reaper's window is exercised in milliseconds instead of the default 5 minutes.
 */
@Tag("slow")
public class Issue6755InsertStreamExternalTxTouchIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  // Idle window shorter than the total time the 4-chunk stream below takes to send (4 x 250ms = 750ms),
  // but longer than the 250ms gap between any two consecutive chunks - so the fix (touching on every
  // chunk) keeps the transaction alive, while the pre-fix behaviour (touch on the first chunk only) lets
  // total idle time accumulate past maxIdleMs and reaps it mid-stream.
  private static final String TX_MAX_IDLE_MS      = "600";
  private static final String TX_REAPER_PERIOD_MS = "100";

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncAuthenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    System.setProperty("arcadedb.grpc.tx.maxIdleMs", TX_MAX_IDLE_MS);
    System.setProperty("arcadedb.grpc.tx.reaperPeriodMs", TX_REAPER_PERIOD_MS);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    asyncAuthenticatedStub = ArcadeDbServiceGrpc.newStub(authenticatedChannel);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    System.clearProperty("arcadedb.grpc.tx.maxIdleMs");
    System.clearProperty("arcadedb.grpc.tx.reaperPeriodMs");
    super.endTest();
  }

  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
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

  private void cmd(final String sql) {
    final ExecuteCommandResponse resp = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build());
    assertThat(resp.getSuccess()).as("setup command failed: %s -> %s", sql, resp.getMessage()).isTrue();
  }

  @Test
  void insertStreamOnExternalTransactionSurvivesGapsShorterThanIdleTimeout() throws Exception {
    final String typeName = "Issue6755Vertex";
    cmd("CREATE VERTEX TYPE " + typeName + " IF NOT EXISTS");
    cmd("CREATE PROPERTY " + typeName + ".k IF NOT EXISTS STRING");

    final BeginTransactionResponse begin = authenticatedStub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build());
    final String txId = begin.getTransactionId();
    assertThat(txId).isNotEmpty();

    final TransactionContext txCtx = TransactionContext.newBuilder()
        .setTransactionId(txId).setDatabase(getDatabaseName()).build();

    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();

    final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
      @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
      @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
      @Override public void onCompleted() { done.countDown(); }
    });

    final int rowCount = 4;
    final InsertOptions options = InsertOptions.newBuilder().setTargetClass(typeName).build();
    // Each gap (250ms) is well under TX_MAX_IDLE_MS (600ms), but the stream's total active duration
    // (750ms across 4 chunks) exceeds it - exactly the scenario #6755 describes.
    for (int i = 0; i < rowCount; i++) {
      final InsertChunk.Builder chunk = InsertChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTransaction(txCtx)
          .setSessionId("issue-6755")
          .setChunkSeq(i)
          .setLast(i == rowCount - 1)
          .addRows(GrpcRecord.newBuilder()
              .setType(typeName)
              .putProperties("k", GrpcValue.newBuilder().setStringValue("row-" + i).build())
              .build());
      if (i == 0)
        chunk.setOptions(options);
      req.onNext(chunk.build());
      if (i < rowCount - 1)
        Thread.sleep(250);
    }
    req.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(errorRef.get()).as("insertStream must not error out").isNull();
    assertThat(summaryRef.get()).isNotNull();
    assertThat(summaryRef.get().getErrorsList())
        .as("no chunk should have failed against a reaped/unknown transaction: %s", summaryRef.get().getErrorsList())
        .isEmpty();
    assertThat(summaryRef.get().getInserted()).as("all rows must have been inserted").isEqualTo(rowCount);

    final CommitTransactionResponse commit = authenticatedStub.commitTransaction(CommitTransactionRequest.newBuilder()
        .setCredentials(credentials())
        .setTransaction(txCtx)
        .build());
    assertThat(commit.getSuccess()).as("the transaction must still be alive to commit").isTrue();

    final ExecuteQueryResponse q = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT count(*) AS cnt FROM " + typeName)
        .build());
    final long persisted = q.getResultsList().get(0).getRecords(0).getPropertiesMap().get("cnt").getInt64Value();
    assertThat(persisted).as("every streamed row must have been durably committed").isEqualTo(rowCount);
  }
}
