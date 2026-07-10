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
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4806: a mid-stream commit failure in {@code InsertStream} must stop
 * the stream and record the failure at the transaction level, instead of mis-attributing it to a
 * single row and then continuing to insert subsequent rows into the already rolled-back
 * transaction.
 *
 * <p>In {@code PER_BATCH} mode with a small server batch size, the deferred unique-index check
 * fires at the mid-stream {@code db.commit()} inside {@code insertRows(...)}. That exception
 * escapes {@code insertRows} (per-row errors are caught row-by-row) and the engine has already
 * rolled the transaction back. The old code recorded the error at {@code ctx.received - 1} with a
 * generic {@code DB_ERROR} code and then kept pulling chunks against the broken transaction,
 * producing further spurious {@code DB_ERROR}s.
 */
public class Issue4806InsertStreamMidStreamCommitFailureIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                          channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncAuthenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
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

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  @Test
  void midStreamCommitFailureStopsStreamAndRecordsTransactionLevelError() throws Exception {
    final String typeName = "Issue4806PerBatch_" + System.currentTimeMillis();

    // Schema + unique index + one pre-existing row the streamed duplicate will collide with at commit.
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName).build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".name STRING").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(name) UNIQUE").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("INSERT INTO " + typeName + " SET name = 'Alice'").build());

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      // PER_BATCH with server_batch_size=1 forces a commit after every row, so the duplicate 'Alice'
      // triggers a mid-stream commit failure inside insertRows. CONFLICT_ERROR (no key_columns) means
      // the duplicate is not pre-checked away; it surfaces only at the deferred commit.
      final InsertOptions options = InsertOptions.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTargetClass(typeName)
          .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
          .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
          .setServerBatchSize(1)
          .build();

      // Chunk 0: the duplicate that fails to commit mid-stream.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4806")
          .setChunkSeq(0)
          .setOptions(options)
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Alice")).build())
          .build());

      // Chunk 1: a row that would succeed against a healthy transaction. It must NOT be processed
      // against the rolled-back transaction.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4806")
          .setChunkSeq(1)
          .setLast(true)
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Charlie")).build())
          .build());

      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();
      final InsertSummary summary = summaryRef.get();

      // Exactly one transaction-level error: the mid-stream commit failure. The old code produced a
      // second spurious DB_ERROR by continuing to insert 'Charlie' into the rolled-back transaction.
      assertThat(summary.getErrorsList())
          .as("a mid-stream commit failure must be reported as a single transaction-level error")
          .hasSize(1);

      final InsertError error = summary.getErrors(0);
      // The failure is not attributable to a single row: it must be reported at the transaction
      // level (rowIndex -1), not mis-indexed at ctx.received-1.
      assertThat(error.getRowIndex())
          .as("a commit failure must be reported at the transaction level (-1), not mis-indexed to a row")
          .isEqualTo(-1);
      assertThat(error.getCode()).isEqualTo("CONFLICT");
      assertThat(error.getMessage()).contains("Alice");

      // 'Charlie' must never have been inserted: the duplicate rolled the transaction back and the
      // stream stopped processing further rows.
      final ExecuteQueryResponse queryResp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT name FROM " + typeName + " WHERE name = 'Charlie'")
          .build());
      final boolean charlieExists = !queryResp.getResultsList().isEmpty()
          && !queryResp.getResultsList().getFirst().getRecordsList().isEmpty();
      assertThat(charlieExists)
          .as("rows after a mid-stream commit failure must not be inserted into the broken transaction")
          .isFalse();
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }

  /**
   * A transaction-level failure that is NOT a commit failure (here: an "options changed mid-stream"
   * rejection) leaves the PER_BATCH transaction still active and bound to the pooled gRPC thread. It
   * must be rolled back (not silently abandoned active), so the rows buffered in the failed
   * transaction are not committed and the failure is reported once at the transaction level.
   */
  @Test
  void midStreamContractChangeAbortsActiveTransactionAndReportsSingleError() throws Exception {
    final String typeName = "Issue4806ContractChange_" + System.currentTimeMillis();

    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName).build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".name STRING").build());

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      // PER_BATCH with the default (large) batch size keeps the first chunk's row buffered in an
      // active, uncommitted transaction.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4806-contract")
          .setChunkSeq(0)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Buffered")).build())
          .build());

      // Second chunk changes the target class -> contract violation -> transaction-level failure while
      // the transaction from chunk 0 is still active.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4806-contract")
          .setChunkSeq(1)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName + "_other")
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Never")).build())
          .build());

      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();
      final InsertSummary summary = summaryRef.get();

      assertThat(summary.getErrorsList())
          .as("a mid-stream contract violation must be reported as a single transaction-level error")
          .hasSize(1);
      final InsertError error = summary.getErrors(0);
      assertThat(error.getRowIndex()).isEqualTo(-1);
      // A pre-insert contract rejection is not a commit failure: it is reported as CONTRACT_VIOLATION.
      assertThat(error.getCode()).isEqualTo("CONTRACT_VIOLATION");

      // The buffered row was counted as inserted before the transaction was aborted. Once abortTransaction()
      // rolls it back it is no longer in the database, so the summary must not still report it as inserted -
      // it must be reclassified as failed so the counts match the durable state.
      assertThat(summary.getInserted())
          .as("rows rolled back by the mid-stream abort must not be reported as inserted")
          .isEqualTo(0);
      assertThat(summary.getUpdated()).isEqualTo(0);
      // Both rows end up not persisted: the rolled-back "Buffered" row (reclassified) and the rejected
      // "Never" row (the structural failure). The summary balances: received == inserted + updated + failed.
      assertThat(summary.getReceived()).isEqualTo(2);
      assertThat(summary.getFailed())
          .as("the rolled-back buffered row and the rejected row must both be counted as failed")
          .isEqualTo(2);

      // The buffered row must have been rolled back with the aborted transaction, not committed by the
      // deferred onCompleted commit running against a leaked active transaction.
      final ExecuteQueryResponse queryResp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT count(*) as cnt FROM " + typeName)
          .build());
      final long count = queryResp.getResultsList().getFirst().getRecordsList().getFirst()
          .getPropertiesMap().get("cnt").getInt64Value();
      assertThat(count)
          .as("rows buffered in a transaction aborted mid-stream must not be committed")
          .isEqualTo(0);
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }
}
