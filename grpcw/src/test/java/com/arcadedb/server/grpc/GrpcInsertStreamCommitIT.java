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
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How an {@code InsertStream} commits: commit-time failures reported in the {@link InsertSummary} rather than as a
 * stream-level error, a mid-stream commit failure, the deferred commit on a different callback thread, the row index
 * an error reports, and which database the first chunk names.
 */
public class GrpcInsertStreamCommitIT extends BaseGrpcServerTest {

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncAuthenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
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

  // Regression: #4198
  /**
   * Regression test for issue #4198: gRPC {@code InsertStream} surfaces commit-time constraint
   * violations as a stream-level {@code Status.INTERNAL} error instead of populating
   * {@link InsertSummary#getErrorsList()}.
   *
   * <p>In {@code PER_STREAM} (and {@code PER_REQUEST}) transaction mode, the commit happens in
   * {@code onCompleted()} after {@code insertRows(...)} has already returned cleanly. If commit
   * raises a {@link com.arcadedb.exception.DuplicatedKeyException} (deferred unique-index check),
   * the original code routed the exception through {@code resp.onError(Status.INTERNAL...)} which
   * aborted the entire RPC and left the client with no {@code InsertSummary}.
   *
   * <p>The fix catches recoverable per-row errors thrown by {@code ctx.flushCommit(true)} and
   * registers them in the totals so the client receives a well-formed {@code InsertSummary}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  void insertStreamUniqueViolationUnderPerStreamShouldPopulateErrors() throws Exception {
    final String typeName = "Issue4198PerStreamType_" + System.currentTimeMillis();

    // Schema + unique index + one pre-existing row that the streamed row will collide with.
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

      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4198-per-stream")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder()
              .setType(typeName)
              .putProperties("name", stringValue("Alice")) // duplicate of the pre-existing row
              .build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      // The RPC must terminate cleanly with an InsertSummary, not as Status.INTERNAL.
      assertThat(errorRef.get())
          .as("InsertStream should not abort with Status.INTERNAL on a per-row constraint violation")
          .isNull();
      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getErrorsList())
          .as("the violating row should appear in InsertSummary.errors")
          .hasSize(1);
      assertThat(summary.getErrors(0).getCode()).isEqualTo("CONFLICT");
      assertThat(summary.getErrors(0).getMessage()).contains("Alice");
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }

  // Regression: #4198
  @Test
  void insertStreamUniqueViolationUnderPerRequestShouldPopulateErrors() throws Exception {
    final String typeName = "Issue4198PerRequestType_" + System.currentTimeMillis();

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
        .setCommand("INSERT INTO " + typeName + " SET name = 'Bob'").build());

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4198-per-request")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_REQUEST)
              .build())
          .addRows(GrpcRecord.newBuilder()
              .setType(typeName)
              .putProperties("name", stringValue("Bob"))
              .build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      assertThat(errorRef.get())
          .as("InsertStream should not abort with Status.INTERNAL on a per-row constraint violation")
          .isNull();
      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getErrorsList()).hasSize(1);
      assertThat(summary.getErrors(0).getCode()).isEqualTo("CONFLICT");
      assertThat(summary.getErrors(0).getMessage()).contains("Bob");
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }

  // Regression: #4644
  /**
   * Regression test for issue #4644: gRPC {@code InsertStream} begins the transaction in
   * {@code onNext} but commits it in {@code onCompleted}, which the gRPC serializing executor may
   * run on a different pool thread. Because ArcadeDB transactions are bound to the calling thread
   * through a {@code ThreadLocal} ({@code DatabaseContext}), the deferred {@code PER_STREAM} commit
   * found no transaction bound to the committing thread and failed with
   * {@code TransactionException: Transaction not begun}, reclassifying every inserted row as failed
   * ({@code inserted=0, failed=N}, one {@code row_index=-1 COMMIT_FAILED} error).
   *
   * <p>The real-world failure is intermittent because it depends on whether gRPC's executor happens
   * to run the two callbacks on the same thread. To make the {@code onNext} -> {@code onCompleted}
   * thread hop deterministic, this test drives the {@code insertStream} handler directly: the data
   * chunk is delivered on one thread and the half-close on a different thread. Without the fix the
   * commit runs on a thread with no active transaction and fails; with the fix the handler re-binds
   * the transaction to the committing thread and the rows commit regardless of callback thread.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  void insertStreamPerStreamCommitMustNotDependOnCallbackThread() throws Exception {
    final String typeName = "Issue4644ThreadHopType_" + System.currentTimeMillis();

    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final ThreadHopResponseObserver resp = new ThreadHopResponseObserver(summaryRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Build a single chunk carrying every row, marked last=true, in PER_STREAM mode so the commit
      // is deferred to onCompleted().
      final InsertChunk.Builder chunk = InsertChunk.newBuilder()
          .setSessionId("issue-4644-thread-hop")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build());
      for (int i = 0; i < 150; i++)
        chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("ag" + i)).build());

      // Deliver the data chunk (which calls db.begin() and inserts the rows) on thread A.
      final AtomicReference<Throwable> onNextError = new AtomicReference<>();
      final Thread threadA = new Thread(() -> {
        try {
          req.onNext(chunk.build());
        } catch (final Throwable t) {
          onNextError.set(t);
        }
      }, "issue4644-onNext");
      threadA.start();
      threadA.join(30_000);
      assertThat(onNextError.get()).as("onNext should not throw").isNull();

      // Deliver the half-close (deferred commit) on a DIFFERENT thread B, deterministically
      // reproducing the onNext -> onCompleted thread hop.
      final AtomicReference<Throwable> onCompletedError = new AtomicReference<>();
      final Thread threadB = new Thread(() -> {
        try {
          req.onCompleted();
        } catch (final Throwable t) {
          onCompletedError.set(t);
        }
      }, "issue4644-onCompleted");
      threadB.start();
      threadB.join(30_000);
      assertThat(onCompletedError.get()).as("onCompleted should not throw").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(150);
      assertThat(summary.getInserted()).isEqualTo(150);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      // The rows must actually be persisted and visible after commit.
      final long total = getServer(0).getDatabaseForOperator(getDatabaseName())
          .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
      assertThat(total).isEqualTo(150L);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Minimal {@link ServerCallStreamObserver} test double that captures the single {@link InsertSummary}
   * emitted by the handler and provides no-op implementations of the flow-control surface the handler
   * touches ({@code request}, {@code setOnCancelHandler}, {@code disableAutoInboundFlowControl}).
   */
  private static final class ThreadHopResponseObserver extends ServerCallStreamObserver<InsertSummary> {
    private final AtomicReference<InsertSummary> summaryRef;

    private ThreadHopResponseObserver(final AtomicReference<InsertSummary> summaryRef) {
      this.summaryRef = summaryRef;
    }

    @Override public void onNext(final InsertSummary value) { summaryRef.set(value); }
    @Override public void onError(final Throwable t) { }
    @Override public void onCompleted() { }

    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(final Runnable onCancelHandler) { }
    @Override public void setCompression(final String compression) { }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(final Runnable onReadyHandler) { }
    @Override public void request(final int count) { }
    @Override public void setMessageCompression(final boolean enable) { }
    @Override public void disableAutoInboundFlowControl() { }
  }

  // Regression: #4806
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
          && !queryResp.getResultsList().get(0).getRecordsList().isEmpty();
      assertThat(charlieExists)
          .as("rows after a mid-stream commit failure must not be inserted into the broken transaction")
          .isFalse();
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }

  // Regression: #4806
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
      final long count = queryResp.getResultsList().get(0).getRecordsList().get(0)
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

  // Regression: #5047
  /**
   * Regression test for issue #5047 (STR-1): {@code InsertError.row_index} is documented as the index within
   * the chunk, but the server reported the cumulative stream-wide {@code ctx.received - 1}. This drives a
   * two-chunk {@code insertStream} where the failing row is at chunk-relative index 1 (but cumulative index 3),
   * and asserts the reported {@code row_index} is the chunk-relative 1.
   */
  @Test
  void insertErrorRowIndexIsChunkRelative() throws Exception {
    final String vType = "Issue5047V_" + System.currentTimeMillis();
    final String eType = "Issue5047E_" + System.currentTimeMillis();

    // Two vertices to use as edge endpoints, plus an edge type.
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE VERTEX TYPE " + vType).build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE EDGE TYPE " + eType).build());

    final ExecuteCommandResponse v0 = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setReturnRows(true)
        .setCommand("CREATE VERTEX " + vType).build());
    final ExecuteCommandResponse v1 = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setReturnRows(true)
        .setCommand("CREATE VERTEX " + vType).build());

    final String rid0 = ridOf(v0);
    final String rid1 = ridOf(v1);
    assertThat(rid0).as("vertex 0 rid").isNotNull();
    assertThat(rid1).as("vertex 1 rid").isNotNull();

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      // PER_STREAM commits once at the end, so a per-row MISSING_ENDPOINTS error does not roll the whole
      // stream back; it is recorded row-by-row inside insertRows with a chunk-relative row_index.
      final InsertOptions options = InsertOptions.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTargetClass(eType)
          .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
          .build();

      // Chunk 0 (seq 0): two valid edges -> ctx.received advances to 2.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-5047")
          .setChunkSeq(0)
          .setOptions(options)
          .addRows(edge(eType, rid0, rid1))
          .addRows(edge(eType, rid0, rid1))
          .build());

      // Chunk 1 (seq 1, last): valid, INVALID (no out/in), valid. The invalid row is at chunk-relative
      // index 1 but cumulative index 3.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-5047")
          .setChunkSeq(1)
          .setLast(true)
          .addRows(edge(eType, rid0, rid1))
          .addRows(GrpcRecord.newBuilder().setType(eType).build()) // missing out/in
          .addRows(edge(eType, rid0, rid1))
          .build());

      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();
      final InsertSummary summary = summaryRef.get();
      assertThat(summary).as("client must receive an InsertSummary").isNotNull();

      assertThat(summary.getErrorsList())
          .as("exactly one per-row MISSING_ENDPOINTS error").hasSize(1);
      final InsertError error = summary.getErrors(0);
      assertThat(error.getCode()).isEqualTo("MISSING_ENDPOINTS");
      assertThat(error.getRowIndex())
          .as("row_index must be chunk-relative (1), not the cumulative stream index (3)")
          .isEqualTo(1L);
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + eType + " IF EXISTS UNSAFE").build());
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + vType + " IF EXISTS UNSAFE").build());
    }
  }

  private GrpcRecord edge(final String type, final String out, final String in) {
    return GrpcRecord.newBuilder().setType(type)
        .putProperties("out", stringValue(out))
        .putProperties("in", stringValue(in))
        .build();
  }

  private static String ridOf(final ExecuteCommandResponse resp) {
    for (final GrpcRecord rec : resp.getRecordsList())
      if (rec.getRid() != null && !rec.getRid().isEmpty())
        return rec.getRid();
    return null;
  }

  // Regression: #6597
  /**
   * Regression test for issue #6597: {@code arcadedb-server.proto} documents
   * {@code InsertChunk.database} (field 1) as REQUIRED on the first chunk of the
   * {@code insertStream} client-streaming RPC, but {@code ArcadeDbGrpcService#insertStream}'s
   * {@code onNext} handler never read it - the extraction line was commented out - and sourced the
   * database name exclusively from {@code InsertOptions.database} instead. A client that followed
   * the proto and set only {@code InsertChunk.database} on the first chunk failed with
   * {@code INVALID_ARGUMENT: Invalid database name: name is required}, even though it supplied
   * exactly what the contract requires. {@code graphBatchLoad}'s {@code onNext(GraphBatchChunk)}
   * reads {@code chunk.getDatabase()} directly and does not have this bug.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  void insertStreamMustHonorChunkLevelDatabaseWhenOptionsDatabaseIsUnset() throws Exception {
    final String typeName = "Issue6597ChunkDbType_" + System.currentTimeMillis();

    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final RecordingResponseObserver resp = new RecordingResponseObserver(summaryRef, errorRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Per the proto contract, InsertChunk.database is REQUIRED on the first chunk.
      // InsertOptions deliberately carries NO database, so the fix must fall back to it.
      final InsertChunk chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6597-chunk-database")
          .setChunkSeq(0)
          .setLast(true)
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setOptions(InsertOptions.newBuilder()
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("chunk-db-row")).build())
          .build();

      req.onNext(chunk);
      assertThat(errorRef.get()).as("onNext should not error when InsertChunk.database is set").isNull();

      req.onCompleted();
      assertThat(errorRef.get()).as("onCompleted should not error").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      final long total = getServer(0).getDatabaseForOperator(getDatabaseName())
          .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
      assertThat(total).isEqualTo(1L);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #6597
  @Test
  void insertStreamMustStillHonorOptionsDatabaseWhenChunkDatabaseIsUnset() throws Exception {
    final String typeName = "Issue6597OptionsDbType_" + System.currentTimeMillis();

    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final RecordingResponseObserver resp = new RecordingResponseObserver(summaryRef, errorRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Pre-existing contract: InsertChunk.database left empty, InsertOptions.database set -
      // must keep working exactly as before the fix.
      final InsertChunk chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6597-options-database-fallback")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("options-db-row")).build())
          .build();

      req.onNext(chunk);
      assertThat(errorRef.get()).as("onNext should not error").isNull();

      req.onCompleted();
      assertThat(errorRef.get()).as("onCompleted should not error").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  // Regression: #6597
  @Test
  void insertStreamMustPreferChunkDatabaseOverOptionsDatabaseWhenBothAreSet() throws Exception {
    final String typeName = "Issue6597PrecedenceType_" + System.currentTimeMillis();

    getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + typeName);

    final ArcadeDbGrpcService service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
    try {
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final RecordingResponseObserver resp = new RecordingResponseObserver(summaryRef, errorRef);

      final StreamObserver<InsertChunk> req = service.insertStream(resp);

      // Both InsertChunk.database and InsertOptions.database are set, to DIFFERENT values.
      // InsertOptions.database is deliberately a name rejected by validateDatabaseName (it
      // contains ".."), so if it were ever consulted instead of InsertChunk.database, the insert
      // would fail loudly rather than silently succeeding against the wrong database. This locks
      // in the precedence rule the fix establishes: InsertChunk.database wins.
      final InsertChunk chunk = InsertChunk.newBuilder()
          .setSessionId("issue-6597-precedence")
          .setChunkSeq(0)
          .setLast(true)
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setOptions(InsertOptions.newBuilder()
              .setDatabase("../should-be-ignored")
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("precedence-row")).build())
          .build();

      req.onNext(chunk);
      assertThat(errorRef.get()).as("onNext should not error").isNull();

      req.onCompleted();
      assertThat(errorRef.get()).as("onCompleted should not error").isNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary).isNotNull();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getInserted()).as("InsertChunk.database must take precedence over InsertOptions.database").isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      final long total = getServer(0).getDatabaseForOperator(getDatabaseName())
          .query("sql", "SELECT count(*) AS total FROM " + typeName).next().<Long>getProperty("total");
      assertThat(total).isEqualTo(1L);
    } finally {
      service.close();
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    }
  }

  /**
   * Minimal {@link ServerCallStreamObserver} test double that captures the single
   * {@link InsertSummary} emitted by the handler (or the terminal error), and provides no-op
   * implementations of the flow-control surface the handler touches ({@code request},
   * {@code setOnCancelHandler}, {@code disableAutoInboundFlowControl}).
   */
  private static final class RecordingResponseObserver extends ServerCallStreamObserver<InsertSummary> {
    private final AtomicReference<InsertSummary> summaryRef;
    private final AtomicReference<Throwable>     errorRef;

    private RecordingResponseObserver(final AtomicReference<InsertSummary> summaryRef, final AtomicReference<Throwable> errorRef) {
      this.summaryRef = summaryRef;
      this.errorRef = errorRef;
    }

    @Override public void onNext(final InsertSummary value) { summaryRef.set(value); }
    @Override public void onError(final Throwable t) { errorRef.set(t); }
    @Override public void onCompleted() { }

    @Override public boolean isCancelled() { return false; }
    @Override public void setOnCancelHandler(final Runnable onCancelHandler) { }
    @Override public void setCompression(final String compression) { }
    @Override public boolean isReady() { return true; }
    @Override public void setOnReadyHandler(final Runnable onReadyHandler) { }
    @Override public void request(final int count) { }
    @Override public void setMessageCompression(final boolean enable) { }
    @Override public void disableAutoInboundFlowControl() { }
  }
}
