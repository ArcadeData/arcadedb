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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9930: a gRPC streaming insert refused because the node is not the leader reported {@code DB_ERROR} (or
 * {@code COMMIT_FAILED}) - the codes a genuine server fault gets - and dropped the leader's address, which only
 * {@code toString()} of the exception carries. It now reports {@code NOT_LEADER} with the address in the new
 * {@code InsertError.leader_address} field, on every path such a refusal reaches an {@code InsertError} by: a row (shared
 * by {@code insertBidirectional}, {@code insertStream} and {@code bulkInsert}), a chunk's own commit on
 * {@code insertBidirectional} and on {@code insertStream}, and the deferred whole-stream commit of {@code insertStream}.
 * <p>
 * The refusal is raised where Raft raises it - by the database's commit - through the same
 * {@code setWrappedDatabaseInstance} hook {@code RaftReplicatedDatabase} installs itself with: a JDK proxy that rolls the
 * transaction back and refuses while armed, and delegates otherwise. Same harness as #9868's {@code /ws} test.
 *
 * @author Arcade Data Ltd
 */
// A hang detector, not a latency bound
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue9930GrpcInsertLeaderRefusalTest extends BaseGraphServerTest {
  private static final String LEADER = "leader-host:2480";
  private static final String TYPE   = "Issue9930Row";

  private final AtomicBoolean refuseCommits = new AtomicBoolean();
  private final AtomicBoolean refuseRows    = new AtomicBoolean();
  private final AtomicBoolean failRows      = new AtomicBoolean();
  private final AtomicBoolean refuseWrapped = new AtomicBoolean();

  private LocalDatabase              local;
  private BeforeRecordCreateListener refuseRow;
  private ArcadeDbGrpcService        service;

  @BeforeEach
  void installRefusingWrapper() {
    getServerDatabase(0, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");

    local = (LocalDatabase) ((DatabaseInternal) getServerDatabase(0, getDatabaseName())).getEmbedded();
    final DatabaseInternal real = local;
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if (refuseCommits.get() && "commit".equals(method.getName()) && method.getParameterCount() == 0) {
            // What a follower does with a write it cannot replicate: nothing of the transaction survives the refusal
            real.rollback();
            throw new ServerIsNotTheLeaderException("This server is not the leader", LEADER);
          }
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
    local.setWrappedDatabaseInstance(wrapper);

    refuseRow = record -> {
      if (refuseRows.get())
        throw new ServerIsNotTheLeaderException("This server is not the leader", LEADER);
      if (refuseWrapped.get())
        // The engine wraps what it raises: the refusal is found along the cause chain, not only at its head
        throw new DatabaseOperationException("Error on saving the record",
            new ServerIsNotTheLeaderException("This server is not the leader", LEADER));
      if (failRows.get())
        throw new DatabaseOperationException("an ordinary row failure");
      return true;
    };
    local.getSchema().getType(TYPE).getEvents().registerListener(refuseRow);

    service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));
  }

  @AfterEach
  void removeRefusingWrapper() {
    refuseCommits.set(false);
    refuseRows.set(false);
    failRows.set(false);
    refuseWrapped.set(false);
    if (service != null)
      service.close();
    if (local != null && local.isOpen()) {
      local.getSchema().getType(TYPE).getEvents().unregisterListener(refuseRow);
      // A database with no HA wrapper is its own wrapped instance: this restores the state before the test
      local.setWrappedDatabaseInstance(local);
    }
  }

  // ---------- insertBidirectional ----------

  @Test
  void bidiRefusedRowsAreReportedAsNotLeaderWithTheLeaderAddress() throws Exception {
    final RecordingInsertResponseObserver resp = new RecordingInsertResponseObserver();
    final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);
    final String sessionId = startBidi(req, resp, InsertOptions.TransactionMode.PER_STREAM);

    refuseRows.set(true);
    final BatchAck ack = sendBidiChunk(req, resp, sessionId, 2);
    refuseRows.set(false);

    assertThat(ack.getFailed()).isEqualTo(2);
    assertThat(ack.getErrorsList()).hasSize(2);
    for (int i = 0; i < ack.getErrorsCount(); i++)
      assertNotLeader(ack.getErrors(i), i);
    req.onCompleted();
  }

  @Test
  void bidiRefusalWrappedInAnotherExceptionIsStillReportedAsNotLeader() throws Exception {
    final RecordingInsertResponseObserver resp = new RecordingInsertResponseObserver();
    final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);
    final String sessionId = startBidi(req, resp, InsertOptions.TransactionMode.PER_STREAM);

    refuseWrapped.set(true);
    final BatchAck ack = sendBidiChunk(req, resp, sessionId, 1);
    refuseWrapped.set(false);

    assertThat(ack.getErrorsList()).hasSize(1);
    assertNotLeader(ack.getErrors(0), 0);
    req.onCompleted();
  }

  @Test
  void bidiChunkWhoseCommitIsRefusedIsReportedAsNotLeader() throws Exception {
    final RecordingInsertResponseObserver resp = new RecordingInsertResponseObserver();
    final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);
    final String sessionId = startBidi(req, resp, InsertOptions.TransactionMode.PER_ROW);

    refuseCommits.set(true);
    final BatchAck ack = sendBidiChunk(req, resp, sessionId, 3);
    refuseCommits.set(false);

    assertThat(ack.getFailed()).isEqualTo(3);
    assertThat(ack.getErrorsList()).as("one whole-chunk entry").hasSize(1);
    assertNotLeader(ack.getErrors(0), -1);
    req.onCompleted();
    assertThat(rowCount()).as("the refused chunk was not written").isZero();
  }

  @Test
  void bidiOrdinaryRowFailureKeepsDbErrorAndNamesNoLeader() throws Exception {
    final RecordingInsertResponseObserver resp = new RecordingInsertResponseObserver();
    final StreamObserver<InsertRequest> req = service.insertBidirectional(resp);
    final String sessionId = startBidi(req, resp, InsertOptions.TransactionMode.PER_STREAM);

    // A row failure that is no leader refusal
    failRows.set(true);
    final BatchAck ack = sendBidiChunk(req, resp, sessionId, 1);
    failRows.set(false);

    assertThat(ack.getErrorsList()).hasSize(1);
    assertThat(ack.getErrors(0).getCode()).isEqualTo("DB_ERROR");
    assertThat(ack.getErrors(0).getLeaderAddress()).isEmpty();
    req.onCompleted();
  }

  // ---------- insertStream ----------

  @Test
  void insertStreamRefusedRowsAreReportedAsNotLeader() throws Exception {
    refuseRows.set(true);
    final InsertSummary summary = insertStream(InsertOptions.TransactionMode.PER_STREAM, 2);
    refuseRows.set(false);

    assertThat(summary.getFailed()).isEqualTo(2);
    assertThat(summary.getErrorsList()).hasSize(2);
    for (int i = 0; i < summary.getErrorsCount(); i++)
      assertNotLeader(summary.getErrors(i), i);
  }

  @Test
  void insertStreamChunkWhoseCommitIsRefusedIsReportedAsNotLeader() throws Exception {
    refuseCommits.set(true);
    final InsertSummary summary = insertStream(InsertOptions.TransactionMode.PER_ROW, 3);
    refuseCommits.set(false);

    final List<InsertError> errors = summary.getErrorsList();
    assertThat(errors).as("one whole-chunk entry").hasSize(1);
    assertNotLeader(errors.getFirst(), -1);
    assertThat(rowCount()).as("the refused chunk was not written").isZero();
  }

  @Test
  void insertStreamDeferredCommitRefusedIsReportedAsNotLeader() throws Exception {
    refuseCommits.set(true);
    final InsertSummary summary = insertStream(InsertOptions.TransactionMode.PER_STREAM, 2);
    refuseCommits.set(false);

    assertThat(summary.getInserted()).as("the refused commit persisted nothing").isZero();
    final List<InsertError> errors = summary.getErrorsList();
    assertThat(errors).as("one whole-stream entry").hasSize(1);
    assertNotLeader(errors.getFirst(), -1);
    assertThat(rowCount()).as("the refused stream was not written").isZero();
  }

  // ---------- bulkInsert ----------

  @Test
  void bulkInsertRefusedRowsAreReportedAsNotLeader() {
    final BulkInsertRequest.Builder request = BulkInsertRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setOptions(options(InsertOptions.TransactionMode.PER_REQUEST));
    for (int i = 0; i < 2; i++)
      request.addRows(row(i));

    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    refuseRows.set(true);
    service.bulkInsert(request.build(), new StreamObserver<>() {
      @Override
      public void onNext(final InsertSummary value) {
        summaryRef.set(value);
      }

      @Override
      public void onError(final Throwable t) {
        errorRef.set(t);
      }

      @Override
      public void onCompleted() {
      }
    });
    refuseRows.set(false);

    assertThat(errorRef.get()).isNull();
    final InsertSummary summary = summaryRef.get();
    assertThat(summary.getErrorsList()).hasSize(2);
    for (int i = 0; i < summary.getErrorsCount(); i++)
      assertNotLeader(summary.getErrors(i), i);
  }

  // ---------- helpers ----------

  private static void assertNotLeader(final InsertError error, final long rowIndex) {
    assertThat(error.getRowIndex()).isEqualTo(rowIndex);
    assertThat(error.getCode()).as("not reported as a server fault").isEqualTo("NOT_LEADER");
    assertThat(error.getLeaderAddress()).isEqualTo(LEADER);
  }

  private InsertSummary insertStream(final InsertOptions.TransactionMode mode, final int rows) throws InterruptedException {
    final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);
    final StreamObserver<InsertChunk> req = service.insertStream(new NoopServerCallStreamObserver<>() {
      @Override
      public void onNext(final InsertSummary value) {
        summaryRef.set(value);
      }

      @Override
      public void onError(final Throwable t) {
        errorRef.set(t);
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }
    });

    final InsertChunk.Builder chunk = InsertChunk.newBuilder().setSessionId("issue-9930-" + mode).setChunkSeq(0).setLast(true)
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setOptions(options(mode));
    for (int i = 0; i < rows; i++)
      chunk.addRows(row(i));
    req.onNext(chunk.build());
    req.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).as("insertStream completed").isTrue();
    assertThat(errorRef.get()).as("the refusal is reported in the summary, not as a stream error").isNull();
    return summaryRef.get();
  }

  private String startBidi(final StreamObserver<InsertRequest> req, final RecordingInsertResponseObserver resp,
      final InsertOptions.TransactionMode mode) throws InterruptedException {
    req.onNext(InsertRequest.newBuilder()
        .setStart(Start.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setOptions(options(mode)).build())
        .build());
    return resp.awaitStarted().getSessionId();
  }

  private BatchAck sendBidiChunk(final StreamObserver<InsertRequest> req, final RecordingInsertResponseObserver resp,
      final String sessionId, final int rows) throws InterruptedException {
    final InsertChunk.Builder chunk = InsertChunk.newBuilder().setSessionId(sessionId).setChunkSeq(1);
    for (int i = 0; i < rows; i++)
      chunk.addRows(row(i));
    req.onNext(InsertRequest.newBuilder().setChunk(chunk.build()).build());
    return resp.awaitBatchAck();
  }

  private InsertOptions options(final InsertOptions.TransactionMode mode) {
    return InsertOptions.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(TYPE)
        .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR).setTransactionMode(mode).build();
  }

  private static GrpcRecord row(final int i) {
    return GrpcRecord.newBuilder().setType(TYPE).putProperties("name", stringValue("n" + i)).build();
  }

  private static GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private long rowCount() {
    return getServerDatabase(0, getDatabaseName()).countType(TYPE, false);
  }

  private static class NoopServerCallStreamObserver<T> extends ServerCallStreamObserver<T> {
    @Override
    public void onNext(final T value) {
    }

    @Override
    public void onError(final Throwable t) {
    }

    @Override
    public void onCompleted() {
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public void setOnCancelHandler(final Runnable onCancelHandler) {
    }

    @Override
    public void setCompression(final String compression) {
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(final Runnable onReadyHandler) {
    }

    @Override
    public void request(final int count) {
    }

    @Override
    public void setMessageCompression(final boolean enable) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }
  }

  /**
   * The bidi handler answers on its own per-stream executor thread, so each expected response is handed to the test
   * thread through a latch.
   */
  private static final class RecordingInsertResponseObserver extends NoopServerCallStreamObserver<InsertResponse> {
    private final AtomicReference<Started>   startedRef  = new AtomicReference<>();
    private final AtomicReference<BatchAck>  batchAckRef = new AtomicReference<>();
    private final AtomicReference<Throwable> errorRef    = new AtomicReference<>();
    private final CountDownLatch             started     = new CountDownLatch(1);
    private final CountDownLatch             batchAck    = new CountDownLatch(1);

    @Override
    public void onNext(final InsertResponse value) {
      switch (value.getMsgCase()) {
      case STARTED -> {
        startedRef.set(value.getStarted());
        started.countDown();
      }
      case BATCH_ACK -> {
        batchAckRef.set(value.getBatchAck());
        batchAck.countDown();
      }
      default -> {
      }
      }
    }

    @Override
    public void onError(final Throwable t) {
      errorRef.set(t);
      started.countDown();
      batchAck.countDown();
    }

    Started awaitStarted() throws InterruptedException {
      assertThat(started.await(30, TimeUnit.SECONDS)).as("timed out waiting for Started").isTrue();
      if (errorRef.get() != null)
        throw new AssertionError("insertBidirectional errored before Started", errorRef.get());
      return startedRef.get();
    }

    BatchAck awaitBatchAck() throws InterruptedException {
      assertThat(batchAck.await(30, TimeUnit.SECONDS)).as("timed out waiting for BatchAck").isTrue();
      if (errorRef.get() != null)
        throw new AssertionError("insertBidirectional errored before BatchAck", errorRef.get());
      return batchAckRef.get();
    }
  }
}
