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
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9875: the gRPC client-streaming loads - {@code InsertStream}, {@code InsertBidirectional}, {@code GraphBatchLoad},
 * {@code TimeSeriesWriteStream} - are listed in the server's running statements for the whole stream, a terminate closes
 * the call with {@code CANCELLED} and retracts what the stream's mode can still retract, and a load that ends on its own
 * leaves no entry behind. Every load here is terminated BETWEEN two chunks, idle and waiting for the client, which is
 * where a {@code PER_STREAM} load spends most of its life holding its transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9875GrpcStreamingLoadsListedIT extends BaseGrpcServerTest {
  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncStub;

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
    asyncStub = ArcadeDbServiceGrpc.newStub(authenticated);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aTerminatedInsertStreamIsCancelledAndRolledBack() throws Exception {
    createDocumentType("Streamed9875");
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = asyncStub.insertStream(outcome);
    stream.onNext(insertChunk("Streamed9875", InsertOptions.TransactionMode.PER_STREAM, null, 10));

    final RunningQuery entry = awaitListed("InsertStream into Streamed9875 (per_stream)");
    assertThat(entry.getProtocol()).isEqualTo("grpc");
    assertThat(entry.getUser()).isEqualTo("root");
    assertThat(entry.getDatabase()).isEqualTo(getDatabaseName());
    assertThat(entry.getSessionId()).isNull();
    // Issue #9962: the handler opens the entry where the call is out of reach, and still lists its peer
    assertThat(entry.getClientAddress()).as("the peer of the stream").isNotBlank().isNotEqualTo("null");

    entry.terminate("root");
    outcome.assertTerminated();
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    // The whole stream was one transaction, and it was rolled back
    assertThat(count("Streamed9875")).isZero();
  }

  @Test
  void aTerminatedPerBatchInsertStreamKeepsTheBatchesItCommitted() throws Exception {
    createDocumentType("Batched9875");
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = asyncStub.insertStream(outcome);
    // Batches of 4: rows 1-8 commit as two batches, rows 9-10 wait in the open one
    final InsertChunk chunk = insertChunk("Batched9875", InsertOptions.TransactionMode.PER_BATCH, null, 10);
    stream.onNext(chunk.toBuilder().setOptions(chunk.getOptions().toBuilder().setServerBatchSize(4)).build());

    final RunningQuery entry = awaitListed("InsertStream into Batched9875 (per_batch)");
    await().atMost(Duration.ofSeconds(30)).until(() -> count("Batched9875") == 8);
    entry.terminate("root");
    outcome.assertTerminated();
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(count("Batched9875")).isEqualTo(8);
  }

  @Test
  void aTerminatedPerRowInsertStreamKeepsTheRowsItCommitted() throws Exception {
    createDocumentType("Rows9875");
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = asyncStub.insertStream(outcome);
    stream.onNext(insertChunk("Rows9875", InsertOptions.TransactionMode.PER_ROW, null, 5));

    final RunningQuery entry = awaitListed("InsertStream into Rows9875 (per_row)");
    await().atMost(Duration.ofSeconds(30)).until(() -> count("Rows9875") == 5);
    entry.terminate("root");
    outcome.assertTerminated();
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(count("Rows9875")).isEqualTo(5);
  }

  @Test
  void anInsertStreamTerminatedWhileItsChunkRunsIsCancelledAndRolledBack() throws Exception {
    createDocumentType("Running9875");
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = asyncStub.insertStream(outcome);
    // Listed as the chunk starts, so the terminate below lands while its rows are being inserted, or right after: the
    // client is told the same either way, and nothing of the stream is committed
    stream.onNext(insertChunk("Running9875", InsertOptions.TransactionMode.PER_STREAM, null, 50_000));

    final RunningQuery entry = awaitListed("InsertStream into Running9875");
    entry.terminate("root");
    outcome.assertTerminated();
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    assertThat(count("Running9875")).isZero();
  }

  @Test
  void aTerminatedInsertStreamEndsTheClientTransactionItWritesInto() throws Exception {
    createDocumentType("InTx9875");
    final String txId = stub.beginTransaction(
        BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
    final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = asyncStub.insertStream(outcome);
    stream.onNext(insertChunk("InTx9875", InsertOptions.TransactionMode.PER_STREAM, tx, 5));

    final RunningQuery entry = awaitListed("InsertStream into InTx9875 (per_stream)");
    // Listed under the client's transaction, as SHOW TRANSACTIONS groups it
    assertThat(entry.getSessionId()).isEqualTo(txId);

    entry.terminate("root");
    outcome.assertTerminated();
    assertThat(entry.awaitEnd(30_000)).isTrue();

    // The transaction went with the load: a commit finds nothing to commit, and what the load wrote is not there
    assertThat(stub.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build())
        .getCommitted()).isFalse();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    assertThat(count("InTx9875")).isZero();
  }

  @Test
  void aTerminatedBidirectionalInsertIsCancelledAndRolledBack() throws Exception {
    createDocumentType("Bidi9875");
    final CountDownLatch acknowledged = new CountDownLatch(1);
    final Outcome<InsertResponse> outcome = new Outcome<>() {
      @Override
      public void onNext(final InsertResponse value) {
        if (value.hasBatchAck())
          acknowledged.countDown();
      }
    };
    final StreamObserver<InsertRequest> stream = asyncStub.insertBidirectional(outcome);
    stream.onNext(InsertRequest.newBuilder().setStart(Start.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setOptions(insertOptions("Bidi9875", InsertOptions.TransactionMode.PER_STREAM))).build());
    stream.onNext(InsertRequest.newBuilder().setChunk(insertChunk("Bidi9875", InsertOptions.TransactionMode.PER_STREAM, null, 7)
        .toBuilder().setChunkSeq(1)).build());
    assertThat(acknowledged.await(30, TimeUnit.SECONDS)).isTrue();

    final RunningQuery entry = awaitListed("InsertBidirectional session ");
    assertThat(entry.getText()).endsWith(" into Bidi9875 (per_stream)");
    assertThat(entry.getUser()).isEqualTo("root");

    entry.terminate("root");
    outcome.assertTerminated();
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    assertThat(count("Bidi9875")).isZero();
  }

  @Test
  void aTerminatedGraphBatchLoadIsCancelledAndGivesTheBatchBack() throws Exception {
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE VERTEX TYPE Node9875 IF NOT EXISTS");
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = asyncStub.graphBatchLoad(outcome);
    stream.onNext(GraphBatchChunk.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .addRecords(vertex("Node9875", "a")).addRecords(vertex("Node9875", "b")).build());

    final RunningQuery entry = awaitListed("GraphBatchLoad");
    entry.terminate("root");
    final StatusRuntimeException error = outcome.assertTerminated();
    // What the batch committed before the termination is reported, as after any other failure of the load
    assertThat(error.getTrailers().get(GraphBatchProtocol.RESULT_TRAILER)).isNotNull();
    assertThat(entry.awaitEnd(30_000)).isTrue();

    // The batch was given back: the same database takes the next load at once
    final Outcome<GraphBatchResult> next = new Outcome<>();
    final StreamObserver<GraphBatchChunk> again = asyncStub.graphBatchLoad(next);
    again.onNext(GraphBatchChunk.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .addRecords(vertex("Node9875", "c")).build());
    again.onCompleted();
    assertThat(next.done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(next.error.get()).isNull();
    assertThat(next.last.get().getVerticesCreated()).isEqualTo(1);
    awaitNotListed("GraphBatchLoad");
  }

  @Test
  void aTerminatedTimeSeriesWriteStreamIsCancelled() throws Exception {
    final Outcome<TimeSeriesWriteSummary> outcome = new Outcome<>();
    final StreamObserver<TimeSeriesWriteChunk> stream = asyncStub.timeSeriesWriteStream(outcome);
    // A measurement nobody created: the chunk is answered in the summary, and the stream stays open
    stream.onNext(TimeSeriesWriteChunk.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setType("Missing9875").addPoints(TimeSeriesPoint.newBuilder().setTimestamp(1_000L)
            .putFields("v", GrpcValue.newBuilder().setDoubleValue(1.5).build())).build());

    final RunningQuery entry = awaitListed("TimeSeriesWriteStream");
    entry.terminate("root");
    outcome.assertTerminated();
    assertThat(entry.awaitEnd(30_000)).isTrue();
    // Nothing to retract: an append is durable as it is made, so what the stream did stands
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.COMPLETED_BEFORE_TERMINATION);
  }

  @Test
  void loadsThatEndOnTheirOwnLeaveNoEntry() throws Exception {
    createDocumentType("Done9875");

    // Completed by the client
    final Outcome<InsertSummary> completed = new Outcome<>();
    final StreamObserver<InsertChunk> stream = asyncStub.insertStream(completed);
    stream.onNext(insertChunk("Done9875", InsertOptions.TransactionMode.PER_STREAM, null, 3));
    awaitListed("InsertStream into Done9875");
    stream.onCompleted();
    assertThat(completed.done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(completed.error.get()).isNull();
    assertThat(completed.last.get().getInserted()).isEqualTo(3);
    awaitNotListed("InsertStream into Done9875");
    assertThat(count("Done9875")).isEqualTo(3);

    // Abandoned by the client
    final Outcome<InsertSummary> abandoned = new Outcome<>();
    final StreamObserver<InsertChunk> cancelled = asyncStub.insertStream(abandoned);
    cancelled.onNext(insertChunk("Done9875", InsertOptions.TransactionMode.PER_STREAM, null, 4));
    final RunningQuery entry = awaitListed("InsertStream into Done9875");
    cancelled.onError(new RuntimeException("client gives up"));
    assertThat(entry.awaitEnd(30_000)).isTrue();
    assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.COMPLETED);
    assertThat(count("Done9875")).isEqualTo(3);
  }

  /** What the client of a stream sees: the last message, and the end of the call. */
  private static class Outcome<T> implements StreamObserver<T> {
    final AtomicReference<T>         last  = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch             done  = new CountDownLatch(1);

    @Override
    public void onNext(final T value) {
      last.set(value);
    }

    @Override
    public void onError(final Throwable t) {
      error.set(t);
      done.countDown();
    }

    @Override
    public void onCompleted() {
      done.countDown();
    }

    /** The call ended with the status of a terminated statement, which the Java client rebuilds the typed exception from. */
    StatusRuntimeException assertTerminated() throws InterruptedException {
      assertThat(done.await(30, TimeUnit.SECONDS)).as("the terminated stream must be closed by the server").isTrue();
      assertThat(error.get()).isInstanceOf(StatusRuntimeException.class);
      final StatusRuntimeException e = (StatusRuntimeException) error.get();
      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
      assertThat(e.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(QueryTerminatedException.class.getName());
      return e;
    }
  }

  private RunningQuery awaitListed(final String textPrefix) {
    final RunningQuery[] found = new RunningQuery[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> (found[0] = findListed(textPrefix)) != null);
    return found[0];
  }

  private void awaitNotListed(final String textPrefix) {
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> findListed(textPrefix) == null);
  }

  private RunningQuery findListed(final String textPrefix) {
    for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
      if (q.getText() != null && q.getText().startsWith(textPrefix))
        return q;
    return null;
  }

  private void createDocumentType(final String name) {
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + name + " IF NOT EXISTS");
  }

  private long count(final String type) {
    return getServer(0).getDatabase(getDatabaseName()).query("sql", "SELECT count(*) AS c FROM " + type).next().<Long>getProperty("c");
  }

  private InsertOptions insertOptions(final String type, final InsertOptions.TransactionMode mode) {
    return InsertOptions.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(type)
        .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR).setTransactionMode(mode).build();
  }

  private InsertChunk insertChunk(final String type, final InsertOptions.TransactionMode mode, final TransactionContext tx,
      final int rows) {
    final InsertChunk.Builder chunk = InsertChunk.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setOptions(insertOptions(type, mode)).setSessionId("issue-9875").setChunkSeq(1);
    if (tx != null)
      chunk.setTransaction(tx);
    for (int i = 0; i < rows; i++)
      chunk.addRows(GrpcRecord.newBuilder().setType(type).putProperties("name", GrpcValue.newBuilder().setStringValue("v" + i).build()));
    return chunk.build();
  }

  private static GraphBatchRecord vertex(final String type, final String tempId) {
    return GraphBatchRecord.newBuilder().setKind(GraphBatchRecord.Kind.VERTEX).setTypeName(type).setTempId(tempId)
        .putProperties("name", GrpcValue.newBuilder().setStringValue(tempId).build()).build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
