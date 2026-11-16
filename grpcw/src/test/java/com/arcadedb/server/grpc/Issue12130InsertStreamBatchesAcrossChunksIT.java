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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12130: {@code InsertStream} under {@code PER_BATCH} must commit every
 * {@code server_batch_size} rows of the STREAM, also when the batch spans several chunks, and the summary of a stream that
 * fails part way must count every row that was committed before the failure, including the rows committed inside the
 * failing chunk. The failure is a row colliding with a UNIQUE index, which surfaces at commit time under
 * {@code CONFLICT_ERROR}.
 */
public class Issue12130InsertStreamBatchesAcrossChunksIT extends BaseGrpcServerTest {

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncStub;
  private String                                          typeName;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);
    asyncStub = ArcadeDbServiceGrpc.newStub(authenticated);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (typeName != null)
      command("DROP TYPE " + typeName + " IF EXISTS UNSAFE");
    if (channel != null) {
      // shutdownNow: an InsertBidirectional half-closed without COMMIT is rolled back but never completed by the server, and
      // would otherwise hold the server's shutdown until its grace period runs out
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Batches of 50 rows sent in chunks of 10: the batches {@code 0..49}, {@code 50..99} and {@code 100..149} commit, the
   * row {@code n = 190} collides, so only the open batch {@code 150..199} is rolled back. Before the fix the batch counter
   * restarted at every chunk, the stream ran as one transaction and the collision rolled back all 200 rows.
   */
  @Test
  void perBatchCommitsBatchesThatSpanChunks() throws Exception {
    createRowTypeWithExistingRow(190);

    final InsertSummary summary = insertStream(InsertOptions.TransactionMode.PER_BATCH, 50, 20, 10);

    assertThat(storedRows()).as("the three batches before the failing one must stand").isEqualTo(150);
    assertThat(summary.getReceived()).isEqualTo(200);
    assertThat(summary.getInserted()).as("inserted must match the rows stored").isEqualTo(150);
    assertThat(summary.getFailed()).as("the open batch rolled back with the failed commit").isEqualTo(50);
    assertThat(summary.getErrorsList()).hasSize(1);
    assertThat(summary.getErrors(0).getCode()).isEqualTo("CONFLICT");
  }

  /**
   * Batches of 5 rows sent in chunks of 10: the row {@code n = 27} collides, so the batch {@code 25..29} is rolled back
   * while the batch {@code 20..24}, committed inside the same chunk, stands. Before the fix the counts of the failing chunk
   * were discarded and the summary said 20 inserted.
   */
  @Test
  void perBatchCountsTheBatchesCommittedInsideTheFailingChunk() throws Exception {
    createRowTypeWithExistingRow(27);

    final InsertSummary summary = insertStream(InsertOptions.TransactionMode.PER_BATCH, 5, 5, 10);

    assertThat(storedRows()).isEqualTo(25);
    assertThat(summary.getReceived()).isEqualTo(30);
    assertThat(summary.getInserted()).as("inserted must match the rows stored").isEqualTo(25);
    assertThat(summary.getFailed()).as("the rolled-back batch 25..29").isEqualTo(5);
  }

  /**
   * Under {@code PER_ROW} the rows {@code 20..24} of the failing chunk commit one by one before the row {@code n = 25}
   * collides; the rest of the chunk is not applied. Before the fix the summary said 20 inserted while 25 rows were
   * stored, so a client retrying the rows it believed were not inserted duplicated five of them.
   */
  @Test
  void perRowCountsTheRowsCommittedInsideTheFailingChunk() throws Exception {
    createRowTypeWithExistingRow(25);

    final InsertSummary summary = insertStream(InsertOptions.TransactionMode.PER_ROW, 0, 5, 10);

    assertThat(storedRows()).isEqualTo(25);
    assertThat(summary.getReceived()).isEqualTo(30);
    assertThat(summary.getInserted()).as("inserted must match the rows stored").isEqualTo(25);
    assertThat(summary.getFailed()).as("the colliding row and the four rows of its chunk never applied").isEqualTo(5);
    assertThat(summary.getErrorsList()).hasSize(1);
    assertThat(summary.getErrors(0).getCode()).isEqualTo("CONFLICT");
  }

  /**
   * {@code InsertBidirectional} shares the insert loop, so its batches span chunks too. The batch {@code 0..14} spans the
   * first two chunks and commits inside the second; the batch {@code 15..29} collides at {@code n = 25} and its commit,
   * due at the end of the third chunk, fails. Its rows {@code 15..19} were acknowledged with the second chunk, so the
   * acknowledgement of the third reports them as failed along with the third chunk's own rows. Before the fix no batch
   * committed before the half-close, which rolled back all 30 rows.
   */
  @Test
  void insertBidirectionalCommitsBatchesThatSpanChunks() throws Exception {
    createRowTypeWithExistingRow(25);

    final BatchAck[] acks = insertBidirectional(InsertOptions.TransactionMode.PER_BATCH, 15, 3, 10);

    assertThat(storedRows()).as("the first batch must be committed across the two chunks").isEqualTo(15);
    assertThat(acks[0].getInserted()).isEqualTo(10);
    assertThat(acks[1].getInserted()).isEqualTo(10);
    assertThat(acks[2].getInserted()).as("no row of the rolled-back batch is inserted").isZero();
    assertThat(acks[2].getFailed()).as("the rolled-back batch 15..29").isEqualTo(15);
    assertThat(acks[2].getErrorsList()).hasSize(1);
  }

  /**
   * Under {@code PER_ROW} the acknowledgement of the failing chunk counts the rows {@code 20..24} committed before the
   * collision at {@code n = 25}. Before the fix it said {@code inserted = 0, failed = 10} while five of its rows were stored.
   */
  @Test
  void insertBidirectionalAcksTheRowsCommittedInsideTheFailingChunk() throws Exception {
    createRowTypeWithExistingRow(25);

    final BatchAck[] acks = insertBidirectional(InsertOptions.TransactionMode.PER_ROW, 0, 3, 10);

    assertThat(storedRows()).isEqualTo(25);
    assertThat(acks[2].getInserted()).as("the rows of the failing chunk committed before the collision").isEqualTo(5);
    assertThat(acks[2].getFailed()).as("the colliding row and the four rows of its chunk never applied").isEqualTo(5);
    assertThat(acks[2].getErrorsList()).hasSize(1);
  }

  /** Sends the chunks one by one, each after the acknowledgement of the previous one, then half-closes. */
  private BatchAck[] insertBidirectional(final InsertOptions.TransactionMode mode, final int serverBatchSize, final int chunks,
      final int rowsPerChunk) throws InterruptedException {
    final BlockingQueue<InsertResponse> responses = new LinkedBlockingQueue<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final StreamObserver<InsertRequest> req = asyncStub.insertBidirectional(new StreamObserver<>() {
      @Override
      public void onNext(final InsertResponse value) {
        responses.add(value);
      }

      @Override
      public void onError(final Throwable t) {
        error.set(t);
      }

      @Override
      public void onCompleted() {
      }
    });

    req.onNext(InsertRequest.newBuilder()
        .setStart(Start.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
            .setOptions(options(mode, serverBatchSize)).build()).build());
    final InsertResponse started = responses.poll(10, TimeUnit.SECONDS);
    assertThat(started).as("Started, error: %s", error.get()).isNotNull();
    final String sessionId = started.getStarted().getSessionId();

    final BatchAck[] acks = new BatchAck[chunks];
    for (int c = 0; c < chunks; c++) {
      req.onNext(InsertRequest.newBuilder().setChunk(chunk(sessionId, c, rowsPerChunk)).build());
      final InsertResponse ack = responses.poll(10, TimeUnit.SECONDS);
      assertThat(ack).as("BatchAck %s, error: %s", c, error.get()).isNotNull();
      acks[c] = ack.getBatchAck();
    }
    req.onCompleted();
    return acks;
  }

  private InsertSummary insertStream(final InsertOptions.TransactionMode mode, final int serverBatchSize, final int chunks,
      final int rowsPerChunk) throws InterruptedException {
    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<InsertSummary> summary = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final StreamObserver<InsertChunk> req = asyncStub.insertStream(new StreamObserver<>() {
      @Override
      public void onNext(final InsertSummary s) {
        summary.set(s);
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
    });

    final InsertOptions options = options(mode, serverBatchSize);
    for (int c = 0; c < chunks; c++)
      req.onNext(chunk("issue-12130", c, rowsPerChunk).toBuilder().setOptions(options).setLast(c == chunks - 1).build());
    req.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();
    assertThat(error.get()).isNull();
    assertThat(summary.get()).as("client must receive an InsertSummary").isNotNull();
    return summary.get();
  }

  private InsertOptions options(final InsertOptions.TransactionMode mode, final int serverBatchSize) {
    return InsertOptions.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setTargetClass(typeName)
        .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR).setTransactionMode(mode).setServerBatchSize(serverBatchSize)
        .build();
  }

  /** The rows {@code n = c * rows .. (c + 1) * rows - 1}. */
  private InsertChunk chunk(final String sessionId, final int c, final int rows) {
    final InsertChunk.Builder chunk = InsertChunk.newBuilder().setSessionId(sessionId).setChunkSeq(c + 1);
    for (int n = c * rows; n < (c + 1) * rows; n++)
      chunk.addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("n", GrpcValue.newBuilder().setInt64Value(n).build()));
    return chunk.build();
  }

  private void createRowTypeWithExistingRow(final int existing) {
    typeName = "Issue12130_" + System.nanoTime();
    command("CREATE DOCUMENT TYPE " + typeName);
    command("CREATE PROPERTY " + typeName + ".n INTEGER");
    command("CREATE INDEX ON " + typeName + "(n) UNIQUE");
    command("INSERT INTO " + typeName + " SET n = " + existing + ", existing = true");
  }

  /** The rows of the stream in the database, the pre-existing row left out. */
  private long storedRows() {
    final ExecuteQueryResponse resp = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(credentials()).setQuery("SELECT count(*) AS cnt FROM " + typeName + " WHERE existing IS NULL").build());
    return resp.getResults(0).getRecords(0).getPropertiesMap().get("cnt").getInt64Value();
  }

  private void command(final String sql) {
    stub.executeCommand(
        ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build());
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
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
}
