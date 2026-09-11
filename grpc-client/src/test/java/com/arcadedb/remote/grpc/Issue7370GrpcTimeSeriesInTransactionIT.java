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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.timeseries.TimeSeriesLatestResult;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesQueryResult;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.DatabaseAbstractHandler;
import com.arcadedb.server.http.handler.GetTimeSeriesLatestHandler;
import com.arcadedb.server.http.handler.PostTimeSeriesQueryHandler;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BeginTransactionRequest;
import com.arcadedb.server.grpc.BeginTransactionResponse;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.RollbackTransactionRequest;
import com.arcadedb.server.grpc.TimeSeriesLatestRequest;
import com.arcadedb.server.grpc.TimeSeriesQueryRequest;
import com.arcadedb.server.grpc.TimeSeriesRow;
import com.arcadedb.server.grpc.TransactionContext;
import io.grpc.stub.BlockingClientCall;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7370: {@code TimeSeriesQueryRequest} and {@code TimeSeriesLatestRequest} (#7305 / PR #7323) carried
 * {@code database} and {@code credentials} but no {@code TransactionContext}. Both handlers therefore resolved
 * the database with {@code getDatabase(req.getDatabase(), ...)} and ran on a gRPC worker thread, whatever
 * transaction the caller believed it was inside - the same gap #7326 closed on the three search RPCs.
 * <p>
 * <b>What this branch fixes, stated as narrowly as it was measured.</b> The observable defect is the silent
 * fall-through: a request naming a transaction the server no longer knows - reaped, committed, or invented -
 * read outside it and answered as if nothing were wrong, where every other transaction-scoped RPC answers
 * FAILED_PRECONDITION. {@link #aTimeSeriesQueryNamingAnUnknownTransactionIsRejected} and
 * {@link #aTimeSeriesLatestNamingAnUnknownTransactionIsRejected} are the two tests that fail against the
 * unfixed server. The rest of this class guards the new dispatch: with a live transaction named, the whole
 * read - including a multi-message stream and the bounded transport wait inside it - now runs on that
 * transaction's single-threaded executor, and must still produce the same complete, ordered answer.
 * <p>
 * <b>What it does not fix, and why the difference is not visible in the samples.</b> #7370 predicts that a
 * caller which appended points inside its transaction would not see them. It does see them - but not because
 * of this change, and not because the read joined anything: a time-series append is not part of the enclosing
 * transaction at all. {@code TimeSeriesShard.appendSamples} wraps the mutable-bucket write in its own
 * {@code db.begin()}/{@code db.commit()}, and an ArcadeDB nested transaction is an independent transaction
 * rather than a savepoint, so the sample is already committed and already global.
 * {@link #timeSeriesAppendsAreNotPartOfTheEnclosingTransaction} measures exactly that, and #7410 tracks it.
 * <p>
 * The issue's other premise - that the HTTP routes already bind the session transaction through
 * {@code DatabaseAbstractHandler} - is also not so: all three {@code /api/v1/ts} handlers extend
 * {@code AbstractServerHttpHandler}, which never reads {@code arcadedb-session-id}. #7402 tracks that.
 */
class Issue7370GrpcTimeSeriesInTransactionIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT = 50051;
  private static final String TYPE      = "GrpcTx7370";
  private static final String DOC_TYPE  = "GrpcTx7370Doc";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     http;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (grpc != null) {
      try {
        if (grpc.isTransactionActive())
          grpc.rollback();
      } catch (final Throwable ignore) {
        // the test has already failed; do not mask it with a teardown failure
      }
      grpc.close();
      grpc = null;
    }
    if (http != null) {
      http.close();
      http = null;
    }
    if (grpcServer != null) {
      grpcServer.close();
      grpcServer = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /** The port the test server actually bound: a stranger already on 2480 pushes this one up. */
  private int httpPort() {
    return getServer(0).getHttpServer().getPort();
  }

  private RemoteGrpcDatabase grpcClient() {
    if (grpc == null) {
      grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
      grpc = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, httpPort(), getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS);
    }
    return grpc;
  }

  private RemoteDatabase httpClient() {
    if (http == null)
      http = new RemoteDatabase("127.0.0.1", httpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    return http;
  }

  /** Two committed samples at t=1000 and t=2000, plus a DOCUMENT type used as a transaction witness. */
  private void seed() {
    // SHARDS 4 explicitly: the shard count decides whether aggregateMulti fans its sealed reads out across the
    // engine's shard executor while the mutable half stays on the calling thread - which is now the
    // transaction's thread. Left to the default it is ASYNC_WORKER_THREADS, so the branch under test would
    // depend on the core count of whoever runs the suite.
    grpcClient().command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE) SHARDS 4");
    grpcClient().command("sql", "CREATE DOCUMENT TYPE " + DOC_TYPE);
    grpcClient().timeSeriesWrite(List.of(
        new TimeSeriesPoint(TYPE, 1_000L, Map.of("location", "us-east"), Map.of("temperature", 10.0)),
        new TimeSeriesPoint(TYPE, 2_000L, Map.of("location", "us-east"), Map.of("temperature", 20.0))));
  }

  private void insertSample(final long timestamp, final double temperature) {
    grpcClient().command("sql", "INSERT INTO " + TYPE + " SET ts = ?, location = ?, temperature = ?",
        timestamp, "us-east", temperature);
  }

  private static List<Long> timestampsOf(final TimeSeriesQueryResult result) {
    final List<Long> timestamps = new ArrayList<>();
    for (final Object[] row : result.rows())
      timestamps.add(((Number) row[0]).longValue());
    return timestamps;
  }

  private static TimeSeriesQuery wholeRange() {
    return new TimeSeriesQuery(TYPE).from(0).to(10_000);
  }

  private static TimeSeriesQuery sumOverTheWholeRange() {
    return wholeRange()
        .aggregate(10_000L, new TimeSeriesQuery.Aggregation("temperature", AggregationType.SUM, "total"));
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The defect: an unknown transaction id was read straight through
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * A non-blank transaction id the server no longer knows must fail loudly rather than silently reading outside
   * the transaction the caller believes it is inside - the contract {@code lookupByRid}, {@code updateRecord}
   * and the three search RPCs (#7326) already carry. Before this branch the field did not exist, so the request
   * could not even express the mistake; the wrong answer was an ordinary-looking result set.
   * <p>
   * Driven through a raw stub because {@link RemoteGrpcDatabase} stamps its own transaction id and so cannot
   * express a wrong one.
   */
  @Test
  void aTimeSeriesQueryNamingAnUnknownTransactionIsRejected() {
    seed();

    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub stub = rawStub();
    final TimeSeriesQueryRequest request = TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(rawCredentials())
        .setType(TYPE)
        .setTransaction(TransactionContext.newBuilder().setTransactionId("no-such-tx-7370").build())
        .build();

    assertThatThrownBy(() -> drain(stub.timeSeriesQuery(request)))
        .hasMessageContaining("no-such-tx-7370");
  }

  @Test
  void aTimeSeriesLatestNamingAnUnknownTransactionIsRejected() {
    seed();

    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub stub = rawStub();
    final TimeSeriesLatestRequest request = TimeSeriesLatestRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(rawCredentials())
        .setType(TYPE)
        .setTransaction(TransactionContext.newBuilder().setTransactionId("no-such-tx-7370").build())
        .build();

    assertThatThrownBy(() -> stub.timeSeriesLatest(request))
        .hasMessageContaining("no-such-tx-7370");
  }

  /**
   * A blank id is not a supplied one: a request carrying an empty {@code TransactionContext} means "no external
   * transaction" and must keep working rather than being refused as unknown. This is the boundary the
   * FAILED_PRECONDITION above must not cross, and it is the shape a client that always sets the message would
   * send.
   */
  @Test
  void aBlankTransactionIdIsNotTreatedAsAnUnknownOne() throws Exception {
    seed();

    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub stub = rawStub();
    final TransactionContext blank = TransactionContext.newBuilder().setDatabase(getDatabaseName()).build();

    assertThat(streamTimestamps(stub, TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).setType(TYPE)
        .setTransaction(blank).build()))
        .containsExactly(1_000L, 2_000L);

    assertThat(stub.timeSeriesLatest(TimeSeriesLatestRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).setType(TYPE)
        .setTransaction(blank).build()).getFound()).isTrue();
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The new dispatch: the read runs on the transaction's own thread
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * The raw-row branch ({@code streamTimeSeriesRows}), now walked on the transaction's executor thread, must
   * still produce the whole series in order - including the sample this transaction just appended.
   */
  @Test
  void timeSeriesQueryInsideATransactionReturnsTheWholeSeries() {
    seed();

    grpcClient().begin();
    try {
      insertSample(3_000L, 30.0);

      assertThat(timestampsOf(grpcClient().timeSeriesQuery(wholeRange())))
          .as("the query runs on the transaction's thread and still answers the complete series")
          .containsExactly(1_000L, 2_000L, 3_000L);
    } finally {
      grpcClient().rollback();
    }
  }

  /**
   * A read issued before a write in the same transaction, and then again after it, must not be pinned to the
   * transaction's first snapshot. Dispatching onto the transaction's thread is exactly what could have
   * introduced that (an open transaction caches the pages it reads), so the order query - insert - query is
   * asserted rather than only insert - query.
   */
  @Test
  void aSecondQueryInTheSameTransactionSeesWhatWasAppendedBetweenThem() {
    seed();

    grpcClient().begin();
    try {
      assertThat(timestampsOf(grpcClient().timeSeriesQuery(wholeRange()))).containsExactly(1_000L, 2_000L);

      insertSample(3_000L, 30.0);

      assertThat(timestampsOf(grpcClient().timeSeriesQuery(wholeRange())))
          .as("the transaction's page cache must not pin the second read to the pre-append snapshot")
          .containsExactly(1_000L, 2_000L, 3_000L);
    } finally {
      grpcClient().rollback();
    }
  }

  /**
   * The aggregated branch is computed by {@code aggregateMulti} rather than {@code iterateQuery}, and that one
   * fans work out across the engine's shard executor, so it is a second entry point through the same handler
   * and needs its own run on the transaction's thread.
   */
  @Test
  void anAggregatedTimeSeriesQueryInsideATransactionReturnsTheRightBuckets() {
    seed();

    grpcClient().begin();
    try {
      insertSample(3_000L, 30.0);

      final TimeSeriesQueryResult aggregated = grpcClient().timeSeriesQuery(sumOverTheWholeRange());

      assertThat(aggregated.isAggregated()).isTrue();
      assertThat(aggregated.buckets()).hasSize(1);
      assertThat(((Number) aggregated.buckets().getFirst().values()[0]).doubleValue())
          .as("10.0 + 20.0 seeded + 30.0 appended in this transaction")
          .isEqualTo(60.0);
    } finally {
      grpcClient().rollback();
    }
  }

  @Test
  void timeSeriesLatestInsideATransactionReturnsTheNewestSample() {
    seed();

    grpcClient().begin();
    try {
      insertSample(3_000L, 30.0);

      final TimeSeriesLatestResult latest = grpcClient().timeSeriesLatest(TYPE);
      assertThat(latest.latest()).isNotNull();
      assertThat(((Number) latest.latest()[0]).longValue()).isEqualTo(3_000L);
    } finally {
      grpcClient().rollback();
    }
  }

  /**
   * A tag-filtered {@code TimeSeriesLatest} inside a transaction: the filter is built and applied on the
   * transaction's thread too, so it gets its own pass rather than being assumed to follow from the unfiltered
   * one.
   */
  @Test
  void aTagFilteredTimeSeriesLatestInsideATransactionSelectsItsOwnSeries() {
    seed();
    grpcClient().timeSeriesWrite(List.of(
        new TimeSeriesPoint(TYPE, 2_500L, Map.of("location", "eu-west"), Map.of("temperature", 5.0))));

    grpcClient().begin();
    try {
      insertSample(3_000L, 30.0); // us-east

      assertThat(((Number) grpcClient().timeSeriesLatest(TYPE, "location", "eu-west").latest()[0]).longValue())
          .isEqualTo(2_500L);
      assertThat(((Number) grpcClient().timeSeriesLatest(TYPE, "location", "us-east").latest()[0]).longValue())
          .isEqualTo(3_000L);
    } finally {
      grpcClient().rollback();
    }
  }

  /**
   * Running the stream on the transaction's single-threaded executor puts {@code emitTimeSeriesBatch} - and the
   * bounded {@code waitUntilReady} inside it - on that one thread. A stream cut into several messages is what
   * exercises that more than once, so this drives {@code batch_size = 1} over five rows through a raw stub
   * ({@link TimeSeriesQuery} exposes no batch size) and asserts the whole answer still arrives, in order, with
   * the terminal message flagged.
   */
  @Test
  void aMultiMessageTimeSeriesStreamRunsToCompletionOnTheTransactionThread() throws Exception {
    seed();

    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub stub = rawStub();
    final BeginTransactionResponse begun = stub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).build());
    final String txId = begun.getTransactionId();
    assertThat(txId).isNotBlank();

    final TransactionContext tx = TransactionContext.newBuilder()
        .setTransactionId(txId).setDatabase(getDatabaseName()).build();
    try {
      for (long ts = 3_000L; ts <= 5_000L; ts += 1_000L)
        stub.executeCommand(ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).setTransaction(tx)
            .setLanguage("sql")
            .setCommand("INSERT INTO " + TYPE + " SET ts = " + ts + ", location = 'us-east', temperature = "
                + (ts / 100.0))
            .build());

      final List<Long> timestamps = new ArrayList<>();
      int messages = 0;
      boolean sawLast = false;

      final BlockingClientCall<?, com.arcadedb.server.grpc.TimeSeriesQueryResult> call =
          stub.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
              .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).setTransaction(tx)
              .setType(TYPE).setBatchSize(1).build());
      while (call.hasNext()) {
        final com.arcadedb.server.grpc.TimeSeriesQueryResult message = call.read();
        if (message == null)
          break;
        messages++;
        for (final TimeSeriesRow row : message.getRowsList())
          timestamps.add(row.getValues(0).getInt64Value());
        if (message.getLast())
          sawLast = true;
      }

      assertThat(timestamps)
          .as("every row, across a stream cut into single-row messages on the transaction's own thread")
          .containsExactly(1_000L, 2_000L, 3_000L, 4_000L, 5_000L);
      assertThat(messages).as("batch_size=1 over 5 rows cannot arrive in one message").isGreaterThan(1);
      assertThat(sawLast).isTrue();
    } finally {
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
          .setTransaction(tx).setCredentials(rawCredentials()).build());
    }
  }

  /**
   * An error raised while the stream runs on the transaction's executor must reach the client as the status it
   * was raised with, not flattened to INTERNAL by the {@link java.util.concurrent.ExecutionException} the
   * executor wraps it in. Two are asserted because they are raised in different places: INVALID_ARGUMENT comes
   * from {@code streamTimeSeriesBuckets} after dispatch, NOT_FOUND from the type resolution that opens
   * {@code streamTimeSeries}.
   */
  @Test
  void anErrorRaisedInsideTheTransactionKeepsItsStatus() throws Exception {
    seed();

    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub stub = rawStub();
    final BeginTransactionResponse begun = stub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).build());
    final TransactionContext tx = TransactionContext.newBuilder()
        .setTransactionId(begun.getTransactionId()).setDatabase(getDatabaseName()).build();
    try {
      assertThatThrownBy(() -> drain(stub.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).setTransaction(tx).setType(TYPE)
          .setAggregation(com.arcadedb.server.grpc.TimeSeriesAggregation.newBuilder()
              .setBucketIntervalMs(0)
              .addRequests(com.arcadedb.server.grpc.TimeSeriesAggregationRequest.newBuilder()
                  .setField("temperature")
                  .setType(com.arcadedb.server.grpc.TimeSeriesAggregationType.TS_AGG_SUM)))
          .build())))
          .hasMessageContaining("INVALID_ARGUMENT")
          .hasMessageContaining("bucket_interval_ms");

      assertThatThrownBy(() -> drain(stub.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(rawCredentials()).setTransaction(tx)
          .setType("NoSuchTimeSeriesType7370").build())))
          .hasMessageContaining("NOT_FOUND");
    } finally {
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
          .setTransaction(tx).setCredentials(rawCredentials()).build());
    }
  }

  /**
   * With no transaction open the read must keep running inline on the gRPC worker against a freshly authorized
   * handle. The fix adds a branch; it does not move the default.
   */
  @Test
  void aQueryWithNoTransactionStillAnswersFromOutsideEveryTransaction() {
    seed();

    assertThat(timestampsOf(grpcClient().timeSeriesQuery(wholeRange()))).containsExactly(1_000L, 2_000L);
    assertThat(((Number) grpcClient().timeSeriesLatest(TYPE).latest()[0]).longValue()).isEqualTo(2_000L);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The two premises of #7370 that do not hold, pinned (issues #7410 and #7402)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * #7370 reasons from "a client that opened a gRPC transaction, appended points through some other RPC in it
   * and then queries the series reads outside its own transaction". There is nothing to miss: a time-series
   * append is not part of the enclosing transaction. {@code TimeSeriesShard.appendSamples} wraps the
   * mutable-bucket write in its own {@code db.begin()}/{@code db.commit()}, and {@code LocalDatabase.begin()}
   * pushes a <i>new</i> {@code TransactionContext} when one is already active rather than joining it, so that
   * inner {@code commit()} really commits.
   * <p>
   * The DOCUMENT insert in the same transaction is the control: it proves the statements ran inside a live
   * transaction that really did roll back, so "the sample survived" cannot be explained by the transaction
   * never having existed. #7410 tracks the divergence between this and {@code TimeSeriesEngine}'s javadoc.
   */
  @Test
  void timeSeriesAppendsAreNotPartOfTheEnclosingTransaction() {
    seed();

    grpcClient().begin();
    try {
      grpcClient().command("sql", "INSERT INTO " + DOC_TYPE + " SET name = 'witness'");
      insertSample(3_000L, 30.0);

      // A second, independent connection: no transaction of its own, and not the one that wrote.
      try (final RemoteDatabase other = new RemoteDatabase("127.0.0.1", httpPort(), getDatabaseName(), "root",
          DEFAULT_PASSWORD_FOR_TESTS)) {
        assertThat(timestampsOf(other.timeSeriesQuery(wholeRange())))
            .as("#7410: the sample is already committed and already global before any commit here")
            .containsExactly(1_000L, 2_000L, 3_000L);
        assertThat(countOfWitnesses(other))
            .as("the DOCUMENT row written in the same transaction is correctly invisible outside it")
            .isZero();
      }
    } finally {
      grpcClient().rollback();
    }

    assertThat(timestampsOf(grpcClient().timeSeriesQuery(wholeRange())))
        .as("#7410: the rollback did not take the sample with it")
        .containsExactly(1_000L, 2_000L, 3_000L);
    assertThat(countOfWitnesses(grpcClient()))
        .as("the rollback did take the DOCUMENT row, so the transaction was real")
        .isZero();
  }

  /**
   * #7370's other premise: that the HTTP routes bind the session's transaction through
   * {@code DatabaseAbstractHandler}. They do not - all three {@code /api/v1/ts} handlers extend
   * {@code AbstractServerHttpHandler}, which never reads {@code arcadedb-session-id}. Asserted on the DOCUMENT
   * witness rather than on the samples, since #7410 means the samples cannot tell the two apart: a session's
   * own uncommitted document row is invisible to {@code POST /api/v1/ts/{db}/query}'s database handle in the
   * sense that the handle is not the session's at all. #7402 tracks closing it; when it is closed the third
   * assertion here fails and names the claim to revisit.
   */
  @Test
  void theHttpTimeSeriesRoutesDoNotYetJoinTheSessionTransaction() {
    seed();

    final RemoteDatabase session = httpClient();
    session.begin();
    try {
      session.command("sql", "INSERT INTO " + DOC_TYPE + " SET name = 'witness'");

      // The session's own SQL does see its uncommitted row: DatabaseAbstractHandler binds the transaction for
      // /api/v1/command. This is the contrast that makes the next assertion mean something.
      assertThat(countOfWitnesses(session))
          .as("POST /api/v1/command does bind arcadedb-session-id")
          .isEqualTo(1L);

      session.command("sql", "INSERT INTO " + TYPE + " SET ts = 3000, location = 'us-east', temperature = 30.0");
      assertThat(timestampsOf(session.timeSeriesQuery(wholeRange()))).containsExactly(1_000L, 2_000L, 3_000L);

      // #7402: the TS routes take no session id at all. Proven by the handler hierarchy rather than by the
      // samples, which #7410 makes indistinguishable; asserted here so the pin is executable.
      assertThat(DatabaseAbstractHandler.class.isAssignableFrom(PostTimeSeriesQueryHandler.class))
          .as("#7402: PostTimeSeriesQueryHandler does not extend DatabaseAbstractHandler, so it never reads "
              + "arcadedb-session-id")
          .isFalse();
      assertThat(DatabaseAbstractHandler.class.isAssignableFrom(GetTimeSeriesLatestHandler.class))
          .as("#7402: same for GetTimeSeriesLatestHandler")
          .isFalse();
    } finally {
      session.rollback();
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  Plumbing
  // ─────────────────────────────────────────────────────────────────────────────

  private static long countOfWitnesses(final RemoteDatabase db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM " + DOC_TYPE)) {
      return ((Number) rs.next().getProperty("cnt")).longValue();
    }
  }

  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub rawStub() {
    grpcClient(); // ensures grpcServer is connected
    return grpcServer.newBlockingStub(30_000, getDatabaseName());
  }

  private DatabaseCredentials rawCredentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private static List<Long> streamTimestamps(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub stub,
      final TimeSeriesQueryRequest request) throws Exception {
    final List<Long> timestamps = new ArrayList<>();
    final BlockingClientCall<?, com.arcadedb.server.grpc.TimeSeriesQueryResult> call = stub.timeSeriesQuery(request);
    while (call.hasNext()) {
      final com.arcadedb.server.grpc.TimeSeriesQueryResult message = call.read();
      if (message == null)
        break;
      for (final TimeSeriesRow row : message.getRowsList())
        timestamps.add(row.getValues(0).getInt64Value());
    }
    return timestamps;
  }

  /** Consumes a server-streaming answer purely for its terminal status. */
  private static void drain(final BlockingClientCall<?, com.arcadedb.server.grpc.TimeSeriesQueryResult> call)
      throws Exception {
    while (call.hasNext())
      if (call.read() == null)
        return;
  }
}
