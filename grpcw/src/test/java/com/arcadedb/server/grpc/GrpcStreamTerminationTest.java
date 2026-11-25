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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * How the gRPC handlers end a call: at most one terminal {@code onCompleted}/{@code onError} per call, a client that already
 * closed it does not turn a success into a failure, and an interrupted stream stops instead of running on. Each handler family
 * keeps its own service and schema in a {@code @Nested} class; the server is this class's.
 */
public class GrpcStreamTerminationTest extends BaseGraphServerTest {

  // Regression: #6756
  /**
   * Regression test for issue #6756 (2): every unary gRPC handler must guard its catch block against
   * calling {@code onError} once {@code onNext} has already handed a response to the observer. A
   * concurrent client-side cancel landing between {@code onNext} and {@code onCompleted} can make
   * {@code onCompleted()} throw; without the {@code responded} guard (already present on
   * {@code executeCommand} since issue #6192) the catch block would call {@code onError} on an
   * already-closed call, letting an {@code IllegalStateException} ("call already closed") escape the
   * handler instead of the call simply completing successfully from the caller's point of view.
   * <p>
   * Calls each handler directly against a real {@link ArcadeDbGrpcService} bound to the test server's
   * database (no gRPC transport involved) with a mocked {@link StreamObserver} whose {@code onCompleted()}
   * throws, and asserts {@code onError} is never invoked afterward.
   */
  @Nested
  class DataServiceDoubleTerminateGuard {
    private static final String TYPE_NAME = "Issue6756Vertex";

    private ArcadeDbGrpcService service;

    @BeforeEach
    void setupService() {
      final String databasePath = getServer(0).getRootPath() + File.separator + "databases";
      // Idle/age/period all zero: no reaper thread is started, keeping this a pure unit-style test.
      service = new ArcadeDbGrpcService(databasePath, getServer(0), 0L, 0L, 0L);

      final ExecuteCommandResponse resp = executeCommand("CREATE VERTEX TYPE " + TYPE_NAME + " IF NOT EXISTS");
      assertSuccess(resp);
    }

    @AfterEach
    void teardownService() {
      if (service != null)
        service.close();
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private void assertSuccess(final ExecuteCommandResponse resp) {
      if (!resp.getSuccess())
        throw new AssertionError("setup command failed: " + resp.getMessage());
    }

    /** Runs a command through the real (non-mocked) path, used only for test setup/fixtures. */
    private ExecuteCommandResponse executeCommand(final String sql) {
      final ExecuteCommandRequest req = ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build();
      @SuppressWarnings("unchecked")
      final StreamObserver<ExecuteCommandResponse> resp = mock(StreamObserver.class);
      service.executeCommand(req, resp);
      final org.mockito.ArgumentCaptor<ExecuteCommandResponse> captor =
          org.mockito.ArgumentCaptor.forClass(ExecuteCommandResponse.class);
      verify(resp).onNext(captor.capture());
      return captor.getValue();
    }

    private String insertOneRecordAndGetRid() {
      final ExecuteCommandResponse resp = executeCommand(
          "INSERT INTO " + TYPE_NAME + " SET k = 'v-" + System.nanoTime() + "'");
      assertSuccess(resp);
      final ExecuteQueryResponse q = executeCommandAsQuery("SELECT FROM " + TYPE_NAME + " LIMIT 1");
      return q.getResults(0).getRecords(0).getRid();
    }

    private ExecuteQueryResponse executeCommandAsQuery(final String sql) {
      final ExecuteQueryRequest req = ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(sql).build();
      @SuppressWarnings("unchecked")
      final StreamObserver<ExecuteQueryResponse> resp = mock(StreamObserver.class);
      service.executeQuery(req, resp);
      final org.mockito.ArgumentCaptor<ExecuteQueryResponse> captor =
          org.mockito.ArgumentCaptor.forClass(ExecuteQueryResponse.class);
      verify(resp).onNext(captor.capture());
      return captor.getValue();
    }

    @Test
    void createRecordDoesNotDoubleTerminateWhenOnCompletedThrows() {
      @SuppressWarnings("unchecked")
      final StreamObserver<CreateRecordResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final CreateRecordRequest req = CreateRecordRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setType(TYPE_NAME).build();

      service.createRecord(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void lookupByRidDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final String rid = insertOneRecordAndGetRid();

      @SuppressWarnings("unchecked")
      final StreamObserver<LookupByRidResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final LookupByRidRequest req = LookupByRidRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setRid(rid).build();

      service.lookupByRid(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void updateRecordDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final String rid = insertOneRecordAndGetRid();

      @SuppressWarnings("unchecked")
      final StreamObserver<UpdateRecordResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final UpdateRecordRequest req = UpdateRecordRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setRid(rid)
          .setPartial(PropertiesUpdate.newBuilder()
              .putProperties("k", GrpcValue.newBuilder().setStringValue("updated").build()).build())
          .build();

      service.updateRecord(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void deleteRecordDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final String rid = insertOneRecordAndGetRid();

      @SuppressWarnings("unchecked")
      final StreamObserver<DeleteRecordResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final DeleteRecordRequest req = DeleteRecordRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setRid(rid).build();

      service.deleteRecord(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void executeQueryDoesNotDoubleTerminateWhenOnCompletedThrows() {
      @SuppressWarnings("unchecked")
      final StreamObserver<ExecuteQueryResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final ExecuteQueryRequest req = ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery("SELECT FROM " + TYPE_NAME).build();

      service.executeQuery(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void bulkInsertDoesNotDoubleTerminateWhenOnCompletedThrows() {
      @SuppressWarnings("unchecked")
      final StreamObserver<InsertSummary> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final BulkInsertRequest req = BulkInsertRequest.newBuilder()
          .setOptions(InsertOptions.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
              .setTargetClass(TYPE_NAME).build())
          .addRows(GrpcRecord.newBuilder()
              .putProperties("k", GrpcValue.newBuilder().setStringValue("bulk").build()).build())
          .build();

      service.bulkInsert(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void beginTransactionDoesNotDoubleTerminateWhenOnCompletedThrows() {
      @SuppressWarnings("unchecked")
      final StreamObserver<BeginTransactionResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final BeginTransactionRequest req = BeginTransactionRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).build();

      service.beginTransaction(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    private String beginRealTransaction() {
      @SuppressWarnings("unchecked")
      final StreamObserver<BeginTransactionResponse> resp = mock(StreamObserver.class);
      final BeginTransactionRequest req = BeginTransactionRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).build();
      service.beginTransaction(req, resp);
      final org.mockito.ArgumentCaptor<BeginTransactionResponse> captor =
          org.mockito.ArgumentCaptor.forClass(BeginTransactionResponse.class);
      verify(resp).onNext(captor.capture());
      return captor.getValue().getTransactionId();
    }

    @Test
    void commitTransactionDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final String txId = beginRealTransaction();
      final TransactionContext txCtx = TransactionContext.newBuilder()
          .setTransactionId(txId).setDatabase(getDatabaseName()).build();

      @SuppressWarnings("unchecked")
      final StreamObserver<CommitTransactionResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final CommitTransactionRequest req = CommitTransactionRequest.newBuilder()
          .setCredentials(credentials()).setTransaction(txCtx).build();

      service.commitTransaction(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void rollbackTransactionDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final String txId = beginRealTransaction();
      final TransactionContext txCtx = TransactionContext.newBuilder()
          .setTransactionId(txId).setDatabase(getDatabaseName()).build();

      @SuppressWarnings("unchecked")
      final StreamObserver<RollbackTransactionResponse> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();

      final RollbackTransactionRequest req = RollbackTransactionRequest.newBuilder()
          .setCredentials(credentials()).setTransaction(txCtx).build();

      service.rollbackTransaction(req, resp);

      verify(resp).onNext(any());
      // Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw
      // alone does not distinguish "invoked and threw" from "never invoked" (CodeRabbit review, cycle 2).
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }
  }

  // Regression: #7035
  /**
   * Regression test for issue #7035: the {@code responded} guard issue #6756 added to every unary handler of
   * {@link ArcadeDbGrpcService} was never applied to its sibling {@link ArcadeDbGrpcAdminService}, registered on the
   * same server by the same plugin, whose seven handlers kept the pre-fix shape. A client cancel landing between
   * {@code onNext} and {@code onCompleted} makes {@code onCompleted()} throw, and the catch block then called
   * {@code onError} on an already-closed call.
   * <p>
   * Same technique as {@code DataServiceDoubleTerminateGuard}: each handler is called directly against a real
   * service bound to the test server, with a mocked observer whose {@code onCompleted()} throws, and {@code onError}
   * must never be invoked afterward.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AdminServiceDoubleTerminateGuard {
    private static final String CREATED_DATABASE = "Issue7035Created";
    private static final String DROPPED_DATABASE = "Issue7035Dropped";

    private ArcadeDbGrpcAdminService service;

    @BeforeEach
    void setupService() {
      service = new ArcadeDbGrpcAdminService(getServer(0), new DefaultCredentialsValidator());
    }

    @AfterEach
    void dropCreatedDatabases() {
      for (final String name : new String[] { CREATED_DATABASE, DROPPED_DATABASE })
        if (getServer(0).existsDatabase(name)) {
          getServer(0).getDatabaseForOperator(name).getEmbedded().drop();
          getServer(0).removeDatabase(name);
        }
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private static <T> StreamObserver<T> observerWhoseCompletionThrows() {
      @SuppressWarnings("unchecked")
      final StreamObserver<T> resp = mock(StreamObserver.class);
      doThrow(new RuntimeException("call already closed")).when(resp).onCompleted();
      return resp;
    }

    /**
     * Proves onCompleted() was actually reached (not skipped by an early return) - the configured throw alone does
     * not distinguish "invoked and threw" from "never invoked" - and that the throw was absorbed.
     */
    private static <T> void assertTerminatedOnce(final StreamObserver<T> resp) {
      verify(resp).onNext(any());
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void pingDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final StreamObserver<PingResponse> resp = observerWhoseCompletionThrows();
      service.ping(PingRequest.newBuilder().setCredentials(credentials()).build(), resp);
      assertTerminatedOnce(resp);
    }

    @Test
    void getServerInfoDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final StreamObserver<GetServerInfoResponse> resp = observerWhoseCompletionThrows();
      service.getServerInfo(GetServerInfoRequest.newBuilder().setCredentials(credentials()).build(), resp);
      assertTerminatedOnce(resp);
    }

    @Test
    void listDatabasesDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final StreamObserver<ListDatabasesResponse> resp = observerWhoseCompletionThrows();
      service.listDatabases(ListDatabasesRequest.newBuilder().setCredentials(credentials()).build(), resp);
      assertTerminatedOnce(resp);
    }

    @Test
    void existsDatabaseDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final StreamObserver<ExistsDatabaseResponse> resp = observerWhoseCompletionThrows();
      service.existsDatabase(
          ExistsDatabaseRequest.newBuilder().setCredentials(credentials()).setName(getDatabaseName()).build(), resp);
      assertTerminatedOnce(resp);
    }

    @Test
    void createDatabaseDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final StreamObserver<CreateDatabaseResponse> resp = observerWhoseCompletionThrows();
      service.createDatabase(
          CreateDatabaseRequest.newBuilder().setCredentials(credentials()).setName(CREATED_DATABASE).setType("graph").build(),
          resp);
      assertTerminatedOnce(resp);
    }

    @Test
    void dropDatabaseDoesNotDoubleTerminateWhenOnCompletedThrows() {
      getServer(0).createDatabase(DROPPED_DATABASE, ComponentFile.MODE.READ_WRITE);

      final StreamObserver<DropDatabaseResponse> resp = observerWhoseCompletionThrows();
      service.dropDatabase(
          DropDatabaseRequest.newBuilder().setCredentials(credentials()).setName(DROPPED_DATABASE).build(), resp);
      assertTerminatedOnce(resp);
    }

    @Test
    void getDatabaseInfoDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final StreamObserver<GetDatabaseInfoResponse> resp = observerWhoseCompletionThrows();
      service.getDatabaseInfo(
          GetDatabaseInfoRequest.newBuilder().setCredentials(credentials()).setName(getDatabaseName()).build(), resp);
      assertTerminatedOnce(resp);
    }
  }

  // Regression: #7887
  /**
   * Regression test for issue #7887: the two server-streaming handlers of {@link ArcadeDbGrpcService},
   * {@code streamQuery} and {@code timeSeriesQuery}, must terminate their call exactly once. Before the fix each sent
   * its terminal inside a {@code try} whose {@code catch} called {@code onError}, guarded only by the asynchronously
   * set {@code cancelled} flag, so a terminal that threw (a concurrent client cancel closing the call under it, the
   * #6756 shape) was followed by a second terminal on a closed call.
   * <p>
   * Each test drives one terminal site of one handler against a real service bound to the test server's database,
   * with a mocked observer whose terminal throws, and asserts that no second terminal follows and that nothing
   * escapes the handler.
   */
  @Nested
  class StreamingDoubleTerminateGuard {
    private static final String VERTEX_TYPE = "Issue7887Vertex";
    private static final String TS_TYPE     = "Issue7887Series";
    private static final long   BASE_TS     = 1_700_000_000_000L;

    private ArcadeDbGrpcService service;

    @BeforeEach
    void setupService() {
      final String databasePath = getServer(0).getRootPath() + File.separator + "databases";
      // Idle/age/period all zero: no reaper thread is started, keeping this a pure unit-style test.
      service = new ArcadeDbGrpcService(databasePath, getServer(0), 0L, 0L, 0L);

      executeCommand("CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS");
      for (int i = 0; i < 5; i++)
        executeCommand("INSERT INTO " + VERTEX_TYPE + " SET k = " + i);

      executeCommand("CREATE TIMESERIES TYPE " + TS_TYPE + " IF NOT EXISTS TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setType(TS_TYPE);
      for (int i = 0; i < 5; i++)
        write.addPoints(TimeSeriesPoint.newBuilder()
            .setTimestamp(BASE_TS + i * 1_000L)
            .putTags("host", GrpcValue.newBuilder().setStringValue("web1").build())
            .putFields("value", GrpcValue.newBuilder().setDoubleValue(i).build()));
      @SuppressWarnings("unchecked")
      final StreamObserver<TimeSeriesWriteSummary> writeResp = mock(StreamObserver.class);
      service.timeSeriesWrite(write.build(), writeResp);
      verify(writeResp).onCompleted();
    }

    @AfterEach
    void teardownService() {
      // Clears any interrupt a test aimed at this thread, so it cannot leak into the fixture's teardown.
      Thread.interrupted();
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS,
          GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.getDefValue());
      if (service != null)
        service.close();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // streamQuery
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void streamQueryDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final ServerCallStreamObserver<QueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onCompleted();

      assertThatCode(() -> service.streamQuery(streamQuery().build(), resp)).doesNotThrowAnyException();

      verify(resp, atLeastOnce()).onNext(any());
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void streamQueryInsideATransactionDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final String txId = beginTransaction();
      final ServerCallStreamObserver<QueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onCompleted();

      assertThatCode(() -> service.streamQuery(streamQuery().setTransaction(txRef(txId)).build(), resp))
          .doesNotThrowAnyException();

      verify(resp, atLeastOnce()).onNext(any());
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void streamQueryRejectingAnUnknownTransactionSendsOneTerminalEvenWhenItThrows() {
      final ServerCallStreamObserver<QueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.streamQuery(streamQuery().setTransaction(txRef("no-such-transaction")).build(), resp))
          .doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      verify(resp, never()).onCompleted();
    }

    @Test
    void streamQueryInterruptedInsideATransactionSendsOneTerminalEvenWhenItThrows() {
      final String txId = beginTransaction();
      final ServerCallStreamObserver<QueryResult> resp = readyObserver();
      interruptCallerOnFirstMessage(resp);
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.streamQuery(streamQuery().setTransaction(txRef(txId)).build(), resp))
          .doesNotThrowAnyException();

      final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
      verify(resp, times(1)).onError(error.capture());
      assertThat(error.getValue()).hasMessageContaining("interrupted");
      verify(resp, never()).onCompleted();
    }

    @Test
    void streamQueryWriteTimeoutSendsOneTerminalEvenWhenItThrows() {
      shortenStreamWriteTimeout();
      final ServerCallStreamObserver<QueryResult> resp = neverReadyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.streamQuery(streamQuery().build(), resp)).doesNotThrowAnyException();

      final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
      verify(resp, times(1)).onError(error.capture());
      assertThat(error.getValue()).hasMessageContaining("DEADLINE_EXCEEDED");
      verify(resp, never()).onCompleted();
    }

    @Test
    void streamQueryRejectingAnUnauthorizedCallerOnARealTransactionSendsOneTerminalEvenWhenItThrows() {
      final String txId = beginTransaction();
      final ServerCallStreamObserver<QueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      // A real transaction id with the wrong password: refused by resolveAuthorizedTransaction's own catch, not by
      // the unknown-transaction branch the test above drives.
      final DatabaseCredentials wrongPassword = DatabaseCredentials.newBuilder().setUsername("root")
          .setPassword(DEFAULT_PASSWORD_FOR_TESTS + "-wrong").build();
      assertThatCode(() -> service.streamQuery(
          streamQuery().setCredentials(wrongPassword).setTransaction(txRef(txId)).build(), resp))
          .doesNotThrowAnyException();

      final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
      verify(resp, times(1)).onError(error.capture());
      assertThat(error.getValue()).isInstanceOf(StatusRuntimeException.class);
      assertThat(((StatusRuntimeException) error.getValue()).getStatus().getCode()).isNotEqualTo(Status.Code.NOT_FOUND);
      verify(resp, never()).onCompleted();
    }

    @Test
    void streamQueryInsideATransactionWriteTimeoutSendsOneTerminalEvenWhenItThrows() {
      final String txId = beginTransaction();
      shortenStreamWriteTimeout();
      final ServerCallStreamObserver<QueryResult> resp = neverReadyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.streamQuery(streamQuery().setTransaction(txRef(txId)).build(), resp))
          .doesNotThrowAnyException();

      final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
      verify(resp, times(1)).onError(error.capture());
      assertThat(error.getValue()).hasMessageContaining("DEADLINE_EXCEEDED");
      verify(resp, never()).onCompleted();
    }

    /**
     * A DEADLINE_EXCEEDED terminal that throws something other than a StatusRuntimeException (the "call already
     * closed" IllegalStateException) must not skip the transaction outcome the request asked for: begin + commit
     * still commits, so the handler thread is not left holding an open transaction.
     */
    @Test
    void streamQueryWriteTimeoutStillAppliesTheRequestedCommitWhenItsTerminalThrows() {
      shortenStreamWriteTimeout();
      final ServerCallStreamObserver<QueryResult> resp = neverReadyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      final TransactionContext beginAndCommit = TransactionContext.newBuilder().setBegin(true).setCommit(true).build();
      assertThatCode(() -> service.streamQuery(streamQuery().setTransaction(beginAndCommit).build(), resp))
          .doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      // Transactions are thread-bound and the inline path ran on this thread against the server's database.
      assertThat(getServer(0).getDatabaseForOperator(getDatabaseName()).isTransactionActive()).isFalse();
    }

    @Test
    void streamQueryFailureErrorTerminalThatThrowsDoesNotEscape() {
      final ServerCallStreamObserver<QueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.streamQuery(streamQuery().setQuery("SELECT FROM NoSuchType7887").build(), resp))
          .doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      verify(resp, never()).onCompleted();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // timeSeriesQuery
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void timeSeriesQueryDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final ServerCallStreamObserver<TimeSeriesQueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onCompleted();

      assertThatCode(() -> service.timeSeriesQuery(timeSeriesQuery().build(), resp)).doesNotThrowAnyException();

      verify(resp, atLeastOnce()).onNext(any());
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void timeSeriesQueryInsideATransactionDoesNotDoubleTerminateWhenOnCompletedThrows() {
      final String txId = beginTransaction();
      final ServerCallStreamObserver<TimeSeriesQueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onCompleted();

      assertThatCode(() -> service.timeSeriesQuery(timeSeriesQuery().setTransaction(txRef(txId)).build(), resp))
          .doesNotThrowAnyException();

      verify(resp, atLeastOnce()).onNext(any());
      verify(resp).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void timeSeriesQueryInterruptedInsideATransactionSendsOneTerminalEvenWhenItThrows() {
      final String txId = beginTransaction();
      final ServerCallStreamObserver<TimeSeriesQueryResult> resp = readyObserver();
      interruptCallerOnFirstMessage(resp);
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.timeSeriesQuery(timeSeriesQuery().setTransaction(txRef(txId)).build(), resp))
          .doesNotThrowAnyException();

      final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
      verify(resp, times(1)).onError(error.capture());
      assertThat(error.getValue()).hasMessageContaining("interrupted");
      verify(resp, never()).onCompleted();
    }

    @Test
    void timeSeriesQueryWriteTimeoutSendsOneTerminalEvenWhenItThrows() {
      shortenStreamWriteTimeout();
      final ServerCallStreamObserver<TimeSeriesQueryResult> resp = neverReadyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.timeSeriesQuery(timeSeriesQuery().build(), resp)).doesNotThrowAnyException();

      final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
      verify(resp, times(1)).onError(error.capture());
      assertThat(error.getValue()).hasMessageContaining("DEADLINE_EXCEEDED");
      verify(resp, never()).onCompleted();
    }

    @Test
    void timeSeriesQueryInsideATransactionWriteTimeoutSendsOneTerminalEvenWhenItThrows() {
      final String txId = beginTransaction();
      shortenStreamWriteTimeout();
      final ServerCallStreamObserver<TimeSeriesQueryResult> resp = neverReadyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.timeSeriesQuery(timeSeriesQuery().setTransaction(txRef(txId)).build(), resp))
          .doesNotThrowAnyException();

      final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
      verify(resp, times(1)).onError(error.capture());
      assertThat(error.getValue()).hasMessageContaining("DEADLINE_EXCEEDED");
      verify(resp, never()).onCompleted();
    }

    @Test
    void timeSeriesQueryFailureErrorTerminalThatThrowsDoesNotEscape() {
      final ServerCallStreamObserver<TimeSeriesQueryResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onError(any());

      assertThatCode(() -> service.timeSeriesQuery(timeSeriesQuery().setType("NoSuchSeries7887").build(), resp))
          .doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      verify(resp, never()).onCompleted();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // fixtures
    // ---------------------------------------------------------------------------------------------------------------

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private StreamQueryRequest.Builder streamQuery() {
      return StreamQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setQuery("SELECT FROM " + VERTEX_TYPE).setBatchSize(1);
    }

    private TimeSeriesQueryRequest.Builder timeSeriesQuery() {
      return TimeSeriesQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setType(TS_TYPE).setBatchSize(1);
    }

    private static TransactionContext txRef(final String txId) {
      return TransactionContext.newBuilder().setTransactionId(txId).build();
    }

    @SuppressWarnings("unchecked")
    private static <T> ServerCallStreamObserver<T> readyObserver() {
      final ServerCallStreamObserver<T> resp = mock(ServerCallStreamObserver.class);
      when(resp.isReady()).thenReturn(true);
      return resp;
    }

    @SuppressWarnings("unchecked")
    private static <T> ServerCallStreamObserver<T> neverReadyObserver() {
      final ServerCallStreamObserver<T> resp = mock(ServerCallStreamObserver.class);
      when(resp.isReady()).thenReturn(false);
      return resp;
    }

    /** Makes the bounded transport-ready wait give up quickly, so the handler takes its DEADLINE_EXCEEDED branch. */
    private void shortenStreamWriteTimeout() {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS, 50L);
    }

    /**
     * A stream bound to a transaction runs on the transaction's executor thread while the handler waits for it in
     * {@code Future.get()}. Interrupting the handler thread from the first {@code onNext}, and holding the executor
     * there long enough for the interrupt to land, makes that wait throw {@link InterruptedException} - the branch
     * that answers with an explicit CANCELLED terminal.
     */
    private static void interruptCallerOnFirstMessage(final ServerCallStreamObserver<?> resp) {
      final Thread caller = Thread.currentThread();
      // One shot: the stream carries on on the executor after the handler returned, and a later message must not
      // interrupt the test thread again while it verifies or tears down.
      final AtomicBoolean fired = new AtomicBoolean();
      doAnswer(invocation -> {
        if (fired.compareAndSet(false, true)) {
          caller.interrupt();
          // Only keeps the executor busy, so Future.get() is still waiting (and throws) when the interrupt lands.
          Thread.sleep(500);
        }
        return null;
      }).when(resp).onNext(any());
    }

    private String beginTransaction() {
      @SuppressWarnings("unchecked")
      final StreamObserver<BeginTransactionResponse> resp = mock(StreamObserver.class);
      service.beginTransaction(BeginTransactionRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).build(), resp);
      final ArgumentCaptor<BeginTransactionResponse> captor = ArgumentCaptor.forClass(BeginTransactionResponse.class);
      verify(resp).onNext(captor.capture());
      return captor.getValue().getTransactionId();
    }

    private void executeCommand(final String sql) {
      @SuppressWarnings("unchecked")
      final StreamObserver<ExecuteCommandResponse> resp = mock(StreamObserver.class);
      service.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build(), resp);
      final ArgumentCaptor<ExecuteCommandResponse> captor = ArgumentCaptor.forClass(ExecuteCommandResponse.class);
      verify(resp).onNext(captor.capture());
      if (!captor.getValue().getSuccess())
        throw new AssertionError("setup command failed: " + sql + " -> " + captor.getValue().getMessage());
    }
  }

  // Regression: #8752
  /**
   * Regression test for issue #8752: when {@code streamQuery} or {@code timeSeriesQuery} runs inside a client
   * transaction and the handler's wait on the transaction's executor is interrupted, the handler answers CANCELLED and
   * returns, but the executor used to keep iterating the result set and calling {@code onNext} on the closed call. The
   * stream must stop at its next batch boundary instead.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class InterruptedInTransactionStreamStops {
    private static final String VERTEX_TYPE = "Issue8752Vertex";
    private static final String TS_TYPE     = "Issue8752Series";
    private static final long   BASE_TS     = 1_700_000_000_000L;
    private static final int    ROWS        = 8;

    private ArcadeDbGrpcService service;

    @BeforeEach
    void setupService() {
      final String databasePath = getServer(0).getRootPath() + File.separator + "databases";
      service = new ArcadeDbGrpcService(databasePath, getServer(0), 0L, 0L, 0L);

      executeCommand("CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS");
      for (int i = 0; i < ROWS; i++)
        executeCommand("INSERT INTO " + VERTEX_TYPE + " SET k = " + i);

      executeCommand("CREATE TIMESERIES TYPE " + TS_TYPE + " IF NOT EXISTS TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
      final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setType(TS_TYPE);
      for (int i = 0; i < ROWS; i++)
        write.addPoints(TimeSeriesPoint.newBuilder()
            .setTimestamp(BASE_TS + i * 1_000L)
            .putTags("host", GrpcValue.newBuilder().setStringValue("web1").build())
            .putFields("value", GrpcValue.newBuilder().setDoubleValue(i).build()));
      @SuppressWarnings("unchecked")
      final StreamObserver<TimeSeriesWriteSummary> writeResp = mock(StreamObserver.class);
      service.timeSeriesWrite(write.build(), writeResp);
      verify(writeResp).onCompleted();
    }

    @AfterEach
    void teardownService() {
      // Clears any interrupt a test aimed at this thread, so it cannot leak into the fixture's teardown.
      Thread.interrupted();
      if (service != null)
        service.close();
    }

    @Test
    void streamQueryStopsStreamingAfterTheHandlerWasInterrupted() {
      final String txId = beginTransaction();
      final AtomicInteger messages = new AtomicInteger();
      final ServerCallStreamObserver<QueryResult> resp = observerInterruptingOnFirstMessage(messages);

      assertThatCode(() -> service.streamQuery(StreamQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setQuery("SELECT FROM " + VERTEX_TYPE).setBatchSize(1)
          .setTransaction(txRef(txId)).build(), resp)).doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      assertExecutorStoppedAfterTheFirstMessage(txId, messages);
    }

    @Test
    void timeSeriesQueryStopsStreamingAfterTheHandlerWasInterrupted() {
      final String txId = beginTransaction();
      final AtomicInteger messages = new AtomicInteger();
      final ServerCallStreamObserver<TimeSeriesQueryResult> resp = observerInterruptingOnFirstMessage(messages);

      assertThatCode(() -> service.timeSeriesQuery(TimeSeriesQueryRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setType(TS_TYPE).setBatchSize(1)
          .setTransaction(txRef(txId)).build(), resp)).doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      assertExecutorStoppedAfterTheFirstMessage(txId, messages);
    }

    /**
     * Rolling the transaction back runs on the same single-threaded executor, so it only returns once the abandoned
     * stream task has ended: after it, the message count is final.
     */
    private void assertExecutorStoppedAfterTheFirstMessage(final String txId, final AtomicInteger messages) {
      // The handler restores the interrupt status it caught, which would abort the rollback's own wait
      Thread.interrupted();
      rollbackTransaction(txId);
      await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(messages.get()).isGreaterThanOrEqualTo(1));
      assertThat(messages.get()).as("messages written after the call was closed with CANCELLED").isEqualTo(1);
    }

    /**
     * The first {@code onNext} interrupts the handler thread and holds the executor long enough for the interrupt to
     * land while the handler waits in {@code Future.get()}.
     */
    @SuppressWarnings("unchecked")
    private static <T> ServerCallStreamObserver<T> observerInterruptingOnFirstMessage(final AtomicInteger messages) {
      final ServerCallStreamObserver<T> resp = mock(ServerCallStreamObserver.class);
      when(resp.isReady()).thenReturn(true);
      final Thread caller = Thread.currentThread();
      final AtomicBoolean fired = new AtomicBoolean();
      doAnswer(invocation -> {
        messages.incrementAndGet();
        if (fired.compareAndSet(false, true)) {
          caller.interrupt();
          Thread.sleep(500);
        }
        return null;
      }).when(resp).onNext(any());
      return resp;
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private static TransactionContext txRef(final String txId) {
      return TransactionContext.newBuilder().setTransactionId(txId).build();
    }

    private String beginTransaction() {
      @SuppressWarnings("unchecked")
      final StreamObserver<BeginTransactionResponse> resp = mock(StreamObserver.class);
      service.beginTransaction(BeginTransactionRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).build(), resp);
      final ArgumentCaptor<BeginTransactionResponse> captor = ArgumentCaptor.forClass(BeginTransactionResponse.class);
      verify(resp).onNext(captor.capture());
      return captor.getValue().getTransactionId();
    }

    private void rollbackTransaction(final String txId) {
      @SuppressWarnings("unchecked")
      final StreamObserver<RollbackTransactionResponse> resp = mock(StreamObserver.class);
      service.rollbackTransaction(RollbackTransactionRequest.newBuilder().setTransaction(txRef(txId)).setCredentials(credentials()).build(), resp);
      verify(resp).onCompleted();
    }

    private void executeCommand(final String sql) {
      @SuppressWarnings("unchecked")
      final StreamObserver<ExecuteCommandResponse> resp = mock(StreamObserver.class);
      service.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(sql).build(), resp);
      final ArgumentCaptor<ExecuteCommandResponse> captor = ArgumentCaptor.forClass(ExecuteCommandResponse.class);
      verify(resp).onNext(captor.capture());
      if (!captor.getValue().getSuccess())
        throw new AssertionError("setup command failed: " + sql + " -> " + captor.getValue().getMessage());
    }
  }

  // Regression: #8936
  /**
   * Regression guard for issue #8936: the two client-streaming handlers {@code insertStream} and {@code graphBatchLoad} send
   * their terminal from {@code onCompleted()} inside a {@code try} whose {@code catch} calls {@code onError}. The report
   * feared a second terminal on a call gRPC already closed when the first one throws, as #7887 fixed for the server-streaming
   * handlers. Both handlers answer through {@link SynchronizedStreamObserver}, which delegates at most one terminal per call
   * (a late or duplicate one is dropped) and swallows a terminal the transport already closed, so the terminate-once guarantee
   * holds by construction. These tests pin it with an observer whose terminal throws.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ClientStreamingDoubleTerminate {
    private static final String TYPE = "Issue8936Doc";

    private ArcadeDbGrpcService service;

    @BeforeEach
    void setupService() {
      final String databasePath = getServer(0).getRootPath() + File.separator + "databases";
      service = new ArcadeDbGrpcService(databasePath, getServer(0), 0L, 0L, 0L);
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
      getServer(0).getDatabaseForOperator(getDatabaseName()).command("sql", "CREATE VERTEX TYPE " + TYPE + "V IF NOT EXISTS");
    }

    @AfterEach
    void teardownService() {
      if (service != null)
        service.close();
    }

    @Test
    void insertStreamSendsOneTerminalWhenOnCompletedThrows() {
      final ServerCallStreamObserver<InsertSummary> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onCompleted();

      final StreamObserver<InsertChunk> req = service.insertStream(resp);
      assertThatCode(() -> {
        req.onNext(insertChunk());
        req.onCompleted();
      }).doesNotThrowAnyException();

      verify(resp, times(1)).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void insertStreamSendsOneTerminalWhenTheFinalOnNextThrows() {
      final ServerCallStreamObserver<InsertSummary> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onNext(any());

      final StreamObserver<InsertChunk> req = service.insertStream(resp);
      assertThatCode(() -> {
        req.onNext(insertChunk());
        req.onCompleted();
      }).doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      verify(resp, never()).onCompleted();
    }

    @Test
    void graphBatchLoadSendsOneTerminalWhenOnCompletedThrows() {
      final ServerCallStreamObserver<GraphBatchResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onCompleted();

      final StreamObserver<GraphBatchChunk> req = service.graphBatchLoad(resp);
      assertThatCode(() -> {
        req.onNext(graphChunk());
        req.onCompleted();
      }).doesNotThrowAnyException();

      verify(resp, times(1)).onCompleted();
      verify(resp, never()).onError(any());
    }

    @Test
    void graphBatchLoadSendsOneTerminalWhenTheFinalOnNextThrows() {
      final ServerCallStreamObserver<GraphBatchResult> resp = readyObserver();
      doThrow(new IllegalStateException("call already closed")).when(resp).onNext(any());

      final StreamObserver<GraphBatchChunk> req = service.graphBatchLoad(resp);
      assertThatCode(() -> {
        req.onNext(graphChunk());
        req.onCompleted();
      }).doesNotThrowAnyException();

      verify(resp, times(1)).onError(any());
      verify(resp, never()).onCompleted();
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private InsertChunk insertChunk() {
      return InsertChunk.newBuilder()
          .setSessionId("issue-8936")
          .setChunkSeq(0)
          .setLast(true)
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setOptions(InsertOptions.newBuilder()
              .setCredentials(credentials())
              .setTargetClass(TYPE)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(TYPE)
              .putProperties("name", GrpcValue.newBuilder().setStringValue("row").build()).build())
          .build();
    }

    private GraphBatchChunk graphChunk() {
      return GraphBatchChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .addRecords(GraphBatchRecord.newBuilder()
              .setKind(GraphBatchRecord.Kind.VERTEX)
              .setTypeName(TYPE + "V")
              .setTempId("v0")
              .putProperties("idx", GrpcValue.newBuilder().setInt32Value(0).build())
              .build())
          .build();
    }

    @SuppressWarnings("unchecked")
    private static <T> ServerCallStreamObserver<T> readyObserver() {
      final ServerCallStreamObserver<T> resp = mock(ServerCallStreamObserver.class);
      when(resp.isReady()).thenReturn(true);
      return resp;
    }
  }
}
