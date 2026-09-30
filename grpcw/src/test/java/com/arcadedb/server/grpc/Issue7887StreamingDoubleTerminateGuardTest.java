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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
public class Issue7887StreamingDoubleTerminateGuardTest extends BaseGraphServerTest {

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
    assertThat(getServer(0).getDatabase(getDatabaseName()).isTransactionActive()).isFalse();
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
