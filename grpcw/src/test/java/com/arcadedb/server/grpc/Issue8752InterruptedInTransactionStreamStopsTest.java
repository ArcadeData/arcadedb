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

import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8752: when {@code streamQuery} or {@code timeSeriesQuery} runs inside a client
 * transaction and the handler's wait on the transaction's executor is interrupted, the handler answers CANCELLED and
 * returns, but the executor used to keep iterating the result set and calling {@code onNext} on the closed call. The
 * stream must stop at its next batch boundary instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8752InterruptedInTransactionStreamStopsTest extends BaseGraphServerTest {

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
