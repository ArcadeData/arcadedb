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

import java.io.File;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class Issue8936ClientStreamingDoubleTerminateTest extends BaseGraphServerTest {

  private static final String TYPE = "Issue8936Doc";

  private ArcadeDbGrpcService service;

  @BeforeEach
  void setupService() {
    final String databasePath = getServer(0).getRootPath() + File.separator + "databases";
    service = new ArcadeDbGrpcService(databasePath, getServer(0), 0L, 0L, 0L);
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    getServer(0).getDatabase(getDatabaseName()).command("sql", "CREATE VERTEX TYPE " + TYPE + "V IF NOT EXISTS");
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
