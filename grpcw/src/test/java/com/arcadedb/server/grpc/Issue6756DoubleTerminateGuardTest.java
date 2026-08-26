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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
public class Issue6756DoubleTerminateGuardTest extends BaseGraphServerTest {

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
