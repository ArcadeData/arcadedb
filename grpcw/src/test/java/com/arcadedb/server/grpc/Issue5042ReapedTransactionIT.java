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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Regression test for issue #5042: reaped/unknown gRPC transactions must not cause silent data loss or
 * partial commits.
 *
 * <p>Covers the server-side guarantees:
 * <ul>
 *   <li>Write RPCs ({@code executeCommand}, {@code createRecord}, {@code updateRecord},
 *       {@code deleteRecord}) reject a non-blank-but-unknown transaction id with
 *       {@code FAILED_PRECONDITION} instead of silently falling through to a per-call auto-commit.</li>
 *   <li>The {@code lookupByRid} read RPC applies the same rejection: a non-blank-but-unknown transaction id
 *       fails with {@code FAILED_PRECONDITION} rather than silently serving an out-of-transaction read.</li>
 *   <li>A genuinely absent/blank transaction id still takes the auto-transaction path.</li>
 *   <li>{@code streamQuery} honors the supplied transaction id and runs inside the transaction, so a
 *       streamed read sees the transaction's own uncommitted writes.</li>
 *   <li>{@code streamQuery} rejects a non-blank-but-unknown transaction id with {@code FAILED_PRECONDITION}
 *       instead of silently running as a throwaway read.</li>
 * </ul>
 */
public class Issue5042ReapedTransactionIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
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

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  @Test
  void writeRpcsRejectUnknownNonBlankTransactionId() {
    // A non-blank transaction id the server never registered (the reaped/unknown case).
    final String unknownTxId = "reaped-" + UUID.randomUUID();
    final TransactionContext unknownTx = TransactionContext.newBuilder()
        .setTransactionId(unknownTxId).setDatabase(getDatabaseName()).build();

    // executeCommand
    final StatusRuntimeException cmdErr = catchThrowableOfType(StatusRuntimeException.class, () ->
        authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand("INSERT INTO Person SET name = 'reaped-write'")
            .setTransaction(unknownTx)
            .build()));
    assertThat(cmdErr).as("executeCommand must reject an unknown non-blank txId").isNotNull();
    assertThat(cmdErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

    // createRecord
    final StatusRuntimeException createErr = catchThrowableOfType(StatusRuntimeException.class, () ->
        authenticatedStub.createRecord(CreateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setType("Person")
            .setRecord(GrpcRecord.newBuilder()
                .putProperties("name", GrpcValue.newBuilder().setStringValue("reaped-create").build()).build())
            .setTransaction(unknownTx)
            .build()));
    assertThat(createErr).as("createRecord must reject an unknown non-blank txId").isNotNull();
    assertThat(createErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

    // updateRecord
    final StatusRuntimeException updateErr = catchThrowableOfType(StatusRuntimeException.class, () ->
        authenticatedStub.updateRecord(UpdateRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid("#1:0")
            .setPartial(PropertiesUpdate.newBuilder()
                .putProperties("name", GrpcValue.newBuilder().setStringValue("reaped-update").build()).build())
            .setTransaction(unknownTx)
            .build()));
    assertThat(updateErr).as("updateRecord must reject an unknown non-blank txId").isNotNull();
    assertThat(updateErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

    // deleteRecord
    final StatusRuntimeException deleteErr = catchThrowableOfType(StatusRuntimeException.class, () ->
        authenticatedStub.deleteRecord(DeleteRecordRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid("#1:0")
            .setTransaction(unknownTx)
            .build()));
    assertThat(deleteErr).as("deleteRecord must reject an unknown non-blank txId").isNotNull();
    assertThat(deleteErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

    // Nothing must have been persisted by any of the rejected writes.
    final ExecuteQueryResponse q = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name IN ['reaped-write','reaped-create','reaped-update']")
        .build());
    assertThat(q.getResultsList().get(0).getRecordsList()).isEmpty();
  }

  @Test
  void lookupByRidRejectsUnknownNonBlankTransactionId() {
    // A non-blank transaction id the server never registered (the reaped/unknown case).
    final String unknownTxId = "reaped-" + UUID.randomUUID();
    final TransactionContext unknownTx = TransactionContext.newBuilder()
        .setTransactionId(unknownTxId).setDatabase(getDatabaseName()).build();

    final StatusRuntimeException lookupErr = catchThrowableOfType(StatusRuntimeException.class, () ->
        authenticatedStub.lookupByRid(LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid("#1:0")
            .setTransaction(unknownTx)
            .build()));
    assertThat(lookupErr).as("lookupByRid must reject an unknown non-blank txId").isNotNull();
    assertThat(lookupErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void streamQueryRejectsUnknownNonBlankTransactionId() {
    // A non-blank transaction id the server never registered (the reaped/unknown case).
    final String unknownTxId = "reaped-" + UUID.randomUUID();
    final TransactionContext unknownTx = TransactionContext.newBuilder()
        .setTransactionId(unknownTxId).setDatabase(getDatabaseName()).build();

    // The streamed read must fail loudly instead of silently running as a throwaway read. The error surfaces
    // when the stream is consumed, so drive the iterator inside the assertion.
    final StatusRuntimeException streamErr = catchThrowableOfType(StatusRuntimeException.class, () -> {
      final Iterator<QueryResult> it = authenticatedStub.streamQuery(StreamQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT FROM Person")
          .setBatchSize(10)
          .setRetrievalMode(StreamQueryRequest.RetrievalMode.CURSOR)
          .setTransaction(unknownTx)
          .build());
      while (it.hasNext())
        it.next();
    });
    assertThat(streamErr).as("streamQuery must reject an unknown non-blank txId").isNotNull();
    assertThat(streamErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void blankTransactionIdStillAutoCommits() {
    final String name = "auto-commit-" + UUID.randomUUID();

    // No transaction supplied at all: the write must auto-commit (genuinely absent txId path).
    final ExecuteCommandResponse resp = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand("INSERT INTO Person SET name = '" + name + "'")
        .build());
    assertThat(resp.getSuccess()).isTrue();

    final ExecuteQueryResponse q = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery("SELECT FROM Person WHERE name = '" + name + "'")
        .build());
    assertThat(q.getResultsList().get(0).getRecordsList())
        .as("blank/absent txId must still auto-commit the write").hasSize(1);
  }

  @Test
  void streamQueryHonorsTransactionIdAndSeesUncommittedWrites() {
    // Begin an externally-managed transaction.
    final BeginTransactionResponse begin = authenticatedStub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .build());
    final String txId = begin.getTransactionId();
    assertThat(txId).isNotEmpty();

    final TransactionContext txCtx = TransactionContext.newBuilder()
        .setTransactionId(txId).setDatabase(getDatabaseName()).build();

    try {
      final String name = "stream-tx-" + txId;

      // Write inside the transaction but DO NOT commit.
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setCommand("INSERT INTO Person SET name = '" + name + "'")
          .setTransaction(txCtx)
          .build());

      // A streamed query bound to the same transaction must see the uncommitted write.
      final Iterator<QueryResult> it = authenticatedStub.streamQuery(StreamQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT FROM Person WHERE name = '" + name + "'")
          .setBatchSize(10)
          .setRetrievalMode(StreamQueryRequest.RetrievalMode.CURSOR)
          .setTransaction(txCtx)
          .build());

      int seen = 0;
      while (it.hasNext())
        seen += it.next().getRecordsList().size();

      assertThat(seen).as("streamed query inside the transaction must see its uncommitted write").isEqualTo(1);
    } finally {
      authenticatedStub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
          .setCredentials(credentials())
          .setTransaction(txCtx)
          .build());
    }
  }
}
