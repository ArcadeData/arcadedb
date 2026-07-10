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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4214: {@code InsertStream} with {@code CONFLICT_IGNORE} +
 * {@code PER_STREAM} transaction mode rolls back the entire stream on a commit-time unique-index
 * violation instead of silently skipping the duplicate and committing the remaining rows.
 */
public class Issue4214InsertStreamConflictIgnoreIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncAuthenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    asyncAuthenticatedStub = ArcadeDbServiceGrpc.newStub(authenticatedChannel);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
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

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
  }

  @Test
  void conflictIgnorePerStreamShouldSkipDuplicateAndCommitRemainingRows() throws Exception {
    final String typeName = "Issue4214PerStreamType_" + System.currentTimeMillis();

    // Schema + unique index + one pre-existing row.
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName).build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".name STRING").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(name) UNIQUE").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("INSERT INTO " + typeName + " SET name = 'Alice'").build());

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      // Three rows: one fresh, one duplicate of the pre-existing Alice, one fresh.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4214-per-stream")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_IGNORE)
              .addKeyColumns("name")
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Bob")).build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Alice")).build())  // duplicate
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Carol")).build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      assertThat(errorRef.get()).as("stream must not abort with an error").isNull();
      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getReceived()).isEqualTo(3);
      assertThat(summary.getInserted()).isEqualTo(2);   // Bob + Carol
      assertThat(summary.getIgnored()).isEqualTo(1);    // Alice silently skipped
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      // Verify Bob and Carol were actually persisted alongside the pre-existing Alice.
      final ExecuteQueryResponse queryResp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT count(*) as cnt FROM " + typeName)
          .build());
      assertThat(queryResp.getResultsList()).isNotEmpty();
      final long count = queryResp.getResultsList().getFirst().getRecordsList().getFirst()
          .getPropertiesMap().get("cnt").getInt64Value();
      assertThat(count).isEqualTo(3);
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }

  @Test
  void conflictIgnorePerBatchShouldSkipDuplicateAndCommitRemainingRows() throws Exception {
    final String typeName = "Issue4214PerBatchType_" + System.currentTimeMillis();

    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE DOCUMENT TYPE " + typeName).build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".name STRING").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(name) UNIQUE").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("INSERT INTO " + typeName + " SET name = 'Alice'").build());

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4214-per-batch")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_IGNORE)
              .addKeyColumns("name")
              .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Bob")).build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Alice")).build())  // duplicate
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Carol")).build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      assertThat(errorRef.get()).as("stream must not abort with an error").isNull();
      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getReceived()).isEqualTo(3);
      assertThat(summary.getInserted()).isEqualTo(2);
      assertThat(summary.getIgnored()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      final ExecuteQueryResponse queryResp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT count(*) as cnt FROM " + typeName)
          .build());
      assertThat(queryResp.getResultsList()).isNotEmpty();
      final long count = queryResp.getResultsList().getFirst().getRecordsList().getFirst()
          .getPropertiesMap().get("cnt").getInt64Value();
      assertThat(count).isEqualTo(3);
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }

  @Test
  void conflictIgnorePerStreamShouldSkipDuplicateAndCommitRemainingRowsForVertices() throws Exception {
    final String typeName = "Issue4214PerStreamVertex_" + System.currentTimeMillis();

    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE VERTEX TYPE " + typeName).build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE PROPERTY " + typeName + ".name STRING").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE INDEX ON " + typeName + "(name) UNIQUE").build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE VERTEX " + typeName + " SET name = 'Alice'").build());

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { errorRef.set(t); done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-4214-per-stream-vertex")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_IGNORE)
              .addKeyColumns("name")
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Bob")).build())
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Alice")).build())  // duplicate
          .addRows(GrpcRecord.newBuilder().setType(typeName).putProperties("name", stringValue("Carol")).build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      assertThat(errorRef.get()).as("stream must not abort with an error").isNull();
      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getReceived()).isEqualTo(3);
      assertThat(summary.getInserted()).isEqualTo(2);
      assertThat(summary.getIgnored()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(0);
      assertThat(summary.getErrorsList()).isEmpty();

      final ExecuteQueryResponse queryResp = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT count(*) as cnt FROM " + typeName)
          .build());
      assertThat(queryResp.getResultsList()).isNotEmpty();
      final long count = queryResp.getResultsList().getFirst().getRecordsList().getFirst()
          .getPropertiesMap().get("cnt").getInt64Value();
      assertThat(count).isEqualTo(3);
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }
}
