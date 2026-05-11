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
import com.arcadedb.test.BaseGraphServerTest;
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
 * Regression test for issue #4198: gRPC {@code InsertStream} surfaces commit-time constraint
 * violations as a stream-level {@code Status.INTERNAL} error instead of populating
 * {@link InsertSummary#getErrorsList()}.
 *
 * <p>In {@code PER_STREAM} (and {@code PER_REQUEST}) transaction mode, the commit happens in
 * {@code onCompleted()} after {@code insertRows(...)} has already returned cleanly. If commit
 * raises a {@link com.arcadedb.exception.DuplicatedKeyException} (deferred unique-index check),
 * the original code routed the exception through {@code resp.onError(Status.INTERNAL...)} which
 * aborted the entire RPC and left the client with no {@code InsertSummary}.
 *
 * <p>The fix catches recoverable per-row errors thrown by {@code ctx.flushCommit(true)} and
 * registers them in the totals so the client receives a well-formed {@code InsertSummary}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue4198InsertStreamCommitErrorIT extends BaseGraphServerTest {

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
  void insertStreamUniqueViolationUnderPerStreamShouldPopulateErrors() throws Exception {
    final String typeName = "Issue4198PerStreamType_" + System.currentTimeMillis();

    // Schema + unique index + one pre-existing row that the streamed row will collide with.
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
          .setSessionId("issue-4198-per-stream")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
              .build())
          .addRows(GrpcRecord.newBuilder()
              .setType(typeName)
              .putProperties("name", stringValue("Alice")) // duplicate of the pre-existing row
              .build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      // The RPC must terminate cleanly with an InsertSummary, not as Status.INTERNAL.
      assertThat(errorRef.get())
          .as("InsertStream should not abort with Status.INTERNAL on a per-row constraint violation")
          .isNull();
      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getErrorsList())
          .as("the violating row should appear in InsertSummary.errors")
          .hasSize(1);
      assertThat(summary.getErrors(0).getCode()).isEqualTo("CONFLICT");
      assertThat(summary.getErrors(0).getMessage()).contains("Alice");
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }

  @Test
  void insertStreamUniqueViolationUnderPerRequestShouldPopulateErrors() throws Exception {
    final String typeName = "Issue4198PerRequestType_" + System.currentTimeMillis();

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
        .setCommand("INSERT INTO " + typeName + " SET name = 'Bob'").build());

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
          .setSessionId("issue-4198-per-request")
          .setChunkSeq(0)
          .setLast(true)
          .setOptions(InsertOptions.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setTargetClass(typeName)
              .setConflictMode(InsertOptions.ConflictMode.CONFLICT_ERROR)
              .setTransactionMode(InsertOptions.TransactionMode.PER_REQUEST)
              .build())
          .addRows(GrpcRecord.newBuilder()
              .setType(typeName)
              .putProperties("name", stringValue("Bob"))
              .build())
          .build());
      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();

      assertThat(errorRef.get())
          .as("InsertStream should not abort with Status.INTERNAL on a per-row constraint violation")
          .isNull();
      assertThat(summaryRef.get()).as("client must receive an InsertSummary").isNotNull();

      final InsertSummary summary = summaryRef.get();
      assertThat(summary.getReceived()).isEqualTo(1);
      assertThat(summary.getFailed()).isEqualTo(1);
      assertThat(summary.getInserted()).isEqualTo(0);
      assertThat(summary.getErrorsList()).hasSize(1);
      assertThat(summary.getErrors(0).getCode()).isEqualTo("CONFLICT");
      assertThat(summary.getErrors(0).getMessage()).contains("Bob");
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + typeName + " IF EXISTS UNSAFE").build());
    }
  }
}
