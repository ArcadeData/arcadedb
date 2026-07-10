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
 * Regression test for issue #5047 (STR-1): {@code InsertError.row_index} is documented as the index within
 * the chunk, but the server reported the cumulative stream-wide {@code ctx.received - 1}. This drives a
 * two-chunk {@code insertStream} where the failing row is at chunk-relative index 1 (but cumulative index 3),
 * and asserts the reported {@code row_index} is the chunk-relative 1.
 */
public class Issue5047InsertErrorRowIndexIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                 channel;
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
  void insertErrorRowIndexIsChunkRelative() throws Exception {
    final String vType = "Issue5047V_" + System.currentTimeMillis();
    final String eType = "Issue5047E_" + System.currentTimeMillis();

    // Two vertices to use as edge endpoints, plus an edge type.
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE VERTEX TYPE " + vType).build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials())
        .setCommand("CREATE EDGE TYPE " + eType).build());

    final ExecuteCommandResponse v0 = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setReturnRows(true)
        .setCommand("CREATE VERTEX " + vType).build());
    final ExecuteCommandResponse v1 = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setReturnRows(true)
        .setCommand("CREATE VERTEX " + vType).build());

    final String rid0 = ridOf(v0);
    final String rid1 = ridOf(v1);
    assertThat(rid0).as("vertex 0 rid").isNotNull();
    assertThat(rid1).as("vertex 1 rid").isNotNull();

    try {
      final CountDownLatch done = new CountDownLatch(1);
      final AtomicReference<InsertSummary> summaryRef = new AtomicReference<>();

      final StreamObserver<InsertChunk> req = asyncAuthenticatedStub.insertStream(new StreamObserver<>() {
        @Override public void onNext(final InsertSummary s) { summaryRef.set(s); }
        @Override public void onError(final Throwable t) { done.countDown(); }
        @Override public void onCompleted() { done.countDown(); }
      });

      // PER_STREAM commits once at the end, so a per-row MISSING_ENDPOINTS error does not roll the whole
      // stream back; it is recorded row-by-row inside insertRows with a chunk-relative row_index.
      final InsertOptions options = InsertOptions.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setTargetClass(eType)
          .setTransactionMode(InsertOptions.TransactionMode.PER_STREAM)
          .build();

      // Chunk 0 (seq 0): two valid edges -> ctx.received advances to 2.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-5047")
          .setChunkSeq(0)
          .setOptions(options)
          .addRows(edge(eType, rid0, rid1))
          .addRows(edge(eType, rid0, rid1))
          .build());

      // Chunk 1 (seq 1, last): valid, INVALID (no out/in), valid. The invalid row is at chunk-relative
      // index 1 but cumulative index 3.
      req.onNext(InsertChunk.newBuilder()
          .setSessionId("issue-5047")
          .setChunkSeq(1)
          .setLast(true)
          .addRows(edge(eType, rid0, rid1))
          .addRows(GrpcRecord.newBuilder().setType(eType).build()) // missing out/in
          .addRows(edge(eType, rid0, rid1))
          .build());

      req.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).as("InsertStream should terminate within 30s").isTrue();
      final InsertSummary summary = summaryRef.get();
      assertThat(summary).as("client must receive an InsertSummary").isNotNull();

      assertThat(summary.getErrorsList())
          .as("exactly one per-row MISSING_ENDPOINTS error").hasSize(1);
      final InsertError error = summary.getErrors(0);
      assertThat(error.getCode()).isEqualTo("MISSING_ENDPOINTS");
      assertThat(error.getRowIndex())
          .as("row_index must be chunk-relative (1), not the cumulative stream index (3)")
          .isEqualTo(1L);
    } finally {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + eType + " IF EXISTS UNSAFE").build());
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName()).setCredentials(credentials())
          .setCommand("DROP TYPE " + vType + " IF EXISTS UNSAFE").build());
    }
  }

  private GrpcRecord edge(final String type, final String out, final String in) {
    return GrpcRecord.newBuilder().setType(type)
        .putProperties("out", stringValue(out))
        .putProperties("in", stringValue(in))
        .build();
  }

  private static String ridOf(final ExecuteCommandResponse resp) {
    for (final GrpcRecord rec : resp.getRecordsList())
      if (rec.getRid() != null && !rec.getRid().isEmpty())
        return rec.getRid();
    return null;
  }
}
