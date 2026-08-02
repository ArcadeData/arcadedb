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
import com.arcadedb.graph.GraphBatch;
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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the gRPC half of the single-batch guard. A database grants one
 * {@link GraphBatch} at a time, because two overlapping batches silently lose edges. The streaming
 * endpoint therefore has to do two things the engine cannot do for it: report the refusal as a
 * client-side failure, and give the slot back when it drops a batch it never closes.
 * <p>
 * The second one is what matters in production. {@code graphBatchLoad} deliberately skips
 * {@code close()} on the error path so the gRPC thread does not block on the async completion of a
 * batch nobody is waiting for, and a slot left behind there would stop that database from batching
 * for the rest of the server's life. Both assertions run against one long-lived database, which is
 * the only way a leaked slot is visible: a fresh database per test hides it.
 */
public class Issue5666GrpcBatchGuardIT extends BaseGraphServerTest {

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

  /**
   * A load that fails mid-stream is dropped without close(), so the endpoint has to hand the slot
   * back by itself. The load that follows is on the same database and would be refused otherwise.
   */
  @Test
  void aFailedLoadDoesNotKeepTheSlot() throws Exception {
    createBatchTypes();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch failed = new CountDownLatch(1);
    final StreamObserver<GraphBatchChunk> failing = asyncAuthenticatedStub.graphBatchLoad(
        new StreamObserver<>() {
          @Override
          public void onNext(final GraphBatchResult result) {
          }

          @Override
          public void onError(final Throwable t) {
            failure.set(t);
            failed.countDown();
          }

          @Override
          public void onCompleted() {
            failed.countDown();
          }
        });

    // An edge pointing at a temporary id no vertex declared: fails inside onNext, which is the path
    // that drops the batch instead of closing it.
    failing.onNext(GraphBatchChunk.newBuilder()
        .setDatabase(getDatabaseName())
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.VERTEX)
            .setTypeName("GuardNode")
            .setTempId("g1")
            .putProperties("name", stringValue("first")))
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.EDGE)
            .setTypeName("GuardLink")
            .setFromRef("g1")
            .setToRef("gNeverDeclared"))
        .build());
    failing.onCompleted();

    assertThat(failed.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
    assertThat(failure.get().getMessage()).contains("Unknown temporary ID");

    // The batch relaxed read-your-writes for the load and was dropped before close() could put it
    // back, so the endpoint has to. Left relaxed, every later reader on this database loses it.
    assertThat(getServerDatabase(0, getDatabaseName()).isReadYourWrites())
        .as("a dropped batch must not leave read-your-writes off")
        .isTrue();

    // Same database, right after: this is the load a leaked slot would refuse forever.
    final GraphBatchResult result = loadTwoVertices("g2", "g3");
    assertThat(result.getVerticesCreated()).isEqualTo(2);
  }

  /**
   * An overlapping load is a client mistake, not an engine fault, so it has to come back as
   * FAILED_PRECONDITION rather than INTERNAL. The competing batch is taken directly on the server
   * database so the overlap is certain instead of a matter of timing.
   */
  @Test
  void anOverlappingLoadIsRefusedWithFailedPrecondition() throws Exception {
    createBatchTypes();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch refused = new CountDownLatch(1);

    try (final GraphBatch holder = getServerDatabase(0, getDatabaseName()).batch().build()) {
      assertThat(holder).isNotNull();

      final StreamObserver<GraphBatchChunk> overlapping = asyncAuthenticatedStub.graphBatchLoad(
          new StreamObserver<>() {
            @Override
            public void onNext(final GraphBatchResult result) {
            }

            @Override
            public void onError(final Throwable t) {
              failure.set(t);
              refused.countDown();
            }

            @Override
            public void onCompleted() {
              refused.countDown();
            }
          });

      overlapping.onNext(GraphBatchChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .addRecords(GraphBatchRecord.newBuilder()
              .setKind(GraphBatchRecord.Kind.VERTEX)
              .setTypeName("GuardNode")
              .setTempId("o1")
              .putProperties("name", stringValue("overlapping")))
          .build());
      overlapping.onCompleted();

      assertThat(refused.await(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
    final StatusRuntimeException e = (StatusRuntimeException) failure.get();
    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(e.getStatus().getDescription()).contains("already in progress");

    // The refused load must not have consumed the slot it was denied.
    assertThat(loadTwoVertices("o2", "o3").getVerticesCreated()).isEqualTo(2);
  }

  private void createBatchTypes() {
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("CREATE VERTEX TYPE GuardNode IF NOT EXISTS")
        .build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("CREATE EDGE TYPE GuardLink IF NOT EXISTS")
        .build());
  }

  private GraphBatchResult loadTwoVertices(final String firstId, final String secondId) throws Exception {
    final AtomicReference<GraphBatchResult> resultRef = new AtomicReference<>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);

    final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(
        new StreamObserver<>() {
          @Override
          public void onNext(final GraphBatchResult result) {
            resultRef.set(result);
          }

          @Override
          public void onError(final Throwable t) {
            errorRef.set(t);
            done.countDown();
          }

          @Override
          public void onCompleted() {
            done.countDown();
          }
        });

    observer.onNext(GraphBatchChunk.newBuilder()
        .setDatabase(getDatabaseName())
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.VERTEX)
            .setTypeName("GuardNode")
            .setTempId(firstId)
            .putProperties("name", stringValue(firstId)))
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.VERTEX)
            .setTypeName("GuardNode")
            .setTempId(secondId)
            .putProperties("name", stringValue(secondId)))
        .build());
    observer.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(errorRef.get()).as("the load after the guard was released must succeed").isNull();
    assertThat(resultRef.get()).isNotNull();
    return resultRef.get();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
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
}
