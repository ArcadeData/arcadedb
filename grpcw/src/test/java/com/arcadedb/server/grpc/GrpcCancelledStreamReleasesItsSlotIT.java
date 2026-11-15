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
import com.arcadedb.database.Database;
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
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A StreamQuery the client cancels stops on the server at once. The handler runs inside the call's half-close callback, and
 * gRPC delivers the cancel handler on the same serialized executor, so the cancel was only seen once the stream was over: the
 * stream went on waiting for the transport until arcadedb.server.grpcStreamWriteTimeoutMs, with its worker thread, its result
 * set and its query admission slot. arcadedb.queryMaxConcurrent cancelled streams then made every remote query, HTTP included,
 * wait in the admission queue.
 */
public class GrpcCancelledStreamReleasesItsSlotIT extends BaseGrpcServerTest {
  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                       channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    // Long enough that a stream still running after a few seconds can only be one that did not see the cancel
    GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.setValue(120_000L);
  }

  @BeforeEach
  void setupGrpcClient() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.getSchema().createDocumentType("Many");
    db.transaction(() -> {
      for (int i = 0; i < 20_000; i++)
        db.newDocument("Many").set("i", i, "pad", "x".repeat(200)).save();
    });

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
          final CallOptions callOptions, final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(USER_HEADER, "root");
            headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
            super.start(responseListener, headers);
          }
        };
      }
    }));
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    GlobalConfiguration.SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS.reset();
  }

  @Test
  void aCancelledStreamStopsAndLeavesNoRunningQuery() throws Exception {
    final CountDownLatch firstBatch = new CountDownLatch(1);
    final CountDownLatch closed = new CountDownLatch(1);
    stub.streamQuery(StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setQuery("SELECT FROM Many")
        .setBatchSize(10)
        .build(), new ClientResponseObserver<StreamQueryRequest, QueryResult>() {
      private ClientCallStreamObserver<StreamQueryRequest> call;

      @Override
      public void beforeStart(final ClientCallStreamObserver<StreamQueryRequest> requestStream) {
        call = requestStream;
      }

      @Override
      public void onNext(final QueryResult value) {
        firstBatch.countDown();
        call.cancel("the client is done", null);
      }

      @Override
      public void onError(final Throwable t) {
        closed.countDown();
      }

      @Override
      public void onCompleted() {
        closed.countDown();
      }
    });

    assertThat(firstBatch.await(30, TimeUnit.SECONDS)).as("the stream answered a first batch").isTrue();
    assertThat(closed.await(30, TimeUnit.SECONDS)).isTrue();

    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (getServer(0).getRunningQueries().size() > 0 && System.nanoTime() < deadline)
      Thread.sleep(50);
    assertThat(getServer(0).getRunningQueries().getRunning())
        .as("the cancelled stream is still running on the server, holding its admission slot").isEmpty();
  }
}
