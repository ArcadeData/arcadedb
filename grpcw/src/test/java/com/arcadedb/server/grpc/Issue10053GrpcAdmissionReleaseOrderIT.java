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
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10053: the admission slot of an RPC goes back before the call is closed towards the client, so a client that has
 * just read the answer of a blocking call never finds its own slot still held. The slot used to be released in the
 * {@code finally} that follows {@code super.onHalfClose()}, after the handler had already sent the response and closed the
 * call: {@code QueryAdmissionGate.getRunning()} read right after the blocking stub returned could still be 1, and with one
 * slot and no queue the very next RPC of a sequential client could be refused with {@code ABORTED} by its own previous one.
 * <p>
 * The first group drives {@link GrpcAdmissionInterceptor} directly with a recording call, so the order is observed at the
 * instant the status leaves the server rather than raced against a client thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10053GrpcAdmissionReleaseOrderIT extends BaseGrpcServerTest {
  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  private ManagedChannel channel;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void gateWithOneSlotAndNoQueue() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
  }

  @AfterEach
  void restoreGate() throws InterruptedException {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void theSlotIsBackWhenTheOkStatusLeavesTheServer() {
    final RecordingCall call = new RecordingCall();
    final ServerCall.Listener<ExecuteQueryRequest> listener = interceptor().interceptCall(call, new Metadata(),
        (c, h) -> new HandlerListener(c, Status.OK));

    listener.onMessage(ExecuteQueryRequest.getDefaultInstance());
    listener.onHalfClose();

    assertThat(call.closed).isEqualTo(1);
    assertThat(call.runningWhenClosed).as("slots held when the status left the server").isZero();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void theSlotIsBackWhenAnErrorStatusLeavesTheServer() {
    final RecordingCall call = new RecordingCall();
    final ServerCall.Listener<ExecuteQueryRequest> listener = interceptor().interceptCall(call, new Metadata(),
        (c, h) -> new HandlerListener(c, Status.INVALID_ARGUMENT));

    listener.onMessage(ExecuteQueryRequest.getDefaultInstance());
    listener.onHalfClose();

    assertThat(call.closed).isEqualTo(1);
    assertThat(call.runningWhenClosed).isZero();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void theSlotIsBackWhenAnotherThreadClosesTheCall() throws InterruptedException {
    // A handler that hands the work to a thread of its own closes the call from there
    final RecordingCall call = new RecordingCall();
    final ServerCall.Listener<ExecuteQueryRequest> listener = interceptor().interceptCall(call, new Metadata(), (c, h) -> {
      final ServerCall.Listener<ExecuteQueryRequest> handler = new ServerCall.Listener<>() {
        @Override
        public void onHalfClose() {
          final Thread other = new Thread(() -> c.close(Status.OK, new Metadata()), "other-closer");
          other.start();
          try {
            other.join();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      };
      return handler;
    });

    listener.onMessage(ExecuteQueryRequest.getDefaultInstance());
    listener.onHalfClose();

    assertThat(call.runningWhenClosed).isZero();
    assertThat(gate.getRunning()).isZero();

    // The thread that ran the handler is admitted again: no stale slot of the call another thread closed
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThat(gate.getRunning()).isEqualTo(1);
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void theSlotIsBackWhenTheHandlerNeverClosesTheCall() {
    // A handler that fails before it answers: gRPC closes the call with UNKNOWN after the exception, so nothing here does
    final RecordingCall call = new RecordingCall();
    final ServerCall.Listener<ExecuteQueryRequest> listener = interceptor().interceptCall(call, new Metadata(),
        (c, h) -> new ServerCall.Listener<>() {
          @Override
          public void onHalfClose() {
            throw new IllegalStateException("handler failed");
          }
        });

    listener.onMessage(ExecuteQueryRequest.getDefaultInstance());
    assertThatThrownBy(listener::onHalfClose).isInstanceOf(IllegalStateException.class);

    assertThat(call.closed).isZero();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void aRefusedRpcLeavesTheSlotOfTheQueryHoldingItAlone() throws Exception {
    // The RPC is refused before it holds anything: it must not give back, or close, the slot of the query that took it
    try (final QueryAdmissionGate.Ticket ignored = gate.admit(0)) {
      final RecordingCall refused = new RecordingCall();
      // On a thread of its own: a thread that holds a slot shares it with the queries it starts, so this one would be admitted
      CompletableFuture.runAsync(() -> {
        final ServerCall.Listener<ExecuteQueryRequest> listener = interceptor().interceptCall(refused, new Metadata(),
            (c, h) -> new HandlerListener(c, Status.OK));
        listener.onMessage(ExecuteQueryRequest.getDefaultInstance());
        listener.onHalfClose();
      }).get(30, TimeUnit.SECONDS);

      assertThat(refused.status.getCode()).isEqualTo(Status.Code.ABORTED);
      assertThat(gate.getRunning()).isEqualTo(1);
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void aSequentialClientNeverFindsItsOwnSlotStillHeld() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(channel)
        .withInterceptors(new ClientInterceptor() {
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
        });
    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setQuery("SELECT 1 AS one").build();

    // One slot and no waiting: a call that returns while its slot is still held makes the next one ABORTED. Read right after the
    // blocking stub returns, the gate must already be empty
    for (int i = 0; i < 2_000; i++) {
      try {
        assertThat(stub.executeQuery(request).getResultsList()).isNotEmpty();
      } catch (final StatusRuntimeException e) {
        throw new AssertionError("call " + i + " was refused by the slot of the call before it: " + e.getStatus(), e);
      }
      assertThat(gate.getRunning()).as("slots held after call %d returned", i).isZero();
    }
  }

  private GrpcAdmissionInterceptor interceptor() {
    return new GrpcAdmissionInterceptor(getServer(0));
  }

  /** A handler that answers with one message, then closes the call with {@code status}, the way a unary handler does. */
  private static final class HandlerListener extends ServerCall.Listener<ExecuteQueryRequest> {
    private final ServerCall<ExecuteQueryRequest, ExecuteQueryResponse> call;
    private final Status                                               status;

    private HandlerListener(final ServerCall<ExecuteQueryRequest, ExecuteQueryResponse> call, final Status status) {
      this.call = call;
      this.status = status;
    }

    @Override
    public void onHalfClose() {
      call.sendMessage(ExecuteQueryResponse.getDefaultInstance());
      call.close(status, new Metadata());
    }
  }

  /** Remembers how many slots the gate held at the instant the call was closed: that is when the client can see the answer. */
  private final class RecordingCall extends ServerCall<ExecuteQueryRequest, ExecuteQueryResponse> {
    private final AtomicInteger closedCount = new AtomicInteger();
    private volatile int        runningWhenClosed = -1;
    private volatile Status     status;
    private volatile int        closed;

    @Override
    public void request(final int numMessages) {
    }

    @Override
    public void sendHeaders(final Metadata headers) {
    }

    @Override
    public void sendMessage(final ExecuteQueryResponse message) {
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      runningWhenClosed = gate.getRunning();
      this.status = status;
      closed = closedCount.incrementAndGet();
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<ExecuteQueryRequest, ExecuteQueryResponse> getMethodDescriptor() {
      return ArcadeDbServiceGrpc.getExecuteQueryMethod();
    }

    @Override
    public Attributes getAttributes() {
      return Attributes.EMPTY;
    }
  }
}
