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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcMetricsInterceptorTest {

  private GrpcMetricsInterceptor interceptor;
  private SimpleMeterRegistry registry;
  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata headers;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    registry = new SimpleMeterRegistry();
    interceptor = new GrpcMetricsInterceptor(registry);
    mockCall = mock(ServerCall.class);
    mockHandler = mock(ServerCallHandler.class);
    mockMethodDescriptor = mock(MethodDescriptor.class);
    headers = new Metadata();

    when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.TestService/TestMethod");
  }

  @Test
  void interceptorCreatesInstance() {
    assertThat(interceptor).isNotNull();
  }

  @Test
  void interceptorForwardsCallToHandler() {
    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void interceptorIncrementsRequestCounter() {
    // First call
    interceptor.interceptCall(mockCall, headers, mockHandler);

    // Second call
    interceptor.interceptCall(mockCall, headers, mockHandler);

    // Verify both calls were forwarded
    verify(mockHandler, times(2)).startCall(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void interceptorWrapsCallForMetrics() {
    // This test verifies the wrapped call behavior
    ArgumentCaptor<ServerCall<Object, Object>> callCaptor = ArgumentCaptor.forClass(ServerCall.class);

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(callCaptor.capture(), any());

    // The wrapped call should have been passed to the handler
    ServerCall<Object, Object> wrappedCall = callCaptor.getValue();
    assertThat(wrappedCall).isNotNull();
  }

  // Regression: #4804
  /**
   * Regression test for issue #4804.
   * <p>
   * At the time this was fixed, {@code ArcadeDbGrpcService.executeCommand} reported execution failures
   * in-band as {@code ExecuteCommandResponse.success=false} while still closing the RPC with gRPC status
   * {@code OK} (unlike {@code executeQuery}/{@code createRecord}, which call {@code onError}).
   * {@code GrpcMetricsInterceptor} previously keyed its error counter solely off {@code !status.isOk()},
   * so every command failure was counted as a success and error observability for the most-used RPC
   * was broken.
   * <p>
   * Issue #6192 later routed every {@code executeCommand} failure through {@code onError} too, so the
   * in-band scenario these tests build by hand no longer happens for a real command failure. The tests
   * are kept as a defensive pin on the interceptor's counting logic itself - message-then-OK-status is
   * still a legal gRPC response shape for it to see, from this RPC or any future one - while the
   * {@code nonOkStatusIsStillCountedAsError} test below now covers what {@code executeCommand} actually
   * produces on failure.
   */
  @Nested
  class CommandErrorMetrics {
    private GrpcMetricsInterceptor                interceptor;
    private ServerCall<Object, Object>            mockCall;
    private ServerCallHandler<Object, Object>     mockHandler;
    private MethodDescriptor<Object, Object>      mockMethodDescriptor;
    private Metadata                              headers;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
      interceptor = new GrpcMetricsInterceptor(new SimpleMeterRegistry());
      mockCall = mock(ServerCall.class);
      mockHandler = mock(ServerCallHandler.class);
      mockMethodDescriptor = mock(MethodDescriptor.class);
      headers = new Metadata();

      when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
      when(mockMethodDescriptor.getFullMethodName())
          .thenReturn("com.arcadedb.grpc.ArcadeDbService/ExecuteCommand");
    }

    @SuppressWarnings("unchecked")
    private ServerCall<Object, Object> wrappedCall() {
      final ArgumentCaptor<ServerCall<Object, Object>> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
      interceptor.interceptCall(mockCall, headers, mockHandler);
      verify(mockHandler).startCall(callCaptor.capture(), any());
      return callCaptor.getValue();
    }

    @Test
    void successfulCommandWithStatusOkIsNotCountedAsError() {
      final ServerCall<Object, Object> wrapped = wrappedCall();

      final ExecuteCommandResponse success = ExecuteCommandResponse.newBuilder()
          .setSuccess(true)
          .setMessage("OK")
          .build();
      wrapped.sendMessage(success);
      wrapped.close(Status.OK, new Metadata());

      assertThat(interceptor.getErrorCount()).isEqualTo(0.0);
    }
  }

  // Regression: #4826
  /**
   * Regression test for issue #4826.
   * <p>
   * {@code GrpcMetricsInterceptor} used to build its own private {@code new SimpleMeterRegistry()},
   * disconnected from the server's shared registry, so every gRPC request/error/duration meter was
   * black-holed: no exporter (Prometheus, OTLP, JMX, Studio) ever saw gRPC telemetry. The interceptor
   * also wrote per-call values into call trailers instead of tagging meters.
   * <p>
   * These tests pin that the interceptor now records into the injected shared {@link
   * SimpleMeterRegistry} and tags each meter with the gRPC {@code method} (and {@code status} for
   * errors/latency), so operators can scrape and slice gRPC telemetry.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SharedRegistry {
    private static final String METHOD = "com.arcadedb.grpc.ArcadeDbService/ExecuteQuery";

    private SimpleMeterRegistry               registry;
    private GrpcMetricsInterceptor            interceptor;
    private ServerCall<Object, Object>        mockCall;
    private ServerCallHandler<Object, Object> mockHandler;
    private MethodDescriptor<Object, Object>  mockMethodDescriptor;
    private Metadata                          headers;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
      registry = new SimpleMeterRegistry();
      interceptor = new GrpcMetricsInterceptor(registry);
      mockCall = mock(ServerCall.class);
      mockHandler = mock(ServerCallHandler.class);
      mockMethodDescriptor = mock(MethodDescriptor.class);
      headers = new Metadata();

      when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
      when(mockMethodDescriptor.getFullMethodName()).thenReturn(METHOD);
    }

    @SuppressWarnings("unchecked")
    private ServerCall<Object, Object> wrappedCall() {
      final ArgumentCaptor<ServerCall<Object, Object>> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
      interceptor.interceptCall(mockCall, headers, mockHandler);
      verify(mockHandler).startCall(callCaptor.capture(), any());
      return callCaptor.getValue();
    }

    @Test
    void requestMeterLandsInSharedRegistryTaggedByMethod() {
      wrappedCall();

      // The request counter must be visible in the shared registry, not a private one, and tagged by method.
      final Counter requests = registry.find(GrpcMetricsInterceptor.REQUESTS_METER).tag("method", METHOD).counter();
      assertThat(requests).isNotNull();
      assertThat(requests.count()).isEqualTo(1.0);
    }

    @Test
    void durationAndErrorMetersLandInSharedRegistryTaggedByMethodAndStatus() {
      final ServerCall<Object, Object> wrapped = wrappedCall();
      wrapped.close(Status.INTERNAL, new Metadata());

      final Timer duration = registry.find(GrpcMetricsInterceptor.DURATION_METER)
          .tag("method", METHOD).tag("status", "INTERNAL").timer();
      assertThat(duration).isNotNull();
      assertThat(duration.count()).isEqualTo(1L);

      final Counter errors = registry.find(GrpcMetricsInterceptor.ERRORS_METER)
          .tag("method", METHOD).tag("status", "INTERNAL").counter();
      assertThat(errors).isNotNull();
      assertThat(errors.count()).isEqualTo(1.0);
    }

    @Test
    void successfulCallDoesNotRecordAnError() {
      final ServerCall<Object, Object> wrapped = wrappedCall();
      wrapped.close(Status.OK, new Metadata());

      assertThat(registry.find(GrpcMetricsInterceptor.ERRORS_METER).counters()).isEmpty();
      assertThat(registry.find(GrpcMetricsInterceptor.DURATION_METER)
          .tag("method", METHOD).tag("status", "OK").timer()).isNotNull();
    }

    @Test
    void noMetricsAreWrittenIntoCallTrailers() {
      final ServerCall<Object, Object> wrapped = wrappedCall();
      final Metadata trailers = new Metadata();
      wrapped.close(Status.OK, trailers);

      // Telemetry now flows to the registry; per-call values must not leak into the wire trailers.
      assertThat(trailers.keys()).noneMatch(k -> k.startsWith("grpc-metrics-"));
    }
  }

  /*
   * Issue #11054: {@link GrpcMetricsInterceptor} counted an in-band failure (a response closed with status OK that says
   * {@code success=false}) for one message type only, {@code ExecuteCommandResponse}. A vetoed {@code updateRecord} or
   * {@code deleteRecord} answers exactly that shape, so on a server whose listeners veto every write
   * {@code grpc.errors.total} stayed at zero while {@code grpc.requests.total} climbed. The check now reads the response's
   * own {@code success} field, whatever its type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11054
  @Test
  void aVetoedUpdateIsCountedAsAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getUpdateRecordMethod(),
        UpdateRecordResponse.newBuilder().setSuccess(false).setUpdated(false).build())).isEqualTo(1.0);
  }

  // Regression: #11054
  @Test
  void aVetoedDeleteIsCountedAsAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getDeleteRecordMethod(),
        DeleteRecordResponse.newBuilder().setSuccess(false).setDeleted(false).build())).isEqualTo(1.0);
  }

  // Regression: #11054
  @Test
  void anInBandCommandFailureIsStillCountedAsAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getExecuteCommandMethod(),
        ExecuteCommandResponse.newBuilder().setSuccess(false).build())).isEqualTo(1.0);
  }

  // Regression: #11054
  @Test
  void anAppliedWriteIsNotAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getUpdateRecordMethod(),
        UpdateRecordResponse.newBuilder().setSuccess(true).setUpdated(true).build())).isZero();
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getDeleteRecordMethod(),
        DeleteRecordResponse.newBuilder().setSuccess(true).setDeleted(true).build())).isZero();
  }

  // Regression: #11054
  @Test
  void aCommitOfNoTransactionSaysSuccessAndIsNotAnError() {
    // success=true with committed=false: the call succeeded and had nothing to commit
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getCommitTransactionMethod(),
        CommitTransactionResponse.newBuilder().setSuccess(true).setCommitted(false).build())).isZero();
  }

  // Regression: #11054
  @Test
  void aResponseWithoutASuccessFieldIsNotAnError() {
    assertThat(errorsAfter(ArcadeDbServiceGrpc.getExecuteQueryMethod(), ExecuteQueryResponse.getDefaultInstance())).isZero();
  }

  // Regression: #11054
  @Test
  void aNonOkStatusIsStillAnErrorWithoutAnyMessage() {
    final GrpcMetricsInterceptor interceptor = new GrpcMetricsInterceptor(new SimpleMeterRegistry());
    final ServerCall<Object, Object> call = start(interceptor, ArcadeDbServiceGrpc.getUpdateRecordMethod());
    call.close(Status.INTERNAL, new Metadata());
    assertThat(interceptor.getErrorCount()).isEqualTo(1.0);
  }

  // Regression: #11054
  @Test
  void everyResponseThatCarriesASuccessFlagIsCovered() {
    // The rule the counter follows, stated over the whole protocol: each response type of both services that declares a
    // "success" flag is one the interceptor reads, so a response type added later cannot be left out as these two were
    final FileDescriptor file = ArcadeDbProto.getDescriptor();
    final List<String> flagged = new ArrayList<>();
    for (final ServiceDescriptor service : file.getServices())
      for (final var method : service.getMethods()) {
        final Descriptor output = method.getOutputType();
        if (output.findFieldByName("success") == null)
          continue;
        flagged.add(output.getName());
        assertThat(GrpcMetricsInterceptor.successField(DynamicMessage.getDefaultInstance(output))).as(output.getName()).isNotNull();
      }

    assertThat(flagged).contains("UpdateRecordResponse", "DeleteRecordResponse", "ExecuteCommandResponse");
  }

  private static double errorsAfter(final MethodDescriptor<?, ?> method, final Message response) {
    final GrpcMetricsInterceptor interceptor = new GrpcMetricsInterceptor(new SimpleMeterRegistry());
    final ServerCall<Object, Object> call = start(interceptor, method);
    call.sendMessage(response);
    call.close(Status.OK, new Metadata());
    assertThat(interceptor.getRequestCount()).isEqualTo(1.0);
    return interceptor.getErrorCount();
  }

  /** The call the interceptor hands the handler, around a call that records nothing. */
  @SuppressWarnings("unchecked")
  private static ServerCall<Object, Object> start(final GrpcMetricsInterceptor interceptor, final MethodDescriptor<?, ?> method) {
    final ServerCall<Object, Object>[] wrapped = new ServerCall[1];
    interceptor.interceptCall(new NoOpCall((MethodDescriptor<Object, Object>) method), new Metadata(), (c, h) -> {
      wrapped[0] = c;
      return new ServerCall.Listener<>() {
      };
    });
    return wrapped[0];
  }

  private static final class NoOpCall extends ServerCall<Object, Object> {
    private final MethodDescriptor<Object, Object> method;

    private NoOpCall(final MethodDescriptor<Object, Object> method) {
      this.method = method;
    }

    @Override
    public void request(final int numMessages) {
    }

    @Override
    public void sendHeaders(final Metadata headers) {
    }

    @Override
    public void sendMessage(final Object message) {
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<Object, Object> getMethodDescriptor() {
      return method;
    }

    @Override
    public Attributes getAttributes() {
      return Attributes.EMPTY;
    }
  }
}
