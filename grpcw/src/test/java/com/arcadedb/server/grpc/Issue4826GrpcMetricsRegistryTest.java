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

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
class Issue4826GrpcMetricsRegistryTest {

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
