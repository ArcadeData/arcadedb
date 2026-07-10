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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Regression test for issue #4804.
 * <p>
 * {@code ArcadeDbGrpcService.executeCommand} reports execution failures in-band as
 * {@code ExecuteCommandResponse.success=false} while still closing the RPC with gRPC status
 * {@code OK} (unlike {@code executeQuery}/{@code createRecord}, which call {@code onError}).
 * {@code GrpcMetricsInterceptor} previously keyed its error counter solely off {@code !status.isOk()},
 * so every command failure was counted as a success and error observability for the most-used RPC
 * was broken.
 * <p>
 * These tests pin that the interceptor now counts an in-band command failure as an error even when
 * the gRPC status is OK, while a successful command is not counted as an error.
 */
class Issue4804GrpcCommandErrorMetricsTest {

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
  void inBandCommandFailureWithStatusOkIsCountedAsError() {
    final ServerCall<Object, Object> wrapped = wrappedCall();

    // executeCommand emits a success=false response and then closes the RPC with status OK.
    final ExecuteCommandResponse failure = ExecuteCommandResponse.newBuilder()
        .setSuccess(false)
        .setMessage("SQL syntax error")
        .build();
    wrapped.sendMessage(failure);
    wrapped.close(Status.OK, new Metadata());

    // Even though the gRPC status is OK, the command failure must be counted as an error.
    assertThat(interceptor.getErrorCount()).isEqualTo(1.0);
    // A failed command is still one request.
    assertThat(interceptor.getRequestCount()).isEqualTo(1.0);
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

  @Test
  void nonOkStatusIsStillCountedAsError() {
    final ServerCall<Object, Object> wrapped = wrappedCall();

    // Pre-existing behaviour: a non-OK status (e.g. from executeQuery onError) is an error.
    wrapped.close(Status.INTERNAL, new Metadata());

    assertThat(interceptor.getErrorCount()).isEqualTo(1.0);
  }
}
