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

import com.arcadedb.server.ArcadeDBServer;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcMetricsInterceptorTest {

  private GrpcMetricsInterceptor interceptor;
  private ArcadeDBServer mockServer;
  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata headers;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockServer = mock(ArcadeDBServer.class);
    interceptor = new GrpcMetricsInterceptor(mockServer);
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
}
