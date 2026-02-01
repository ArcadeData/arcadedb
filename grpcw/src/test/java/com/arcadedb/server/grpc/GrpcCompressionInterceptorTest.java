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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcCompressionInterceptorTest {

  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata headers;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockCall = mock(ServerCall.class);
    mockHandler = mock(ServerCallHandler.class);
    mockMethodDescriptor = mock(MethodDescriptor.class);
    headers = new Metadata();

    when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.TestService/TestMethod");
  }

  @Test
  void interceptorCreatesWithDefaultCompression() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(false, null, 1024);
    assertThat(interceptor).isNotNull();
  }

  @Test
  void interceptorCreatesWithGzipCompression() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "gzip", 1024);
    assertThat(interceptor).isNotNull();
  }

  @Test
  void interceptorForwardsCallToHandler() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(false, "gzip", 1024);

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void interceptorDetectsCompressedRequest() {
    GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "gzip", 0);

    Metadata.Key<String> encodingKey = Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(encodingKey, "gzip");

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void compressionInfoStoresRequestState() {
    GrpcCompressionInterceptor.CompressionInfo info =
        new GrpcCompressionInterceptor.CompressionInfo(true, "gzip");

    assertThat(info.requestCompressed).isTrue();
    assertThat(info.requestEncoding).isEqualTo("gzip");
  }

  @Test
  void compressionInfoStoresUncompressedState() {
    GrpcCompressionInterceptor.CompressionInfo info =
        new GrpcCompressionInterceptor.CompressionInfo(false, null);

    assertThat(info.requestCompressed).isFalse();
    assertThat(info.requestEncoding).isNull();
  }

  @Test
  void compressionKeyIsAccessible() {
    assertThat(GrpcCompressionInterceptor.COMPRESSION_KEY).isNotNull();
  }
}
