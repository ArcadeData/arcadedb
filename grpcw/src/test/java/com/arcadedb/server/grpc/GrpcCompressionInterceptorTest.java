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

import io.grpc.Compressor;
import io.grpc.CompressorRegistry;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.OutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GrpcCompressionInterceptorTest {

  private static final Metadata.Key<String> ACCEPT_ENCODING = Metadata.Key.of("grpc-accept-encoding",
      Metadata.ASCII_STRING_MARSHALLER);

  // A registered, non-default codec used to prove the configured type is honored (not hardcoded to gzip).
  private static final String CUSTOM_CODEC = "snappy";

  private ServerCall<Object, Object>        mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object>  mockMethodDescriptor;
  private Metadata                          headers;

  @BeforeAll
  static void registerCustomCodec() {
    // Register a no-op compressor so the interceptor sees the custom codec as available server-side.
    CompressorRegistry.getDefaultInstance().register(new Compressor() {
      @Override
      public String getMessageEncoding() {
        return CUSTOM_CODEC;
      }

      @Override
      public OutputStream compress(final OutputStream os) {
        return os;
      }
    });
  }

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

  @SuppressWarnings("unchecked")
  private ServerCall<Object, Object> intercept(final GrpcCompressionInterceptor interceptor) {
    interceptor.interceptCall(mockCall, headers, mockHandler);
    final ArgumentCaptor<ServerCall<Object, Object>> captor = ArgumentCaptor.forClass(ServerCall.class);
    verify(mockHandler).startCall(captor.capture(), any());
    return captor.getValue();
  }

  @Test
  void interceptorForwardsCallToHandler() {
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(false, "gzip");

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void honorsConfiguredCompressionTypeWhenClientAcceptsIt() {
    // Reproduces #4827: the interceptor must use the configured codec, not a hardcoded "gzip".
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, CUSTOM_CODEC);
    headers.put(ACCEPT_ENCODING, "identity," + CUSTOM_CODEC);

    final ServerCall<Object, Object> wrapped = intercept(interceptor);
    wrapped.sendHeaders(new Metadata());

    verify(mockCall).setCompression(CUSTOM_CODEC);
  }

  @Test
  void usesGzipWhenConfiguredAndAccepted() {
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "gzip");
    headers.put(ACCEPT_ENCODING, "gzip");

    final ServerCall<Object, Object> wrapped = intercept(interceptor);
    wrapped.sendHeaders(new Metadata());

    verify(mockCall).setCompression("gzip");
  }

  @Test
  void doesNotCompressWhenClientDoesNotAcceptTheCodec() {
    // Client only accepts gzip but server is configured for the custom codec: must not force it,
    // otherwise the client would receive an undecodable response.
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, CUSTOM_CODEC);
    headers.put(ACCEPT_ENCODING, "gzip");

    final ServerCall<Object, Object> wrapped = intercept(interceptor);
    wrapped.sendHeaders(new Metadata());

    verify(mockCall, never()).setCompression(any());
  }

  @Test
  void doesNotCompressWhenClientSendsNoAcceptEncoding() {
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "gzip");

    final ServerCall<Object, Object> wrapped = intercept(interceptor);
    wrapped.sendHeaders(new Metadata());

    verify(mockCall, never()).setCompression(any());
  }

  @Test
  void doesNotCompressWhenForceDisabled() {
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(false, "gzip");
    headers.put(ACCEPT_ENCODING, "gzip");

    final ServerCall<Object, Object> wrapped = intercept(interceptor);
    wrapped.sendHeaders(new Metadata());

    verify(mockCall, never()).setCompression(any());
  }

  @Test
  void doesNotCompressWhenCodecNotRegistered() {
    // "br" (brotli) is not registered in the default registry, so compression must be skipped even
    // if the client claims to accept it.
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "br");
    headers.put(ACCEPT_ENCODING, "br");

    final ServerCall<Object, Object> wrapped = intercept(interceptor);
    wrapped.sendHeaders(new Metadata());

    verify(mockCall, never()).setCompression(any());
  }

  @Test
  void interceptorDetectsCompressedRequest() {
    final GrpcCompressionInterceptor interceptor = new GrpcCompressionInterceptor(true, "gzip");

    headers.put(Metadata.Key.of("grpc-encoding", Metadata.ASCII_STRING_MARSHALLER), "gzip");

    interceptor.interceptCall(mockCall, headers, mockHandler);

    verify(mockHandler).startCall(any(), any());
  }

  @Test
  void compressionInfoStoresRequestState() {
    final GrpcCompressionInterceptor.CompressionInfo info =
        new GrpcCompressionInterceptor.CompressionInfo(true, "gzip");

    assertThat(info.requestCompressed).isTrue();
    assertThat(info.requestEncoding).isEqualTo("gzip");
  }

  @Test
  void compressionInfoStoresUncompressedState() {
    final GrpcCompressionInterceptor.CompressionInfo info =
        new GrpcCompressionInterceptor.CompressionInfo(false, null);

    assertThat(info.requestCompressed).isFalse();
    assertThat(info.requestEncoding).isNull();
  }

  @Test
  void compressionKeyIsAccessible() {
    assertThat(GrpcCompressionInterceptor.COMPRESSION_KEY).isNotNull();
  }
}
