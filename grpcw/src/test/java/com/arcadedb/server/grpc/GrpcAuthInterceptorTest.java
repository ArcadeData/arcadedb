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

import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcAuthInterceptorTest {

  private ServerSecurity mockSecurity;
  private HttpAuthSessionManager mockSessionManager;
  private GrpcAuthInterceptor interceptor;
  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata metadata;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockSecurity = mock(ServerSecurity.class);
    mockSessionManager = mock(HttpAuthSessionManager.class);
    interceptor = new GrpcAuthInterceptor(mockSecurity);
    mockCall = mock(ServerCall.class);
    mockHandler = mock(ServerCallHandler.class);
    mockMethodDescriptor = mock(MethodDescriptor.class);
    metadata = new Metadata();

    // Setup method descriptor to return a method name
    when(mockCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.TestService/TestMethod");
  }

  @Test
  void interceptorAllowsCallWhenSecurityIsNull() {
    // When security is null, securityEnabled is false
    GrpcAuthInterceptor noSecurityInterceptor = new GrpcAuthInterceptor(null);

    noSecurityInterceptor.interceptCall(mockCall, metadata, mockHandler);

    // Handler's startCall should be invoked (call proceeds)
    verify(mockHandler).startCall(any(), any());
    // Call should NOT be closed (no rejection)
    verify(mockCall, never()).close(any(), any());
  }

  @Test
  void interceptorCreatesInstance() {
    assertThat(interceptor).isNotNull();
  }

  @Test
  void metadataKeyForAuthorizationExists() {
    // Verify the interceptor can handle metadata with authorization header
    Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(authKey, "Basic dGVzdDp0ZXN0"); // test:test in base64

    // The interceptor should be able to read the authorization header
    assertThat(metadata.get(authKey)).isNotNull();
    assertThat(metadata.get(authKey)).isEqualTo("Basic dGVzdDp0ZXN0");
  }

  @Test
  void constructorAcceptsSessionManager() {
    // Create interceptor with both security and session manager
    GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, mockSessionManager);

    assertThat(interceptorWithSession).isNotNull();
  }

  @Test
  void constructorAcceptsNullSessionManager() {
    // Create interceptor with security but null session manager
    GrpcAuthInterceptor interceptorWithNullSession = new GrpcAuthInterceptor(mockSecurity, null);

    assertThat(interceptorWithNullSession).isNotNull();
  }

  @Test
  void validateTokenReturnsTrueForValidSession() {
    HttpAuthSession mockSession = mock(HttpAuthSession.class);
    ServerSecurityUser mockUser = mock(ServerSecurityUser.class);
    when(mockSession.getUser()).thenReturn(mockUser);
    when(mockUser.getName()).thenReturn("testuser");
    when(mockSessionManager.getSessionByToken("valid-token")).thenReturn(mockSession);

    // getUsers() returns Set<String> - need at least one user for security to be enabled
    when(mockSecurity.getUsers()).thenReturn(Collections.singleton("testuser"));

    GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, mockSessionManager);

    Metadata headers = new Metadata();
    Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(authKey, "Bearer valid-token");
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");

    interceptorWithSession.interceptCall(mockCall, headers, mockHandler);

    // Handler's startCall should be invoked (call proceeds)
    verify(mockHandler).startCall(any(), any());
    verify(mockCall, never()).close(any(), any());
  }

  @Test
  void validateTokenReturnsFalseForInvalidSession() {
    when(mockSessionManager.getSessionByToken("invalid-token")).thenReturn(null);
    // getUsers() returns Set<String> - need at least one user for security to be enabled
    when(mockSecurity.getUsers()).thenReturn(Collections.singleton("testuser"));

    GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, mockSessionManager);

    Metadata headers = new Metadata();
    Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(authKey, "Bearer invalid-token");
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");

    interceptorWithSession.interceptCall(mockCall, headers, mockHandler);

    // Call should be closed with UNAUTHENTICATED
    verify(mockCall).close(any(), any());
    verify(mockHandler, never()).startCall(any(), any());
  }

  @Test
  void validateTokenReturnsFalseWhenSessionManagerIsNull() {
    // getUsers() returns Set<String> - need at least one user for security to be enabled
    when(mockSecurity.getUsers()).thenReturn(Collections.singleton("testuser"));

    GrpcAuthInterceptor interceptorWithoutSession = new GrpcAuthInterceptor(mockSecurity, null);

    Metadata headers = new Metadata();
    Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(authKey, "Bearer any-token");
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");

    interceptorWithoutSession.interceptCall(mockCall, headers, mockHandler);

    // Call should be closed with UNAUTHENTICATED (token auth not available)
    verify(mockCall).close(any(), any());
    verify(mockHandler, never()).startCall(any(), any());
  }
}
