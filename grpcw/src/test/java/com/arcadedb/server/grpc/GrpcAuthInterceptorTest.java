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

import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcAuthInterceptorTest {

  private FakeServerSecurity mockSecurity;
  private HttpAuthSessionManager sessionManager;
  private GrpcAuthInterceptor interceptor;
  private ServerCall<Object, Object> mockCall;
  private ServerCallHandler<Object, Object> mockHandler;
  private MethodDescriptor<Object, Object> mockMethodDescriptor;
  private Metadata metadata;

  @AfterEach
  void tearDown() {
    sessionManager.close();
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockSecurity = FakeServerSecurity.create();
    // A real session manager: sessions are minted for real users and looked up by the token it issued
    sessionManager = new HttpAuthSessionManager(60_000L);
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
    GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, sessionManager);

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
    final ServerSecurityUser mockUser = TestServerHelper.securityUser("testuser");
    final HttpAuthSession session = sessionManager.createSession(mockUser);

    // getUsers() returns Set<String> - need at least one user for security to be enabled
    mockSecurity.returns("getUsers", Collections.singleton("testuser"));
    // The interceptor re-resolves the session's principal against the live users map on every call, so a
    // session alone is not enough to be accepted: the principal has to still exist (see #6808).
    mockSecurity.on("getUser", args -> "testuser".equals(args[0]) ? mockUser : null);

    GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, sessionManager);

    Metadata headers = new Metadata();
    Metadata.Key<String> authKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(authKey, "Bearer " + session.getToken());
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");

    interceptorWithSession.interceptCall(mockCall, headers, mockHandler);

    // Handler's startCall should be invoked (call proceeds)
    verify(mockHandler).startCall(any(), any());
    verify(mockCall, never()).close(any(), any());
  }

  @Test
  void validateTokenReturnsFalseWhenPrincipalNoLongerExists() {
    // #6808: an AU- login token is minted against a principal captured at login time. Once that principal
    // is dropped (or its password rotated), the token must stop working AT ONCE rather than lingering
    // until the session idle-expires, so the interceptor re-checks the name against the live users map and
    // drops the now-orphaned session.
    final ServerSecurityUser mockUser = TestServerHelper.securityUser("droppeduser");
    final String orphanToken = sessionManager.createSession(mockUser).getToken();

    // Security is enabled and still holds other principals, but not the one this token was minted for.
    mockSecurity.returns("getUsers", Collections.singleton("testuser"));
    // A real security holding no "droppeduser": the live lookup answers null on its own.

    final GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, sessionManager);

    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + orphanToken);
    headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), "testdb");

    when(mockMethodDescriptor.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/Query");

    interceptorWithSession.interceptCall(mockCall, headers, mockHandler);

    verify(mockCall).close(any(), any());
    verify(mockHandler, never()).startCall(any(), any());
    // The orphaned session is evicted, so it cannot keep answering getSessionByToken() until it expires.
    assertThat(sessionManager.getSessionByToken(orphanToken)).isNull();
  }

  @Test
  void validateTokenReturnsFalseForInvalidSession() {
    // A token this manager never issued
    // getUsers() returns Set<String> - need at least one user for security to be enabled
    mockSecurity.returns("getUsers", Collections.singleton("testuser"));

    GrpcAuthInterceptor interceptorWithSession = new GrpcAuthInterceptor(mockSecurity, sessionManager);

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
    mockSecurity.returns("getUsers", Collections.singleton("testuser"));

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
