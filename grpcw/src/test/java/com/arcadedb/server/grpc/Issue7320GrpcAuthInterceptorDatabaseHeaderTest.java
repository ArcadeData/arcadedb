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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7320: a data-plane call that carries no {@code x-arcade-database} metadata was authenticated
 * against the literal database name {@code "default"}, which {@code ServerSecurity.authenticate}
 * treats as a real grant check. Every principal whose grants name actual databases was therefore
 * refused on its first RPC, and the refusal blamed the password.
 * <p>
 * The interceptor now authenticates a header-less call at server level - no database, so no grant
 * check - and leaves per-database authorization where it already was: at
 * {@code ArcadeDbGrpcService.validateCredentials}, against the database named in the request body
 * (pinned by {@code Issue4794GrpcPerDbAuthorizationIT}).
 */
class Issue7320GrpcAuthInterceptorDatabaseHeaderTest {

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private FakeServerSecurity                    security;
  private ServerSecurityUser                scopedUser;
  private GrpcAuthInterceptor               interceptor;
  private ServerCall<Object, Object>        call;
  private ServerCallHandler<Object, Object> handler;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    security = FakeServerSecurity.create();
    scopedUser = TestServerHelper.securityUser("scoped", "graph7320");
    call = mock(ServerCall.class);
    handler = mock(ServerCallHandler.class);

    final MethodDescriptor<Object, Object> method = mock(MethodDescriptor.class);
    when(call.getMethodDescriptor()).thenReturn(method);
    when(method.getFullMethodName()).thenReturn("com.arcadedb.grpc.ArcadeDbService/ExecuteQuery");

    // Security is enabled: at least one principal is configured.
    security.returns("getUsers", Collections.singleton("scoped"));
    security.on("authenticate", args -> authenticateScoped((String) args[2]));

    interceptor = new GrpcAuthInterceptor(security);
  }

  /**
   * The configured principal with the right password: granted {@code graph7320} and nothing else, refused on any other
   * database exactly as the real security refuses it, and accepted at server level when no database is named.
   */
  private ServerSecurityUser authenticateScoped(final String database) {
    if (database != null && !"graph7320".equals(database))
      throw new ServerSecurityException("User has not access to database '" + database + "'");
    return scopedUser;
  }

  private Metadata basicAuthHeaders(final String database) {
    final Metadata headers = new Metadata();
    headers.put(USER_HEADER, "scoped");
    headers.put(PASSWORD_HEADER, "scopedpassword");
    if (database != null)
      headers.put(DATABASE_HEADER, database);
    return headers;
  }

  /**
   * The mock authenticates only at server level, exactly like a principal granted {@code graph7320}
   * and nothing else: any grant check against another name throws. Before the fix the interceptor
   * asked for {@code "default"} and the call was closed.
   */
  @Test
  void absentHeaderAuthenticatesAtServerLevel() {

    interceptor.interceptCall(call, basicAuthHeaders(null), handler);

    assertThat(security.calls("authenticate")).containsOnlyOnce(Arrays.asList("scoped", "scopedpassword", null));
    verify(handler).startCall(any(), any());
    verify(call, never()).close(any(), any());
  }

  /**
   * An empty header value is the same as no header: it names no database, so it must not be handed to
   * the grant check as an empty string either.
   */
  @Test
  void emptyHeaderAuthenticatesAtServerLevel() {

    interceptor.interceptCall(call, basicAuthHeaders(""), handler);

    assertThat(security.calls("authenticate")).containsOnlyOnce(Arrays.asList("scoped", "scopedpassword", null));
    verify(handler).startCall(any(), any());
    verify(call, never()).close(any(), any());
  }

  /**
   * A client that does name its database keeps being authenticated against that database, so the fix
   * does not turn the header into decoration.
   */
  @Test
  void presentHeaderStillAuthenticatesAgainstThatDatabase() {

    interceptor.interceptCall(call, basicAuthHeaders("graph7320"), handler);

    assertThat(security.calls("authenticate")).containsOnlyOnce(Arrays.asList("scoped", "scopedpassword", "graph7320"));
    verify(handler).startCall(any(), any());
    verify(call, never()).close(any(), any());
  }

  /**
   * "Invalid credentials" pointed at the password when the real cause was a missing grant. The
   * refusal now carries the reason security gave, so the operator sees which database was checked.
   */
  @Test
  void refusalNamesTheDatabaseThatWasChecked() {

    interceptor.interceptCall(call, basicAuthHeaders("forbidden7320"), handler);

    final ArgumentCaptor<Status> status = ArgumentCaptor.forClass(Status.class);
    verify(call).close(status.capture(), any());
    verify(handler, never()).startCall(any(), any());

    assertThat(status.getValue().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(status.getValue().getDescription()).contains("forbidden7320");
  }

  /**
   * A wrong password is still refused, and still says so.
   */
  @Test
  void wrongPasswordIsStillRefused() {
    security.fails("authenticate", new ServerSecurityException("User/Password not valid"));

    interceptor.interceptCall(call, basicAuthHeaders(null), handler);

    final ArgumentCaptor<Status> status = ArgumentCaptor.forClass(Status.class);
    verify(call).close(status.capture(), any());
    verify(handler, never()).startCall(any(), any());

    assertThat(status.getValue().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(status.getValue().getDescription()).contains("User/Password not valid");
  }
}
