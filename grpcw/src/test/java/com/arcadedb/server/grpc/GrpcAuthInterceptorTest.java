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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.google.protobuf.Message;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

  // Regression: #7320
  /**
   * Issue #7320: a data-plane call that carries no {@code x-arcade-database} metadata was authenticated
   * against the literal database name {@code "default"}, which {@code ServerSecurity.authenticate}
   * treats as a real grant check. Every principal whose grants name actual databases was therefore
   * refused on its first RPC, and the refusal blamed the password.
   * <p>
   * The interceptor now authenticates a header-less call at server level - no database, so no grant
   * check - and leaves per-database authorization where it already was: at
   * {@code ArcadeDbGrpcService.validateCredentials}, against the database named in the request body
   * (pinned by {@code GrpcControlPlaneAuthorizationIT.PerDatabaseAuthorization}).
   */
  @Nested
  class DatabaseHeader {
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

  // Regression: #10142
  /**
   * Issue #10142: a failure to READ an admin request's credentials (a reflection failure, a protobuf mismatch, a missing
   * native-image registration) refused every admin RPC as "Authentication required" - byte-identical to a wrong
   * password - and logged it at FINE, below the default INFO level. The refusal must stay fail-closed, but be logged at
   * SEVERE, counted on {@code grpc.auth.faults.total}, and described as a server fault. An Error (the shape a missing
   * native-image registration takes) must be caught as well instead of escaping as UNKNOWN.
   */
  @Nested
  class AdminCredentialReadFault {
    private static final String ADMIN_METHOD = "com.arcadedb.grpc.ArcadeDbAdminService/ListDatabases";
    private static final String DATA_METHOD  = "com.arcadedb.grpc.ArcadeDbService/ExecuteQuery";

    private FakeServerSecurity                security;
    private SimpleMeterRegistry               registry;
    private GrpcAuthInterceptor               interceptor;
    private ServerCall<Object, Object>        call;
    private ServerCallHandler<Object, Object> handler;
    private MethodDescriptor<Object, Object>  methodDescriptor;
    private ServerCall.Listener<Object>       downstream;
    private ServerCall.Listener<Object>       listener;
    private Logger                            jul;
    private Level                             previousLevel;
    private final List<LogRecord>             published = new CopyOnWriteArrayList<>();
    private final Handler                     capture   = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        published.add(record);
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
      security = FakeServerSecurity.create();
      security.returns("getUsers", Collections.singleton("root"));
      registry = new SimpleMeterRegistry();
      interceptor = new GrpcAuthInterceptor(security, null, registry);

      call = mock(ServerCall.class);
      handler = mock(ServerCallHandler.class);
      methodDescriptor = mock(MethodDescriptor.class);
      downstream = mock(ServerCall.Listener.class);
      when(call.getMethodDescriptor()).thenReturn(methodDescriptor);
      when(handler.startCall(any(), any())).thenReturn(downstream);

      // The logger stays at the default INFO: what is asserted is what an operator sees without turning anything on
      jul = Logger.getLogger(GrpcAuthInterceptor.class.getName());
      previousLevel = jul.getLevel();
      jul.setLevel(Level.INFO);
      capture.setLevel(Level.ALL);
      jul.addHandler(capture);
    }

    @AfterEach
    void tearDown() {
      jul.removeHandler(capture);
      jul.setLevel(previousLevel);
    }

    @Test
    void aCredentialReadThatThrowsIsLoggedAtSevereCountedAndNotDescribedAsAWrongPassword() {
      // The descriptor-driven read is what failed on native: a message whose descriptor cannot be resolved
      final Message unreadable = mock(Message.class);
      when(unreadable.getDescriptorForType()).thenThrow(new IllegalStateException("no reflection metadata for the accessor table"));

      final Status status = sendAdminMessage(unreadable);

      assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
      assertThat(status.getDescription()).isEqualTo(GrpcAuthInterceptor.ADMIN_CREDENTIAL_READ_FAULT);
      assertThat(status.getDescription()).isNotEqualTo("Authentication required");
      assertThat(faults(GrpcAuthInterceptor.STAGE_ADMIN_CREDENTIAL_READ)).isEqualTo(1.0);
      assertFaultLogged(IllegalStateException.class);
      verify(downstream, never()).onMessage(any());
    }

    @Test
    void anErrorDuringTheCredentialReadIsCaughtRatherThanEscapingAsUnknown() {
      // A missing native-image registration surfaces as an Error on the first touch of a class, not as an Exception
      security.on("authenticate", args -> {
        throw new ExceptionInInitializerError("class initialization failed");
      });

      final Status status = sendAdminMessage(adminRequest("root", "secret"));

      assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
      assertThat(status.getDescription()).isEqualTo(GrpcAuthInterceptor.ADMIN_CREDENTIAL_READ_FAULT);
      assertThat(faults(GrpcAuthInterceptor.STAGE_ADMIN_CREDENTIAL_READ)).isEqualTo(1.0);
      assertFaultLogged(ExceptionInInitializerError.class);
      verify(downstream, never()).onMessage(any());

      // The call is halted: the half-close that follows must not reach the handler either
      listener.onHalfClose();
      verify(downstream, never()).onHalfClose();
    }

    @Test
    void anUnexpectedFailureInsideAuthenticateIsCountedAndNamedAsAServerFault() {
      // Already SEVERE before #10142, but neither counted nor told apart: a security backend outage answered the admin
      // caller "Authentication required", exactly like a wrong password
      security.fails("authenticate", new IllegalStateException("security backend down"));

      final Status status = sendAdminMessage(adminRequest("root", "secret"));

      assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
      assertThat(status.getDescription()).isEqualTo(GrpcAuthInterceptor.AUTHENTICATE_FAULT);
      assertThat(faults(GrpcAuthInterceptor.STAGE_AUTHENTICATE)).isEqualTo(1.0);
      assertFaultLogged(IllegalStateException.class);
    }

    @Test
    void anUnexpectedFailureInsideAuthenticateOnTheDataPlaneIsNamedAsAServerFault() {
      when(methodDescriptor.getFullMethodName()).thenReturn(DATA_METHOD);
      security.fails("authenticate", new IllegalStateException("security backend down"));

      interceptor.interceptCall(call, basicAuthHeaders(), handler);

      final ArgumentCaptor<Status> status = ArgumentCaptor.forClass(Status.class);
      verify(call).close(status.capture(), any());
      assertThat(status.getValue().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
      assertThat(status.getValue().getDescription()).isEqualTo(GrpcAuthInterceptor.AUTHENTICATE_FAULT);
      assertThat(faults(GrpcAuthInterceptor.STAGE_AUTHENTICATE)).isEqualTo(1.0);
    }

    @Test
    void aVirtualMachineErrorIsNotSwallowedAsAnAuthenticationFault() {
      // An OOM or a stack overflow must reach the JVM, not be reported as an auth fault. The StackOverflowError is
      // constructed and thrown, not provoked by recursion, so the test thread's stack is never actually exhausted
      security.on("authenticate", args -> {
        throw new StackOverflowError();
      });
      when(methodDescriptor.getFullMethodName()).thenReturn(ADMIN_METHOD);
      listener = interceptor.interceptCall(call, new Metadata(), handler);

      assertThatThrownBy(() -> listener.onMessage(adminRequest("root", "secret"))).isInstanceOf(StackOverflowError.class);
      assertThat(totalFaults()).isEqualTo(0.0);
      verify(downstream, never()).onMessage(any());

      when(methodDescriptor.getFullMethodName()).thenReturn(DATA_METHOD);
      assertThatThrownBy(() -> interceptor.interceptCall(call, basicAuthHeaders(), handler)).isInstanceOf(StackOverflowError.class);
      assertThat(totalFaults()).isEqualTo(0.0);
    }

    @Test
    void aSecurityExceptionWhoseMessageEqualsTheFaultTextIsStillACredentialRefusal() {
      // The fault is told apart by reference, not by text: a security layer that happens to say the same words is
      // still an ordinary refusal on the admin path, neither counted nor described as a server fault
      security.fails("authenticate", new ServerSecurityException(new String(GrpcAuthInterceptor.AUTHENTICATE_FAULT)));

      final Status status = sendAdminMessage(adminRequest("root", "wrong"));

      assertThat(status.getDescription()).isEqualTo("Authentication required");
      assertThat(totalFaults()).isEqualTo(0.0);
    }

    @Test
    void aWrongPasswordIsNeitherCountedAsAFaultNorLoggedAtWarningOrAbove() {
      security.fails("authenticate", new ServerSecurityException("User/Password not valid"));

      final Status status = sendAdminMessage(adminRequest("root", "wrong"));

      assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
      assertThat(status.getDescription()).isEqualTo("Authentication required");
      assertThat(totalFaults()).isEqualTo(0.0);
      assertThat(published).noneMatch(r -> r.getLevel().intValue() >= Level.WARNING.intValue());
    }

    @Test
    void anErrorOnTheDataPlanePathIsCaughtLoggedAndCounted() {
      when(methodDescriptor.getFullMethodName()).thenReturn(DATA_METHOD);
      security.on("authenticate", args -> {
        throw new NoClassDefFoundError("com/example/Missing");
      });
      interceptor.interceptCall(call, basicAuthHeaders(), handler);

      final ArgumentCaptor<Status> status = ArgumentCaptor.forClass(Status.class);
      verify(call).close(status.capture(), any());
      assertThat(status.getValue().getCode()).isEqualTo(Status.Code.INTERNAL);
      verify(handler, never()).startCall(any(), any());
      assertThat(faults(GrpcAuthInterceptor.STAGE_INTERCEPT)).isEqualTo(1.0);
      assertFaultLogged(NoClassDefFoundError.class);
    }

    @Test
    void theFaultCounterIsPublishedAtZeroBeforeAnyFault() {
      assertThat(registry.find(GrpcAuthInterceptor.AUTH_FAULTS_METER).counters()).hasSize(3);
      assertThat(totalFaults()).isEqualTo(0.0);
    }

    private Status sendAdminMessage(final Object message) {
      when(methodDescriptor.getFullMethodName()).thenReturn(ADMIN_METHOD);
      listener = interceptor.interceptCall(call, new Metadata(), handler);
      listener.onMessage(message);

      final ArgumentCaptor<Status> status = ArgumentCaptor.forClass(Status.class);
      verify(call).close(status.capture(), any());
      return status.getValue();
    }

    private static Metadata basicAuthHeaders() {
      final Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
      headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), "secret");
      return headers;
    }

    private static ListDatabasesRequest adminRequest(final String user, final String password) {
      return ListDatabasesRequest.newBuilder()
          .setCredentials(DatabaseCredentials.newBuilder().setUsername(user).setPassword(password).build())
          .build();
    }

    private void assertFaultLogged(final Class<? extends Throwable> cause) {
      assertThat(published).anySatisfy(record -> {
        assertThat(record.getLevel().intValue()).isGreaterThanOrEqualTo(Level.WARNING.intValue());
        assertThat(record.getThrown()).isInstanceOf(cause);
      });
    }

    private double faults(final String stage) {
      return registry.get(GrpcAuthInterceptor.AUTH_FAULTS_METER).tag("stage", stage).counter().count();
    }

    private double totalFaults() {
      return registry.find(GrpcAuthInterceptor.AUTH_FAULTS_METER).counters().stream().mapToDouble(Counter::count).sum();
    }
  }
}
