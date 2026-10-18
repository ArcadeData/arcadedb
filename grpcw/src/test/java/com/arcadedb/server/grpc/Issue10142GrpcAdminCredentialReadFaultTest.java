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
import com.arcadedb.server.security.ServerSecurityException;
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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

/**
 * Issue #10142: a failure to READ an admin request's credentials (a reflection failure, a protobuf mismatch, a missing
 * native-image registration) refused every admin RPC as "Authentication required" - byte-identical to a wrong
 * password - and logged it at FINE, below the default INFO level. The refusal must stay fail-closed, but be logged at
 * SEVERE, counted on {@code grpc.auth.faults.total}, and described as a server fault. An Error (the shape a missing
 * native-image registration takes) must be caught as well instead of escaping as UNKNOWN.
 */
class Issue10142GrpcAdminCredentialReadFaultTest {
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
