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

import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4792: the gRPC auth interceptor must NOT blanket-skip the whole
 * Admin service. Authentication is enforced centrally in the interceptor from the request-body
 * credentials, so a missing/invalid credential closes the call with UNAUTHENTICATED and the request
 * never reaches the service handler. Drives {@link GrpcAuthInterceptor#interceptCall} directly with
 * a real {@link ServerSecurity} and hand-written gRPC test doubles (no mocking framework).
 */
class GrpcAdminAuthInterceptorIT extends BaseGraphServerTest {

  private GrpcAuthInterceptor interceptor() {
    final ServerSecurity security = getServer(0).getSecurity();
    return new GrpcAuthInterceptor(security);
  }

  private CreateDatabaseRequest requestWithCredentials(final String user, final String password) {
    final CreateDatabaseRequest.Builder builder = CreateDatabaseRequest.newBuilder().setName("issue4792_db").setType("document");
    if (user != null)
      builder.setCredentials(DatabaseCredentials.newBuilder().setUsername(user).setPassword(password == null ? "" : password).build());
    return builder.build();
  }

  @Test
  void adminCallWithInvalidCredentialsIsRejectedBeforeReachingHandler() {
    final RecordingServerCall<CreateDatabaseRequest, CreateDatabaseResponse> call = new RecordingServerCall<>(
        ArcadeDbAdminServiceGrpc.getCreateDatabaseMethod());
    final AtomicBoolean delivered = new AtomicBoolean(false);
    final ServerCallHandler<CreateDatabaseRequest, CreateDatabaseResponse> handler = recordingHandler(delivered);

    final ServerCall.Listener<CreateDatabaseRequest> listener = interceptor().interceptCall(call, new Metadata(), handler);
    listener.onMessage(requestWithCredentials("root", "this_is_the_wrong_password"));
    listener.onHalfClose();

    assertThat(delivered).as("request must not reach the admin handler with invalid credentials").isFalse();
    assertThat(call.closeStatus).as("call must be closed by the interceptor").isNotNull();
    assertThat(call.closeStatus.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  void adminCallWithoutCredentialsIsRejectedBeforeReachingHandler() {
    final RecordingServerCall<CreateDatabaseRequest, CreateDatabaseResponse> call = new RecordingServerCall<>(
        ArcadeDbAdminServiceGrpc.getCreateDatabaseMethod());
    final AtomicBoolean delivered = new AtomicBoolean(false);
    final ServerCallHandler<CreateDatabaseRequest, CreateDatabaseResponse> handler = recordingHandler(delivered);

    final ServerCall.Listener<CreateDatabaseRequest> listener = interceptor().interceptCall(call, new Metadata(), handler);
    listener.onMessage(requestWithCredentials(null, null));
    listener.onHalfClose();

    assertThat(delivered).as("request must not reach the admin handler without credentials").isFalse();
    assertThat(call.closeStatus).as("call must be closed by the interceptor").isNotNull();
    assertThat(call.closeStatus.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  void adminCallWithBlankUsernameIsRejectedBeforeReachingHandler() {
    final RecordingServerCall<CreateDatabaseRequest, CreateDatabaseResponse> call = new RecordingServerCall<>(
        ArcadeDbAdminServiceGrpc.getCreateDatabaseMethod());
    final AtomicBoolean delivered = new AtomicBoolean(false);
    final ServerCallHandler<CreateDatabaseRequest, CreateDatabaseResponse> handler = recordingHandler(delivered);

    final ServerCall.Listener<CreateDatabaseRequest> listener = interceptor().interceptCall(call, new Metadata(), handler);
    // Credentials present but blank username must fail closed.
    listener.onMessage(requestWithCredentials("   ", "irrelevant"));
    listener.onHalfClose();

    assertThat(delivered).as("request with a blank username must not reach the admin handler").isFalse();
    assertThat(call.closeStatus).isNotNull();
    assertThat(call.closeStatus.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  void adminCallPassesThroughWhenSecurityDisabled() {
    // securityEnabled is false when no ServerSecurity is configured; admin methods are then reachable,
    // consistent with the data-plane methods.
    final GrpcAuthInterceptor interceptor = new GrpcAuthInterceptor(null);
    final RecordingServerCall<CreateDatabaseRequest, CreateDatabaseResponse> call = new RecordingServerCall<>(
        ArcadeDbAdminServiceGrpc.getCreateDatabaseMethod());
    final AtomicBoolean delivered = new AtomicBoolean(false);
    final ServerCallHandler<CreateDatabaseRequest, CreateDatabaseResponse> handler = recordingHandler(delivered);

    final ServerCall.Listener<CreateDatabaseRequest> listener = interceptor.interceptCall(call, new Metadata(), handler);
    listener.onMessage(requestWithCredentials(null, null));
    listener.onHalfClose();

    assertThat(delivered).as("admin call must pass through when security is disabled").isTrue();
    assertThat(call.closeStatus).isNull();
  }

  @Test
  void adminCallWithValidCredentialsReachesHandler() {
    final RecordingServerCall<CreateDatabaseRequest, CreateDatabaseResponse> call = new RecordingServerCall<>(
        ArcadeDbAdminServiceGrpc.getCreateDatabaseMethod());
    final AtomicBoolean delivered = new AtomicBoolean(false);
    final ServerCallHandler<CreateDatabaseRequest, CreateDatabaseResponse> handler = recordingHandler(delivered);

    final ServerCall.Listener<CreateDatabaseRequest> listener = interceptor().interceptCall(call, new Metadata(), handler);
    listener.onMessage(requestWithCredentials("root", DEFAULT_PASSWORD_FOR_TESTS));
    listener.onHalfClose();

    assertThat(delivered).as("request with valid credentials must reach the admin handler").isTrue();
    assertThat(call.closeStatus).as("interceptor must not reject a valid request").isNull();
  }

  private static <ReqT, RespT> ServerCallHandler<ReqT, RespT> recordingHandler(final AtomicBoolean delivered) {
    return (call, headers) -> new ServerCall.Listener<>() {
      @Override
      public void onMessage(final ReqT message) {
        delivered.set(true);
      }
    };
  }

  /**
   * Minimal hand-written {@link ServerCall} test double that records the {@link Status} the
   * interceptor closes the call with.
   */
  private static final class RecordingServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
    private final MethodDescriptor<ReqT, RespT> methodDescriptor;
    private       Status                        closeStatus;

    private RecordingServerCall(final MethodDescriptor<ReqT, RespT> methodDescriptor) {
      this.methodDescriptor = methodDescriptor;
    }

    @Override
    public void request(final int numMessages) {
    }

    @Override
    public void sendHeaders(final Metadata headers) {
    }

    @Override
    public void sendMessage(final RespT message) {
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      this.closeStatus = status;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
      return methodDescriptor;
    }
  }
}
