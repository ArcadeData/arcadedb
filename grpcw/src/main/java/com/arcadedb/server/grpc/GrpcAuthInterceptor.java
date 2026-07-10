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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.security.ServerSecurity;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.logging.Level;

/**
 * Authentication interceptor for gRPC requests
 */
class GrpcAuthInterceptor implements ServerInterceptor {

  private static final String               BEARER_TYPE          = "Bearer";
  private static final Metadata.Key<String> AUTHORIZATION_HEADER =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> USER_HEADER          =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER      =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER      =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);
  private final        ServerSecurity          security;
  private final        boolean                 securityEnabled;
  private final        HttpAuthSessionManager  authSessionManager;

  public GrpcAuthInterceptor(final ServerSecurity security) {
    this(security, null);
  }

  public GrpcAuthInterceptor(final ServerSecurity security, final HttpAuthSessionManager authSessionManager) {
    this.security = security;
    this.authSessionManager = authSessionManager;
    // Check if security is enabled by checking if it's not null and has users configured
    this.securityEnabled = security != null && security.getUsers() != null && !security.getUsers().isEmpty();
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {

    String methodName = call.getMethodDescriptor().getFullMethodName();

    // Skip auth for health check and reflection
    if (methodName.startsWith("grpc.health.") ||
        methodName.startsWith("grpc.reflection.")) {
      return next.startCall(call, headers);
    }

    // Admin service: enforce authentication centrally from the request-body credentials instead of
    // trusting every RPC to authenticate itself. This is the central authentication choke point: a
    // missing, malformed or invalid credential closes the call before the request reaches the
    // handler, so an admin method that forgets its own authenticate() call cannot expose a side
    // effect. This choke point enforces authentication only; admin-role authorization for mutating
    // operations (create/drop database) is enforced by the handlers themselves.
    if (methodName.startsWith("com.arcadedb.grpc.ArcadeDbAdminService/")) {
      // If security is not enabled, allow all requests (consistent with the data-plane methods below)
      if (!securityEnabled)
        return next.startCall(call, headers);

      // All admin RPCs are unary, so onMessage fires exactly once with the full request carrying the
      // credentials. If a client-streaming admin RPC is ever added, revisit this per-message logic.
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
        private boolean halted = false;

        @Override
        public void onMessage(final ReqT message) {
          if (halted)
            return;
          boolean authenticated;
          try {
            authenticated = authenticateAdminRequest(message);
          } catch (final Exception e) {
            // Keep the fail-closed posture uniform with the rest of the interceptor: any unexpected
            // error denies the call rather than surfacing as a generic UNKNOWN status.
            LogManager.instance().log(this, Level.FINE, "Admin authentication error", e);
            authenticated = false;
          }
          if (!authenticated) {
            halted = true;
            call.close(Status.UNAUTHENTICATED.withDescription("Authentication required"), new Metadata());
            return;
          }
          super.onMessage(message);
        }

        @Override
        public void onHalfClose() {
          if (halted)
            return;
          super.onHalfClose();
        }
      };
    }

    // If security is not enabled, allow all requests
    if (!securityEnabled) {
      return next.startCall(call, headers);
    }

    try {
      // Get database name from header (required for authentication)
      String database = headers.get(DATABASE_HEADER);
      if (database == null || database.isEmpty()) {
        database = "default"; // Use default database if not specified
      }

      // Try Bearer token authentication first
      final String authorization = headers.get(AUTHORIZATION_HEADER);
      if (authorization != null && authorization.startsWith(BEARER_TYPE)) {
        final String token = authorization.substring(BEARER_TYPE.length()).trim();
        final HttpAuthSession session = getValidSession(token, database);
        if (session == null) {
          call.close(Status.UNAUTHENTICATED.withDescription("Invalid token"), new Metadata());
          return new ServerCall.Listener<ReqT>() {
          };
        }
        // Add user to context from session
        final Context context = Context.current().withValue(USER_CONTEXT_KEY, session.getUser().getName());
        return Contexts.interceptCall(context, call, headers, next);
      } else {
        // Try basic authentication
        final String username = headers.get(USER_HEADER);
        final String password = headers.get(PASSWORD_HEADER);

        if (username == null || password == null) {
          // No authentication provided for secured server
          call.close(Status.UNAUTHENTICATED.withDescription("Authentication required"), new Metadata());
          return new ServerCall.Listener<ReqT>() {
          };
        } else {
          // Validate credentials
          if (!validateCredentials(username, password, database)) {
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid credentials"), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
          }
          // Add user to context
          final Context context = Context.current().withValue(USER_CONTEXT_KEY, username);
          return Contexts.interceptCall(context, call, headers, next);
        }
      }

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Authentication error", e);
      call.close(Status.INTERNAL.withDescription("Authentication error"), new Metadata());
      return new ServerCall.Listener<ReqT>() {
      };
    }
  }

  /**
   * Validates the credentials carried in the body of an admin request. Every admin request message
   * declares an optional {@code DatabaseCredentials credentials = 1} field; the credential is
   * resolved generically via the protobuf descriptor so new admin RPCs are covered automatically.
   * Returns {@code true} only when valid credentials authenticate against server security. Fails
   * closed: a non-protobuf message, a missing credentials field, a blank username or any
   * authentication failure all return {@code false}.
   */
  private boolean authenticateAdminRequest(final Object message) {
    if (!(message instanceof final Message protoMessage))
      return false;

    // 'credentials' is a message-typed field on every admin request, so hasField() reports explicit
    // presence (proto3 tracks presence for message fields). This presence check is coupled to that
    // field staying message-typed.
    final FieldDescriptor credentialsField = protoMessage.getDescriptorForType().findFieldByName("credentials");
    if (credentialsField == null || !protoMessage.hasField(credentialsField))
      return false;

    if (!(protoMessage.getField(credentialsField) instanceof final DatabaseCredentials credentials))
      return false;

    final String username = credentials.getUsername();
    if (username == null || username.isBlank())
      return false;

    return validateCredentials(username, credentials.getPassword(), null);
  }

  private HttpAuthSession getValidSession(final String token, final String database) {
    if (authSessionManager == null) {
      LogManager.instance().log(this, Level.FINE,
          "Token authentication not available - no session manager configured");
      return null;
    }

    final HttpAuthSession session = authSessionManager.getSessionByToken(token);
    if (session == null) {
      LogManager.instance().log(this, Level.FINE,
          "Invalid or expired token for database: %s", database);
      return null;
    }

    return session;
  }

  private boolean validateCredentials(String username, String password, String database) {
    if (security == null) {
      return true; // No security configured
    }

    try {
      // ArcadeDB's authenticate method requires database name as well
      // Returns a SecurityUser object if authentication succeeds, null otherwise
      Object authenticatedUser = security.authenticate(username, password, database);
      return authenticatedUser != null;
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Failed to authenticate user: %s for database: %s", e, username, database);
      return false;
    }
  }

  // Context key for storing authenticated user
  public static final Context.Key<String> USER_CONTEXT_KEY = Context.key("user");
}
