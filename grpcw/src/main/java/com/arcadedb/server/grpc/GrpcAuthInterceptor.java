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
import io.grpc.Context;
import io.grpc.Contexts;
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
    this.securityEnabled = (security != null && security.getUsers() != null && !security.getUsers().isEmpty());
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

    // Skip auth for admin service - it handles its own authentication via request body
    if (methodName.startsWith("com.arcadedb.grpc.ArcadeDbAdminService/")) {
      return next.startCall(call, headers);
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
