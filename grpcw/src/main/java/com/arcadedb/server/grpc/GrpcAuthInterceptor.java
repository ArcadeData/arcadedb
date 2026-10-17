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
import com.arcadedb.server.security.ServerSecurityException;
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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

import java.util.Set;
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
  /**
   * Admin methods that are reachable without credentials. Matched on the full method name so a
   * future RPC whose name merely starts with "Health" is not exempted by accident.
   */
  private static final Set<String> UNAUTHENTICATED_ADMIN_METHODS = Set.of(
      ArcadeDbAdminServiceGrpc.getHealthMethod().getFullMethodName(),
      ArcadeDbAdminServiceGrpc.getReadyMethod().getFullMethodName());

  // ---- Authentication faults (issue #10142) ----
  //
  // The constants below belong together: the meter, its stage tags, and the descriptions a refusal carries when the
  // server, not the client, is at fault.
  //
  // The status of such a refusal stays UNAUTHENTICATED on the admin path and on the security-layer arm of the
  // data-plane path, deliberately: the call must fail closed, and a client that retries on INTERNAL or UNAVAILABLE
  // would hammer a server whose credential evaluation is broken. What changes is that the refusal is no longer
  // byte-identical to a wrong password AND invisible. The data-plane outer catch keeps its pre-existing INTERNAL.

  /**
   * Requests refused because the SERVER could not evaluate the credential, as opposed to a credential the server read
   * and rejected (issue #10142). A refusal of this kind is a fault on the server, and before this counter it was
   * indistinguishable from a wrong password: the native image, missing protobuf reflection metadata, refused every
   * admin RPC as "Authentication required" with nothing at the default log level. The {@code stage} tag says where:
   * {@link #STAGE_ADMIN_CREDENTIAL_READ} (the descriptor-driven read of an admin request's credentials),
   * {@link #STAGE_AUTHENTICATE} (an unexpected failure inside {@link ServerSecurity#authenticate}, such as a security
   * backend outage) or {@link #STAGE_INTERCEPT} (any other unexpected failure on the data-plane path). Every stage is
   * also logged at SEVERE, and every stage's refusal names a server fault rather than a bad credential.
   */
  static final String AUTH_FAULTS_METER            = "grpc.auth.faults.total";
  static final String STAGE_ADMIN_CREDENTIAL_READ  = "admin-credential-read";
  static final String STAGE_AUTHENTICATE           = "authenticate";
  static final String STAGE_INTERCEPT              = "intercept";
  static final String ADMIN_CREDENTIAL_READ_FAULT  =
      "Authentication required: the server could not read the request credentials (server fault, see the server log)";
  static final String AUTHENTICATE_FAULT           =
      "Authentication error: the server could not evaluate the credentials (server fault, see the server log)";
  // The reason adminRequestFailure() gives for every credential refusal. Kept generic on the admin path, so a caller
  // is not told which part of its credential was wrong
  private static final String AUTHENTICATION_REQUIRED = "Authentication required";

  private final        ServerSecurity          security;
  private final        boolean                 securityEnabled;
  private final        HttpAuthSessionManager  authSessionManager;
  private final        Counter                 adminCredentialReadFaults;
  private final        Counter                 authenticateFaults;
  private final        Counter                 interceptFaults;

  public GrpcAuthInterceptor(final ServerSecurity security) {
    this(security, null);
  }

  public GrpcAuthInterceptor(final ServerSecurity security, final HttpAuthSessionManager authSessionManager) {
    // The server's JVM-wide registry, beside the meters GrpcMetricsInterceptor publishes
    this(security, authSessionManager, Metrics.globalRegistry);
  }

  GrpcAuthInterceptor(final ServerSecurity security, final HttpAuthSessionManager authSessionManager,
      final MeterRegistry meterRegistry) {
    this.security = security;
    this.authSessionManager = authSessionManager;
    // Check if security is enabled by checking if it's not null and has users configured
    this.securityEnabled = security != null && security.getUsers() != null && !security.getUsers().isEmpty();
    // Registered up front, so a dashboard or an alert sees a zero rather than a meter that does not exist yet
    this.adminCredentialReadFaults = faultCounter(meterRegistry, STAGE_ADMIN_CREDENTIAL_READ);
    this.authenticateFaults = faultCounter(meterRegistry, STAGE_AUTHENTICATE);
    this.interceptFaults = faultCounter(meterRegistry, STAGE_INTERCEPT);
  }

  private static Counter faultCounter(final MeterRegistry meterRegistry, final String stage) {
    return Counter.builder(AUTH_FAULTS_METER)
        .description("gRPC requests refused because the server could not evaluate the credential (a server fault, not a bad credential)")
        .tag("stage", stage)
        .register(meterRegistry);
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

    // The two container probes, exempt for the same reason the HTTP ones are: GetHealthHandler and
    // GetReadyHandler both return false from isRequireAuthentication(), so an orchestrator can probe
    // the node without holding server credentials. Requiring credentials here would leave a gRPC-only
    // deployment unable to express a liveness or readiness probe at all (issue #7304).
    //
    // Neither answer discloses anything an unauthenticated caller could not already establish by
    // opening the port: Health is constant, and Ready reports only whether this node is serving and,
    // when it is not, which of the three published readiness gates it is behind.
    if (UNAUTHENTICATED_ADMIN_METHODS.contains(methodName))
      return next.startCall(call, headers);

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

      // Every admin RPC takes exactly one request message - the unary ones, and the server-streaming
      // RestoreBackup / RestoreDatabase / ImportDatabase added in issue #7308, whose stream is on the
      // response side only. So onMessage fires exactly once, with the full request carrying the
      // credentials, and authenticating there closes the call before the handler runs.
      //
      // A CLIENT-streaming admin RPC would break that: onMessage would fire per chunk, the first
      // chunk would be the only one carrying credentials, and this listener would either re-check
      // every chunk or trust chunks it never checked. Nothing in the service is client-streaming
      // today; adding one means reworking this, not extending it.
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
        private boolean halted = false;

        @Override
        public void onMessage(final ReqT message) {
          if (halted)
            return;
          final String failure;
          try {
            failure = adminRequestFailure(message);
          } catch (final VirtualMachineError e) {
            // An OutOfMemoryError or a StackOverflowError is not an authentication fault: the JVM must see it
            throw e;
          } catch (final Exception | Error e) {
            // Keep the fail-closed posture uniform with the rest of the interceptor: any unexpected
            // error denies the call rather than surfacing as a generic UNKNOWN status. Error is caught
            // too: a missing native-image registration surfaces as ExceptionInInitializerError or
            // NoClassDefFoundError on the first touch of a class, and would otherwise escape as UNKNOWN.
            //
            // Denying the call and telling nobody why are two separate decisions (issue #10142). This is
            // a refusal the SERVER could not explain, not one the client caused, so it is logged at
            // SEVERE like the two sibling fault arms below, counted, and described as a server fault
            // rather than byte-identically to a wrong password.
            adminCredentialReadFaults.increment();
            LogManager.instance().log(GrpcAuthInterceptor.this, Level.SEVERE,
                "Admin request %s refused: its credentials could not be read. This is a server fault, not a credential refusal",
                e, methodName);
            halted = true;
            call.close(Status.UNAUTHENTICATED.withDescription(ADMIN_CREDENTIAL_READ_FAULT), new Metadata());
            return;
          }
          if (failure != null) {
            halted = true;
            // A security fault keeps its own description; every credential refusal reads as before. The sentinel
            // is compared by REFERENCE on purpose: a ServerSecurityException message is arbitrary text, and no
            // text the security layer produces can be this instance, whatever it says
            call.close(Status.UNAUTHENTICATED.withDescription(
                failure == AUTHENTICATE_FAULT ? AUTHENTICATE_FAULT : AUTHENTICATION_REQUIRED), new Metadata());
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
      // The database the caller named, or null when it named none. An absent header used to resolve to
      // the literal name "default", which ServerSecurity.authenticate treats as a real grant check: a
      // principal configured with "databases": { "graph": [...] } was refused on its first RPC because
      // it holds no grant on a database called "default" (issue #7320). Authenticating a header-less
      // call at server level instead grants nothing extra - per-database authorization is enforced
      // downstream in ArcadeDbGrpcService.validateCredentials against the database named in the REQUEST
      // BODY, which is the gate Issue4794GrpcPerDbAuthorizationIT pins.
      final String database = normalizeDatabase(headers.get(DATABASE_HEADER));

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
          // Validate credentials. The refusal repeats the reason security gave, so a missing grant does
          // not read as a mistyped password - the complaint in issue #7320.
          final String failure = authenticationFailure(username, password, database);
          if (failure != null) {
            call.close(Status.UNAUTHENTICATED.withDescription(failure), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
          }
          // Add user to context
          final Context context = Context.current().withValue(USER_CONTEXT_KEY, username);
          return Contexts.interceptCall(context, call, headers, next);
        }
      }

    } catch (final VirtualMachineError e) {
      throw e;
    } catch (final Exception | Error e) {
      // Error included for the same reason as the admin arm above: a class-initialization failure must be reported,
      // not escape as a bare UNKNOWN
      interceptFaults.increment();
      LogManager.instance().log(this, Level.SEVERE, "Authentication error", e);
      call.close(Status.INTERNAL.withDescription("Authentication error"), new Metadata());
      return new ServerCall.Listener<ReqT>() {
      };
    }
  }

  /**
   * Validates the credentials carried in the body of an admin request and returns {@code null} when
   * they authenticate, otherwise the reason for the refusal ({@link #AUTHENTICATE_FAULT}, the same
   * instance, when the server could not evaluate them). Every admin request message declares an
   * optional {@code DatabaseCredentials credentials = 1} field; the credential is resolved
   * generically via the protobuf descriptor so new admin RPCs are covered automatically.
   * Fails closed: a non-protobuf message, a missing credentials field, a blank username or any
   * authentication failure all return a non-null reason.
   */
  private String adminRequestFailure(final Object message) {
    if (!(message instanceof final Message protoMessage))
      return AUTHENTICATION_REQUIRED;

    // 'credentials' is a message-typed field on every admin request, so hasField() reports explicit
    // presence (proto3 tracks presence for message fields). This presence check is coupled to that
    // field staying message-typed.
    final FieldDescriptor credentialsField = protoMessage.getDescriptorForType().findFieldByName("credentials");
    if (credentialsField == null || !protoMessage.hasField(credentialsField))
      return AUTHENTICATION_REQUIRED;

    if (!(protoMessage.getField(credentialsField) instanceof final DatabaseCredentials credentials))
      return AUTHENTICATION_REQUIRED;

    final String username = credentials.getUsername();
    if (username == null || username.isBlank())
      return AUTHENTICATION_REQUIRED;

    return authenticationFailure(username, credentials.getPassword(), null);
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

    // The session captured its ServerSecurityUser at login, so a principal dropped afterwards would keep
    // authenticating here until the token idle-expired. Re-checked against the live users map, exactly as
    // the HTTP bearer branch does, so a revocation takes effect on this transport too.
    //
    // An existence check is enough HERE, unlike the HTTP branch which re-resolves the object, because
    // nothing on this transport reads authority off the session's user: the only other use of it is
    // session.getUser().getName() below, propagated into the gRPC context, and the services re-resolve the
    // principal by name per call. Anything that starts reading permissions off session.getUser() directly
    // must switch this to the same re-resolution, or it will honour the grants the token was minted with.
    if (security != null && security.getUser(session.getUser().getName()) == null) {
      authSessionManager.removeSession(token);
      LogManager.instance().log(this, Level.FINE,
          "Token of a principal that no longer exists, rejected for database: %s", database);
      return null;
    }

    return session;
  }

  /**
   * Treats a blank database name as no database at all, so an empty header is never handed to the grant
   * check as the database {@code ""}.
   */
  private static String normalizeDatabase(final String database) {
    return database == null || database.isBlank() ? null : database;
  }

  /**
   * Authenticates {@code username}/{@code password}, additionally requiring a grant on {@code database}
   * when one is named. Returns {@code null} when the caller is authenticated, otherwise the reason to
   * report - the message {@link ServerSecurity} itself produced, so "user has no access to database X"
   * is not reported as a bad password (issue #7320).
   */
  private String authenticationFailure(final String username, final String password, final String database) {
    if (security == null)
      return null; // No security configured

    try {
      // ArcadeDB's authenticate method takes the database name as well, and enforces the grant only when
      // it is non-null.
      return security.authenticate(username, password, database) != null ? null : "Invalid credentials";
    } catch (final ServerSecurityException e) {
      // Expected refusal (bad password, missing grant, lockout): FINE, not SEVERE, and the caller is told
      // which of them it was. A blank message would read as SUCCESS to the caller of this method, so it
      // falls back to the generic wording rather than to null.
      LogManager.instance().log(this, Level.FINE, "Failed to authenticate user: %s for database: %s", username, database);
      final String reason = e.getMessage();
      return reason == null || reason.isBlank() ? "Invalid credentials" : reason;
    } catch (final Exception e) {
      // Not a credential refusal: the security layer itself failed, so the caller is told so rather than that its
      // password was wrong (issue #10142)
      authenticateFaults.increment();
      LogManager.instance().log(this, Level.SEVERE, "Failed to authenticate user: %s for database: %s", e, username, database);
      return AUTHENTICATE_FAULT;
    }
  }

  // Context key for storing authenticated user
  public static final Context.Key<String> USER_CONTEXT_KEY = Context.key("user");
}
