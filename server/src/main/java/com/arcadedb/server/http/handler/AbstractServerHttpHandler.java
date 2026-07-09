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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.exception.*;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.transport.RequestReplyReceiverContext;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public abstract class AbstractServerHttpHandler implements HttpHandler {
  private static final String AUTHORIZATION_BASIC  = "Basic";
  private static final String AUTHORIZATION_BEARER = "Bearer";
  // Cached once: tryFromString scans/validates the header name, wasteful to repeat on every request.
  private static final HttpString REQUEST_ID_HEADER = HttpString.tryFromString(IdempotencyCache.HEADER_REQUEST_ID);
  // Response header set by session-establishing routes (e.g. /begin). Its presence means the response
  // is session-scoped and must not be replayed from the idempotency cache (the session id would be lost).
  private static final HttpString SESSION_ID_HEADER = HttpString.tryFromString(HttpSessionManager.ARCADEDB_SESSION_ID);
  // Bounded wait for a concurrent identical retry to observe the in-flight winner's result before it
  // gives up and executes on its own. Caps worker-thread blocking so a slow request cannot pile up retries.
  private static final long       IN_FLIGHT_WAIT_MS = 5_000L;
  // Upper bound on a client-supplied X-Request-Id we echo and log, to keep a hostile value bounded.
  private static final int        MAX_REQUEST_ID_LENGTH = 128;
  // Process-wide monotonic counter used, together with a per-thread random, to mint cheap correlation ids
  // when the client sends no X-Request-Id. Avoids the shared-SecureRandom cost of UUID.randomUUID() per request.
  private static final AtomicLong CORRELATION_ID_COUNTER = new AtomicLong();
  protected final HttpServer httpServer;

  public AbstractServerHttpHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, JSONObject payload)
          throws Exception;

  protected String parseRequestPayload(final HttpServerExchange e) {
    if (!e.isInIoThread() && !e.isBlocking())
      e.startBlocking();

    if (!mustExecuteOnWorkerThread())
      LogManager.instance()
              .log(this, Level.SEVERE, "Error: handler must return true at mustExecuteOnWorkerThread() to read payload from request");

    final AtomicReference<String> result = new AtomicReference<>();
    e.getRequestReceiver().receiveFullBytes(
            // OK
            (exchange, data) -> result.set(new String(data, DatabaseFactory.getDefaultCharset())),
            // ERROR
            (exchange, err) -> {
              LogManager.instance().log(this, Level.SEVERE, "receiveFullBytes completed with an error: %s", err, err.getMessage());
              exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
              exchange.getResponseSender().send("Invalid Request");
            });
    return result.get();
  }

  @Override
  public void handleRequest(final HttpServerExchange exchange) {
    if (mustExecuteOnWorkerThread() && exchange.isInIoThread()) {
      exchange.dispatch(this);
      return;
    }

    // An idempotent POST may block briefly on IdempotencyCache await() while a concurrent identical retry
    // is in flight; that must never happen on an Undertow IO thread (blocking IO threads starves the
    // server). Dispatch to a worker thread first for handlers that would otherwise run on the IO thread.
    // A blank X-Request-Id is not treated as idempotent (matches the gating below).
    final String dispatchRequestId = exchange.getRequestHeaders().getFirst(IdempotencyCache.HEADER_REQUEST_ID);
    if (exchange.isInIoThread()
        && "POST".equalsIgnoreCase(exchange.getRequestMethod().toString())
        && dispatchRequestId != null && !dispatchRequestId.isBlank()
        && exchange.getRequestHeaders().getFirst(SESSION_ID_HEADER) == null) {
      exchange.dispatch(this);
      return;
    }

    // Return 503 during snapshot installation to prevent cryptic errors
    if (httpServer.getServer().isSnapshotInstallInProgress()) {
      exchange.setStatusCode(503);
      exchange.getResponseHeaders().put(HttpString.tryFromString("Retry-After"), "5");
      exchange.getResponseSender().send(
          error2json("Server is installing a snapshot, please retry", "", null, null, null));
      return;
    }

    // Always-on RED timer: capture the start of the worker-thread (or IO-thread for handlers that
    // do not dispatch) request handling. Recorded in the finally block below into the
    // arcadedb.http.requests Micrometer timer. When no tracer is registered this is metrics-only.
    final long httpStartNanos = System.nanoTime();
    ProtocolContext.set("http");

    // Span-only Observation wrapping request handling. With no tracer registered the server's
    // ObservationRegistry has no handlers, so this is a zero-overhead no-op and the default
    // (tracing-disabled) behavior is unchanged. When the optional tracing plugin attaches a tracer
    // the same code emits an OTLP span (continuing an inbound traceparent when present). HTTP
    // latency metrics remain on the dedicated arcadedb.http.requests timer recorded in finally.
    final Observation observation = Observation.createNotStarted("arcadedb.http.server.requests",
            () -> {
              // Built lazily: the supplier is only invoked when a tracer is attached, so the
              // default (tracing-disabled) path allocates nothing. The carrier lets the tracing
              // handler read an inbound W3C traceparent header to continue an upstream trace.
              final RequestReplyReceiverContext<HttpServerExchange, Object> ctx = new RequestReplyReceiverContext<>(
                  (carrier, key) -> carrier.getRequestHeaders().getFirst(key));
              ctx.setCarrier(exchange);
              return ctx;
            }, httpServer.getServer().getObservationRegistry())
        .lowCardinalityKeyValue("method", exchange.getRequestMethod().toString())
        .lowCardinalityKeyValue("path", pathTemplate(exchange))
        .lowCardinalityKeyValue("db", databaseTag(exchange));
    observation.start();
    // Open the scope inside the try so that if a misbehaving handler throws on scope-open the
    // catch/finally still runs: the observation is stopped (not leaked) and the request is answered
    // with an error rather than propagating an uncaught exception.
    Observation.Scope observationScope = null;

    // Idempotency reservation bookkeeping, visible to the finally block: when this request owns a PENDING
    // marker and execution throws, the finally clears it so a concurrent identical retry is released
    // instead of blocking until the marker's TTL expires.
    String  idempotencyKey             = null;
    boolean idempotencyReservationOwned = false;

    try {
      observationScope = observation.openScope();
      LogManager.instance().setContext(httpServer.getServer().getServerName());

      // Per-request correlation context (issue #4466). requestId always works (generated when the
      // client sends no X-Request-Id); db comes from the path template; traceId/spanId are populated
      // only when the optional tracing plugin has registered a supplier - the observation scope is
      // already open above, so the active span is visible here. Cleared in the finally block to avoid
      // leaking across pooled Undertow worker threads.
      String correlationRequestId = sanitizeRequestId(exchange.getRequestHeaders().getFirst(IdempotencyCache.HEADER_REQUEST_ID));
      if (correlationRequestId == null)
        correlationRequestId = generateCorrelationId();
      exchange.getResponseHeaders().put(REQUEST_ID_HEADER, correlationRequestId);
      // The supplier is an SPI: tolerate an array shorter than 2 (or null) instead of indexing blindly.
      final String[] traceContext = LogManager.instance().currentTraceContext();
      final String traceId = traceContext != null && traceContext.length > 0 ? traceContext[0] : null;
      final String spanId = traceContext != null && traceContext.length > 1 ? traceContext[1] : null;
      LogManager.instance().setCorrelation(correlationRequestId, databaseTag(exchange), traceId, spanId);

      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      ServerSecurityUser user = null;

      // Cluster-internal forwarded auth: a follower forwarded a request on behalf of an
      // end user. The original per-node session token (Bearer AU-...) cannot be resolved on
      // the leader, so the follower substitutes X-ArcadeDB-Cluster-Token plus
      // X-ArcadeDB-Forwarded-User. Validated before the standard Authorization header check.
      final HeaderValues clusterTokenHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Cluster-Token");
      if (clusterTokenHeader != null && !clusterTokenHeader.isEmpty()) {
        user = validateClusterForwardedAuth(exchange,
            clusterTokenHeader.getFirst(),
            exchange.getRequestHeaders().get("X-ArcadeDB-Forwarded-User"));
        if (user == null)
          return; // 401 already sent
      }

      if (user == null) {
        final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
        if (isRequireAuthentication() && (authorization == null || authorization.isEmpty())) {
          exchange.setStatusCode(401);
          exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic");
          sendErrorResponse(exchange, 401, "", null, null);
          return;
        }

        if (authorization != null) {
          try {
            final String auth = authorization.getFirst();

            if (auth.startsWith(AUTHORIZATION_BEARER)) {
              // Bearer token authentication
              final String token = auth.substring(AUTHORIZATION_BEARER.length()).trim();

              if (ApiTokenConfiguration.isApiToken(token)) {
                // API token authentication (at- prefix)
                try {
                  user = httpServer.getServer().getSecurity().authenticateByApiToken(token);
                } catch (final ServerSecurityException ex) {
                  exchange.setStatusCode(401);
                  sendErrorResponse(exchange, 401, "Invalid or expired API token", null, null);
                  return;
                }
              } else {
                // Session token authentication (AU- prefix)
                final HttpAuthSession authSession = httpServer.getAuthSessionManager().getSessionByToken(token);
                if (authSession == null) {
                  exchange.setStatusCode(401);
                  sendErrorResponse(exchange, 401, "Invalid or expired authentication token", null, null);
                  return;
                }
                user = authSession.getUser();
              }

            } else if (auth.startsWith(AUTHORIZATION_BASIC)) {
              // Basic authentication
              final String authPairCypher = auth.substring(AUTHORIZATION_BASIC.length() + 1);

              final String authPairClear = new String(Base64.getDecoder().decode(authPairCypher), DatabaseFactory.getDefaultCharset());

              final String[] authPair = authPairClear.split(":");

              if (authPair.length != 2) {
                sendErrorResponse(exchange, 403, "Basic authentication error", null, null);
                return;
              }

              user = authenticate(authPair[0], authPair[1]);

            } else {
              sendErrorResponse(exchange, 403, "Authentication not supported", null, null);
              return;
            }

          } catch (ServerSecurityException e) {
            // PASS THROUGH
            throw e;
          } catch (Exception e) {
            throw new ServerSecurityException("Authentication error");
          }
        }
      }

      JSONObject payload = null;
      String payloadAsString = null;
      if (mustExecuteOnWorkerThread()) {
        payloadAsString = parseRequestPayload(exchange);
        if (requiresJsonPayload() && payloadAsString != null && !payloadAsString.isBlank())
          try {
            payload = new JSONObject(payloadAsString.trim());
          } catch (Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Error parsing request payload: %s", e.getMessage());
          }
      }

      // Idempotency applies only to POST requests that carry a non-blank X-Request-Id and are NOT part of
      // an open, client-managed session transaction (a session-scoped request's outcome is not settled
      // until the client commits, so it must never be cached/replayed). A blank id is ignored: distinct
      // clients sending an empty header would otherwise collide on the same composite key.
      final String rawRequestId = exchange.getRequestHeaders().getFirst(IdempotencyCache.HEADER_REQUEST_ID);
      final boolean idempotentPost = "POST".equalsIgnoreCase(exchange.getRequestMethod().toString())
          && rawRequestId != null && !rawRequestId.isBlank()
          && exchange.getRequestHeaders().getFirst(SESSION_ID_HEADER) == null;

      if (idempotentPost) {
        // Bind the key to method/path/database/body so a reused correlation id cannot replay a different
        // request's response (the core defect: same X-Request-Id across distinct writes).
        idempotencyKey = buildIdempotencyKey(rawRequestId, exchange.getRequestMethod().toString(),
            exchange.getRelativePath(), databaseTag(exchange), payloadAsString);
        final String currentPrincipal = user != null ? user.getName() : null;

        final IdempotencyCache.Reservation reservation = httpServer.getIdempotencyCache().reserve(idempotencyKey);
        if (reservation.isHit()) {
          if (replayCachedResponse(exchange, reservation.entry(), currentPrincipal))
            return;
          // Principal mismatch: fall through and execute as this caller, without owning the reservation.
        } else if (reservation.isInFlight()) {
          // A concurrent identical retry is already executing. Wait briefly for its result rather than
          // running the write a second time; if it does not settle in time, fall through and execute uncached.
          if (reservation.entry().await(IN_FLIGHT_WAIT_MS)
              && replayCachedResponse(exchange, httpServer.getIdempotencyCache().get(idempotencyKey), currentPrincipal))
            return;
        } else if (reservation.isReserved())
          idempotencyReservationOwned = true;
      }

      final ExecutionResponse response = execute(exchange, user, payload);
      if (response != null) {
        response.send(exchange);
        if (idempotencyReservationOwned) {
          // Do not cache a response that established a client session (e.g. /begin): replaying it would
          // return the body without the arcadedb-session-id header, orphaning the real session.
          if (exchange.getResponseHeaders().contains(SESSION_ID_HEADER))
            httpServer.getIdempotencyCache().abort(idempotencyKey);
          else
            httpServer.getIdempotencyCache().complete(idempotencyKey, response.getCode(), response.getResponse(),
                response.getBinary(), user != null ? user.getName() : null);
          idempotencyReservationOwned = false;
        }
      } else if (idempotencyReservationOwned) {
        httpServer.getIdempotencyCache().abort(idempotencyKey);
        idempotencyReservationOwned = false;
      }

    } catch (final ServerSecurityException e) {
      // PASS SecurityException TO THE CLIENT
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on command execution (%s): %s",
              SecurityException.class.getSimpleName(), e.getMessage());
      sendErrorResponse(exchange, 403, "Security error", e, null);
    } catch (final SecurityException e) {
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on command execution (%s): %s",
              SecurityException.class.getSimpleName(), e.getMessage());
      sendErrorResponse(exchange, 403, "Security error", e, null);
    } catch (final ServerIsNotTheLeaderException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, e.getLeaderAddress());
    } catch (final NeedRetryException e) {
      LogManager.instance()
              .log(this, Level.FINE, "Error on command execution (%s): %s", getClass().getSimpleName(), e.getMessage());
      sendErrorResponse(exchange, 503, "Cannot execute command", e, null);
    } catch (final TransactionCommittedRemotelyException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      // 409 Conflict, NOT 5xx (#5064/#5075 review): the transaction IS durably committed cluster-wide -
      // only the local apply failed. A 5xx would invite HTTP clients and load balancers to RETRY, which
      // would apply the changes a second time (duplicate inserts) - the exact hazard the distinct
      // exception type exists to prevent. Same rationale as the DuplicatedKeyException 409 below (#4350).
      sendErrorResponse(exchange, 409, "Transaction committed cluster-wide but the local apply failed - do not retry", e, null);
    } catch (final DuplicatedKeyException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      // 409 Conflict (RFC 9110 §15.5.10): a unique-constraint violation is a client data conflict,
      // not a transient server-availability problem. 503 told clients/load balancers the request was
      // retry-worthy, amplifying the bad write. See issue #4350.
      sendErrorResponse(exchange, 409, "Found duplicate key in index", e,
              e.getIndexName() + "|" + e.getKeys() + "|" + e.getCurrentIndexedRID());
    } catch (final RecordNotFoundException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 404, "Record not found", e, null);
    } catch (final QueryNotIdempotentException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 400, "Query is not idempotent", e, null);
    } catch (final IllegalArgumentException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, null);
    } catch (final CommandExecutionException | CommandParsingException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      if (realException instanceof QueryNotIdempotentException) {
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 400, "Query is not idempotent", realException, null);
      } else if (realException instanceof SecurityException) {
        LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on command execution (%s): %s",
                SecurityException.class.getSimpleName(), realException.getMessage());
        sendErrorResponse(exchange, 403, "Security error", realException, null);
      } else if (realException instanceof TransactionCommittedRemotelyException committedRemotely) {
        // Symmetric with the un-wrapped arm (#5064/#5075): a wrapped committed-remotely outcome must keep
        // its non-retryable 409 - degrading to 500 invites the client retry that inserts duplicates of
        // records the cluster already committed. Same defense-in-depth as the DuplicatedKeyException
        // branch below.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 409, "Transaction committed cluster-wide but the local apply failed - do not retry",
                committedRemotely, null);
      } else if (realException instanceof DuplicatedKeyException dup) {
        // Symmetric with the un-wrapped DuplicatedKeyException catch arm. Some code paths
        // (e.g. script execution, command planners) wrap DuplicatedKeyException in
        // CommandExecutionException; without this branch the response would degrade to 500.
        // See issue #4350.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 409, "Found duplicate key in index", dup,
                dup.getIndexName() + "|" + dup.getKeys() + "|" + dup.getCurrentIndexedRID());
      } else {
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        e.getMessage());
        sendErrorResponse(exchange, 500, "Cannot execute command", realException, null);
      }
    } catch (final TransactionException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      if (realException instanceof SecurityException) {
        LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on transaction execution (%s): %s",
                SecurityException.class.getSimpleName(), realException.getMessage());
        sendErrorResponse(exchange, 403, "Security error", realException, null);
      } else if (realException instanceof QueryNotIdempotentException) {
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 400, "Query is not idempotent", realException, null);
      } else if (realException instanceof IllegalArgumentException) {
        // Bad client input (malformed parameter, unparseable marker, etc.) wrapped by the
        // surrounding transaction wrapper. Surface as HTTP 400 just like the un-wrapped
        // IllegalArgumentException catch arm above so the contract is symmetric regardless of
        // whether the request happened to run inside a transaction.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 400, "Cannot execute command", realException, null);
      } else if (realException instanceof TransactionCommittedRemotelyException committedRemotely) {
        // Same as the un-wrapped committed-remotely arm above (#5064/#5075), reached when the auto-commit
        // wrapper re-wrapped it: the non-retryable 409 must survive the wrapping.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 409, "Transaction committed cluster-wide but the local apply failed - do not retry",
                committedRemotely, null);
      } else if (realException instanceof DuplicatedKeyException dup) {
        // Same as the un-wrapped DuplicatedKeyException arm above, but reached when the
        // exception was thrown inside the auto-commit transaction wrapper in
        // DatabaseAbstractHandler (which wraps any Exception thrown by execute() in a
        // TransactionException). Without this branch the response degrades to 500. See issue #4350.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 409, "Found duplicate key in index", dup,
                dup.getIndexName() + "|" + dup.getKeys() + "|" + dup.getCurrentIndexedRID());
      } else {
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on transaction execution (%s): %s", getClass().getSimpleName(),
                        e.getMessage());
        sendErrorResponse(exchange, 500, "Error on transaction commit", realException, null);
      }
    } catch (final Throwable e) {
      // Check if a SecurityException is wrapped at any depth
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof SecurityException) {
          LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on command execution (%s): %s",
                  SecurityException.class.getSimpleName(), cause.getMessage());
          sendErrorResponse(exchange, 403, "Security error", cause, null);
          return;
        }
        cause = cause.getCause();
      }
      LogManager.instance()
              .log(this, getErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(), e.getMessage());
      sendErrorResponse(exchange, 500, "Internal error", e, null);
    } finally {
      // If execution threw after this request reserved the idempotency key, clear the PENDING marker so a
      // concurrent identical retry is released immediately instead of blocking until the marker's TTL.
      if (idempotencyReservationOwned) {
        try {
          httpServer.getIdempotencyCache().abort(idempotencyKey);
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.WARNING, "Error aborting idempotency reservation", t);
        }
      }

      // Finalize the optional tracing span. Each step is isolated so a failure in the optional
      // tracing layer can never skip the core cleanup or the RED timer below; closing the scope is
      // attempted independently to avoid thread-local context leaking across pooled worker threads.
      try {
        observation.lowCardinalityKeyValue("status", Integer.toString(exchange.getStatusCode()));
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.WARNING, "Error tagging tracing observation", t);
      }
      try {
        if (observationScope != null)
          observationScope.close();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.WARNING, "Error closing tracing observation scope", t);
      }
      try {
        observation.stop();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.WARNING, "Error stopping tracing observation", t);
      }

      ProtocolContext.clear();
      LogManager.instance().setContext(null);
      // Invariant: the correlation context stays populated until here, AFTER observation.stop() above
      // has fired the tracing/observation handlers. LogCorrelationIT relies on reading the requestId
      // inside an ObservationHandler.onStop callback, so this clear must remain the last step.
      LogManager.instance().clearCorrelation();

      Timer.builder("arcadedb.http.requests")
          .description("HTTP request duration")
          .tag("method", exchange.getRequestMethod().toString())
          .tag("path", pathTemplate(exchange))
          .tag("status", Integer.toString(exchange.getStatusCode()))
          .tag("db", databaseTag(exchange))
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry)
          .record(System.nanoTime() - httpStartNanos, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Returns a bounded, low-cardinality path tag for the request: the route template
   * (e.g. {@code /command/{database}}) resolved by the Undertow routing handler, never the raw URI
   * carrying the concrete database name. Falls back to the relative path for fixed routes
   * registered without a path template.
   */
  private static String pathTemplate(final HttpServerExchange exchange) {
    final PathTemplateMatch match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
    if (match != null)
      return match.getMatchedTemplate();
    return exchange.getRelativePath();
  }

  /**
   * Replays a cached idempotent response onto {@code exchange}, honoring the stored principal so a
   * different user cannot replay another caller's response merely by guessing the request id. Returns
   * false (nothing written) when there is no usable entry or the principal does not match, so the caller
   * can fall back to executing the request.
   */
  private boolean replayCachedResponse(final HttpServerExchange exchange, final IdempotencyCache.CachedEntry cached,
      final String currentPrincipal) {
    if (cached == null)
      return false;
    if (cached.principal != null && !cached.principal.equals(currentPrincipal))
      return false;
    exchange.setStatusCode(cached.statusCode);
    // Replay a binary body faithfully; falling back to the string body would send an empty response for a
    // cached binary export/backup and silently lose data.
    if (cached.binary != null)
      exchange.getResponseSender().send(ByteBuffer.wrap(cached.binary));
    else
      exchange.getResponseSender().send(cached.body != null ? cached.body : "");
    return true;
  }

  /**
   * Builds the idempotency cache key for a POST request. The key is a SHA-256 over the client
   * {@code X-Request-Id} joined with the HTTP method, path, database and request body, so two unrelated
   * requests that reuse the same correlation id (a common proxy / client practice) never collide and
   * replay each other's response. Package-private for direct unit testing.
   */
  static String buildIdempotencyKey(final String requestId, final String method, final String path,
      final String database, final String body) {
    try {
      final MessageDigest md = MessageDigest.getInstance("SHA-256");
      final Charset cs = DatabaseFactory.getDefaultCharset();
      updateDigest(md, requestId, cs);
      updateDigest(md, method, cs);
      updateDigest(md, path, cs);
      updateDigest(md, database, cs);
      if (body != null)
        md.update(body.getBytes(cs));
      final byte[] digest = md.digest();
      final StringBuilder sb = new StringBuilder(digest.length * 2);
      for (final byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (final NoSuchAlgorithmException e) {
      // SHA-256 is a JCA-mandated algorithm, so this is unreachable. Fall back to a NUL-namespaced raw key
      // that is still bound to every component, preserving correctness at the cost of a longer key.
      return requestId + " " + method + " " + path + " " + database + " " + body;
    }
  }

  private static void updateDigest(final MessageDigest md, final String value, final Charset cs) {
    if (value != null)
      md.update(value.getBytes(cs));
    // NUL separator delimits fields so ("a","b") and ("ab","") cannot produce the same digest.
    md.update((byte) 0);
  }

  /**
   * Sanitizes a client-supplied {@code X-Request-Id} before it is echoed in the response and stored in
   * the log correlation context: drops control characters (which could corrupt a log line) and caps the
   * length, returning {@code null} when nothing usable remains so the caller generates a fresh id.
   * Allocates only when the input actually needs cleaning, keeping the request hot path cheap.
   * Package-private for direct unit testing.
   */
  static String sanitizeRequestId(final String raw) {
    if (raw == null || raw.isEmpty())
      return null;
    final int len = Math.min(raw.length(), MAX_REQUEST_ID_LENGTH);
    StringBuilder cleaned = null;
    for (int i = 0; i < len; i++) {
      final char c = raw.charAt(i);
      if (c < 0x20 || c == 0x7F) {
        if (cleaned == null)
          cleaned = new StringBuilder(len).append(raw, 0, i);
      } else if (cleaned != null)
        cleaned.append(c);
    }
    final String result = cleaned != null ? cleaned.toString()
        : raw.length() > MAX_REQUEST_ID_LENGTH ? raw.substring(0, MAX_REQUEST_ID_LENGTH) : raw;
    return result.isEmpty() ? null : result;
  }

  /**
   * Mints a cheap, non-cryptographic correlation id for a request that carries no {@code X-Request-Id}.
   * This value is used only for response echo and log correlation - it is never the idempotency key, which
   * always comes from the raw client header - so a fast {@link ThreadLocalRandom} high-entropy prefix plus a
   * process-wide monotonic counter is sufficient and avoids the shared-{@code SecureRandom} synchronization
   * of {@code UUID.randomUUID()} on the request hot path. The result is short and printable, so
   * {@link #sanitizeRequestId(String)} returns it unchanged.
   * Package-private for direct unit testing.
   */
  static String generateCorrelationId() {
    return Long.toHexString(ThreadLocalRandom.current().nextLong()) + "-"
        + Long.toHexString(CORRELATION_ID_COUNTER.incrementAndGet());
  }

  /**
   * Returns the database name resolved from the route's {@code {database}} path parameter, or
   * {@code none} for routes that are not database-scoped (e.g. {@code /ready}, {@code /server}).
   */
  private static String databaseTag(final HttpServerExchange exchange) {
    final PathTemplateMatch match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
    if (match != null) {
      final String db = match.getParameters().get("database");
      if (db != null)
        return db;
    }
    return "none";
  }

  /**
   * Validates cluster-internal forwarded-auth headers. Returns the resolved user on success,
   * or {@code null} after sending a 401 response.
   */
  private ServerSecurityUser validateClusterForwardedAuth(final HttpServerExchange exchange,
      final String providedToken, final HeaderValues forwardedUserValues) {

    // Prefer the HA plugin's effective token (which may be PBKDF2-derived when not explicitly
    // configured) over the raw config value. Falls back to the raw config for non-Raft setups.
    String clusterToken = null;
    final var ha = httpServer.getServer().getHA();
    if (ha != null)
      clusterToken = ha.getClusterToken();
    if (clusterToken == null || clusterToken.isBlank())
      clusterToken = httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);

    if (clusterToken == null || clusterToken.isBlank()
        || !constantTimeEquals(clusterToken, providedToken)) {
      exchange.setStatusCode(401);
      sendErrorResponse(exchange, 401, "Invalid cluster token", null, null);
      return null;
    }

    if (forwardedUserValues == null || forwardedUserValues.isEmpty()) {
      exchange.setStatusCode(401);
      sendErrorResponse(exchange, 401, "Missing forwarded user", null, null);
      return null;
    }

    final ServerSecurityUser forwardedUser = httpServer.getServer().getSecurity()
        .getUser(forwardedUserValues.getFirst());
    if (forwardedUser == null) {
      exchange.setStatusCode(401);
      sendErrorResponse(exchange, 401, "Unknown forwarded user", null, null);
      return null;
    }
    return forwardedUser;
  }

  private static boolean constantTimeEquals(final String a, final String b) {
    if (a == null || b == null)
      return false;
    final byte[] aBytes = a.getBytes(DatabaseFactory.getDefaultCharset());
    final byte[] bBytes = b.getBytes(DatabaseFactory.getDefaultCharset());
    return MessageDigest.isEqual(aBytes, bBytes);
  }

  /**
   * Returns true if the handler require authentication to be executed, any valid user. False means the handler can be executed without authentication.
   */
  public boolean isRequireAuthentication() {
    return true;
  }

  protected ServerSecurityUser authenticate(final String userName, final String userPassword) {
    return httpServer.getServer().getSecurity().authenticate(userName, userPassword, null);
  }

  /**
   * Authorization choke point for database-scoped routes that do NOT extend {@link DatabaseAbstractHandler}
   * (time-series, batch, Prometheus and Grafana handlers). Without this check those handlers resolved and
   * operated on any database named in the path, letting a user authorized for one database read and write
   * another (cross-database IDOR, GHSA-x8mg-6r4p-87pf). Mirrors the gate in
   * {@link DatabaseAbstractHandler#execute}. Throws {@link SecurityException} (mapped to HTTP 403) when the
   * authenticated user cannot access the database; fails closed on a missing database name.
   */
  protected void checkAuthorizationOnDatabase(final ServerSecurityUser user, final String databaseName) {
    if (databaseName == null || databaseName.isEmpty())
      throw new IllegalArgumentException("Database parameter is null");
    if (user != null && !user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not allowed to access database '" + databaseName + "'");
  }

  /**
   * Ensures only the root user can execute server administration commands.
   * API token-authenticated users have synthetic names like "apitoken:&lt;name&gt;" and will
   * always fail this check — this is intentional, as token management requires root credentials.
   */
  protected void checkRootUser(ServerSecurityUser user) {
    if (!"root".equals(user.getName()))
      throw new ServerSecurityException("Only root user is authorized to execute server commands");
  }

  protected String error2json(final String error, final String detail, final Throwable exception, final String exceptionArgs,
                              final String help) {
    final JSONObject json = new JSONObject();
    json.put("error", error);
    if (detail != null)
      json.put("detail", encodeError(detail));
    if (exception != null)
      json.put("exception", exception.getClass().getName());
    if (exceptionArgs != null)
      json.put("exceptionArgs", exceptionArgs);
    if (help != null)
      json.put("help", help);
    return json.toString();
  }

  /**
   * Returns true if the handler is reading the payload in the request. In this case, the execution is delegated to the worker thread.
   */
  protected boolean mustExecuteOnWorkerThread() {
    return false;
  }

  protected boolean requiresJsonPayload() {
    return true;
  }

  protected String encodeError(final String message) {
    return message.replace("\\\\", " ").replace('\n', ' ');
  }

  /**
   * Returns the per-request correlation id echoed in the response header (issue #4466), or {@code null} if none is set.
   * Handlers that build a bespoke error body (e.g. the streaming batch endpoint) reuse this so their client-facing
   * error stays cross-referenceable with the server log, exactly like the standard {@code sendErrorResponse} envelope.
   */
  protected String getCorrelationId(final HttpServerExchange exchange) {
    return exchange.getResponseHeaders().getFirst(REQUEST_ID_HEADER);
  }

  protected String getQueryParameter(final HttpServerExchange exchange, final String name) {
    return getQueryParameter(exchange, name, null);
  }

  protected String getQueryParameter(final HttpServerExchange exchange, final String name, final String defaultValue) {
    final Deque<String> par = exchange.getQueryParameters().get(name);
    return par == null || par.isEmpty() ? defaultValue : par.getFirst();
  }

  private Level getErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ?
            Level.SEVERE :
            Level.FINE;
  }

  private Level getUserSevereErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ?
            Level.INFO :
            Level.FINE;
  }

  /**
   * Returns true when the server runs in {@code production} mode. In production the error responses conceal the
   * free-form cause chain ({@code detail}), which can leak file paths and engine internals; the bounded
   * {@code exception} class name and structured {@code exceptionArgs} are still emitted because the remote driver
   * and HA rely on them. {@code development} and {@code test} keep the full verbose body to aid debugging.
   */
  private boolean isProductionMode() {
    return "production".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE));
  }

  private void sendErrorResponse(final HttpServerExchange exchange, final int code, final String errorMessage, final Throwable e,
                                 final String exceptionArgs) {
    if (!exchange.isResponseStarted())
      exchange.setStatusCode(code);

    // Reuse the correlation id already echoed in the response header so operators can cross-reference a
    // concealed production error with the detailed server log entry.
    final String correlationId = exchange.getResponseHeaders().getFirst(REQUEST_ID_HEADER);

    exchange.getResponseSender().send(buildErrorBody(!isProductionMode(), errorMessage, e, exceptionArgs, correlationId));
  }

  /**
   * Builds the JSON error body sent to the client. The exception class name ({@code exception}) and the structured
   * {@code exceptionArgs} are a wire contract consumed by the remote Java driver
   * ({@code RemoteHttpComponent.manageException}) and by HA leader-exception reconstruction
   * ({@code RaftReplicatedDatabase.reconstructLeaderException}) to rebuild typed exceptions, leader-redirect hints and
   * duplicate-key details; they are bounded, non-sensitive values and are therefore emitted in every mode. Only the
   * free-form cause chain ({@code detail}), which can carry file paths and engine internals, is concealed in
   * production ({@code verbose == false}) so it is never leaked to a client probing endpoints. Package-private for
   * direct unit testing.
   */
  String buildErrorBody(final boolean verbose, final String errorMessage, final Throwable e, final String exceptionArgs,
                        final String correlationId) {
    final JSONObject json = new JSONObject();
    json.put("error", errorMessage);
    if (correlationId != null && !correlationId.isEmpty())
      json.put("requestId", correlationId);

    if (e != null)
      json.put("exception", e.getClass().getName());
    if (exceptionArgs != null)
      json.put("exceptionArgs", exceptionArgs);

    // The cause chain is the only free-form field: conceal it outside development/test to avoid leaking
    // internal file paths and engine errors to a client probing endpoints.
    if (verbose && e != null)
      json.put("detail", encodeError(buildDetailChain(e)));

    return json.toString();
  }

  /**
   * Renders an exception and its cause chain as a single line ({@code msg -> cause -> cause...}), stopping when a cause
   * has already been seen to avoid an infinite loop on cyclic chains. Uses identity comparison so distinct exceptions
   * with equal {@code equals}/{@code hashCode} are still walked. Package-private for direct unit testing.
   */
  static String buildDetailChain(final Throwable e) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(e.getMessage() != null ? e.getMessage() : e.toString());

    final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    visited.add(e);
    Throwable current = e.getCause();
    while (current != null && visited.add(current)) {
      buffer.append(" -> ");
      buffer.append(current.getMessage() != null ? current.getMessage() : current.getClass().getSimpleName());
      current = current.getCause();
    }
    return buffer.toString();
  }
}
