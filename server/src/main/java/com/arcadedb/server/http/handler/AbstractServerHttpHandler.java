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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.exception.*;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSessionException;
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
import io.undertow.util.AttachmentKey;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public abstract class AbstractServerHttpHandler implements HttpHandler {
  // Raw request body, kept on the exchange for the handlers that need the text rather than the JSONObject
  // parsed from it: the request body is consumed once and cannot be read again from the exchange.
  public static final AttachmentKey<String> RAW_PAYLOAD = AttachmentKey.create(String.class);
  // Request body parsed as a top-level JSON array (issue #5415). A JSON array is a legitimate request body,
  // but it is not a JSONObject, so it cannot travel in the `payload` argument of execute(). It is parsed
  // once by the shared request pipeline and attached here, where a handler reads it back with
  // getPayloadAsArray(exchange) - keeping the single execute() entry point, and with it every wrapper the
  // handler hierarchy layers on top of it (database resolution, authorization, session and transaction
  // handling in DatabaseAbstractHandler).
  public static final AttachmentKey<JSONArray> ARRAY_PAYLOAD = AttachmentKey.create(JSONArray.class);

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
  // Per-thread SHA-256 for the idempotency key: reused (reset) each call so the request hot path avoids the
  // JCA provider lookup of MessageDigest.getInstance() per request. SHA-256 is JCA-mandated, so init cannot
  // fail in practice; if it ever did the digest would be unusable, so we fail fast.
  private static final ThreadLocal<MessageDigest> SHA_256_DIGEST = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is a JCA-mandated algorithm but was not found", e);
    }
  });
  // Upper bound on a client-supplied X-Request-Id we echo and log, to keep a hostile value bounded.
  private static final int        MAX_REQUEST_ID_LENGTH = 128;
  // Process-wide monotonic counter used, together with a per-thread random, to mint cheap correlation ids
  // when the client sends no X-Request-Id. Avoids the shared-SecureRandom cost of UUID.randomUUID() per request.
  private static final AtomicLong CORRELATION_ID_COUNTER = new AtomicLong();
  // Constant path tag used for any request that does not resolve to a route template (Studio "/"
  // static fallback, prefix handlers, 404 probes). Never echo the raw client URI as a tag value: it
  // is attacker-controlled and would register an unbounded number of permanent meters (issue #5025).
  private static final String     UNMATCHED_PATH_TAG = "unmatched";
  // Cache of resolved arcadedb.http.requests timers keyed by the bounded tag tuple
  // (method|path|status|db). Avoids rebuilding the Timer.Builder/Tags/Meter.Id and doing a registry
  // hash lookup on every request. The key space is bounded because every tag is low-cardinality: the
  // path is collapsed to a route template or the constant "unmatched", method and status are small
  // enumerations, and db is the finite set of database names.
  private static final ConcurrentHashMap<String, Timer> HTTP_REQUEST_TIMERS = new ConcurrentHashMap<>();
  // Tracks the database whose DatabaseContext this request bound the authenticated principal onto in
  // checkAuthorizationOnDatabase, so handleRequest's finally can clear the binding on THIS pooled worker
  // thread (GHSA-c23x-pqcj-7hfm). Handler instances are shared across threads, so this per-thread marker
  // - not an instance field - is what makes the cleanup thread-safe and precise (clears only what was bound).
  private static final ThreadLocal<DatabaseInternal>    BOUND_PRINCIPAL_DB = new ThreadLocal<>();
  protected final HttpServer httpServer;

  public AbstractServerHttpHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user, JSONObject payload)
          throws Exception;

  /**
   * Maximum number of rows an endpoint serializes into a single response when the caller states no limit of its
   * own. A non-positive value means unlimited. Shared by every row-returning endpoint so one setting governs
   * them all (issue #5711).
   */
  protected int getDefaultRowLimit() {
    return httpServer.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT);
  }

  /**
   * Validates a row limit that arrived as a JSON value and narrows it to an {@code int}. Every endpoint reading
   * a {@code limit} shares this so the same input gets the same answer: {@link Number#intValue()} truncates the
   * high bits, so {@code 3000000000} would arrive as a negative value and be read as "unlimited", silently
   * turning off the very cap the field governs (issue #5711). Out of range, NaN and a non-numeric value are all
   * client errors, mapped to HTTP 400 by the {@link IllegalArgumentException} arm below.
   */
  protected static int requireIntLimit(final Object value, final String field) {
    if (!(value instanceof Number n))
      throw new IllegalArgumentException("Field '" + field + "' must be an integer");
    final double magnitude = n.doubleValue();
    if (Double.isNaN(magnitude) || magnitude > Integer.MAX_VALUE || magnitude < Integer.MIN_VALUE)
      throw unusableLimit(field, null);
    return n.intValue();
  }

  /**
   * Rejection of a row limit that is not an integer this server can apply, worded identically wherever the limit
   * arrives from so the surfaces report the same thing.
   */
  protected static IllegalArgumentException unusableLimit(final String field, final Throwable cause) {
    return new IllegalArgumentException(
        "Field '" + field + "' must be an integer between " + Integer.MIN_VALUE + " and " + Integer.MAX_VALUE, cause);
  }

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
    // marker and execution throws, the finally clears exactly that marker so a concurrent identical retry
    // is released instead of blocking until the marker's TTL expires.
    String                       idempotencyKey         = null;
    IdempotencyCache.Reservation idempotencyReservation = null;

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
      JSONArray payloadAsArray = null;
      String payloadAsString = null;
      if (mustExecuteOnWorkerThread()) {
        payloadAsString = parseRequestPayload(exchange);
        // The body can only be read once from the exchange, so keep the raw text available to handlers whose
        // payload is neither a JSON object nor a JSON array (e.g. a line-protocol or CSV body).
        if (payloadAsString != null)
          exchange.putAttachment(RAW_PAYLOAD, payloadAsString);
        if (requiresJsonPayload() && payloadAsString != null && !payloadAsString.isBlank()) {
          final String trimmedPayload = payloadAsString.trim();
          try {
            // The body of a JSON request is legitimately either an object or a top-level array (issue #5415).
            // The kind is decided from the first character so the body is parsed exactly once: the previous
            // code always attempted a JSONObject parse, paying for a thrown exception on every array body and
            // leaving the array unreachable to the handler.
            if (trimmedPayload.charAt(0) == '[') {
              payloadAsArray = new JSONArray(trimmedPayload);
              exchange.putAttachment(ARRAY_PAYLOAD, payloadAsArray);
            } else
              payload = new JSONObject(trimmedPayload);
          } catch (Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Error parsing request payload: %s", e.getMessage());
          }
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
          idempotencyReservation = reservation;
      }

      final ExecutionResponse response;
      if (payloadAsArray != null && !acceptsArrayPayload())
        // A top-level JSON array reached a route that only understands a JSON object: answer with an explicit
        // client error instead of running the handler with a null payload, which surfaced as a misleading
        // "field is null" message (or worse as a silent no-op) with no hint that the body shape was wrong.
        response = new ExecutionResponse(400, error2json("The request payload must be a JSON object",
                "This endpoint does not accept a top-level JSON array", null, null, null));
      else
        response = execute(exchange, user, payload);

      if (response != null) {
        response.send(exchange);
        if (idempotencyReservation != null) {
          // Do not cache a response that established a client session (e.g. /begin): replaying it would
          // return the body without the arcadedb-session-id header, orphaning the real session.
          if (exchange.getResponseHeaders().contains(SESSION_ID_HEADER))
            httpServer.getIdempotencyCache().abort(idempotencyKey, idempotencyReservation);
          else
            httpServer.getIdempotencyCache().complete(idempotencyKey, idempotencyReservation, response.getCode(),
                response.getResponse(), response.getBinary(), user != null ? user.getName() : null);
          idempotencyReservation = null;
        }
      } else if (idempotencyReservation != null) {
        httpServer.getIdempotencyCache().abort(idempotencyKey, idempotencyReservation);
        idempotencyReservation = null;
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
      // Resolved once: the arm below needs both the answer and the exception itself, and the chain walk is not worth
      // repeating.
      final ArithmeticErrorException arithmetic = arithmeticError(e);

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
      } else if (arithmetic != null) {
        // An integer overflow or a division by zero is decided by the values the caller supplied, not by anything
        // wrong with the server, and Neo4j classifies the whole category as a client error
        // (Neo.ClientError.Statement.ArithmeticError). Reported as 400 with the arithmetic message rather than the
        // 500 it used to degrade to. See issue #5602.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        arithmetic.getMessage());
        sendErrorResponse(exchange, 400, "Cannot execute command", arithmetic, null);
      } else if (e instanceof CommandParsingException || realException instanceof CommandParsingException) {
        // A parsing/semantic validation error (malformed query, unknown variable, invalid MERGE
        // rebind, unsupported Gremlin syntax such as Groovy closures, ...) is a client error - the query
        // text is invalid, not an internal server fault. Surface as HTTP 400 with the real validation
        // message so API consumers can fix the query, instead of a misleading 500. The check covers a
        // CommandParsingException wrapped as the cause of a CommandExecutionException as well as a
        // directly-thrown CommandParsingException, even when it carries its own cause (e.g. a Gremlin
        // ScriptException, in which case realException is that cause). See issues #5191 and #5201.
        final Throwable reported = e instanceof CommandParsingException ? e : realException;
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        reported.getMessage());
        sendErrorResponse(exchange, 400, "Cannot execute command", reported, null);
      } else {
        // UNEXPECTED INTERNAL ERROR (not a client/validation error handled above): log the FULL stack trace so
        // an internal fault - e.g. a BufferUnderflowException from a truncated/corrupted record read - is
        // diagnosable. Passing the throwable is what makes the logger emit the trace; without it no stack trace
        // is ever printed, at any log level. Use realException (the actual cause) for a useful trace.
        LogManager.instance()
                .log(this, getInternalErrorLogLevel(), "Error on command execution (%s)", realException,
                        getClass().getSimpleName());
        sendErrorResponse(exchange, 500, "Cannot execute command", realException, null);
      }
    } catch (final HttpSessionException e) {
      // A referenced HTTP transaction session id is no longer resolvable (committed/rolled back, expired,
      // owned by another principal, or invalidated). Surface as an explicit 404 client error - never a 500,
      // and never a silent implicit-transaction commit. Must precede the TransactionException arm below since
      // HttpSessionException extends it.
      LogManager.instance()
              .log(this, Level.FINE, "Transaction session error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 404, "Remote transaction session not found or expired", e, null);
    } catch (final TransactionException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();
      final ArithmeticErrorException arithmetic = arithmeticError(e);

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
      } else if (arithmetic != null) {
        // Symmetric with the un-wrapped arithmetic arm above (#5602): the auto-commit wrapper in
        // DatabaseAbstractHandler re-wraps the failure as a TransactionException, and without this branch the
        // client error would degrade back to 500.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        arithmetic.getMessage());
        sendErrorResponse(exchange, 400, "Cannot execute command", arithmetic, null);
      } else if (realException instanceof CommandParsingException) {
        // Symmetric with the un-wrapped CommandParsingException arm above. A Cypher/SQL validation
        // error thrown during execution is wrapped by the auto-commit transaction wrapper in
        // DatabaseAbstractHandler (TransactionException -> CommandParsingException cause). Without this
        // branch the response degraded to 500 "Error on transaction commit", hiding the real
        // client-side validation message. Surface as HTTP 400 instead. See issue #5191.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 400, "Cannot execute command", realException, null);
      } else if (realException instanceof CommandExecutionException) {
        // Symmetric with the un-wrapped CommandExecutionException arm above. A runtime execution error
        // (valid query, but the command failed while running - e.g. a Gremlin `.next()` on an empty
        // traversal raising NoSuchElementException) is wrapped by the auto-commit transaction wrapper in
        // DatabaseAbstractHandler (TransactionException -> CommandExecutionException cause). The failure
        // happened during execute(), not at commit, so the honest label is "Cannot execute command", not
        // the misleading "Error on transaction commit". Keep the HTTP 500 (runtime server-side error,
        // matching Apache TinkerPop's SERVER_ERROR_SCRIPT_EVALUATION mapping). See issue #5219.
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 500, "Cannot execute command", realException, null);
      } else {
        // UNEXPECTED INTERNAL ERROR wrapped by the auto-commit transaction wrapper (the client sees the generic
        // "Error on transaction commit"). Log the FULL stack trace of the real cause: without passing the
        // throwable the logger never prints a trace, at any level, which is why a BufferUnderflowException on a
        // read-only command surfaced with no diagnosable trace even at DEBUG.
        LogManager.instance()
                .log(this, getInternalErrorLogLevel(), "Error on transaction execution (%s)", realException,
                        getClass().getSimpleName());
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
      // UNEXPECTED RAW THROWABLE (typical for non-database handlers): same treatment as the other
      // unexpected-internal-error arms - full stack trace, visible in production mode (issue #5374).
      LogManager.instance()
              .log(this, getInternalErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 500, "Internal error", e, null);
    } finally {
      // Drop any principal this request bound onto the thread's DatabaseContext in
      // checkAuthorizationOnDatabase (GHSA-c23x-pqcj-7hfm). This runs on a pooled worker thread, so a
      // leaked binding would be inherited by the next request served by the same thread. Only the current
      // user is cleared (not the whole context) so a still-open session transaction is never disturbed;
      // a no-op when nothing was bound. DatabaseAbstractHandler manages its own binding/cleanup and never
      // sets this marker.
      final DatabaseInternal boundDb = BOUND_PRINCIPAL_DB.get();
      if (boundDb != null) {
        BOUND_PRINCIPAL_DB.remove();
        try {
          final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(boundDb.getDatabasePath());
          if (context != null)
            context.setCurrentUser(null);
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.WARNING, "Error clearing bound principal from database context", t);
        }
      }

      // If execution threw after this request reserved the idempotency key, clear the PENDING marker so a
      // concurrent identical retry is released immediately instead of blocking until the marker's TTL.
      if (idempotencyReservation != null) {
        try {
          httpServer.getIdempotencyCache().abort(idempotencyKey, idempotencyReservation);
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

      httpRequestTimer(exchange.getRequestMethod().toString(), pathTemplate(exchange),
          Integer.toString(exchange.getStatusCode()), databaseTag(exchange))
          .record(System.nanoTime() - httpStartNanos, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Drops every cached timer. A cached {@link Timer} is bound to the registries backing
   * {@code Metrics.globalRegistry} when it was built, so it must not outlive them: recording into a meter
   * whose backing registry is gone silently discards the sample. Called when the server dismantles the
   * metrics subsystem, so the next server generation rebuilds its timers from scratch.
   */
  public static void invalidateTimerCache() {
    HTTP_REQUEST_TIMERS.clear();
  }

  /**
   * Resolves (and caches) the {@code arcadedb.http.requests} RED timer for the given bounded tag tuple.
   * Building the timer once per distinct {@code method|path|status|db} tuple and reusing it removes the
   * per-request {@code Timer.Builder}/{@code Tags}/{@code Meter.Id} allocation and registry hash lookup
   * on the hot path. The cache stays bounded because every tag is low-cardinality (issue #5025).
   * Package-private for direct unit testing.
   */
  static Timer httpRequestTimer(final String method, final String path, final String status, final String db) {
    return HTTP_REQUEST_TIMERS.computeIfAbsent(method + '|' + path + '|' + status + '|' + db,
        k -> Timer.builder("arcadedb.http.requests")
            .description("HTTP request duration")
            .tag("method", method)
            .tag("path", path)
            .tag("status", status)
            .tag("db", db)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry));
  }

  /**
   * Returns a bounded, low-cardinality path tag for the request: the route template
   * (e.g. {@code /command/{database}}) resolved by the Undertow routing handler, never the raw URI
   * carrying the concrete database name. All {@code /api/v1/*} routes (including fixed ones such as
   * {@code /ready} and {@code /server}) are registered through a {@code RoutingHandler}, which always
   * attaches a {@link PathTemplateMatch}; only prefix/fallback traffic (the Studio {@code /} static
   * handler and unmatched 404 probes) reaches the fallback branch. That path is client-controlled, so it
   * is collapsed to the constant {@link #UNMATCHED_PATH_TAG} to keep the meter cardinality bounded and
   * avoid a heap-growth DoS driven by arbitrary URIs (issue #5025). Package-private for direct unit testing.
   */
  static String pathTemplate(final HttpServerExchange exchange) {
    final PathTemplateMatch match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
    if (match != null)
      return match.getMatchedTemplate();
    return UNMATCHED_PATH_TAG;
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
    final MessageDigest md = SHA_256_DIGEST.get();
    md.reset();
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
   * <p>
   * The coarse {@code canAccessToDatabase} gate above is database-level only. It is NOT enough for a
   * deployment that segments data with per-type/per-group ACLs: the engine's fine-grained per-type layer
   * ({@code LocalDatabase.checkPermissionsOnFile}, {@code LocalBucket} CREATE_RECORD/READ_RECORD) is
   * deliberately a no-op unless the authenticated principal is bound onto this thread's
   * {@link DatabaseContext}. Because these handlers do not extend {@link DatabaseAbstractHandler} (which
   * binds the principal in its own {@code execute}), the engine saw a null current user and silently skipped
   * every per-type check, so a user with DB access but only per-type READ on some types could read/write
   * types it was not entitled to (GHSA-c23x-pqcj-7hfm). Binding here - mirroring {@code DatabaseAbstractHandler}
   * - makes the per-type layer enforce. The binding is dropped in {@link #handleRequest}'s finally so it
   * cannot leak onto the pooled worker thread and be inherited by a later request.
   */
  protected void checkAuthorizationOnDatabase(final ServerSecurityUser user, final String databaseName) {
    if (databaseName == null || databaseName.isEmpty())
      throw new IllegalArgumentException("Database parameter is null");
    if (user != null && !user.canAccessToDatabase(databaseName))
      throw new SecurityException("User '" + user.getName() + "' is not allowed to access database '" + databaseName + "'");

    // Bind the authenticated principal so the engine's per-type ACL layer enforces on this handler.
    // A null user (unauthenticated handler) leaves the engine in its no-user no-op mode, matching prior behavior.
    if (user != null && httpServer.getServer().existsDatabase(databaseName)) {
      final DatabaseInternal database = httpServer.getServer().getDatabase(databaseName, false, false);
      DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
      if (context == null)
        context = DatabaseContext.INSTANCE.init(database);
      context.setCurrentUser(user.getDatabaseUser(database));
      BOUND_PRINCIPAL_DB.set(database);
    }
  }

  /**
   * Companion of {@link #checkAuthorizationOnDatabase} for the server-scoped routes that enumerate the whole
   * database registry instead of naming one database in the path (database listing, server status, cluster
   * status). Those routes stay open to any authenticated user because what they primarily report is
   * server-level - but every per-database entry they emit has to be reduced to what the caller may access,
   * otherwise a tenant scoped to one database learns the names of all the others, and with them whatever the
   * route hangs off a name: transaction ids, bootstrap fingerprints, schema, index metrics.
   * <p>
   * Authorization is delegated to {@link ServerSecurityUser#canAccessToDatabase}, so the wildcard grant is
   * honoured in exactly one place. A {@code null} user means the route runs without authentication and
   * everything is returned, matching the permissive behaviour of {@link #checkAuthorizationOnDatabase}.
   *
   * @return a new set holding the accessible subset, in the iteration order of {@code databaseNames}
   */
  protected Set<String> filterAuthorizedDatabases(final ServerSecurityUser user, final Collection<String> databaseNames) {
    final Set<String> authorized = new LinkedHashSet<>(databaseNames.size());
    for (final String databaseName : databaseNames)
      if (user == null || user.canAccessToDatabase(databaseName))
        authorized.add(databaseName);
    return authorized;
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

  /**
   * Returns true if the handler understands a request body that is a top-level JSON array. Default false: the
   * shared pipeline then answers HTTP 400 for an array body, instead of invoking
   * {@link #execute(HttpServerExchange, ServerSecurityUser, JSONObject)} with a null payload - a JSON array is
   * not a JSONObject, and every route in the server (except the MCP JSON-RPC endpoint) expects an object.
   * <p>
   * A handler that accepts arrays overrides this to true and reads the parsed array with
   * {@link #getPayloadAsArray(HttpServerExchange)}. The array is intentionally NOT delivered through a second
   * {@code execute()} overload: {@link DatabaseAbstractHandler} implements the single
   * {@code execute(exchange, user, payload)} entry point to resolve the database, enforce database-level
   * authorization and open the session/transaction around the handler body, so a parallel array entry point
   * would either duplicate that pipeline or silently bypass it. See issue #5415.
   */
  protected boolean acceptsArrayPayload() {
    return false;
  }

  /**
   * Returns the request body parsed as a top-level JSON array, or {@code null} when the body was not an array
   * (it was a JSON object, was empty, or failed to parse). Only meaningful for a handler that returns true from
   * both {@link #mustExecuteOnWorkerThread()} and {@link #requiresJsonPayload()}, and that accepts an array body
   * ({@link #acceptsArrayPayload()}). The body is parsed once by the shared request pipeline, so calling this
   * costs nothing beyond an exchange attachment lookup. See issue #5415.
   */
  protected JSONArray getPayloadAsArray(final HttpServerExchange exchange) {
    return exchange.getAttachment(ARRAY_PAYLOAD);
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

  /**
   * Log level for UNEXPECTED INTERNAL faults (potential data corruption, engine bugs): SEVERE in development
   * mode, WARNING in production. Unlike user-triggered errors (see {@link #getUserSevereErrorLogLevel()}),
   * these must stay visible with default logging in production - demoting them to FINE is how a
   * BufferUnderflowException on a read-only command went undiagnosable (issue #5374).
   */
  private Level getInternalErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ?
            Level.SEVERE :
            Level.WARNING;
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

  /**
   * The arithmetic error - a 64-bit overflow or a division by zero (issue #5602) - anywhere in the cause chain, or
   * {@code null} when there is none. Returning the exception itself rather than a boolean lets the caller report
   * ArcadeDB's message ({@code long overflow}) instead of whatever the wrapper happened to say.
   * <p>
   * The whole chain is searched rather than only the outermost throwable and its immediate cause, because the
   * exception is wrapped differently depending on how the request arrived: directly, inside the auto-commit
   * {@code TransactionException} wrapper, or with the JDK {@code ArithmeticException} it came from as its own cause.
   */
  private static ArithmeticErrorException arithmeticError(final Throwable error) {
    return CauseChain.find(error, ArithmeticErrorException.class);
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
