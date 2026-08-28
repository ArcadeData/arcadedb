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
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSessionException;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.ResultSetTooLargeException;
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
  // Constant db tag used for any request whose {database} path parameter does not name a database this
  // server actually has. The parameter is a free path segment, so an unauthenticated caller can invent
  // one per request; echoing it verbatim registered a permanent percentile-histogram Timer per invented
  // name, the same unbounded heap growth #5025 fixed for the path tag (issue #6805).
  private static final String     UNKNOWN_DB_TAG     = "unknown";
  // Constant db tag substituted once the timer cache is full, so a database name churn (create/drop in a
  // loop) cannot grow the cache without bound either. The overflow tuple space is itself finite: method,
  // path template and status are all small enumerations.
  private static final String     OVERFLOW_DB_TAG    = "other";
  // Maximum number of distinct "db" tag values allowed on arcadedb.http.requests. ArcadeDBServer.startMetrics()
  // installs the matching MeterFilter from this constant, so the registry-side bound and the cache-side bound
  // below are one number rather than two independently-chosen ones. Far above any realistic per-server
  // database count.
  public static final  int        MAX_DB_TAG_VALUES  = 1_000;
  // Ceiling on the number of cached tuples: a MeterFilter can deny the meter but cannot stop computeIfAbsent
  // from retaining the key, so the cache needs a bound of its own. Sized as a multiple of MAX_DB_TAG_VALUES
  // so that a deployment with the maximum admissible number of databases still gets per-database RED
  // visibility across ten method/path/status combinations each before anything collapses onto
  // OVERFLOW_DB_TAG - the collapse is a backstop against unbounded growth, not a routine operating mode. At
  // roughly a hundred bytes per entry the whole cache is about a megabyte at the ceiling. Note this is a soft
  // ceiling: the size test and the computeIfAbsent are not one atomic step, so concurrent misses right at the
  // boundary can overshoot it by a bounded handful of entries before the collapse engages.
  private static final int        MAX_HTTP_REQUEST_TIMERS = MAX_DB_TAG_VALUES * 10;
  // Cache of resolved arcadedb.http.requests timers keyed by the bounded tag tuple
  // (method|path|status|db). Avoids rebuilding the Timer.Builder/Tags/Meter.Id and doing a registry
  // hash lookup on every request. The key space is bounded because every tag is low-cardinality: the
  // path is collapsed to a route template or the constant "unmatched", method and status are small
  // enumerations, and db is either an existing database name or the constant "unknown".
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
   * <p>
   * Bounded by the hard ceiling, so a default configured above it (or left unlimited while the ceiling is not)
   * is lowered rather than refused. Without that clamp a deployment whose two settings disagree would answer a
   * caller that stated <i>nothing</i> with a 413: the cap it never asked for would exceed the ceiling, and the
   * refusal is meant for a caller that asked to go past it, never for one served by the default. What a
   * no-limit caller gets instead is the ordinary truncation it has always got, at the lower of the two values,
   * reported as usual with {@code truncated} (issue #5719).
   */
  protected int getDefaultRowLimit() {
    return applyMaxResultRows(
        httpServer.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT),
        getMaxResultRows());
  }

  /**
   * Hard ceiling on the number of rows a single response may carry, whatever the caller asked for. A
   * non-positive value means unlimited (issue #5719).
   */
  protected int getMaxResultRows() {
    return httpServer.getServer().getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS);
  }

  /**
   * Lowers a row cap to the hard ceiling, so no caller can widen a single response past it: the value stated by
   * the caller (or by the query, or by the configured default) wins while it stays at or below the ceiling, and
   * an explicitly unlimited cap - {@code <= 0} - is bounded by it like any other. Returns the cap unchanged when
   * the ceiling is disabled, which is exactly what makes {@code applied != stated} the test for "the ceiling is
   * the one deciding here" that the callers use to answer 413 instead of reporting an ordinary truncation.
   */
  protected static int applyMaxResultRows(final int limit, final int maxResultRows) {
    if (maxResultRows <= 0)
      return limit;
    return limit <= 0 || limit > maxResultRows ? maxResultRows : limit;
  }

  /**
   * Refusal of a response the hard ceiling would not let through, worded identically on every endpoint that
   * enforces it. Names the setting so an operator reading only the error body knows which knob decided.
   */
  protected static ResultSetTooLargeException resultSetTooLarge(final int maxResultRows) {
    return new ResultSetTooLargeException(
        "The result exceeds the maximum of " + maxResultRows + " rows a single HTTP response may carry ("
            + GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey()
            + "): narrow the query, page it with a smaller 'limit', or raise the setting", maxResultRows);
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

        // A peer already redirected this request to the leader, so this node must execute it or refuse it -
        // redirecting it again sends it round the cycle a wrong leader address creates, and nothing else in
        // the exchange says the request has been here before (issue #6191). Published onto a thread-local
        // because one of the redirect decisions is taken deep in the engine, where the exchange is out of
        // reach. Read only here, inside the cluster-token branch: the marker is a statement one node makes to
        // another, and honoring it from an ordinary client request would let any caller turn its own
        // transparent forward into a refusal by copying the header through.
        if (exchange.getRequestHeaders().contains(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER))
          LeaderForwardContext.markAlreadyForwarded();
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

    } catch (final Throwable e) {
      sendMappedErrorResponse(exchange, e);
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
      LeaderForwardContext.clear();
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
   * Turns any exception raised while serving a request into an HTTP status, a client-facing message and the
   * structured {@code exceptionArgs} of the wire contract. The <b>single</b> place that decision is taken.
   * <p>
   * It used to be three: a chain of typed {@code catch} arms for an exception that reached the boundary as
   * itself, plus one hand-written {@code instanceof} mirror of it inside the {@code CommandExecutionException}
   * arm and another inside the {@code TransactionException} arm, for the same exception wrapped. The mirrors
   * were maintained by hand and were never complete, and every gap needed its own issue to be noticed - #4350
   * (duplicated key), #5064/#5075 (committed remotely), #5935 (malformed JSON), #5191 (parsing), #5602
   * (arithmetic), #6191 (not the leader) each added one arm to one or two of the three. #6201 closed the
   * remaining pair the same way it closed the wrapping that produced them: {@code NeedRetryException} (503) and
   * {@code RecordNotFoundException} (404) existed only un-wrapped, so a retryable conflict raised inside the
   * auto-commit wrapper was answered 500 "Error on transaction commit" - opaque to a client whose retry policy
   * keys on 503, and the exact opposite of the contract {@code PostBatchHandler} documents.
   * <p>
   * The classification below is therefore written once and applied to both shapes. Order is significant only
   * where one type extends another, which is called out at each such arm.
   *
   * @param e the exception that reached the request boundary
   */
  private void sendMappedErrorResponse(final HttpServerExchange exchange, final Throwable e) {
    // Exactly one level of unwrapping, and only for the generic wrappers - which is what the three former
    // chains did, since only their wrapper arms ever consulted getCause(). Unwrapping unconditionally would
    // change what a mapping keyed on the OUTER type answers: an IllegalArgumentException that happens to carry
    // a DuplicatedKeyException cause is a 400, not a 409.
    //
    // One level is sufficient because at most one is produced: DatabaseAbstractHandler.executeInTransaction
    // rethrows a RuntimeException unchanged rather than re-wrapping it (#6201), so a failure arrives here either
    // as itself or under a single planner/commit wrapper. That is a coupling, not a coincidence - a change that
    // reintroduces double-wrapping has to deepen the walk here too, or the mapping it buries silently degrades to
    // the generic 500 this method exists to stop producing.
    final Throwable cause = e.getCause() != null && isGenericWrapper(e) ? e.getCause() : e;

    final Throwable security = isSecurityFailure(e) ? e : isSecurityFailure(cause) ? cause : null;
    if (security != null) {
      // PASS SecurityException TO THE CLIENT
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on command execution (%s): %s",
              SecurityException.class.getSimpleName(), security.getMessage());
      sendErrorResponse(exchange, 403, "Security error", security, null);
      return;
    }

    // 413 Content Too Large: the response the caller asked for exceeds the hard row ceiling
    // (arcadedb.server.httpQueryMaxResultRows). Independent of every other arm - nothing extends it and it
    // extends nothing but ServerException - so its position here is only for readability. A 4xx and not a 5xx:
    // the request is answerable, just not as written, and the caller fixes it by narrowing or paging the query
    // (issue #5719).
    final ResultSetTooLargeException tooLarge = firstOf(e, cause, ResultSetTooLargeException.class);
    if (tooLarge != null) {
      logUserError(tooLarge);
      // The setting goes in the label and the ceiling in exceptionArgs, both of which survive production mode:
      // 'detail' - where the full sentence lives - is concealed there, and a caller that cannot see WHICH knob
      // refused it, or WHAT number to stay under, has been told nothing it can act on.
      sendErrorResponse(exchange, 413,
          "Result set too large for a single response (" + GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey()
              + ")", tooLarge, String.valueOf(tooLarge.getMaxResultRows()));
      return;
    }

    // Before the NeedRetryException arm below, which it extends: the refusal names the leader the caller has to
    // dial instead of merely saying "try again", and the forwarding peer rebuilds the typed exception from it
    // (issue #6191).
    final ServerIsNotTheLeaderException notTheLeader = firstOf(e, cause, ServerIsNotTheLeaderException.class);
    if (notTheLeader != null) {
      logUserError(notTheLeader);
      sendErrorResponse(exchange, 400, "Cannot execute command", notTheLeader, notTheLeader.getLeaderAddress());
      return;
    }

    // Before the TransactionException arm below, which it extends. A referenced HTTP transaction session id is
    // no longer resolvable (committed/rolled back, expired, owned by another principal, or invalidated):
    // an explicit 404 client error - never a 500, and never a silent implicit-transaction commit.
    final HttpSessionException sessionGone = firstOf(e, cause, HttpSessionException.class);
    if (sessionGone != null) {
      LogManager.instance()
              .log(this, Level.FINE, "Transaction session error on command execution (%s): %s",
                      getClass().getSimpleName(), sessionGone.getMessage());
      sendErrorResponse(exchange, 404, "Remote transaction session not found or expired", sessionGone, null);
      return;
    }

    // Before the TransactionException arm below, which it extends. 409 Conflict, NOT 5xx (#5064/#5075): the
    // transaction IS durably committed cluster-wide - only the local apply failed. A 5xx would invite HTTP
    // clients and load balancers to RETRY, applying the changes a second time (duplicate inserts) - the exact
    // hazard the distinct exception type exists to prevent. Same rationale as the DuplicatedKeyException 409.
    final TransactionCommittedRemotelyException committedRemotely = firstOf(e, cause,
            TransactionCommittedRemotelyException.class);
    if (committedRemotely != null) {
      logUserError(committedRemotely);
      sendErrorResponse(exchange, 409, "Transaction committed cluster-wide but the local apply failed - do not retry",
              committedRemotely, null);
      return;
    }

    // 409 Conflict (RFC 9110 15.5.10): a unique-constraint violation is a client data conflict, not a transient
    // server-availability problem. 503 told clients/load balancers the request was retry-worthy, amplifying the
    // bad write. See issue #4350.
    final DuplicatedKeyException dup = firstOf(e, cause, DuplicatedKeyException.class);
    if (dup != null) {
      logUserError(dup);
      sendErrorResponse(exchange, 409, "Found duplicate key in index", dup,
              dup.getIndexName() + "|" + dup.getKeys() + "|" + dup.getCurrentIndexedRID());
      return;
    }

    // 503: the conflict is transient and the same request can succeed as issued. Reached from inside the
    // auto-commit wrapper as well since #6201, which is where the engine raises most of them.
    final NeedRetryException retryable = firstOf(e, cause, NeedRetryException.class);
    if (retryable != null) {
      sendRetryableResponse(exchange, retryable);
      return;
    }

    // 503: an HA snapshot-reinstall resync (issue #5977 pattern) closed and reinstalled the database out from
    // under a handle a request had already resolved (or resolved while one was in flight). The condition is
    // transient by construction - a handle resolved a moment later sees the reinstalled database - so it must be
    // reported the same retryable way a Raft conflict already is, instead of the opaque 500 it fell through to
    // before, which RemoteHttpComponent's NeedRetryException-driven auto-retry never saw (issue #6770).
    //
    // Scope: this maps EVERY DatabaseIsClosedException to 503, not only the resync race above. A concurrent
    // DROP/CLOSE DATABASE admin action throws the same exception type on a still in-flight request, and that
    // close is permanent rather than transient - a retry costs one wasted round trip before falling through to
    // the arm below (issue #6778, #6770 follow-up). Scoping THIS 503 itself to the resync case needs a
    // resync-vs-permanent-close signal that does not exist yet, so that half of #6778 is not attempted here.
    final DatabaseIsClosedException databaseClosed = firstOf(e, cause, DatabaseIsClosedException.class);
    if (databaseClosed != null) {
      sendRetryableResponse(exchange, databaseClosed);
      return;
    }

    // 404: the wasted retry the comment above describes re-resolves the database with allowLoad=false
    // (DatabaseAbstractHandler.execute), which raises this narrower type - not the generic
    // DatabaseOperationException - once the registry entry is gone. Answered as an accurate "not there"
    // instead of falling through to the generic 500 below (issue #6778).
    final DatabaseNotAvailableException notAvailable = firstOf(e, cause, DatabaseNotAvailableException.class);
    if (notAvailable != null) {
      logUserError(notAvailable);
      sendErrorResponse(exchange, 404, "Database not found", notAvailable, null);
      return;
    }

    final RecordNotFoundException notFound = firstOf(e, cause, RecordNotFoundException.class);
    if (notFound != null) {
      logUserError(notFound);
      sendErrorResponse(exchange, 404, "Record not found", notFound, null);
      return;
    }

    final QueryNotIdempotentException notIdempotent = firstOf(e, cause, QueryNotIdempotentException.class);
    if (notIdempotent != null) {
      logUserError(notIdempotent);
      sendErrorResponse(exchange, 400, "Query is not idempotent", notIdempotent, null);
      return;
    }

    // The whole chain is searched here rather than only the two throwables above, because the arithmetic error
    // is wrapped differently depending on how the request arrived: directly, inside the auto-commit wrapper, or
    // with the JDK ArithmeticException it came from as its own cause. An integer overflow or a division by zero
    // is decided by the values the caller supplied, not by anything wrong with the server, and Neo4j classifies
    // the whole category as a client error (Neo.ClientError.Statement.ArithmeticError). See issue #5602.
    final ArithmeticErrorException arithmetic = arithmeticError(e);
    if (arithmetic != null) {
      logUserError(arithmetic);
      sendErrorResponse(exchange, 400, "Cannot execute command", arithmetic, null);
      return;
    }

    // Ahead of the JSON arm below, which is the precedence the old CommandExecutionException|CommandParsingException
    // chain had: a statement whose text failed to parse is reported as the parsing failure it is, even when the
    // parser's own cause happens to be a JSONException. Both answer 400, so the order decides the message and the
    // wire contract's exception field, not the status - and "the query text is invalid" is the actionable half,
    // while how the parser tripped over it is an implementation detail.
    //
    // A parsing/semantic validation error (malformed query, unknown variable, invalid MERGE rebind, unsupported
    // Gremlin syntax such as Groovy closures, ...) is a client error - the query text is invalid, not an
    // internal server fault. Surfaced with the real validation message so API consumers can fix the query,
    // instead of a misleading 500. See issues #5191 and #5201.
    final CommandParsingException parsing = firstOf(e, cause, CommandParsingException.class);
    if (parsing != null) {
      logUserError(parsing);
      sendErrorResponse(exchange, 400, "Cannot execute command", parsing, null);
      return;
    }

    // The request payload is missing a property, carries a null where a value is required, or holds the wrong
    // type for it: a malformed request, not a server fault. Without this it degraded to 500 (issue #5935).
    final JSONException invalidJson = firstOf(e, cause, JSONException.class);
    if (invalidJson != null) {
      logUserError(invalidJson);
      sendErrorResponse(exchange, 400, "Invalid JSON payload", invalidJson, null);
      return;
    }

    // Bad client input (malformed parameter, unparseable marker, ...).
    final IllegalArgumentException badArgument = firstOf(e, cause, IllegalArgumentException.class);
    if (badArgument != null) {
      logUserError(badArgument);
      sendErrorResponse(exchange, 400, "Cannot execute command", badArgument, null);
      return;
    }

    // From here on nothing identified the failure as a client error, so the two generic wrappers answer for
    // whatever they carry. UNEXPECTED INTERNAL ERROR: log the FULL stack trace - passing the throwable is what
    // makes the logger emit one, and without it a BufferUnderflowException from a truncated record read was not
    // diagnosable at any log level.
    final CommandExecutionException commandFailure = firstOf(e, cause, CommandExecutionException.class);
    if (commandFailure != null) {
      // The failure happened during execute(), not at commit, so the honest label is "Cannot execute command".
      // The 500 stands: a runtime server-side error, matching Apache TinkerPop's SERVER_ERROR_SCRIPT_EVALUATION
      // mapping (issue #5219).
      //
      // Reported as itself and not as its cause, unlike the TransactionException arm below: this type is raised
      // BY the engine to say what failed, so its message is the diagnosis - "Backup failed for database 'x' to
      // directory 'y'" over a bare reflection failure - and dropping it would leave the client with the plumbing
      // and none of the context. Nothing is lost either way, since the error body's detail field renders the
      // whole cause chain and the logger prints it as "Caused by".
      LogManager.instance()
              .log(this, getInternalErrorLogLevel(), "Error on command execution (%s)", commandFailure,
                      getClass().getSimpleName());
      sendErrorResponse(exchange, 500, "Cannot execute command", commandFailure, null);
      return;
    }

    if (firstOf(e, cause, TransactionException.class) != null) {
      // Reported as its CAUSE, unlike the arm above: this wrapper is put on by the plumbing rather than raised by
      // it, so its message ("Error on executing command") says nothing the label does not, while the cause is the
      // real fault - and the wire contract's exception field is what a customer report is diagnosed from.
      LogManager.instance()
              .log(this, getInternalErrorLogLevel(), "Error on transaction execution (%s)", cause,
                      getClass().getSimpleName());
      sendErrorResponse(exchange, 500, "Error on transaction commit", cause, null);
      return;
    }

    // Last resort, and the only place the cause chain is walked to any depth for a security failure: one buried
    // under an unrecognised wrapper must still be answered as one. Deliberately below every arm above, so a
    // recognised mapping keeps deciding - which is where this walk already sat, in the catch-Throwable arm. It
    // asks isSecurityFailure like the shallow probe does: the walk used to test only SecurityException, so a
    // ServerSecurityException buried two levels deep came out as a generic 500 while the same exception one
    // level up came out as 403.
    for (Throwable deep = e; deep != null; deep = deep.getCause())
      if (isSecurityFailure(deep)) {
        LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on command execution (%s): %s",
                SecurityException.class.getSimpleName(), deep.getMessage());
        sendErrorResponse(exchange, 403, "Security error", deep, null);
        return;
      }

    // UNEXPECTED RAW THROWABLE (typical for non-database handlers): same treatment as the other
    // unexpected-internal-error arms - full stack trace, visible in production mode (issue #5374).
    LogManager.instance()
            .log(this, getInternalErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
    sendErrorResponse(exchange, 500, "Internal error", e, null);
  }

  /**
   * Whether {@code e} is a security refusal the client must be told about as a 403. Two unrelated types express
   * one thing: {@link ServerSecurityException} extends {@code ServerException}, NOT {@link SecurityException}, so
   * neither {@code instanceof} implies the other and asking for one of them is always a half-answer. Written once
   * so the shallow probe and the last-resort cause walk cannot disagree about what counts.
   */
  private static boolean isSecurityFailure(final Throwable e) {
    return e instanceof SecurityException || e instanceof ServerSecurityException;
  }

  /**
   * The exceptions {@link #sendMappedErrorResponse} looks through rather than at: they carry no classification
   * of their own beyond "something failed while executing/committing", and the failure that matters is their
   * cause. Every other type is answered as itself, cause or no cause.
   */
  private static boolean isGenericWrapper(final Throwable e) {
    return e instanceof TransactionException || e instanceof CommandExecutionException
            || e instanceof CommandParsingException;
  }

  /**
   * The first of {@code e} and its unwrapped {@code cause} that is an instance of {@code type}, or {@code null}
   * when neither is. {@code e} is tested first so a mapping reports the outermost throwable of its own type,
   * which is the one carrying the message and the structured arguments the client is given.
   */
  private static <T> T firstOf(final Throwable e, final Throwable cause, final Class<T> type) {
    if (type.isInstance(e))
      return type.cast(e);
    if (type.isInstance(cause))
      return type.cast(cause);
    return null;
  }

  /**
   * Sends a 503 for a failure the caller can retry as-is - a Raft conflict or a resync race, both transient by
   * construction. Shared by the {@link NeedRetryException} and {@link DatabaseIsClosedException} arms of
   * {@link #sendMappedErrorResponse}.
   */
  private void sendRetryableResponse(final HttpServerExchange exchange, final Throwable retryable) {
    LogManager.instance()
            .log(this, Level.FINE, "Error on command execution (%s): %s", getClass().getSimpleName(),
                    retryable.getMessage());
    sendErrorResponse(exchange, 503, "Cannot execute command", retryable, null);
  }

  /**
   * Logs a failure the caller caused. Demoted under flood protection in production mode
   * ({@link #getUserSevereErrorLogLevel()}) and without a stack trace: the message is the diagnosis, and the
   * client is being told what went wrong anyway.
   */
  private void logUserError(final Throwable e) {
    LogManager.instance()
            .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                    e.getMessage());
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
   * on the hot path. The cache stays bounded because every tag is low-cardinality (issue #5025), and a
   * hard {@link #MAX_HTTP_REQUEST_TIMERS} ceiling backs that up: a registry {@code MeterFilter} can deny
   * the meter but cannot stop {@code computeIfAbsent} from retaining the key, so past the ceiling the
   * {@code db} half of the tuple collapses onto {@link #OVERFLOW_DB_TAG} (issue #6805).
   * Package-private for direct unit testing.
   */
  static Timer httpRequestTimer(final String method, final String path, final String status, final String db) {
    final String key = method + '|' + path + '|' + status + '|' + db;
    final Timer cached = HTTP_REQUEST_TIMERS.get(key);
    if (cached != null)
      return cached;

    // Recurses at most once: the overflow tuple space is finite, so the collapsed key is always cacheable.
    if (HTTP_REQUEST_TIMERS.size() >= MAX_HTTP_REQUEST_TIMERS && !OVERFLOW_DB_TAG.equals(db))
      return httpRequestTimer(method, path, status, OVERFLOW_DB_TAG);

    return HTTP_REQUEST_TIMERS.computeIfAbsent(key,
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
  private String databaseTag(final HttpServerExchange exchange) {
    return databaseTag(exchange, httpServer.getServer());
  }

  /**
   * Bounded {@code db} tag for the RED timer: the raw {@code {database}} path parameter is echoed only when
   * it names a database this server actually has, and collapses to the constant {@link #UNKNOWN_DB_TAG}
   * otherwise. The parameter matches any path segment and the timer is recorded in a {@code finally} that
   * also runs for the early 401, so without the existence test any unauthenticated caller could register one
   * permanent percentile-histogram Timer per invented name - the {@code path}-tag leak of #5025, on the other
   * half of the tuple (issue #6805). Package-private for direct unit testing.
   */
  static String databaseTag(final HttpServerExchange exchange, final ArcadeDBServer server) {
    final PathTemplateMatch match = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
    if (match != null) {
      final String db = match.getParameters().get("database");
      if (db != null)
        return server != null && server.existsDatabase(db) ? db : UNKNOWN_DB_TAG;
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
   * Resolves the {@link HAReplicatedDatabase} backing {@code database}, either directly or through
   * {@link DatabaseInternal#getWrappedDatabaseInstance()}, or {@code null} on a standalone (non-HA)
   * database. Shared by {@link DatabaseAbstractHandler} and {@link PostBatchHandler} so the
   * wrapper-unwrapping rule for HA read-your-writes support (bookmark header, read-consistency context)
   * lives in one place.
   */
  protected static HAReplicatedDatabase resolveHAReplicatedDatabase(final DatabaseInternal database) {
    if (database == null)
      return null;
    if (database instanceof HAReplicatedDatabase haDb)
      return haDb;
    return database.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb ? haDb : null;
  }

  /**
   * Emits the {@code X-ArcadeDB-Commit-Index} response header, the read-your-writes bookmark a
   * {@code READ_YOUR_WRITES} client captures and carries into its next read. A no-op when {@code haDb} is
   * {@code null} (standalone database) or has not applied anything yet. Shared by
   * {@link DatabaseAbstractHandler} (issue #5845), {@link PostBatchHandler} (issue #5862),
   * {@link PostTimeSeriesWriteHandler} and {@link PostPrometheusWriteHandler} (issue #5866) - every write
   * path whose commit happens outside, or beyond, the generic per-request wrapper in {@link #handleRequest}.
   */
  protected static void emitCommitIndexBookmark(final HttpServerExchange exchange, final HAReplicatedDatabase haDb) {
    if (haDb == null)
      return;
    final long lastApplied = haDb.getLastAppliedIndex();
    if (lastApplied >= 0)
      exchange.getResponseHeaders().put(new HttpString("X-ArcadeDB-Commit-Index"), String.valueOf(lastApplied));
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
