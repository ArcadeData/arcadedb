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
import com.arcadedb.exception.*;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.HAPlugin;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public abstract class AbstractServerHttpHandler implements HttpHandler {
  private static final String AUTHORIZATION_BASIC = "Basic";
  private static final String AUTHORIZATION_BEARER = "Bearer";
  /**
   * HTTP header a client may set to enable server-side idempotency replay of a non-idempotent
   * request (POST). The server caches the successful response under this header and, if the
   * same authenticated principal replays the request with the same value, returns the cached
   * response instead of re-executing the operation. See
   * {@link com.arcadedb.server.http.IdempotencyCache}.
   */
  public static final String IDEMPOTENCY_HEADER = "X-Request-Id";
  private static final io.undertow.util.AttachmentKey<String> RAW_PAYLOAD_KEY = io.undertow.util.AttachmentKey.create(String.class);
  static final io.undertow.util.AttachmentKey<String> BASIC_AUTH_KEY = io.undertow.util.AttachmentKey.create(String.class);
  protected final HttpServer   httpServer;
  private final   LeaderProxy  leaderProxy;

  public AbstractServerHttpHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
    this.leaderProxy = new LeaderProxy(httpServer);
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

    try {
      LogManager.instance().setContext(httpServer.getServer().getServerName());

      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      // Check cluster-internal token auth first (inter-node forwarding in HA)
      final HeaderValues clusterTokenHeader = exchange.getRequestHeaders().get(LeaderProxy.HEADER_CLUSTER_TOKEN);
      final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");

      if (isRequireAuthentication() && clusterTokenHeader == null
          && (authorization == null || authorization.isEmpty())) {
        exchange.setStatusCode(401);
        exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic");
        sendErrorResponse(exchange, 401, "", null, null);
        return;
      }

      ServerSecurityUser user = null;
      if (clusterTokenHeader != null && !clusterTokenHeader.isEmpty()) {
        user = validateClusterForwardedAuth(exchange, clusterTokenHeader.getFirst(),
            exchange.getRequestHeaders().get(LeaderProxy.HEADER_FORWARDED_USER));
        if (user == null)
          return; // error response already sent
      } else if (authorization != null) {
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

            final String[] authPair = authPairClear.split(":", 2);

            if (authPair.length < 2) {
              sendErrorResponse(exchange, 403, "Basic authentication error", null, null);
              return;
            }

            user = authenticate(authPair[0], authPair[1]);
            // Store Basic auth for potential cross-server proxy forwarding in HA
            exchange.putAttachment(BASIC_AUTH_KEY, auth);

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

      JSONObject payload = null;
      String rawPayload = null;
      if (mustExecuteOnWorkerThread()) {
        rawPayload = parseRequestPayload(exchange);
        if (requiresJsonPayload() && rawPayload != null && !rawPayload.isBlank())
          try {
            payload = new JSONObject(rawPayload.trim());
          } catch (Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Error parsing request payload: %s", e.getMessage());
          }
      }

      // Store raw payload for potential proxy forwarding
      exchange.putAttachment(RAW_PAYLOAD_KEY, rawPayload != null ? rawPayload : "");

      // Idempotency replay: if the client sent an X-Request-Id header that matches a cached
      // successful response for the same authenticated principal, replay it without executing
      // the operation. This makes safe retries possible for non-idempotent requests (POST)
      // that may have already been committed server-side while the original response was lost
      // in transit.
      final io.undertow.util.HeaderValues requestIdHeader = exchange.getRequestHeaders().get(IDEMPOTENCY_HEADER);
      final String requestId = requestIdHeader != null && !requestIdHeader.isEmpty() ? requestIdHeader.getFirst() : null;
      if (requestId != null) {
        final com.arcadedb.server.http.IdempotencyCache.CachedEntry cached =
            httpServer.getIdempotencyCache().get(requestId);
        if (cached != null && java.util.Objects.equals(cached.principal, user != null ? user.getName() : null)) {
          exchange.setStatusCode(cached.statusCode);
          if (cached.binary != null) {
            exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, cached.binary.length);
            exchange.getResponseSender().send(java.nio.ByteBuffer.wrap(cached.binary));
          } else
            exchange.getResponseSender().send(cached.body);
          return;
        }
      }

      final ExecutionResponse response = execute(exchange, user, payload);
      if (response != null) {
        response.send(exchange);
        if (requestId != null)
          httpServer.getIdempotencyCache().putSuccess(requestId, response.getCode(), response.getResponse(),
              response.getBinary(), user != null ? user.getName() : null);
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
      // Forward the request to the leader via HTTP proxy
      final String leaderAddr = e.getLeaderAddress();
      if (leaderAddr == null || leaderAddr.isEmpty()) {
        // Leader unknown (election in progress) - return 503 so client retries
        sendErrorResponse(exchange, 503, "Leader election in progress, retry later", e, null);
        return;
      }
      try {
        proxyToLeader(exchange, leaderAddr);
        return;
      } catch (final Exception proxyEx) {
        LogManager.instance().log(this, Level.WARNING, "Failed to proxy request to leader %s: %s", leaderAddr,
            proxyEx.getMessage());
      }
      sendErrorResponse(exchange, 503, "Leader proxy failed, retry later", e, leaderAddr);
    } catch (final NeedRetryException e) {
      LogManager.instance()
              .log(this, Level.FINE, "Error on command execution (%s): %s", getClass().getSimpleName(), e.getMessage());
      sendErrorResponse(exchange, 503, "Cannot execute command", e, null);
    } catch (final DuplicatedKeyException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 503, "Found duplicate key in index", e,
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

      if (realException instanceof ServerIsNotTheLeaderException notLeader) {
        final String leaderAddr = notLeader.getLeaderAddress();
        if (leaderAddr != null && !leaderAddr.isEmpty()) {
          try {
            proxyToLeader(exchange, leaderAddr);
            return;
          } catch (final Exception proxyEx) {
            LogManager.instance().log(this, Level.WARNING, "Failed to proxy request to leader: %s", proxyEx.getMessage());
          }
        }
        sendErrorResponse(exchange, 400, "Cannot execute command", realException, leaderAddr);
      } else if (realException instanceof SecurityException) {
        LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on transaction execution (%s): %s",
                SecurityException.class.getSimpleName(), realException.getMessage());
        sendErrorResponse(exchange, 403, "Security error", realException, null);
      } else if (realException instanceof QueryNotIdempotentException) {
        LogManager.instance()
                .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                        realException.getMessage());
        sendErrorResponse(exchange, 400, "Query is not idempotent", realException, null);
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
      LogManager.instance().setContext(null);
    }
  }

  /**
   * Proxies the current HTTP request to the leader server. Used when a write operation
   * hits a non-leader node in the Ratis HA cluster.
   */
  private void proxyToLeader(final HttpServerExchange exchange, final String leaderAddr) throws Exception {
    try {
      leaderProxy.proxy(exchange, leaderAddr, exchange.getAttachment(RAW_PAYLOAD_KEY));
    } catch (final LeaderProxy.ProxyAuthException e) {
      sendErrorResponse(exchange, e.getStatusCode(), e.getMessage(), null, null);
    }
  }

  /**
   * Validates cluster-internal forwarded auth using a shared token.
   * Returns the user, or null if validation failed (error response already sent).
   */
  private ServerSecurityUser validateClusterForwardedAuth(final HttpServerExchange exchange,
      final String providedToken, final HeaderValues forwardedUserValues) {
    final var haPlugin = httpServer.getServer().getHA();
    final String expectedToken = haPlugin != null ? haPlugin.getClusterToken() : null;

    if (providedToken == null || expectedToken == null || expectedToken.isEmpty()
        || !constantTimeTokenEquals(expectedToken, providedToken)) {
      sendErrorResponse(exchange, 401, "Invalid cluster token", null, null);
      return null;
    }
    final String forwardedUser = forwardedUserValues != null && !forwardedUserValues.isEmpty()
        ? forwardedUserValues.getFirst() : null;
    if (forwardedUser == null || forwardedUser.isEmpty()) {
      sendErrorResponse(exchange, 401, "Missing forwarded user", null, null);
      return null;
    }
    final ServerSecurityUser user = httpServer.getServer().getSecurity().getUser(forwardedUser);
    if (user == null) {
      sendErrorResponse(exchange, 401, "Invalid forwarded authentication", null, null);
      return null;
    }
    return user;
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
   * Ensures only the root user can execute server administration commands.
   * API token-authenticated users have synthetic names like "apitoken:&lt;name&gt;" and will
   * always fail this check — this is intentional, as token management requires root credentials.
   */
  protected void checkRootUser(ServerSecurityUser user) {
    if (!"root".equals(user.getName()))
      throw new ServerSecurityException("Only root user is authorized to execute server commands");
  }

  /**
   * Throws {@link ServerIsNotTheLeaderException} if HA is active and this node is not the leader.
   * The exception is caught by {@link #handleRequest} which proxies the request to the leader.
   */
  protected void checkServerIsLeaderIfInHA() {
    final HAPlugin ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      throw new ServerIsNotTheLeaderException("This operation can only be executed on the leader server", ha.getLeaderHTTPAddress());
  }

  protected String decode(final String command) {
    return command.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&#039;", "'");
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

  private void sendErrorResponse(final HttpServerExchange exchange, final int code, final String errorMessage, final Throwable e,
                                 final String exceptionArgs) {
    // Reclassify 500-level failures that fire during a snapshot install window as 503 with
    // Retry-After: the database was transiently unavailable while being replaced on disk,
    // and retries are safe (idempotent by construction for GETs; coordinated with the
    // IdempotencyCache for POST/PUT/DELETE). Done here so every catch branch that routes
    // through sendErrorResponse benefits without having to repeat the check.
    int effectiveCode = code;
    String effectiveMessage = errorMessage;
    if (code == 500 && httpServer.getServer().isSnapshotInstallInProgress()) {
      effectiveCode = 503;
      effectiveMessage = "Database temporarily unavailable (snapshot install in progress), retry the request";
      exchange.getResponseHeaders().put(io.undertow.util.HttpString.tryFromString("Retry-After"), "5");
    }

    if (!exchange.isResponseStarted())
      exchange.setStatusCode(effectiveCode);

    String detail = "";
    if (e != null) {
      final StringBuilder buffer = new StringBuilder();
      buffer.append(e.getMessage() != null ? e.getMessage() : e.toString());

      Throwable current = e.getCause();
      while (current != null && current != current.getCause() && current != e) {
        buffer.append(" -> ");
        buffer.append(current.getMessage() != null ? current.getMessage() : current.getClass().getSimpleName());
        current = current.getCause();
      }
      detail = buffer.toString();
    }

    exchange.getResponseSender().send(error2json(effectiveMessage, detail, e, exceptionArgs, null));
  }

  private static boolean constantTimeTokenEquals(final String expected, final String provided) {
    try {
      final MessageDigest sha = MessageDigest.getInstance("SHA-256");
      final byte[] a = sha.digest(expected.getBytes(StandardCharsets.UTF_8));
      sha.reset();
      final byte[] b = sha.digest(provided.getBytes(StandardCharsets.UTF_8));
      return MessageDigest.isEqual(a, b);
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }
}
