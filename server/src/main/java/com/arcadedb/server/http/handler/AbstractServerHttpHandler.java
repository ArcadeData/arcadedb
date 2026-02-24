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

import java.util.Base64;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public abstract class AbstractServerHttpHandler implements HttpHandler {
  private static final String AUTHORIZATION_BASIC  = "Basic";
  private static final String AUTHORIZATION_BEARER = "Bearer";
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

    try {
      LogManager.instance().setContext(httpServer.getServer().getServerName());

      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
      if (isRequireAuthentication() && (authorization == null || authorization.isEmpty())) {
        exchange.setStatusCode(401);
        exchange.getResponseHeaders().put(Headers.WWW_AUTHENTICATE, "Basic");
        sendErrorResponse(exchange, 401, "", null, null);
        return;
      }

      ServerSecurityUser user = null;
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

      JSONObject payload = null;
      if (mustExecuteOnWorkerThread()) {
        final String payloadAsString = parseRequestPayload(exchange);
        if (requiresJsonPayload() && payloadAsString != null && !payloadAsString.isBlank())
          try {
            payload = new JSONObject(payloadAsString.trim());
          } catch (Exception e) {
            LogManager.instance().log(this, Level.WARNING, "Error parsing request payload: %s", e.getMessage());
          }
      }

      final ExecutionResponse response = execute(exchange, user, payload);
      if (response != null)
        response.send(exchange);

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
    } catch (final IllegalArgumentException e) {
      LogManager.instance()
              .log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s): %s", getClass().getSimpleName(),
                      e.getMessage());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, null);
    } catch (final CommandExecutionException | CommandParsingException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      // If the root cause is a SecurityException, return 403 instead of 500
      if (realException instanceof SecurityException) {
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

      if (realException instanceof SecurityException) {
        LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on transaction execution (%s): %s",
                SecurityException.class.getSimpleName(), realException.getMessage());
        sendErrorResponse(exchange, 403, "Security error", realException, null);
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
    if (!exchange.isResponseStarted())
      exchange.setStatusCode(code);

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

    exchange.getResponseSender().send(error2json(errorMessage, detail, e, exceptionArgs, null));
  }
}
