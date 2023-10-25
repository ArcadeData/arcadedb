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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.security.SecurityUser;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public abstract class AbstractServerHttpHandler implements HttpHandler {
  private static final String     AUTHORIZATION_BASIC = "Basic";
  protected final      HttpServer httpServer;

  public AbstractServerHttpHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract ExecutionResponse execute(HttpServerExchange exchange, ServerSecurityUser user) throws Exception;

  protected String parseRequestPayload(final HttpServerExchange e) {
    if (!e.isInIoThread() && !e.isBlocking())
      e.startBlocking();

    if (!mustExecuteOnWorkerThread())
      LogManager.instance().log(this, Level.SEVERE, "Error: handler must return true at mustExecuteOnWorkerThread() to read payload from request");

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
        exchange.setStatusCode(403);
        sendErrorResponse(exchange, 403, "No authentication was provided", null, null);
        return;
      }

      ServerSecurityUser user = null;
      if (authorization != null) {
        try {
          final String auth = authorization.getFirst();
          if (!auth.startsWith(AUTHORIZATION_BASIC)) {
            sendErrorResponse(exchange, 403, "Authentication not supported", null, null);
            return;
          }

          final String authPairCypher = auth.substring(AUTHORIZATION_BASIC.length() + 1);

          final String authPairClear = new String(Base64.getDecoder().decode(authPairCypher), DatabaseFactory.getDefaultCharset());

          final String[] authPair = authPairClear.split(":");

          if (authPair.length != 2) {
            sendErrorResponse(exchange, 403, "Basic authentication error", null, null);
            return;
          }

          user = authenticate(authPair[0], authPair[1]);
        } catch (ServerSecurityException e) {
          // PASS THROUGH
          throw e;
        } catch (Exception e) {
          throw new ServerSecurityException("Authentication error");
        }
      }

      final ExecutionResponse response = execute(exchange, user);
      if (response != null)
        response.send(exchange);

    } catch (final ServerSecurityException e) {
      // PASS SecurityException TO THE CLIENT
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Security error on command execution (%s)", e, SecurityException.class.getSimpleName());
      sendErrorResponse(exchange, 403, "Security error", e, null);
    } catch (final ServerIsNotTheLeaderException e) {
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, e.getLeaderAddress());
    } catch (final NeedRetryException e) {
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 503, "Cannot execute command", e, null);
    } catch (final DuplicatedKeyException e) {
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 503, "Found duplicate key in index", e, e.getIndexName() + "|" + e.getKeys() + "|" + e.getCurrentIndexedRID());
    } catch (final RecordNotFoundException e) {
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 404, "Record not found", e, null);
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, null);
    } catch (final CommandExecutionException | CommandParsingException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 500, "Cannot execute command", realException, null);
    } catch (final TransactionException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      LogManager.instance().log(this, getUserSevereErrorLogLevel(), "Error on transaction execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 500, "Error on transaction commit", realException, null);
    } catch (final Throwable e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
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

  protected static void checkRootUser(ServerSecurityUser user) {
    if (!"root".equals(user.getName()))
      throw new ServerSecurityException("Only root user is authorized to execute server commands");
  }

  protected JSONObject createResult(final SecurityUser user, final Database database) {
    final JSONObject json = new JSONObject();
    if (database != null)
      json.setDateFormat(database.getSchema().getDateTimeFormat());
    json.put("user", user.getName()).put("version", Constants.getVersion()).put("serverName", httpServer.getServer().getServerName());
    return json;
  }

  protected String decode(final String command) {
    return command.replace("&amp;", " ").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&#039;", "'");
  }

  protected String error2json(final String error, final String detail, final Throwable exception, final String exceptionArgs, final String help) {
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
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ? Level.SEVERE : Level.FINE;
  }

  private Level getUserSevereErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ? Level.INFO : Level.FINE;
  }

  private void sendErrorResponse(final HttpServerExchange exchange, final int code, final String errorMessage, final Throwable e, final String exceptionArgs) {
    if (!exchange.isResponseStarted())
      exchange.setStatusCode(code);
    exchange.getResponseSender().send(error2json(errorMessage, e != null ? e.getMessage() : "", e, exceptionArgs, null));
  }
}
