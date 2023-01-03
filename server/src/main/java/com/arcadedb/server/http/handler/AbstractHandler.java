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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.security.SecurityUser;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import org.json.JSONObject;

import java.util.*;
import java.util.logging.*;

public abstract class AbstractHandler implements HttpHandler {
  private static final String     AUTHORIZATION_BASIC = "Basic";
  protected final      HttpServer httpServer;

  public AbstractHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract void execute(HttpServerExchange exchange, ServerSecurityUser user) throws Exception;

  protected String parseRequestPayload(final HttpServerExchange e) {
    final StringBuilder result = new StringBuilder();
    //e.startBlocking();
    e.getRequestReceiver().receiveFullBytes(
        // OK
        (exchange, data) -> result.append(new String(data, DatabaseFactory.getDefaultCharset())),
        // ERROR
        (exchange, err) -> {
          LogManager.instance().log(this, Level.SEVERE, "getFullBytes completed with an error: %s", err, err.getMessage());
          exchange.setStatusCode(500);
          exchange.getResponseSender().send("Invalid Request");
        });
    return result.toString();
  }

  @Override
  public void handleRequest(final HttpServerExchange exchange) {
    LogManager.instance().setContext(httpServer.getServer().getServerName());

    try {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
      if (isRequireAuthentication() && (authorization == null || authorization.isEmpty())) {
        exchange.setStatusCode(403);
        sendErrorResponse(exchange, 403, "No authentication was provided", null, null);
        return;
      }

      ServerSecurityUser user = null;
      if (authorization != null) {
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
      }

      final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.request");
      try {
        execute(exchange, user);

      } finally {
        timer.stop();
      }

    } catch (final ServerSecurityException e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Security error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 403, "Security error", e, null);
    } catch (final ServerIsNotTheLeaderException e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 400, "Cannot execute command", e, e.getLeaderAddress());
    } catch (final NeedRetryException e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 503, "Cannot execute command", e, null);
    } catch (final DuplicatedKeyException e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 503, "Found duplicate key in index", e, e.getIndexName() + "|" + e.getKeys() + "|" + e.getCurrentIndexedRID());
    } catch (final RecordNotFoundException e) {
      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 404, "Record not found", e, null);
    } catch (final CommandExecutionException | CommandSQLParsingException | QueryParsingException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      LogManager.instance().log(this, getErrorLogLevel(), "Error on command execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 500, "Cannot execute command", realException, null);
    } catch (final TransactionException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      LogManager.instance().log(this, getErrorLogLevel(), "Error on transaction execution (%s)", e, getClass().getSimpleName());
      sendErrorResponse(exchange, 500, "Error on transaction commit", realException, null);
    } catch (final Exception e) {
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

  protected JSONObject createResult(final SecurityUser user) {
    return new JSONObject().put("user", user.getName()).put("version", Constants.getVersion());
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

  protected String encodeError(final String message) {
    return message.replaceAll("\\\\", " ").replaceAll("\n", " ");//.replaceAll("\"", "'");
  }

  private Level getErrorLogLevel() {
    return "development".equals(httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_MODE)) ? Level.INFO : Level.FINE;
  }

  private void sendErrorResponse(final HttpServerExchange exchange, final int code, final String errorMessage, final Throwable e, final String exceptionArgs) {
    if (!exchange.isResponseStarted())
      exchange.setStatusCode(code);
    exchange.getResponseSender().send(error2json(errorMessage, e != null ? e.getMessage() : "", e, exceptionArgs, null));
  }
}
