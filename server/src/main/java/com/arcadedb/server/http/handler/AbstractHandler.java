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
 */
package com.arcadedb.server.http.handler;

import com.arcadedb.Constants;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Base64;
import java.util.logging.Level;

public abstract class AbstractHandler implements HttpHandler {
  private              boolean    requireAuthentication = true;
  private static final String     AUTHORIZATION_BASIC   = "Basic";
  protected final      HttpServer httpServer;

  public AbstractHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  protected abstract void execute(HttpServerExchange exchange, ServerSecurity.ServerUser user) throws Exception;

  protected String parseRequestPayload(final HttpServerExchange e) throws IOException {
    final StringBuilder result = new StringBuilder();
    //e.startBlocking();
    e.getRequestReceiver().receiveFullBytes(
        // OK
        (exchange, data) -> {
          result.append(new String(data,DatabaseFactory.getDefaultCharset()));
        },
        // ERROR
        (exchange, err) -> {
          LogManager.instance().log(this, Level.SEVERE, "getFullBytes completed with an error: %s", err, err.getMessage());
          exchange.setStatusCode(500);
          exchange.getResponseSender().send("Invalid Request");
        });
    return result.toString();
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) {
    LogManager.instance().setContext(httpServer.getServer().getServerName());

    try {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");

      final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
      if (requireAuthentication && (authorization == null || authorization.isEmpty())) {
        exchange.setStatusCode(403);
        exchange.getResponseSender().send("{ \"error\" : \"No authentication was provided\"}");
        return;
      }

      ServerSecurity.ServerUser user = null;
      if (authorization != null) {
        final String auth = authorization.getFirst();
        if (!auth.startsWith(AUTHORIZATION_BASIC)) {
          exchange.setStatusCode(403);
          exchange.getResponseSender().send("{ \"error\" : \"Authentication not supported\"}");
          return;
        }

        final String authPairCypher = auth.substring(AUTHORIZATION_BASIC.length() + 1);

        final String authPairClear = new String(Base64.getDecoder().decode(authPairCypher), DatabaseFactory.getDefaultCharset());

        final String[] authPair = authPairClear.split(":");

        if (authPair.length != 2) {
          exchange.setStatusCode(403);
          exchange.getResponseSender().send("{ \"error\" : \"Basic authentication error\"}");
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

    } catch (ServerSecurityException e) {
      LogManager.instance().log(this, Level.FINE, "Security error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(403);
      exchange.getResponseSender()
          .send("{ \"error\" : \"Security error\", \"detail\":\"" + e + "\", \"exception\": \"" + e.getClass().getName() + "\"}");
    } catch (ServerIsNotTheLeaderException e) {
      LogManager.instance().log(this, Level.FINE, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(400);
      exchange.getResponseSender().send(
          "{ \"error\" : \"Cannot execute command\", \"detail\":\"" + e + "\", \"exception\": \"" + e.getClass().getName()
              + "\", \"exceptionArg\": \"" + e.getLeaderAddress() + "\"}");
    } catch (NeedRetryException e) {
      LogManager.instance().log(this, Level.FINE, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(503);
      exchange.getResponseSender()
          .send("{ \"error\" : \"Cannot execute command\", \"detail\":\"" + e + "\", \"exception\": \"" + e.getClass().getName() + "\"}");
    } catch (DuplicatedKeyException e) {
      LogManager.instance().log(this, Level.FINE, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(503);
      exchange.getResponseSender().send(
          "{ \"error\" : \"Cannot execute command\", \"detail\":\"" + e + "\", \"exception\": \"" + e.getClass().getName()
              + "\", \"exceptionArg\": \"" + e.getIndexName() + "|" + e.getKeys() + "|" + e.getCurrentIndexedRID() + "\" }");
    } catch (CommandExecutionException e) {
      Throwable realException = e;
      if (e.getCause() != null)
        realException = e.getCause();

      LogManager.instance().log(this, Level.FINE, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(500);
      exchange.getResponseSender().send(
          "{ \"error\" : \"Internal error\", \"detail\":\"" + realException.toString() + "\", \"exception\": \"" + realException.getClass().getName() + "\"}");
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error on command execution (%s)", e, getClass().getSimpleName());
      exchange.setStatusCode(500);
      exchange.getResponseSender()
          .send("{ \"error\" : \"Internal error\", \"detail\":\"" + e + "\", \"exception\": \"" + e.getClass().getName() + "\"}");
    } finally {
      LogManager.instance().setContext(null);
    }
  }

  public boolean isRequireAuthentication() {
    return requireAuthentication;
  }

  public void setRequireAuthentication(final boolean requireAuthentication) {
    this.requireAuthentication = requireAuthentication;
  }

  protected ServerSecurity.ServerUser authenticate(final String userName, final String userPassword) {
    return httpServer.getServer().getSecurity().authenticate(userName, userPassword);
  }

  protected JSONObject createResult(final ServerSecurity.ServerUser user) {
    return new JSONObject().put("user", user.name).put("version", Constants.getVersion());
  }

  protected String decode(final String command) {
    return command.replace("&amp;", " ").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&#039;", "'");
  }
}
