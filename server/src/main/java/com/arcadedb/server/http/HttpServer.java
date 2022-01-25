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
package com.arcadedb.server.http;

import static io.undertow.UndertowOptions.SHUTDOWN_TIMEOUT;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.*;
import com.arcadedb.server.http.ws.WebSocketConnectionHandler;
import com.arcadedb.server.http.ws.WebSocketEventBus;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.PathHandler;

import java.net.BindException;
import java.util.logging.Level;

public class HttpServer implements ServerPlugin {
  private final ArcadeDBServer     server;
  private final HttpSessionManager sessionManager;
  private final JsonSerializer     jsonSerializer = new JsonSerializer();
  private final WebSocketEventBus  webSocketEventBus;
  private       Undertow           undertow;
  private       String             listeningAddress;
  private       String             host;
  private       int                portListening;
  private       int                portFrom;
  private       int                portTo;

  public HttpServer(final ArcadeDBServer server) {
    this.server = server;
    this.sessionManager = new HttpSessionManager(server.getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_TX_EXPIRE_TIMEOUT) * 1000L);
    this.webSocketEventBus = new WebSocketEventBus(this.server);
  }

  @Override
  public void stopService() {
    webSocketEventBus.stop();

    if (undertow != null)
      try {
        undertow.stop();
      } catch (Exception e) {
        // IGNORE IT
      }

    sessionManager.close();
  }

  @Override
  public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
  }

  @Override
  public void startService() {
    final ContextConfiguration configuration = server.getConfiguration();

    host = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
    final Object configuredPort = configuration.getValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT);
    if (configuredPort instanceof Number)
      portFrom = portTo = ((Number) configuredPort).intValue();
    else {
      final String[] parts = configuredPort.toString().split("-");
      if (parts.length > 2)
        throw new IllegalArgumentException("Invalid format for http server port range");
      else if (parts.length == 1)
        portFrom = portTo = Integer.parseInt(parts[0]);
      else {
        portFrom = Integer.parseInt(parts[0]);
        portTo = Integer.parseInt(parts[1]);
      }
    }

    com.arcadedb.log.LogManager.instance().log(this, Level.INFO, "- Starting HTTP Server (host=%s port=%s)...", host, configuredPort.toString());

    final PathHandler routes = new PathHandler();

    final RoutingHandler basicRoutes = Handlers.routing();

    routes.addPrefixPath("/ws", new WebSocketConnectionHandler(this, webSocketEventBus));

    routes.addPrefixPath("/api/v1",//
        basicRoutes//
            .post("/begin/{database}", new PostBeginHandler(this))//
            .post("/command/{database}", new PostCommandHandler(this))//
            .post("/commit/{database}", new PostCommitHandler(this))//
            .post("/create/{database}", new PostCreateDatabaseHandler(this))//
            .get("/databases", new GetDatabasesHandler(this))//
            .get("/document/{database}/{rid}", new GetDocumentHandler(this))//
            .post("/document/{database}", new PostCreateDocumentHandler(this))//
            .post("/drop/{database}", new PostDropDatabaseHandler(this))//
            .get("/exists/{database}", new GetExistsDatabaseHandler(this))//
            .get("/query/{database}/{language}/{command}", new GetQueryHandler(this))//
            .post("/query/{database}", new PostQueryHandler(this))//
            .post("/rollback/{database}", new PostRollbackHandler(this))//
            .post("/server", new PostServersHandler(this))//
    );

    if (!"production".equals(GlobalConfiguration.SERVER_MODE.getValueAsString())) {
      routes.addPrefixPath("/", Handlers.routing().setFallbackHandler(new GetDynamicContentHandler(this)));
    }

    // REGISTER PLUGIN API
    for (ServerPlugin plugin : server.getPlugins())
      plugin.registerAPI(this, routes);

    for (portListening = portFrom; portListening <= portTo; ++portListening) {
      try {
        undertow = Undertow.builder().addHttpListener(portListening, host).setHandler(routes).setServerOption(SHUTDOWN_TIMEOUT, 1000).build();
        undertow.start();

        LogManager.instance().log(this, Level.INFO, "- HTTP Server started (host=%s port=%d)", host, portListening);
        listeningAddress = host + ":" + portListening;
        return;

      } catch (Exception e) {
        undertow = null;

        if (e.getCause() instanceof BindException) {
          // RETRY
          LogManager.instance().log(this, Level.WARNING, "- HTTP Port %s not available", portListening);
          continue;
        }

        throw new ServerException("Error on starting HTTP Server", e);
      }
    }

    portListening = -1;
    final String msg = String.format("Unable to listen to a HTTP port in the configured port range %d - %d", portFrom, portTo);
    LogManager.instance().log(this, Level.SEVERE, msg);
    throw new ServerException("Error on starting HTTP Server: " + msg);
  }

  public HttpSessionManager getSessionManager() {
    return sessionManager;
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  public JsonSerializer getJsonSerializer() {
    return jsonSerializer;
  }

  public String getListeningAddress() {
    return listeningAddress;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return portListening;
  }

  public WebSocketEventBus getWebSocketEventBus() {
    return webSocketEventBus;
  }

  @Override
  public void registerAPI(HttpServer httpServer, final PathHandler routes) {
  }
}
