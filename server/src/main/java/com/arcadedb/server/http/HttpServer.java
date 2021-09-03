/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.*;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.PathHandler;

import java.net.BindException;
import java.util.logging.Level;

import static io.undertow.UndertowOptions.SHUTDOWN_TIMEOUT;

public class HttpServer implements ServerPlugin {
  private       Undertow       undertow;
  private       JsonSerializer jsonSerializer = new JsonSerializer();
  private final ArcadeDBServer server;
  private       String         listeningAddress;
  private       String         host;
  private       int            port;

  public HttpServer(final ArcadeDBServer server) {
    this.server = server;
  }

  @Override
  public void stopService() {
    if (undertow != null)
      try {
        undertow.stop();
      } catch (Exception e) {
        // IGNORE IT
      }
  }

  @Override
  public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
  }

  @Override
  public void startService() {
    final ContextConfiguration configuration = server.getConfiguration();

    final boolean httpAutoIncrementPort = configuration.getValueAsBoolean(GlobalConfiguration.SERVER_HTTP_AUTOINCREMENT_PORT);

    host = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
    port = configuration.getValueAsInteger(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT);

    server.log(this, Level.INFO, "- Starting HTTP Server (host=%s port=%d)...", host, port);

    final PathHandler routes = new PathHandler();

    final RoutingHandler basicRoutes = Handlers.routing();
    routes.addPrefixPath("/api/v1",//
        basicRoutes//
            .post("/command/{database}", new CommandHandler(this))//
            .post("/create/{database}", new CreateDatabaseHandler(this))//
            .get("/databases", new GetDatabasesHandler(this))//
            .get("/document/{database}/{rid}", new GetDocumentHandler(this))//
            .post("/document/{database}", new CreateDocumentHandler(this))//
            .post("/drop/{database}", new DropDatabaseHandler(this))//
            .post("/exists/{database}", new ExistsDatabaseHandler(this))//
            .post("/import", new ImportDatabaseHandler(this))//
            .get("/query/{database}/{language}/{command}", new GetQueryHandler(this))//
            .post("/query/{database}", new PostQueryHandler(this))//
            .post("/server", new ServersHandler(this))//
    );

    if (!"production".equals(GlobalConfiguration.SERVER_MODE.getValueAsString())) {
      routes.addPrefixPath("/", Handlers.routing().setFallbackHandler(new DynamicContentHandler(this)));
    }

    // REGISTER PLUGIN API
    for (ServerPlugin plugin : server.getPlugins())
      plugin.registerAPI(this, routes);

    do {
      try {
        undertow = Undertow.builder().addHttpListener(port, host).setHandler(routes).setServerOption(SHUTDOWN_TIMEOUT, 1000).build();
        undertow.start();

        server.log(this, Level.INFO, "- HTTP Server started (host=%s port=%d)", host, port);
        listeningAddress = host + ":" + port;
        break;

      } catch (Exception e) {
        undertow = null;

        if (e.getCause() instanceof BindException) {
          // RETRY
          server.log(this, Level.WARNING, "- HTTP Port %s not available", port);
          ++port;
          continue;
        }

        throw new ServerException("Error on starting HTTP Server", e);
      }
    } while (httpAutoIncrementPort);
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
    return port;
  }

  @Override
  public void registerAPI(HttpServer httpServer, final PathHandler routes) {
  }
}
