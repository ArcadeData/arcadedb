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
import io.undertow.Undertow;
import io.undertow.server.RoutingHandler;

import java.net.BindException;
import java.util.logging.Level;

import static io.undertow.UndertowOptions.SHUTDOWN_TIMEOUT;

public class HttpServer implements ServerPlugin {
  private       Undertow       undertow;
  private       JsonSerializer jsonSerializer = new JsonSerializer();
  private final ArcadeDBServer server;
  private       String         listeningAddress;

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

    final String host = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
    int port = configuration.getValueAsInteger(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT);

    server.log(this, Level.INFO, "- Starting HTTP Server (host=%s port=%d)...", host, port);

    final RoutingHandler routes = new RoutingHandler();
    routes.get("/query/{database}/{language}/{command}", new GetQueryHandler(this));
    routes.post("/query/{database}", new PostQueryHandler(this));
    routes.post("/command/{database}", new CommandHandler(this));
    routes.get("/document/{database}/{rid}", new GetDocumentHandler(this));
    routes.post("/document/{database}", new CreateDocumentHandler(this));
    routes.post("/server", new ServersHandler(this));
    routes.post("/create/{database}", new CreateDatabaseHandler(this));
    routes.post("/exists/{database}", new ExistsDatabaseHandler(this));
    routes.post("/drop/{database}", new DropDatabaseHandler(this));

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
}
