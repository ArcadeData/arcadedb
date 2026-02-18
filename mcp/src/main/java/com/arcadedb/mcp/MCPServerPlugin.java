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
package com.arcadedb.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;

import java.util.logging.Level;

public class MCPServerPlugin implements ServerPlugin {
  private ArcadeDBServer  server;
  private MCPConfiguration config;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.config = new MCPConfiguration(arcadeDBServer.getRootPath());
    this.config.load();
  }

  @Override
  public void startService() {
    LogManager.instance().log(this, Level.INFO, "MCP Server plugin started");
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    final MCPHttpHandler mcpHandler = new MCPHttpHandler(httpServer, server, config);
    final MCPConfigHandler configHandler = new MCPConfigHandler(httpServer, config);

    routes.addExactPath("/api/v1/mcp", mcpHandler);
    routes.addExactPath("/api/v1/mcp/config", configHandler);

    LogManager.instance().log(this, Level.INFO, "MCP HTTP endpoint registered at /api/v1/mcp");
  }

  // Uses default BEFORE_HTTP_ON priority so configure() is called before registerAPI().
}
