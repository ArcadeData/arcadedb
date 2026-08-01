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

/**
 * Installs the Model Context Protocol server: owns its configuration and registers the HTTP transport.
 * <p>
 * Auto-discovered, so a standard distribution keeps answering on /api/v1/mcp with no SERVER_PLUGINS entry and
 * no migration. What makes MCP optional is building a distribution without this module, not configuration.
 * <p>
 * Runs at BEFORE_HTTP_ON, and that matters: the plugin manager starts those plugins before
 * {@code HttpServer.startService()} assembles the routes, so {@link #configure} has already loaded the
 * configuration by the time {@link #registerAPI} hands it to the handlers. An AFTER_HTTP_ON plugin is called
 * the other way round.
 */
public class MCPPlugin implements ServerPlugin {

  // volatile: written by the startup thread in configure(), i.e. AFTER PluginManager has already published this
  // instance into its plugin map during discovery. Readers reach them off that thread through of() and
  // getConfiguration() (Undertow worker threads serving /api/v1/mcp/config, tests, embedding code), and
  // getPlugins()' synchronized(plugins) gives no happens-before with a write that came after the insertion, so
  // under the JMM those readers may observe null indefinitely.
  private volatile ArcadeDBServer   server;
  private volatile MCPConfiguration configuration;

  /**
   * Returns the installed plugin, or null when this server was built without the MCP module.
   */
  public static MCPPlugin of(final ArcadeDBServer server) {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof MCPPlugin mcpPlugin)
        return mcpPlugin;
    return null;
  }

  public MCPConfiguration getConfiguration() {
    return configuration;
  }

  @Override
  public boolean isAutoDiscovered(final ContextConfiguration contextConfiguration) {
    return true;
  }

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration contextConfiguration) {
    configuration = new MCPConfiguration(arcadeDBServer.getRootPath());
    configuration.load();
    configuration.warnUnknownDatabaseOverrides(arcadeDBServer.getDatabaseNames());
    this.server = arcadeDBServer;
  }

  @Override
  public void startService() {
    // NO-OP: the HTTP transport is installed by registerAPI, and the stdio transport is a separate process.
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    // MCP routes are always registered; the handler checks isEnabled() at request time to support runtime toggling
    routes.addExactPath("/api/v1/mcp", new MCPHttpHandler(httpServer, server, configuration));
    routes.addExactPath("/api/v1/mcp/config", new MCPConfigHandler(httpServer, configuration));

    LogManager.instance().log(this, Level.INFO, "MCP server endpoint registered at /api/v1/mcp");
  }
}
