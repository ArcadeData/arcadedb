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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.logging.*;

public class GremlinServerPlugin implements ServerPlugin {
  private static final String               CONFIG_GREMLIN_SERVER_YAML = "/config/gremlin-server.yaml";
  private              ArcadeDBServer       server;
  private              ContextConfiguration configuration;
  private              GremlinServer        gremlinServer;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public void startService() {
    // Set the server instance for dynamic database registration
    ArcadeGraphManager.setServer(server);

    Settings settings = null;
    final File confFile = new File(server.getRootPath() + CONFIG_GREMLIN_SERVER_YAML);
    if (confFile.exists()) {
      try (final FileInputStream is = new FileInputStream(confFile.getAbsolutePath())) {
        settings = ArcadeDBGremlinSettings.read(is);
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.INFO, "Error on loading Gremlin Server configuration file '%s'. Using default configuration", CONFIG_GREMLIN_SERVER_YAML);
      }
    } else
      LogManager.instance()
          .log(this, Level.INFO, "Cannot find Gremlin Server configuration file '%s'. Using default configuration", CONFIG_GREMLIN_SERVER_YAML);

    if (settings == null)
      // DEFAULT CONFIGURATION
      settings = new Settings();

    // Use ArcadeDB's custom GraphManager for dynamic database registration
    settings.graphManager = ArcadeGraphManager.class.getName();

    // OVERWRITE AUTHENTICATION USING THE SERVER SECURITY
    settings.authentication = new Settings.AuthenticationSettings();
    settings.authentication.authenticator = GremlinServerAuthenticator.class.getName();
    settings.authentication.config = new HashMap<>(1);
    settings.authentication.config.put("server", server);

    for (final String key : configuration.getContextKeys()) {
      if (key.startsWith("gremlin.")) {
        final Object value = configuration.getValue(key, null);
        final String gremlinConfigKey = key.substring("gremlin.".length());

        try {
          final Field field = settings.getClass().getField(gremlinConfigKey);
          field.set(settings, value);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
          // IGNORE IT
        }
      }
    }

    // Ensure databases referenced in the graphs section of gremlin-server.yaml are created/opened.
    // This restores the pre-2026.2.1 behaviour where a static `graphs:` entry in gremlin-server.yaml
    // would cause ArcadeGraph to create the database on first access (issue #3661).
    initPreConfiguredDatabases(settings);

    gremlinServer = new GremlinServer(settings);
    try {
      gremlinServer.start();
    } catch (final Exception e) {
      throw new ServerException("Error on starting GremlinServer plugin", e);
    }
  }

  /**
   * For every graph declared in the Gremlin settings' {@code graphs} section, reads the matching
   * {@code .properties} file, extracts {@value ArcadeGraph#CONFIG_DIRECTORY}, and makes sure the
   * database is registered with (and, if absent, created by) ArcadeDBServer before the Gremlin
   * server starts.
   */
  private void initPreConfiguredDatabases(final Settings settings) {
    if (settings.graphs == null || settings.graphs.isEmpty())
      return;

    for (final Map.Entry<String, String> entry : settings.graphs.entrySet()) {
      final String graphName = entry.getKey();
      final String propertiesPath = entry.getValue();

      try {
        final String resolvedPath = resolveConfigPath(server.getRootPath(), propertiesPath);
        final File propertiesFile = new File(resolvedPath);
        if (!propertiesFile.exists()) {
          LogManager.instance().log(this, Level.WARNING,
              "Gremlin graph '%s': properties file '%s' not found — skipping database init", graphName, resolvedPath);
          continue;
        }

        final Properties props = new Properties();
        try (final FileInputStream fis = new FileInputStream(propertiesFile)) {
          props.load(fis);
        }

        final String dbDirectory = props.getProperty(ArcadeGraph.CONFIG_DIRECTORY);
        if (dbDirectory == null) {
          LogManager.instance().log(this, Level.WARNING,
              "Gremlin graph '%s': property '%s' not found in '%s' — skipping database init",
              graphName, ArcadeGraph.CONFIG_DIRECTORY, resolvedPath);
          continue;
        }

        // Derive the database name from the last path component (e.g. "./databases/graph" → "graph")
        final String dbName = new File(dbDirectory).getName();

        if (!server.existsDatabase(dbName)) {
          // createIfNotExists=true: creates the database if it doesn't exist yet
          server.getDatabase(dbName, true, true);
          LogManager.instance().log(this, Level.INFO,
              "Gremlin graph '%s': created/opened database '%s'", graphName, dbName);
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Gremlin graph '%s': error initializing database — %s", graphName, e.getMessage());
      }
    }
  }

  private static String resolveConfigPath(final String rootPath, final String path) {
    final File f = new File(path);
    if (f.isAbsolute())
      return path;
    return rootPath + File.separator + path;
  }

  @Override
  public void stopService() {
    if (gremlinServer != null) {
      // Close all dynamically created ArcadeGraph instances
      final var graphManager = gremlinServer.getServerGremlinExecutor().getGraphManager();
      if (graphManager instanceof ArcadeGraphManager) {
        ((ArcadeGraphManager) graphManager).closeAll();
      }
      gremlinServer.stop().join();
    }
  }
}
