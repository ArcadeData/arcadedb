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
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;

public class GremlinServerPlugin implements ServerPlugin {
  private static final String               CONFIG_GREMLIN_SERVER_YAML = "/config/gremlin-server.yaml";
  private static final String               IO_REGISTRIES_KEY          = "ioRegistries";
  private static final String               ARCADE_IO_REGISTRY         = ArcadeIoRegistry.class.getName();

  // SERIALIZERS CONFIGURED WHEN NONE ARE DECLARED IN gremlin-server.yaml. THEY MIRROR THE SHIPPED CONFIGURATION SO THAT
  // ARCADEDB TYPES (VERTICES, EDGES AND RAW RIDs - SEE ISSUE #5309) ARE ALWAYS SERIALIZABLE, EVEN WITH DEFAULT SETTINGS.
  private static final String[]             DEFAULT_SERIALIZERS        = {
      "org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1",
      "org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3",
      "org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2" };
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

    // GUARANTEE THAT ARCADEDB TYPES (VERTICES, EDGES AND RAW RIDs) CAN BE SERIALIZED REGARDLESS OF THE (POSSIBLY
    // ABSENT OR CUSTOM) gremlin-server.yaml, BY ENSURING ArcadeIoRegistry IS REGISTERED ON EVERY SERIALIZER (#5309).
    ensureArcadeIoRegistry(settings);

    gremlinServer = new GremlinServer(settings);
    try {
      gremlinServer.start();
    } catch (final Exception e) {
      throw new ServerException("Error on starting GremlinServer plugin", e);
    }
  }

  /**
   * Ensures that {@link ArcadeIoRegistry} is registered on every configured serializer so that ArcadeDB types
   * (vertices, edges and raw RIDs) round-trip correctly. When {@code gremlin-server.yaml} declares no serializers, the
   * shipped default set (GraphBinary v1, GraphSON v3, GraphSON v2) is installed; when it does, each entry is augmented
   * with the registry if missing. Without this, a traversal result carrying a raw RID (e.g. the value of a LINK
   * property) fails to be serialized with "Serializer for type com.arcadedb.database.DatabaseRID not found" (#5309).
   */
  private void ensureArcadeIoRegistry(final Settings settings) {
    if (settings.serializers == null || settings.serializers.isEmpty()) {
      final List<Settings.SerializerSettings> serializers = new ArrayList<>(DEFAULT_SERIALIZERS.length);
      for (final String className : DEFAULT_SERIALIZERS) {
        final Settings.SerializerSettings serializer = new Settings.SerializerSettings();
        serializer.className = className;
        serializer.config = new HashMap<>();
        addArcadeIoRegistry(serializer);
        serializers.add(serializer);
      }
      settings.serializers = serializers;
    } else {
      for (final Settings.SerializerSettings serializer : settings.serializers)
        addArcadeIoRegistry(serializer);
    }
  }

  @SuppressWarnings("unchecked")
  private void addArcadeIoRegistry(final Settings.SerializerSettings serializer) {
    if (serializer.config == null)
      serializer.config = new HashMap<>();

    final Object existing = serializer.config.get(IO_REGISTRIES_KEY);
    final List<Object> registries = existing instanceof List ? new ArrayList<>((List<Object>) existing) : new ArrayList<>();

    if (!registries.contains(ARCADE_IO_REGISTRY))
      registries.add(ARCADE_IO_REGISTRY);

    serializer.config.put(IO_REGISTRIES_KEY, registries);
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
        final String dbName = new File(dbDirectory.replaceAll("/+$", "")).getName();
        if (dbName.isEmpty()) {
          LogManager.instance().log(this, Level.WARNING,
              "Gremlin graph '%s': cannot derive database name from directory '%s' — skipping",
              graphName, dbDirectory);
          continue;
        }

        if (!server.existsDatabase(dbName)) {
          // createIfNotExists=true: creates the database if it doesn't exist yet
          server.getDatabase(dbName, true, true);
          LogManager.instance().log(this, Level.INFO,
              "Gremlin graph '%s': created/opened database '%s'", graphName, dbName);
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Gremlin graph '%s': error initializing database", e, graphName);
      }
    }
  }

  private static String resolveConfigPath(final String rootPath, final String path) {
    final File f = new File(path);
    if (f.isAbsolute())
      return path;
    return new File(rootPath, path).getPath();
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
