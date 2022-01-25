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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.logging.Level;

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
    Settings settings = null;
    final File confFile = new File(server.getRootPath() + CONFIG_GREMLIN_SERVER_YAML);
    if (confFile.exists()) {
      try (FileInputStream is = new FileInputStream(confFile.getAbsolutePath())) {
        settings = Settings.read(is);
      } catch (Exception e) {
        LogManager.instance()
            .log(this, Level.INFO, "Error on loading Gremlin Server configuration file '%s'. Using default configuration", CONFIG_GREMLIN_SERVER_YAML);
      }
    } else
      LogManager.instance()
          .log(this, Level.INFO, "Cannot find Gremlin Server configuration file '%s'. Using default configuration", CONFIG_GREMLIN_SERVER_YAML);

    if (settings == null)
      // DEFAULT CONFIGURATION
      settings = new Settings();

    for (String key : configuration.getContextKeys()) {
      if (key.startsWith("gremlin.")) {
        final Object value = configuration.getValue(key, null);
        final String gremlinConfigKey = key.substring("gremlin.".length());

        try {
          final Field field = settings.getClass().getField(gremlinConfigKey);
          field.set(settings, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
          // IGNORE IT
        }
      }
    }

    gremlinServer = new GremlinServer(settings);
    try {
      gremlinServer.start();
    } catch (Exception e) {
      throw new ServerException("Error on starting GremlinServer plugin", e);
    }
  }

  @Override
  public void stopService() {
    if (gremlinServer != null)
      gremlinServer.stop();
  }

  @Override
  public void registerAPI(HttpServer httpServer, final PathHandler routes) {
  }
}
