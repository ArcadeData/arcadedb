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
package com.arcadedb.server.plugin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6852. Plugin discovery installs every plugin instance up front, but {@code loadDatabases()} runs before
 * {@code startPlugins(AFTER_DATABASES_OPEN)}: a plugin of that priority was therefore handed a "database registered"
 * callback for every database already on disk while its own {@code server} field was still null. In a deployment with
 * ten databases that meant ten SEVERE NullPointerException stack traces at every single startup, from
 * {@code AutoBackupSchedulerPlugin.syncDatabase} -&gt; {@code this.server.existsDatabase(...)}.
 * <p>
 * The fix narrows the notification audience to the plugins whose {@code configure()} has returned. This test pins both
 * halves: nothing arrives before {@code configure()}, and everything that happens afterwards still does - muting the
 * callbacks entirely would "fix" the stack traces by re-breaking issue #6752.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6852PluginLifecycleBeforeConfigureTest extends StaticBaseServerTest {
  private static final String DATABASE_DIRECTORY = "./target/databases";
  private static final String EXISTING_DATABASE  = "issue6852existing";
  private static final String RUNTIME_DATABASE   = "issue6852runtime";

  @Test
  void aPluginIsNotNotifiedBeforeItHasBeenConfigured() {
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue(DATABASE_DIRECTORY);

    // A DATABASE THAT ALREADY EXISTS WHEN THE SERVER STARTS: THIS IS WHAT loadDatabases() REGISTERS, AND WHAT USED TO
    // BE ANNOUNCED TO A PLUGIN THAT HAD NOT BEEN CONFIGURED YET
    try (final Database db = new DatabaseFactory(DATABASE_DIRECTORY + File.separator + EXISTING_DATABASE).create()) {
      db.getSchema().createDocumentType("Doc");
    }

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_0");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DATABASE_DIRECTORY);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    config.setValue(GlobalConfiguration.SERVER_PLUGINS,
        "lifecycle-recorder:" + DatabaseLifecycleTestPlugin.class.getName());

    final ArcadeDBServer server = new ArcadeDBServer(config);
    server.start();
    try {
      final DatabaseLifecycleTestPlugin plugin = findPlugin(server);
      assertThat(server.getDatabaseNames()).contains(EXISTING_DATABASE);

      // THE BUG: ONE CALLBACK PER PRE-EXISTING DATABASE, EVERY ONE OF THEM WITH A NULL SERVER
      assertThat(plugin.getCallbacks())
          .as("no callback may be delivered before configure() has handed the plugin its server")
          .allMatch(DatabaseLifecycleTestPlugin.Callback::hadServer);
      assertThat(plugin.getCallbacks())
          .as("the databases open at startup are the plugin's own job to pick up in startService()")
          .noneMatch(c -> EXISTING_DATABASE.equals(c.databaseName()));

      // ...AND THE CALLBACKS THE PLUGIN IS ACTUALLY THERE FOR STILL ARRIVE (ISSUE #6752)
      server.createDatabase(RUNTIME_DATABASE, ComponentFile.MODE.READ_WRITE);
      assertThat(plugin.getCallbacks()).anyMatch(c -> c.event().equals("registered")
          && c.databaseName().equals(RUNTIME_DATABASE) && c.hadServer() && c.hadStarted());

      server.getDatabase(RUNTIME_DATABASE).getEmbedded().drop();
      server.removeDatabase(RUNTIME_DATABASE);
      assertThat(plugin.getCallbacks()).anyMatch(c -> c.event().equals("unregistered")
          && c.databaseName().equals(RUNTIME_DATABASE) && c.hadServer() && c.hadStarted());
    } finally {
      server.stop();
    }
  }

  private static DatabaseLifecycleTestPlugin findPlugin(final ArcadeDBServer server) {
    for (final ServerPlugin plugin : server.getPlugins())
      if (plugin instanceof final DatabaseLifecycleTestPlugin recorder)
        return recorder;
    throw new AssertionError("the lifecycle recorder plugin was not installed");
  }
}
