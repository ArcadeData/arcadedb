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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records the database lifecycle callbacks it receives, together with how far its own initialization had got when each
 * one arrived. It runs at {@link PluginInstallationPriority#AFTER_DATABASES_OPEN}, the priority whose plugins the server
 * used to notify about every pre-existing database before handing them the server itself (issue #6852).
 * <p>
 * Not auto-discovered: it is installed only by a test that names it in {@code SERVER_PLUGINS}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseLifecycleTestPlugin implements ServerPlugin {
  /** One recorded callback: what happened, to which database, and what the plugin held at that moment. */
  public record Callback(String event, String databaseName, boolean hadServer, boolean hadStarted) {
  }

  private final List<Callback> callbacks = new CopyOnWriteArrayList<>();
  private       ArcadeDBServer server;
  private       boolean        started;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
  }

  @Override
  public void startService() {
    started = true;
  }

  @Override
  public void stopService() {
    started = false;
  }

  @Override
  public PluginInstallationPriority getInstallationPriority() {
    return PluginInstallationPriority.AFTER_DATABASES_OPEN;
  }

  @Override
  public void onDatabaseRegistered(final String databaseName) {
    callbacks.add(new Callback("registered", databaseName, server != null, started));
  }

  @Override
  public void onDatabaseUnregistered(final String databaseName) {
    callbacks.add(new Callback("unregistered", databaseName, server != null, started));
  }

  public List<Callback> getCallbacks() {
    return callbacks;
  }
}
