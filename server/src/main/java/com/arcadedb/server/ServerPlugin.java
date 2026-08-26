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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;

import static com.arcadedb.server.ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON;

public interface ServerPlugin {
  enum PluginInstallationPriority {BEFORE_HTTP_ON, AFTER_HTTP_ON, AFTER_DATABASES_OPEN}

  default String getName() {
    return this.getClass().getSimpleName();
  }

  default void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
    // DEFAULT IMPLEMENTATION
  }

  void startService();

  default void stopService() {
    // DEFAULT IMPLEMENTATION
  }

  default void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    // DEFAULT IMPLEMENTATION
  }

  default PluginInstallationPriority getInstallationPriority() {
    return BEFORE_HTTP_ON;
  }

  default boolean isActive() {
    return true;
  }

  /**
   * Invoked right after a database entered the server registry: created at runtime, restored, imported, opened on
   * demand or re-registered by the HA snapshot installer. A plugin that keeps per-database state (a schedule, a
   * cache, a listener) uses this to pick up a database that did not exist when the plugin started.
   * <p>
   * Called on the thread that performed the registration, which may still hold the server's database lock, so the
   * implementation has to be short and non-blocking. The callback is dispatched after the registry mutation rather
   * than under its lock, so two concurrent mutations of the same name can deliver their callbacks in either order:
   * reconcile against the registry as it is now (see {@link ArcadeDBServer#existsDatabase}) instead of assuming the
   * event order. Any exception thrown here is logged and swallowed: a misbehaving plugin must not fail the operation
   * that created the database.
   *
   * @param databaseName name of the database that has just been registered
   */
  default void onDatabaseRegistered(final String databaseName) {
    // DEFAULT IMPLEMENTATION
  }

  /**
   * Invoked right after a database left the server registry: dropped, explicitly closed, or removed by the HA
   * apply/snapshot-install paths. A plugin that keeps per-database state must release it here, otherwise the state
   * outlives its database - a scheduled task, for instance, would keep firing against a name that is either gone or
   * that the operator explicitly closed (issue #6752).
   * <p>
   * Called on the thread that performed the removal, which may still hold the server's database lock, so the
   * implementation has to be short and non-blocking. The same out-of-order delivery caveat as
   * {@link #onDatabaseRegistered(String)} applies. Any exception thrown here is logged and swallowed: a misbehaving
   * plugin must not fail the drop/close.
   *
   * @param databaseName name of the database that has just been unregistered
   */
  default void onDatabaseUnregistered(final String databaseName) {
    // DEFAULT IMPLEMENTATION
  }

  /**
   * Whether this plugin activates on classpath presence alone, without an entry in {@code SERVER_PLUGINS}.
   * <p>
   * The default is {@code false}: a plugin is opt-in and a deployment names it explicitly. A plugin that is
   * part of the standard distribution and must keep answering after an upgrade returns {@code true} instead,
   * so excluding it from a custom build is the only thing that removes it. Owning the rule here rather than
   * in the plugin manager is what lets that manager stay ignorant of plugin identities.
   */
  default boolean isAutoDiscovered(final ContextConfiguration configuration) {
    return false;
  }
}
