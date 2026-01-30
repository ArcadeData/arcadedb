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

/**
 * Plugin architecture for ArcadeDB with isolated class loaders.
 * <p>
 * This package provides infrastructure for loading and managing plugins in isolated class loaders,
 * allowing each plugin to have its own set of dependencies without conflicts.
 *
 * <h2>Architecture</h2>
 * <p>
 * The plugin system consists of three main components:
 * <ul>
 *   <li>{@link com.arcadedb.server.plugin.PluginManager} - Discovers and manages plugin lifecycle</li>
 *   <li>{@link com.arcadedb.server.plugin.PluginClassLoader} - Provides isolated class loading</li>
 *   <li>{@link com.arcadedb.server.plugin.PluginDescriptor} - Holds plugin metadata and state</li>
 * </ul>
 *
 * <h2>Plugin Discovery</h2>
 * <p>
 * Plugins are discovered using the Java ServiceLoader pattern. Each plugin JAR must:
 * <ol>
 *   <li>Implement {@link com.arcadedb.server.ServerPlugin} interface</li>
 *   <li>Provide a META-INF/services/com.arcadedb.server.ServerPlugin file with the implementation class name</li>
 *   <li>Be placed in the {@code lib/plugins/} directory</li>
 * </ol>
 *
 * <h2>Class Loading Strategy</h2>
 * <p>
 * The {@link com.arcadedb.server.plugin.PluginClassLoader} uses a hybrid delegation model:
 * <ul>
 *   <li>Server API classes (com.arcadedb.*) are loaded from the parent class loader (shared)</li>
 *   <li>Plugin-specific classes are loaded from the plugin's JAR first (isolated)</li>
 *   <li>Other classes fall back to parent class loader if not found in plugin JAR</li>
 * </ul>
 *
 * <h2>Plugin Lifecycle</h2>
 * <p>
 * Plugins follow this lifecycle:
 * <ol>
 *   <li>Discovery - PluginManager scans lib/plugins/ directory</li>
 *   <li>Loading - Each plugin JAR gets its own PluginClassLoader</li>
 *   <li>Instantiation - ServiceLoader creates plugin instances</li>
 *   <li>Configuration - {@link com.arcadedb.server.ServerPlugin#configure} is called</li>
 *   <li>Starting - {@link com.arcadedb.server.ServerPlugin#startService} is called</li>
 *   <li>Running - Plugin provides its functionality</li>
 *   <li>Stopping - {@link com.arcadedb.server.ServerPlugin#stopService} is called</li>
 *   <li>Cleanup - ClassLoaders are closed</li>
 * </ol>
 *
 * <h2>Creating a Plugin</h2>
 * <p>
 * To create a new plugin:
 * <pre>{@code
 * public class MyPlugin implements ServerPlugin {
 *   @Override
 *   public void configure(ArcadeDBServer server, ContextConfiguration config) {
 *     // Initialize configuration
 *   }
 *
 *   @Override
 *   public void startService() {
 *     // Start plugin services
 *   }
 *
 *   @Override
 *   public void stopService() {
 *     // Stop plugin services
 *   }
 * }
 * }</pre>
 *
 * <p>
 * Create {@code src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin}:
 * <pre>
 * com.example.MyPlugin
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * The plugin system manages the thread context class loader during plugin operations to ensure
 * proper class loading context. Plugin implementations should be thread-safe if they handle
 * concurrent requests.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see com.arcadedb.server.ServerPlugin
 * @see com.arcadedb.server.plugin.PluginManager
 * @see com.arcadedb.server.plugin.PluginClassLoader
 */
package com.arcadedb.server.plugin;
