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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.utility.CodeUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Manager for loading and managing plugins using isolated class loaders.
 * Plugins are discovered from the lib/plugins directory using the ServiceLoader pattern.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PluginManager {
  private final ArcadeDBServer                       server;
  private final ContextConfiguration                 configuration;
  private final String                               pluginsDirectory;
  private final Map<String, PluginDescriptor>        plugins = new LinkedHashMap<>();
  private final Map<ClassLoader, PluginDescriptor>   classLoaderMap = new ConcurrentHashMap<>();

  public PluginManager(final ArcadeDBServer server, final ContextConfiguration configuration) {
    this.server = server;
    this.configuration = configuration;
    this.pluginsDirectory = server.getRootPath() + File.separator + "lib" + File.separator + "plugins";
  }

  /**
   * Discover and load plugins from the plugins directory.
   * Each plugin JAR is loaded in its own isolated class loader.
   */
  public void discoverPlugins() {
    final File pluginsDir = new File(pluginsDirectory);
    if (!pluginsDir.exists() || !pluginsDir.isDirectory()) {
      LogManager.instance().log(this, Level.INFO, "Plugins directory not found: %s", pluginsDirectory);
      return;
    }

    final File[] pluginJars = pluginsDir.listFiles((dir, name) -> name.endsWith(".jar"));
    if (pluginJars == null || pluginJars.length == 0) {
      LogManager.instance().log(this, Level.INFO, "No plugin JARs found in: %s", pluginsDirectory);
      return;
    }

    for (final File pluginJar : pluginJars) {
      try {
        loadPlugin(pluginJar);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Failed to load plugin from: %s", e, pluginJar.getAbsolutePath());
      }
    }
  }

  /**
   * Load a plugin from a JAR file using an isolated class loader.
   */
  private void loadPlugin(final File pluginJar) throws Exception {
    final String jarName = pluginJar.getName();
    final String pluginName = jarName.substring(0, jarName.lastIndexOf('.'));

    LogManager.instance().log(this, Level.FINE, "Loading plugin from: %s", pluginJar.getAbsolutePath());

    // Create isolated class loader for this plugin
    final PluginClassLoader classLoader = new PluginClassLoader(pluginName, pluginJar, getClass().getClassLoader());

    // Create plugin descriptor
    final PluginDescriptor descriptor = new PluginDescriptor(pluginName, pluginJar, classLoader);

    // Use ServiceLoader to discover plugin implementations
    final ServiceLoader<ServerPlugin> serviceLoader = ServiceLoader.load(ServerPlugin.class, classLoader);
    final Iterator<ServerPlugin> iterator = serviceLoader.iterator();

    if (!iterator.hasNext()) {
      LogManager.instance().log(this, Level.WARNING,
          "No ServerPlugin implementation found in: %s (missing META-INF/services entry?)", pluginJar.getAbsolutePath());
      return;
    }

    // Load the first plugin implementation (typically only one per JAR)
    final ServerPlugin pluginInstance = iterator.next();
    descriptor.setPluginInstance(pluginInstance);

    // Register the plugin
    plugins.put(pluginName, descriptor);
    classLoaderMap.put(classLoader, descriptor);

    LogManager.instance().log(this, Level.INFO, "Discovered plugin: %s from %s", pluginName, pluginJar.getName());
  }

  /**
   * Start plugins based on their installation priority.
   */
  public void startPlugins(final ServerPlugin.INSTALLATION_PRIORITY priority) {
    for (final Map.Entry<String, PluginDescriptor> entry : plugins.entrySet()) {
      final String pluginName = entry.getKey();
      final PluginDescriptor descriptor = entry.getValue();
      final ServerPlugin plugin = descriptor.getPluginInstance();

      if (plugin == null || descriptor.isStarted()) {
        continue;
      }

      if (plugin.getInstallationPriority() != priority) {
        continue;
      }

      try {
        // Set the context class loader to the plugin's class loader
        final Thread currentThread = Thread.currentThread();
        final ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(descriptor.getClassLoader());

          // Configure and start the plugin
          plugin.configure(server, configuration);
          plugin.startService();

          descriptor.setStarted(true);
          LogManager.instance().log(this, Level.INFO, "- %s plugin started", pluginName);

        } finally {
          currentThread.setContextClassLoader(originalClassLoader);
        }
      } catch (final Exception e) {
        throw new ServerException("Error starting plugin: " + pluginName, e);
      }
    }
  }

  /**
   * Stop all plugins in reverse order of registration.
   */
  public void stopPlugins() {
    final List<Map.Entry<String, PluginDescriptor>> pluginList = new ArrayList<>(plugins.entrySet());
    Collections.reverse(pluginList);

    for (final Map.Entry<String, PluginDescriptor> entry : pluginList) {
      final String pluginName = entry.getKey();
      final PluginDescriptor descriptor = entry.getValue();
      final ServerPlugin plugin = descriptor.getPluginInstance();

      if (plugin == null || !descriptor.isStarted()) {
        continue;
      }

      LogManager.instance().log(this, Level.INFO, "- Stop %s plugin", pluginName);

      final Thread currentThread = Thread.currentThread();
      final ClassLoader originalClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(descriptor.getClassLoader());
        CodeUtils.executeIgnoringExceptions(plugin::stopService,
            "Error stopping plugin: " + pluginName, false);
        descriptor.setStarted(false);
      } finally {
        currentThread.setContextClassLoader(originalClassLoader);
      }
    }

    // Close class loaders
    for (final PluginDescriptor descriptor : plugins.values()) {
      final ClassLoader classLoader = descriptor.getClassLoader();
      if (classLoader instanceof PluginClassLoader) {
        try {
          ((PluginClassLoader) classLoader).close();
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.WARNING, "Error closing class loader for plugin: %s",
              e, descriptor.getPluginName());
        }
      }
    }

    plugins.clear();
    classLoaderMap.clear();
  }

  /**
   * Get all loaded plugins.
   */
  public Collection<ServerPlugin> getPlugins() {
    final List<ServerPlugin> result = new ArrayList<>();
    for (final PluginDescriptor descriptor : plugins.values()) {
      if (descriptor.getPluginInstance() != null) {
        result.add(descriptor.getPluginInstance());
      }
    }
    return Collections.unmodifiableCollection(result);
  }

  /**
   * Get the number of loaded plugins.
   */
  public int getPluginCount() {
    return plugins.size();
  }

  /**
   * Get plugin names.
   */
  public Set<String> getPluginNames() {
    return Collections.unmodifiableSet(plugins.keySet());
  }

  /**
   * Get plugin descriptor by name.
   */
  public PluginDescriptor getPluginDescriptor(final String pluginName) {
    return plugins.get(pluginName);
  }
}
