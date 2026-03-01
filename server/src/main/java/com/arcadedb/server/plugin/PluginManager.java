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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.utility.CodeUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Manager for loading and managing plugins using isolated class loaders.
 * Plugins are discovered from the lib/plugins directory using the ServiceLoader pattern.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PluginManager {
  private final ArcadeDBServer                     server;
  private final ContextConfiguration               configuration;
  private final String                             pluginsDirectory;
  private final Map<String, PluginDescriptor>      plugins        = new LinkedHashMap<>();
  private final Map<ClassLoader, PluginDescriptor> classLoaderMap = new ConcurrentHashMap<>();
  private final Set<String>                        configuredPlugins;

  public PluginManager(final ArcadeDBServer server, final ContextConfiguration configuration) {
    this.server = server;
    this.configuration = configuration;
    this.pluginsDirectory = server.getRootPath() + File.separator + "lib" + File.separator + "plugins";
    configuredPlugins = getConfiguredPlugins();
  }

  private Set<String> getConfiguredPlugins() {
    final String configuration = this.configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    Set<String> configuredPlugins = new HashSet<>();
    if (!configuration.isEmpty()) {
      final String[] pluginEntries = configuration.split(",");
      for (final String p : pluginEntries) {
        final String[] pluginPair = p.split(":");

        final String pluginName = pluginPair[0];
        configuredPlugins.add(pluginName);
        final String pluginClass = pluginPair.length > 1 ? pluginPair[1] : pluginPair[0];
        configuredPlugins.add(pluginClass);
      }
    }
    return configuredPlugins;
  }

  private void discoverPluginsOnMainClassLoader() {
    final ServiceLoader<ServerPlugin> serviceLoader = ServiceLoader.load(ServerPlugin.class, getClass().getClassLoader());

    for (ServerPlugin pluginInstance : serviceLoader) {
      final String name = pluginInstance.getClass().getSimpleName();
      final PluginDescriptor descriptor = new PluginDescriptor(name, getClass().getClassLoader());
      descriptor.setPluginInstance(pluginInstance);
      plugins.put(name, descriptor);

      LogManager.instance().log(this, Level.INFO, "Discovered plugin on main class loader: %s", name);
    }
  }

  /**
   * Discover and load plugins from the plugins directory.
   * Each plugin JAR is loaded in its own isolated class loader.
   */
  public void discoverPlugins() {
    discoverPluginsOnMainClassLoader();

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
    boolean registered = false;

    try {
      // Use ServiceLoader to discover plugin implementations
      final ServiceLoader<ServerPlugin> serviceLoader = ServiceLoader.load(ServerPlugin.class, classLoader);

      // Load the first new plugin implementation from this JAR
      for (final ServerPlugin pluginInstance : serviceLoader) {
        final String name = pluginInstance.getName();
        LogManager.instance().log(this, Level.FINE, "Discovered plugin class: %s", name);

        // Skip plugins already registered (e.g., from main classpath discovery)
        if (plugins.containsKey(name))
          continue;

        if (configuredPlugins.contains(name) || configuredPlugins.contains(pluginName) || configuredPlugins.contains(
            pluginInstance.getClass().getName())) {
          final PluginDescriptor descriptor = new PluginDescriptor(name, classLoader);
          descriptor.setPluginInstance(pluginInstance);
          plugins.put(name, descriptor);
          classLoaderMap.put(classLoader, descriptor);
          registered = true;

          LogManager.instance().log(this, Level.INFO, "Loaded plugin: %s from %s", name, pluginJar.getName());
        } else {
          LogManager.instance().log(this, Level.INFO, "Skipping plugin: %s as not registered in configuration", name);
        }
        break; // Only load the first new plugin from each JAR
      }
    } finally {
      if (!registered) {
        classLoader.close();
      }
    }
  }

  /**
   * Start plugins based on their installation priority.
   */
  public void startPlugins(final ServerPlugin.PluginInstallationPriority priority) {
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
        throw new ServerException("Error starting plugin: " + pluginName + " (priority: " + priority + ")", e);
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

  public void registerPlugin(final String pluginName, final ServerPlugin pluginInstance) {
    final PluginDescriptor descriptor = new PluginDescriptor(pluginName, pluginInstance.getClass().getClassLoader());
    descriptor.setPluginInstance(pluginInstance);
    plugins.put(pluginName, descriptor);
  }
}
