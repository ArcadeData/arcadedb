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

import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * Dedicated class loader for wrapper plugins to isolate them from the main application class loader.
 * This class loader is used for loading MongoDB, Redis, PostgreSQL, and Gremlin protocol wrappers.
 */
public class WrapperPluginClassLoader extends URLClassLoader {
  private static final ConcurrentMap<String, WrapperPluginClassLoader> classLoaders = new ConcurrentHashMap<>();

  private final String pluginName;

  private WrapperPluginClassLoader(final String pluginName, final URL[] urls, final ClassLoader parent) {
    super(urls, parent);
    this.pluginName = pluginName;
    LogManager.instance().log(this, Level.FINE, "Created dedicated class loader for wrapper plugin: %s", pluginName);
  }

  /**
   * Creates or returns a dedicated class loader for a wrapper plugin.
   *
   * @param pluginName the name of the plugin
   * @param urls the URLs from which to load classes and resources
   * @param parent the parent class loader
   * @return the dedicated class loader for the plugin
   */
  public static synchronized WrapperPluginClassLoader getOrCreateClassLoader(
      final String pluginName,
      final URL[] urls,
      final ClassLoader parent) {
    return classLoaders.computeIfAbsent(pluginName, name -> new WrapperPluginClassLoader(name, urls, parent));
  }

  /**
   * Checks if the given plugin class name represents a wrapper plugin that should be loaded
   * with a dedicated class loader.
   *
   * @param pluginClassName the class name of the plugin
   * @return true if this is a wrapper plugin, false otherwise
   */
  public static boolean isWrapperPlugin(final String pluginClassName) {
    return pluginClassName != null && (
        pluginClassName.contains("mongo") ||
        pluginClassName.contains("redis") ||
        pluginClassName.contains("postgres") ||
        pluginClassName.contains("gremlin")
    );
  }

  /**
   * Extracts the wrapper plugin name from the plugin class name.
   *
   * @param pluginClassName the class name of the plugin
   * @return the wrapper plugin name or null if not a wrapper plugin
   */
  public static String getWrapperPluginName(final String pluginClassName) {
    if (pluginClassName == null) return null;

    final String lowerClassName = pluginClassName.toLowerCase();
    if (lowerClassName.contains("mongo")) return "MongoDB";
    if (lowerClassName.contains("redis")) return "Redis";
    if (lowerClassName.contains("postgres")) return "PostgreSQL";
    if (lowerClassName.contains("gremlin")) return "Gremlin";

    return null;
  }

  @Override
  public void close() throws IOException {
    LogManager.instance().log(this, Level.FINE, "Closing dedicated class loader for wrapper plugin: %s", pluginName);
    classLoaders.remove(pluginName);
    super.close();
  }

  /**
   * Closes all wrapper plugin class loaders.
   */
  public static void closeAllClassLoaders() {
    for (final WrapperPluginClassLoader classLoader : classLoaders.values()) {
      try {
        classLoader.close();
      } catch (final IOException e) {
        LogManager.instance().log(WrapperPluginClassLoader.class, Level.WARNING,
            "Error closing class loader for plugin: %s", e, classLoader.pluginName);
      }
    }
    classLoaders.clear();
  }

  @Override
  public String toString() {
    return "WrapperPluginClassLoader{pluginName='" + pluginName + "'}";
  }
}
