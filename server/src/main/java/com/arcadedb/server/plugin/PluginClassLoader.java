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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

/**
 * Custom class loader for plugins that provides isolation while allowing access to server APIs.
 * <p>
 * This class loader follows a parent-first delegation model for server classes (com.arcadedb.server.*)
 * and a child-first model for plugin-specific classes, allowing each plugin to have its own
 * version of dependencies.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PluginClassLoader extends URLClassLoader {
  private static final String SERVER_PACKAGE_PREFIX = "com.arcadedb.";

  public PluginClassLoader(final String pluginName, final File pluginJarFile, final ClassLoader parent)
      throws MalformedURLException {
    super(new URL[]{pluginJarFile.toURI().toURL()}, parent);
  }

  @Override
  protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
    // Always delegate server API classes to parent to ensure shared instances
    if (name.startsWith(SERVER_PACKAGE_PREFIX)) {
      return super.loadClass(name, resolve);
    }

    // For plugin classes, try to load from this class loader first
    synchronized (getClassLoadingLock(name)) {
      // Check if the class has already been loaded by this class loader
      Class<?> c = findLoadedClass(name);
      if (c == null) {
        try {
          // Try to load from this class loader's JAR first
          c = findClass(name);
        } catch (final ClassNotFoundException e) {
          // If not found, delegate to parent
          c = super.loadClass(name, resolve);
        }
      }
      if (resolve) {
        resolveClass(c);
      }
      return c;
    }
  }

  @Override
  public String toString() {
    return "PluginClassLoader{urls=" + Arrays.toString(getURLs()) + "}";
  }
}
