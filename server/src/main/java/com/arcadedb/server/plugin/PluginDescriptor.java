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

import com.arcadedb.server.ServerPlugin;

import java.util.Objects;

/**
 * Descriptor for a plugin that provides metadata and lifecycle management.
 * Each plugin is loaded in its own class loader for isolation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PluginDescriptor {
  private final String       pluginName;
  private final ClassLoader  classLoader;
  private       ServerPlugin pluginInstance;
  private       boolean      started;

  public PluginDescriptor(final String pluginName, final ClassLoader classLoader) {
    this.pluginName = Objects.requireNonNull(pluginName, "Plugin name cannot be null");
    this.classLoader = Objects.requireNonNull(classLoader, "Class loader cannot be null");
    this.started = false;
  }

  public String getPluginName() {
    return pluginName;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public ServerPlugin getPluginInstance() {
    return pluginInstance;
  }

  public void setPluginInstance(final ServerPlugin pluginInstance) {
    this.pluginInstance = pluginInstance;
  }

  public boolean isStarted() {
    return started;
  }

  public void setStarted(final boolean started) {
    this.started = started;
  }

  @Override
  public String toString() {
    return "PluginDescriptor{" +
        "pluginName='" + pluginName + '\'' +
        ", started=" + started +
        '}';
  }
}
