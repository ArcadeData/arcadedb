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
  // WHETHER configure() HAS RETURNED, I.E. WHETHER THE PLUGIN HOLDS ITS SERVER REFERENCE YET. `started` CANNOT ANSWER
  // THAT QUESTION: IT IS ONLY SET AFTER startService() RETURNS AND ONLY WHEN THE PLUGIN REPORTS ITSELF ACTIVE, SO A
  // PLUGIN REGISTERING ITS OWN DATABASES FROM startService() WOULD BE HIDDEN FROM ITS OWN CALLBACKS (ISSUE #6852).
  // VOLATILE, UNLIKE `started`: THIS ONE IS WRITTEN BY THE STARTING THREAD AND READ BY WHICHEVER THREAD REGISTERS A
  // DATABASE, WHICH IS ANY HTTP OR HA THREAD. startPlugins() DELIBERATELY DOES NOT HOLD THE `plugins` MONITOR ACROSS
  // THE PLUGIN LIFECYCLE CALLS (THEY CAN BLOCK), SO THE MONITOR getInitializedPlugins() TAKES ORDERS NOTHING AGAINST
  // THIS WRITE, AND A READER SEEING A STALE false WOULD SILENTLY DROP THE CALLBACK ISSUE #6752 EXISTS TO DELIVER
  private volatile boolean   initialized;

  public PluginDescriptor(final String pluginName, final ClassLoader classLoader) {
    this.pluginName = Objects.requireNonNull(pluginName, "Plugin name cannot be null");
    this.classLoader = Objects.requireNonNull(classLoader, "Class loader cannot be null");
    this.started = false;
    this.initialized = false;
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

  /**
   * Returns true once {@link ServerPlugin#configure} has returned for this plugin, i.e. once it may be handed the
   * server's lifecycle callbacks.
   */
  public boolean isInitialized() {
    return initialized;
  }

  public void setInitialized(final boolean initialized) {
    this.initialized = initialized;
  }

  @Override
  public String toString() {
    return "PluginDescriptor{" +
        "pluginName='" + pluginName + '\'' +
        ", initialized=" + initialized +
        ", started=" + started +
        '}';
  }
}
