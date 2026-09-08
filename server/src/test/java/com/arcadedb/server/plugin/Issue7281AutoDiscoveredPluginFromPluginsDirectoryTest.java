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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Second half of issue #7281: {@link PluginManager} has two loaders, and only one of them used to ask a plugin
 * whether it wants to be auto-discovered.
 * <p>
 * {@code discoverPluginsOnMainClassLoader} gates on {@code configured || isAutoDiscovered(configuration)}, while
 * {@code loadPlugin} - the isolated-class-loader path for a jar dropped in {@code lib/plugins}, which
 * {@code PLUGINS.md} documents as the supported way to add one - gated on the configuration entry alone. So the
 * same jar that auto-installed from {@code lib} silently did nothing from {@code lib/plugins}, and
 * {@link ServerPlugin#isAutoDiscovered} promised activation "on classpath presence alone" without saying that the
 * promise held for only one of the two ways onto the classpath.
 */
class Issue7281AutoDiscoveredPluginFromPluginsDirectoryTest {

  @TempDir
  Path tempDir;

  private PluginManager pluginManager;

  @BeforeEach
  void setup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, tempDir.toString());
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    // Nothing is named: the plugin's own answer is the only thing that can install it.
    configuration.setValue(GlobalConfiguration.SERVER_PLUGINS, "");

    pluginManager = new PluginManager(new ArcadeDBServer(configuration), configuration);
  }

  @Test
  void anAutoDiscoveredPluginInThePluginsDirectoryIsInstalledWithoutBeingNamed() throws Exception {
    installJar("auto-discovered-plugin", AutoDiscoveredPlugin.class);

    pluginManager.discoverPlugins();

    assertThat(pluginManager.getPluginNames()).contains(AutoDiscoveredPlugin.class.getSimpleName());
  }

  /**
   * The gate still is a gate: the default answer is {@code false}, and a plugin that keeps it stays opt-in.
   */
  @Test
  void aPluginThatDoesNotAutoDiscoverIsStillSkipped() throws Exception {
    installJar("opt-in-plugin", OptInPlugin.class);

    pluginManager.discoverPlugins();

    assertThat(pluginManager.getPluginNames()).doesNotContain(OptInPlugin.class.getSimpleName());
  }

  private void installJar(final String jarName, final Class<? extends ServerPlugin> pluginClass) throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    final File jarFile = pluginsDir.resolve(jarName + ".jar").toFile();
    try (final JarOutputStream jar = new JarOutputStream(new FileOutputStream(jarFile))) {
      final String classFileName = pluginClass.getName().replace('.', '/') + ".class";
      jar.putNextEntry(new JarEntry(classFileName));
      try (final InputStream is = getClass().getClassLoader().getResourceAsStream(classFileName)) {
        assertThat(is).as("class bytes of %s", pluginClass.getName()).isNotNull();
        is.transferTo(jar);
      }
      jar.closeEntry();

      jar.putNextEntry(new JarEntry("META-INF/services/com.arcadedb.server.ServerPlugin"));
      jar.write(pluginClass.getName().getBytes(StandardCharsets.UTF_8));
      jar.closeEntry();
    }
  }

  /** Mirrors what a distribution plugin does: it decides for itself, the way {@code MCPPlugin} does. */
  public static class AutoDiscoveredPlugin implements ServerPlugin {
    @Override
    public boolean isAutoDiscovered(final ContextConfiguration configuration) {
      return true;
    }

    @Override
    public void startService() {
      // NO-OP
    }
  }

  public static class OptInPlugin implements ServerPlugin {
    @Override
    public void startService() {
      // NO-OP
    }
  }
}
