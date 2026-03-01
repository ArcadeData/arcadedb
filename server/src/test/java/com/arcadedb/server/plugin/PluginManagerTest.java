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
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * Test for PluginManager to verify plugin discovery and loading with isolated class loaders.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PluginManagerTest {
  private ArcadeDBServer server;
  private PluginManager  pluginManager;

  @TempDir
  Path tempDir;

  @BeforeEach
  void setup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, tempDir.toString());
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());
    configuration.setValue(GlobalConfiguration.SERVER_PLUGINS,
        TestPlugin1.class.getSimpleName() + "," +
            TestPlugin2.class.getSimpleName() + "," +
            LifecycleTestPlugin.class.getSimpleName() + "," +
            AfterHttpPlugin.class.getSimpleName() + "," +
            FailingPlugin.class.getSimpleName() + "," +
            OrderTestPlugin1.class.getSimpleName() + "," +
            OrderTestPlugin2.class.getSimpleName() + "," +
            OrderTestPlugin3.class.getSimpleName() + "," +
            BeforeHttpPlugin.class.getSimpleName());

    server = new ArcadeDBServer(configuration);
    pluginManager = new PluginManager(server, configuration);
  }

  @AfterEach
  void teardown() {
    if (pluginManager != null) {
      pluginManager.stopPlugins();
    }
    if (server != null && server.isStarted()) {
      server.stop();
    }
  }

  @Test
  void pluginManagerCreation() {
    assertThat(pluginManager).isNotNull();
    assertThat(pluginManager.getPluginCount()).isEqualTo(0);
  }

  @Test
  void discoverPluginsWithNoDirectory() {
    // Should handle missing plugins directory gracefully and still discover main classpath plugins
    pluginManager.discoverPlugins();
    // AutoBackupSchedulerPlugin is registered via META-INF/services on the main classpath
    assertThat(pluginManager.getPluginCount()).isGreaterThanOrEqualTo(1);
    assertThat(pluginManager.getPluginNames()).contains("AutoBackupSchedulerPlugin");
  }

  @Test
  void getPluginNames() {
    final Collection<String> names = pluginManager.getPluginNames();
    assertThat(names).isNotNull();
    assertThat(names.isEmpty()).isTrue();
  }

  @Test
  void getPlugins() {
    final Collection<ServerPlugin> plugins = pluginManager.getPlugins();
    assertThat(plugins).isNotNull();
    assertThat(plugins.isEmpty()).isTrue();
  }

  @Test
  void stopPluginsWhenEmpty() {
    // Should handle stopping with no plugins loaded
    Assertions.assertThatCode(() -> pluginManager.stopPlugins()).doesNotThrowAnyException();
  }

  @Test
  void discoverPluginsWithEmptyDirectory() throws Exception {
    // Create empty plugins directory
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    pluginManager.discoverPlugins();
    // Only main classpath plugins (AutoBackupSchedulerPlugin) should be found
    assertThat(pluginManager.getPluginCount()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void loadPluginWithMetaInfServices() throws Exception {
    // Create a test plugin JAR with proper META-INF/services
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    final File pluginJar = createTestPluginJar(pluginsDir, "test-plugin", TestPlugin1.class);

    pluginManager.discoverPlugins();

    // +1 for AutoBackupSchedulerPlugin discovered on main classpath
    assertThat(pluginManager.getPluginCount()).isEqualTo(2);
    assertThat(pluginManager.getPluginNames()).contains(TestPlugin1.class.getSimpleName());

    final Collection<ServerPlugin> plugins = pluginManager.getPlugins();
    assertThat(plugins.size()).isEqualTo(2);
  }

  @Test
  void loadMultiplePlugins() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "plugin1", TestPlugin1.class);
    createTestPluginJar(pluginsDir, "plugin2", TestPlugin2.class);

    pluginManager.discoverPlugins();

    // +1 for AutoBackupSchedulerPlugin discovered on main classpath
    assertThat(pluginManager.getPluginCount()).isEqualTo(3);
    final Set<String> names = pluginManager.getPluginNames();
    assertThat(names).contains(TestPlugin1.class.getSimpleName());
    assertThat(names).contains(TestPlugin2.class.getSimpleName());
  }

  @Test
  void pluginLifecycle() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "lifecycle-plugin", LifecycleTestPlugin.class);

    pluginManager.discoverPlugins();
    // +1 for AutoBackupSchedulerPlugin discovered on main classpath
    assertThat(pluginManager.getPluginCount()).isEqualTo(2);

    // Start the plugin
    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON);

    // Verify plugin was configured and started
    final PluginDescriptor descriptor = pluginManager.getPluginDescriptor(LifecycleTestPlugin.class.getSimpleName());
    assertThat(descriptor).isNotNull();
    assertThat(descriptor.isStarted()).isTrue();
    assertThat(descriptor.getPluginInstance()).isInstanceOf(LifecycleTestPlugin.class);

    final LifecycleTestPlugin plugin = (LifecycleTestPlugin) descriptor.getPluginInstance();
    assertThat(plugin.configured.get()).isTrue();
    assertThat(plugin.started.get()).isTrue();
    assertThat(plugin.stopped.get()).isFalse();

    // Stop the plugin
    pluginManager.stopPlugins();
    assertThat(plugin.stopped.get()).isTrue();
    assertThat(descriptor.isStarted()).isFalse();
  }

  @Test
  void pluginStartOrderByPriority() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "before-plugin", BeforeHttpPlugin.class);
    createTestPluginJar(pluginsDir, "after-plugin", AfterHttpPlugin.class);

    pluginManager.discoverPlugins();
    // +1 for AutoBackupSchedulerPlugin discovered on main classpath
    assertThat(pluginManager.getPluginCount()).isEqualTo(3);

    // Start BEFORE_HTTP_ON plugins
    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON);

    PluginDescriptor beforeDesc = pluginManager.getPluginDescriptor(BeforeHttpPlugin.class.getSimpleName());
    PluginDescriptor afterDesc = pluginManager.getPluginDescriptor(AfterHttpPlugin.class.getSimpleName());

    assertThat(beforeDesc.isStarted()).isTrue();
    assertThat(afterDesc.isStarted()).isFalse();

    // Start AFTER_HTTP_ON plugins
    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.AFTER_HTTP_ON);

    assertThat(beforeDesc.isStarted()).isTrue();
    assertThat(afterDesc.isStarted()).isTrue();
  }

  @Test
  void pluginWithoutMetaInfServices() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    // Create JAR without META-INF/services
    final File pluginJar = pluginsDir.resolve("invalid-plugin.jar").toFile();
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(pluginJar))) {
      // Just create an empty JAR
      jos.putNextEntry(new JarEntry("dummy.txt"));
      jos.write("test".getBytes());
      jos.closeEntry();
    }

    pluginManager.discoverPlugins();

    // JAR has no META-INF/services, but AutoBackupSchedulerPlugin is on main classpath
    assertThat(pluginManager.getPluginCount()).isEqualTo(1);
  }

  @Test
  void pluginStartException() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "failing-plugin", FailingPlugin.class);

    pluginManager.discoverPlugins();
    // +1 for AutoBackupSchedulerPlugin discovered on main classpath
    assertThat(pluginManager.getPluginCount()).isEqualTo(2);

    // Starting the plugin should throw exception
    assertThatExceptionOfType(ServerException.class).isThrownBy(() ->
        pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON));
  }

  @Test
  void getPluginDescriptor() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "test-plugin", TestPlugin1.class);

    pluginManager.discoverPlugins();

    final PluginDescriptor descriptor = pluginManager.getPluginDescriptor(TestPlugin1.class.getSimpleName());
    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPluginName()).isEqualTo(TestPlugin1.class.getSimpleName());
    assertThat(descriptor.getClassLoader()).isNotNull();
    assertThat(descriptor.getPluginInstance()).isNotNull();
    assertThat(descriptor.isStarted()).isFalse();
  }

  @Test
  void registerPluginStoresInstance() {
    final TestPlugin1 plugin = new TestPlugin1();
    pluginManager.registerPlugin("manual-plugin", plugin);

    assertThat(pluginManager.getPluginCount()).isEqualTo(1);
    assertThat(pluginManager.getPluginNames()).contains("manual-plugin");

    final PluginDescriptor descriptor = pluginManager.getPluginDescriptor("manual-plugin");
    assertThat(descriptor).isNotNull();
    assertThat(descriptor.getPluginInstance()).isSameAs(plugin);

    final Collection<ServerPlugin> plugins = pluginManager.getPlugins();
    assertThat(plugins).hasSize(1);
    assertThat(plugins).contains(plugin);
  }

  @Test
  void classLoaderIsolation() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "plugin1", TestPlugin1.class);
    createTestPluginJar(pluginsDir, "plugin2", TestPlugin2.class);

    pluginManager.discoverPlugins();

    final PluginDescriptor desc1 = pluginManager.getPluginDescriptor(TestPlugin1.class.getSimpleName());
    final PluginDescriptor desc2 = pluginManager.getPluginDescriptor(TestPlugin2.class.getSimpleName());

    // Each plugin should have its own class loader
    assertThat(desc1.getClassLoader()).isNotNull();
    assertThat(desc2.getClassLoader()).isNotNull();
    Assertions.assertThat(desc2.getClassLoader()).isNotSameAs(desc1.getClassLoader());

    // Both should be PluginClassLoader instances
    assertThat(desc1.getClassLoader()).isInstanceOf(PluginClassLoader.class);
    assertThat(desc2.getClassLoader()).isInstanceOf(PluginClassLoader.class);
  }

  /**
   * Helper method to create a test plugin JAR with proper META-INF/services
   */
  private File createTestPluginJar(final Path pluginsDir,
      final String pluginName,
      final Class<? extends ServerPlugin> pluginClass)
      throws Exception {
    final File jarFile = pluginsDir.resolve(pluginName + ".jar").toFile();

    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
      // Add the plugin class
      final String classFileName = pluginClass.getName().replace('.', '/') + ".class";
      jos.putNextEntry(new JarEntry(classFileName));

      // Load class bytes from current classloader
      try (InputStream is = getClass().getClassLoader().getResourceAsStream(classFileName)) {
        if (is != null) {
          is.transferTo(jos);
        }
      }
      jos.closeEntry();

      // Add META-INF/services/com.arcadedb.server.ServerPlugin
      jos.putNextEntry(new JarEntry("META-INF/services/com.arcadedb.server.ServerPlugin"));
      jos.write(pluginClass.getName().getBytes());
      jos.closeEntry();
    }

    return jarFile;
  }

  // Test plugin implementations
  public static class TestPlugin1 implements ServerPlugin {
    @Override
    public void startService() {
    }
  }

  public static class TestPlugin2 implements ServerPlugin {
    @Override
    public void startService() {
    }
  }

  public static class LifecycleTestPlugin implements ServerPlugin {
    public final AtomicBoolean configured = new AtomicBoolean(false);
    public final AtomicBoolean started    = new AtomicBoolean(false);
    public final AtomicBoolean stopped    = new AtomicBoolean(false);

    @Override
    public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
      configured.set(true);
    }

    @Override
    public void startService() {
      started.set(true);
    }

    @Override
    public void stopService() {
      stopped.set(true);
    }
  }

  public static class BeforeHttpPlugin implements ServerPlugin {
    @Override
    public void startService() {
    }

    @Override
    public PluginInstallationPriority getInstallationPriority() {
      return PluginInstallationPriority.BEFORE_HTTP_ON;
    }
  }

  public static class AfterHttpPlugin implements ServerPlugin {
    @Override
    public void startService() {
    }

    @Override
    public PluginInstallationPriority getInstallationPriority() {
      return PluginInstallationPriority.AFTER_HTTP_ON;
    }
  }

  public static class FailingPlugin implements ServerPlugin {
    @Override
    public void startService() {
      throw new RuntimeException("Plugin failed to start");
    }
  }

  public static class OrderTestPlugin1 implements ServerPlugin {
    public static final AtomicInteger stopCounter = new AtomicInteger(0);
    public static final AtomicInteger stopOrder   = new AtomicInteger(0);

    @Override
    public void startService() {
    }

    @Override
    public void stopService() {
      stopOrder.set(stopCounter.incrementAndGet());
    }
  }

  public static class OrderTestPlugin2 implements ServerPlugin {
    public static final AtomicInteger stopCounter = new AtomicInteger(0);
    public static final AtomicInteger stopOrder   = new AtomicInteger(0);

    @Override
    public void startService() {
    }

    @Override
    public void stopService() {
      stopOrder.set(stopCounter.incrementAndGet());
    }
  }

  public static class OrderTestPlugin3 implements ServerPlugin {
    public static final AtomicInteger stopCounter = new AtomicInteger(0);
    public static final AtomicInteger stopOrder   = new AtomicInteger(0);

    @Override
    public void startService() {
    }

    @Override
    public void stopService() {
      stopOrder.set(stopCounter.incrementAndGet());
    }
  }
}
