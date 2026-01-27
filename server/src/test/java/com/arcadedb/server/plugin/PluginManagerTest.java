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
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for PluginManager to verify plugin discovery and loading with isolated class loaders.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PluginManagerTest {
  private ArcadeDBServer server;
  private PluginManager  pluginManager;

  @TempDir
  Path tempDir;

  @BeforeEach
  public void setup() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, tempDir.toString());
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, tempDir.resolve("databases").toString());

    server = new ArcadeDBServer(configuration);
    pluginManager = new PluginManager(server, configuration);
  }

  @AfterEach
  public void teardown() {
    if (pluginManager != null) {
      pluginManager.stopPlugins();
    }
    if (server != null && server.isStarted()) {
      server.stop();
    }
  }

  @Test
  public void testPluginManagerCreation() {
    assertNotNull(pluginManager);
    assertEquals(0, pluginManager.getPluginCount());
  }

  @Test
  public void testDiscoverPluginsWithNoDirectory() {
    // Should handle missing plugins directory gracefully
    pluginManager.discoverPlugins();
    assertEquals(0, pluginManager.getPluginCount());
  }

  @Test
  public void testGetPluginNames() {
    final Collection<String> names = pluginManager.getPluginNames();
    assertNotNull(names);
    assertTrue(names.isEmpty());
  }

  @Test
  public void testGetPlugins() {
    final Collection<ServerPlugin> plugins = pluginManager.getPlugins();
    assertNotNull(plugins);
    assertTrue(plugins.isEmpty());
  }

  @Test
  public void testStopPluginsWhenEmpty() {
    // Should handle stopping with no plugins loaded
    assertDoesNotThrow(() -> pluginManager.stopPlugins());
  }

  @Test
  public void testDiscoverPluginsWithEmptyDirectory() throws IOException {
    // Create empty plugins directory
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    pluginManager.discoverPlugins();
    assertEquals(0, pluginManager.getPluginCount());
  }

  @Test
  public void testLoadPluginWithMetaInfServices() throws Exception {
    // Create a test plugin JAR with proper META-INF/services
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    final File pluginJar = createTestPluginJar(pluginsDir, "test-plugin", TestPlugin1.class);

    pluginManager.discoverPlugins();

    assertEquals(1, pluginManager.getPluginCount());
    assertTrue(pluginManager.getPluginNames().contains("test-plugin"));

    final Collection<ServerPlugin> plugins = pluginManager.getPlugins();
    assertEquals(1, plugins.size());
  }

  @Test
  public void testLoadMultiplePlugins() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "plugin1", TestPlugin1.class);
    createTestPluginJar(pluginsDir, "plugin2", TestPlugin2.class);

    pluginManager.discoverPlugins();

    assertEquals(2, pluginManager.getPluginCount());
    final Set<String> names = pluginManager.getPluginNames();
    assertTrue(names.contains("plugin1"));
    assertTrue(names.contains("plugin2"));
  }

  @Test
  public void testPluginLifecycle() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "lifecycle-plugin", LifecycleTestPlugin.class);

    pluginManager.discoverPlugins();
    assertEquals(1, pluginManager.getPluginCount());

    // Start the plugin
    pluginManager.startPlugins(ServerPlugin.INSTALLATION_PRIORITY.BEFORE_HTTP_ON);

    // Verify plugin was configured and started
    final PluginDescriptor descriptor = pluginManager.getPluginDescriptor("lifecycle-plugin");
    assertNotNull(descriptor);
    assertTrue(descriptor.isStarted());
    assertTrue(descriptor.getPluginInstance() instanceof LifecycleTestPlugin);

    final LifecycleTestPlugin plugin = (LifecycleTestPlugin) descriptor.getPluginInstance();
    assertTrue(plugin.configured.get());
    assertTrue(plugin.started.get());
    assertFalse(plugin.stopped.get());

    // Stop the plugin
    pluginManager.stopPlugins();
    assertTrue(plugin.stopped.get());
    assertFalse(descriptor.isStarted());
  }

  @Test
  public void testPluginStartOrderByPriority() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "before-plugin", BeforeHttpPlugin.class);
    createTestPluginJar(pluginsDir, "after-plugin", AfterHttpPlugin.class);

    pluginManager.discoverPlugins();
    assertEquals(2, pluginManager.getPluginCount());

    // Start BEFORE_HTTP_ON plugins
    pluginManager.startPlugins(ServerPlugin.INSTALLATION_PRIORITY.BEFORE_HTTP_ON);

    PluginDescriptor beforeDesc = pluginManager.getPluginDescriptor("before-plugin");
    PluginDescriptor afterDesc = pluginManager.getPluginDescriptor("after-plugin");

    assertTrue(beforeDesc.isStarted());
    assertFalse(afterDesc.isStarted());

    // Start AFTER_HTTP_ON plugins
    pluginManager.startPlugins(ServerPlugin.INSTALLATION_PRIORITY.AFTER_HTTP_ON);

    assertTrue(beforeDesc.isStarted());
    assertTrue(afterDesc.isStarted());
  }

  @Test
  public void testPluginWithoutMetaInfServices() throws Exception {
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

    // Plugin should not be loaded due to missing META-INF/services
    assertEquals(0, pluginManager.getPluginCount());
  }

  @Test
  public void testPluginStartException() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "failing-plugin", FailingPlugin.class);

    pluginManager.discoverPlugins();
    assertEquals(1, pluginManager.getPluginCount());

    // Starting the plugin should throw exception
    assertThrows(ServerException.class, () ->
        pluginManager.startPlugins(ServerPlugin.INSTALLATION_PRIORITY.BEFORE_HTTP_ON));
  }

  @Test
  public void testGetPluginDescriptor() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "test-plugin", TestPlugin1.class);

    pluginManager.discoverPlugins();

    final PluginDescriptor descriptor = pluginManager.getPluginDescriptor("test-plugin");
    assertNotNull(descriptor);
    assertEquals("test-plugin", descriptor.getPluginName());
    assertNotNull(descriptor.getPluginJarFile());
    assertNotNull(descriptor.getClassLoader());
    assertNotNull(descriptor.getPluginInstance());
    assertFalse(descriptor.isStarted());
  }

  @Test
  public void testClassLoaderIsolation() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "plugin1", TestPlugin1.class);
    createTestPluginJar(pluginsDir, "plugin2", TestPlugin2.class);

    pluginManager.discoverPlugins();

    final PluginDescriptor desc1 = pluginManager.getPluginDescriptor("plugin1");
    final PluginDescriptor desc2 = pluginManager.getPluginDescriptor("plugin2");

    // Each plugin should have its own class loader
    assertNotNull(desc1.getClassLoader());
    assertNotNull(desc2.getClassLoader());
    assertNotSame(desc1.getClassLoader(), desc2.getClassLoader());

    // Both should be PluginClassLoader instances
    assertTrue(desc1.getClassLoader() instanceof PluginClassLoader);
    assertTrue(desc2.getClassLoader() instanceof PluginClassLoader);
  }

  @Test
  public void testStopPluginsReverseOrder() throws Exception {
    final Path pluginsDir = tempDir.resolve("lib/plugins");
    Files.createDirectories(pluginsDir);

    createTestPluginJar(pluginsDir, "plugin1", OrderTestPlugin1.class);
    createTestPluginJar(pluginsDir, "plugin2", OrderTestPlugin2.class);
    createTestPluginJar(pluginsDir, "plugin3", OrderTestPlugin3.class);

    OrderTestPlugin1.stopOrder.set(0);
    OrderTestPlugin2.stopOrder.set(0);
    OrderTestPlugin3.stopOrder.set(0);
    OrderTestPlugin1.stopCounter.set(0);
    OrderTestPlugin2.stopCounter.set(0);
    OrderTestPlugin3.stopCounter.set(0);

    pluginManager.discoverPlugins();
    pluginManager.startPlugins(ServerPlugin.INSTALLATION_PRIORITY.BEFORE_HTTP_ON);

    // Stop plugins - should be in reverse order of discovery
    pluginManager.stopPlugins();

    // Verify plugins were stopped in reverse order
    assertTrue(OrderTestPlugin3.stopOrder.get() < OrderTestPlugin2.stopOrder.get());
    assertTrue(OrderTestPlugin2.stopOrder.get() < OrderTestPlugin1.stopOrder.get());
  }

  /**
   * Helper method to create a test plugin JAR with proper META-INF/services
   */
  private File createTestPluginJar(final Path pluginsDir, final String pluginName, final Class<? extends ServerPlugin> pluginClass)
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
    public final AtomicBoolean started = new AtomicBoolean(false);
    public final AtomicBoolean stopped = new AtomicBoolean(false);

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
    public INSTALLATION_PRIORITY getInstallationPriority() {
      return INSTALLATION_PRIORITY.BEFORE_HTTP_ON;
    }
  }

  public static class AfterHttpPlugin implements ServerPlugin {
    @Override
    public void startService() {
    }

    @Override
    public INSTALLATION_PRIORITY getInstallationPriority() {
      return INSTALLATION_PRIORITY.AFTER_HTTP_ON;
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
    public static final AtomicInteger stopOrder = new AtomicInteger(0);

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
    public static final AtomicInteger stopOrder = new AtomicInteger(0);

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
    public static final AtomicInteger stopOrder = new AtomicInteger(0);

    @Override
    public void startService() {
    }

    @Override
    public void stopService() {
      stopOrder.set(stopCounter.incrementAndGet());
    }
  }
}
