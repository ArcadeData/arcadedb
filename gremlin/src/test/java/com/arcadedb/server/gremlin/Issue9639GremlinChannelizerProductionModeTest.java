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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.UnifiedChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9639: production-mode concealment on the Gremlin Server is installed by swapping the default WebSocket channelizer
 * for {@link ConcealingWebSocketChannelizer}. Any other channelizer (WsAndHttp, Http, the deprecated Unified one, a custom
 * class) answers with the engine's own text, stored key values included, so in production mode it now fails the start
 * instead of logging a WARNING and serving the leak.
 */
class Issue9639GremlinChannelizerProductionModeTest {
  @TempDir
  Path root;

  // ---------------------------------------------------------------------------------------------
  // The decision itself
  // ---------------------------------------------------------------------------------------------

  @Test
  void theDefaultChannelizerIsSwappedForTheConcealingOneInEveryMode() {
    assertThat(GremlinServerPlugin.resolveChannelizer(WebSocketChannelizer.class.getName(), true))
        .isEqualTo(ConcealingWebSocketChannelizer.class.getName());
    assertThat(GremlinServerPlugin.resolveChannelizer(WebSocketChannelizer.class.getName(), false))
        .isEqualTo(ConcealingWebSocketChannelizer.class.getName());
  }

  @Test
  void anAbsentChannelizerIsTheDefaultOne() {
    assertThat(GremlinServerPlugin.resolveChannelizer(null, true)).isEqualTo(ConcealingWebSocketChannelizer.class.getName());
    assertThat(GremlinServerPlugin.resolveChannelizer("  ", true)).isEqualTo(ConcealingWebSocketChannelizer.class.getName());
  }

  @Test
  void theConcealingChannelizerNamedExplicitlyIsKept() {
    assertThat(GremlinServerPlugin.resolveChannelizer(ConcealingWebSocketChannelizer.class.getName(), true))
        .isEqualTo(ConcealingWebSocketChannelizer.class.getName());
  }

  @ParameterizedTest
  @ValueSource(classes = { WsAndHttpChannelizer.class, HttpChannelizer.class, UnifiedChannelizer.class })
  void aChannelizerThatCannotConcealFailsTheStartInProductionMode(final Class<?> channelizer) {
    assertThatThrownBy(() -> GremlinServerPlugin.resolveChannelizer(channelizer.getName(), true))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining(channelizer.getName())
        .hasMessageContaining("production")
        .hasMessageContaining("gremlin.channelizer");
  }

  @Test
  void aCustomChannelizerFailsTheStartInProductionMode() {
    assertThatThrownBy(() -> GremlinServerPlugin.resolveChannelizer("com.example.MyChannelizer", true))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("com.example.MyChannelizer");
  }

  @Test
  void aChannelizerNameIsTrimmed() {
    assertThat(GremlinServerPlugin.resolveChannelizer(" " + WebSocketChannelizer.class.getName() + " ", true))
        .isEqualTo(ConcealingWebSocketChannelizer.class.getName());
    assertThat(GremlinServerPlugin.resolveChannelizer(" " + HttpChannelizer.class.getName() + " ", false))
        .isEqualTo(HttpChannelizer.class.getName());
  }

  @ParameterizedTest
  @ValueSource(classes = { WsAndHttpChannelizer.class, HttpChannelizer.class, UnifiedChannelizer.class })
  void aChannelizerThatCannotConcealIsKeptOutsideProductionMode(final Class<?> channelizer) {
    assertThat(GremlinServerPlugin.resolveChannelizer(channelizer.getName(), false)).isEqualTo(channelizer.getName());
  }

  // ---------------------------------------------------------------------------------------------
  // The two ways an operator selects a channelizer, driven through startService()
  // ---------------------------------------------------------------------------------------------

  private GremlinServerPlugin newPlugin(final ContextConfiguration configuration) {
    final int port = StaticBaseServerTest.allocateFreePorts(1)[0];
    configuration.setValue("gremlin.host", "127.0.0.1");
    configuration.setValue("gremlin.port", String.valueOf(port));
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, configuration);
    final GremlinServerPlugin plugin = new GremlinServerPlugin();
    plugin.configure(server, configuration);
    return plugin;
  }

  private static void assertRefused(final GremlinServerPlugin plugin, final String channelizer) {
    // stopService() on a plugin whose start was refused must be a no-op: the PluginManager stops a failed plugin too
    try {
      assertThatThrownBy(plugin::startService).isInstanceOf(ServerException.class).hasMessageContaining(channelizer)
          .hasMessageContaining("production");
      assertThat(plugin.isActive()).as("a refused start leaves the plugin inactive").isFalse();
      assertThat(plugin.getAdvertisedPorts()).as("nothing listens, so nothing is advertised").isEmpty();
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void aChannelizerFromTheServerSettingFailsTheStartInProductionMode() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    configuration.setValue("gremlin.channelizer", WsAndHttpChannelizer.class.getName());

    assertRefused(newPlugin(configuration), WsAndHttpChannelizer.class.getName());
  }

  @Test
  void aChannelizerFromGremlinServerYamlFailsTheStartInProductionMode() throws Exception {
    final File configDir = root.resolve("config9639").toFile();
    configDir.mkdirs();
    FileUtils.writeFile(new File(configDir, "gremlin-server.yaml"), "channelizer: " + HttpChannelizer.class.getName() + "\n");

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    configuration.setValue(GlobalConfiguration.SERVER_CONFIG_DIRECTORY, configDir.getPath());

    assertRefused(newPlugin(configuration), HttpChannelizer.class.getName());
  }

  @Test
  void theDefaultChannelizerStillStartsInProductionMode() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    configuration.setValue("gremlin.channelizer", WebSocketChannelizer.class.getName());
    assertStarts(newPlugin(configuration));
  }

  @Test
  void theConcealingChannelizerNamedExplicitlyStillStartsInProductionMode() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "production");
    configuration.setValue("gremlin.channelizer", ConcealingWebSocketChannelizer.class.getName());
    assertStarts(newPlugin(configuration));
  }

  private static void assertStarts(final GremlinServerPlugin plugin) {
    try {
      plugin.startService();
      assertThat(plugin.isActive()).isTrue();
      assertThat(plugin.getAdvertisedPorts()).containsKey("gremlin");
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void aChannelizerThatCannotConcealStillStartsInDevelopmentMode() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "development");
    configuration.setValue("gremlin.channelizer", WsAndHttpChannelizer.class.getName());
    assertStarts(newPlugin(configuration));
  }
}
