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
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.UnifiedChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9749, follow-up of #9639: the start refuses a channelizer that cannot conceal engine error text in production
 * mode, but {@code arcadedb.server.mode} can also be switched at runtime. While the Gremlin Server runs with such a
 * channelizer the plugin now refuses that switch, so the server cannot end up in production mode with a Gremlin port that
 * answers with {@code Throwable.getMessage()}.
 */
class Issue9749GremlinRuntimeProductionModeTest {
  @TempDir
  Path root;

  private GremlinServerPlugin newPlugin(final String channelizer) {
    return newPlugin(channelizer, StaticBaseServerTest.allocateFreePorts(1)[0]);
  }

  private GremlinServerPlugin newPlugin(final String channelizer, final int port) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, "development");
    if (channelizer != null)
      configuration.setValue("gremlin.channelizer", channelizer);
    configuration.setValue("gremlin.host", "127.0.0.1");
    configuration.setValue("gremlin.port", String.valueOf(port));
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, configuration);
    final GremlinServerPlugin plugin = new GremlinServerPlugin();
    plugin.configure(server, configuration);
    return plugin;
  }

  @ParameterizedTest
  @ValueSource(classes = { WsAndHttpChannelizer.class, HttpChannelizer.class, UnifiedChannelizer.class })
  void aRunningChannelizerThatCannotConcealRefusesTheSwitchToProduction(final Class<?> channelizer) {
    final GremlinServerPlugin plugin = newPlugin(channelizer.getName());
    try {
      plugin.startService();

      assertThatThrownBy(() -> plugin.checkServerSettingChange(GlobalConfiguration.SERVER_MODE, "production"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(channelizer.getName())
          .hasMessageContaining(GlobalConfiguration.SERVER_MODE.getKey())
          .hasMessageContaining(WebSocketChannelizer.class.getName());
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void aRunningChannelizerThatCannotConcealAcceptsEveryOtherChange() {
    final GremlinServerPlugin plugin = newPlugin(WsAndHttpChannelizer.class.getName());
    try {
      plugin.startService();

      assertThatCode(() -> plugin.checkServerSettingChange(GlobalConfiguration.SERVER_MODE, "development")).doesNotThrowAnyException();
      assertThatCode(() -> plugin.checkServerSettingChange(GlobalConfiguration.SERVER_MODE, "test")).doesNotThrowAnyException();
      assertThatCode(() -> plugin.checkServerSettingChange(GlobalConfiguration.SERVER_NAME, "production")).doesNotThrowAnyException();
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void theDefaultChannelizerAcceptsTheSwitchToProduction() {
    // It conceals per message, reading the mode for every answer, so a runtime switch takes effect on it at once (#9317)
    for (final String channelizer : new String[] { null, WebSocketChannelizer.class.getName(),
        ConcealingWebSocketChannelizer.class.getName() }) {
      final GremlinServerPlugin plugin = newPlugin(channelizer);
      try {
        plugin.startService();
        assertThatCode(() -> plugin.checkServerSettingChange(GlobalConfiguration.SERVER_MODE, "production"))
            .as("channelizer %s", channelizer).doesNotThrowAnyException();
      } finally {
        plugin.stopService();
      }
    }
  }

  @Test
  void aStoppedPluginNoLongerRefusesTheSwitch() {
    final GremlinServerPlugin plugin = newPlugin(HttpChannelizer.class.getName());
    plugin.startService();
    plugin.stopService();

    assertThatCode(() -> plugin.checkServerSettingChange(GlobalConfiguration.SERVER_MODE, "production")).doesNotThrowAnyException();
  }

  @Test
  void aStartThatFailedToBindNoLongerRefusesTheSwitch() throws Exception {
    // The channelizer is recorded before the server is built, so a failed start must release it with the server
    final int port = StaticBaseServerTest.allocateFreePorts(1)[0];
    final GremlinServerPlugin plugin = newPlugin(WsAndHttpChannelizer.class.getName(), port);
    try (final ServerSocket holder = new ServerSocket()) {
      holder.setReuseAddress(false);
      holder.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port));

      assertThatThrownBy(plugin::startService).isInstanceOf(ServerException.class);

      assertThatCode(() -> plugin.checkServerSettingChange(GlobalConfiguration.SERVER_MODE, "production"))
          .as("nothing serves with the channelizer").doesNotThrowAnyException();
    } finally {
      plugin.stopService();
    }
  }

  @Test
  void aPluginThatNeverStartedDoesNotRefuseTheSwitch() {
    assertThatCode(() -> newPlugin(HttpChannelizer.class.getName()).checkServerSettingChange(GlobalConfiguration.SERVER_MODE,
        "production")).doesNotThrowAnyException();
  }
}
