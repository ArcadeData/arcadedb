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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9319: TinkerPop's {@code GremlinServer.start()} reports a failed bind on the future it returns, which the plugin
 * threw away, so a port already in use left the plugin "started" and its port advertised with nothing listening. The start
 * now fails like the other listeners do, and the advertised port is the one the channel is bound to.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9319GremlinBindFailureTest {
  @TempDir
  Path root;

  private GremlinServerPlugin newPlugin(final int port) {
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, new ContextConfiguration());
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue("gremlin.host", "127.0.0.1");
    configuration.setValue("gremlin.port", String.valueOf(port));
    final GremlinServerPlugin plugin = new GremlinServerPlugin();
    plugin.configure(server, configuration);
    return plugin;
  }

  @Test
  void aPortAlreadyInUseFailsTheStartAndAdvertisesNothing() throws Exception {
    final int port = StaticBaseServerTest.allocateFreePorts(1)[0];
    final GremlinServerPlugin plugin = newPlugin(port);
    try (final ServerSocket holder = new ServerSocket()) {
      holder.setReuseAddress(false);
      holder.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port));

      assertThatThrownBy(plugin::startService).isInstanceOf(ServerException.class).hasMessageContaining(String.valueOf(port));

      assertThat(plugin.isActive()).as("a plugin whose bind failed is not active").isFalse();
      assertThat(plugin.getAdvertisedPorts()).as("nothing listens, so nothing is advertised").isEmpty();
      assertThat(gremlinExecutorThreads()).as("a failed start leaves no Gremlin executor thread running").isZero();
      plugin.stopService(); // the PluginManager stops a plugin whose start failed too: it must cope with the released state
    }
  }

  private static long gremlinExecutorThreads() throws InterruptedException {
    long count = 0;
    // shutdownNow() interrupts the workers, which exit asynchronously
    for (int i = 0; i < 50; i++) {
      count = Thread.getAllStackTraces().keySet().stream()
          .filter(t -> t.isAlive() && t.getName().startsWith("arcadedb-gremlin-exec-")).count();
      if (count == 0)
        break;
      Thread.sleep(100);
    }
    return count;
  }

  @Test
  void anEphemeralPortIsAdvertisedAsTheOneTheChannelBoundTo() throws Exception {
    final GremlinServerPlugin plugin = newPlugin(0);
    try {
      plugin.startService();

      assertThat(plugin.isActive()).isTrue();
      final Integer advertised = plugin.getAdvertisedPorts().get("gremlin");
      assertThat(advertised).as("the port the OS chose, not the 0 of the setting").isNotNull().isPositive();
      try (final Socket socket = new Socket(InetAddress.getByName("127.0.0.1"), advertised)) {
        assertThat(socket.isConnected()).isTrue();
      }
    } finally {
      plugin.stopService();
    }
    assertThat(plugin.isActive()).as("not active after stopService()").isFalse();
    assertThat(plugin.getAdvertisedPorts()).isEmpty();
  }
}
