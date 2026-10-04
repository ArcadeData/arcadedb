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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.network.MultiAddressServerSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression test for #9224: with {@code host=localhost} the listener bound the first address of the name only, so a port held on
 * the other family ({@code [::1]}) looked free and {@code localhost:<port>} reached either process. The listener now answers on
 * every local address of the host.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9224PostgresListensOnEveryLocalAddressIT extends PostgresWireProtocolTestBase {
  private ServerSocket squatter;
  private int          squattedPort;
  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
    GlobalConfiguration.POSTGRES_HOST.setValue("localhost");
    // a port taken from the shared allocator, held on [::1] only and offered as the first port of the range: it must not look free
    // for "localhost"
    squattedPort = StaticBaseServerTest.allocateFreePorts(1)[0];
    try {
      squatter = new ServerSocket(squattedPort, 0, InetAddress.getByName("::1"));
    } catch (final IOException e) {
      squatter = null; // no IPv6 loopback here
    }
    GlobalConfiguration.POSTGRES_PORT.setValue(squattedPort + "-" + (squattedPort + 9));
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      super.endTest();
    } finally {
      if (squatter != null)
        try {
          squatter.close();
        } catch (final IOException e) {
          // IGNORE IT
        }
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      GlobalConfiguration.POSTGRES_HOST.reset();
    }
  }

  @Test
  void listenerAnswersOnEveryLocalAddressOfLocalhost() throws Exception {
    final int port = getServerPostgresPort();
    assertThat(port).isGreaterThan(0);
    for (final String host : MultiAddressServerSocket.resolveListenHosts("localhost"))
      try (final Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(InetAddress.getByName(host), port), 2000);
        assertThat(socket.isConnected()).as(host).isTrue();
      }
  }

  @Test
  void aPortHeldOnTheOtherFamilyIsSkipped() {
    assumeThat(squatter).isNotNull();
    assumeThat(MultiAddressServerSocket.resolveListenHosts("localhost").size()).isGreaterThan(1);
    assertThat(getServerPostgresPort()).isNotEqualTo(squattedPort);
  }
}
