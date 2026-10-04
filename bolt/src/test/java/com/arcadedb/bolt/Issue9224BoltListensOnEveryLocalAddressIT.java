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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.network.MultiAddressServerSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #9224: with {@code host=localhost} the listener bound the first address of the name only, so a port held on
 * the other family ({@code [::1]}) looked free and {@code localhost:<port>} reached either process. The listener now answers on
 * every local address of the host.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9224BoltListensOnEveryLocalAddressIT extends BaseBoltServerTest {
  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    GlobalConfiguration.BOLT_HOST.setValue("localhost");
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      super.endTest();
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      GlobalConfiguration.BOLT_HOST.reset();
    }
  }

  @Test
  void listenerAnswersOnEveryLocalAddressOfLocalhost() throws Exception {
    final int port = getServerBoltPort();
    assertThat(port).isGreaterThan(0);
    for (final String host : MultiAddressServerSocket.resolveListenHosts("localhost"))
      try (final Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(InetAddress.getByName(host), port), 2000);
        assertThat(socket.isConnected()).as(host).isTrue();
      }
  }
}
