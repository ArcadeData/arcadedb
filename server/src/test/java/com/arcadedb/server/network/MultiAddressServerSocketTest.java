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
package com.arcadedb.server.network;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression test for #9224: a wire-protocol listener with {@code host=localhost} bound the first address only, so a port held on
 * the other family ({@code [::1]}) looked free.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MultiAddressServerSocketTest {
  private static final ServerSocketFactory FACTORY = new ServerSocketFactory() {
    @Override
    public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
      final ServerSocket socket = new ServerSocket();
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress(ifAddress, port), backlog);
      return socket;
    }
  };

  private static boolean hasLoopbackV6() {
    try (final ServerSocket probe = new ServerSocket(0, 0, InetAddress.getByName("::1"))) {
      return true;
    } catch (final IOException e) {
      return false;
    }
  }

  @Test
  void singleAddressHostsAreUnchanged() {
    assertThat(MultiAddressServerSocket.resolveListenHosts("127.0.0.1")).containsExactly("127.0.0.1");
    assertThat(MultiAddressServerSocket.resolveListenHosts("0.0.0.0")).containsExactly("0.0.0.0");
    assertThat(MultiAddressServerSocket.resolveListenHosts("no-such-host.invalid")).containsExactly("no-such-host.invalid");
  }

  @Test
  void bindsAndAcceptsOnEveryAddressOfALoopbackName() throws Exception {
    final List<String> hosts = MultiAddressServerSocket.resolveListenHosts("localhost");
    try (final MultiAddressServerSocket socket = MultiAddressServerSocket.bind(FACTORY, "localhost", 0)) {
      assertThat(socket.getServerSockets()).hasSize(hosts.size());
      assertThat(socket.getLocalPort()).isPositive();
      for (final ServerSocket s : socket.getServerSockets())
        assertThat(s.getLocalPort()).isEqualTo(socket.getLocalPort());
      for (final String host : hosts)
        try (final Socket client = new Socket(InetAddress.getByName(host), socket.getLocalPort())) {
          try (final Socket server = socket.accept()) {
            assertThat(server.getLocalAddress()).isEqualTo(InetAddress.getByName(host));
          }
        }
    }
  }

  @Test
  void aPortHeldOnTheOtherFamilyIsNotTaken() throws Exception {
    assumeThat(MultiAddressServerSocket.resolveListenHosts("localhost").size()).isGreaterThan(1);
    assumeThat(hasLoopbackV6()).isTrue();
    try (final ServerSocket stranger = new ServerSocket(0, 0, InetAddress.getByName("::1"))) {
      assertThatThrownBy(() -> MultiAddressServerSocket.bind(FACTORY, "localhost", stranger.getLocalPort()))
          .isInstanceOf(BindException.class);
      // nothing stays bound on 127.0.0.1 after the refusal
      try (final ServerSocket v4 = new ServerSocket(stranger.getLocalPort(), 0, InetAddress.getByName("127.0.0.1"))) {
        assertThat(v4.isBound()).isTrue();
      }
    }
  }

  @Test
  void closeUnblocksAccept() throws Exception {
    final MultiAddressServerSocket socket = MultiAddressServerSocket.bind(FACTORY, "localhost", 0);
    final Thread closer = new Thread(() -> {
      try {
        Thread.sleep(200);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      socket.close();
    });
    closer.start();
    assertThatThrownBy(socket::accept).isInstanceOf(SocketException.class);
    closer.join();
    assertThat(socket.isClosed()).isTrue();
  }

  @Test
  void connectionsAcceptedButNeverHandedOutAreClosedOnClose() throws Exception {
    assumeThat(MultiAddressServerSocket.resolveListenHosts("localhost").size()).isGreaterThan(1);
    final MultiAddressServerSocket socket = MultiAddressServerSocket.bind(FACTORY, "localhost", 0);
    try (final Socket client = new Socket(InetAddress.getByName(MultiAddressServerSocket.resolveListenHosts("localhost").getFirst()),
        socket.getLocalPort())) {
      Thread.sleep(300); // the acceptor thread has queued it, nobody calls accept()
      socket.close();
      client.setSoTimeout(5000);
      // the server side was closed: end of stream (or a reset), never a timeout
      try {
        assertThat(client.getInputStream().read()).isEqualTo(-1);
      } catch (final IOException e) {
        assertThat(e).isNotInstanceOf(SocketTimeoutException.class);
      }
    }
  }
}
