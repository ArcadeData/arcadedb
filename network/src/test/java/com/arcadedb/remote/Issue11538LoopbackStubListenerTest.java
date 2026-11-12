/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.remote;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11538: the stub HTTP servers of this module's client tests listened on the wildcard address
 * ({@code new ServerSocket(0)}) while their clients dialled {@code 127.0.0.1}. On macOS another process can bind
 * {@code 127.0.0.1} on a port a wildcard listener holds, and then receives every connection to it: during a parallel
 * build a stranger answered {@code Issue8710RemoteServerSecurityRoutesFailoverTest}'s client with a {@code 404}. The
 * stubs now listen through {@link LoopbackStubListener}, on the address the client dials.
 */
class Issue11538LoopbackStubListenerTest {

  @Test
  void aStrangerCannotBindTheLoopbackPortTheStubHolds() throws IOException {
    try (final ServerSocket stub = LoopbackStubListener.bind()) {
      assertThat(stub.getInetAddress()).isEqualTo(LoopbackStubListener.address());

      // The exact binding that stole the client's connections from a wildcard stub: same address, same port, and
      // SO_REUSEADDR on, as Java sets it on every ServerSocket.
      assertThatThrownBy(() -> {
        try (final ServerSocket stranger = new ServerSocket()) {
          stranger.setReuseAddress(true);
          stranger.bind(new InetSocketAddress(LoopbackStubListener.address(), stub.getLocalPort()));
        }
      }).isInstanceOf(BindException.class);
    }
  }

  @Test
  void aStrangerOnTheWildcardAddressDoesNotReceiveTheClient() throws IOException {
    try (final ServerSocket stub = LoopbackStubListener.bind()) {
      final int port = stub.getLocalPort();
      // Linux refuses this bind outright; macOS allows it. Either way the client must still reach the stub.
      ServerSocket stranger = null;
      try {
        stranger = new ServerSocket();
        stranger.setReuseAddress(true);
        try {
          stranger.bind(new InetSocketAddress(port));
        } catch (final BindException refused) {
          stranger.close();
          stranger = null;
        }

        try (final Socket client = new Socket(LoopbackStubListener.HOST, port)) {
          stub.setSoTimeout(5_000);
          try (final Socket accepted = stub.accept()) {
            assertThat(accepted.getPort()).as("the stub, not the stranger, accepted the client").isEqualTo(client.getLocalPort());
          }
        }

        if (stranger != null) {
          stranger.setSoTimeout(100);
          final ServerSocket strangerSocket = stranger;
          assertThatThrownBy(() -> strangerSocket.accept().close()).isInstanceOf(SocketTimeoutException.class);
        }
      } finally {
        if (stranger != null)
          stranger.close();
      }
    }
  }
}
