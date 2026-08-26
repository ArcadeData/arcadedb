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
package com.arcadedb.network.binary;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6761: the socket options and the close order the wire protocols depend on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ChannelSocketOptionsTest {

  /**
   * The Postgres and Redis executors set SO_TIMEOUT to 0 after authentication, so keepalive is the only thing that
   * can ever unblock a read from a peer that died without a FIN/RST.
   */
  @Test
  void keepAliveIsEnabledOnEveryChannel() throws Exception {
    try (final ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        final Socket client = new Socket(InetAddress.getLoopbackAddress(), server.getLocalPort());
        final Socket accepted = server.accept()) {

      final ChannelBinaryServer channel = new ChannelBinaryServer(accepted, new ContextConfiguration());
      try {
        assertThat(accepted.getKeepAlive()).isTrue();
      } finally {
        channel.close();
      }
      assertThat(client.isConnected()).isTrue();
    }
  }

  /** The switch has to actually switch it off, otherwise it is not a knob. */
  @Test
  void keepAliveCanBeDisabled() throws Exception {
    GlobalConfiguration.NETWORK_SOCKET_KEEP_ALIVE.setValue(false);
    try (final ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        final Socket client = new Socket(InetAddress.getLoopbackAddress(), server.getLocalPort());
        final Socket accepted = server.accept()) {

      final ChannelBinaryServer channel = new ChannelBinaryServer(accepted, new ContextConfiguration());
      try {
        assertThat(accepted.getKeepAlive()).isFalse();
      } finally {
        channel.close();
      }
      assertThat(client.isConnected()).isTrue();
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_KEEP_ALIVE.reset();
    }
  }

  /**
   * close() used to shut the input stream - and with it the socket - before the output stream, so the buffered bytes
   * the out.close() should have flushed were dropped instead.
   */
  @Test
  void closeFlushesTheBufferedOutputInsteadOfDroppingIt() throws Exception {
    try (final ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        final Socket client = new Socket(InetAddress.getLoopbackAddress(), server.getLocalPort());
        final Socket accepted = server.accept()) {

      final ChannelBinaryServer channel = new ChannelBinaryServer(accepted, new ContextConfiguration());
      // written into the BufferedOutputStream and deliberately NOT flushed: only close() can get it onto the wire
      channel.writeUnsignedInt(0xCAFEBABE);
      channel.close();

      final DataInputStream in = new DataInputStream(client.getInputStream());
      assertThat(in.readInt()).isEqualTo(0xCAFEBABE);
    }
  }

}
