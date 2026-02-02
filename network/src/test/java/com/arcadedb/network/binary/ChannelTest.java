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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChannelTest {

  private TestChannel channel;
  private Socket mockSocket;

  @BeforeEach
  void setUp() throws IOException {
    mockSocket = mock(Socket.class);
    when(mockSocket.getRemoteSocketAddress()).thenReturn(null);
    when(mockSocket.getLocalSocketAddress()).thenReturn(null);
    channel = new TestChannel(mockSocket);
  }

  @AfterEach
  void tearDown() {
    if (channel != null)
      channel.close();
  }

  @Test
  void getLocalIpAddressPreferIPv4() throws SocketException {
    // This test may return null on systems without network interfaces
    final String address = Channel.getLocalIpAddress(true);
    // Just verify it doesn't throw - actual address depends on system
    // If an address is returned, it should not be loopback
    if (address != null) {
      assertThat(address).doesNotStartWith("127.");
      assertThat(address).isNotEqualTo("::1");
    }
  }

  @Test
  void getLocalIpAddressPreferIPv6() throws SocketException {
    final String address = Channel.getLocalIpAddress(false);
    // Just verify it doesn't throw - actual address depends on system
  }

  @Test
  void inputHasDataWhenNoInputStream() {
    channel.inStream = null;
    assertThat(channel.inputHasData()).isFalse();
  }

  @Test
  void inputHasDataWhenStreamEmpty() {
    channel.inStream = new ByteArrayInputStream(new byte[0]);
    assertThat(channel.inputHasData()).isFalse();
  }

  @Test
  void inputHasDataWhenStreamHasData() {
    channel.inStream = new ByteArrayInputStream(new byte[] { 1, 2, 3 });
    assertThat(channel.inputHasData()).isTrue();
  }

  @Test
  void flushWhenOutputStreamExists() throws IOException {
    final ByteArrayOutputStream mockOutput = mock(ByteArrayOutputStream.class);
    channel.outStream = mockOutput;
    channel.flush();
    verify(mockOutput).flush();
  }

  @Test
  void flushWhenOutputStreamNull() throws IOException {
    channel.outStream = null;
    // Should not throw
    channel.flush();
  }

  @Test
  void closeChannel() throws IOException {
    final InputStream mockIn = mock(InputStream.class);
    final OutputStream mockOut = mock(OutputStream.class);
    channel.inStream = mockIn;
    channel.outStream = mockOut;

    channel.close();

    verify(mockSocket).close();
    verify(mockIn).close();
    verify(mockOut).close();
    assertThat(channel.socket).isNull();
    assertThat(channel.inStream).isNull();
    assertThat(channel.outStream).isNull();
  }

  @Test
  void closeChannelWhenAlreadyClosed() {
    channel.socket = null;
    channel.inStream = null;
    channel.outStream = null;

    // Should not throw
    channel.close();
  }

  @Test
  void toStringWhenConnected() {
    when(mockSocket.getRemoteSocketAddress()).thenReturn(new java.net.InetSocketAddress("192.168.1.1", 2480));
    assertThat(channel.toString()).contains("192.168.1.1");
    assertThat(channel.toString()).contains("2480");
  }

  @Test
  void toStringWhenNotConnected() {
    channel.socket = null;
    assertThat(channel.toString()).isEqualTo("Not connected");
  }

  @Test
  void getLocalSocketAddressWhenConnected() {
    when(mockSocket.getLocalSocketAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));
    assertThat(channel.getLocalSocketAddress()).contains("127.0.0.1");
  }

  @Test
  void getLocalSocketAddressWhenNotConnected() {
    channel.socket = null;
    assertThat(channel.getLocalSocketAddress()).isEqualTo("?");
  }

  /**
   * Concrete test implementation of the abstract Channel class.
   */
  static class TestChannel extends Channel {
    TestChannel(final Socket socket) throws IOException {
      super(socket);
    }
  }
}
