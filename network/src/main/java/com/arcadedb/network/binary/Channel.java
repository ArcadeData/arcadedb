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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import jdk.net.ExtendedSocketOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public abstract class Channel {
  private static final AtomicLong   metricGlobalTransmittedBytes = new AtomicLong();
  private static final AtomicLong   metricGlobalReceivedBytes    = new AtomicLong();
  private static final AtomicLong   metricGlobalFlushes          = new AtomicLong();
  public volatile      Socket       socket;
  public               InputStream  inStream;
  public               OutputStream outStream;
  private final        AtomicLong   metricTransmittedBytes       = new AtomicLong();
  private final        AtomicLong   metricReceivedBytes          = new AtomicLong();
  private final        AtomicLong   metricFlushes                = new AtomicLong();
  private static final boolean      EXTENDED_KEEP_ALIVE_AVAILABLE = extendedKeepAliveAvailable();

  public Channel(final Socket iSocket) throws IOException {
    socket = iSocket;
    socket.setTcpNoDelay(true);
    enableKeepAlive(socket);
    // THIS TIMEOUT IS CORRECT BUT CREATE SOME PROBLEM ON REMOTE, NEED CHECK BEFORE BE ENABLED
    // timeout = iConfig.getValueAsLong(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT);
  }

  /**
   * Turns on TCP keepalive, the only liveness backstop the wire protocols have.
   * <p>
   * {@code PostgresNetworkExecutor} and {@code RedisNetworkExecutor} both set {@code SO_TIMEOUT} to 0 once a
   * connection is authenticated - correctly, because an authenticated client is entitled to hold an idle connection
   * open - and neither protocol carries an application-level heartbeat. A peer that disappears without a FIN/RST
   * (host crash, silent partition) therefore left the server thread parked in {@code read()} for the life of the
   * JVM, leaking a thread and a file descriptor per event, with nothing able to notice. With keepalive on, the OS
   * probes the dead peer and the read fails (issue #6761).
   * <p>
   * The probe timings are applied through {@code jdk.net.ExtendedSocketOptions}, which the JDK only supports where
   * the platform does (Linux and macOS); anywhere else - and for any value left at 0 - the system-wide defaults
   * stand, which still detect the dead peer, just far later. Failures are non-fatal: keepalive is a backstop, and a
   * platform that refuses an option must not stop the connection from being served.
   */
  private static void enableKeepAlive(final Socket socket) {
    if (!GlobalConfiguration.NETWORK_SOCKET_KEEP_ALIVE.getValueAsBoolean())
      return;

    try {
      socket.setKeepAlive(true);
    } catch (final SocketException e) {
      LogManager.instance().log(Channel.class, Level.FINE, "Cannot enable TCP keepalive on the socket", e);
      return;
    }

    if (!EXTENDED_KEEP_ALIVE_AVAILABLE)
      return; // keepalive is on, just with the system-wide probe timings

    setKeepAliveOption(socket, ExtendedSocketOptions.TCP_KEEPIDLE,
        GlobalConfiguration.NETWORK_SOCKET_KEEP_ALIVE_IDLE.getValueAsInteger());
    setKeepAliveOption(socket, ExtendedSocketOptions.TCP_KEEPINTERVAL,
        GlobalConfiguration.NETWORK_SOCKET_KEEP_ALIVE_INTERVAL.getValueAsInteger());
    setKeepAliveOption(socket, ExtendedSocketOptions.TCP_KEEPCOUNT,
        GlobalConfiguration.NETWORK_SOCKET_KEEP_ALIVE_COUNT.getValueAsInteger());
  }

  /**
   * Whether {@code jdk.net.ExtendedSocketOptions} can be reached at all. Resolved once, and defensively: the class
   * lives in the {@code jdk.net} module, which a trimmed jlink image or a native image need not carry. Without it
   * keepalive still works, it just runs on the system-wide probe timings.
   */
  private static boolean extendedKeepAliveAvailable() {
    try {
      return ExtendedSocketOptions.TCP_KEEPIDLE != null;
    } catch (final Throwable e) {
      return false;
    }
  }

  private static void setKeepAliveOption(final Socket socket, final SocketOption<Integer> option, final int value) {
    if (value <= 0)
      return; // leave the system-wide default in place
    try {
      if (socket.supportedOptions().contains(option))
        socket.setOption(option, value);
    } catch (final IOException | UnsupportedOperationException | IllegalArgumentException e) {
      LogManager.instance().log(Channel.class, Level.FINE, "Cannot set the TCP keepalive option %s", e, option.name());
    }
  }

  public static String getLocalIpAddress(final boolean iFavoriteIp4) throws SocketException {
    String bestAddress = null;
    final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    while (interfaces.hasMoreElements()) {
      final NetworkInterface current = interfaces.nextElement();
      if (!current.isUp() || current.isLoopback() || current.isVirtual())
        continue;
      final Enumeration<InetAddress> addresses = current.getInetAddresses();
      while (addresses.hasMoreElements()) {
        final InetAddress current_addr = addresses.nextElement();
        if (current_addr.isLoopbackAddress())
          continue;

        if (bestAddress == null || (iFavoriteIp4 && current_addr instanceof Inet4Address))
          // FAVORITE IP4 ADDRESS
          bestAddress = current_addr.getHostAddress();
      }
    }
    return bestAddress;
  }

  public boolean inputHasData() {
    if (inStream != null)
      try {
        return inStream.available() > 0;
      } catch (final IOException e) {
        // RETURN FALSE
      }
    return false;
  }

  public void flush() throws IOException {
    if (outStream != null)
      outStream.flush();
  }

  /**
   * Closes the output side first, then the input side, then the socket (issue #6761).
   * <p>
   * Order matters: closing a socket - or its input stream, which closes the socket with it - discards whatever is
   * still sitting in the output buffer, so the {@code out.close()} that would have flushed it fails instead and the
   * bytes are dropped with only a FINE log to show for it.
   */
  public synchronized void close() {
    try {
      if (outStream != null) {
        outStream.close();
        outStream = null;
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error during closing of output stream", e);
    }

    try {
      if (inStream != null) {
        inStream.close();
        inStream = null;
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error during closing of input stream", e);
    }

    try {
      if (socket != null) {
        socket.close();
        socket = null;
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error during socket close", e);
    }
  }

  @Override
  public String toString() {
    return socket != null ? socket.getRemoteSocketAddress().toString() + "@" + hashCode() : "Not connected";
  }

  public String getLocalSocketAddress() {
    return socket != null ? socket.getLocalSocketAddress().toString() : "?";
  }

  protected void updateMetricTransmittedBytes(final int iDelta) {
    metricGlobalTransmittedBytes.addAndGet(iDelta);
    metricTransmittedBytes.addAndGet(iDelta);
  }

  protected void updateMetricReceivedBytes(final int iDelta) {
    metricGlobalReceivedBytes.addAndGet(iDelta);
    metricReceivedBytes.addAndGet(iDelta);
  }

  protected void updateMetricFlushes() {
    metricGlobalFlushes.incrementAndGet();
    metricFlushes.incrementAndGet();
  }

}
