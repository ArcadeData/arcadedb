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

import com.arcadedb.log.LogManager;

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

  public Channel(final Socket iSocket) throws IOException {
    socket = iSocket;
    socket.setTcpNoDelay(true);
    // THIS TIMEOUT IS CORRECT BUT CREATE SOME PROBLEM ON REMOTE, NEED CHECK BEFORE BE ENABLED
    // timeout = iConfig.getValueAsLong(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT);
  }

  public static String getLocalIpAddress(final boolean iFavoriteIp4) throws SocketException {
    String bestAddress = null;
    final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    while (interfaces.hasMoreElements()) {
      final NetworkInterface current = interfaces.nextElement();
      if (!current.isUp() || current.isLoopback() || current.isVirtual())
        continue;
      Enumeration<InetAddress> addresses = current.getInetAddresses();
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
      } catch (IOException e) {
        // RETURN FALSE
      }
    return false;
  }

  public void flush() throws IOException {
    if (outStream != null)
      outStream.flush();
  }

  public synchronized void close() {
    try {
      if (socket != null) {
        socket.close();
        socket = null;
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error during socket close", e);
    }

    try {
      if (inStream != null) {
        inStream.close();
        inStream = null;
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error during closing of input stream", e);
    }

    try {
      if (outStream != null) {
        outStream.close();
        outStream = null;
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Error during closing of output stream", e);
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
