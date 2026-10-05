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

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A listening endpoint that binds EVERY local address a host name resolves to on one port (issue #9224).
 * <p>
 * {@code new InetSocketAddress(name, port)} and {@code InetAddress.getByName(name)} keep only the FIRST address of a name: for
 * {@code localhost} that is {@code 127.0.0.1}, so a port another process holds on {@code [::1]} looked free and
 * {@code localhost:<port>} then reached either process depending on the client's resolver order. A name that resolves to several
 * local addresses is bound on each of them, and the bind fails (so the caller tries its next port) when any of them is taken.
 * <p>
 * A host with one address (a literal, {@code 0.0.0.0}, a single-address name, an unresolvable name) keeps one plain
 * {@link ServerSocket} and {@link #accept()} delegates to it. With several, one daemon thread per socket accepts into a queue
 * that {@link #accept()} drains, so the callers keep their single accept loop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class MultiAddressServerSocket implements AutoCloseable {
  private static final Object CLOSED           = new Object();
  private static final long   ERROR_BACKOFF_MS = 50;
  private static final long   CLOSED_CHECK_MS  = 100;

  private final List<ServerSocket>          sockets;
  private final LinkedBlockingQueue<Object> accepted;
  private final List<Thread>                acceptors = new ArrayList<>();
  private volatile boolean                  closed;

  private MultiAddressServerSocket(final List<ServerSocket> sockets) {
    this.sockets = sockets;
    if (sockets.size() > 1) {
      // one slot: the backlog stays in the kernel instead of every connection being accepted ahead of the pre-authentication and
      // connection-limit checks that run after accept(). At most one queued connection plus one held by each blocked acceptor
      accepted = new LinkedBlockingQueue<>(1);
      for (final ServerSocket socket : sockets) {
        final Thread thread = new Thread(() -> acceptLoop(socket), "ArcadeDB listener " + socket.getLocalSocketAddress());
        thread.setDaemon(true);
        acceptors.add(thread);
        thread.start();
      }
    } else
      accepted = null;
  }

  /**
   * Binds {@code port} on every local address of {@code host}.
   *
   * @param port the port, or 0 for one the operating system picks (the same one is then taken on every address)
   *
   * @throws BindException when the port is held on any of the addresses; nothing stays bound
   */
  public static MultiAddressServerSocket bind(final ServerSocketFactory factory, final String host, final int port)
      throws IOException {
    final List<String> hosts = resolveListenHosts(host);
    // an ephemeral port is picked on the first address, then asked for on the others: it can be taken there, so a few tries
    final int attempts = port == 0 && hosts.size() > 1 ? 10 : 1;
    BindException last = null;
    for (int attempt = 0; attempt < attempts; attempt++) {
      final List<ServerSocket> bound = new ArrayList<>(hosts.size());
      try {
        int boundPort = port;
        for (final String address : hosts) {
          final ServerSocket socket = factory.createServerSocket(boundPort, 0, InetAddress.getByName(address));
          bound.add(socket);
          if (!socket.isBound())
            throw new BindException("Cannot bind " + address + ":" + boundPort);
          boundPort = socket.getLocalPort();
        }
        return new MultiAddressServerSocket(bound);
      } catch (final BindException e) {
        last = e;
        closeAll(bound);
      } catch (final IOException | RuntimeException e) {
        closeAll(bound);
        throw e;
      }
    }
    throw last;
  }

  /**
   * The addresses to bind for a configured host: every LOCAL address a name resolves to. Addresses no interface carries are left
   * out ({@code /etc/hosts} commonly maps {@code localhost} to {@code ::1} where IPv6 is disabled, and binding it would fail on
   * every port). A literal, a single-address name and an unresolvable name are returned unchanged; a name with exactly one local
   * address among several is returned as that address's literal.
   */
  public static List<String> resolveListenHosts(final String host) {
    if (host == null || host.isEmpty())
      return Collections.singletonList(host);

    final InetAddress[] resolved;
    try {
      resolved = InetAddress.getAllByName(host);
    } catch (final UnknownHostException e) {
      return List.of(host);
    }
    if (resolved.length < 2)
      return List.of(host);

    final List<String> hosts = new ArrayList<>(resolved.length);
    for (final InetAddress address : resolved) {
      if (!isLocalAddress(address))
        continue;
      final String literal = address.getHostAddress();
      if (!hosts.contains(literal))
        hosts.add(literal);
    }
    return hosts.isEmpty() ? List.of(host) : List.copyOf(hosts);
  }

  private static boolean isLocalAddress(final InetAddress address) {
    if (address.isAnyLocalAddress())
      return true;
    try {
      return NetworkInterface.getByInetAddress(address) != null;
    } catch (final SocketException e) {
      return false;
    }
  }

  /**
   * Waits for the next connection on any of the addresses.
   *
   * @throws SocketException once closed
   */
  public Socket accept() throws IOException {
    if (accepted == null)
      return sockets.getFirst().accept();
    try {
      // polled, not taken: the CLOSED marker can be lost to a connection queued while close() ran, so the flag is the authority
      Object next;
      do {
        if (closed)
          throw new SocketException("Socket is closed");
        next = accepted.poll(CLOSED_CHECK_MS, TimeUnit.MILLISECONDS);
      } while (next == null);
      if (next instanceof Socket socket) {
        if (closed) {
          closeQuietly(socket);
          throw new SocketException("Socket is closed");
        }
        return socket;
      }
      if (next instanceof IOException e)
        throw e;
      accepted.offer(CLOSED); // leave the marker for any other waiter
      throw new SocketException("Socket is closed");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SocketException("Interrupted");
    }
  }

  private void acceptLoop(final ServerSocket socket) {
    while (!closed && !socket.isClosed())
      try {
        final Socket client = socket.accept();
        try {
          accepted.put(client); // blocks while the listener is busy
        } catch (final InterruptedException e) {
          closeQuietly(client);
          return;
        }
        if (closed)
          drain();
      } catch (final IOException e) {
        if (closed || socket.isClosed())
          return;
        // handed to the caller like the error of a single socket, unless it has not consumed the previous one; either way a pause,
        // so a persistent failure (no file descriptors left) is not retried in a hot loop
        accepted.offer(e); // dropped on purpose when the caller has not consumed the previous one: the backoff bounds the rate
        try {
          Thread.sleep(ERROR_BACKOFF_MS);
        } catch (final InterruptedException ie) {
          return;
        }
      }
  }

  /** Closes the connections accepted but never handed out. */
  private void drain() {
    Object left;
    while ((left = accepted.poll()) != null)
      if (left instanceof Socket socket)
        closeQuietly(socket);
  }

  private static void closeQuietly(final Socket socket) {
    try {
      socket.close();
    } catch (final IOException e) {
      // IGNORE IT
    }
  }

  /** The port bound, the same on every address; -1 when not bound or closed. */
  public int getLocalPort() {
    final ServerSocket socket = sockets.getFirst();
    return socket.isBound() && !socket.isClosed() ? socket.getLocalPort() : -1;
  }

  public boolean isClosed() {
    return closed || sockets.getFirst().isClosed();
  }

  List<ServerSocket> getServerSockets() {
    return Collections.unmodifiableList(sockets);
  }

  @Override
  public void close() {
    closed = true;
    closeAll(sockets);
    if (accepted != null) {
      for (final Thread acceptor : acceptors)
        acceptor.interrupt();
      drain();
      accepted.offer(CLOSED);
    }
  }

  private static void closeAll(final List<ServerSocket> sockets) {
    for (final ServerSocket socket : sockets)
      try {
        socket.close();
      } catch (final IOException e) {
        // IGNORE IT
      }
  }

  @Override
  public String toString() {
    final StringBuilder text = new StringBuilder();
    for (final ServerSocket socket : sockets) {
      if (!text.isEmpty())
        text.append(", ");
      text.append(socket.getLocalSocketAddress());
    }
    return text.toString();
  }
}
