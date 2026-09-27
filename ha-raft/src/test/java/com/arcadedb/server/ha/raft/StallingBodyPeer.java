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
package com.arcadedb.server.ha.raft;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A peer that answers {@code 200} with a {@code Content-Length} and then stalls inside its body (issues #8325,
 * #8472). On JDK 21-25 the JDK request timeout stops at the response headers, so a {@code BodyHandlers.ofString()}
 * read against this peer is bounded by nothing but the caller's own deadline.
 * <p>
 * It takes ONE connection, reads the request headers, sends {@link #STALLED_BODY} and then says nothing more,
 * keeping the connection open until the caller closes it - which it records. It stops listening after that
 * connection, so a caller that retries is refused at once instead of meeting a second stall.
 * <p>
 * Built with an {@link SSLContext}, it listens with TLS instead, so the HTTPS branch of a dial - which builds or
 * borrows a client of its own - meets the same stall behind a completed handshake.
 */
final class StallingBodyPeer implements AutoCloseable {
  /** A hang detector, not a latency bound: how long a test waits before calling the call unbounded. */
  static final long HANG_DETECT_MS = 60_000L;

  /** Headers promising 100 bytes, then five of them, then silence. */
  private static final String STALLED_BODY =
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"res";

  private final ServerSocket            serverSocket;
  private final AtomicReference<Socket> accepted                 = new AtomicReference<>();
  final CountDownLatch                  connectionClosedByCaller = new CountDownLatch(1);

  StallingBodyPeer() throws IOException {
    this(null);
  }

  /** @param tls the server identity to listen with, or {@code null} to listen in plain text */
  StallingBodyPeer(final SSLContext tls) throws IOException {
    serverSocket = tls == null ? new ServerSocket(0, 16, InetAddress.getLoopbackAddress())
        : tls.getServerSocketFactory().createServerSocket(0, 16, InetAddress.getLoopbackAddress());
    final Thread acceptor = new Thread(() -> {
      try (final Socket socket = serverSocket.accept()) {
        accepted.set(socket);
        serverSocket.close();
        final InputStream in = socket.getInputStream();
        skipRequestHeaders(in);
        final OutputStream out = socket.getOutputStream();
        out.write(STALLED_BODY.getBytes(StandardCharsets.US_ASCII));
        out.flush();
        final byte[] buffer = new byte[1024];
        while (in.read(buffer) >= 0) {
          // discard the request body, until the caller closes the connection
        }
        connectionClosedByCaller.countDown();
      } catch (final IOException e) {
        connectionClosedByCaller.countDown();
      }
    }, "stalling-body-peer");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  String address() {
    return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
  }

  /**
   * Runs {@code call} on its own thread and fails - rather than hanging the suite - when it has not returned within
   * {@link #HANG_DETECT_MS}. Closing the peer is what releases a call that never gave up.
   */
  <T> T callWithin(final Callable<T> call) throws Exception {
    return callWithin(call, HANG_DETECT_MS);
  }

  /** As {@link #callWithin(Callable)}, with an explicit hang detector. */
  <T> T callWithin(final Callable<T> call, final long hangDetectMs) throws Exception {
    final FutureTask<T> task = new FutureTask<>(call);
    final Thread thread = new Thread(task, "stalling-body-peer-caller");
    thread.setDaemon(true);
    thread.start();
    try {
      return task.get(hangDetectMs, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      close();
      throw new AssertionError("The call was still waiting on the stalled peer after " + hangDetectMs
          + " ms: nothing bounds the read of the peer's body", e);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof Exception cause)
        throw cause;
      throw e;
    }
  }

  private static void skipRequestHeaders(final InputStream in) throws IOException {
    int matched = 0;
    final byte[] terminator = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    while (matched < terminator.length) {
      final int b = in.read();
      if (b < 0)
        throw new IOException("the caller closed before sending its request");
      matched = b == terminator[matched] ? matched + 1 : (b == terminator[0] ? 1 : 0);
    }
  }

  @Override
  public void close() throws IOException {
    serverSocket.close();
    final Socket socket = accepted.get();
    if (socket != null)
      socket.close();
  }
}
