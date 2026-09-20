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
package com.arcadedb.server.http;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A listener that accepts connections and then says nothing at all: the peer in a long stop-the-world pause,
 * behind a partition that does not RST, or behind an iptables DROP. What a test needs to park an exchange that
 * will never be answered, and so to tell a release that <em>cancels</em> in-flight work from one that waits it
 * out (issue #7985).
 * <p>
 * It also reports when a parked exchange is genuinely <b>on the wire</b>: every accepted connection gets a
 * reader that blocks for the first byte of the request and only then counts down the latch {@link #expect(int)}
 * armed. A fixed sleep in its place establishes nothing - on a slow runner the release under test would be
 * handed an idle client, which even an unfixed release handles correctly, so the regression test would pass for
 * the wrong reason (CodeRabbit review on PR #8026).
 * <p>
 * Lives in the server test-jar rather than once per module: {@code ha-raft} already depends on it in test scope,
 * and the first copy of this class had drifted from the second before the PR that introduced it was merged
 * (review on PR #8026).
 */
public final class SilentPeer implements Closeable {
  private final    ServerSocket   listener;
  private final    Thread         acceptor;
  private final    List<Socket>   accepted  = new ArrayList<>();
  private volatile CountDownLatch onTheWire = new CountDownLatch(0);

  private SilentPeer(final ServerSocket listener) {
    this.listener = listener;
    this.acceptor = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          final Socket socket = listener.accept();
          synchronized (accepted) {
            accepted.add(socket);
          }
          // The latch is read HERE, not inside the reader: a connection accepted after expect() belongs to the
          // round expect() armed, and one accepted before it counts down the round it was opened for. A caller
          // that reuses one peer across rounds depends on that.
          countDownWhenRequestArrives(socket, onTheWire);
        }
      } catch (final IOException ignored) {
        // the listener was closed while the test was tearing down
      }
    }, "silent-peer");
    this.acceptor.setDaemon(true);
    this.acceptor.start();
  }

  /** Binds a listener on a free loopback port. */
  public static SilentPeer start() throws IOException {
    return new SilentPeer(new ServerSocket(0, 256, InetAddress.getLoopbackAddress()));
  }

  /** The {@code host:port} to dial this peer on. */
  public String address() {
    return listener.getInetAddress().getHostAddress() + ":" + listener.getLocalPort();
  }

  /** Arms the latch for the next {@code exchanges} requests. Called before they are sent. */
  public void expect(final int exchanges) {
    onTheWire = new CountDownLatch(exchanges);
  }

  /** Blocks until every request armed by {@link #expect(int)} has arrived here. */
  public void awaitOnTheWire() throws InterruptedException {
    assertThat(onTheWire.await(30, TimeUnit.SECONDS))
        .as("every parked request reached the silent peer, so the release under test has an exchange to cancel")
        .isTrue();
  }

  /**
   * The peer never answers, so the only thing this reader does is prove the request was written: one blocking
   * read of its first byte, and no response ever.
   */
  private static void countDownWhenRequestArrives(final Socket socket, final CountDownLatch latch) {
    final Thread reader = new Thread(() -> {
      try {
        if (socket.getInputStream().read() >= 0)
          latch.countDown();
      } catch (final IOException ignored) {
        // closed by the release under test or by teardown; either way nothing of the test's is in flight
      }
    }, "silent-peer-reader");
    reader.setDaemon(true);
    reader.start();
  }

  @Override
  public void close() throws IOException {
    listener.close();
    acceptor.interrupt();
    synchronized (accepted) {
      for (final Socket socket : accepted)
        try {
          socket.close();
        } catch (final IOException ignored) {
          // best effort: the test is finished with it either way
        }
    }
  }
}
