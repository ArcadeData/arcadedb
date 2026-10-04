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
package com.arcadedb.network;

import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8719, on the bound itself: {@link BoundedHttpExchange#sendWhileProgressing} counts its
 * deadline from the last time a progress counter moved, not from the send. The follower's streamed batch forward feeds
 * it the bytes of the upload it relays, because on HTTP/1.1 the JDK client hands back no answer before the request body
 * has been published whole.
 */
class Issue8719SendWhileProgressingTest {

  private static final long BUDGET_MS        = 1_000L;
  /** The tripwire between "the deadline fired" and "the wait is unbounded". */
  private static final long GAVE_UP_BOUND_MS = 15_000L;
  /** A hang detector, not a latency bound. */
  private static final long HANG_DETECT_MS   = 60_000L;

  /** As long as the counter moves the wait goes on, however long in total; once it stops, it ends one deadline later. */
  @Test
  @Tag("slow")
  void aCounterThatKeepsMovingKeepsTheExchangeAliveUntilItStops() throws Exception {
    final AtomicLong progress = new AtomicLong();
    final long movingForMs = BUDGET_MS * 3;
    final Thread ticker = new Thread(() -> {
      final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(movingForMs);
      while (System.nanoTime() < until) {
        progress.incrementAndGet();
        sleep(BUDGET_MS / 10);
      }
    }, "issue8719-progress");
    ticker.setDaemon(true);

    try (final SilentPeer peer = new SilentPeer()) {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      ticker.start();
      assertGivesUp(peer, progress::get);
      // A lower bound only: a stall can only make it more true.
      assertThat(watch.elapsedMs()).as("not given up on while the counter was moving").isGreaterThanOrEqualTo(movingForMs);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a bound from the last progress from an unbounded wait");
      assertThat(peer.connectionClosed.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
          .as("the exchange is cancelled, not only abandoned by the calling thread").isTrue();
    }
  }

  /**
   * A single move just before the deadline restarts the whole window: the exchange is given up on one deadline after
   * that move, not at the deadline counted from the send.
   */
  @Test
  void aMoveJustBeforeTheDeadlineRestartsTheWholeWindow() throws Exception {
    final long moveAtMs = BUDGET_MS * 3 / 4;

    try (final SilentPeer peer = new SilentPeer()) {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final long start = System.nanoTime();
      // Changes once, moveAtMs after the send, and never again: no thread involved, so the timing is the sampler's.
      final LongSupplier movesOnce = () -> System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(moveAtMs) ? 1L : 0L;
      assertGivesUp(peer, movesOnce);
      assertThat(watch.elapsedMs()).as("the window restarted at the move")
          .isGreaterThanOrEqualTo(moveAtMs + BUDGET_MS);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a bound from the last progress from an unbounded wait");
    }
  }

  /** A counter that never moves leaves exactly the bound {@link BoundedHttpExchange#send} has. */
  @Test
  void aCounterThatNeverMovesGivesUpOneDeadlineAfterTheSend() throws Exception {
    try (final SilentPeer peer = new SilentPeer()) {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertGivesUp(peer, () -> 42L);
      assertThat(watch.elapsedMs()).isGreaterThanOrEqualTo(BUDGET_MS);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound from an unbounded wait");
    }
  }

  // ------------------------------------------------------------------------------------------------------------

  private static void assertGivesUp(final SilentPeer peer, final LongSupplier progress) {
    try (final HttpClient client = HttpClient.newHttpClient()) {
      final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + peer.address() + "/")).GET().build();
      assertThatThrownBy(() -> callWithin(() -> BoundedHttpExchange.sendWhileProgressing(client, request,
          HttpResponse.BodyHandlers.ofString(), BUDGET_MS, progress, null), peer))
          .isInstanceOf(HttpTimeoutException.class)
          .hasMessageContaining(peer.address());
    }
  }

  private static <T> T callWithin(final Callable<T> call, final SilentPeer peer) throws Exception {
    final FutureTask<T> task = new FutureTask<>(call);
    final Thread thread = new Thread(task, "issue8719-send");
    thread.setDaemon(true);
    thread.start();
    try {
      return task.get(HANG_DETECT_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      peer.close();
      throw new AssertionError("Still waiting after " + HANG_DETECT_MS + " ms: nothing bounds the exchange", e);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof Exception cause)
        throw cause;
      throw e;
    }
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** A peer that accepts one connection, never answers, and records when the client closes it. */
  private static final class SilentPeer implements AutoCloseable {
    private final ServerSocket            serverSocket;
    private final AtomicReference<Socket> accepted         = new AtomicReference<>();
    final         CountDownLatch          connectionClosed = new CountDownLatch(1);

    SilentPeer() throws IOException {
      // Bound to a port the OS picks and read back: the shared allocator (StaticBaseServerTest.allocateFreePorts) lives
      // in the server module's test sources, which this module cannot depend on. Nothing addresses a hand-picked port.
      serverSocket = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
      final Thread acceptor = new Thread(() -> {
        try (final Socket socket = serverSocket.accept()) {
          accepted.set(socket);
          final InputStream in = socket.getInputStream();
          final byte[] buffer = new byte[1024];
          while (in.read(buffer) >= 0) {
            // discard, until the client closes the connection
          }
        } catch (final IOException ignored) {
          // closed
        } finally {
          connectionClosed.countDown();
        }
      }, "issue8719-silent-peer");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    String address() {
      return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
      final Socket socket = accepted.get();
      if (socket != null)
        socket.close();
    }
  }
}
