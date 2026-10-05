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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A socket-level stand-in for an HA leader, for the tests of the follower-side forwarders and relays: a leader that
 * accepts and then says nothing, one that reads the request and never answers, one that drops every connection, or
 * one that answers with a script the test writes byte by byte. Each is a shape a real {@code ArcadeDBServer} cannot be
 * made to take on demand.
 * <p>
 * One fixture rather than one private copy per test class (issue #8691): the copies had already drifted on the bind
 * address, on whether accepted sockets were closed at teardown, and on how they counted connections.
 * <ul>
 *   <li><b>Bind address</b>: always the literal {@code 127.0.0.1}. {@link InetAddress#getLoopbackAddress()} is
 *   {@code ::1} under {@code java.net.preferIPv6Addresses=true}, and an unbracketed IPv6 literal followed by
 *   {@code :port} is not a URI authority the forwarders can build.</li>
 *   <li><b>Port</b>: an ephemeral one, read back from the listener and <em>held bound</em> for the fixture's whole
 *   life. That is the one safe use of port 0: nothing else can take a port that is still bound, whereas a port probed
 *   and released is up for grabs by any outgoing connection before the test reuses it (see
 *   {@code HardcodedTestServerPortsTest}).</li>
 *   <li><b>Teardown</b>: {@link #close()} closes the listener and every connection it accepted. That is what releases a
 *   follower that never gave up: without it an unbounded relay would hold its worker and hang the test's teardown
 *   instead of failing an assertion.</li>
 * </ul>
 */
public final class FakeLeader implements AutoCloseable {

  /** What the leader does with each connection it accepts. */
  public enum Mode {
    /** Accepts and never reads, never writes: a leader wedged before it serviced the request. */
    SILENT,
    /** Reads and discards everything the client sends, and never answers. */
    DRAIN,
    /** Closes every connection as soon as it has accepted it. */
    DROP,
    /**
     * Reads the request headers, writes the {@link Script}'s answer, then drains until the client closes. Every
     * accepted connection gets the script, not only the first one.
     */
    SCRIPTED
  }

  /** The bytes a {@link Mode#SCRIPTED} leader writes once it has read the request headers. */
  @FunctionalInterface
  public interface Script {
    void answer(OutputStream out) throws IOException;
  }

  private static final byte[] HEADERS_END = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

  private final    Mode           mode;
  private final    Script         script;
  private final    ServerSocket   listener;
  private final    List<Socket>   accepted                 = new ArrayList<>();
  private final    CountDownLatch firstConnection          = new CountDownLatch(1);
  private final    CountDownLatch requestReceived          = new CountDownLatch(1);
  private final    CountDownLatch answered                 = new CountDownLatch(1);
  private final    CountDownLatch connectionClosedByClient = new CountDownLatch(1);
  private volatile boolean        closing;
  private volatile Throwable      scriptFailure;

  private FakeLeader(final Mode mode, final Script script) throws IOException {
    this.mode = mode;
    this.script = script;
    this.listener = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
    final Thread acceptor = new Thread(this::acceptLoop, "fake-leader-" + mode.name().toLowerCase());
    acceptor.setDaemon(true);
    acceptor.start();
  }

  /** A leader that accepts connections and answers nothing at all, not even reading the request. */
  public static FakeLeader silent() throws IOException {
    return new FakeLeader(Mode.SILENT, null);
  }

  /** A leader that reads every request and never answers. */
  public static FakeLeader draining() throws IOException {
    return new FakeLeader(Mode.DRAIN, null);
  }

  /** A leader that closes every connection as soon as it has accepted it. */
  public static FakeLeader dropping() throws IOException {
    return new FakeLeader(Mode.DROP, null);
  }

  /**
   * A leader that reads each request's headers, answers with {@code script}, and then keeps the connection open,
   * draining whatever else the client sends, until the client closes it. Every accepted connection gets the script;
   * the {@code await*} methods report the first such event across all of them.
   */
  public static FakeLeader scripted(final Script script) throws IOException {
    if (script == null)
      throw new IllegalArgumentException("a scripted leader needs a script");
    return new FakeLeader(Mode.SCRIPTED, script);
  }

  /** The {@code host:port} to dial this leader on. */
  public String address() {
    return listener.getInetAddress().getHostAddress() + ":" + listener.getLocalPort();
  }

  public int port() {
    return listener.getLocalPort();
  }

  /** How many connections were ever accepted, including the ones already closed by either side. */
  public int acceptedConnections() {
    synchronized (accepted) {
      return accepted.size();
    }
  }

  /** Whether a first connection was accepted within the bound. */
  public boolean awaitFirstConnection(final long timeout, final TimeUnit unit) throws InterruptedException {
    return firstConnection.await(timeout, unit);
  }

  /** {@link Mode#SCRIPTED} only: whether a request's headers had been read in full within the bound. */
  public boolean awaitRequestReceived(final long timeout, final TimeUnit unit) throws InterruptedException {
    requireMode("awaitRequestReceived", Mode.SCRIPTED);
    return requestReceived.await(timeout, unit);
  }

  /**
   * {@link Mode#SCRIPTED} only: whether the script had written its answer within the bound. A script that threw a
   * runtime exception or failed an assertion fails this call with that exception as the cause, rather than reading as a plain timeout.
   */
  public boolean awaitAnswered(final long timeout, final TimeUnit unit) throws InterruptedException {
    requireMode("awaitAnswered", Mode.SCRIPTED);
    if (!answered.await(timeout, unit))
      return false;
    final Throwable failure = scriptFailure;
    if (failure != null)
      throw new IllegalStateException("the leader's script threw instead of answering", failure);
    return true;
  }

  /**
   * {@link Mode#DRAIN} and {@link Mode#SCRIPTED}: whether the client closed (or reset) a connection within the bound.
   * The leader's own teardown in {@link #close()} does not count: a client that never gave up must not read as one
   * that did.
   */
  public boolean awaitConnectionClosedByClient(final long timeout, final TimeUnit unit) throws InterruptedException {
    requireMode("awaitConnectionClosedByClient", Mode.DRAIN, Mode.SCRIPTED);
    return connectionClosedByClient.await(timeout, unit);
  }

  /**
   * {@link Mode#SILENT} only: blocks until the client end of the first accepted connection is closed, or the bound
   * expires. Reads from this side of the socket, where end of stream means the client closed it; in any other mode a
   * reader thread already owns the input stream.
   */
  public boolean firstConnectionClosedByClientWithin(final long boundMs) throws IOException {
    requireMode("firstConnectionClosedByClientWithin", Mode.SILENT);
    final Socket socket;
    synchronized (accepted) {
      if (closing || accepted.isEmpty())
        return false;
      socket = accepted.getFirst();
    }
    // setSoTimeout(0) would block forever: a bound of zero or less is the shortest wait instead
    // SO_TIMEOUT bounds each read, not the loop: a client that keeps sending would otherwise extend the wait forever
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1L, boundMs));
    final byte[] drain = new byte[4096];
    try {
      while (true) {
        final long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remainingMs <= 0)
          return false;
        // setSoTimeout(0) would block forever, so the remaining time never goes below 1ms
        socket.setSoTimeout((int) Math.min(remainingMs, Integer.MAX_VALUE));
        if (socket.getInputStream().read(drain) == -1)
          return !closing;
        // discard: only the end of the stream answers the question
      }
    } catch (final SocketTimeoutException e) {
      return false;
    } catch (final SocketException e) {
      // "connection reset" is the client tearing it down just as abruptly, which is the same answer - unless the
      // socket was closed by this leader's own teardown
      return !closing;
    }
  }

  private void requireMode(final String method, final Mode... allowed) {
    for (final Mode m : allowed)
      if (mode == m)
        return;
    throw new IllegalStateException(method + "() never fires on a " + mode + " leader");
  }

  private void acceptLoop() {
    while (!listener.isClosed()) {
      final Socket socket;
      try {
        socket = listener.accept();
      } catch (final IOException e) {
        return; // the listener was closed by teardown
      }
      synchronized (accepted) {
        if (closing) {
          // accepted while close() was tearing down, after it had walked the list
          closeQuietly(socket);
          return;
        }
        accepted.add(socket);
      }
      firstConnection.countDown();
      switch (mode) {
      case SILENT -> {
        // never answers: the socket stays open until close() tears it down
      }
      case DROP -> closeQuietly(socket);
      case DRAIN, SCRIPTED -> {
        final Thread handler = new Thread(() -> serve(socket), "fake-leader-" + mode.name().toLowerCase() + "-connection");
        handler.setDaemon(true);
        handler.start();
      }
      }
    }
  }

  private void serve(final Socket socket) {
    try {
      final InputStream in = socket.getInputStream();
      if (mode == Mode.SCRIPTED) {
        skipRequestHeaders(in);
        requestReceived.countDown();
        script.answer(socket.getOutputStream());
        socket.getOutputStream().flush();
        answered.countDown();
      }
      final byte[] buffer = new byte[8_192];
      while (in.read(buffer) >= 0) {
        // discard whatever the client still sends, until it closes the connection
      }
    } catch (final IOException e) {
      // a reset, or the client closing before its request was complete: either way the client let go of it, unless
      // this is the leader's own teardown, which the check below tells apart
    } catch (final RuntimeException | AssertionError e) {
      // a broken script: surfaced by awaitAnswered() instead of dying silently with this thread
      scriptFailure = e;
      answered.countDown();
      return;
    }
    if (!closing)
      connectionClosedByClient.countDown();
  }

  private static void skipRequestHeaders(final InputStream in) throws IOException {
    int matched = 0;
    while (matched < HEADERS_END.length) {
      final int b = in.read();
      if (b < 0)
        throw new IOException("the client closed before sending its request");
      matched = b == HEADERS_END[matched] ? matched + 1 : (b == HEADERS_END[0] ? 1 : 0);
    }
  }

  @Override
  public void close() {
    closeQuietly(listener);
    synchronized (accepted) {
      closing = true;
      for (final Socket socket : accepted)
        closeQuietly(socket);
    }
  }

  private static void closeQuietly(final AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (final Exception ignored) {
      // best effort: the test is finished with it either way
    }
  }
}
