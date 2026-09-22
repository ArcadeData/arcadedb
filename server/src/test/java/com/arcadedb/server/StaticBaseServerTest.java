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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

/**
 * Executes all the tests while the server is up and running.
 */
public abstract class StaticBaseServerTest {
  public static final String DEFAULT_PASSWORD_FOR_TESTS = "DefaultPasswordForTests";

  protected StaticBaseServerTest() {
  }

  public void setTestConfiguration() {
    GlobalConfiguration.resetAll();
    GlobalConfiguration.TEST.setValue(true);
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(DEFAULT_PASSWORD_FOR_TESTS);
    GlobalConfiguration.SERVER_HTTP_IO_THREADS.setValue(2);
    GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(2);
  }

  @BeforeEach
  public void beginTest() {
    TestServerHelper.checkActiveDatabases();

    // Issue #6297: the configuration first, THEN the cleanup that resolves its paths. The previous class ends on a
    // resetAll(), and SERVER_DATABASE_DIRECTORY read from a reset configuration resolves to '/databases' rather than
    // to './target/databases', so cleaning first deleted nothing this class was about to write to.
    setTestConfiguration();

    TestServerHelper.deleteDatabaseFolders(5);

    LogManager.instance().log(StaticBaseServerTest.class, Level.FINE, "Starting test...");
  }

  @AfterEach
  public void endTest() {
    TestServerHelper.checkActiveDatabases();
    TestServerHelper.deleteDatabaseFolders(5);
  }

  /**
   * The HTTP port {@code server} ACTUALLY bound, which is not the 2480 the configured range starts at.
   * {@code arcadedb.server.httpIncomingPort} defaults to the range {@code 2480-2489} precisely so a test server
   * can start next to anything else already listening, and {@link HttpServer#getPort()} is the only place the
   * choice it made is recorded.
   * <p>
   * A test that hardcodes 2480 instead is answered by whatever holds that port - a developer's own instance, an
   * IDE-launched server, a previous run that has not exited - and the failure reads as {@code 403} or "Too many
   * failed authentication attempts", never as a port conflict. In a cluster {@code 2480 + serverIndex} is worse
   * rather than better: one stranger on 2480 shifts EVERY server up by one, so each index addresses its
   * neighbour and the test fails somewhere else entirely.
   *
   * @throws IllegalStateException when the server has not bound a port, because there is none to answer with and
   *                               a guess would reintroduce exactly the defect this method exists to remove
   */
  protected static int getServerHttpPort(final ArcadeDBServer server) {
    final HttpServer http = server != null ? server.getHttpServer() : null;
    // <= 0 rather than just a null http: getPort() hands back the raw field, which is 0 until the bind loop
    // runs and -1 once handleServerStartFailure() has given up on the whole range. Neither is a port, and
    // returning one would be the guess this method exists to refuse.
    if (http == null || http.getPort() <= 0)
      throw new IllegalStateException("Server is not started: it has not bound an HTTP port yet, so there is none to address");
    return http.getPort();
  }

  /**
   * {@code http://127.0.0.1:<the port server actually bound><path>}. {@code path} starts with a slash, e.g.
   * {@code "/api/v1/ready"}, or is empty for the origin alone.
   */
  protected static String getServerHttpUrl(final ArcadeDBServer server, final String path) {
    return "http://127.0.0.1:" + getServerHttpPort(server) + path;
  }

  /**
   * {@code ws://localhost:<the port server actually bound><path>}, for the WebSocket endpoint. The host is
   * {@code localhost} rather than {@code 127.0.0.1} to match what the WebSocket helpers already dial.
   */
  protected static String getServerWsUrl(final ArcadeDBServer server, final String path) {
    return "ws://localhost:" + getServerHttpPort(server) + path;
  }

  /**
   * First and last port {@link #allocateFreePorts(int)} draws from: below EVERY operating system's ephemeral range
   * (Linux 32768-60999, macOS and Windows 49152-65535), and above most of ArcadeDB's default ports (2424, 2434, 2480,
   * 5432, 6379, 7687). Some fixed ports do fall inside it (the MongoDB plugin's 27017, a few tests' 22480, ...): a
   * probe skips any port held when it looks, so one of those only matters if it starts listening in the moment between
   * the probe and the caller's bind.
   * <p>
   * The ephemeral range is where every outgoing TCP connection on the host takes its local port from. A port probed
   * free there and released can be taken by the very next {@code connect()} anywhere on the machine before the caller
   * binds it - the fixture's own first Raft node dialling the second one before the second has bound is enough, and
   * Ratis answers that bind failure with {@code System.exit(1)}, which kills the whole test fork. That is what the
   * first version of this helper, which asked the OS for port 0, ran into while verifying issue #7496.
   */
  static final int FREE_PORT_RANGE_FIRST = 15000;
  static final int FREE_PORT_RANGE_LAST  = 32767;

  /**
   * {@code count} distinct TCP ports free right now, for a listener that has no range to fall back on the way the HTTP
   * server does (gRPC, Raft, ...).
   * <p>
   * A fixed port - {@code 51141 + serverIndex} - is answered by whatever already holds it: a server from an earlier
   * class in the same fork that has not released it yet, another suite that picked the same number, another process
   * on the runner. The failure then lands on whichever test starts next, as "Address already in use" during startup
   * or as a call answered by a stranger, and never names the real cause (issue #7496).
   * <p>
   * Candidates are drawn at random from {@link #FREE_PORT_RANGE_FIRST}..{@link #FREE_PORT_RANGE_LAST}, so two fixtures
   * running at once do not walk the range in the same order, and each is proven free by binding it. Every socket stays
   * open until all {@code count} are taken, so the ports are distinct from each other; they are closed before
   * returning, so the caller can bind them. Allocate everything one fixture needs in ONE call: a second call cannot
   * see the ports the first one has already released.
   */
  protected static int[] allocateFreePorts(final int count) {
    return allocateFreePorts(count, FREE_PORT_RANGE_FIRST, FREE_PORT_RANGE_LAST);
  }

  /** {@link #allocateFreePorts(int)} over an explicit, inclusive range: package-private so a test can make it tiny. */
  static int[] allocateFreePorts(final int count, final int first, final int last) {
    final ServerSocket[] sockets = new ServerSocket[count];
    final int[] ports = new int[count];
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    // Enough draws to find a free port in a tiny range and to ride out a crowded large one, bounded so an exhausted
    // range fails instead of spinning.
    final int maxAttempts = Math.max(64, 32 * count);
    int taken = 0;
    try {
      for (int attempt = 0; attempt < maxAttempts && taken < count; attempt++) {
        final int candidate = random.nextInt(first, last + 1);
        final ServerSocket socket = bindIfFree(candidate);
        if (socket != null) {
          sockets[taken] = socket;
          ports[taken++] = candidate;
        }
      }
      if (taken < count)
        throw new IllegalStateException(
            "Cannot find " + count + " free ports for the test in " + first + ".." + last + " after " + maxAttempts + " attempts");
      return ports;
    } finally {
      for (final ServerSocket socket : sockets)
        if (socket != null)
          try {
            socket.close();
          } catch (final IOException ignore) {
            // Nothing useful to do: the port is reported anyway and binding it is what proves it free.
          }
    }
  }

  /**
   * A socket bound to {@code port} on every interface, or null if anything holds it - including a socket this same
   * allocation bound a moment ago, which is what keeps the ports of one call distinct.
   */
  private static ServerSocket bindIfFree(final int port) {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket();
      socket.bind(new InetSocketAddress(port));
      return socket;
    } catch (final IOException busy) {
      if (socket != null)
        try {
          socket.close();
        } catch (final IOException ignore) {
          // The bind failed, so there is nothing held to release.
        }
      return null;
    }
  }

  protected static void testLog(final String msg, final Object... args) {
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO,
        "***********************************************************************************");
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO, "TEST: " + msg, args);
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO,
        "***********************************************************************************");
  }
}
