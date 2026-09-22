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
import java.net.ServerSocket;
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
   * {@code count} distinct TCP ports the operating system hands out as free right now, for a listener that has no
   * range to fall back on the way the HTTP server does (gRPC, Raft, ...).
   * <p>
   * A fixed port - {@code 51141 + serverIndex} - is answered by whatever already holds it: a server from an earlier
   * class in the same fork that has not released it yet, another suite that picked the same number, another process
   * on the runner. The failure then lands on whichever test starts next, as "Address already in use" during startup
   * or as a call answered by a stranger, and never names the real cause (issue #7496). Every socket stays open until
   * all {@code count} are taken, so the ports are distinct from each other; they are closed before returning, so the
   * caller can bind them.
   * <p>
   * Allocate everything one fixture needs in ONE call: two separate calls can in principle return the same port,
   * because the first call has already released its sockets when the second one asks.
   */
  protected static int[] allocateFreePorts(final int count) {
    final ServerSocket[] sockets = new ServerSocket[count];
    final int[] ports = new int[count];
    try {
      for (int i = 0; i < count; i++) {
        sockets[i] = new ServerSocket(0);
        sockets[i].setReuseAddress(true);
        ports[i] = sockets[i].getLocalPort();
      }
      return ports;
    } catch (final IOException e) {
      throw new IllegalStateException("Cannot allocate " + count + " free ports for the test", e);
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

  protected static void testLog(final String msg, final Object... args) {
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO,
        "***********************************************************************************");
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO, "TEST: " + msg, args);
    LogManager.instance().log(StaticBaseServerTest.class, Level.INFO,
        "***********************************************************************************");
  }
}
