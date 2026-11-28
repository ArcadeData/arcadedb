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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12827: {@link ArcadeDBServer#main} let a failed {@code start()} escape and left the JVM running. A plugin that
 * owns a non-daemon listener thread (the Postgres, Redis, MongoDB and Bolt wire protocols) went on serving clients
 * while the HTTP service, which failed to bind, was down, and the process never exited, so a supervisor never restarted it.
 * <p>
 * The entry point now goes through {@link ArcadeDBServer#startOrStop}, which stops everything the failed start brought
 * up and tells {@code main} to exit with a failure status.
 */
class FailedStartStopsServerTest extends StaticBaseServerTest {
  private static final AtomicReference<Thread> LISTENER = new AtomicReference<>();

  private ServerSocket httpPortHolder;

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    LISTENER.set(null);
  }

  @AfterEach
  public void endTest() {
    final Thread listener = LISTENER.getAndSet(null);
    if (listener != null)
      listener.interrupt();
    if (httpPortHolder != null) {
      try {
        httpPortHolder.close();
      } catch (final IOException e) {
        // NOTHING TO DO: THE SOCKET IS BEING DISCARDED
      }
    }
    httpPortHolder = null;
    super.endTest();
  }

  @Test
  void failedStartStopsThePluginsItAlreadyStarted() throws Exception {
    final int httpPort = allocateFreePorts(1)[0];
    httpPortHolder = new ServerSocket(httpPort, 1, InetAddress.getByName("127.0.0.1"));

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "127.0.0.1");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(httpPort));
    config.setValue(GlobalConfiguration.SERVER_PLUGINS, "Listener:" + ListenerPlugin.class.getName());

    final ArcadeDBServer server = new ArcadeDBServer(config);

    assertThat(ArcadeDBServer.startOrStop(server)).as("the start failed on the busy HTTP port").isFalse();

    final Thread listener = LISTENER.get();
    assertThat(listener).as("the plugin started before the HTTP service failed").isNotNull();
    listener.join(10_000);
    assertThat(listener.isAlive())
        .as("the plugin's non-daemon listener thread must not outlive the failed start: it keeps the JVM running (issue #12827)")
        .isFalse();
    assertThat(server.isStarted()).isFalse();
  }

  @Test
  void successfulStartKeepsTheServerRunning() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    final ArcadeDBServer server = new ArcadeDBServer(config);
    try {
      assertThat(ArcadeDBServer.startOrStop(server)).isTrue();
      assertThat(server.isStarted()).isTrue();
    } finally {
      if (server.isStarted())
        server.stop();
    }
  }

  /** A plugin shaped like the wire protocols: a non-daemon thread that holds the process open until the plugin stops. */
  public static class ListenerPlugin implements ServerPlugin {
    private final AtomicBoolean running = new AtomicBoolean();

    @Override
    public void startService() {
      running.set(true);
      final Thread thread = new Thread(() -> {
        while (running.get()) {
          try {
            Thread.sleep(50);
          } catch (final InterruptedException e) {
            return;
          }
        }
      }, "test-non-daemon-listener");
      thread.setDaemon(false);
      LISTENER.set(thread);
      thread.start();
    }

    @Override
    public void stopService() {
      running.set(false);
    }
  }
}
