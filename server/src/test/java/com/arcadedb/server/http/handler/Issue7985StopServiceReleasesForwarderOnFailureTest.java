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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.ws.WebSocketEventBus;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7985, the second root cause behind #7677: the forwarder's {@code HttpClient} was released by the LAST
 * statement of {@link HttpServer#stopService()}, and only {@code undertow.stop()} in front of it was guarded. A
 * throw from any earlier step skipped the release - and {@code ArcadeDBServer.stopInternal()} calls
 * {@code stopService()} inside {@code CodeUtils.executeIgnoringExceptions}, so the throw never reached a caller
 * either. The server reported a clean stop while holding the client, its connection pool and its selector thread
 * for the life of the JVM, which is precisely the symptom {@code Issue7507ForwarderClientLifecycleTest} reports.
 * <p>
 * The sibling test {@code Issue7985BoundedForwarderClientReleaseTest} pins the other half - that the release,
 * once reached, leaves the client terminated within a bound. This one pins that it is reached at all.
 * <p>
 * <b>On the reflection.</b> Every collaborator {@code stopService()} touches is a {@code final} field assigned in
 * the {@code HttpServer} constructor, so there is no setter and no seam to pass a failing one through. The
 * failure is therefore injected by swapping in a real subclass of {@link WebSocketEventBus} that throws from
 * {@code stop()} - a real object of a real type rather than a mock, which is what this repository's testing
 * guidelines ask for; only the assignment needs reflection.
 */
class Issue7985StopServiceReleasesForwarderOnFailureTest {

  private static final String DATABASE_DIRECTORY = "./target/databases-issue7985";

  /**
   * The first step of {@link HttpServer#stopService()}. Anything that throws before the release would do; this
   * one is chosen because it is the furthest from the release, so it proves every step in between was guarded
   * rather than only the last one.
   */
  static final class FailingWebSocketEventBus extends WebSocketEventBus {
    FailingWebSocketEventBus(final ArcadeDBServer server) {
      super(server);
    }

    @Override
    public void stop() {
      throw new IllegalStateException("induced failure in the first step of HttpServer.stopService() (issue #7985)");
    }
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aThrowFromAnEarlierStepStillReleasesTheForwardersHttpClient() throws Exception {
    FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_issue7985");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DATABASE_DIRECTORY);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "DefaultPasswordForTests123!");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
    // A free port, not the default 2480: this test must not fight whatever else is listening there.
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(freePort()));

    final ArcadeDBServer server = new ArcadeDBServer(config);
    final LeaderCommandForwarder forwarder;
    try {
      server.start();

      final HttpServer httpServer = server.getHttpServer();
      forwarder = httpServer.getLeaderCommandForwarder();
      assertThat(forwarder.transport().client().isTerminated())
          .as("the client is live while the server is up")
          .isFalse();

      failTheFirstStepOfStopService(httpServer, server);
    } finally {
      // stopInternal() swallows whatever stopService() throws, so this returns either way. What must not happen
      // is that it returns with the client still running.
      server.stop();
    }

    assertThat(forwarder.transport().client().isTerminated())
        .as("a failing step earlier in stopService() must not skip the release of the forwarder's HTTP client")
        .isTrue();

    FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
  }

  private static void failTheFirstStepOfStopService(final HttpServer httpServer, final ArcadeDBServer server)
      throws ReflectiveOperationException {
    final Field field = HttpServer.class.getDeclaredField("webSocketEventBus");
    field.setAccessible(true);
    field.set(httpServer, new FailingWebSocketEventBus(server));
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
