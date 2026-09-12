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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7507: bounding the forward meant giving {@link LeaderCommandForwarder} its own {@code HttpClient}
 * (built with the server's configured connect timeout) in place of the JVM-wide static one it used before. A
 * client owns a selector thread and an executor, and there is now one per {@code HttpServer}, so the server has
 * to release it on the way down.
 */
class Issue7507ForwarderClientLifecycleTest {

  private static final String DATABASE_DIRECTORY = "./target/databases-issue7507";

  @Test
  void stoppingTheServerReleasesTheForwardersHttpClient() throws Exception {
    FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_issue7507");
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
      forwarder = server.getHttpServer().getLeaderCommandForwarder();

      assertThat(forwarder.transport().client().connectTimeout())
          .as("the forwarder dials the leader with a bounded client")
          .isPresent();
      assertThat(forwarder.transport().client().isTerminated())
          .as("the client is live while the server is up")
          .isFalse();
    } finally {
      server.stop();
    }

    assertThat(forwarder.transport().client().isTerminated())
        .as("stopping the server releases the forwarder's HTTP client")
        .isTrue();

    FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }
}
