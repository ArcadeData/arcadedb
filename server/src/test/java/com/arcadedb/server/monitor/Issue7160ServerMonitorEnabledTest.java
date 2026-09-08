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
package com.arcadedb.server.monitor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #7160: {@link ServerMonitor} was never instantiated - the field that would
 * have held it was commented out in {@code ArcadeDBServer} - so none of its three checks ran. The low-disk
 * warning in particular is the signal that precedes a database which can no longer write, and issue #7124 had
 * just finished making it measure the right filesystem on a class nobody started.
 * <p>
 * It now starts with the server, behind {@code arcadedb.server.healthCheck.enabled}, and stops with it: its
 * thread is a daemon, but an embedded start/stop cycle that left one behind would leak a thread per restart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7160ServerMonitorEnabledTest extends StaticBaseServerTest {
  // Deliberately NOT the default 2480: this test needs no HTTP client, and a server left listening by another
  // run would otherwise take the port and the failure would read as something else entirely.
  private static final int HTTP_PORT = 2496;

  private ArcadeDBServer server;

  @AfterEach
  @Override
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    server = null;
    super.endTest();
  }

  @Test
  void theHealthMonitorRunsByDefaultAndStopsWithTheServer() {
    server = startServer(new ContextConfiguration());

    assertThat(server.isStarted()).isTrue();

    final ServerMonitor monitor = server.getServerMonitor();
    assertThat(monitor).as("the monitor is constructed and held by the server").isNotNull();
    assertThat(monitor.getStatus().isRunning).isTrue();

    assertThat(monitorThreads()).as("exactly one monitor thread while the server runs").hasSize(1);

    server.stop();

    assertThat(server.getServerMonitor()).as("a stopped server holds no monitor").isNull();
    assertThat(monitor.getStatus().isRunning).as("and the thread it started is asked to exit").isFalse();
    // stop() joins the thread, so by the time it returns the thread must be GONE - not merely flagged to exit.
    // A daemon thread cannot hold the JVM up, but one left behind per in-process restart is the leak this is
    // stopped explicitly to avoid (issue #7160).
    assertThat(monitorThreads()).as("no monitor thread survives the stop").isEmpty();
  }

  @Test
  void theHealthMonitorCanBeTurnedOff() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_HEALTH_CHECK_ENABLED, false);

    server = startServer(configuration);

    assertThat(server.getServerMonitor()).isNull();
  }

  /**
   * Turning it off must work from the server configuration FILE, which reaches a ContextConfiguration as JSON
   * text through fromJSON - a different write path from setValue, and the one an operator actually uses.
   */
  @Test
  void theSettingIsReadFromTheServerConfigurationFileText() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"server.healthCheck.enabled\":false}}");

    assertThat(configuration.getValueAsBoolean(GlobalConfiguration.SERVER_HEALTH_CHECK_ENABLED)).isFalse();

    server = startServer(configuration);

    assertThat(server.getServerMonitor()).isNull();
  }

  /** The same file with the value spelled as text, which is how a hand-edited configuration usually reads. */
  @Test
  void theSettingIsReadFromTheServerConfigurationFileAsAString() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"server.healthCheck.enabled\":\"false\"}}");

    server = startServer(configuration);

    assertThat(server.getServerMonitor()).isNull();
  }

  /** Names every live thread the monitor would have started. */
  private static List<Thread> monitorThreads() {
    return Thread.getAllStackTraces().keySet().stream().filter(Thread::isAlive)
        .filter(t -> "ArcadeDB-ServerMonitor".equals(t.getName())).toList();
  }

  private ArcadeDBServer startServer(final ContextConfiguration configuration) {
    configuration.setValue(GlobalConfiguration.SERVER_NAME, "health_check_7160");
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases0");
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    configuration.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, HTTP_PORT);
    configuration.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    configuration.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    final ArcadeDBServer newServer = new ArcadeDBServer(configuration);
    newServer.start();
    return newServer;
  }
}
