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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9225: with SSL enabled, a held HTTPS port never advanced through {@code arcadedb.server.httpsIncomingPort}'s
 * range ({@code handleServerStartException} incremented its own parameter), so every attempt failed on the same HTTPS
 * port while only the HTTP port moved, and each failed attempt left its HTTP listener bound for the life of the JVM
 * ({@code Undertow.start()} closes nothing it already bound when a later listener fails). The server must move to the
 * next free HTTPS port of the range, never exceed the range, and leave no stray HTTP listener behind.
 */
class Issue9225HttpsPortRangeAdvancesTest {

  private static final String DATABASE_DIRECTORY = "./target/databases-issue9225";
  private static final String LOOPBACK           = "127.0.0.1";
  /**
   * Only the first port of each range is drawn by {@code allocateFreePorts}; the rest of the range is assumed free, which
   * a short range in the 15000-32767 band all but guarantees. Should it ever flake, the startup log names every port it
   * skipped and why.
   */
  private static final int    RANGE_WIDTH        = 5;

  @BeforeEach
  @AfterEach
  void cleanDirectory() {
    FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aHeldHttpsPortMovesToTheNextOneOfTheRangeAndLeavesNoStrayHttpListener() throws Exception {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(2);
    final int httpFirst = ports[0];
    final int httpsFirst = ports[1];

    try (final ServerSocket stranger = new ServerSocket()) {
      stranger.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), httpsFirst));

      final ArcadeDBServer server = new ArcadeDBServer(sslConfiguration(httpFirst, httpsFirst, httpsFirst + RANGE_WIDTH - 1));
      try {
        server.start();

        assertThat(server.getHttpServer().getHttpsPort())
            .as("the held HTTPS port %d is skipped for the next one of the range", httpsFirst)
            .isEqualTo(httpsFirst + 1);
        assertThat(server.getHttpServer().getPort())
            .as("the HTTP port was free all along, so the server keeps the first one of its range")
            .isEqualTo(httpFirst);
        assertThat(readyStatus(server.getHttpServer().getPort())).isEqualTo(204);
      } finally {
        server.stop();
      }
    }

    // A failed attempt used to keep its HTTP channel open for the life of the JVM: after stop() nothing may hold it
    assertThat(isBindable(httpFirst)).as("no stray HTTP listener is left on %d", httpFirst).isTrue();
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void anExhaustedHttpsRangeFailsTheStartAndReleasesTheHttpPort() throws Exception {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(2);
    final int httpFirst = ports[0];
    final int httpsFirst = ports[1];

    try (final ServerSocket first = new ServerSocket(); final ServerSocket second = new ServerSocket()) {
      first.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), httpsFirst));
      second.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), httpsFirst + 1));

      // The HTTPS range has two ports and both are held: its upper bound must be honored, not walked past
      final ArcadeDBServer server = new ArcadeDBServer(sslConfiguration(httpFirst, httpsFirst, httpsFirst + 1));
      try {
        assertThatThrownBy(server::start)
            .as("the start fails on the HTTPS range, not on the HTTP one")
            .hasStackTraceContaining("HTTPS port")
            .hasStackTraceContaining(httpsFirst + " - " + (httpsFirst + 1));
      } finally {
        server.stop();
      }
    }

    for (int port = httpFirst; port < httpFirst + RANGE_WIDTH; port++)
      assertThat(isBindable(port)).as("no stray HTTP listener is left on %d", port).isTrue();
  }

  /**
   * A bare {@code ArcadeDBServer} rather than a fixture base, as in {@code Issue8692LocalhostBindsEveryAddressTest}: the
   * stranger must hold its HTTPS port before the server starts, the HTTP and HTTPS ranges are both explicit and start at
   * ports drawn by {@code allocateFreePorts} (never hand-picked), and the second test needs the start itself to fail.
   */
  private static ContextConfiguration sslConfiguration(final int httpFirst, final int httpsFirst, final int httpsLast) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_issue9225");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DATABASE_DIRECTORY);
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "DefaultPasswordForTests123!");
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, httpFirst + "-" + (httpFirst + RANGE_WIDTH - 1));
    config.setValue(GlobalConfiguration.SERVER_HTTPS_INCOMING_PORT, httpsFirst + "-" + httpsLast);
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "src/test/resources/keystore.pkcs12");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "src/test/resources/truststore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "nphgDK7ugjGR");
    return config;
  }

  private static boolean isBindable(final int port) throws IOException {
    try (final ServerSocket probe = new ServerSocket()) {
      probe.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), port));
      return true;
    } catch (final BindException e) {
      return false;
    }
  }

  private static int readyStatus(final int port) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) URI.create("http://" + LOOPBACK + ":" + port + "/api/v1/ready").toURL()
        .openConnection();
    try {
      connection.setConnectTimeout(5_000);
      connection.setReadTimeout(5_000);
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
