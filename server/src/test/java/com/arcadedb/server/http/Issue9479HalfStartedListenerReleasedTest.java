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
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9479: a port taken between the pre-bind probe and {@code Undertow.start()} makes the start fail after the first
 * listener is already bound. The issue assumed that listener stayed bound for the life of the JVM. It does not: on the
 * failure {@code Undertow.start()} calls {@code shutdownNow()} on the XNIO worker it created, and each XNIO I/O thread
 * closes every channel still registered with its selector before it exits, so the half-started listener is released
 * with the worker. These tests pin that behavior, so an Undertow or XNIO upgrade that changes it fails here instead of
 * silently leaking a port per failed attempt.
 * <p>
 * The window is reproduced deterministically through {@link HttpServer#beforeUndertowStart}: on the first attempt it
 * binds the HTTPS port the attempt is about to take, after the probe has already called that port free. The HTTP
 * listener is added to Undertow before the HTTPS one, so the first attempt binds the HTTP port and then fails.
 * <p>
 * The release happens on the dying I/O threads, so it is awaited rather than asserted at one instant. The wait is a
 * tripwire between "released" and "held for the life of the JVM", so a generous bound costs nothing.
 */
class Issue9479HalfStartedListenerReleasedTest {

  private static final String   DATABASE_DIRECTORY = "./target/databases-issue9479";
  private static final String   LOOPBACK           = "127.0.0.1";
  /** Only the first port of each range is drawn by {@code allocateFreePorts}; the rest of the short range is assumed free. */
  private static final int      RANGE_WIDTH        = 5;
  private static final Duration RELEASE_BOUND      = Duration.ofSeconds(30);

  private final AtomicInteger attempts = new AtomicInteger();

  @BeforeEach
  @AfterEach
  void cleanUp() {
    HttpServer.beforeUndertowStart = null;
    FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aPortTakenAfterTheProbeReleasesTheFirstListenerAndTheRetrySucceeds() throws Exception {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(2);
    final int httpFirst = ports[0];
    final int httpsFirst = ports[1];

    try (final ServerSocket stranger = new ServerSocket()) {
      takePortOnFirstAttempt(stranger, httpsFirst);

      final ArcadeDBServer server = new ArcadeDBServer(sslConfiguration(httpFirst, httpsFirst, httpsFirst + RANGE_WIDTH - 1));
      try {
        server.start();

        assertThat(attempts.get()).as("the first attempt failed in the window and the second one started").isEqualTo(2);
        assertThat(stranger.isBound()).as("the stranger took the HTTPS port inside the window").isTrue();
        assertThat(server.getHttpServer().getPort())
            .as("the failed attempt may have held the HTTP port, so the retry moves past it")
            .isEqualTo(httpFirst + 1);
        assertThat(server.getHttpServer().getHttpsPort()).as("the taken HTTPS port is skipped").isEqualTo(httpsFirst + 1);
        assertThat(readyStatus(server.getHttpServer().getPort())).isEqualTo(204);

        // The failed attempt had bound httpFirst before its HTTPS listener failed: it must be released while the server
        // is still running, not held until the JVM exits
        awaitBindable(httpFirst);
      } finally {
        server.stop();
      }
    }

    awaitBindable(httpFirst + 1);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aPortTakenAfterTheProbeWithNoHttpsPortLeftFailsTheStartAndReleasesTheFirstListener() throws Exception {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(2);
    final int httpFirst = ports[0];
    final int httpsFirst = ports[1];

    try (final ServerSocket stranger = new ServerSocket()) {
      takePortOnFirstAttempt(stranger, httpsFirst);

      // A one-port HTTPS range: the retry cannot move the HTTPS port, so the catch itself fails the start
      final ArcadeDBServer server = new ArcadeDBServer(sslConfiguration(httpFirst, httpsFirst, httpsFirst));
      try {
        assertThatThrownBy(server::start)
            .as("the start fails on the exhausted HTTPS range")
            .hasStackTraceContaining("HTTPS port")
            .hasStackTraceContaining(httpsFirst + " - " + httpsFirst);

        assertThat(attempts.get()).as("only the attempt that failed in the window ran").isEqualTo(1);
        assertThat(server.getHttpServer().getPort()).as("a failed start reports no HTTP port").isEqualTo(-1);
        awaitBindable(httpFirst);
      } finally {
        server.stop();
      }
    }
  }

  /** Binds {@code port} with {@code stranger} right before the first {@code Undertow.start()}, after the probe passed. */
  private void takePortOnFirstAttempt(final ServerSocket stranger, final int port) {
    HttpServer.beforeUndertowStart = () -> {
      if (attempts.incrementAndGet() == 1) {
        try {
          stranger.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), port));
        } catch (final IOException e) {
          throw new IllegalStateException("The test could not take port " + port, e);
        }
      }
    };
  }

  private static void awaitBindable(final int port) {
    await().atMost(RELEASE_BOUND).pollInterval(Duration.ofMillis(50))
        .alias("no listener is left holding port " + port)
        .until(() -> isBindable(port));
  }

  /**
   * A bare {@code ArcadeDBServer} rather than a fixture base, as in {@code Issue9225HttpsPortRangeAdvancesTest}: both
   * ranges are explicit and start at ports drawn by {@code allocateFreePorts} (never hand-picked), and the second test
   * needs the start itself to fail.
   */
  private static ContextConfiguration sslConfiguration(final int httpFirst, final int httpsFirst, final int httpsLast) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_issue9479");
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
