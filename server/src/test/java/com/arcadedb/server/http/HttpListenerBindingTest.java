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
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * How the HTTP/HTTPS listeners bind: every address a host name resolves to, advancing through the port ranges, and
 * releasing a listener whose start failed half way.
 */
class HttpListenerBindingTest {
  // Regression: #8692 localhost bound only its first resolved address
  /**
   * Issue #8692: with {@code arcadedb.server.httpIncomingHost=localhost} the HTTP server bound only the first address
   * the name resolves to ({@code 127.0.0.1}), so a port another process held on {@code [::1]} looked free, and
   * {@code localhost:<port>} named two different servers depending on the address family the client's resolver tried
   * first. The server must bind every local address the name resolves to, and skip a port that is held on any of them.
   */
  @Nested
  class LocalhostBindsEveryAddress {

    private static final String DATABASE_DIRECTORY = "./target/databases-issue8692";
    /**
     * Only {@code first} is drawn by {@code allocateFreePorts}; the test needs ONE more free port above it, which a
     * range of ten in the 15000-32767 band all but guarantees. Should it ever flake, the startup log names every port it
     * skipped and why.
     */
    private static final int    RANGE_WIDTH        = 10;

    @Test
    void aNameResolvingToSeveralLocalAddressesListensOnEachOfThem() throws Exception {
      assumeTrue(localhostResolvesToBothLoopbackFamilies(), "this host resolves 'localhost' to one address family only");

      assertThat(HttpServer.resolveListenHosts("localhost")).contains("127.0.0.1", InetAddress.getByName("::1").getHostAddress());
    }

    @Test
    void aLiteralOrASingleAddressNameIsBoundAsConfigured() {
      assertThat(HttpServer.resolveListenHosts("0.0.0.0")).containsExactly("0.0.0.0");
      assertThat(HttpServer.resolveListenHosts("127.0.0.1")).containsExactly("127.0.0.1");
      assertThat(HttpServer.resolveListenHosts("::1")).containsExactly("::1");
      // An unresolvable name is left to the listener, which reports it exactly as before.
      assertThat(HttpServer.resolveListenHosts("no-such-host.invalid")).containsExactly("no-such-host.invalid");
    }

    @Test
    void theProbeReportsAPortHeldOnAnyOfTheAddresses() throws Exception {
      assumeTrue(localhostResolvesToBothLoopbackFamilies(), "this host resolves 'localhost' to one address family only");

      final List<String> hosts = HttpServer.resolveListenHosts("localhost");
      final int port = StaticBaseServerTest.allocateFreePorts(1)[0];
      assertThat(HttpServer.portConflict(hosts, port)).as("a free port has no conflict").isNull();

      try (final ServerSocket stranger = new ServerSocket()) {
        stranger.bind(new InetSocketAddress(InetAddress.getByName("::1"), port));
        assertThat(HttpServer.portConflict(hosts, port))
            .as("a port held on [::1] alone is a conflict, and the reason names the address")
            .isNotNull()
            .contains(InetAddress.getByName("::1").getHostAddress());
      }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void theHttpsListenerIsBoundOnEveryAddressToo() throws Exception {
      assumeTrue(localhostResolvesToBothLoopbackFamilies(), "this host resolves 'localhost' to one address family only");

      final int[] ports = StaticBaseServerTest.allocateFreePorts(2);
      final ContextConfiguration config = serverConfiguration(ports[0] + "-" + (ports[0] + RANGE_WIDTH - 1));
      config.setValue(GlobalConfiguration.SERVER_HTTPS_INCOMING_PORT, String.valueOf(ports[1]));
      config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
      config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, System.getProperty("basedir", ".") + "/src/test/resources/keystore.pkcs12");
      config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
      config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, System.getProperty("basedir", ".") + "/src/test/resources/truststore.jks");
      config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "nphgDK7ugjGR");

      FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
      final ArcadeDBServer server = new ArcadeDBServer(config);
      try {
        server.start();
        final int httpsPort = server.getHttpServer().getHttpsPort();
        assertThat(httpsPort).isEqualTo(ports[1]);

        for (final String address : new String[] { "127.0.0.1", "::1" })
          try (final Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(InetAddress.getByName(address), httpsPort), 5_000);
            assertThat(socket.isConnected()).as("the HTTPS listener answers on %s", address).isTrue();
          }
      } finally {
        server.stop();
        FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
      }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void aPortHeldOnTheIpv6LoopbackIsSkippedAndLocalhostReachesThisServerOnBothFamilies() throws Exception {
      assumeTrue(localhostResolvesToBothLoopbackFamilies(), "this host resolves 'localhost' to one address family only");

      final int first = StaticBaseServerTest.allocateFreePorts(1)[0];
      final InetAddress ipv6Loopback = InetAddress.getByName("::1");

      FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
      try (final ServerSocket stranger = new ServerSocket()) {
        // The stranger holds the first port of the range on [::1] ONLY, so a bind on 127.0.0.1 alone still succeeds.
        stranger.bind(new InetSocketAddress(ipv6Loopback, first));

        final ContextConfiguration config = serverConfiguration(first + "-" + (first + RANGE_WIDTH - 1));

        final ArcadeDBServer server = new ArcadeDBServer(config);
        try {
          server.start();
          final int bound = server.getHttpServer().getPort();

          assertThat(bound)
              .as("a port held on [::1] is not free for a server asked to listen on 'localhost'")
              .isNotEqualTo(first)
              .isBetween(first + 1, first + RANGE_WIDTH - 1);

          assertThat(readyStatus("127.0.0.1", bound)).as("localhost:%d over IPv4 reaches this server", bound).isEqualTo(204);
          assertThat(readyStatus("[::1]", bound)).as("localhost:%d over IPv6 reaches this server", bound).isEqualTo(204);

          // Skipping <first> must leave nothing of this server on it. Without the pre-start probe, Undertow would have bound
          // 127.0.0.1:<first> before [::1]:<first> failed and kept it open (and stop() on that half-started server hangs).
          try (final ServerSocket probe = new ServerSocket()) {
            probe.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), first));
            assertThat(probe.isBound()).as("the abandoned attempt released 127.0.0.1:%d", first).isTrue();
          }
        } finally {
          server.stop();
        }
      } finally {
        FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
      }
    }

    /**
     * A bare {@code ArcadeDBServer} rather than a fixture base, as in {@code ForwarderClientLifecycleTest.StopServiceReleasesForwarderOnFailure}:
     * the stranger must hold its port before the server starts, and the range starts at a port drawn by
     * {@code allocateFreePorts}, never a hand-picked one. Every URL is built from the port the server reports.
     */
    private static ContextConfiguration serverConfiguration(final String httpPortRange) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_issue8692");
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DATABASE_DIRECTORY);
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "DefaultPasswordForTests123!");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, httpPortRange);
      return config;
    }

    private static int readyStatus(final String host, final int port) throws IOException {
      final HttpURLConnection connection = (HttpURLConnection) URI.create("http://" + host + ":" + port + "/api/v1/ready").toURL()
          .openConnection();
      try {
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(5_000);
        return connection.getResponseCode();
      } finally {
        connection.disconnect();
      }
    }

    private static boolean localhostResolvesToBothLoopbackFamilies() {
      try {
        final List<InetAddress> addresses = List.of(InetAddress.getAllByName("localhost"));
        final InetAddress ipv6Loopback = InetAddress.getByName("::1");
        return addresses.contains(InetAddress.getByName("127.0.0.1")) && addresses.contains(ipv6Loopback)
            && NetworkInterface.getByInetAddress(ipv6Loopback) != null;
      } catch (final UnknownHostException | SocketException e) {
        return false;
      }
    }
  }

  // Regression: #9225 a held HTTPS port never advanced through its range
  /**
   * Issue #9225: with SSL enabled, a held HTTPS port never advanced through {@code arcadedb.server.httpsIncomingPort}'s
   * range ({@code handleServerStartException} incremented its own parameter), so every attempt failed on the same HTTPS
   * port while only the HTTP port moved, and each failed attempt left its HTTP listener bound for the life of the JVM
   * ({@code Undertow.start()} closes nothing it already bound when a later listener fails). The server must move to the
   * next free HTTPS port of the range, never exceed the range, and leave no stray HTTP listener behind.
   */
  @Nested
  class HttpsPortRangeAdvances {

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
    void aHeldHttpPortWithSslEnabledMovesOnlyTheHttpPort() throws Exception {
      final int[] ports = StaticBaseServerTest.allocateFreePorts(2);
      final int httpFirst = ports[0];
      final int httpsFirst = ports[1];

      try (final ServerSocket stranger = new ServerSocket()) {
        stranger.bind(new InetSocketAddress(InetAddress.getByName(LOOPBACK), httpFirst));

        final ArcadeDBServer server = new ArcadeDBServer(sslConfiguration(httpFirst, httpsFirst, httpsFirst + RANGE_WIDTH - 1));
        try {
          server.start();

          assertThat(server.getHttpServer().getPort())
              .as("the held HTTP port %d is skipped for the next one of the range", httpFirst)
              .isEqualTo(httpFirst + 1);
          assertThat(server.getHttpServer().getHttpsPort())
              .as("the HTTPS port was free all along, so it stays on the first one of its range")
              .isEqualTo(httpsFirst);
          assertThat(readyStatus(server.getHttpServer().getPort())).isEqualTo(204);
        } finally {
          server.stop();
        }
      }

      // The skipped attempt must not have bound the HTTPS port: a leaked HTTPS listener would still hold it after stop()
      assertThat(isBindable(httpsFirst)).as("no stray HTTPS listener is left on %d", httpsFirst).isTrue();
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
          assertThat(server.getHttpServer().getPort()).as("a failed start reports no HTTP port").isEqualTo(-1);
          assertThat(server.getHttpServer().getHttpsPort()).as("a failed start reports no HTTPS port").isEqualTo(-1);
        } finally {
          server.stop();
        }
      }

      for (int port = httpFirst; port < httpFirst + RANGE_WIDTH; port++)
        assertThat(isBindable(port)).as("no stray HTTP listener is left on %d", port).isTrue();
    }

    /**
     * A bare {@code ArcadeDBServer} rather than a fixture base, as in {@code HttpListenerBindingTest.LocalhostBindsEveryAddress}: the
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
      config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, System.getProperty("basedir", ".") + "/src/test/resources/keystore.pkcs12");
      config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
      config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, System.getProperty("basedir", ".") + "/src/test/resources/truststore.jks");
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

  // Regression: #9479 a half-started listener is released with the XNIO worker
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
  @Nested
  class HalfStartedListenerReleased {

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
     * A bare {@code ArcadeDBServer} rather than a fixture base, as in {@code HttpListenerBindingTest.HttpsPortRangeAdvances}: both
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
      config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, System.getProperty("basedir", ".") + "/src/test/resources/keystore.pkcs12");
      config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
      config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, System.getProperty("basedir", ".") + "/src/test/resources/truststore.jks");
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
}
