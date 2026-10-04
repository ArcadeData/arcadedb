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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #8692: with {@code arcadedb.server.httpIncomingHost=localhost} the HTTP server bound only the first address
 * the name resolves to ({@code 127.0.0.1}), so a port another process held on {@code [::1]} looked free, and
 * {@code localhost:<port>} named two different servers depending on the address family the client's resolver tried
 * first. The server must bind every local address the name resolves to, and skip a port that is held on any of them.
 */
class Issue8692LocalhostBindsEveryAddressTest {

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
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "src/test/resources/keystore.pkcs12");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "src/test/resources/truststore.jks");
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

        // The attempt on the held port opened 127.0.0.1:<first> before [::1]:<first> failed: it must be released.
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
   * A bare {@code ArcadeDBServer} rather than a fixture base, as in {@code Issue7985StopServiceReleasesForwarderOnFailureTest}:
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
