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
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12442: a follower forwarded a command to the leader on a {@link HttpClient} left at its default version, which
 * is HTTP/2 - an h2c upgrade of a plain-HTTP connection, ALPN {@code h2} over TLS. The leader's read-timeout lift of
 * #9953 covers HTTP/1.1 exchanges only, so a forwarded statement longer than {@code arcadedb.network.socketTimeout}
 * lost its connection to the leader's read timer. The cluster's own hop therefore asks for HTTP/1.1
 * ({@link LeaderDial#FORWARD_HTTP_VERSION}).
 * <p>
 * Nothing here can tell the two versions apart by a leader that answers: the end-to-end proof is
 * {@code HaForwardedLongCommandIT} (and its TLS twin) in {@code ha-raft}. What a unit test can pin is what a forward
 * puts on the wire. A plain-HTTP client at the default version opens with {@code Connection: Upgrade, HTTP2-Settings} and
 * {@code Upgrade: h2c} on its first request, whatever the server then answers; a client pinned to HTTP/1.1 sends neither.
 * The stub leader is a JDK HTTP/1.1 server, and the control test shows it sees what a default client asks for, so a
 * client that merely stopped being observed cannot pass the others.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12442ForwardClientsSpeakHttp11Test {

  /** The sites that build the follower-to-leader client for plain HTTP, each reaching the stub the way it reaches a leader. */
  @Test
  void theConnectTimeoutBoundedClientAsksForHttp11() throws Exception {
    final HttpClient client = LeaderDial.newConnectTimeoutBoundedClient(new ContextConfiguration());
    try {
      assertSpeaksHttp11("LeaderDial.newConnectTimeoutBoundedClient", client);
    } finally {
      LeaderDial.releaseBounded(client);
    }
  }

  @Test
  void theCommandForwarderTransportAsksForHttp11() throws Exception {
    final HttpClient client = new LeaderCommandForwarder.Transport(new ContextConfiguration()).client();
    try {
      assertSpeaksHttp11("LeaderCommandForwarder.Transport", client);
    } finally {
      LeaderDial.releaseBounded(client);
    }
  }

  /**
   * The control: a client at the JDK's default version does ask for the upgrade, and the stub does see it. Without this,
   * the two tests above would pass for a stub that never records the headers at all.
   */
  @Test
  void aClientLeftAtTheDefaultVersionAsksForTheH2cUpgrade() throws Exception {
    final HttpClient client = HttpClient.newHttpClient();
    try (final StubLeader leader = new StubLeader()) {
      final HttpResponse<String> response = get(client, leader);

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(leader.upgrades())
          .as("the JDK client at its default version opens a plain-HTTP connection with an h2c upgrade: the stub must see it, "
              + "or the pins above prove nothing")
          .containsExactly("h2c");
    } finally {
      LeaderDial.releaseBounded(client);
    }
  }

  private static void assertSpeaksHttp11(final String site, final HttpClient client) throws Exception {
    assertThat(client.version()).as("%s: the version the client is built for", site).isEqualTo(HttpClient.Version.HTTP_1_1);
    assertThat(LeaderDial.FORWARD_HTTP_VERSION).isEqualTo(HttpClient.Version.HTTP_1_1);

    try (final StubLeader leader = new StubLeader()) {
      final HttpResponse<String> response = get(client, leader);

      assertThat(response.statusCode()).as("%s", site).isEqualTo(200);
      assertThat(response.version()).as("%s: the version the exchange ran on", site).isEqualTo(HttpClient.Version.HTTP_1_1);
      assertThat(leader.upgrades())
          .as("%s: a forward must not ask the leader to upgrade the connection to HTTP/2 (issue #12442)", site)
          .isEmpty();
    }
  }

  private static HttpResponse<String> get(final HttpClient client, final StubLeader leader) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create("http://" + leader.address() + "/api/v1/server"))
        .timeout(Duration.ofSeconds(30)).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  /** A loopback HTTP/1.1 listener that records the {@code Upgrade} header of every request it answers. */
  private static final class StubLeader implements AutoCloseable {
    private final HttpServer   server;
    private final List<String> upgrades = new CopyOnWriteArrayList<>();

    StubLeader() throws IOException {
      // Port 0 on a listener that stays bound for the whole test: nothing can take the port between the bind and the dial
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.createContext("/", exchange -> {
        final String upgrade = exchange.getRequestHeaders().getFirst("Upgrade");
        if (upgrade != null)
          upgrades.add(upgrade);
        exchange.getRequestBody().readAllBytes();
        final byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
      });
      server.start();
    }

    String address() {
      return "127.0.0.1:" + server.getAddress().getPort();
    }

    List<String> upgrades() {
      return upgrades;
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
