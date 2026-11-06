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
package com.arcadedb.remote;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Issue #11304: a failed replica must not stop cluster configuration reload before the next known replica. */
class Issue11304RemoteClusterReloadFailoverTest {

  @Test
  void reloadTriesTheNextReplicaWhenTheFirstCannotProvideAClusterConfiguration() throws Exception {
    final HttpServer original = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    final HttpServer failingReplica = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    final HttpServer liveReplica = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    final AtomicInteger liveReplicaRequests = new AtomicInteger();

    failingReplica.createContext("/api/v1/server", exchange -> {
      exchange.sendResponseHeaders(500, -1);
      exchange.close();
    });
    liveReplica.createContext("/api/v1/server", exchange -> {
      liveReplicaRequests.incrementAndGet();
      respondWithClusterConfiguration(exchange, liveReplica.getAddress().getPort(), "");
    });
    original.createContext("/api/v1/server", exchange -> respondWithClusterConfiguration(exchange,
        original.getAddress().getPort(), "127.0.0.1:" + failingReplica.getAddress().getPort() + ",127.0.0.1:"
            + liveReplica.getAddress().getPort()));

    original.start();
    failingReplica.start();
    liveReplica.start();
    final RemoteServer client = new RemoteServer("127.0.0.1", original.getAddress().getPort(), "root", "test");
    try {
      original.stop(0);

      assertThat(client.reloadClusterConfiguration()).isTrue();
      assertThat(liveReplicaRequests).hasValue(1);
      assertThat(client.getLeaderAddress()).isEqualTo("127.0.0.1:" + liveReplica.getAddress().getPort());
    } finally {
      client.close();
      original.stop(0);
      failingReplica.stop(0);
      liveReplica.stop(0);
    }
  }

  private static void respondWithClusterConfiguration(final HttpExchange exchange, final int leaderPort,
      final String replicas) throws IOException {
    final byte[] body = ("{\"ha\":{\"leaderAddress\":\"127.0.0.1:" + leaderPort
        + "\",\"replicaAddresses\":\"" + replicas + "\"}}")
        .getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
  }
}
