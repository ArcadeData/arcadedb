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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8578: the ports a server advertises in {@code GET /api/v1/server?mode=cluster} are read by the remote client,
 * ignoring malformed and non-positive values, and a refresh that fails does not leave the previous answer's ports behind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8578AdvertisedPortsClientTest {

  @Test
  void readsValidPortsIgnoresBadOnesAndForgetsThemWhenARefreshFails() throws Exception {
    final AtomicReference<String> answer = new AtomicReference<>(
        "{\"ports\":{\"gremlin\":18182,\"text\":\"x\",\"negative\":-1,\"zero\":0}}");
    final AtomicReference<Integer> status = new AtomicReference<>(200);

    final HttpServer http = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    http.createContext("/api/v1/server", exchange -> {
      final byte[] body = answer.get().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(status.get(), status.get() == 200 ? body.length : -1);
      if (status.get() == 200)
        exchange.getResponseBody().write(body);
      exchange.close();
    });
    http.start();
    try {
      final RemoteServer server = new RemoteServer("127.0.0.1", http.getAddress().getPort(), "root", "test");
      try {
        assertThat(server.getAdvertisedPort("gremlin")).isEqualTo(18182);
        assertThat(server.getAdvertisedPort("text")).isZero();
        assertThat(server.getAdvertisedPort("negative")).isZero();
        assertThat(server.getAdvertisedPort("zero")).isZero();
        assertThat(server.getAdvertisedPort("unknown")).isZero();

        status.set(500);
        server.requestClusterConfiguration();
        assertThat(server.getAdvertisedPort("gremlin")).as("a failed refresh must not keep the previous ports").isZero();

        status.set(200);
        answer.set("{}");
        server.requestClusterConfiguration();
        assertThat(server.getAdvertisedPort("gremlin")).as("an older server advertises none").isZero();
      } finally {
        server.close();
      }
    } finally {
      http.stop(0);
    }
  }
}
