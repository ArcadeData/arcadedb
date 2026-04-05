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
package com.arcadedb.e2e;

import com.arcadedb.remote.RemoteDatabase;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for HA e2e tests that inject network faults via Toxiproxy.
 * <p>
 * Architecture: ArcadeDB nodes communicate via Raft on port 2424. Toxiproxy sits between
 * them, proxying the Raft traffic so we can inject latency, packet loss, etc.
 * <p>
 * Topology for 3 nodes:
 * <pre>
 *   arcadedb-0 <-> toxiproxy (port 12424) <-> arcadedb-1 (port 2424)
 *   arcadedb-0 <-> toxiproxy (port 22424) <-> arcadedb-2 (port 2424)
 *   arcadedb-1 <-> toxiproxy (port 32424) <-> arcadedb-2 (port 2424)
 * </pre>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public abstract class ArcadeHAToxiproxyTemplate {

  protected static final String ROOT_PASSWORD = "playwithdata";
  protected static final String DATABASE_NAME = "testdb";
  protected static final int    HTTP_PORT     = 2480;
  protected static final int    RAFT_PORT     = 2424;
  protected static final int    TOXI_API_PORT = 8474;

  protected final Network                   network    = Network.newNetwork();
  protected final List<GenericContainer<?>> containers = new ArrayList<>();
  protected GenericContainer<?>             toxiproxy;
  protected ToxiproxyClient                 toxiClient;
  protected final List<Proxy>               proxies    = new ArrayList<>();

  /**
   * Starts a cluster with Toxiproxy intercepting Raft traffic between nodes.
   * Each node's server list points to Toxiproxy addresses instead of direct peer addresses.
   */
  protected void startClusterWithToxiproxy(final int size) throws IOException {
    // Start Toxiproxy container
    toxiproxy = new GenericContainer<>("ghcr.io/shopify/toxiproxy:2.9.0")
        .withNetwork(network)
        .withNetworkAliases("toxiproxy")
        .withExposedPorts(TOXI_API_PORT)
        .waitingFor(Wait.forListeningPort());

    // Expose proxy ports (one per node pair direction)
    final int proxyCount = size * (size - 1);
    for (int i = 0; i < proxyCount; i++)
      toxiproxy.addExposedPort(10000 + i);
    toxiproxy.start();

    // Create Toxiproxy client
    toxiClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getMappedPort(TOXI_API_PORT));

    // Create proxies: for each pair (i -> j), create a proxy that routes traffic
    // from toxiproxy:listenPort to arcadedb-j:2424
    int proxyIdx = 0;
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < size; j++) {
        if (i == j) continue;
        final int listenPort = 10000 + proxyIdx;
        final String proxyName = "raft-" + i + "-to-" + j;
        final Proxy proxy = toxiClient.createProxy(proxyName,
            "0.0.0.0:" + listenPort, "arcadedb-" + j + ":" + RAFT_PORT);
        proxies.add(proxy);
        proxyIdx++;
      }
    }

    // Build server list where each node points to toxiproxy for its peers
    // Node i's view: itself at arcadedb-i:2424, peers at toxiproxy:<proxy_port>
    for (int i = 0; i < size; i++) {
      final StringBuilder serverList = new StringBuilder();
      int pIdx = 0;
      for (int src = 0; src < size; src++) {
        for (int dst = 0; dst < size; dst++) {
          if (src == dst) continue;
          if (src == i) {
            // This node connects to peer dst via toxiproxy
            if (!serverList.isEmpty()) serverList.append(",");
            serverList.append("toxiproxy:").append(10000 + pIdx);
          }
          pIdx++;
        }
      }
      // Add self direct address
      if (!serverList.isEmpty()) serverList.append(",");
      serverList.append("arcadedb-").append(i).append(":").append(RAFT_PORT);

      final String alias = "arcadedb-" + i;
      final GenericContainer<?> container = new GenericContainer<>("arcadedata/arcadedb:latest")
          .withNetwork(network)
          .withNetworkAliases(alias)
          .withExposedPorts(HTTP_PORT)
          .withStartupTimeout(Duration.ofSeconds(120))
          .withEnv("JAVA_OPTS",
              "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD
                  + " -Darcadedb.ha.enabled=true"
                  + " -Darcadedb.ha.serverList=" + serverList
                  + " -Darcadedb.ha.clusterName=toxi-test"
                  + " -Darcadedb.ha.quorum=majority"
                  + " -Darcadedb.server.name=" + alias
                  + " -Darcadedb.ha.replicationIncomingHost=0.0.0.0"
                  + " -Darcadedb.server.defaultDatabases=" + DATABASE_NAME + "[root]"
          )
          .waitingFor(Wait.forHttp("/api/v1/ready").forPort(HTTP_PORT).forStatusCode(204));

      container.start();
      containers.add(container);
    }

    // Wait for leader election
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
      for (final GenericContainer<?> c : containers) {
        try (final RemoteDatabase db = createRemoteDatabase(c)) {
          // A simple query succeeding means the cluster is up
          db.query("SQL", "SELECT 1");
          return true;
        } catch (final Exception ignored) {}
      }
      return false;
    });
  }

  protected void stopAll() {
    for (final GenericContainer<?> c : containers)
      if (c.isRunning()) c.stop();
    containers.clear();
    if (toxiproxy != null && toxiproxy.isRunning()) toxiproxy.stop();
    proxies.clear();
    network.close();
  }

  /**
   * Adds latency to a specific proxy direction.
   */
  protected void addLatency(final Proxy proxy, final ToxicDirection direction, final long latencyMs, final long jitterMs)
      throws IOException {
    proxy.toxics().latency(proxy.getName() + "-latency-" + direction, direction, latencyMs)
        .setJitter(jitterMs);
  }

  /**
   * Adds latency to ALL proxies (symmetric network-wide latency).
   */
  protected void addLatencyAll(final long latencyMs, final long jitterMs) throws IOException {
    for (final Proxy proxy : proxies)
      addLatency(proxy, ToxicDirection.DOWNSTREAM, latencyMs, jitterMs);
  }

  /**
   * Removes all toxics from all proxies.
   */
  protected void removeAllToxics() throws IOException {
    for (final Proxy proxy : proxies) {
      final var toxics = proxy.toxics().getAll();
      for (final var toxic : toxics)
        toxic.remove();
    }
  }

  protected RemoteDatabase createRemoteDatabase(final GenericContainer<?> container) {
    return new RemoteDatabase(container.getHost(), container.getMappedPort(HTTP_PORT),
        DATABASE_NAME, "root", ROOT_PASSWORD);
  }
}
