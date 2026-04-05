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
import com.arcadedb.serializer.json.JSONObject;
import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for HA end-to-end tests using TestContainers.
 * Starts a multi-node ArcadeDB cluster with Ratis HA enabled.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public abstract class ArcadeHAContainerTemplate {

  protected static final String ROOT_PASSWORD = "playwithdata";
  protected static final String DATABASE_NAME = "testdb";
  protected static final int    HTTP_PORT     = 2480;
  protected static final int    RAFT_PORT     = 2424;

  protected final Network                    network    = Network.newNetwork();
  protected final List<GenericContainer<?>>  containers = new ArrayList<>();
  protected final HttpClient                 httpClient = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(10)).build();

  /**
   * Creates a cluster of the specified size.
   */
  protected void startCluster(final int size) {
    final String serverList = buildServerList(size);

    for (int i = 0; i < size; i++) {
      final String alias = "arcadedb-" + i;
      final GenericContainer<?> container = new GenericContainer<>("arcadedata/arcadedb:latest")
          .withNetwork(network)
          .withNetworkAliases(alias)
          .withExposedPorts(HTTP_PORT, RAFT_PORT)
          .withStartupTimeout(Duration.ofSeconds(120))
          .withEnv("JAVA_OPTS",
              "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD
                  + " -Darcadedb.ha.enabled=true"
                  + " -Darcadedb.ha.serverList=" + serverList
                  + " -Darcadedb.ha.clusterName=e2e-test"
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
    waitForLeader();
  }

  protected void stopCluster() {
    for (final GenericContainer<?> container : containers)
      if (container.isRunning())
        container.stop();
    containers.clear();
    network.close();
  }

  /**
   * Waits until at least one node reports itself as leader.
   */
  protected void waitForLeader() {
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          for (final GenericContainer<?> container : containers) {
            if (!container.isRunning()) continue;
            try {
              final JSONObject cluster = getClusterInfo(container);
              if (cluster.has("isLeader") && cluster.getBoolean("isLeader"))
                return true;
            } catch (final Exception ignored) {}
          }
          return false;
        });
  }

  /**
   * Returns the cluster info JSON from a specific container.
   */
  protected JSONObject getClusterInfo(final GenericContainer<?> container) throws Exception {
    final String url = "http://" + container.getHost() + ":" + container.getMappedPort(HTTP_PORT)
        + "/api/v1/server?mode=cluster";
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Authorization", basicAuth())
        .GET().build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    return new JSONObject(response.body()).getJSONObject("ha");
  }

  /**
   * Finds the container that is the current leader.
   */
  protected GenericContainer<?> findLeader() {
    for (final GenericContainer<?> container : containers) {
      if (!container.isRunning()) continue;
      try {
        final JSONObject cluster = getClusterInfo(container);
        if (cluster.has("isLeader") && cluster.getBoolean("isLeader"))
          return container;
      } catch (final Exception ignored) {}
    }
    return null;
  }

  /**
   * Creates a RemoteDatabase connected to a specific container.
   */
  protected RemoteDatabase createRemoteDatabase(final GenericContainer<?> container) {
    return new RemoteDatabase(container.getHost(), container.getMappedPort(HTTP_PORT),
        DATABASE_NAME, "root", ROOT_PASSWORD);
  }

  protected String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + ROOT_PASSWORD).getBytes());
  }

  private String buildServerList(final int size) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      if (!sb.isEmpty()) sb.append(",");
      sb.append("arcadedb-").append(i).append(":").append(RAFT_PORT);
    }
    return sb.toString();
  }
}
