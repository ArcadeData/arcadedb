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
package com.arcadedb.server.ha.discovery;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.HAServer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

/**
 * Static list-based discovery service implementation.
 * This is the traditional discovery mechanism that uses a pre-configured
 * static list of server addresses. It's suitable for environments where
 * cluster membership is known in advance and doesn't change dynamically.
 *
 * <p>This implementation is thread-safe and can be used as the default
 * discovery mechanism for backward compatibility with existing configurations.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class StaticListDiscovery implements HADiscoveryService {

  private final Set<HAServer.ServerInfo> staticServerList;

  /**
   * Creates a new static list discovery service with the given server list.
   *
   * @param servers the set of server addresses to use for discovery
   * @throws IllegalArgumentException if servers is null
   */
  public StaticListDiscovery(Set<HAServer.ServerInfo> servers) {
    if (servers == null) {
      throw new IllegalArgumentException("Server list cannot be null");
    }
    this.staticServerList = Collections.unmodifiableSet(new HashSet<>(servers));
    LogManager.instance()
        .log(this, Level.INFO, "Initialized static discovery with %d servers: %s",
            staticServerList.size(), staticServerList);
  }

  /**
   * Creates a new static list discovery service with a comma-separated list of servers.
   * Server addresses should be in the format: {alias}host:port
   *
   * @param serverListString comma-separated list of server addresses
   * @throws IllegalArgumentException if serverListString is null or empty
   */
  public StaticListDiscovery(String serverListString) {
    if (serverListString == null || serverListString.trim().isEmpty()) {
      throw new IllegalArgumentException("Server list string cannot be null or empty");
    }

    Set<HAServer.ServerInfo> servers = new HashSet<>();
    String[] serverAddresses = serverListString.split(",");
    for (String address : serverAddresses) {
      address = address.trim();
      if (!address.isEmpty()) {
        servers.add(HAServer.ServerInfo.fromString(address));
      }
    }

    if (servers.isEmpty()) {
      throw new IllegalArgumentException("Server list string must contain at least one valid server address");
    }

    this.staticServerList = Collections.unmodifiableSet(servers);
    LogManager.instance()
        .log(this, Level.INFO, "Initialized static discovery with %d servers: %s",
            staticServerList.size(), staticServerList);
  }

  @Override
  public Set<HAServer.ServerInfo> discoverNodes(String clusterName) throws DiscoveryException {
    LogManager.instance()
        .log(this, Level.FINE, "Discovering nodes for cluster '%s' - returning %d static servers",
            clusterName, staticServerList.size());
    return new HashSet<>(staticServerList);
  }

  @Override
  public void registerNode(HAServer.ServerInfo self) throws DiscoveryException {
    // No-op for static discovery - nodes are pre-configured
    LogManager.instance()
        .log(this, Level.FINE, "Node registration is not required for static discovery: %s", self);
  }

  @Override
  public void deregisterNode(HAServer.ServerInfo self) throws DiscoveryException {
    // No-op for static discovery - nodes are pre-configured
    LogManager.instance()
        .log(this, Level.FINE, "Node deregistration is not required for static discovery: %s", self);
  }

  @Override
  public String getName() {
    return "static";
  }

  /**
   * Returns the configured server list.
   * This is useful for debugging and validation purposes.
   *
   * @return an unmodifiable set of configured servers
   */
  public Set<HAServer.ServerInfo> getConfiguredServers() {
    return staticServerList;
  }

  @Override
  public String toString() {
    return "StaticListDiscovery{servers=" + staticServerList.size() + "}";
  }
}
