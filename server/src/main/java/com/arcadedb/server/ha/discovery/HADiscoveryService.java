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

import com.arcadedb.server.ha.HAServer;

import java.util.Set;

/**
 * Service interface for discovering nodes in a High Availability cluster.
 * This interface provides a pluggable mechanism for different discovery strategies
 * in various cloud and container environments (Kubernetes, Docker, Consul, etc.).
 *
 * <p>Implementations should be thread-safe as they may be called from multiple threads
 * during cluster formation and maintenance.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface HADiscoveryService {

  /**
   * Discovers all available nodes in the specified cluster.
   * This method is called during cluster initialization and periodically
   * to detect new nodes joining the cluster or nodes leaving the cluster.
   *
   * @param clusterName the name of the cluster to discover nodes for
   * @return a set of ServerInfo objects representing discovered nodes,
   *         or an empty set if no nodes are discovered
   * @throws DiscoveryException if discovery fails due to network or service errors
   */
  Set<HAServer.ServerInfo> discoverNodes(String clusterName) throws DiscoveryException;

  /**
   * Registers the current node with the discovery service.
   * This allows other nodes to discover this node when they perform discovery.
   * This method is typically called during server startup.
   *
   * @param self the ServerInfo representing the current node
   * @throws DiscoveryException if registration fails
   */
  void registerNode(HAServer.ServerInfo self) throws DiscoveryException;

  /**
   * Deregisters the current node from the discovery service.
   * This should be called during graceful shutdown to inform other nodes
   * that this node is leaving the cluster.
   *
   * @param self the ServerInfo representing the current node
   * @throws DiscoveryException if deregistration fails
   */
  void deregisterNode(HAServer.ServerInfo self) throws DiscoveryException;

  /**
   * Returns the name of this discovery service implementation.
   * This is useful for logging and configuration purposes.
   *
   * @return the discovery service name (e.g., "static", "kubernetes", "consul")
   */
  String getName();
}
