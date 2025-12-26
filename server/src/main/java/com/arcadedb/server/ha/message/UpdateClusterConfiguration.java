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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.HAServer;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class UpdateClusterConfiguration extends HAAbstractCommand {
  private HAServer.HACluster cluster;
  private Map<HAServer.ServerInfo, String> httpAddresses;

  // Constructor for serialization
  public UpdateClusterConfiguration() {
  }

  /**
   * Creates a new UpdateClusterConfiguration message with cluster and HTTP addresses.
   *
   * @param cluster the HACluster containing server information
   * @param httpAddresses map of ServerInfo to HTTP address (can be null)
   */
  public UpdateClusterConfiguration(final HAServer.HACluster cluster, final Map<HAServer.ServerInfo, String> httpAddresses) {
    this.cluster = cluster;
    this.httpAddresses = httpAddresses;
  }

  /**
   * Legacy constructor for backward compatibility.
   * @deprecated Use {@link #UpdateClusterConfiguration(HAServer.HACluster, Map)} instead
   */
  @Deprecated
  public UpdateClusterConfiguration(final HAServer.HACluster cluster) {
    this(cluster, null);
  }

  @Override
  public HACommand execute(final HAServer server, final HAServer.ServerInfo remoteServer, final long messageNumber) {
    LogManager.instance().log(this, Level.INFO, "Updating server list=%s from `%s` ", cluster, remoteServer);
    server.setServerAddresses(cluster);

    // Update HTTP addresses if present
    if (httpAddresses != null && !httpAddresses.isEmpty()) {
      for (Map.Entry<HAServer.ServerInfo, String> entry : httpAddresses.entrySet()) {
        server.setReplicaHTTPAddress(entry.getKey(), entry.getValue());
        LogManager.instance().log(this, Level.FINE, "Updated HTTP address for %s: %s",
            entry.getKey().alias(), entry.getValue());
      }
    }

    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    // Serialize server list
    stream.putString(cluster.getServers().stream()
        .map(HAServer.ServerInfo::toString)
        .collect(Collectors.joining(",")));

    // Serialize HTTP addresses if present
    if (httpAddresses != null && !httpAddresses.isEmpty()) {
      final StringBuilder httpAddressesStr = new StringBuilder();
      for (Map.Entry<HAServer.ServerInfo, String> entry : httpAddresses.entrySet()) {
        if (httpAddressesStr.length() > 0) {
          httpAddressesStr.append(",");
        }
        httpAddressesStr.append(entry.getKey().alias())
            .append("=")
            .append(entry.getValue());
      }
      stream.putString(httpAddressesStr.toString());
    } else {
      // Empty string for backward compatibility
      stream.putString("");
    }
  }

  @Override
  public void fromStream(final ArcadeDBServer server, final Binary stream) {
    // Deserialize server list
    cluster = new HAServer.HACluster(server.getHA().parseServerList(stream.getString()));

    // Deserialize HTTP addresses if present (backward compatible)
    try {
      final String httpAddressesStr = stream.getString();
      if (httpAddressesStr != null && !httpAddressesStr.isEmpty()) {
        httpAddresses = new HashMap<>();
        final String[] entries = httpAddressesStr.split(",");
        for (String entry : entries) {
          final String[] parts = entry.split("=", 2);
          if (parts.length == 2) {
            final String alias = parts[0];
            final String httpAddress = parts[1];

            // Find the ServerInfo by alias
            for (HAServer.ServerInfo serverInfo : cluster.getServers()) {
              if (serverInfo.alias().equals(alias)) {
                httpAddresses.put(serverInfo, httpAddress);
                break;
              }
            }
          }
        }
      }
    } catch (Exception e) {
      // Backward compatibility: if reading HTTP addresses fails, just log and continue
      LogManager.instance().log(this, Level.FINE,
          "No HTTP addresses in cluster configuration (backward compatibility mode)");
      httpAddresses = null;
    }
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder("updateClusterConfig(servers=").append(cluster);
    if (httpAddresses != null && !httpAddresses.isEmpty()) {
      result.append(", httpAddresses=").append(httpAddresses.size()).append(" entries");
    }
    result.append(")");
    return result.toString();
  }
}
