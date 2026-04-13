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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Parses the HA server list into Raft peers and maintains the mapping from Raft peer IDs to HTTP
 * addresses. Extracted from {@link RaftHAServer} to separate address-resolution concerns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftPeerAddressResolver {

  private final ArcadeDBServer      server;
  private final ContextConfiguration configuration;

  /** Peer ID (e.g. "host_raftPort") -> HTTP address (e.g. "host:httpPort"). */
  private final Map<String, String> peerHttpAddresses    = new ConcurrentHashMap<>();
  /** Tracks peers for which we already emitted a "derived address" warning, to avoid log spam. */
  private final Set<String>         derivedAddressWarned = ConcurrentHashMap.newKeySet();

  public RaftPeerAddressResolver(final ArcadeDBServer server, final ContextConfiguration configuration) {
    this.server = server;
    this.configuration = configuration;
  }

  /**
   * Parses a comma-separated server list into Raft peers and populates the HTTP address mapping.
   * <p>
   * Supported entry formats:
   * <ul>
   *   <li>{@code host:raftPort:httpPort:priority}</li>
   *   <li>{@code host:raftPort:httpPort}</li>
   *   <li>{@code host:raftPort} - HTTP address derived from local port offset</li>
   * </ul>
   */
  public List<RaftPeer> parsePeers(final String serverList) {
    final List<RaftPeer> peers = new ArrayList<>();
    final int httpPortOffset = getHttpPortOffset();

    for (final String entry : serverList.split(",")) {
      final String trimmed = entry.trim();
      if (trimmed.isEmpty())
        continue;

      final String[] parts = parseHostPort(trimmed);
      final String host = parts[0];
      final int raftPort = Integer.parseInt(parts[1]);
      final String raftAddress = host + ":" + raftPort;

      final String httpAddress;
      if (parts.length >= 3) {
        httpAddress = host + ":" + parts[2];
      } else {
        httpAddress = host + ":" + (raftPort + httpPortOffset);
        LogManager.instance().log(this, Level.INFO,
            "Peer '%s:%d': no explicit HTTP port in HA_SERVER_LIST, deriving HTTP address %s using local port offset (%+d). "
                + "Use format 'host:raftPort:httpPort' if peers have different port layouts",
            host, raftPort, httpAddress, httpPortOffset);
      }

      int priority = 0;
      if (parts.length >= 4) {
        try {
          priority = Integer.parseInt(parts[3]);
        } catch (final NumberFormatException e) {
          throw new ConfigurationException("Invalid priority value '" + parts[3] + "' in peer address '" + trimmed + "'");
        }
      }

      // Use underscore in peer ID to avoid JMX ObjectName issues (colon is invalid in JMX values)
      final String peerIdStr = host + "_" + raftPort;
      final RaftPeerId peerId = RaftPeerId.valueOf(peerIdStr);
      peers.add(RaftPeer.newBuilder().setId(peerId).setAddress(raftAddress).setPriority(priority).build());

      // Store HTTP address separately (NOT on RaftPeer.clientAddress which Ratis uses for gRPC)
      peerHttpAddresses.put(peerIdStr, httpAddress);
    }

    if (peers.size() < 3)
      LogManager.instance().log(this, Level.WARNING,
          "Ratis HA cluster has less than 3 peers (%d). A minimum of 3 is recommended for fault tolerance",
          peers.size());

    return peers;
  }

  /**
   * Resolves which peer in the list corresponds to this server instance.
   * Matching order:
   * <ol>
   *   <li>Exact peer ID match using incoming host + port (e.g., "myhost_2424")</li>
   *   <li>Server name match (e.g., "arcadedb-0" matches "arcadedb-0_2424")</li>
   *   <li>Hostname match via {@code InetAddress.getLocalHost()}</li>
   *   <li>Port-only match (only if a single peer uses this port, to avoid ambiguity)</li>
   * </ol>
   */
  public RaftPeerId resolveLocalPeerId(final List<RaftPeer> peers) {
    final String localHost = configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST);
    final int localPort = parseFirstPort(configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));

    // 1. Exact match: peer ID = incomingHost_port
    final String exactId = localHost + "_" + localPort;
    for (final RaftPeer peer : peers)
      if (peer.getId().toString().equals(exactId))
        return peer.getId();

    // 2. Match by server name
    final String serverName = server.getServerName();
    if (serverName != null && !serverName.isEmpty()) {
      final String serverNameId = serverName + "_" + localPort;
      for (final RaftPeer peer : peers)
        if (peer.getId().toString().equals(serverNameId))
          return peer.getId();
    }

    // 3. Match by hostname
    try {
      final String hostname = java.net.InetAddress.getLocalHost().getHostName();
      final String hostnameId = hostname + "_" + localPort;
      for (final RaftPeer peer : peers)
        if (peer.getId().toString().equals(hostnameId))
          return peer.getId();
    } catch (final java.net.UnknownHostException ignored) {
    }

    // 4. Fallback: port-only match (unambiguous only when a single peer uses this port)
    RaftPeerId portMatch = null;
    int portMatchCount = 0;
    for (final RaftPeer peer : peers) {
      final String address = peer.getAddress();
      if (address != null && address.endsWith(":" + localPort)) {
        portMatch = peer.getId();
        portMatchCount++;
      }
    }
    if (portMatchCount == 1)
      return portMatch;

    throw new ConfigurationException(
        "Cannot find local server in HA_SERVER_LIST. serverName=" + serverName
            + ", localAddress=" + localHost + ":" + localPort + ", server list: " + peers);
  }

  /**
   * Returns the HTTP address of a peer given its Raft peer ID.
   * If no explicit mapping exists, derives the HTTP address from the peer ID using the port offset.
   * Derived addresses are NOT cached so a wrong derivation does not persist permanently.
   */
  public String getPeerHTTPAddress(final RaftPeerId peerId) {
    final String httpAddr = peerHttpAddresses.get(peerId.toString());
    if (httpAddr != null)
      return httpAddr;

    final String peerIdStr = peerId.toString();
    final int lastUnderscore = peerIdStr.lastIndexOf('_');
    if (lastUnderscore > 0 && lastUnderscore < peerIdStr.length() - 1) {
      try {
        final int raftPort = Integer.parseInt(peerIdStr.substring(lastUnderscore + 1));
        final String host = peerIdStr.substring(0, lastUnderscore);
        final String derived = host + ":" + (raftPort + getHttpPortOffset());
        if (derivedAddressWarned.add(peerIdStr))
          LogManager.instance().log(this, Level.WARNING,
              "No explicit HTTP address for peer '%s', deriving %s using local HTTP/Raft port offset (%+d). "
                  + "Specify explicit HTTP ports in HA_SERVER_LIST (format: host:raftPort:httpPort)",
              peerIdStr, derived, getHttpPortOffset());
        return derived;
      } catch (final NumberFormatException ignored) {
      }
    }
    return peerIdStr;
  }

  /** Returns the HTTP address for the given leader peer name. */
  public String getLeaderHTTPAddress(final String leaderName) {
    if (leaderName == null)
      return null;
    return getPeerHTTPAddress(RaftPeerId.valueOf(leaderName));
  }

  /**
   * Registers an explicit HTTP address for a dynamically added peer.
   * If no HTTP address is given, derives it from the Raft address and logs a warning.
   */
  public void registerPeerHttpAddress(final String peerId, final String raftAddress, final String httpAddress) {
    if (httpAddress != null && !httpAddress.isEmpty()) {
      peerHttpAddresses.put(peerId, httpAddress);
    } else {
      try {
        final String[] addrParts = parseHostPort(raftAddress);
        final int raftPort = Integer.parseInt(addrParts[1]);
        final String derived = addrParts[0] + ":" + (raftPort + getHttpPortOffset());
        peerHttpAddresses.put(peerId, derived);
        LogManager.instance().log(this, Level.WARNING,
            "Dynamically added peer '%s': no HTTP address provided, derived as %s using local port offset (%+d). "
                + "Use 'httpAddress' parameter for explicit control",
            peerId, derived, getHttpPortOffset());
      } catch (final ConfigurationException | NumberFormatException ignored) {
        LogManager.instance().log(this, Level.WARNING,
            "Dynamically added peer '%s': could not derive HTTP address from '%s'", peerId, raftAddress);
      }
    }
  }

  public int getHttpPortOffset() {
    final int localHttpPort = parseFirstPort(configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT));
    final int localRaftPort = parseFirstPort(configuration.getValueAsString(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS));
    return localHttpPort - localRaftPort;
  }

  /**
   * Validates that the given address is a well-formed host:port string with a port in range 1-65535.
   */
  public static void validatePeerAddress(final String address) {
    final String[] parts = parseHostPort(address);

    if (parts[0].isEmpty())
      throw new ConfigurationException("HA peer address has empty host: " + address);

    final String portStr = parts[1];
    final int port;
    try {
      port = Integer.parseInt(portStr);
    } catch (final NumberFormatException e) {
      throw new ConfigurationException("HA peer address has non-numeric port '" + portStr + "': " + address);
    }
    if (port < 1 || port > 65535)
      throw new ConfigurationException("HA peer address port out of range (must be 1-65535): " + port);
  }

  /**
   * Parses a host:port string, supporting IPv4/hostname and bracketed IPv6 notation.
   * Returns an array where [0]=host, [1]=first port, and optionally [2]=second port, [3]=third port.
   */
  public static String[] parseHostPort(final String address) {
    if (address == null || address.isEmpty())
      throw new ConfigurationException("HA peer address is empty");

    if (address.startsWith("[")) {
      // Bracketed IPv6: [addr]:port or [addr]:port:extraPort
      final int closeBracket = address.indexOf(']');
      if (closeBracket < 0)
        throw new ConfigurationException("Invalid IPv6 address (missing closing bracket): " + address);

      final String host = address.substring(0, closeBracket + 1);
      final String remainder = address.substring(closeBracket + 1);
      if (remainder.isEmpty() || remainder.charAt(0) != ':')
        throw new ConfigurationException("HA peer address missing port after IPv6 host: " + address);

      final String[] ports = remainder.substring(1).split(":");
      final String[] result = new String[1 + ports.length];
      result[0] = host;
      System.arraycopy(ports, 0, result, 1, ports.length);
      return result;
    }

    // Detect bare (un-bracketed) IPv6: more than 2 colons and no dots
    final long colonCount = address.chars().filter(c -> c == ':').count();
    if (colonCount > 2 && !address.contains("."))
      throw new ConfigurationException(
          "IPv6 addresses must use bracketed notation (e.g., [::1]:2424) in HA peer address: " + address);

    final String[] parts = address.split(":");
    if (parts.length < 2)
      throw new ConfigurationException("HA peer address missing port: " + address);

    return parts;
  }

  /**
   * Parses the first port from a port spec that may contain a range (e.g. "2424-2430") or
   * a comma-separated list (e.g. "2424,2425").
   */
  public static int parseFirstPort(final String portSpec) {
    if (portSpec.contains("-"))
      return Integer.parseInt(portSpec.split("-")[0].trim());
    if (portSpec.contains(","))
      return Integer.parseInt(portSpec.split(",")[0].trim());
    return Integer.parseInt(portSpec.trim());
  }
}
