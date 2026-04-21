/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the HA_SERVER_LIST configuration into Raft peers and HTTP address mappings.
 */
final class RaftPeerAddressResolver {

  /**
   * Result of parsing the HA server list. Contains the Raft peers (with raft addresses) and
   * a map from peer ID to HTTP address for replica-to-leader HTTP command forwarding.
   * The {@code httpAddresses} map is empty when no httpPort is specified in the server list.
   */
  public record ParsedPeerList(List<RaftPeer> peers, Map<RaftPeerId, String> httpAddresses) {
  }

  private RaftPeerAddressResolver() {
  }

  /**
   * Parses a comma-separated server list into a {@link ParsedPeerList}.
   * <p>
   * Each entry supports the following formats:
   * <ul>
   *   <li>{@code host:raftPort:httpPort:priority} - explicit Raft port, HTTP port, and leader-election priority</li>
   *   <li>{@code host:raftPort:httpPort} - explicit Raft and HTTP ports, priority defaults to 0</li>
   *   <li>{@code host:raftPort} - explicit Raft port, no HTTP address stored, priority defaults to 0</li>
   *   <li>{@code host} - Raft port defaults to {@code defaultPort}, no HTTP address, priority defaults to 0</li>
   * </ul>
   * The {@code httpAddresses} map in the result is populated only for entries with 3 or 4 parts;
   * it is keyed by the {@link RaftPeerId} of each peer.
   * <p>
   * Priority is used for Raft leader election: the node with the highest priority is preferred as leader.
   * This is a soft preference - if the preferred leader is unavailable, another node will take over.
   */
  static ParsedPeerList parsePeerList(final String serverList, final int defaultPort) {
    final String[] entries = serverList.split(",");
    final List<RaftPeer> peers = new ArrayList<>(entries.length);
    final Map<RaftPeerId, String> httpAddresses = new HashMap<>(entries.length);

    for (int i = 0; i < entries.length; i++) {
      final String entry = entries[i].trim();
      final String[] parts = entry.split(":");

      if (parts.length > 4 || parts.length == 0 || parts[0].isBlank())
        throw new ServerException(
            "Invalid peer address format '" + entry + "'. Expected host[:raftPort[:httpPort[:priority]]]");

      final String raftAddress;
      String httpAddress = null;
      int priority = 0;

      if (parts.length == 4) {
        // host:raftPort:httpPort:priority
        raftAddress = parts[0] + ":" + parts[1];
        httpAddress = parts[0] + ":" + parts[2];
        try {
          priority = Integer.parseInt(parts[3]);
        } catch (final NumberFormatException e) {
          throw new ServerException("Invalid priority value '" + parts[3] + "' in peer address '" + entry + "'");
        }
      } else if (parts.length == 3) {
        // host:raftPort:httpPort
        raftAddress = parts[0] + ":" + parts[1];
        httpAddress = parts[0] + ":" + parts[2];
      } else if (parts.length == 2) {
        // host:raftPort
        raftAddress = entry;
      } else {
        // host only - use default Raft port
        raftAddress = entry + ":" + defaultPort;
      }

      // Use host_raftPort as peer ID (underscore avoids JMX ObjectName issues with colons)
      final String peerIdStr = raftAddress.replace(':', '_');
      final RaftPeer peer = RaftPeer.newBuilder()
          .setId(peerIdStr)
          .setAddress(raftAddress)
          .setPriority(priority)
          .build();
      peers.add(peer);

      if (httpAddress != null)
        httpAddresses.put(peer.getId(), httpAddress);
    }

    // Validate: mixing localhost/127.0.0.1 with non-localhost addresses is a misconfiguration
    boolean hasLocalhost = false;
    boolean hasNonLocalhost = false;
    for (final RaftPeer peer : peers) {
      final String host = peer.getAddress().split(":")[0].trim();
      if (host.equals("localhost") || host.equals("127.0.0.1"))
        hasLocalhost = true;
      else
        hasNonLocalhost = true;
    }
    if (hasLocalhost && hasNonLocalhost)
      throw new ServerException(
          "Found a localhost (127.0.0.1) in the server list among non-localhost servers. "
              + "Please fix the server list configuration.");

    return new ParsedPeerList(Collections.unmodifiableList(peers), Collections.unmodifiableMap(httpAddresses));
  }

  /**
   * Determines the local peer ID by parsing the numeric suffix from the server name.
   * For example, "arcadedb-0" or "ArcadeDB_0" maps to index 0 in the peer list.
   */
  static RaftPeerId findLocalPeerId(final List<RaftPeer> peers, final String serverName,
      final ArcadeDBServer server) {
    final int separatorIdx = findLastSeparatorIndex(serverName);
    final int index = Integer.parseInt(serverName.substring(separatorIdx + 1));
    if (index < 0 || index >= peers.size())
      throw new IllegalArgumentException(
          "Server index " + index + " from name '" + serverName + "' is out of range [0, " + peers.size() + ")");

    return peers.get(index).getId();
  }

  /**
   * Validates a peer address of the form {@code host:port} or {@code [ipv6]:port}.
   * Throws {@link ConfigurationException} if the address is malformed.
   */
  static void validatePeerAddress(final String address) {
    if (address == null || address.isBlank())
      throw new ConfigurationException("Peer address must not be empty");

    final int colonIdx;
    if (address.startsWith("[")) {
      // IPv6 bracketed form: [::1]:port
      final int closingBracket = address.indexOf(']');
      if (closingBracket < 0 || closingBracket + 1 >= address.length() || address.charAt(closingBracket + 1) != ':')
        throw new ConfigurationException("Invalid IPv6 peer address: " + address);
      colonIdx = closingBracket + 1;
    } else {
      colonIdx = address.lastIndexOf(':');
    }

    if (colonIdx < 0)
      throw new ConfigurationException("Peer address missing port: " + address);

    final String host = address.substring(0, colonIdx);
    if (host.isBlank())
      throw new ConfigurationException("Peer address has empty host: " + address);

    final String portStr = address.substring(colonIdx + 1);
    final int port;
    try {
      port = Integer.parseInt(portStr);
    } catch (final NumberFormatException e) {
      throw new ConfigurationException("Invalid port in peer address '" + address + "': " + portStr);
    }
    if (port < 1 || port > 65535)
      throw new ConfigurationException("port out of range [1,65535] in peer address '" + address + "': " + port);
  }

  /**
   * Finds the index of the last separator character ({@code '-'} or {@code '_'}) in the server name.
   * Server names follow the pattern {@code prefix-N} or {@code prefix_N} where N is the node index.
   */
  static int findLastSeparatorIndex(final String serverName) {
    final int hyphenIdx = serverName.lastIndexOf('-');
    final int underscoreIdx = serverName.lastIndexOf('_');
    final int idx = Math.max(hyphenIdx, underscoreIdx);
    if (idx < 0 || idx == serverName.length() - 1)
      throw new IllegalArgumentException("Cannot parse server index from server name: " + serverName);
    return idx;
  }
}
