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
   * The {@code httpsAddresses} map is empty when no httpsPort (the optional 5th field) is specified;
   * it is used for encrypted peer-to-peer transfers (e.g. snapshot download) when SSL is enabled.
   * The {@code peerNames} map contains user-provided peer names from the optional {@code name@}
   * prefix and is empty when no entry uses that syntax.
   * The {@code boltAddresses} map is empty when no entry declares the object-form {@code bolt} field;
   * it holds the client-reachable Bolt address advertised in the ROUTE routing table.
   */
  public record ParsedPeerList(List<RaftPeer> peers, Map<RaftPeerId, String> httpAddresses,
                               Map<RaftPeerId, String> httpsAddresses, Map<RaftPeerId, String> peerNames,
                               Map<RaftPeerId, String> boltAddresses) {
  }

  private RaftPeerAddressResolver() {
  }

  /**
   * Parses a comma-separated server list into a {@link ParsedPeerList}.
   * <p>
   * Each entry supports two interchangeable syntaxes.
   * <p>
   * <b>1. Object form</b> (recommended for readability): {@code host:{raft:2434,http:2480,https:2490,bolt:7687,priority:10}}.
   * The fields are unordered and all optional except that {@code raft} defaults to {@code defaultPort}
   * when omitted ({@code http}/{@code https}/{@code bolt} are simply not stored when absent, {@code priority}
   * defaults to 0). This avoids the positional ambiguity of the colon form. Example:
   * {@code frankfurt@db1:{raft:2434,http:2480,https:2490,bolt:7687}}. The {@code bolt} field is the
   * client-reachable Bolt address advertised in the ROUTE routing table and is available only in the object
   * form (the positional colon form has no Bolt field).
   * <p>
   * <b>2. Positional (colon) form</b>:
   * <ul>
   *   <li>{@code host:raftPort:httpPort:priority:httpsPort} - explicit Raft port, HTTP port,
   *       leader-election priority, and HTTPS port (used for encrypted peer-to-peer transfers when
   *       {@code arcadedb.ssl.enabled} is true)</li>
   *   <li>{@code host:raftPort:httpPort:priority} - explicit Raft port, HTTP port, and leader-election priority</li>
   *   <li>{@code host:raftPort:httpPort} - explicit Raft and HTTP ports, priority defaults to 0</li>
   *   <li>{@code host:raftPort} - explicit Raft port, no HTTP address stored, priority defaults to 0</li>
   *   <li>{@code host} - Raft port defaults to {@code defaultPort}, no HTTP address, priority defaults to 0</li>
   * </ul>
   * Both forms may be prefixed with an optional human-readable peer name using the {@code name@} syntax
   * (e.g. {@code frankfurt@host:2434:2480}). Names must be unique within the cluster but are otherwise
   * free-form. Names allow operators to identify nodes in logs and Studio without relying on the
   * {@code prefix_N} server-name convention required for positional resolution. The two forms may be
   * freely mixed within the same comma-separated list.
   * <p>
   * The {@code httpAddresses} map in the result is populated only when an HTTP port is given;
   * the {@code httpsAddresses} map only when an HTTPS port is given. Both are keyed by the
   * {@link RaftPeerId} of each peer. The HTTPS port is optional: when omitted, a homogeneous cluster
   * can still transfer snapshots over HTTPS because the address is derived from this node's local
   * HTTPS listening port (see {@code RaftHAServer#resolveHttpsAddress}).
   * <p>
   * Priority is used for Raft leader election: the node with the highest priority is preferred as leader.
   * This is a soft preference - if the preferred leader is unavailable, another node will take over.
   */
  static ParsedPeerList parsePeerList(final String serverList, final int defaultPort) {
    return parsePeerList(serverList, defaultPort, "");
  }

  /**
   * Same as {@link #parsePeerList(String, int)} but additionally appends a Kubernetes DNS suffix to
   * every peer host so short pod names (e.g. {@code arcadedb-0}) written in the server list expand to
   * the cluster-internal FQDN, consistent with the self-advertised host built in
   * {@code ArcadeDBServer.assignHostAddress}. The suffix is applied to the Raft, HTTP and HTTPS host
   * components. It is a no-op when {@code k8sDnsSuffix} is empty, when a host is already fully
   * qualified with that suffix (idempotent), and for raw IP literals or {@code localhost}, which need
   * no DNS resolution.
   */
  static ParsedPeerList parsePeerList(final String serverList, final int defaultPort, final String k8sDnsSuffix) {
    // Split on top-level commas only: the object form {raft:..,http:..} contains commas that must not
    // be treated as entry separators.
    final List<String> entries = splitEntries(serverList);
    final List<RaftPeer> peers = new ArrayList<>(entries.size());
    final Map<RaftPeerId, String> httpAddresses = new HashMap<>(entries.size());
    final Map<RaftPeerId, String> httpsAddresses = new HashMap<>(entries.size());
    final Map<RaftPeerId, String> boltAddresses = new HashMap<>(entries.size());
    final Map<RaftPeerId, String> peerNames = new HashMap<>(entries.size());
    final Map<String, String> nameToEntry = new HashMap<>(entries.size());

    for (final String rawEntry : entries) {
      String entry = rawEntry.trim();

      String peerName = null;
      final int atIdx = entry.indexOf('@');
      if (atIdx >= 0) {
        if (entry.indexOf('@', atIdx + 1) >= 0)
          throw new ServerException("Invalid peer address '" + entry + "': only one '@' allowed");
        peerName = entry.substring(0, atIdx).trim();
        if (peerName.isEmpty())
          throw new ServerException("Invalid peer address '" + entry + "': peer name before '@' is blank");
        final String previous = nameToEntry.put(peerName, entry);
        if (previous != null)
          throw new ServerException(
              "Duplicate peer name '" + peerName + "' in entries '" + previous + "' and '" + entry + "'");
        entry = entry.substring(atIdx + 1).trim();
      }

      final String raftAddress;
      String httpAddress = null;
      String httpsAddress = null;
      String boltAddress = null;
      int priority = 0;

      final int braceIdx = entry.indexOf('{');
      if (braceIdx >= 0) {
        // Object form: host:{raft:2434,http:2480,https:2490,priority:10}
        final PeerSpec spec = parseObjectForm(entry, braceIdx, defaultPort);
        final String host = applyDnsSuffix(spec.host, k8sDnsSuffix);
        raftAddress = host + ":" + spec.raftPort;
        if (spec.httpPort != null)
          httpAddress = host + ":" + spec.httpPort;
        if (spec.httpsPort != null)
          httpsAddress = host + ":" + spec.httpsPort;
        if (spec.boltPort != null)
          boltAddress = host + ":" + spec.boltPort;
        priority = spec.priority;
      } else {
        final String[] parts = entry.split(":");

        if (parts.length > 5 || parts.length == 0 || parts[0].isBlank())
          throw new ServerException(
              "Invalid peer address format '" + entry
                  + "'. Expected [name@]host[:raftPort[:httpPort[:priority[:httpsPort]]]] "
                  + "or [name@]host:{raft:..,http:..,https:..,priority:..}");

        final String host = applyDnsSuffix(parts[0], k8sDnsSuffix);

        if (parts.length == 5) {
          // host:raftPort:httpPort:priority:httpsPort
          raftAddress = host + ":" + parts[1];
          httpAddress = host + ":" + parts[2];
          priority = parseIntField(parts[3], "priority", entry);
          httpsAddress = host + ":" + parts[4];
        } else if (parts.length == 4) {
          // host:raftPort:httpPort:priority
          raftAddress = host + ":" + parts[1];
          httpAddress = host + ":" + parts[2];
          priority = parseIntField(parts[3], "priority", entry);
        } else if (parts.length == 3) {
          // host:raftPort:httpPort
          raftAddress = host + ":" + parts[1];
          httpAddress = host + ":" + parts[2];
        } else if (parts.length == 2) {
          // host:raftPort
          raftAddress = host + ":" + parts[1];
        } else {
          // host only - use default Raft port
          raftAddress = host + ":" + defaultPort;
        }
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
      if (httpsAddress != null)
        httpsAddresses.put(peer.getId(), httpsAddress);
      if (boltAddress != null)
        boltAddresses.put(peer.getId(), boltAddress);
      if (peerName != null)
        peerNames.put(peer.getId(), peerName);
    }

    // Validate: mixing localhost/127.0.0.1 with non-localhost addresses is a misconfiguration
    boolean hasLocalhost = false;
    boolean hasNonLocalhost = false;
    for (final RaftPeer peer : peers) {
      final String host = peer.getAddress().split(":")[0].trim();
      if ("localhost".equals(host) || "127.0.0.1".equals(host))
        hasLocalhost = true;
      else
        hasNonLocalhost = true;
    }
    if (hasLocalhost && hasNonLocalhost)
      throw new ServerException(
          """
          Found a localhost (127.0.0.1) in the server list among non-localhost servers. \
          Please fix the server list configuration.""");

    return new ParsedPeerList(
        Collections.unmodifiableList(peers),
        Collections.unmodifiableMap(httpAddresses),
        Collections.unmodifiableMap(httpsAddresses),
        Collections.unmodifiableMap(peerNames),
        Collections.unmodifiableMap(boltAddresses));
  }

  /** Resolved ports for a single peer entry, regardless of the syntax it was written in. */
  private static final class PeerSpec {
    final String  host;
    final int     raftPort;
    final Integer httpPort;  // null when not specified
    final Integer httpsPort; // null when not specified
    final Integer boltPort;  // null when not specified
    final int     priority;

    PeerSpec(final String host, final int raftPort, final Integer httpPort, final Integer httpsPort, final Integer boltPort,
        final int priority) {
      this.host = host;
      this.raftPort = raftPort;
      this.httpPort = httpPort;
      this.httpsPort = httpsPort;
      this.boltPort = boltPort;
      this.priority = priority;
    }
  }

  /**
   * Parses the object form {@code host:{raft:..,http:..,https:..,bolt:..,priority:..}} into a {@link PeerSpec}.
   * Fields are unordered and all optional; {@code raft} defaults to {@code defaultPort}. Unknown or
   * duplicated keys throw {@link ServerException}.
   */
  private static PeerSpec parseObjectForm(final String entry, final int braceIdx, final int defaultPort) {
    if (!entry.endsWith("}"))
      throw new ServerException("Invalid peer address '" + entry + "': missing closing '}'");

    String host = entry.substring(0, braceIdx).trim();
    if (host.endsWith(":"))
      host = host.substring(0, host.length() - 1).trim();
    if (host.isBlank())
      throw new ServerException("Invalid peer address '" + entry + "': host before '{' is blank");

    final String body = entry.substring(braceIdx + 1, entry.length() - 1).trim();

    int raftPort = defaultPort;
    Integer httpPort = null;
    Integer httpsPort = null;
    Integer boltPort = null;
    int priority = 0;
    final Map<String, String> seen = new HashMap<>();

    if (!body.isEmpty()) {
      for (final String kv : body.split(",")) {
        final String pair = kv.trim();
        if (pair.isEmpty())
          continue;
        final int colon = pair.indexOf(':');
        if (colon < 0)
          throw new ServerException("Invalid field '" + pair + "' in peer address '" + entry + "': expected key:value");
        final String key = pair.substring(0, colon).trim().toLowerCase();
        final String value = pair.substring(colon + 1).trim();
        if (seen.put(key, value) != null)
          throw new ServerException("Duplicate key '" + key + "' in peer address '" + entry + "'");

        switch (key) {
        case "raft" -> raftPort = parseIntField(value, "raft", entry);
        case "http" -> httpPort = parseIntField(value, "http", entry);
        case "https" -> httpsPort = parseIntField(value, "https", entry);
        case "bolt" -> boltPort = parseIntField(value, "bolt", entry);
        case "priority" -> priority = parseIntField(value, "priority", entry);
        default -> throw new ServerException(
            "Unknown key '" + key + "' in peer address '" + entry + "'. Supported keys: raft, http, https, bolt, priority");
        }
      }
    }
    return new PeerSpec(host, raftPort, httpPort, httpsPort, boltPort, priority);
  }

  /**
   * Appends the Kubernetes DNS suffix to a peer host so short pod names in the server list resolve to
   * the cluster-internal FQDN, consistent with the self-advertised host. Returns the host unchanged
   * when the suffix is empty, when the host is already fully qualified with that suffix (idempotent),
   * or when the host is a raw IP literal or {@code localhost} (which need no DNS resolution).
   */
  static String applyDnsSuffix(final String host, final String k8sDnsSuffix) {
    if (k8sDnsSuffix == null || k8sDnsSuffix.isEmpty() || host == null || host.isEmpty())
      return host;
    if (host.endsWith(k8sDnsSuffix))
      return host;
    if ("localhost".equals(host) || isIpLiteral(host))
      return host;
    return host + k8sDnsSuffix;
  }

  /** Returns true when the host is an IPv4/IPv6 literal, which must never receive a DNS suffix. */
  private static boolean isIpLiteral(final String host) {
    // IPv6 literals contain ':' (or are written in bracketed form); they are never DNS names.
    if (host.indexOf(':') >= 0 || host.charAt(0) == '[')
      return true;
    // IPv4 dotted-quad: exactly four numeric octets.
    final String[] octets = host.split("\\.");
    if (octets.length != 4)
      return false;
    for (final String octet : octets) {
      if (octet.isEmpty())
        return false;
      for (int i = 0; i < octet.length(); i++)
        if (!Character.isDigit(octet.charAt(i)))
          return false;
    }
    return true;
  }

  /** Parses an integer field, throwing a descriptive {@link ServerException} on malformed input. */
  private static int parseIntField(final String value, final String fieldName, final String entry) {
    try {
      return Integer.parseInt(value.trim());
    } catch (final NumberFormatException e) {
      throw new ServerException("Invalid " + fieldName + " value '" + value + "' in peer address '" + entry + "'");
    }
  }

  /**
   * Splits a comma-separated server list into entries, treating commas inside an object-form
   * {@code {...}} block as literal (not entry separators). Throws on unbalanced braces.
   */
  static List<String> splitEntries(final String serverList) {
    final List<String> entries = new ArrayList<>();
    final StringBuilder current = new StringBuilder();
    int depth = 0;
    for (int i = 0; i < serverList.length(); i++) {
      final char c = serverList.charAt(i);
      if (c == '{')
        depth++;
      else if (c == '}') {
        if (depth == 0)
          throw new ServerException("Unbalanced '}' in server list: " + serverList);
        depth--;
      }
      if (c == ',' && depth == 0) {
        entries.add(current.toString());
        current.setLength(0);
      } else
        current.append(c);
    }
    if (depth != 0)
      throw new ServerException("Unbalanced '{' in server list: " + serverList);
    entries.add(current.toString());
    return entries;
  }

  /**
   * Determines the local peer ID using three strategies, in priority order:
   * <ol>
   *   <li><b>Named peer match:</b> if {@code peerNames} contains an entry whose value equals the
   *       {@code serverName} (the {@code name@host} syntax), the corresponding peer is returned.</li>
   *   <li><b>Hostname match:</b> if {@code serverName} equals the host component of a peer's Raft
   *       address (e.g. server name {@code arcadesplit1} for the list entry
   *       {@code arcadesplit1:2434:2480}, or an IP), that peer is returned. This lets a plain
   *       {@code host:raftPort:httpPort} server list work without the {@code prefix_N} convention.</li>
   *   <li><b>Numeric suffix:</b> the server name is parsed for the {@code prefix_N} / {@code prefix-N}
   *       convention and used as the zero-based index into {@code peers} (e.g. {@code arcadedb-0}
   *       or {@code ArcadeDB_0} maps to index 0).</li>
   * </ol>
   */
  static RaftPeerId findLocalPeerId(final List<RaftPeer> peers, final Map<RaftPeerId, String> peerNames,
      final String serverName, final ArcadeDBServer server) {
    if (peerNames != null) {
      for (final Map.Entry<RaftPeerId, String> entry : peerNames.entrySet())
        if (entry.getValue().equals(serverName))
          return entry.getKey();
    }

    // Match the server name against the host component of each peer's Raft address. Covers the
    // plain "host:raftPort:httpPort" (or bare-IP) server list where the node is named after its host.
    for (final RaftPeer peer : peers)
      if (serverName.equals(RaftHAServer.extractHost(peer.getAddress())))
        return peer.getId();

    final int separatorIdx;
    try {
      separatorIdx = findLastSeparatorIndex(serverName);
    } catch (final IllegalArgumentException e) {
      final List<String> configuredNames = peerNames != null ? new ArrayList<>(peerNames.values()) : List.of();
      final List<String> peerHosts = new ArrayList<>(peers.size());
      for (final RaftPeer peer : peers)
        peerHosts.add(RaftHAServer.extractHost(peer.getAddress()));
      throw new IllegalArgumentException(
          "Cannot determine which cluster peer this node is: server name '" + serverName
              + "' does not match any configured peer name"
              + (configuredNames.isEmpty() ? "" : " (configured names: " + configuredNames + ")")
              + ", does not match any host in the server list (hosts: " + peerHosts
              + "), and does not end with a node index ('-N' or '_N'). Fix this in one of these ways: "
              + "(1) set 'arcadedb.server.name' to the host of this node as it appears in the server list "
              + "(e.g. one of " + peerHosts + "); "
              + "or (2) use the 'name@host:raftPort:httpPort' syntax in the server list and set "
              + "'arcadedb.server.name' to one of those names; "
              + "or (3) name each node '" + serverName + "-N' (or '_N'), where N is its zero-based position "
              + "in the server list (first entry = 0); for " + peers.size() + " peers the valid suffixes are -0 .. -"
              + (peers.size() - 1) + ".",
          e);
    }
    final int index = Integer.parseInt(serverName.substring(separatorIdx + 1));
    if (index < 0 || index >= peers.size())
      throw new IllegalArgumentException(
          "Server index " + index + " parsed from node name '" + serverName + "' is out of range: the server list has "
              + peers.size() + " peers, so the index must be zero-based in [0, " + peers.size() + "). "
              + "For " + peers.size() + " peers name the nodes with suffixes -0 .. -" + (peers.size() - 1)
              + " (the first server-list entry is index 0, not 1).");

    return peers.get(index).getId();
  }

  /**
   * Synthesizes the local Raft peer for a Kubernetes StatefulSet scale-up pod whose ordinal is beyond
   * the static {@code HA_SERVER_LIST} (issue #4836). A {@code kubectl scale} that grows the StatefulSet
   * without simultaneously editing {@code HA_SERVER_LIST} starts pods (e.g. {@code arcadedb-3} for a
   * 3-entry list) whose ordinal is {@code >= peers.size()}; {@link #findLocalPeerId} would throw
   * "index out of range" and the pod would crash-loop. In K8s mode the local peer is instead
   * reconstructed from the pod name + DNS suffix so the node can boot and let
   * {@link KubernetesAutoJoin} add it to the running cluster via an atomic {@code Mode.ADD}.
   * <p>
   * Returns {@code null} (no synthesis, caller falls back to the normal/error path) when:
   * <ul>
   *   <li>not in K8s mode ({@code k8sMode == false});</li>
   *   <li>the server name has no parseable {@code prefix-N} / {@code prefix_N} ordinal, so it cannot be
   *       a StatefulSet pod;</li>
   *   <li>the ordinal is within {@code [0, peers.size())} - a normal node that resolves through the
   *       index path and must not be duplicated.</li>
   * </ul>
   * The synthesized address follows {@code <pod-name><dnsSuffix>:<raftPort>}, matching the local
   * listening address computed by {@code ArcadeDBServer#assignHostAddress} ({@code HOSTNAME + dnsSuffix})
   * and the {@code HA_RAFT_PORT} this node binds to, so the address advertised to the cluster is the one
   * peers actually reach.
   */
  static RaftPeer synthesizeK8sScaleUpPeer(final boolean k8sMode, final List<RaftPeer> peers,
      final String serverName, final String dnsSuffix, final int raftPort) {
    if (!k8sMode)
      return null;

    final int separatorIdx;
    try {
      separatorIdx = findLastSeparatorIndex(serverName);
    } catch (final IllegalArgumentException e) {
      return null;
    }

    final int index;
    try {
      index = Integer.parseInt(serverName.substring(separatorIdx + 1));
    } catch (final NumberFormatException e) {
      return null;
    }

    // Within the configured range: the node resolves through the normal index path - do not synthesize.
    if (index < peers.size())
      return null;

    final String host = serverName + (dnsSuffix == null ? "" : dnsSuffix);
    final String raftAddress = host + ":" + raftPort;
    // Use host_raftPort as peer ID (underscore avoids JMX ObjectName issues with colons), matching parsePeerList.
    final String peerIdStr = raftAddress.replace(':', '_');
    return RaftPeer.newBuilder()
        .setId(peerIdStr)
        .setAddress(raftAddress)
        .build();
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
