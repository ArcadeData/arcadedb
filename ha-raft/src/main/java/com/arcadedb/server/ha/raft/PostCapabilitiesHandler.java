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

import com.arcadedb.Constants;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;

/**
 * {@code POST /api/v1/cluster/capabilities} - peer-to-peer capability advertisement RPC (issue #7219).
 * <p>
 * Answers what wire-format sections THIS node can decode, so a leader can decide whether it may write an optional
 * section rather than leaving that to an operator's rolling-upgrade discipline. The response shape is:
 * <pre>
 *   {
 *     "peerId": "arcadedb0",
 *     "version": "26.9.1",
 *     "capabilities": ["schema-delta"]
 *   }
 * </pre>
 * <p>
 * <b>The absence of this route is itself the answer.</b> A node running a build that predates it replies 404, which
 * the caller reads as "cannot decode anything optional" - so no negotiation, no handshake and no version parsing is
 * needed to be safe against a peer that has never heard of the mechanism.
 * <p>
 * {@code capabilities} is sorted so two nodes running the same build return byte-identical documents, which makes a
 * diff between two peers' answers readable by eye. {@code version} is reported for the operator and for support
 * logs; nothing decides on it, because a version string says what a build calls itself rather than what its decoder
 * handles.
 * <p>
 * Authentication is inherited from {@link AbstractServerHttpHandler}: the {@code X-ArcadeDB-Cluster-Token} +
 * {@code X-ArcadeDB-Forwarded-User} pair every other peer-to-peer cluster RPC uses. Root-only for the same reason
 * {@link PostBootstrapStateHandler} is - it answers a whole-cluster question no single tenant can act on - and it is
 * cheap by construction: it reads two in-memory constants and opens nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostCapabilitiesHandler extends AbstractServerHttpHandler {

  /**
   * The document's flag saying this node holds a database it cannot serve (issue #8665, see
   * {@link ArcadeStateMachine#hasLeaderServiceGap()}). Absent means no gap.
   */
  static final String SERVICE_GAP = "serviceGap";

  /**
   * The document's list of the databases this node holds quarantined (issue #9553), sorted. Absent means none. Read by
   * every peer's capability poll, so a node can tell that no voter of the cluster holds a copy of a database it could
   * resync from - the state in which every quarantine waits on a resync no peer can serve.
   */
  static final String QUARANTINED = "quarantined";

  /**
   * The document's map of the HTTP addresses this node holds for the OTHER members of its configuration - declared by an
   * operator, here or on another node, or confirmed by a probe the member answered - keyed by peer id (issue #9255).
   * Absent when it holds none. The caller does not believe it: each address is a candidate until the member it names
   * answers a probe on it.
   */
  static final String PEER_HTTP_ADDRESSES = "peerHttpAddresses";

  /** The request's member: the calling node's peer id (issue #9255). */
  static final String CALLER_PEER_ID      = "peerId";
  /** The request's member: the HTTP address the calling node resolves for itself (issue #9255). */
  static final String CALLER_HTTP_ADDRESS = "httpAddress";
  /** The request's member: the port the calling node's HTTP listener is bound to (issue #9255). */
  static final String CALLER_HTTP_PORT    = "httpPort";

  private static final String CLUSTER_TOKEN_HEADER = "X-ArcadeDB-Cluster-Token";

  private final RaftHAPlugin plugin;

  public PostCapabilitiesHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    // The caller says where it listens (issue #9255): a candidate this node confirms with a probe of its own before it
    // dials the caller there for anything else. Only from a peer: that probe carries the cluster token, so an address a
    // root user could plant through Basic auth would be a way to send the token somewhere. A peer holds it already, and
    // the header was validated before this method runs (an invalid one is refused with 401)
    if (payload != null && exchange.getRequestHeaders().contains(CLUSTER_TOKEN_HEADER))
      try {
        raftHAServer.offerCallerHttpAddress(payload.getString(CALLER_PEER_ID, ""),
            payload.getString(CALLER_HTTP_ADDRESS, ""), payload.getInt(CALLER_HTTP_PORT, -1));
      } catch (final RuntimeException e) {
        // A malformed self-description costs the caller its candidate, never the answer it asked for
        LogManager.instance().log(this, Level.FINE, "Ignoring the self-description of a capability request: %s", e.getMessage());
      }

    final ArcadeStateMachine stateMachine = raftHAServer.getStateMachine();
    return new ExecutionResponse(200,
        advertisement(raftHAServer.getLocalPeerId().toString(), raftHAServer.getAdvertisedCapabilities(),
            stateMachine != null && stateMachine.hasLeaderServiceGap(),
            stateMachine != null ? stateMachine.getQuarantinedDatabaseNames() : Set.of(),
            raftHAServer.getRelayablePeerHttpAddresses()).toString());
  }

  /**
   * The advertisement document for {@code peerId}. Package-private and pure so the shape this contract depends on -
   * the peer id a caller matches against, and the capability array - can be pinned without an HTTP server.
   */
  // @VisibleForTesting
  static JSONObject advertisement(final String peerId, final Set<String> capabilities) {
    return advertisement(peerId, capabilities, false);
  }

  /**
   * As {@link #advertisement(String, Set)}, also carrying {@link #SERVICE_GAP} when this node has one (issue #8665).
   * Written only when true, so the document of a healthy node stays byte-identical to what it was before the field.
   */
  // @VisibleForTesting
  static JSONObject advertisement(final String peerId, final Set<String> capabilities, final boolean serviceGap) {
    return advertisement(peerId, capabilities, serviceGap, Set.of());
  }

  /**
   * As {@link #advertisement(String, Set, boolean)}, also carrying {@link #QUARANTINED} when this node holds a
   * quarantine (issue #9553). Written only when non-empty, for the same reason as {@link #SERVICE_GAP}.
   */
  // @VisibleForTesting
  static JSONObject advertisement(final String peerId, final Set<String> capabilities, final boolean serviceGap,
      final Set<String> quarantined) {
    return advertisement(peerId, capabilities, serviceGap, quarantined, Map.of());
  }

  /**
   * As {@link #advertisement(String, Set, boolean, Set)}, also carrying {@link #PEER_HTTP_ADDRESSES} when this node holds
   * an address for another member (issue #9255), sorted by peer id for the same reason the capabilities are.
   */
  // @VisibleForTesting
  static JSONObject advertisement(final String peerId, final Set<String> capabilities, final boolean serviceGap,
      final Set<String> quarantined, final Map<String, String> peerHttpAddresses) {
    final JSONArray array = new JSONArray();
    for (final String capability : new TreeSet<>(capabilities))
      array.put(capability);

    final JSONObject json = new JSONObject()
        .put("peerId", peerId)
        .put("version", Constants.getVersion())
        .put("capabilities", array);
    if (serviceGap)
      json.put(SERVICE_GAP, true);
    if (quarantined != null && !quarantined.isEmpty()) {
      final JSONArray names = new JSONArray();
      for (final String name : new TreeSet<>(quarantined))
        names.put(name);
      json.put(QUARANTINED, names);
    }
    if (peerHttpAddresses != null && !peerHttpAddresses.isEmpty()) {
      final JSONObject addresses = new JSONObject();
      for (final Map.Entry<String, String> entry : new TreeMap<>(peerHttpAddresses).entrySet())
        addresses.put(entry.getKey(), entry.getValue());
      json.put(PEER_HTTP_ADDRESSES, addresses);
    }
    return json;
  }
}
