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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Set;
import java.util.TreeSet;

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

    return new ExecutionResponse(200,
        advertisement(raftHAServer.getLocalPeerId().toString(), raftHAServer.getAdvertisedCapabilities()).toString());
  }

  /**
   * The advertisement document for {@code peerId}. Package-private and pure so the shape this contract depends on -
   * the peer id a caller matches against, and the capability array - can be pinned without an HTTP server.
   */
  // @VisibleForTesting
  static JSONObject advertisement(final String peerId, final Set<String> capabilities) {
    final JSONArray array = new JSONArray();
    for (final String capability : new TreeSet<>(capabilities))
      array.put(capability);

    return new JSONObject()
        .put("peerId", peerId)
        .put("version", Constants.getVersion())
        .put("capabilities", array);
  }
}
