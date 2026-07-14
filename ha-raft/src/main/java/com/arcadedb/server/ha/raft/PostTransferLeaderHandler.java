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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Objects;

public class PostTransferLeaderHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostTransferLeaderHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  /** Usage hint returned on every request-validation failure (issue #5276). */
  static final String EXPECTED_BODY =
      "Expected {\"peerId\": \"<peer-id>\", \"timeoutMs\": <millis, optional>} to transfer leadership to a specific peer, "
          + "or {} to let the cluster pick the target";

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    // Leadership transfer has side effects: require an explicit body ({} at minimum) instead of
    // treating a bare POST as a transfer-to-any, and never NPE on a missing one (issue #5276).
    if (payload == null)
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing request body. " + EXPECTED_BODY).toString());

    // Reject unrecognized fields loudly: a mistyped key (e.g. "target" instead of "peerId") would
    // otherwise be silently ignored, turning a targeted transfer into a transfer-to-any that reports
    // success while leadership never reaches the intended peer (issue #5276).
    for (final String key : payload.keySet())
      if (!"peerId".equals(key) && !"timeoutMs".equals(key))
        return new ExecutionResponse(400,
            new JSONObject().put("error", "Unknown field '" + key + "'. " + EXPECTED_BODY).toString());

    final String peerId = payload.getString("peerId", "");
    final long timeoutMs = payload.getLong("timeoutMs", 30_000);

    if (peerId.isEmpty()) {
      // Transfer to any peer (Ratis picks the best candidate)
      final boolean success = raftHAServer.transferLeadership(timeoutMs);
      if (success)
        return new ExecutionResponse(200, new JSONObject().put("result", "Leadership transferred")
            .put("leaderId", Objects.toString(raftHAServer.getLeaderId(), "")).toString());
      return new ExecutionResponse(500,
          new JSONObject().put("error", "Leadership transfer failed").toString());
    }

    // transferLeadership throws on failure (mapped to an error response by the base handler), and on
    // success the manager has confirmed the target is the leader - report it so callers can verify
    // the outcome instead of trusting a bare success string (issue #5276).
    raftHAServer.transferLeadership(peerId, timeoutMs);
    return new ExecutionResponse(200, new JSONObject().put("result", "Leadership transferred to " + peerId)
        .put("leaderId", peerId).toString());
  }
}
