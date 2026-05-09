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

import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.File;
import java.util.logging.Level;

/**
 * {@code POST /api/v1/cluster/bootstrap-state} - peer-to-peer pre-bootstrap state RPC.
 * <p>
 * Issued by the bootstrap leader during first cluster formation
 * (see {@code arcadedb.ha.bootstrapFromLocalDatabase}, issue #4147) to collect each peer's local
 * database state. The response shape per database is:
 * <pre>
 *   {
 *     "databases": [
 *       {
 *         "name": "heimdall",
 *         "fingerprint": "&lt;sha256-hex&gt;",
 *         "lastTxId": 123456,
 *         "oldestRetainedTxId": 100000     // -1 means "no WAL retained, only full-snapshot can satisfy"
 *       },
 *       ...
 *     ]
 *   }
 * </pre>
 * The leader picks the peer with the highest {@code lastTxId} as the bootstrap source. Each follower
 * later uses {@code oldestRetainedTxId} (combined with the local gap and
 * {@code arcadedb.ha.bootstrapDeltaThreshold}) to decide whether it can catch up via the delta
 * endpoint or has to fall back to the full-snapshot install.
 * <p>
 * Authentication is inherited from {@link AbstractServerHttpHandler}: the standard
 * {@code X-ArcadeDB-Cluster-Token} + {@code X-ArcadeDB-Forwarded-User} pair used by every other
 * peer-to-peer cluster RPC.
 */
public class PostBootstrapStateHandler extends AbstractServerHttpHandler {

  /**
   * Sentinel value for {@code oldestRetainedTxId} meaning "this peer has not retained any WAL
   * history, so a delta resync is not possible against it; only a full snapshot satisfies the
   * follower". Read by both the encode and decode sides; do not change without bumping the
   * Phase-3 wire format.
   */
  public static final long NO_DELTA_AVAILABLE = -1L;

  private final RaftHAPlugin plugin;

  public PostBootstrapStateHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final ArcadeDBServer server = httpServer.getServer();
    final JSONArray dbs = new JSONArray();

    for (final String dbName : server.getDatabaseNames()) {
      // Reserved internal databases (e.g. ".raft") are not part of the operator-visible state and
      // their fingerprint would be meaningless to a peer that's about to seed itself; skip them.
      if (dbName.startsWith("."))
        continue;

      try {
        final ServerDatabase serverDb = server.getDatabase(dbName);
        // Unwrap to LocalDatabase: getEmbedded() returns the underlying engine even when the
        // server has wrapped it for HA (RaftReplicatedDatabase) - same pattern as #4144.
        final DatabaseInternal embedded = serverDb.getWrappedDatabaseInstance().getEmbedded();
        if (!(embedded instanceof LocalDatabase localDb))
          continue;

        final File dbDir = new File(localDb.getDatabasePath());
        final String fingerprint = BootstrapFingerprint.compute(dbDir);
        final long lastTxId = localDb.getLastTransactionId();
        // Phase 3 placeholder: WAL retention lands in Phase 6, so until then no peer can serve a
        // delta. Followers will fall back to the full-snapshot path, identical to today's
        // behaviour. The wire field is forward-compatible: when Phase 6 ships, this returns the
        // real oldest retained txId without any client-side change.
        final long oldestRetainedTxId = NO_DELTA_AVAILABLE;

        final JSONObject dbJson = new JSONObject();
        dbJson.put("name", dbName);
        dbJson.put("fingerprint", fingerprint);
        dbJson.put("lastTxId", lastTxId);
        dbJson.put("oldestRetainedTxId", oldestRetainedTxId);
        dbs.put(dbJson);
      } catch (final Exception e) {
        // A single broken database must not poison the whole RPC: report it with -1/-1 so the
        // bootstrap leader treats this peer as "no usable state for this db" and falls through to
        // the full-snapshot path.
        LogManager.instance().log(this, Level.WARNING,
            "Could not compute bootstrap state for '%s': %s", dbName, e.getMessage());
        final JSONObject dbJson = new JSONObject();
        dbJson.put("name", dbName);
        dbJson.put("fingerprint", "");
        dbJson.put("lastTxId", -1L);
        dbJson.put("oldestRetainedTxId", NO_DELTA_AVAILABLE);
        dbJson.put("error", e.getMessage());
        dbs.put(dbJson);
      }
    }

    final JSONObject response = new JSONObject();
    response.put("databases", dbs);
    response.put("peerId", raftHAServer.getLocalPeerId().toString());
    return new ExecutionResponse(200, response.toString());
  }
}
