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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.apache.ratis.protocol.RaftPeer;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

public class PostVerifyDatabaseHandler extends AbstractServerHttpHandler {

  static final Pattern VALID_DATABASE_NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_\\-.]*");

  private final RaftHAPlugin plugin;

  public PostVerifyDatabaseHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String relativePath = exchange.getRelativePath();
    final String databaseName = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
    if (databaseName.isEmpty())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing database name in path").toString());

    if (!VALID_DATABASE_NAME.matcher(databaseName).matches())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Invalid database name: " + databaseName).toString());

    if (!httpServer.getServer().existsDatabase(databaseName))
      return new ExecutionResponse(404,
          new JSONObject().put("error", "Database '" + databaseName + "' not found").toString());

    try {
      final var db = httpServer.getServer().getDatabase(databaseName);

      // Flush pages and hold a read lock to ensure a consistent point-in-time view of database files
      final Map<String, Long> localChecksums = db.executeInReadLock(() -> {
        final java.util.concurrent.atomic.AtomicReference<Map<String, Long>> ref = new java.util.concurrent.atomic.AtomicReference<>();
        db.getPageManager().suspendFlushAndExecute(db, () -> {
          try {
            ref.set(SnapshotManager.computeFileChecksums(new File(db.getDatabasePath())));
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        });
        return ref.get();
      });

      final JSONObject result = new JSONObject();
      result.put("database", databaseName);
      result.put("localNode", raftHAServer.getLocalPeerId().toString());

      final JSONArray nodesResult = new JSONArray();

      final String clusterToken = raftHAServer.getClusterToken();

      for (final RaftPeer peer : raftHAServer.getLivePeers()) {
        if (peer.getId().equals(raftHAServer.getLocalPeerId()))
          continue;

        final String httpAddr = raftHAServer.getPeerHttpAddress(peer.getId());
        if (httpAddr == null)
          continue;

        final JSONObject nodeResult = new JSONObject();
        nodeResult.put("peerId", peer.getId().toString());

        try {
          final String url = "http://" + httpAddr + "/api/v1/ha/snapshot/" + databaseName + "/checksums";
          final HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
          conn.setRequestMethod("GET");
          conn.setConnectTimeout(10_000);
          conn.setReadTimeout(30_000);
          if (clusterToken != null && !clusterToken.isEmpty())
            conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);

          if (conn.getResponseCode() == 200) {
            final String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            final JSONObject remoteChecksums = new JSONObject(body);

            boolean match = true;
            final JSONArray diffs = new JSONArray();
            for (final var entry : localChecksums.entrySet()) {
              if (!remoteChecksums.has(entry.getKey()) ||
                  remoteChecksums.getLong(entry.getKey()) != entry.getValue()) {
                match = false;
                diffs.put(entry.getKey());
              }
            }
            nodeResult.put("match", match);
            if (!match)
              nodeResult.put("differingFiles", diffs);
          } else {
            nodeResult.put("error", "HTTP " + conn.getResponseCode());
          }
          conn.disconnect();
        } catch (final Exception e) {
          nodeResult.put("error", e.getMessage());
        }

        nodesResult.put(nodeResult);
      }

      result.put("nodes", nodesResult);
      return new ExecutionResponse(200, result.toString());

    } catch (final Exception e) {
      return new ExecutionResponse(500,
          new JSONObject().put("error", "Verification failed: " + e.getMessage()).toString());
    }
  }
}
