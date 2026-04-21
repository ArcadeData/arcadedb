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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.apache.ratis.protocol.RaftPeer;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * POST /api/v1/cluster/verify/{database} - verifies database consistency across cluster nodes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostVerifyDatabaseHandler extends AbstractServerHttpHandler {
  private static final int     PEER_CONNECT_TIMEOUT_MS = 30_000;
  private static final int     PEER_READ_TIMEOUT_MS    = 60_000;
  private static final int     MAX_PEER_RESPONSE_BYTES = 1024 * 1024; // 1 MB
  /** Valid database name: alphanumeric, underscore, hyphen, dot. No path traversal sequences. */
  static final         Pattern VALID_DATABASE_NAME     = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_\\-.]*");

  private final RaftHAPlugin    plugin;
  /**
   * Dedicated pool for fanning peer verify calls out in parallel. Cached (threads idle out after
   * 60 s by default) so a rarely-invoked endpoint does not keep N idle threads around, daemon so
   * the JVM can shut down without an explicit close on this handler.
   */
  private final ExecutorService peerQueryExecutor;

  public PostVerifyDatabaseHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
    final AtomicInteger threadId = new AtomicInteger();
    this.peerQueryExecutor = Executors.newCachedThreadPool(r -> {
      final Thread t = new Thread(r, "arcadedb-verify-peer-" + threadId.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Raft HA is not enabled\"}");

    // Extract database name from path: /api/v1/cluster/verify/{database}
    final String path = exchange.getRelativePath();
    final String databaseName = (path.startsWith("/") ? path.substring(1) : path).trim();

    if (databaseName.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database name is required in path\"}");

    if (!VALID_DATABASE_NAME.matcher(databaseName).matches())
      return new ExecutionResponse(400, "{ \"error\" : \"Invalid database name\"}");

    final var server = httpServer.getServer();
    if (!server.existsDatabase(databaseName))
      return new ExecutionResponse(404, "{ \"error\" : \"Database '" + databaseName + "' not found\"}");

    final var db = (com.arcadedb.database.DatabaseInternal) server.getDatabase(databaseName);

    // Compute local checksums with file type categorization
    final JSONObject localChecksums = new JSONObject();
    final JSONArray localFiles = new JSONArray();
    db.executeInReadLock(() -> {
      db.getPageManager().suspendFlushAndExecute(db, () -> {
        for (final var file : db.getFileManager().getFiles())
          if (file != null) {
            final String name = file.getFileName();
            try {
              final long crc = file.calculateChecksum();
              localChecksums.put(name, crc);

              final JSONObject fileInfo = new JSONObject();
              fileInfo.put("name", name);
              fileInfo.put("checksum", crc);
              fileInfo.put("size", file.getSize());
              fileInfo.put("type", categorizeFile(name));
              localFiles.put(fileInfo);
            } catch (final Exception e) {
              // skip files that cannot be checksummed (e.g. in-flight creation)
            }
          }
      });
      return null;
    });

    final JSONObject response = new JSONObject();

    // Non-leader: return local checksums only
    if (!raftHAServer.isLeader()) {
      response.put("localChecksums", localChecksums);
      response.put("files", localFiles);
      response.put("localServer", server.getServerName());
      return new ExecutionResponse(200, response.toString());
    }

    final JSONObject result = new JSONObject();
    result.put("database", databaseName);
    result.put("files", localFiles);
    result.put("localServer", server.getServerName());
    result.put("localPeerId", raftHAServer.getLocalPeerId().toString());
    result.put("localChecksums", localChecksums);

    // Fan out peer queries in parallel so wall-clock latency is max(peer) not sum(peers).
    // Each queryPeer call catches its own exceptions and returns an error JSONObject, so the
    // futures themselves never fail; join() below is safe.
    final List<CompletableFuture<JSONObject>> futures = new ArrayList<>();
    final boolean useSsl = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    for (final RaftPeer peer : raftHAServer.getRaftGroup().getPeers()) {
      if (peer.getId().equals(raftHAServer.getLocalPeerId()))
        continue;
      futures.add(CompletableFuture.supplyAsync(
          () -> queryPeer(raftHAServer, peer, databaseName, localChecksums, user, useSsl),
          peerQueryExecutor));
    }

    final JSONArray peerResults = new JSONArray();
    for (final CompletableFuture<JSONObject> f : futures) {
      try {
        peerResults.put(f.join());
      } catch (final CompletionException | CancellationException e) {
        final Throwable cause = e.getCause() != null ? e.getCause() : e;
        final JSONObject err = new JSONObject();
        err.put("status", "ERROR");
        err.put("error", "peer query failed: " + cause.getMessage());
        peerResults.put(err);
      }
    }

    result.put("peers", peerResults);

    boolean allConsistent = true;
    for (int i = 0; i < peerResults.length(); i++)
      if (!"CONSISTENT".equals(peerResults.getJSONObject(i).getString("status", "ERROR")))
        allConsistent = false;

    result.put("overallStatus", allConsistent ? "ALL_CONSISTENT" : "INCONSISTENCY_DETECTED");
    response.put("result", result);
    return new ExecutionResponse(200, response.toString());
  }

  /**
   * Queries a single peer for its checksums and compares them against the leader's. Always returns
   * a JSONObject describing the outcome (CONSISTENT, INCONSISTENT, or ERROR); never throws so the
   * caller can safely join on the CompletableFuture.
   */
  private JSONObject queryPeer(final RaftHAServer raftHAServer, final RaftPeer peer, final String databaseName,
      final JSONObject localChecksums, final ServerSecurityUser user, final boolean useSsl) {
    final JSONObject peerResult = new JSONObject();
    peerResult.put("peerId", peer.getId().toString());
    final String peerHttpAddr = raftHAServer.getPeerHttpAddress(peer.getId());
    peerResult.put("httpAddress", peerHttpAddr);

    try {
      final String url = (useSsl ? "https" : "http") + "://" + peerHttpAddr
          + "/api/v1/cluster/verify/" + databaseName;
      final var conn = (HttpURLConnection) new URI(url).toURL().openConnection();
      try {
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setConnectTimeout(PEER_CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(PEER_READ_TIMEOUT_MS);

        final String clusterToken = raftHAServer.getClusterToken();
        if (clusterToken != null) {
          conn.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);
          // Forward the initiating user's identity so that authorization on the peer evaluates
          // against the actual caller (matching LeaderProxy's pattern).
          conn.setRequestProperty("X-ArcadeDB-Forwarded-User", user.getName());
        }

        conn.setDoOutput(true);
        try (final var os = conn.getOutputStream()) {
          os.write("{}".getBytes(StandardCharsets.UTF_8));
        }

        if (conn.getResponseCode() == 200) {
          final String body;
          try (final var in = conn.getInputStream()) {
            final byte[] bytes = in.readNBytes(MAX_PEER_RESPONSE_BYTES);
            if (bytes.length == MAX_PEER_RESPONSE_BYTES && in.read() != -1) {
              peerResult.put("status", "ERROR");
              peerResult.put("error", "Peer response exceeds " + MAX_PEER_RESPONSE_BYTES + " bytes limit");
              return peerResult;
            }
            body = new String(bytes, StandardCharsets.UTF_8);
          }
          final JSONObject peerResponse = new JSONObject(body);

          if (peerResponse.has("localChecksums")) {
            final JSONObject remoteChecksums = peerResponse.getJSONObject("localChecksums");

            int matchCount = 0;
            int mismatchCount = 0;
            final JSONArray mismatches = new JSONArray();

            for (final String fileName : localChecksums.keySet()) {
              final long localCrc = localChecksums.getLong(fileName);
              if (remoteChecksums.has(fileName)) {
                final long remoteCrc = remoteChecksums.getLong(fileName);
                if (localCrc == remoteCrc)
                  matchCount++;
                else {
                  mismatchCount++;
                  mismatches.put(new JSONObject()
                      .put("file", fileName)
                      .put("type", categorizeFile(fileName))
                      .put("localChecksum", localCrc)
                      .put("remoteChecksum", remoteCrc));
                }
              } else {
                mismatchCount++;
                mismatches.put(new JSONObject()
                    .put("file", fileName)
                    .put("type", categorizeFile(fileName))
                    .put("localChecksum", localCrc)
                    .put("remoteChecksum", "MISSING"));
              }
            }

            peerResult.put("status", mismatchCount == 0 ? "CONSISTENT" : "INCONSISTENT");
            peerResult.put("matchingFiles", matchCount);
            peerResult.put("mismatchedFiles", mismatchCount);
            if (mismatchCount > 0)
              peerResult.put("mismatches", mismatches);
          } else {
            peerResult.put("status", "ERROR");
            peerResult.put("error", "peer response missing 'localChecksums'");
          }
        } else {
          peerResult.put("status", "ERROR");
          peerResult.put("error", "HTTP " + conn.getResponseCode());
        }
      } finally {
        conn.disconnect();
      }
    } catch (final Exception e) {
      peerResult.put("status", "ERROR");
      peerResult.put("error", e.getMessage());
    }
    return peerResult;
  }

  private static String categorizeFile(final String fileName) {
    if (fileName == null) return "unknown";
    final String lower = fileName.toLowerCase();
    if (lower.endsWith(".json") || lower.equals("configuration") || lower.contains("schema"))
      return "config";
    if (lower.contains("index") || lower.contains(".idx") || lower.contains(".ridx") || lower.contains(".notunique")
        || lower.contains(".unique") || lower.contains(".dictionary"))
      return "index";
    if (lower.contains("bucket") || lower.contains(".pcf"))
      return "bucket";
    return "data";
  }
}
