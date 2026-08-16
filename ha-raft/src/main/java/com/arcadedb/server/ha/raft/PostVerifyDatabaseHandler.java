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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.apache.ratis.protocol.RaftPeer;

import javax.net.ssl.HttpsURLConnection;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * POST /api/v1/cluster/verify/{database} - verifies database consistency across cluster nodes.
 * <p>
 * On the leader it CRCs every file of the local database and fans the same request out to every other peer,
 * comparing their checksums against its own. On any other node - and on any node serving a query a peer fanned
 * out to it - it answers with its local checksums and nothing else.
 * <p>
 * Both halves of that split matter, because this endpoint's whole job is to tell an operator whether the
 * cluster's copies agree, and both ways of getting it wrong point the same way (issue #6221): a node that
 * compares itself against itself matches on every file and is reported as a peer that <em>agrees</em>, and a
 * fan-out that lands back on the leader fans out again, multiplying by (N-1) per level with a full CRC of the
 * database at each. So every peer is dialled through {@link PeerDialAddress}, a peer with no address of its own
 * is reported as unverified rather than consistent, and the query carries the one-hop marker that stops a node
 * from fanning out a request that was itself fanned out.
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
   * 60 s by default) so a rarely-invoked endpoint does not keep N idle threads around; daemon so a
   * process exit is never blocked on it. {@link #close()} shuts it down explicitly on a plugin
   * stop/restart within one JVM, where daemon-ness alone would not prevent a leak (issue #5890).
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

  /**
   * Shuts down the peer-query pool. Called by {@link RaftHAPlugin#stopService()} so that a server
   * stop/restart cycle within one JVM does not leak the pool (issue #5890): a fresh
   * {@code PostVerifyDatabaseHandler} (and pool) is otherwise created on every restart while the
   * previous one, and any in-flight peer queries on it, are left running.
   * <p>
   * Not instantaneous: {@code shutdownNow()} interrupts pool threads, but a thread blocked in
   * {@link java.net.HttpURLConnection}'s blocking socket read is not woken by {@code Thread.interrupt()},
   * so an in-flight peer query can still linger up to {@code PEER_READ_TIMEOUT_MS} after this returns.
   * Harmless (daemon threads, no new work accepted), just not immediate.
   */
  void close() {
    peerQueryExecutor.shutdownNow();
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

    final var db = (DatabaseInternal) server.getDatabase(databaseName);

    // Compute local checksums with file type categorization
    final JSONObject localChecksums = new JSONObject();
    final JSONArray localFiles = new JSONArray();
    db.executeInReadLock(() -> {
      // #6075: CRC THE FILES THROUGH A POINT-IN-TIME SNAPSHOT INSTEAD OF FREEZING THEM WITH A FLUSH SUSPENSION. A
      // VERIFY OF A LARGE DATABASE READS EVERY BYTE OF EVERY FILE, SO THE OLD PATH THROTTLED WRITERS FOR ITS WHOLE
      // DURATION AND POSTPONED INDEX COMPACTION WITH THEM. THE CHECKSUM IS BYTE-FOR-BYTE THE SAME VALUE, SO A PEER
      // STILL ON THE FALLBACK PATH COMPARES EQUAL
      if (db.getConfiguration().getValueAsBoolean(GlobalConfiguration.PAGE_SNAPSHOT_ENABLED)) {
        // COLLECTED ASIDE AND MERGED ONLY ON SUCCESS: A WINDOW INVALIDATED HALFWAY THROUGH MUST NOT LEAVE THE
        // RESPONSE HOLDING A MIX OF SNAPSHOT AND FALLBACK CHECKSUMS
        final JSONObject snapshotChecksums = new JSONObject();
        final JSONArray snapshotFiles = new JSONArray();
        try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
          for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
            try {
              collectFileInfo(snapshotChecksums, snapshotFiles, file.fileName(), snapshot.calculateChecksum(file.fileId()),
                  file.size());
            } catch (final PageSnapshotException e) {
              throw e;
            } catch (final Exception e) {
              // skip files that cannot be checksummed (e.g. in-flight creation)
            }

          for (final String name : snapshotChecksums.keySet())
            localChecksums.put(name, snapshotChecksums.getLong(name));
          for (int i = 0; i < snapshotFiles.length(); i++)
            localFiles.put(snapshotFiles.getJSONObject(i));
          return null;
        } catch (final PageSnapshotException e) {
          LogManager.instance().log(this, Level.WARNING,
              "Point-in-time snapshot unusable for the verify of database '%s' (%s): falling back to suspending the page flush",
              null, db.getName(), e.getMessage());
        }
      }

      db.getPageManager().suspendFlushAndExecute(db, () -> {
        for (final var file : db.getFileManager().getFiles())
          if (file != null) {
            try {
              collectFileInfo(localChecksums, localFiles, file.getFileName(), file.calculateChecksum(), file.getSize());
            } catch (final Exception e) {
              // skip files that cannot be checksummed (e.g. in-flight creation)
            }
          }
      });
      return null;
    });

    final JSONObject response = new JSONObject();

    // Non-leader, or a query a peer already fanned out to us: return local checksums only.
    //
    // The marker is the bound the address checks cannot provide (issue #6221). A resolved peer address can name a
    // real node that is not the one meant - which no local self-check can see - and if that node is a leader it
    // fans out again, multiplying by (N-1) per level and CRC-ing every byte of the database before each fan-out.
    // Honored only on a request that authenticated with the cluster token, so a client cannot use it to suppress
    // a verify of its own; that is the same gate, and the same header, the write-forward paths use, because it
    // states the same thing: a peer already relayed this request, so this node answers it rather than relaying it
    // on.
    if (LeaderForwardContext.isAlreadyForwarded() || !raftHAServer.isLeader()) {
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
      // This node is not a peer of itself: skipped here rather than refused in queryPeer so it is absent from the
      // report instead of listed as a peer that could not be verified. Every OTHER peer is dialled through the
      // guard, because an id that is not ours says nothing about where the address resolved for it points.
      if (peer.getId().equals(raftHAServer.getLocalPeerId()))
        continue;
      futures.add(submitPeerQuery(raftHAServer, peer, databaseName, localChecksums, user, useSsl));
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

    // Three outcomes, not two: a peer this node could not compare against is not a peer that agrees with it
    // (issue #6221). Rolling an unverified peer up as ALL_CONSISTENT hands the operator a clean bill of health
    // from the divergence detector at the moment they are asking precisely because they suspect divergence, and
    // rolling it up as INCONSISTENCY_DETECTED sends them looking for a divergence nobody has observed. The
    // distinction is in the peer entries either way; overallStatus is what alerting keys on.
    boolean anyMismatch = false;
    boolean anyUnverified = false;
    for (int i = 0; i < peerResults.length(); i++) {
      final String peerStatus = peerResults.getJSONObject(i).getString("status", "ERROR");
      if ("INCONSISTENT".equals(peerStatus))
        anyMismatch = true;
      else if (!"CONSISTENT".equals(peerStatus))
        anyUnverified = true;
    }

    result.put("overallStatus", anyMismatch ? "INCONSISTENCY_DETECTED"
        : anyUnverified ? "VERIFICATION_INCOMPLETE" : "ALL_CONSISTENT");
    response.put("result", result);
    return new ExecutionResponse(200, response.toString());
  }

  /**
   * Submits a peer query to {@link #peerQueryExecutor}. {@code stopService()}/a repeated {@code registerAPI()}
   * call can shut the pool down concurrently with an in-flight request (issue #5890 follow-up: closing the
   * pool introduced this narrow race, which could not happen while it leaked); {@code ExecutorService.execute()}
   * then throws {@link RejectedExecutionException} synchronously, before {@code queryPeer} ever runs. Catching
   * it here and degrading to a completed ERROR future keeps that one peer's failure inside the normal
   * per-peer error reporting instead of aborting the whole request with an uncaught exception. Package-private
   * for unit testing.
   */
  CompletableFuture<JSONObject> submitPeerQuery(final RaftHAServer raftHAServer, final RaftPeer peer, final String databaseName,
      final JSONObject localChecksums, final ServerSecurityUser user, final boolean useSsl) {
    try {
      return CompletableFuture.supplyAsync(
          () -> queryPeer(raftHAServer, peer, databaseName, localChecksums, user, useSsl), peerQueryExecutor);
    } catch (final RejectedExecutionException e) {
      final JSONObject err = new JSONObject();
      err.put("peerId", peer.getId().toString());
      err.put("status", "ERROR");
      err.put("error", "peer query rejected: server is stopping");
      return CompletableFuture.completedFuture(err);
    }
  }

  /**
   * Queries a single peer for its checksums and compares them against the leader's. Always returns
   * a JSONObject describing the outcome (CONSISTENT, INCONSISTENT, or ERROR); never throws so the
   * caller can safely join on the CompletableFuture.
   * <p>
   * The address it dials is the guarded one (issue #6221). The best-effort {@code getPeerHttpAddress} is still
   * what the report shows - an operator diagnosing the refusal needs to see the address that was refused - but it
   * is not dialled: on a cluster whose nodes share a host and declare no {@code http} port it is this node's own,
   * and comparing this node against itself matches on every file and is reported as the peer agreeing.
   */
  private JSONObject queryPeer(final RaftHAServer raftHAServer, final RaftPeer peer, final String databaseName,
      final JSONObject localChecksums, final ServerSecurityUser user, final boolean useSsl) {
    final JSONObject peerResult = new JSONObject();
    peerResult.put("peerId", peer.getId().toString());
    final String peerHttpAddr = raftHAServer.getPeerHttpAddress(peer.getId());
    peerResult.put("httpAddress", peerHttpAddr);

    final PeerDialAddress dial = PeerDialAddress.resolve(raftHAServer, peer.getId(), "peer");
    if (dial.refused()) {
      peerResult.put("status", "ERROR");
      peerResult.put("error", "not verified: " + dial.refusal());
      return peerResult;
    }

    try {
      // Peers only advertise their plain HTTP port; the HTTPS listener (when SSL is enabled) binds a
      // separate port. Forcing an https scheme onto the plain HTTP port fails with "Unsupported or
      // unrecognized SSL message" (issue #4470). When SSL is enabled, prefer the peer's resolved HTTPS
      // endpoint (explicit 5th field of HA_SERVER_LIST, or derived from the local HTTPS port);
      // otherwise fall back to plain HTTP on the always-present HTTP listener. Both endpoints come from the
      // guard: the HTTPS one is read from a different field of the server list, with its own derive fallback
      // onto THIS node's HTTPS port, so a cluster that declares distinct http ports and omits the https ones
      // passes the HTTP check with every peer's HTTPS endpoint still collapsed onto our own. A withheld HTTPS
      // address arrives here as null and takes the same route an absent one always has (issue #6221).
      String endpoint = dial.httpAddress();
      boolean https = false;
      if (useSsl && dial.httpsAddress() != null) {
        endpoint = dial.httpsAddress();
        https = true;
      }

      final String url = (https ? "https" : "http") + "://" + endpoint
          + "/api/v1/cluster/verify/" + databaseName;
      final var conn = (HttpURLConnection) new URI(url).toURL().openConnection();
      // Validate the peer certificate against the configured trust store, consistent with the
      // snapshot download path.
      if (conn instanceof HttpsURLConnection httpsConn)
        httpsConn.setSSLSocketFactory(SnapshotInstaller.buildSSLContext(httpServer.getServer()).getSocketFactory());
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
          // One hop, whatever this address turns out to name: the node that serves this query answers with its
          // own checksums instead of fanning out again (issue #6221). Set inside the token branch because the
          // marker is only honored on a token-authenticated request.
          conn.setRequestProperty(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
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

  /** Records one file's checksum in both shapes the response carries: the flat map peers compare, and the detail list. */
  private static void collectFileInfo(final JSONObject checksums, final JSONArray files, final String name, final long crc,
      final long size) {
    checksums.put(name, crc);

    final JSONObject fileInfo = new JSONObject();
    fileInfo.put("name", name);
    fileInfo.put("checksum", crc);
    fileInfo.put("size", size);
    fileInfo.put("type", categorizeFile(name));
    files.put(fileInfo);
  }

  private static String categorizeFile(final String fileName) {
    if (fileName == null) return "unknown";
    final String lower = fileName.toLowerCase();
    if (lower.endsWith(".json") || "configuration".equals(lower) || lower.contains("schema"))
      return "config";
    if (lower.contains("index") || lower.contains(".idx") || lower.contains(".ridx") || lower.contains(".notunique")
        || lower.contains(".unique") || lower.contains(".dictionary"))
      return "index";
    if (lower.contains("bucket") || lower.contains(".pcf"))
      return "bucket";
    return "data";
  }
}
