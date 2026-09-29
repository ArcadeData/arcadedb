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
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.handler.LeaderDial;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * The leader's side of issue #8605. A copy carrying the {@link ArcadeDBServer#UNVERIFIED_CLOSED_COPY_FILE} marker was
 * closed on this node while it was a follower, and a resync could not verify it because the leader of the time did not
 * hold it (issue #8589). A follower refuses to reopen it; a leader that reopens it makes it the cluster's copy, the one
 * every follower then installs from. So the leader asks every peer of the committed configuration about its own copy
 * first, and reopens only when none of them holds one this copy is behind.
 * <p>
 * The recency signal is the per-database applied Raft index each node persists ({@link
 * ArcadeStateMachine#readPersistedAppliedIndex(String)}): a Raft log position is the same number on every node, unlike
 * a database's last transaction id, which a follower's replicated apply never advances. A copy with no recorded
 * position, or one this node holds quarantined (its position may be overstated), cannot be ordered, so it refuses.
 * <p>
 * Every refusal is recorded for the {@code unverified-closed-copy-refused} cluster alert, and dropped once the copy is
 * reopened or its marker is gone.
 */
final class UnverifiedClosedCopyCheck {

  /** Request flag on {@code POST /api/v1/cluster/bootstrap-state}: answer with this node's copy of the named database. */
  static final String COPY_OF = "copyOf";
  /** Response member carrying the {@link CopyState} of the database {@link #COPY_OF} named. */
  static final String COPY    = "copy";

  /** Per-peer budget of one question. Every peer is asked in turn, on the request thread that names the database. */
  static final long PEER_TIMEOUT_MS = 5_000L;

  private static final long LOG_INTERVAL_MS = 60_000L;

  /**
   * One node's copy of a database: whether it holds one at all (registered, or a directory on disk) and the last Raft
   * index applied to it there, or {@code -1} when it cannot be ordered.
   */
  record CopyState(boolean present, long appliedIndex) {
    JSONObject toJSON(final String databaseName) {
      return new JSONObject().put("name", databaseName).put("present", present).put("appliedIndex", appliedIndex);
    }

    static CopyState fromJSON(final JSONObject json) {
      return new CopyState(json.getBoolean("present", true), json.getLong("appliedIndex", -1L));
    }
  }

  /** How a peer is asked; an HTTP round trip in production. */
  @FunctionalInterface
  interface PeerQuestion {
    CopyState ask(String url, String databaseName) throws Exception;
  }

  private final RaftHAServer                  raftHAServer;
  private final ArcadeDBServer                server;
  private final Map<String, String>           refusals   = new ConcurrentHashMap<>();
  private final Map<String, Long>             lastLogged = new ConcurrentHashMap<>();

  UnverifiedClosedCopyCheck(final RaftHAServer raftHAServer, final ArcadeDBServer server) {
    this.raftHAServer = raftHAServer;
    this.server = server;
  }

  /**
   * Asks every peer of the committed configuration about its copy of {@code databaseName}.
   *
   * @return {@code null} when the leader may reopen its copy, otherwise why not
   */
  String check(final String databaseName) {
    final ArcadeStateMachine stateMachine = raftHAServer.getStateMachine();
    final BootstrapElection election = raftHAServer.getBootstrapElection();
    if (stateMachine == null || election == null)
      return record(databaseName, "the HA layer of this server has not started yet");

    final boolean useSSL = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final Map<RaftPeerId, String> urls = election.peerProbeUrls(useSSL);
    if (useSSL && urls.values().stream().anyMatch(url -> url != null && url.startsWith("http://")))
      PlainHttpFallbackNotice.sayOnce(UnverifiedClosedCopyCheck.class, "asking about an unverified closed copy");

    HttpClient httpsClient = null;
    try {
      if (urls.values().stream().anyMatch(url -> url != null && url.startsWith("https://")))
        httpsClient = BootstrapElection.newTrustingClient(server);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot build the HTTPS client from the cluster truststore to ask the peers about database '%s': %s", null,
          databaseName, e.getMessage());
    }
    final HttpClient https = httpsClient;
    try {
      return check(databaseName, localCopyState(server, stateMachine, databaseName), urls,
          (url, name) -> askOverHttp(url.startsWith("https://") ? https : BootstrapElection.HTTP, url, name,
              raftHAServer.getClusterToken()));
    } finally {
      if (https != null)
        https.close();
    }
  }

  /** {@link #check(String)} with the peers, their URLs and the way to ask them given. Package-private for tests. */
  String check(final String databaseName, final CopyState local, final Map<RaftPeerId, String> peerUrls,
      final PeerQuestion question) {
    final Map<String, CopyState> answered = new TreeMap<>();
    final List<String> unanswered = new ArrayList<>();
    for (final Map.Entry<RaftPeerId, String> entry : peerUrls.entrySet()) {
      final String peer = entry.getKey().toString();
      if (entry.getValue() == null) {
        unanswered.add(peer + " (no HTTP address this node may dial)");
        continue;
      }
      try {
        answered.put(peer, question.ask(entry.getValue(), databaseName));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        unanswered.add(peer + " (interrupted)");
      } catch (final Exception e) {
        unanswered.add(peer + " (" + e.getMessage() + ")");
      }
    }
    final String refusal = verdict(local, answered, unanswered);
    if (refusal == null) {
      refusals.remove(databaseName);
      lastLogged.remove(databaseName);
      LogManager.instance().log(this, Level.INFO,
          "Database '%s': this leader's copy, which the last resync could not verify, is at applied index %d and no "
              + "other server holds a newer one (%s), so it is reopened as the cluster's copy (issue #8605)", null,
          databaseName, local.appliedIndex(), answered);
      return null;
    }
    return record(databaseName, refusal);
  }

  /**
   * The rule, free of I/O: every peer must have answered, and every copy a peer holds must be ordered at or below this
   * node's. Package-private for tests.
   *
   * @return {@code null} when the leader may reopen its copy, otherwise why not
   */
  static String verdict(final CopyState local, final Map<String, CopyState> answered, final List<String> unanswered) {
    if (!unanswered.isEmpty())
      return "the other servers could not all be asked about their copy, and one of them may hold a newer one: "
          + String.join(", ", unanswered);
    final List<String> newer = new ArrayList<>();
    final List<String> unordered = new ArrayList<>();
    for (final Map.Entry<String, CopyState> entry : answered.entrySet()) {
      final CopyState peer = entry.getValue();
      if (!peer.present())
        continue;
      if (peer.appliedIndex() < 0 || local.appliedIndex() < 0)
        unordered.add(entry.getKey() + " (applied index " + peer.appliedIndex() + ")");
      else if (peer.appliedIndex() > local.appliedIndex())
        newer.add(entry.getKey() + " (applied index " + peer.appliedIndex() + ")");
    }
    if (!newer.isEmpty())
      return "a newer copy is held by " + String.join(", ", newer) + ", while this node's is at applied index "
          + local.appliedIndex();
    if (!unordered.isEmpty())
      return "it cannot be ordered against the copy held by " + String.join(", ", unordered)
          + ", because one of the two has no recorded applied index (this node's is " + local.appliedIndex() + ")";
    return null;
  }

  /**
   * This node's copy of {@code databaseName}: present when it is registered or has a directory on disk, ordered by the
   * applied index this node persisted for it. A copy this node holds quarantined is reported unordered: the entries
   * skipped while it waits for its resync still advanced the position. Never opens anything.
   */
  static CopyState localCopyState(final ArcadeDBServer server, final ArcadeStateMachine stateMachine,
      final String databaseName) {
    final boolean present = server.existsDatabase(databaseName) || Files.isDirectory(
        Path.of(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), databaseName));
    if (!present)
      return new CopyState(false, -1L);
    if (stateMachine == null || stateMachine.isDatabaseDiverged(databaseName))
      return new CopyState(true, -1L);
    return new CopyState(true, stateMachine.readPersistedAppliedIndex(databaseName));
  }

  /**
   * The databases a refusal is standing for, with its reason, for the cluster alert. A refusal whose marker has gone
   * since - an install replaced the copy, a drop removed it, an operator accepted it - no longer stands.
   */
  Map<String, String> getRefusals() {
    if (refusals.isEmpty())
      return Collections.emptyMap();
    final String root = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    refusals.keySet().removeIf(name -> !Files.exists(Path.of(root, name, ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE)));
    return new TreeMap<>(refusals);
  }

  private String record(final String databaseName, final String refusal) {
    refusals.put(databaseName, refusal);
    // Every request that names the database asks again, so the SEVERE is throttled per database, not per request.
    final long now = System.currentTimeMillis();
    final Long previous = lastLogged.get(databaseName);
    if (previous == null || now - previous >= LOG_INTERVAL_MS) {
      lastLogged.put(databaseName, now);
      LogManager.instance().log(this, Level.SEVERE,
          "Database '%s' is NOT reopened on this leader: its copy was closed here and the last resync could not verify "
              + "it, and %s. Transfer the leadership to the server holding the newer copy, or remove this node's '%s' "
              + "marker to accept this copy as it is (issue #8605)", null, databaseName, refusal,
          ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE);
    }
    return refusal;
  }

  private static CopyState askOverHttp(final HttpClient client, final String url, final String databaseName,
      final String clusterToken) throws IOException, InterruptedException {
    if (client == null)
      throw new IOException("no HTTPS client could be built from the cluster truststore");
    final HttpRequest request = BootstrapElection.bootstrapStateRequestTo(url, clusterToken, PEER_TIMEOUT_MS,
        new JSONObject().put(COPY_OF, databaseName).toString());
    final HttpResponse<String> response = LeaderDial.sendBounded(client, request, HttpResponse.BodyHandlers.ofString(),
        PEER_TIMEOUT_MS);
    return parseAnswer(response.statusCode(), response.body());
  }

  /**
   * A peer's answer. One that predates issue #8605 ignores {@link #COPY_OF} and lists its open databases instead, which
   * says nothing about a closed copy, so an answer without {@link #COPY} is not an answer. Package-private for tests.
   */
  static CopyState parseAnswer(final int statusCode, final String body) throws IOException {
    if (statusCode != 200)
      throw new IOException("HTTP " + statusCode);
    final JSONObject copy = new JSONObject(body).getJSONObject(COPY, null);
    if (copy == null)
      throw new IOException("the server does not report a single database's copy (older version)");
    return CopyState.fromJSON(copy);
  }
}
