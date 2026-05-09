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
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Drives the offline cluster bootstrap protocol described in issue #4147.
 * <p>
 * Engages once per cluster lifetime when {@code arcadedb.ha.bootstrapFromLocalDatabase=true} and
 * the freshly-elected Raft leader observes an empty Raft log. The protocol:
 * <ol>
 *   <li>Snapshot every local database's {@code (fingerprint, lastTxId)}.</li>
 *   <li>Fan out {@code POST /api/v1/cluster/bootstrap-state} to every peer in
 *       {@code HA_SERVER_LIST} with a bounded timeout
 *       ({@code arcadedb.ha.bootstrapTimeoutMs}).</li>
 *   <li>For every database that appears on at least one peer, pick the peer with the highest
 *       {@code lastTxId} as the bootstrap source.</li>
 *   <li>If the source is the local peer, commit
 *       {@link RaftLogEntryType#BOOTSTRAP_FINGERPRINT_ENTRY} via the existing transaction broker.
 *       If the source is a different peer, transfer Raft leadership to that peer; the new leader
 *       will re-enter this protocol on its own {@code notifyLeaderChanged} callback and commit the
 *       entry then.</li>
 * </ol>
 * <p>
 * The protocol is idempotent across leader changes: each newly-elected leader checks whether the
 * cluster's commit index is still 0 and, if so, re-runs. A leader transfer triggered by a previous
 * bootstrap pass is therefore safe even if the new leader doesn't know it was selected as the
 * source - it picks itself again from its own state.
 * <p>
 * Late joiners (peers that arrive after the bootstrap entry has been committed) skip this path
 * entirely; their {@code BOOTSTRAP_FINGERPRINT_ENTRY} application is the responsibility of
 * {@code ArcadeStateMachine} (Phase 5).
 */
class BootstrapElection {

  /**
   * Single attempt outcome. Used by tests; production code only inspects {@link #COMMITTED} vs
   * the rest to know whether the protocol can stop running on subsequent leader changes.
   */
  enum Outcome {
    SKIPPED_DISABLED,           // bootstrapFromLocalDatabase=false
    SKIPPED_NOT_FIRST_FORMATION,// commit index > 0, the cluster has been running
    SKIPPED_NO_DATABASES,       // nothing to bootstrap
    NOT_LEADER,                 // we're not the leader anymore by the time we ran
    TRANSFERRED,                // leadership transferred to the source peer; new leader will retry
    COMMITTED,                  // we committed BOOTSTRAP_FINGERPRINT_ENTRY for at least one DB
    FAILED                      // unexpected error; bootstrap will be re-attempted on next leader change
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private final RaftHAServer            haServer;
  private final ArcadeDBServer          server;
  // Tracks per-database whether the bootstrap protocol has been attempted in this leader's
  // lifetime. Used to short-circuit on repeated notifyLeaderChanged callbacks for the same term.
  private final Set<String>             attemptedThisTerm = ConcurrentHashMap.newKeySet();

  BootstrapElection(final RaftHAServer haServer, final ArcadeDBServer server) {
    this.haServer = haServer;
    this.server = server;
  }

  /**
   * Reset per-term attempt tracking. Called by {@link ArcadeStateMachine} on every leader change so
   * a new term gets a fresh attempt.
   */
  void onLeaderChanged() {
    attemptedThisTerm.clear();
  }

  /**
   * Run the bootstrap protocol if conditions allow. Safe to call from the leader-change callback
   * even when bootstrap shouldn't engage - the method short-circuits and returns the appropriate
   * {@link Outcome}.
   */
  Outcome runIfEligible() {
    final var configuration = server.getConfiguration();
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_BOOTSTRAP_FROM_LOCAL_DATABASE))
      return Outcome.SKIPPED_DISABLED;

    if (!haServer.isLeader())
      return Outcome.NOT_LEADER;

    // The bootstrap path is gated on first cluster formation: every peer's Raft log empty. The
    // leader's commit index is the only piece of cluster-wide state we have to check; once any
    // entry has been committed, we never re-engage. See section 8 ("Gating") in #4147.
    final long commitIndex = haServer.getCommitIndex();
    if (commitIndex > 0)
      return Outcome.SKIPPED_NOT_FIRST_FORMATION;

    final List<String> dbNames = collectLocalDatabaseNames();
    if (dbNames.isEmpty())
      return Outcome.SKIPPED_NO_DATABASES;

    // Already attempted this term and we were the source for everything? Don't re-run.
    final List<String> pending = new ArrayList<>();
    for (final String db : dbNames)
      if (attemptedThisTerm.add(db))
        pending.add(db);
    if (pending.isEmpty())
      return Outcome.SKIPPED_NOT_FIRST_FORMATION;

    try {
      final Map<String, List<PeerState>> states = collectStates(pending);
      return decideAndAct(states);
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.WARNING,
          "Bootstrap election failed (will retry on next leader change): %s", null, t.getMessage());
      // Allow a retry next term.
      attemptedThisTerm.removeAll(pending);
      return Outcome.FAILED;
    }
  }

  /**
   * For each pending database, query every peer for its (fingerprint, lastTxId, oldestRetainedTxId)
   * via the Phase-3 RPC. Self is queried locally without HTTP. Peers that don't respond within
   * {@code bootstrapTimeoutMs} are simply absent from the result; the leader proceeds with what it
   * has and a SEVERE log explains who was missed.
   */
  private Map<String, List<PeerState>> collectStates(final List<String> dbNames) {
    final long timeoutMs = server.getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_BOOTSTRAP_TIMEOUT_MS);
    final RaftPeerId localId = haServer.getLocalPeerId();
    final Collection<RaftPeer> peers = haServer.getLivePeers();
    final Set<String> dbFilter = new HashSet<>(dbNames);
    final Map<RaftPeerId, String> httpAddresses = haServer.getHttpAddresses();

    // Self state computed locally to avoid a self-loop HTTP call.
    final Map<String, PeerState> selfStates = computeLocalStates(localId, dbFilter);

    // Fan out to other peers in parallel; bounded by timeoutMs.
    final List<CompletableFuture<Map<RaftPeerId, Map<String, PeerState>>>> futures = new ArrayList<>();
    for (final RaftPeer peer : peers) {
      if (peer.getId().equals(localId))
        continue;
      final String httpAddr = httpAddresses.get(peer.getId());
      if (httpAddr == null) {
        LogManager.instance().log(this, Level.WARNING,
            "Bootstrap: peer %s has no known HTTP address; skipping", peer.getId());
        continue;
      }
      futures.add(queryPeer(peer.getId(), httpAddr, dbFilter, timeoutMs));
    }

    final Map<RaftPeerId, Map<String, PeerState>> remoteStates = new HashMap<>();
    final long deadline = System.currentTimeMillis() + timeoutMs;
    for (final CompletableFuture<Map<RaftPeerId, Map<String, PeerState>>> f : futures) {
      final long remaining = Math.max(0, deadline - System.currentTimeMillis());
      try {
        final Map<RaftPeerId, Map<String, PeerState>> result = f.get(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (result != null)
          remoteStates.putAll(result);
      } catch (final java.util.concurrent.TimeoutException te) {
        // Pending peer didn't respond by deadline; logged below.
        f.cancel(true);
      } catch (final Exception e) {
        // Individual peer error; logged below.
      }
    }

    // Identify any configured peer that didn't report and warn loudly.
    final Set<RaftPeerId> heard = new HashSet<>(remoteStates.keySet());
    heard.add(localId);
    final List<String> missing = new ArrayList<>();
    for (final RaftPeer peer : peers)
      if (!heard.contains(peer.getId()))
        missing.add(peer.getId().toString());
    if (!missing.isEmpty())
      LogManager.instance().log(this, Level.SEVERE,
          "Bootstrap timeout: peers %s did not report state within %dms; proceeding with majority. " +
              "Unreachable peers will catch up via leader-shipped snapshot when they reconnect.",
          missing, timeoutMs);

    // Pivot: from peer→db→state to db→list of (peer, state).
    final Map<String, List<PeerState>> byDb = new HashMap<>();
    for (final var entry : selfStates.entrySet())
      byDb.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
    for (final var byPeer : remoteStates.values())
      for (final var entry : byPeer.entrySet())
        byDb.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
    return byDb;
  }

  private CompletableFuture<Map<RaftPeerId, Map<String, PeerState>>> queryPeer(final RaftPeerId peerId,
      final String httpAddr, final Set<String> dbFilter, final long timeoutMs) {
    final String url = "http://" + httpAddr + "/api/v1/cluster/bootstrap-state";
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}"));
    final String token = haServer.getClusterToken();
    if (token != null && !token.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", token);
    builder.header("X-ArcadeDB-Forwarded-User", "root");

    return HTTP.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString())
        .thenApply(resp -> {
          if (resp.statusCode() != 200) {
            LogManager.instance().log(this, Level.WARNING,
                "Bootstrap: peer %s responded with HTTP %d to /bootstrap-state", peerId, resp.statusCode());
            return null;
          }
          try {
            final JSONObject json = new JSONObject(resp.body());
            final JSONArray dbs = json.getJSONArray("databases");
            final Map<String, PeerState> result = new HashMap<>();
            for (int i = 0; i < dbs.length(); i++) {
              final JSONObject db = dbs.getJSONObject(i);
              final String name = db.getString("name");
              if (!dbFilter.contains(name))
                continue;
              result.put(name, new PeerState(peerId, name, db.getString("fingerprint"),
                  db.getLong("lastTxId"), db.getLong("oldestRetainedTxId")));
            }
            return Map.of(peerId, result);
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING,
                "Bootstrap: peer %s returned malformed JSON: %s", peerId, e.getMessage());
            return null;
          }
        })
        .exceptionally(t -> {
          LogManager.instance().log(this, Level.WARNING,
              "Bootstrap: failed to query peer %s: %s", peerId, t.getMessage());
          return null;
        });
  }

  private Map<String, PeerState> computeLocalStates(final RaftPeerId localId, final Set<String> dbFilter) {
    final Map<String, PeerState> result = new HashMap<>();
    for (final String dbName : server.getDatabaseNames()) {
      if (!dbFilter.contains(dbName))
        continue;
      try {
        final ServerDatabase serverDb = server.getDatabase(dbName);
        final DatabaseInternal embedded = serverDb.getWrappedDatabaseInstance().getEmbedded();
        if (!(embedded instanceof LocalDatabase localDb))
          continue;
        final String fp = BootstrapFingerprint.compute(new File(localDb.getDatabasePath()));
        result.put(dbName, new PeerState(localId, dbName, fp, localDb.getLastTransactionId(),
            PostBootstrapStateHandler.NO_DELTA_AVAILABLE));
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Bootstrap: cannot compute local state for '%s': %s", dbName, e.getMessage());
      }
    }
    return result;
  }

  private List<String> collectLocalDatabaseNames() {
    final List<String> out = new ArrayList<>();
    for (final String dbName : server.getDatabaseNames())
      if (!dbName.startsWith("."))
        out.add(dbName);
    return out;
  }

  /**
   * Pick a source per database and either commit the bootstrap entry locally or transfer
   * leadership to the source. Returns {@link Outcome#TRANSFERRED} as soon as ANY database
   * triggers a leadership transfer; the current leader role ends here and the new leader will
   * handle the rest on its own {@code notifyLeaderChanged} callback.
   */
  private Outcome decideAndAct(final Map<String, List<PeerState>> states) {
    final RaftPeerId localId = haServer.getLocalPeerId();
    boolean anyCommitted = false;
    final long timeoutMs = server.getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_BOOTSTRAP_TIMEOUT_MS);

    for (final var entry : states.entrySet()) {
      final String dbName = entry.getKey();
      final List<PeerState> peerStates = entry.getValue();
      if (peerStates.isEmpty())
        continue;

      // Source picking has two rules:
      //
      //  1. Highest lastTxId wins. The whole point of recency-aware bootstrap.
      //  2. On ties, PREFER THE LOCAL PEER over remote peers. Two consequences worth calling out:
      //     - When every peer is at the same state (the customer's happy path: identical
      //       pre-staged backups), the protocol commits the bootstrap entry on the current leader
      //       without an unnecessary leadership transfer.
      //     - It avoids interfering with operator-driven leadership changes (e.g. tests that
      //       explicitly call transferLeadership() on a brand-new empty cluster).
      //
      // Determinism across the cluster still holds: only the leader runs this code path; the
      // tie-break only affects WHO the leader picks, not whether peers agree on the result.
      PeerState source = peerStates.get(0);
      for (int i = 1; i < peerStates.size(); i++) {
        final PeerState candidate = peerStates.get(i);
        if (candidate.lastTxId > source.lastTxId)
          source = candidate;
        else if (candidate.lastTxId == source.lastTxId) {
          // Prefer self when tied; otherwise lex-lower peer id for repeatability.
          if (candidate.peerId.equals(localId))
            source = candidate;
          else if (!source.peerId.equals(localId)
              && candidate.peerId.toString().compareTo(source.peerId.toString()) < 0)
            source = candidate;
        }
      }

      // If no peer has any data (every lastTxId is -1) the offline-bootstrap path adds no value:
      // there is nothing to ship and nothing to compare against. Skip silently.
      if (source.lastTxId < 0) {
        LogManager.instance().log(this, Level.FINE,
            "Bootstrap: no peer has data for '%s' (all lastTxId=-1); skipping protocol", dbName);
        continue;
      }

      LogManager.instance().log(this, Level.INFO,
          "Bootstrap source for '%s': peer=%s, lastTxId=%d, fingerprint=%s",
          dbName, source.peerId, source.lastTxId, abbreviate(source.fingerprint));

      if (source.peerId.equals(localId)) {
        // We are the source. Commit BOOTSTRAP_FINGERPRINT_ENTRY via the broker.
        try {
          haServer.getTransactionBroker().replicateBootstrapFingerprint(dbName, source.fingerprint, source.lastTxId);
          anyCommitted = true;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Bootstrap: failed to commit BOOTSTRAP_FINGERPRINT_ENTRY for '%s': %s",
              dbName, e.getMessage());
          // Re-throw to let runIfEligible() back off; we'll retry next term.
          throw new RuntimeException(e);
        }
      } else {
        // Another peer is the source. Hand off Raft leadership to it; that peer's own
        // notifyLeaderChanged callback will commit the entry. Returning early ensures any
        // remaining databases in this map are NOT processed by us - we're no longer leader.
        LogManager.instance().log(this, Level.INFO,
            "Bootstrap: transferring leadership to %s (source for '%s')", source.peerId, dbName);
        try {
          haServer.transferLeadership(source.peerId.toString(), timeoutMs);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Bootstrap: leadership transfer to %s failed: %s; will retry next term",
              source.peerId, e.getMessage());
          throw new RuntimeException(e);
        }
        return Outcome.TRANSFERRED;
      }
    }

    return anyCommitted ? Outcome.COMMITTED : Outcome.SKIPPED_NO_DATABASES;
  }

  private static String abbreviate(final String fingerprint) {
    if (fingerprint == null || fingerprint.length() <= 16)
      return String.valueOf(fingerprint);
    return fingerprint.substring(0, 8) + "..." + fingerprint.substring(fingerprint.length() - 8);
  }

  /** Per-peer per-database state collected during bootstrap. */
  record PeerState(RaftPeerId peerId, String dbName, String fingerprint, long lastTxId, long oldestRetainedTxId) {
  }
}
