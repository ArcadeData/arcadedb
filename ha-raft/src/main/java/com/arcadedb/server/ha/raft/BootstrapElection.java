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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 *   <li>Elect a SINGLE bootstrap source node for the whole cluster. ArcadeDB runs one Raft group
 *       with one leader for every database, so every write flows through that one leader and the
 *       last leader necessarily holds the freshest copy of <em>all</em> databases at shutdown. We
 *       therefore pick one node - the node that is freshest (highest {@code lastTxId}) for the most
 *       databases (plurality), tie-broken by highest aggregate {@code lastTxId}, then prefer-self,
 *       then lexicographically-lowest peer id - and bootstrap every database from that node.</li>
 *   <li>If the elected source is the local peer, commit one
 *       {@link RaftLogEntryType#BOOTSTRAP_FINGERPRINT_ENTRY} per database via the existing
 *       transaction broker, all from the local copies. If the elected source is a different peer,
 *       transfer Raft leadership to that peer <em>without committing anything</em> (so the cluster
 *       commit index stays 0); the new leader re-enters this protocol on its own
 *       {@code notifyLeaderChanged} callback, elects itself, and commits every database then.</li>
 * </ol>
 * <p>
 * Because the protocol never commits a baseline before transferring leadership, the single
 * transfer-then-commit-all sequence keeps the {@code commitIndex == 0} first-formation gate valid
 * and converges in at most one leadership move. A database whose freshest copy happens to live on a
 * node <em>other</em> than the elected source (only reachable by an operator manually staging
 * mismatched backups, never by normal single-leader replication) is protected by the
 * {@code localLastTxId > baseline} "refusing to overwrite" guard in
 * {@code ArcadeStateMachine.applyBootstrapFingerprintEntry}, which surfaces a SEVERE for the
 * operator instead of silently discarding the fresher data.
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
    SKIPPED_NOT_FIRST_FORMATION,// commit index not a confirmed 0 (cluster running, or index unknown)
    SKIPPED_NO_DATABASES,       // nothing to bootstrap
    NOT_LEADER,                 // we're not the leader anymore by the time we ran
    TRANSFERRED,                // leadership transferred to the source peer; new leader will retry
    COMMITTED,                  // we committed BOOTSTRAP_FINGERPRINT_ENTRY for at least one DB
    FAILED                      // unexpected error; bootstrap will be re-attempted on next leader change
  }

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  // Poll interval while waiting for the freshly-elected leader's Raft division to become readable.
  private static final long             COMMIT_INDEX_READINESS_POLL_MS = 100L;

  private final RaftHAServer            haServer;
  private final ArcadeDBServer          server;
  // Tracks per-database whether the bootstrap protocol has been attempted in this leader's
  // lifetime. Used to short-circuit on repeated notifyLeaderChanged callbacks for the same term.
  private final Set<String>             attemptedThisTerm = ConcurrentHashMap.newKeySet();
  // Maximum time to wait for getCommitIndex() to return a definitive (non-negative) value right after
  // this node becomes leader. The notifyLeaderChanged callback fires before the Raft division's log is
  // queryable, so the very first read returns the -1 UNKNOWN sentinel for a brief window (~100 ms in
  // practice); without waiting, the once-per-term bootstrap pass would skip on that transient -1 and
  // never re-run. Package-private and non-final so tests can shorten it. See awaitReadableCommitIndex.
  long                                  commitIndexReadinessTimeoutMs = 10_000L;

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

    // The bootstrap path is gated on first cluster formation: every peer's Raft log empty, i.e. a
    // commit index of exactly 0. The leader's commit index is the only piece of cluster-wide state
    // we have to check; once any entry has been committed, we never re-engage. See section 8
    // ("Gating") in #4147. A negative value means the index is UNKNOWN (the Raft division is not
    // ready, or getCommitIndex() hit a transient IOException) and must NOT be mistaken for an empty
    // log - see isConfirmedFirstFormation and issue #4800.
    //
    // The notifyLeaderChanged callback that drives this runs before the freshly-elected leader's Raft
    // division exposes its committed index, so the very first read returns the -1 UNKNOWN sentinel for
    // a brief window. Since bootstrap runs at most once per term, skipping on that transient -1 would
    // leave the baseline uncommitted forever. Wait a bounded time for a definitive reading: on genuine
    // first formation it resolves to 0 and the gate opens; on an already-running cluster it resolves
    // to a positive index and the gate stays closed; if it never resolves (persistent read failure) we
    // conservatively skip, exactly as issue #4800 requires.
    final long commitIndex = awaitReadableCommitIndex();
    if (!isConfirmedFirstFormation(commitIndex))
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
   * The first-formation gate. Bootstrap may only engage on a positively-confirmed empty cluster,
   * i.e. a commit index of exactly {@code 0}. A negative value means the commit index is UNKNOWN -
   * {@link RaftHAServer#getCommitIndex()} returns {@code -1} when the Raft division is not ready yet
   * or a transient {@code IOException} is thrown while reading it - and MUST be treated as "skip" so
   * a transient read failure during a leader-change callback can never re-trigger bootstrap on a
   * live cluster (issue #4800). Any positive value means the cluster has already committed entries
   * and the first-formation window is long past.
   */
  static boolean isConfirmedFirstFormation(final long commitIndex) {
    return commitIndex == 0L;
  }

  /**
   * Poll {@link RaftHAServer#getCommitIndex()} until it returns a definitive (non-negative) value or
   * {@link #commitIndexReadinessTimeoutMs} elapses, then return the last reading. Right after a leader
   * change the division briefly reports -1 (log not yet queryable); this absorbs that startup window
   * so the gate sees the real first-formation index (0) instead of the transient -1. A non-negative
   * reading is returned immediately (no wait for a running cluster or a confirmed empty log). If this
   * node stops being the leader while waiting, we stop early and return the last reading so the gate
   * can skip. A persistent -1 (genuine read failure) is returned unchanged after the budget, keeping
   * the issue #4800 "never bootstrap on an unconfirmed index" guarantee.
   */
  private long awaitReadableCommitIndex() {
    final long deadline = System.currentTimeMillis() + commitIndexReadinessTimeoutMs;
    long commitIndex = haServer.getCommitIndex();
    while (commitIndex < 0 && System.currentTimeMillis() < deadline && haServer.isLeader()) {
      try {
        Thread.sleep(COMMIT_INDEX_READINESS_POLL_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      commitIndex = haServer.getCommitIndex();
    }
    return commitIndex;
  }

  /**
   * For each pending database, query every peer for its (fingerprint, lastTxId) via the Phase-3
   * RPC. Self is queried locally without HTTP. Peers that don't respond within
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
        final Map<RaftPeerId, Map<String, PeerState>> result = f.get(remaining, TimeUnit.MILLISECONDS);
        if (result != null)
          remoteStates.putAll(result);
      } catch (final TimeoutException te) {
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
          """
          Bootstrap timeout: peers %s did not report state within %dms; proceeding with majority. \
          Unreachable peers will catch up via leader-shipped snapshot when they reconnect.""",
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
                  db.getLong("lastTxId")));
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
        result.put(dbName, new PeerState(localId, dbName, fp, localDb.getLastTransactionId()));
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
   * Elect a single cluster-wide bootstrap source and act on it: either commit a baseline for every
   * local database (when the local peer is elected) or transfer leadership once to the elected peer
   * without committing anything (when a remote peer is elected). See the class Javadoc for why a
   * single source is both correct and sufficient under ArcadeDB's one-leader-per-cluster model.
   */
  private Outcome decideAndAct(final Map<String, List<PeerState>> states) {
    final RaftPeerId localId = haServer.getLocalPeerId();
    final long timeoutMs = server.getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_BOOTSTRAP_TIMEOUT_MS);

    final RaftPeerId source = electSourceNode(states, localId);

    // No peer reported any data for any database (every lastTxId is -1): the offline-bootstrap path
    // adds no value - there is nothing to ship and nothing to compare against. Skip silently.
    if (source == null)
      return Outcome.SKIPPED_NO_DATABASES;

    if (!source.equals(localId)) {
      // A remote peer is the elected source. Hand off Raft leadership to it WITHOUT committing
      // anything (commit index stays 0), so its own notifyLeaderChanged callback re-runs the
      // protocol, elects itself, and commits every database from its local copies.
      LogManager.instance().log(this, Level.INFO,
          "Bootstrap: transferring leadership to elected source %s (freshest copy of the cluster)", source);
      try {
        haServer.transferLeadership(source.toString(), timeoutMs);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Bootstrap: leadership transfer to %s failed: %s; will retry next term", source, e.getMessage());
        throw new RuntimeException(e);
      }
      return Outcome.TRANSFERRED;
    }

    // We are the elected source. Commit one BOOTSTRAP_FINGERPRINT_ENTRY per database we hold, all
    // from our local copies, in a deterministic order (sorted by database name).
    boolean anyCommitted = false;
    final List<String> dbNames = new ArrayList<>(states.keySet());
    Collections.sort(dbNames);
    for (final String dbName : dbNames) {
      final PeerState local = localStateFor(states.get(dbName), localId);
      if (local == null || local.lastTxId < 0) {
        // The elected source does not hold this database (or holds no data for it). This is only
        // reachable when an operator staged a database onto a different node outside Raft; warn so
        // the misconfiguration is visible. The node that does hold it keeps its copy; the overwrite
        // guard handles reconciliation if a baseline is ever committed for it later.
        LogManager.instance().log(this, Level.WARNING,
            "Bootstrap: elected source %s has no local data for '%s'; skipping its baseline", localId, dbName);
        continue;
      }

      LogManager.instance().log(this, Level.INFO,
          "Bootstrap source for '%s': peer=%s, lastTxId=%d, fingerprint=%s",
          dbName, local.peerId, local.lastTxId, abbreviate(local.fingerprint));
      try {
        haServer.getTransactionBroker().replicateBootstrapFingerprint(dbName, local.fingerprint, local.lastTxId);
        anyCommitted = true;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Bootstrap: failed to commit BOOTSTRAP_FINGERPRINT_ENTRY for '%s': %s", dbName, e.getMessage());
        // Re-throw to let runIfEligible() back off; we'll retry next term.
        throw new RuntimeException(e);
      }
    }

    return anyCommitted ? Outcome.COMMITTED : Outcome.SKIPPED_NO_DATABASES;
  }

  /**
   * Elect the single cluster-wide bootstrap source from the collected per-database peer states.
   * <p>
   * Each database votes for every peer holding its freshest ({@code max lastTxId}) copy. The winner
   * is the peer with the most votes (freshest for the most databases), tie-broken by highest
   * aggregate {@code lastTxId} across databases, then a preference for the local peer (to avoid an
   * unnecessary leadership transfer when the local peer is already as fresh as the best), then the
   * lexicographically-lowest peer id for full determinism. The function is order-independent so
   * every peer computing it from the same data reaches the same winner.
   *
   * @return the elected source peer id, or {@code null} when no peer reported any data.
   */
  static RaftPeerId electSourceNode(final Map<String, List<PeerState>> states, final RaftPeerId localId) {
    final Map<RaftPeerId, Integer> votes = new HashMap<>();
    final Map<RaftPeerId, Long> aggregate = new HashMap<>();
    for (final List<PeerState> peerStates : states.values()) {
      if (peerStates == null || peerStates.isEmpty())
        continue;
      long max = -1;
      for (final PeerState ps : peerStates)
        if (ps.lastTxId > max)
          max = ps.lastTxId;
      if (max < 0)
        continue; // no data for this database anywhere
      for (final PeerState ps : peerStates) {
        if (ps.lastTxId < 0)
          continue;
        aggregate.merge(ps.peerId, ps.lastTxId, Long::sum);
        if (ps.lastTxId == max)
          votes.merge(ps.peerId, 1, Integer::sum);
      }
    }
    if (votes.isEmpty())
      return null;

    final List<RaftPeerId> candidates = new ArrayList<>(votes.keySet());
    candidates.sort(Comparator.comparing(RaftPeerId::toString));
    RaftPeerId best = null;
    int bestVotes = -1;
    long bestAggregate = -1;
    for (final RaftPeerId candidate : candidates) {
      final int candidateVotes = votes.get(candidate);
      final long candidateAggregate = aggregate.getOrDefault(candidate, 0L);
      if (best == null
          || candidateVotes > bestVotes
          || (candidateVotes == bestVotes && candidateAggregate > bestAggregate)
          || (candidateVotes == bestVotes && candidateAggregate == bestAggregate
              && candidate.equals(localId) && !best.equals(localId))) {
        best = candidate;
        bestVotes = candidateVotes;
        bestAggregate = candidateAggregate;
      }
    }
    return best;
  }

  /** Returns the local peer's state for a database from its per-peer state list, or {@code null}. */
  private static PeerState localStateFor(final List<PeerState> peerStates, final RaftPeerId localId) {
    if (peerStates == null)
      return null;
    for (final PeerState ps : peerStates)
      if (ps.peerId.equals(localId))
        return ps;
    return null;
  }

  private static String abbreviate(final String fingerprint) {
    if (fingerprint == null || fingerprint.length() <= 16)
      return String.valueOf(fingerprint);
    return fingerprint.substring(0, 8) + "..." + fingerprint.substring(fingerprint.length() - 8);
  }

  /** Per-peer per-database state collected during bootstrap. */
  record PeerState(RaftPeerId peerId, String dbName, String fingerprint, long lastTxId) {
  }
}
