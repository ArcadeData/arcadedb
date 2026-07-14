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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
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
 *       transfer Raft leadership to that peer <em>without committing anything</em>; the new leader
 *       re-enters this protocol on its own {@code notifyLeaderChanged} callback, elects itself, and
 *       commits every database then.</li>
 * </ol>
 * <p>
 * The protocol never commits a baseline before transferring leadership, so no <em>application</em>
 * entry is committed until the elected source commits the whole cluster's fingerprints. The
 * leadership transfer itself, however, makes Ratis append internal no-op / configuration entries, so
 * the new leader's raw commit index is <em>not</em> 0 by the time it re-runs (issue #5099). The
 * first-formation gate therefore keys on "the state machine has never applied an application entry"
 * rather than an exact {@code commitIndex == 0}: that signal survives the transfer's internal term
 * bump yet still closes the instant any real data commits, converging in at most one leadership
 * move. See {@link #isFirstFormation(long)}. A database whose freshest copy happens to live on a
 * node <em>other</em> than the elected source (only reachable by an operator manually staging
 * mismatched backups, never by normal single-leader replication) is protected by the
 * {@code localLastTxId > baseline} "refusing to overwrite" guard in
 * {@code ArcadeStateMachine.applyBootstrapFingerprintEntry}, which surfaces a SEVERE for the
 * operator instead of silently discarding the fresher data.
 * <p>
 * The protocol is idempotent across leader changes: each newly-elected leader re-checks the
 * first-formation gate and, if it still holds, re-runs. A leader transfer triggered by a previous
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
    SKIPPED_NOT_FIRST_FORMATION,// not first formation (application data already committed, or index unknown)
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
  // Maximum time to wait for getCommitIndex() to return a definitive (non-negative) value right after
  // this node becomes leader. The notifyLeaderChanged callback fires before the Raft division's log is
  // queryable, so the very first read returns the -1 UNKNOWN sentinel for a brief window (~100 ms in
  // practice); without waiting, the once-per-term bootstrap pass would skip on that transient -1 and
  // never re-run. 10 s is deliberately generous versus the observed ~100 ms readiness gap: the wait
  // ends as soon as the index resolves (or leadership is lost), so the full budget is only ever spent
  // on a genuinely broken read - a rare case that does not warrant an operator-facing config knob.
  // Package-private and non-final so tests can shorten it. See awaitReadableCommitIndex.
  long                                  commitIndexReadinessTimeoutMs = 10_000L;
  // Poll interval while waiting for the division to become readable. Package-private and non-final so
  // tests can set it to 0 and spin without sleeping.
  long                                  commitIndexReadinessPollMs    = 100L;
  // Backoff between bootstrap-state probe rounds. A peer whose HTTP/security stack is still
  // initializing when the probe arrives (common in a parallel cold boot where every pod starts at
  // once) answers 401/403/5xx; treating that as a definitive "no state" could elect a stale baseline
  // (issue #5273). We retry such probes within the overall bootstrap-timeout budget instead. Package-
  // private and non-final so tests can shorten them.
  long                                  probeRetryBackoffMs           = 500L;
  // Per-attempt HTTP timeout ceiling for a single bootstrap-state probe. The overall budget is
  // HA_BOOTSTRAP_TIMEOUT_MS (default 120s); capping each attempt lets an unreachable/slow peer be
  // retried rather than consuming the whole budget in one hung attempt.
  long                                  probeAttemptTimeoutMs         = 5_000L;

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

    // The bootstrap path is gated on first cluster formation: the cluster has never committed any
    // application data. See section 8 ("Gating") in #4147. A commit index of exactly 0 is the fast,
    // unambiguous case (empty Raft log, no leadership transfer). But a bootstrap that transfers
    // leadership to the elected source makes Ratis append internal no-op / configuration entries, so
    // the new leader's commit index is above 0 even though no application entry has committed (issue
    // #5099); the exact-0 test alone would wrongly reject it. isFirstFormation therefore also accepts
    // a positive commit index when the state machine has never applied an application entry - a signal
    // that survives the internal term bump yet still closes the instant any real data commits, keeping
    // the issue #4800 guarantee that bootstrap never re-engages on a running cluster.
    //
    // The notifyLeaderChanged callback that drives this runs before the freshly-elected leader's Raft
    // division exposes its committed index, so the very first read returns the -1 UNKNOWN sentinel for
    // a brief window. Since bootstrap runs at most once per term, skipping on that transient -1 would
    // leave the baseline uncommitted forever. Wait a bounded time for a definitive reading: a negative
    // value never opens the gate (division not ready / transient IOException, exactly as #4800
    // requires); a non-negative value is then judged by isFirstFormation.
    final long commitIndex = awaitReadableCommitIndex();
    if (!isFirstFormation(commitIndex))
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
   * The first-formation gate. Bootstrap may only engage on a cluster that has never committed any
   * application data, evaluated in two steps:
   * <ol>
   *   <li>A commit index of exactly {@code 0} - a positively-confirmed empty Raft log - opens the
   *       gate immediately. This is the no-transfer path and is unchanged from #4800
   *       (see {@link #isConfirmedFirstFormation}).</li>
   *   <li>A positive commit index opens the gate only when the state machine reports it has never
   *       applied an application entry ({@link ArcadeStateMachine#hasNeverAppliedApplicationEntry()}).
   *       This is the leadership-transfer path (issue #5099): the transfer appends internal
   *       no-op / configuration entries that raise the commit index above {@code 0} without
   *       committing any application data. The durable no-application-entry signal survives that
   *       internal term bump yet still closes the instant any real mutation commits, so #4800's
   *       "never re-engage on a running cluster" guarantee is preserved.</li>
   * </ol>
   * A negative commit index means the index is UNKNOWN - {@link RaftHAServer#getCommitIndex()}
   * returns {@code -1} when the Raft division is not ready yet or a transient {@code IOException} is
   * thrown while reading it - and MUST be treated as "skip" so a transient read failure during a
   * leader-change callback can never re-trigger bootstrap on a live cluster (issue #4800).
   * <p>
   * <b>Residual window (do not weaken the backstops without accounting for it):</b> the
   * no-application-entry signal is strictly weaker than the exact-{@code 0} test in one narrow case -
   * a node whose Raft log holds application entries that are committed but not yet run through
   * {@code applyTransaction} reads no applied index and no persisted file, so it reports first
   * formation. That state is essentially unreachable after normal operation (any prior apply writes
   * the {@code .raft/applied-index} file, and Raft's election restriction stops a node from winning
   * leadership until its log is up to date), and if it were ever hit, the elected source's commit is
   * still refused by the {@code localLastTxId > baseline} overwrite guard in
   * {@code ArcadeStateMachine.applyBootstrapFingerprintEntry} - so the worst case is a spurious,
   * refused bootstrap attempt, never data loss.
   */
  private boolean isFirstFormation(final long commitIndex) {
    if (isConfirmedFirstFormation(commitIndex))
      return true;
    if (commitIndex < 0L)
      return false; // division not ready / transient read failure: never treat as an empty log (#4800)
    final ArcadeStateMachine stateMachine = haServer.getStateMachine();
    return stateMachine != null && stateMachine.hasNeverAppliedApplicationEntry();
  }

  /**
   * The exact-{@code 0} empty-log test: {@code true} only for a positively-confirmed empty Raft log.
   * Kept as a distinct static helper (issue #4800) that {@link #isFirstFormation(long)} layers the
   * leadership-transfer signal on top of; a negative or positive commit index both return
   * {@code false} here.
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
    // Monotonic deadline: nanoTime is immune to wall-clock steps/NTP adjustments and the
    // (now - start) form is overflow-safe, unlike a currentTimeMillis-based absolute deadline.
    final long start = System.nanoTime();
    final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(commitIndexReadinessTimeoutMs);
    long commitIndex = haServer.getCommitIndex();
    while (commitIndex < 0 && (System.nanoTime() - start) < timeoutNanos && haServer.isLeader()) {
      try {
        Thread.sleep(commitIndexReadinessPollMs);
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

    // Collect every other peer's state, retrying transient probe failures (401/403/5xx/unreachable)
    // within the overall bootstrap-timeout budget instead of treating the first failure as a
    // definitive "no state" (issue #5273).
    final Map<RaftPeerId, String> peerAddresses = new LinkedHashMap<>();
    for (final RaftPeer peer : peers) {
      if (peer.getId().equals(localId))
        continue;
      final String httpAddr = httpAddresses.get(peer.getId());
      if (httpAddr == null) {
        LogManager.instance().log(this, Level.WARNING,
            "Bootstrap: peer %s has no known HTTP address; the election assumes it holds NO local data", peer.getId());
        continue;
      }
      peerAddresses.put(peer.getId(), httpAddr);
    }

    final List<RaftPeerId> assumedEmpty = new ArrayList<>();
    final Map<RaftPeerId, Map<String, PeerState>> remoteStates = collectRemoteStatesWithRetry(
        peerAddresses, (pid, addr, attemptMs) -> queryPeer(pid, addr, dbFilter, attemptMs),
        timeoutMs, Math.min(timeoutMs, probeAttemptTimeoutMs), probeRetryBackoffMs,
        haServer::isLeader, assumedEmpty);

    // Any configured peer that never returned a usable state after retries: warn loudly and state
    // exactly what the election assumes for it (empty baseline) so the operator can reason about
    // whether a stale baseline could have been chosen (issue #5273).
    if (!assumedEmpty.isEmpty())
      LogManager.instance().log(this, Level.SEVERE,
          """
          Bootstrap: peers %s did not return a usable state within %dms (after retries); the election \
          ASSUMES they hold no local data (empty baseline) and proceeds with the peers that responded. \
          If one of these peers actually holds the freshest copy it will resync from the elected leader \
          when it reconnects.""",
          assumedEmpty, timeoutMs);

    // Pivot: from peer→db→state to db→list of (peer, state).
    final Map<String, List<PeerState>> byDb = new HashMap<>();
    for (final var entry : selfStates.entrySet())
      byDb.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
    for (final var byPeer : remoteStates.values())
      for (final var entry : byPeer.entrySet())
        byDb.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
    return byDb;
  }

  /**
   * Round-based, retrying collection of remote peer states. Each round probes every still-unresolved
   * peer in parallel (via {@code prober}); peers that answer with data are resolved, peers that answer
   * with a retryable failure (401/403/5xx/unreachable - typically a peer whose HTTP/security stack is
   * still initializing during a parallel cold boot, issue #5273) are re-probed on the next round after
   * {@code backoffMs}. The loop stops when every peer is resolved, the {@code overallTimeoutMs} budget
   * is exhausted, or {@code keepGoing} reports this node is no longer the leader. Peers still
   * unresolved at the end are added to {@code assumedEmptyOut} so the caller can log what the election
   * assumes for them. Package-private and static so the retry/backoff/deadline logic is unit-testable
   * with an injected {@code prober} and no real HTTP.
   */
  static Map<RaftPeerId, Map<String, PeerState>> collectRemoteStatesWithRetry(
      final Map<RaftPeerId, String> peerAddresses, final ProbeFunction prober,
      final long overallTimeoutMs, final long attemptTimeoutMs, final long backoffMs,
      final BooleanSupplier keepGoing, final List<RaftPeerId> assumedEmptyOut) {
    final Map<RaftPeerId, Map<String, PeerState>> results = new HashMap<>();
    // Peers still needing a definitive answer; the address is kept so we can re-probe.
    final Map<RaftPeerId, String> pending = new LinkedHashMap<>(peerAddresses);
    // Monotonic deadline: nanoTime is immune to wall-clock steps and the (now - start) form is
    // overflow-safe.
    final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(overallTimeoutMs);
    while (!pending.isEmpty() && System.nanoTime() < deadlineNanos && keepGoing.getAsBoolean()) {
      final long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
      final long perAttemptMs = Math.max(1, Math.min(remainingMs, attemptTimeoutMs));

      // Fan out this round in parallel.
      final Map<RaftPeerId, CompletableFuture<ProbeOutcome>> round = new LinkedHashMap<>();
      for (final Map.Entry<RaftPeerId, String> e : pending.entrySet())
        round.put(e.getKey(), prober.probe(e.getKey(), e.getValue(), perAttemptMs));

      boolean anyRetryable = false;
      for (final Map.Entry<RaftPeerId, CompletableFuture<ProbeOutcome>> e : round.entrySet()) {
        final RaftPeerId peerId = e.getKey();
        ProbeOutcome outcome;
        try {
          outcome = e.getValue().get(perAttemptMs, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException te) {
          e.getValue().cancel(true);
          outcome = ProbeOutcome.retryable("probe timed out");
        } catch (final Exception ex) {
          outcome = ProbeOutcome.retryable(ex.getMessage());
        }
        switch (outcome.result()) {
        case OK -> {
          results.put(peerId, outcome.states());
          pending.remove(peerId);
        }
        case FATAL ->
          // Definitive answer that isn't going to change on retry (e.g. Raft HA not enabled on the
          // peer): stop probing it. It ends up in assumedEmptyOut below.
          pending.remove(peerId);
        case RETRYABLE -> anyRetryable = true;
        }
      }

      if (!pending.isEmpty() && anyRetryable && System.nanoTime() < deadlineNanos && keepGoing.getAsBoolean()) {
        try {
          Thread.sleep(backoffMs);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (assumedEmptyOut != null)
      for (final RaftPeerId peerId : peerAddresses.keySet())
        if (!results.containsKey(peerId))
          assumedEmptyOut.add(peerId);
    return results;
  }

  /**
   * Whether an HTTP status returned by a peer's {@code /bootstrap-state} probe should be retried
   * (rather than concluded as "no state"). {@code 401}/{@code 403} are the common case during a
   * parallel cold boot: the probed peer's HTTP server is up but its security stack is still
   * initializing so the shared cluster credentials are momentarily rejected (issue #5273).
   * {@code 408}/{@code 425}/{@code 429} and any {@code 5xx} are likewise transient "not ready yet"
   * answers. Everything else (notably {@code 200} success and {@code 400}/{@code 404} "Raft HA not
   * enabled / no such endpoint") is definitive.
   */
  static boolean isRetryableProbeStatus(final int statusCode) {
    return statusCode == 401 || statusCode == 403 || statusCode == 408 || statusCode == 425
        || statusCode == 429 || statusCode >= 500;
  }

  private CompletableFuture<ProbeOutcome> queryPeer(final RaftPeerId peerId,
      final String httpAddr, final Set<String> dbFilter, final long attemptTimeoutMs) {
    final String url = "http://" + httpAddr + "/api/v1/cluster/bootstrap-state";
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMillis(attemptTimeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{}"));
    final String token = haServer.getClusterToken();
    if (token != null && !token.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", token);
    builder.header("X-ArcadeDB-Forwarded-User", "root");

    return HTTP.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString())
        .thenApply(resp -> {
          final int status = resp.statusCode();
          if (status != 200) {
            final boolean retryable = isRetryableProbeStatus(status);
            // Transient failures are expected during a parallel cold boot and are retried below, so
            // log them at INFO (not WARNING) to avoid alarming operators mid-recovery (issue #5273).
            LogManager.instance().log(this, retryable ? Level.INFO : Level.WARNING,
                "Bootstrap: peer %s responded with HTTP %d to /bootstrap-state%s", peerId, status,
                retryable ? " (transient; retrying within the bootstrap budget)" : "");
            return retryable ? ProbeOutcome.retryable("HTTP " + status) : ProbeOutcome.fatal("HTTP " + status);
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
            return ProbeOutcome.ok(result);
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.WARNING,
                "Bootstrap: peer %s returned malformed JSON: %s", peerId, e.getMessage());
            return ProbeOutcome.retryable("malformed JSON: " + e.getMessage());
          }
        })
        .exceptionally(t -> {
          LogManager.instance().log(this, Level.INFO,
              "Bootstrap: failed to query peer %s (transient; retrying within the bootstrap budget): %s",
              peerId, t.getMessage());
          return ProbeOutcome.retryable(t.getMessage());
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
        // The elected source contributes no usable bootstrap state for this database, so there is
        // nothing to seed from it and its baseline is skipped. This is only reachable when an operator
        // staged a database onto a different node outside Raft, OR when this database was not part of
        // the elected source's own reported state at election time. Distinguish the two so the message
        // is not misleading during an outage recovery (issue #5273): "no local data" here means "no
        // bootstrap state / no committed transaction id", NOT "the database files are missing" - any
        // on-disk copy is left untouched and reconciled by the overwrite guard if a baseline is
        // committed for it later.
        if (local == null)
          LogManager.instance().log(this, Level.WARNING,
              "Bootstrap: elected source %s did not report bootstrap state for '%s' (database not present or not yet "
                  + "open on this node at election time); skipping its baseline. Any on-disk copy is left untouched.",
              localId, dbName);
        else
          LogManager.instance().log(this, Level.WARNING,
              "Bootstrap: elected source %s holds '%s' but it reports no committed transaction id (lastTxId=-1); "
                  + "nothing to seed, skipping its baseline. The on-disk copy is left untouched.", localId, dbName);
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

  /** A single peer probe, injected so {@link #collectRemoteStatesWithRetry} is testable without HTTP. */
  @FunctionalInterface
  interface ProbeFunction {
    CompletableFuture<ProbeOutcome> probe(RaftPeerId peerId, String httpAddr, long attemptTimeoutMs);
  }

  /** Classification of one {@code /bootstrap-state} probe attempt. */
  enum ProbeResult {
    OK,        // peer answered 200 with its state
    RETRYABLE, // transient failure (401/403/5xx/unreachable): re-probe within the budget
    FATAL      // definitive non-answer that won't change on retry (e.g. Raft HA not enabled)
  }

  /** Outcome of one {@code /bootstrap-state} probe attempt. {@code states} is non-null only for {@link ProbeResult#OK}. */
  record ProbeOutcome(ProbeResult result, Map<String, PeerState> states, String detail) {
    static ProbeOutcome ok(final Map<String, PeerState> states) {
      return new ProbeOutcome(ProbeResult.OK, states, null);
    }

    static ProbeOutcome retryable(final String detail) {
      return new ProbeOutcome(ProbeResult.RETRYABLE, null, detail);
    }

    static ProbeOutcome fatal(final String detail) {
      return new ProbeOutcome(ProbeResult.FATAL, null, detail);
    }
  }
}
