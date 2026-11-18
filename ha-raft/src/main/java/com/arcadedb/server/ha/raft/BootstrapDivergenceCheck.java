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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.BootstrapBaseline;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.BootstrapUnreconciled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * The databases whose local copy was kept at bootstrap although it could not be reconciled with the cluster's: it re-checks
 * them against the leader on a back-off and, once the leader's fingerprint is known, either clears them or installs the
 * leader's copy. A database missing locally is installed as soon as a leader can serve it.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which records the unreconciled databases when it applies a bootstrap entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BootstrapDivergenceCheck {
  private final ArcadeStateMachine sm;

  // Wall-clock of the last bootstrap-divergence verification submitted by verifyBootstrapDivergence();
  // 0 = none yet. Throttles the HealthMonitor-driven check, which ticks far more often than a probe of
  // the leader (which computes a SHA-256 over each database directory there) is worth paying for.
  private final AtomicLong lastBootstrapDivergenceCheckMs = new AtomicLong();

  // How often a still-unreconciled bootstrap divergence is re-verified against the leader. Deliberately
  // far slower than the snapshot backstops: the condition is permanent until an operator chooses which
  // copy the cluster keeps, so re-probing it at the health-tick rate would only re-hash every database
  // on the leader to reach the same conclusion. Five minutes still clears the mark promptly once the
  // copies do converge, and it bounds the repeated SEVERE to a rate an operator can live with.
  private static final long BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS = 300_000L;

  // Per-probe HTTP ceiling, matching BootstrapElection's own per-attempt cap: an unreachable or slow
  // leader must cost one bounded attempt, not park the lifecycle executor until the next check window.
  private static final long BOOTSTRAP_DIVERGENCE_PROBE_TIMEOUT_MS  = 5_000L;

  BootstrapDivergenceCheck(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * What is left of a flush-settle deadline shared by a sweep over several databases (issue #8843), never negative:
   * once it is spent, the remaining databases are hashed without waiting.
   */
  static long settleBudget(final long sweepDeadlineMillis) {
    return Math.max(0L, sweepDeadlineMillis - System.currentTimeMillis());
  }

  /**
   * Databases that took the bootstrap "local is fresher, refuse to overwrite" branch and have not been
   * reconciled with the cluster since (issue #6124), sorted for deterministic output. Read by
   * {@code ClusterAlerts} so the condition is visible in {@code GET /api/v1/cluster} rather than only in
   * a SEVERE line emitted once, at bootstrap, possibly several restarts ago.
   * <p>
   * The WHOLE marked set, both of the conditions {@link BootstrapUnreconciled} separates. A caller that renders
   * it to an operator wants that split instead - see there for why.
   */
  public List<String> getBootstrapUnreconciledDatabases() {
    sm.baselines.ensureBootstrapBaselinesLoaded();
    // The overwhelmingly common answer, and this is read on every Studio status poll: allocate nothing
    // for it.
    if (sm.baselines.bootstrapUnreconciledDatabases.isEmpty())
      return Collections.emptyList();
    final List<String> names = new ArrayList<>(sm.baselines.bootstrapUnreconciledDatabases);
    Collections.sort(names);
    return names;
  }

  /**
   * The marked set split by the one fact that decides what an operator should do about it: whether this node
   * still holds a copy of the database (issue #7902).
   * <p>
   * Both halves reach {@link ArcadeStateMachine#markBootstrapUnreconciled} and are indistinguishable in the set itself, but they
   * are opposite conditions:
   * <ul>
   *   <li>{@link ArcadeStateMachine#keptLocalCopy()} - the #6124 overwrite guard kept a copy FRESHER than the cluster's baseline.
   *       The data is here and intact; what is wrong is that its file ids came from a history no peer shares.</li>
   *   <li>{@link ArcadeStateMachine#missingLocally()} - the #7298 replay-skip found the database gone and could not pull it back.
   *       There is no copy here at all.</li>
   * </ul>
   * Reporting the second under the first's text told an operator that a database this node does not have was
   * "kept", and recommended copying this node's directory to every peer - which for a missing database would
   * overwrite every good copy in the cluster.
   *
   * @param seen which databases the caller may be told about by NAME; {@code null} for the unrestricted operator
   *             view. It reduces the names only: {@link ArcadeStateMachine#missingCount()} is the raw figure, because whether this
   *             node is serving a database is a node-level fact and not a per-tenant one, exactly as
   *             {@code localResync.inProgress} is
   */
  public BootstrapUnreconciled getBootstrapUnreconciled(final Set<String> seen) {
    sm.baselines.ensureBootstrapBaselinesLoaded();
    // Same fast path as above, and for the same reason: this runs on every Studio status poll, and the answer
    // is almost always "nothing is marked". Nothing is allocated and no file is stat'ed for it.
    if (sm.baselines.bootstrapUnreconciledDatabases.isEmpty())
      return BootstrapUnreconciled.NONE;

    final List<String> kept = new ArrayList<>();
    final List<String> missing = new ArrayList<>();
    int missingTotal = 0;
    for (final String dbName : getBootstrapUnreconciledDatabases()) {
      // The LIVE fact, not the reason the mark was recorded. A mark is durable and the condition under it is
      // not: an operator who restores a missing directory by hand, or deletes a kept copy, moves the database
      // from one half to the other without anything re-running the branch that marked it. The remedy has to
      // follow the database, so it is derived from where the database is now.
      //
      // existsDatabase OR the directory, because a closed-but-present database is still a copy this node holds:
      // reporting it as missing would recommend a reinstall over files an operator deliberately left closed.
      if (!sm.isDatabasePresentLocally(dbName)) {
        ++missingTotal;
        if (seen == null || seen.contains(dbName))
          missing.add(dbName);
      } else if (seen == null || seen.contains(dbName))
        kept.add(dbName);
    }
    return new BootstrapUnreconciled(kept, missing, missingTotal);
  }

  /**
   * Periodic re-verification of every database left diverged by the bootstrap overwrite guard
   * (issue #6124), driven by the {@link HealthMonitor} tick.
   * <p>
   * The guard is deliberately passive - it protects an operator's fresher copy by leaving it alone -
   * but it used to leave nothing behind that would ever look at that copy again. This asks the leader
   * for its current {@code (fingerprint, lastTxId)} for exactly those databases and either
   * <ul>
   *   <li><b>confirms convergence</b> (the leader's fingerprint now equals this node's, i.e. the two
   *       copies are byte-identical over the persisted content) and drops the mark, or</li>
   *   <li><b>escalates</b>: re-raises the divergence as an operator-visible SEVERE naming both states
   *       and the resync endpoint, and keeps the mark (and therefore the cluster alert) raised.</li>
   * </ul>
   * It deliberately does NOT resync by itself. Reinstalling from the leader is exactly what the guard
   * refused to do, and doing it later behind the operator's back would discard the fresher data the
   * guard exists to protect (the philosophy issue #6118 kept). The reconciliation is one explicit
   * {@code POST /api/v1/cluster/resync/{database}} away, and this makes sure someone knows to run it.
   * <p>
   * Zero cost in the normal case: the marked set is empty and the method returns after one volatile
   * read. When it is non-empty, the probe is throttled to one attempt per
   * {@link #BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS} - it makes the leader hash every database directory
   * it holds - and skipped on the leader, which has nothing to compare itself against.
   */
  public void verifyBootstrapDivergence() {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null || sm.server == null || raftHA.isLeader())
      return;
    sm.baselines.ensureBootstrapBaselinesLoaded();
    if (sm.baselines.bootstrapUnreconciledDatabases.isEmpty())
      return;

    // Both endpoints, from ONE look at the cluster, each having answered the two questions issue #6202
    // requires of an address that is acted on unattended: it must identify a single peer, and it must not be
    // our own.
    //
    // Through PeerDialAddress rather than by hand, and the encrypted half is why. getLeaderHttpsAddress()
    // answers only the second of the two, and its javadoc says so: it resolves through the raw resolver on
    // the argument that an address naming the wrong node is caught by the receiver's one-hop refusal of
    // LeaderForwardContext.FORWARDED_TO_LEADER_HEADER. That argument is the leader FORWARD's, and it does not
    // transfer here - POST /api/v1/cluster/bootstrap-state answers with the receiving node's own state and
    // refuses nothing. On a cluster declaring distinct 'http' ports and one shared 'https' port the HTTP guard
    // passes, and the probe would then dial an address identifying neither of the peers behind it and hand
    // whatever answered to reconcileBootstrapDivergence as the leader's state (issue #7563 review). The
    // resolver withholds such an endpoint, leaving the guarded plain one to fall back to.
    final RaftPeerId leaderId = raftHA.getLeaderId();
    final PeerDialAddress leaderDial = PeerDialAddress.resolve(raftHA, leaderId, "leader");
    if (leaderDial.refused())
      return; // no leader to compare against yet
    final String leaderHttpAddr = leaderDial.httpAddress();
    final String leaderHttpsAddr = leaderDial.httpsAddress();
    // Read once, off the volatile, so the task cannot see a different server than the one this tick checked.
    final ArcadeDBServer probeServer = sm.server;

    // Floored at the snapshot cadence so a WAN cluster that has widened its watchdog does not get probed
    // more often than it resyncs.
    if (!claimBootstrapDivergenceCheckSlot(System.currentTimeMillis(),
        Math.max(BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS, sm.computeSnapshotWatchdogTimeoutMs())))
      return;

    final Set<String> pending = new HashSet<>(sm.baselines.bootstrapUnreconciledDatabases);
    final String clusterToken = raftHA.getClusterToken();
    try {
      // Off the HealthMonitor thread: the probe is a blocking HTTP call to the leader and the monitor
      // tick drives the Ratis lifecycle checks behind it.
      //
      // On the lifecycleExecutor like every other leader-facing task, and bounded so it cannot become the
      // thing this module's CLAUDE.md warns about: the executor is single-threaded and already runs
      // multi-minute full resyncs on it, so what matters is that this task's worst case is small next to
      // those. It is - BOOTSTRAP_DIVERGENCE_PROBE_TIMEOUT_MS (5 s) at most, at most once per check window
      // (>= 5 minutes), against downloads measured in minutes. A queued resync therefore waits seconds in
      // the worst case, not for the length of a download.
      sm.lifecycleExecutor.submit(() -> {
        final Map<String, BootstrapBaseline> leaderStates = BootstrapElection.fetchBootstrapState(
            probeServer, leaderId.toString(), leaderHttpAddr, leaderHttpsAddr, clusterToken, pending, BOOTSTRAP_DIVERGENCE_PROBE_TIMEOUT_MS);
        if (leaderStates == null) {
          // The throttle slot is spent whether or not the probe answered, exactly as the stale-snapshot
          // backstop spends its own on a failed attempt: the next try is the next check window, not the
          // next health tick. Said plainly here so the wait is not read as seconds. The mark stays raised
          // in the meantime, so a failed probe never retires an alert.
          LogManager.instance().log(sm, Level.INFO,
              "Could not verify bootstrap divergence of %s against leader %s; the divergence stays reported "
                  + "and the check is retried in the next window (>= %d ms)",
              pending, leaderHttpAddr, BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS);
          return;
        }
        reconcileBootstrapDivergence(leaderStates);
      });
    } catch (final RejectedExecutionException ree) {
      LogManager.instance().log(sm, Level.WARNING,
          "Cannot schedule the bootstrap-divergence verification: executor is shut down", ree);
    }
  }

  /**
   * Claims the one bootstrap-divergence probe slot per {@code intervalMs} window, returning whether the
   * caller may probe. A lost CAS means another tick took the slot, so this one stands down rather than
   * probing twice. Extracted (package-private) so the throttle is testable without a live leader: the
   * rest of {@link #verifyBootstrapDivergence()} needs a reachable peer to reach it.
   */
  // @VisibleForTesting
  boolean claimBootstrapDivergenceCheckSlot(final long now, final long intervalMs) {
    final long previous = lastBootstrapDivergenceCheckMs.get();
    if (previous != 0 && now - previous < intervalMs)
      return false;
    return lastBootstrapDivergenceCheckMs.compareAndSet(previous, now);
  }

  /**
   * Compares this node's copy of every still-unreconciled database against the leader's reported state
   * and either clears the mark or re-raises the alert. Package-private and free of network I/O so the
   * verdict is unit-testable without a live cluster; {@link #verifyBootstrapDivergence()} supplies the
   * leader's states over the existing {@code /api/v1/cluster/bootstrap-state} RPC.
   * <p>
   * A database the leader does not report (dropped there, or not open) is left marked: absence is not
   * evidence of convergence.
   */
  // @VisibleForTesting
  void reconcileBootstrapDivergence(final Map<String, BootstrapBaseline> leaderStates) {
    final long settleDeadline = System.currentTimeMillis() + BootstrapFingerprint.SETTLE_MAX_WAIT_MILLIS;
    for (final String dbName : getBootstrapUnreconciledDatabases()) {
      final BootstrapBaseline leaderState = leaderStates.get(dbName);
      if (leaderState == null) {
        // Checked BEFORE the missing-directory retry below, deliberately: a leader that reports no state for
        // this database is not a leader worth pulling it from - it does not have it open, and an install
        // sourced from it would fail or, worse, succeed with nothing. The retry is throttled and self-healing,
        // so deferring to the next tick costs one interval and nothing else; the alternative, retrying against
        // a leader that just said it cannot speak for this database, costs a download every tick forever.
        LogManager.instance().log(sm, Level.INFO,
            "Bootstrap divergence of '%s' could not be verified: the leader reported no state for it", dbName);
        continue;
      }
      if (sm.server == null || !sm.server.existsDatabase(dbName)) {
        // Not currently loaded on this node. Deliberately neither cleared nor opened: existsDatabase
        // answers "is it in the registry", not "is it on disk", so retiring the alert here would drop a
        // real divergence for a database that is merely closed - and opening one just to fingerprint it
        // would undo an operator's decision to leave it closed. A database actually dropped loses its
        // mark with its baseline, on the DROP entry.
        //
        // The one case that is NOT "merely closed" is the database whose directory is gone too, which is
        // how a marked database looks after the replay-skip's reinstall failed (issue #7298). There is no
        // local copy to protect and nothing else retries it - the applied index already reads as applied -
        // so this tick is the bounded retry, throttled to BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS and
        // ending by itself the moment the install succeeds and clears the mark.
        if (!sm.databaseDirectoryExists(dbName))
          retryMissingBootstrapDatabase(dbName);
        continue;
      }
      final BootstrapBaseline local;
      try {
        local = sm.readLocalBootstrapState(dbName, settleBudget(settleDeadline));
      } catch (final Exception e) {
        // A database closed on a node going away is expected, not a warning (issue #9568)
        LogManager.instance().log(sm, sm.isClosedUnderShutdown(dbName, e) ? Level.FINE : Level.WARNING,
            "Could not read local state of '%s' to verify bootstrap divergence: %s", dbName, e.getMessage());
        continue;
      }
      if (local == null)
        continue; // not a LocalDatabase: nothing to fingerprint, leave the mark alone

      if (leaderState.fingerprint() != null && leaderState.fingerprint().equals(local.fingerprint())) {
        sm.baselines.clearBootstrapUnreconciled(dbName);
        LogManager.instance().log(sm, Level.INFO,
            "Database '%s' is no longer diverged from the cluster: its content now matches the leader "
                + "(fingerprint=%s, lastTxId=%d). Clearing the bootstrap-divergence alert.",
            dbName, BootstrapElection.abbreviate(local.fingerprint()), local.lastTxId());
        continue;
      }

      LogManager.instance().log(sm, Level.SEVERE,
          """
          Database '%s' is STILL diverged from the cluster after the bootstrap overwrite guard kept the \
          local copy: local (lastTxId=%d, fingerprint=%s) vs leader (lastTxId=%d, fingerprint=%s). This \
          node's file ids were assigned by an independent history, so a later replicated schema change \
          can collide with them. Either preserve this copy (stop the cluster and copy this node's \
          database directory to every peer) or discard it and adopt the leader's copy by running \
          POST /api/v1/cluster/resync/%s on this node.""",
          dbName, local.lastTxId(), BootstrapElection.abbreviate(local.fingerprint()),
          leaderState.lastTxId(), BootstrapElection.abbreviate(leaderState.fingerprint()), dbName);
    }
  }

  /**
   * Re-attempts the leader install of a marked database whose files are gone (issue #7298).
   * <p>
   * Runs on the {@code lifecycleExecutor}, not on the {@link HealthMonitor} tick that started the check and not on
   * the Raft apply thread - {@link #verifyBootstrapDivergence()} submits the whole probe-and-reconcile task there.
   * The distinction is worth stating because that executor is single-threaded: this is a DOWNLOAD, so unlike the
   * 5-second probe it is queued behind and ahead of tasks measured in minutes. That is the same class of work
   * {@code triggerSnapshotDownload} already does on it, and it is bounded by the same check window, so it adds a
   * task of a size the executor already carries rather than a new kind of one.
   * <p>
   * Never throws: a failure here just leaves the mark in place for the next window, which is the whole point of
   * hanging the retry off a periodic check.
   */
  private void retryMissingBootstrapDatabase(final String dbName) {
    LogManager.instance().log(sm, Level.WARNING,
        "Database '%s' is marked unreconciled and its directory is absent on this node; retrying the install from "
            + "the leader", dbName);
    try {
      sm.bootstrapInstalls.installFromLeaderForBootstrap(dbName);
    } catch (final RuntimeException e) {
      LogManager.instance().log(sm, Level.WARNING,
          "Reinstalling the missing database '%s' from the leader failed again: %s. Keeping the mark; the next "
              + "bootstrap-divergence check retries it", dbName, e.getMessage());
    }
  }
}
