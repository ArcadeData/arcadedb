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
import com.arcadedb.server.ArcadeDBServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * Database reconciliation collaborator for {@link ArcadeStateMachine} (issue #4727, refactored out in #4748).
 * <p>
 * Owns everything related to bringing a joining/catching-up follower's local database set in line with the
 * leader's during a Raft InstallSnapshot: the pure set-classification ({@link #classifyReconcile}), the
 * failure-isolated execution ({@link #executeReconcilePlan}), the status / give-up bookkeeping
 * ({@link #applyReconcileOutcome}), and the per-database auto-acquisition status surfaced in the cluster status
 * endpoint and the Studio HA panel. Keeping this out of the state machine isolates the reconciliation
 * orchestration from the core Ratis contract; {@link ArcadeStateMachine} simply delegates to a single
 * reconciler instance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseReconciler {

  public enum AcquireState {
    /** A full-snapshot pull of this database is in progress. */
    ACQUIRING,
    /** This database was successfully pulled from the leader. */
    ACQUIRED,
    /** This node holds this database but the leader does not - kept (not dropped); needs operator attention. */
    LEADER_MISSING,
    /** The last acquisition attempt failed; it will be retried on the next reconcile. */
    FAILED
  }

  /** Snapshot of a database's acquisition state for status export. */
  public record AcquireStatus(AcquireState state, long timestamp, String error) {
  }

  /**
   * The classification of a reconcile pass (issue #4727): which leader databases this node must acquire (never
   * seen), which it already has and merely refreshes, and which it holds that the leader does not (kept, flagged
   * {@link AcquireState#LEADER_MISSING}). Pure and package-private so the set logic is unit-testable.
   */
  public record ReconcilePlan(List<String> toAcquire, List<String> toRefresh, List<String> leaderMissing) {
  }

  /** A per-database install operation that may fail with {@link IOException}. */
  @FunctionalInterface
  interface DbInstallOp {
    void run(String databaseName) throws IOException;
  }

  /** Result of running a {@link ReconcilePlan}: which databases were acquired/refreshed and which failed. */
  public record ReconcileOutcome(List<String> acquired, Map<String, String> acquireFailures,
      List<String> refreshed, Map<String, String> refreshFailures) {
  }

  /**
   * Per-database auto-acquisition status (issue #4727), surfaced in the cluster status endpoint and the
   * Studio HA panel. Updated by {@link #reconcileDatabasesFromLeader} as a joining node reconciles its
   * local database set against the leader's.
   */
  private final ConcurrentHashMap<String, AcquireStatus> acquireStatuses = new ConcurrentHashMap<>();

  // Consecutive acquire/refresh failures per database (issue #4727). After ACQUIRE_GIVE_UP_AFTER consecutive
  // failures, that database stops failing the overall install so Ratis no longer re-triggers InstallSnapshot in a
  // tight loop - which would otherwise re-download every healthy database on this follower each pass just because
  // one database's snapshot opens on the leader but persistently fails validation here. The database is left
  // FAILED (surfaced in cluster status) and retried on the next natural InstallSnapshot; the counter resets on
  // any success.
  private final ConcurrentHashMap<String, Integer> acquireFailureCounts = new ConcurrentHashMap<>();
  private static final int                          ACQUIRE_GIVE_UP_AFTER = 3;

  private volatile ArcadeDBServer server;

  public DatabaseReconciler() {
  }

  public void setServer(final ArcadeDBServer server) {
    this.server = server;
  }

  /**
   * Computes the {@link ReconcilePlan} from the leader's database set and this node's local (non-reserved) set.
   * Lists are sorted for deterministic output. Pure - no side effects.
   */
  static ReconcilePlan classifyReconcile(final Set<String> leaderDbs, final Set<String> localDbs) {
    final List<String> toAcquire = new ArrayList<>();
    final List<String> toRefresh = new ArrayList<>();
    final List<String> leaderMissing = new ArrayList<>();
    for (final String db : leaderDbs) {
      if (localDbs.contains(db))
        toRefresh.add(db);
      else
        toAcquire.add(db);
    }
    for (final String db : localDbs)
      if (!leaderDbs.contains(db))
        leaderMissing.add(db);
    Collections.sort(toAcquire);
    Collections.sort(toRefresh);
    Collections.sort(leaderMissing);
    return new ReconcilePlan(toAcquire, toRefresh, leaderMissing);
  }

  /**
   * Runs a {@link ReconcilePlan}: acquires the never-seen databases and refreshes the existing ones, isolating
   * per-database failures so one bad database never starves the others. A failing acquire (e.g. a persistently
   * corrupt snapshot that fails validation every pass) must not abort the whole reconcile - otherwise the healthy,
   * already-replicated databases on this follower would never get refreshed. Each failure is collected and the
   * caller fails the overall install at the end so Ratis re-triggers (installs are idempotent). Package-private
   * and operation-injected so the no-starvation behavior is unit-testable without a Raft cluster.
   */
  static ReconcileOutcome executeReconcilePlan(final ReconcilePlan plan, final DbInstallOp acquireOp,
      final DbInstallOp refreshOp) {
    final List<String> acquired = new ArrayList<>();
    final Map<String, String> acquireFailures = new LinkedHashMap<>();
    final List<String> refreshed = new ArrayList<>();
    final Map<String, String> refreshFailures = new LinkedHashMap<>();

    // Catch Exception (not just the declared IOException): an unchecked failure escaping one database's
    // acquire/refresh must be isolated too, otherwise it would abort the whole reconcile and defeat the
    // no-starvation guarantee that is the entire point of this method.
    for (final String db : plan.toAcquire()) {
      try {
        acquireOp.run(db);
        acquired.add(db);
      } catch (final Exception e) {
        acquireFailures.put(db, String.valueOf(e.getMessage()));
      }
    }
    for (final String db : plan.toRefresh()) {
      try {
        refreshOp.run(db);
        refreshed.add(db);
      } catch (final Exception e) {
        refreshFailures.put(db, String.valueOf(e.getMessage()));
      }
    }
    return new ReconcileOutcome(acquired, acquireFailures, refreshed, refreshFailures);
  }

  /**
   * Reconciles this node's local database set against the leader's and installs every database the leader holds -
   * refreshing the ones already present and acquiring the ones this node has never seen (issue #4727).
   * <p>
   * Gated on {@link GlobalConfiguration#HA_AUTO_ACQUIRE_DATABASES}: when disabled, falls back to the legacy
   * behavior of refreshing only databases already present on disk. When the leader's database list cannot be
   * fetched (leader momentarily unreachable), it also falls back to the legacy refresh so the install never
   * regresses; the missing databases are retried on the next install-snapshot / leader change.
   * <p>
   * <b>Additive only:</b> a database present locally but absent on the leader is never dropped (that keeps a real
   * {@code DROP_DATABASE_ENTRY} distinguishable from "the leader never had it"); it is flagged
   * {@link AcquireState#LEADER_MISSING} for operator attention.
   * <p>
   * <b>Documented boundary:</b> auto-acquire runs only on the follower InstallSnapshot path, so a brand-new empty
   * node that became leader without first receiving a snapshot would not acquire anything. Raft's up-to-date-log
   * election rule prevents an empty-log node from winning over peers that hold committed data, and the
   * {@link AcquireState#LEADER_MISSING} alert plus the leadership-transfer flow cover the #4522 redistribution
   * edge, so this boundary is not reachable in normal operation.
   *
   * @throws IOException if a database install fails (so the caller leaves the Ratis snapshot install incomplete
   *                     and Ratis re-triggers it; installs are idempotent).
   */
  void reconcileDatabasesFromLeader(final String leaderHttpAddr, final String leaderHttpsAddr,
      final String clusterToken) throws IOException {

    final boolean autoAcquire = server.getConfiguration().getValueAsBoolean(
        GlobalConfiguration.HA_AUTO_ACQUIRE_DATABASES);

    if (!autoAcquire) {
      refreshExistingDatabases(leaderHttpAddr, leaderHttpsAddr, clusterToken);
      return;
    }

    // Enumerate the leader's databases via the existing bootstrap-state RPC. On failure, degrade to the legacy
    // refresh-existing-only path rather than failing the whole install - UNLESS this node holds no databases at
    // all, in which case ACKing the snapshot index would leave a fresh/empty follower believing it is caught up
    // with zero data installed (issue #4799); fail the install instead so Ratis retries.
    final List<LeaderDatabaseQuery.DatabaseInfo> leaderDbs;
    try {
      final long timeoutMs = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_BOOTSTRAP_TIMEOUT_MS);
      leaderDbs = LeaderDatabaseQuery.fetch(leaderHttpAddr, leaderHttpsAddr, clusterToken, timeoutMs, server);
    } catch (final InterruptedException e) {
      // Preserve the interrupt so the pool/executor can observe it and shut down cleanly.
      Thread.currentThread().interrupt();
      LogManager.instance().log(this, Level.WARNING,
          """
          Interrupted while listing the leader's databases for auto-acquire; refreshing only the databases \
          already present locally.""");
      refreshExistingDatabasesOrFailWhenEmpty(leaderHttpAddr, leaderHttpsAddr, clusterToken);
      return;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          """
          Could not list the leader's databases for auto-acquire (%s); refreshing only the databases already \
          present locally. Missing databases will be retried on the next reconcile.""", e.getMessage());
      refreshExistingDatabasesOrFailWhenEmpty(leaderHttpAddr, leaderHttpsAddr, clusterToken);
      return;
    }

    // Build the leader's database set. Skip entries the leader reported it could not open (lastTxId < 0): such a
    // database is not a usable snapshot source, so trying to acquire it would only fail validation every pass.
    final Set<String> leaderDbNames = new HashSet<>();
    for (final LeaderDatabaseQuery.DatabaseInfo info : leaderDbs) {
      if (info.lastTxId() < 0) {
        LogManager.instance().log(this, Level.WARNING,
            "Leader reported database '%s' as unreadable (no usable snapshot source); skipping it this reconcile.",
            info.name());
        continue;
      }
      leaderDbNames.add(info.name());
    }

    final Set<String> localDbNames = localUserDatabaseNames();

    // Prune acquisition statuses for databases that are neither local nor on the leader (e.g. a previously
    // LEADER_MISSING database later dropped via DROP_DATABASE_ENTRY). Otherwise the leader-missing alert would
    // stay raised for a database that no longer exists anywhere this node knows about.
    acquireStatuses.keySet().removeIf(name -> !localDbNames.contains(name) && !leaderDbNames.contains(name));
    acquireFailureCounts.keySet().removeIf(name -> !localDbNames.contains(name) && !leaderDbNames.contains(name));

    final ReconcilePlan plan = classifyReconcile(leaderDbNames, localDbNames);

    // Execute the plan with per-database failure isolation so one bad acquire never starves the refresh of the
    // healthy, already-replicated databases (and vice-versa). Statuses are applied from the outcome below, and
    // any failure fails the overall install at the end so Ratis re-triggers (installs are idempotent).
    final ReconcileOutcome outcome = executeReconcilePlan(plan,
        dbName -> {
          acquireStatuses.put(dbName, new AcquireStatus(AcquireState.ACQUIRING, System.currentTimeMillis(), null));
          LogManager.instance().log(this, Level.INFO, "Acquiring database '%s' from leader %s...", dbName, leaderHttpAddr);
          SnapshotInstaller.acquireNewDatabase(dbName, () -> leaderHttpAddr, () -> leaderHttpsAddr, clusterToken, server);
        },
        dbName -> {
          LogManager.instance().log(this, Level.INFO, "Refreshing database '%s' from leader %s...", dbName, leaderHttpAddr);
          // install() downloads with the database still open and only closes + swaps once a complete copy is on
          // disk, rolling back on failure, so a failed download never leaves the database closed.
          SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
              leaderHttpAddr, leaderHttpsAddr, clusterToken, server);
        });

    // Apply the status-map / failure-counter bookkeeping and decide whether to fail the overall install so Ratis
    // re-triggers it. Extracted to a package-private method so these transitions are unit-testable without a live
    // cluster (the InstallSnapshot path is not deterministically reachable in-process - see SnapshotAcquireNewDatabaseIT).
    if (applyReconcileOutcome(plan, outcome))
      throw new IOException(String.format("Reconcile from leader had database failure(s); will retry (acquire=%s, refresh=%s)",
          outcome.acquireFailures(), outcome.refreshFailures()));
  }

  /**
   * Applies the outcome of a reconcile pass to {@link #acquireStatuses} and {@link #acquireFailureCounts}, and
   * returns whether the overall install should be failed so Ratis re-triggers it. Package-private and free of
   * leader/network I/O so the FAILED/ACQUIRED/LEADER_MISSING transitions and the give-up decision are directly
   * unit-testable.
   *
   * @return {@code true} if at least one failed database is still within its retry budget
   *         ({@link #ACQUIRE_GIVE_UP_AFTER}); {@code false} when there were no failures, or every failure has
   *         exhausted its budget (so a persistently bad database no longer forces the whole install to re-run).
   */
  boolean applyReconcileOutcome(final ReconcilePlan plan, final ReconcileOutcome outcome) {
    // acquireStatuses records only genuine acquisitions, so operators can tell a true pull from a routine refresh.
    // A success also resets the consecutive-failure counter for that database.
    for (final String dbName : outcome.acquired()) {
      acquireStatuses.put(dbName, new AcquireStatus(AcquireState.ACQUIRED, System.currentTimeMillis(), null));
      acquireFailureCounts.remove(dbName);
    }
    // A refreshed database is one the leader holds, so clear any stale LEADER_MISSING/FAILED status left from a
    // prior pass (the resync / leadership-transfer recovery flow this PR documents).
    for (final String dbName : outcome.refreshed()) {
      acquireStatuses.remove(dbName);
      acquireFailureCounts.remove(dbName);
    }
    for (final Map.Entry<String, String> e : outcome.acquireFailures().entrySet())
      acquireStatuses.put(e.getKey(), new AcquireStatus(AcquireState.FAILED, System.currentTimeMillis(), e.getValue()));
    for (final Map.Entry<String, String> e : outcome.refreshFailures().entrySet())
      acquireStatuses.put(e.getKey(), new AcquireStatus(AcquireState.FAILED, System.currentTimeMillis(), e.getValue()));

    // Additive-only guard: a database we hold that the leader does not must NOT be dropped here. Flag it so the
    // cluster status / Studio surfaces it; a genuine DROP still arrives via DROP_DATABASE_ENTRY replay.
    for (final String dbName : plan.leaderMissing()) {
      acquireStatuses.put(dbName, new AcquireStatus(AcquireState.LEADER_MISSING, System.currentTimeMillis(), null));
      LogManager.instance().log(this, Level.WARNING,
          """
          Database '%s' is present locally but the leader does not hold it; keeping the local copy (not dropping). \
          If this node is an authoritative source, transfer leadership to a node that holds '%s' and resync.""",
          dbName, dbName);
    }

    // A failure is "still worth retrying" only until it has failed ACQUIRE_GIVE_UP_AFTER times in a row. Past that
    // a persistently bad database stops forcing the retry, so it no longer makes Ratis re-trigger InstallSnapshot
    // in a tight loop (which would re-download every healthy database on this follower each pass). It is left
    // FAILED and is still attempted on the next *natural* InstallSnapshot - which already re-downloads every
    // database anyway - so it can still recover if the leader's copy is later fixed; it just no longer hot-loops.
    boolean retryWorthwhile = false;
    for (final String dbName : outcome.acquireFailures().keySet())
      retryWorthwhile |= bumpFailureAndShouldRetry(dbName);
    for (final String dbName : outcome.refreshFailures().keySet())
      retryWorthwhile |= bumpFailureAndShouldRetry(dbName);
    return retryWorthwhile;
  }

  /**
   * Increments the consecutive-failure counter for a database and returns whether it is still within the retry
   * budget ({@link #ACQUIRE_GIVE_UP_AFTER}). Once exceeded, logs once and returns false so the caller stops
   * forcing Ratis to re-trigger the whole install for this one database.
   */
  private boolean bumpFailureAndShouldRetry(final String dbName) {
    final int count = acquireFailureCounts.merge(dbName, 1, Integer::sum);
    if (count < ACQUIRE_GIVE_UP_AFTER)
      return true;
    if (count == ACQUIRE_GIVE_UP_AFTER)
      LogManager.instance().log(this, Level.SEVERE,
          """
          Database '%s' failed to acquire/refresh %d times in a row; leaving it FAILED and no longer re-triggering \
          the snapshot install for it (it will be retried on the next install). Check the leader's copy.""",
          dbName, count);
    return false;
  }

  /** This node's local non-reserved (user) database names. */
  private Set<String> localUserDatabaseNames() {
    final Set<String> names = new HashSet<>();
    for (final String name : server.getDatabaseNames())
      if (!name.startsWith(ArcadeDBServer.RESERVED_DATABASE_PREFIX))
        names.add(name);
    return names;
  }

  /**
   * Decides whether the failure-path fallback (the leader's database list could not be enumerated while
   * auto-acquire is enabled) must fail the snapshot install instead of silently refreshing only what is already
   * on disk. Returns {@code true} when this node has NO local user databases: a fresh/empty follower that cannot
   * discover what the leader holds must not ACK the snapshot index with zero data installed, or later
   * {@code AppendEntries} deltas would apply to databases that were never created and the gap-detector could not
   * catch it (issue #4799). When the node already holds at least one user database, the legacy best-effort refresh
   * is kept and the missing databases are retried on the next reconcile. Pure and package-private so the decision
   * is unit-testable without a Raft cluster.
   */
  static boolean mustFailInstallWhenLeaderListUnavailable(final Set<String> localUserDbNames) {
    return localUserDbNames.isEmpty();
  }

  /**
   * Failure-path fallback for the auto-acquire flow when the leader's database list could not be enumerated.
   * Refreshes the databases already present locally (best effort, legacy behavior). If this node holds no
   * databases at all, throws instead so the caller leaves the Ratis snapshot install incomplete and Ratis
   * re-triggers it once the leader's list is reachable again - rather than ACKing the snapshot index on an
   * empty follower that received no data (issue #4799).
   */
  private void refreshExistingDatabasesOrFailWhenEmpty(final String leaderHttpAddr, final String leaderHttpsAddr,
      final String clusterToken) throws IOException {
    if (mustFailInstallWhenLeaderListUnavailable(localUserDatabaseNames()))
      throw new IOException(
          """
          Cannot enumerate the leader's databases for auto-acquire and this node holds no databases locally; \
          failing the snapshot install so Ratis retries rather than ACKing the snapshot index with no data \
          installed (issue #4799)""");
    refreshExistingDatabases(leaderHttpAddr, leaderHttpsAddr, clusterToken);
  }

  /**
   * Legacy behavior: refresh only the databases already present on this node from the leader. Used when
   * {@link GlobalConfiguration#HA_AUTO_ACQUIRE_DATABASES} is disabled or the leader's database list is
   * unavailable.
   */
  private void refreshExistingDatabases(final String leaderHttpAddr, final String leaderHttpsAddr,
      final String clusterToken) throws IOException {
    for (final String dbName : server.getDatabaseNames()) {
      // Skip reserved internal databases (e.g. the Raft control directory '.raft'): the leader does not serve them
      // as snapshots, so an install attempt would only fail. Mirrors the filter on the auto-acquire path.
      if (dbName.startsWith(ArcadeDBServer.RESERVED_DATABASE_PREFIX))
        continue;
      if (server.existsDatabase(dbName)) {
        LogManager.instance().log(this, Level.INFO,
            "Installing snapshot for database '%s' from leader %s...", dbName, leaderHttpAddr);
        SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
            leaderHttpAddr, leaderHttpsAddr, clusterToken, server);
      }
    }
  }

  /**
   * Clears the follower-side reconcile states when this node becomes leader (issue #4727).
   * <p>
   * LEADER_MISSING ("the leader lacks a database this node holds") and FAILED ("could not acquire from the
   * leader") are both follower-side reconcile states, meaningless once this node IS the leader. They are cleared
   * along with the parallel failure counters so their cluster alerts do not linger on the new leader - they would
   * otherwise survive because the leader never runs the follower reconcile path. ACQUIRED is harmless history and
   * is kept.
   */
  void clearFollowerReconcileStatesOnBecomeLeader() {
    acquireStatuses.values().removeIf(s -> s.state() == AcquireState.LEADER_MISSING || s.state() == AcquireState.FAILED);
    acquireFailureCounts.clear();
  }

  /** Snapshot of the per-database auto-acquisition status, for the cluster status endpoint (issue #4727). */
  public AcquireStatus getAcquireStatus(final String dbName) {
    return acquireStatuses.get(dbName);
  }

  /** Database names currently in the given auto-acquisition state (issue #4727), e.g. for cluster alerts. */
  public List<String> getDatabasesWithAcquireState(final AcquireState state) {
    final List<String> out = new ArrayList<>();
    for (final Map.Entry<String, AcquireStatus> e : acquireStatuses.entrySet())
      if (e.getValue().state() == state)
        out.add(e.getKey());
    Collections.sort(out);
    return out;
  }
}
