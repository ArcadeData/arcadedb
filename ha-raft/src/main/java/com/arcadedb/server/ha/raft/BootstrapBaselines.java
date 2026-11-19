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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.BootstrapBaseline;
import com.arcadedb.utility.FileUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * The bootstrap baselines of this node and the file that keeps them: per database, the fingerprint and last transaction
 * id the cluster agreed on at formation, the databases whose local copy could not be reconciled with it, and the
 * replacements still owed. They are loaded once, on first use, and every change is written back before it is relied on.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which consults them when it applies bootstrap and install entries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BootstrapBaselines {
  private final ArcadeStateMachine sm;

  /**
   * Per-database bootstrap baseline committed via {@link RaftLogEntryType#BOOTSTRAP_FINGERPRINT_ENTRY}.
   * Populated when the entry is applied (locally on every peer), used by the catch-up decision
   * tree (locally bootstrapped vs leader-shipped vs late-newer-joiner refusal). Issue #4147.
   * <p>
   * The map is also durably persisted to {@code .raft/bootstrap-baselines} and reloaded lazily on
   * first access. The committed {@code BOOTSTRAP_FINGERPRINT_ENTRY} is compacted below the Ratis
   * snapshot index and is therefore not replayed after a restart, and the map is not part of the
   * state-machine snapshot; without the persisted copy the durable baseline in the Raft log would be
   * invisible to {@link #getBootstrapBaseline} after a restart (issue #5100).
   */
  final ConcurrentHashMap<String, BootstrapBaseline> bootstrapBaselines =
      new ConcurrentHashMap<>();

  private volatile boolean bootstrapBaselinesLoaded   = false;

  final Object  bootstrapBaselinesFileLock = new Object();

  /**
   * Databases that took the "local is fresher, refuse to overwrite" branch of
   * {@link ArcadeStateMachine#applyBootstrapFingerprintEntry} and were therefore NOT reinstalled from the cluster's
   * chosen bootstrap source (issue #6124).
   * <p>
   * The refusal itself is correct - it protects a genuinely fresher operator copy from being
   * silently discarded - but it leaves this node's file-id space assigned by an independent history,
   * out of step with every other peer. Nothing reconciled that afterwards: issue #6118 made the one
   * fatal consequence (a later replicated schema change reusing a file id already in use here) throw
   * and resync, but that fires only if such an entry ever happens to arrive. A node that never
   * receives a colliding schema change stayed diverged indefinitely and the condition was invisible
   * outside a single SEVERE line emitted once at bootstrap.
   * <p>
   * The set is the durable record of that state: it is persisted alongside the baselines (the
   * {@code unreconciled} flag of each entry in {@code .raft/bootstrap-baselines}), because the
   * per-database replay-skip means the refusal branch never re-runs after a restart and the mark
   * would otherwise be lost. It is re-verified periodically against the leader by
   * {@link ArcadeStateMachine#verifyBootstrapDivergence()}, surfaced to operators by {@code ClusterAlerts}, and cleared
   * exactly where this node's copy is actually replaced by the leader's.
   */
  final Set<String> bootstrapUnreconciledDatabases = ConcurrentHashMap.newKeySet();

  BootstrapBaselines(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Returns the bootstrap baseline committed for {@code dbName}, or {@code null} if no
   * {@link RaftLogEntryType#BOOTSTRAP_FINGERPRINT_ENTRY} has been applied for it. Visible to
   * tests and the cluster-status exporter (Phase 7).
   */
  public BootstrapBaseline getBootstrapBaseline(final String dbName) {
    ensureBootstrapBaselinesLoaded();
    return bootstrapBaselines.get(dbName);
  }

  /**
   * Lazily parses the persisted bootstrap-baselines file once into {@link #bootstrapBaselines}. A
   * baseline already recorded in this session (freshly applied from a replayed entry) is authoritative,
   * so entries from the file are merged with {@code putIfAbsent} and never overwrite it. A
   * missing/unreadable file simply means "nothing persisted yet" and still latches the cache as loaded.
   * <p>
   * When the file path cannot yet be resolved (no server wired) the cache is NOT latched, so a later
   * call retries once the server is available. In the current wiring {@code setServer(...)} always runs
   * before the first read, so this only guards against a future reordering.
   */
  void ensureBootstrapBaselinesLoaded() {
    if (bootstrapBaselinesLoaded)
      return;
    synchronized (bootstrapBaselinesFileLock) {
      if (bootstrapBaselinesLoaded)
        return;
      final Path file = getBootstrapBaselinesFile();
      if (file == null)
        return; // server not wired yet: do not latch, retry once the path is resolvable
      try {
        if (Files.exists(file)) {
          final String content = Files.readString(file).trim();
          if (!content.isEmpty()) {
            final JSONObject json = new JSONObject(content);
            for (final String name : json.keySet()) {
              final JSONObject entry = json.getJSONObject(name);
              final String fingerprint = entry.getString("fingerprint", null);
              // putIfAbsent is defensive: recordBootstrapBaseline already loads before it puts, so a
              // session-applied baseline is written after this load runs and would win anyway; this
              // just guarantees the on-disk copy never overwrites a value already present in memory.
              if (fingerprint == null)
                continue;
              bootstrapBaselines.putIfAbsent(name, new BootstrapBaseline(fingerprint, entry.getLong("lastTxId", -1)));
              // The overwrite-guard mark (issue #6124). Unlike the baseline it is not re-derivable from
              // the Raft log after a restart - the per-database replay-skip stops the refusal branch from
              // running again - so the persisted flag is the only thing that carries it forward. Absent
              // in files written before #6124: getBoolean's default reads those as "not diverged".
              //
              // Read only for an entry that also carries a baseline, which is what makes "every marked
              // database has a baseline" an invariant of the in-memory state rather than a property of
              // the current call graph - and therefore what lets the writer below iterate the baselines
              // alone without dropping a mark on the floor.
              if (entry.getBoolean("unreconciled", false))
                bootstrapUnreconciledDatabases.add(name);
              // The pending bootstrap replacement (issue #8411), restored with the readiness holder it owns, so a
              // node restarted before the replacement landed does not come back serving the copy the committed
              // baseline rejected. Absent in files written before #8411, which read as "nothing pending".
              if (entry.getBoolean("pendingReplacement", false) && sm.bootstrapReplacementsPending.add(name)) {
                sm.bootstrapInstalls.beginBootstrapInstall(name);
                LogManager.instance().log(sm, Level.WARNING,
                    "Database '%s' is still waiting to be replaced by the leader's copy the cluster's bootstrap baseline "
                        + "chose (recorded before the restart). This node keeps it out of the Service and refuses client "
                        + "requests on it until it is; the periodic health check retries the install once a leader is "
                        + "reachable. To force it, run POST /api/v1/cluster/resync/%s on this node.", name, name);
              }
            }
          }
        }
      } catch (final Exception e) {
        LogManager.instance().log(sm, Level.FINE, "Could not read persisted bootstrap baselines: %s", e.getMessage());
      } finally {
        // The path was resolvable and we attempted a read: latch even on a parse failure so a corrupt
        // file is not re-read on every access (it degrades to no persisted baselines, which re-runs the
        // idempotent bootstrap verification on the next committed entry rather than losing correctness).
        bootstrapBaselinesLoaded = true;
      }
    }
  }

  /**
   * Records {@code baseline} for {@code dbName} and durably persists the baselines, all under
   * {@link #bootstrapBaselinesFileLock} so the in-memory mutation and the file write are one atomic
   * step (mirrors the applied-index writers, which mutate and persist together inside their lock).
   * The load-before-mutate keeps other databases' baselines intact when the file is rewritten.
   */
  void recordBootstrapBaseline(final String dbName, final BootstrapBaseline baseline) {
    synchronized (bootstrapBaselinesFileLock) {
      ensureBootstrapBaselinesLoaded();
      bootstrapBaselines.put(dbName, baseline);
      persistBootstrapBaselinesFile();
    }
  }

  /**
   * Removes {@code dbName}'s baseline and rewrites the file only if an entry was actually present, all
   * under {@link #bootstrapBaselinesFileLock}. Idempotent, so it is safe to call unconditionally on
   * every {@code DROP_DATABASE_ENTRY} (including replays and databases never present locally).
   */
  void evictBootstrapBaseline(final String dbName) {
    synchronized (bootstrapBaselinesFileLock) {
      ensureBootstrapBaselinesLoaded();
      // The overwrite-guard mark lives in the same file entry, so a dropped database must lose both or
      // the alert would outlive the database it names (issue #6124).
      final boolean wasUnreconciled = bootstrapUnreconciledDatabases.remove(dbName);
      if (bootstrapBaselines.remove(dbName) != null || wasUnreconciled)
        persistBootstrapBaselinesFile();
    }
    // A dropped database has no copy left to replace; a holder kept for it would refuse clients on a database
    // later recreated under the same name (issue #8367).
    sm.bootstrapInstalls.settleBootstrapReplacement(dbName);
  }

  /**
   * Durably records that {@code dbName} took the bootstrap "local is fresher, refuse to overwrite"
   * branch and is therefore diverged from the rest of the cluster (issue #6124). Written under the same
   * lock as the baselines because it is persisted in the same file entry; the caller has already
   * recorded the baseline, so the entry exists.
   */
  // @VisibleForTesting
  void markBootstrapUnreconciled(final String dbName) {
    synchronized (bootstrapBaselinesFileLock) {
      ensureBootstrapBaselinesLoaded();
      if (bootstrapUnreconciledDatabases.add(dbName))
        persistBootstrapBaselinesFile();
    }
  }

  /**
   * Clears the bootstrap-divergence mark of {@code dbName} (issue #6124). Called from every path that
   * actually replaces this node's copy with the leader's - the bootstrap install, the targeted and
   * operator-triggered resyncs, the forced reinstall - and from
   * {@link ArcadeStateMachine#reconcileBootstrapDivergence} when the two copies are confirmed identical. Idempotent, and
   * it does not rewrite the file when nothing was marked.
   */
  // @VisibleForTesting
  void clearBootstrapUnreconciled(final String dbName) {
    synchronized (bootstrapBaselinesFileLock) {
      ensureBootstrapBaselinesLoaded();
      if (bootstrapUnreconciledDatabases.remove(dbName))
        persistBootstrapBaselinesFile();
    }
  }

  /**
   * Clears the bootstrap-divergence mark (issue #6124) of every database not in {@code notReplaced}, in one durable
   * write. Used by the two full-resync paths, which reinstall every present database except the ones they gave up on
   * and the ones the leader does not hold (issue #8702): those still hold the copy the overwrite guard kept.
   */
  void clearBootstrapUnreconciledExcept(final Set<String> notReplaced) {
    synchronized (bootstrapBaselinesFileLock) {
      ensureBootstrapBaselinesLoaded();
      if (!bootstrapUnreconciledDatabases.isEmpty() && bootstrapUnreconciledDatabases.removeIf(
          dbName -> !notReplaced.contains(dbName)))
        persistBootstrapBaselinesFile();
    }
  }

  /** The present databases a full install did NOT replace with the leader's copy: given up on, or not held by the leader. */
  static Set<String> notReplaced(final Set<String> notInstalled, final Set<String> leaderMissing) {
    if (leaderMissing.isEmpty())
      return notInstalled;
    if (notInstalled.isEmpty())
      return leaderMissing;
    final Set<String> union = new HashSet<>(notInstalled);
    union.addAll(leaderMissing);
    return union;
  }

  /**
   * Clears every bootstrap-divergence mark (issue #6124), for a path that reinstalled EVERY database present on
   * this node from the leader - the same reasoning {@link ArcadeStateMachine#clearDivergedState()} makes for the diverged set.
   */
  // @VisibleForTesting
  void clearAllBootstrapUnreconciled() {
    synchronized (bootstrapBaselinesFileLock) {
      ensureBootstrapBaselinesLoaded();
      if (!bootstrapUnreconciledDatabases.isEmpty()) {
        bootstrapUnreconciledDatabases.clear();
        persistBootstrapBaselinesFile();
      }
    }
  }

  /**
   * Serialises {@link #bootstrapBaselines} to {@code .raft/bootstrap-baselines} via
   * {@link FileUtils#atomicWriteFile} (temp file, fsync, atomic rename with a non-atomic fallback and
   * temp cleanup), so a crash mid-write never leaves a corrupt file. Written only when a bootstrap
   * baseline is recorded or evicted (rare), not on the hot apply path. Callers hold
   * {@link #bootstrapBaselinesFileLock}.
   */
  void persistBootstrapBaselinesFile() {
    try {
      final Path file = getBootstrapBaselinesFile();
      if (file == null)
        return;
      final JSONObject json = new JSONObject();
      for (final Map.Entry<String, BootstrapBaseline> e : bootstrapBaselines.entrySet()) {
        final JSONObject entry = new JSONObject();
        entry.put("fingerprint", e.getValue().fingerprint());
        entry.put("lastTxId", e.getValue().lastTxId());
        // Written only when set, so a healthy cluster's file keeps exactly the shape it had before
        // issue #6124 and an older build reading it simply ignores the extra key.
        if (bootstrapUnreconciledDatabases.contains(e.getKey()))
          entry.put("unreconciled", true);
        // Same rule for the pending bootstrap replacement (issue #8411): written only while one is pending.
        if (sm.bootstrapReplacementsPending.contains(e.getKey()))
          entry.put("pendingReplacement", true);
        json.put(e.getKey(), entry);
      }
      // Iterating the baselines alone is sufficient for the marks too: a mark is only ever added right
      // after its baseline was recorded, and the loader refuses one on an entry that carries no
      // fingerprint, so a marked database without a baseline cannot exist in memory to be missed here.
      // The pending replacement follows the same rule: it is only armed by the bootstrap-mismatch arm of
      // applyBootstrapFingerprintEntry, which records the baseline first, and a DROP evicts both together.
      FileUtils.atomicWriteFile(file.toFile(), json.toString());
    } catch (final Exception e) {
      // WARNING, not FINE: unlike the applied-index file (whose loss merely re-runs an idempotent
      // verification), a lost bootstrap baseline silently re-introduces #5100 - the baseline would be
      // invisible after the next restart. Surface a breadcrumb so an operator can notice.
      LogManager.instance().log(sm, Level.WARNING, "Could not write persisted bootstrap baselines: %s", e.getMessage());
    }
  }

  private Path getBootstrapBaselinesFile() {
    final Path raftDir = sm.appliedIndexes.getRaftDir();
    return raftDir != null ? raftDir.resolve("bootstrap-baselines") : null;
  }
}
