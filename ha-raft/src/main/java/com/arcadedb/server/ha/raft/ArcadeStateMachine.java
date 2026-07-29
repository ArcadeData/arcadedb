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
import com.arcadedb.database.Binary;
import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Ratis state machine that bridges the Raft log and ArcadeDB storage.
 * <p>
 * Handles five entry types:
 * <ul>
 *   <li>{@code TX_ENTRY} - WAL page diffs from committed transactions</li>
 *   <li>{@code SCHEMA_ENTRY} - DDL operations with file creation/removal, buffered WAL entries,
 *       and schema JSON updates</li>
 *   <li>{@code INSTALL_DATABASE_ENTRY} - create a new database or force-restore from leader snapshot</li>
 *   <li>{@code DROP_DATABASE_ENTRY} - drop a database (idempotent on replay)</li>
 *   <li>{@code SECURITY_USERS_ENTRY} - replicate user/role changes across the cluster</li>
 * </ul>
 * <p>
 * <b>Threading model:</b> {@link #applyTransaction} is called sequentially by Ratis on a single
 * thread per Raft group. No concurrent apply calls occur for the same group.
 * <p>
 * <b>Idempotency:</b> All apply methods are safe for replay after a crash. {@code applyTxEntry}
 * uses page-version guards in {@link com.arcadedb.engine.TransactionManager#applyChanges} to skip
 * already-applied pages. {@code applySchemaEntry} uses file-existence guards for file creation
 * and the same page-version guards for WAL application. Schema reload is naturally idempotent.
 * <p>
 * <b>Crash recovery:</b> On startup, {@link SnapshotInstaller#recoverPendingSnapshotSwaps} is
 * called from {@link #initialize} to complete or roll back any snapshot installations that were
 * interrupted by a process crash.
 */
public class ArcadeStateMachine extends BaseStateMachine {

  /**
   * Test-only WAL gap counter. When non-null, incremented each time a follower detects a
   * WAL page-version gap. Used by deterministic tests to verify no gap occurred.
   * <p>
   * Tests that set this MUST reset it to {@code null} in an {@code @AfterEach} method, otherwise
   * it leaks into subsequent tests in the same JVM.
   */
  public static volatile AtomicInteger TEST_WAL_GAP_COUNTER = null;

  private final    SimpleStateMachineStorage storage          = new SimpleStateMachineStorage();
  private final    AtomicLong                lastAppliedIndex = new AtomicLong(-1);
  private final    AtomicLong                electionCount    = new AtomicLong(0);

  // Persisted applied-index bookkeeping. One ArcadeStateMachine multiplexes every database onto a
  // single Raft group, so a single global scalar cannot answer a per-database question: a co-located
  // database advancing the shared log past another database's entry would make the global value
  // overstate that other database's progress (issue #4824). We keep BOTH: a global Raft-log position
  // (the highest applied index across all databases, used by reinitialize()'s snapshot-gap check,
  // which compares against the inherently global Ratis snapshot index) AND a per-database map (used by
  // the per-database bootstrap replay-skip). The values live in memory so the hot apply path never
  // reads the file back; the file is parsed once lazily on first access and serialised on each write.
  // globalAppliedIndex tracks the same value as lastAppliedIndex (the AtomicLong above) on the apply
  // path. They are seeded independently (this one from the persisted file on load, lastAppliedIndex
  // from the Ratis snapshot in reinitialize()) and can briefly differ after reinitialize() - e.g. when
  // there is no snapshot lastAppliedIndex is -1 while globalAppliedIndex may hold the persisted value -
  // but every applyTransaction advances both to the same index, reconverging them.
  private final    Map<String, Long>         appliedIndexByDb     = new ConcurrentHashMap<>();
  private volatile long                      globalAppliedIndex   = -1;
  private volatile boolean                   appliedIndexLoaded   = false;
  private final    Object                    appliedIndexFileLock = new Object();
  private volatile long                      lastElectionTime = 0;
  private final    long                      startTime        = System.currentTimeMillis();
  // Tracks the previous leader so leader-change logs can show "X -> Y" instead of just "Y".
  // Useful when diagnosing churn: if X == Y across multiple changes, the leader is bouncing.
  private volatile RaftPeerId                previousLeaderId = null;
  // Tracks the highest term observed so notifyTermIndexUpdated can log only the first time we
  // see each term (otherwise it fires on every config/metadata entry, which is noisy).
  private final    AtomicLong                highestTermSeen  = new AtomicLong(-1);
  // Raft term seen at the last notifyLeaderChanged. Lets us tell a genuine re-election (term
  // advanced) from a same-term re-notification, so we only warn about real leader churn (#4809 follow-up).
  private volatile long                      lastNotifiedLeaderTerm = -1;

  private volatile ArcadeDBServer server;
  private volatile RaftHAServer   raftHAServer;

  /** Multiplier applied to HA_ELECTION_TIMEOUT_MAX when flooring the watchdog timeout. */
  static final int WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER = 4;

  private final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor(r -> {
    final Thread t = new Thread(r, "arcadedb-sm-lifecycle");
    t.setDaemon(true);
    return t;
  });

  /**
   * Removes dropped database directories away from the apply loop. Deliberately not the lifecycleExecutor: a
   * deletion is unbounded in the size of the database and would delay the snapshot-download triggers that
   * executor carries.
   */
  private volatile DeferredDatabaseDeleter deferredDatabaseDeleter = new DeferredDatabaseDeleter();

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
  private final ConcurrentHashMap<String, BootstrapBaseline> bootstrapBaselines =
      new ConcurrentHashMap<>();
  private volatile boolean bootstrapBaselinesLoaded   = false;
  private final    Object  bootstrapBaselinesFileLock = new Object();

  /** Per-database bootstrap baseline as it appears in the committed Raft log entry. */
  public record BootstrapBaseline(String fingerprint, long lastTxId) {
  }

  /**
   * Database reconciliation collaborator (issue #4727, extracted in #4748). Owns the per-database
   * auto-acquisition status, the failure/give-up bookkeeping, and the reconcile orchestration the state machine
   * delegates to from {@link #notifyInstallSnapshotFromLeader}. Exposed via {@link #getReconciler()} so
   * {@code GetClusterHandler} and {@code ClusterAlerts} can read the per-database statuses.
   */
  private final DatabaseReconciler reconciler = new DatabaseReconciler();

  private final AtomicBoolean needsSnapshotDownload      = new AtomicBoolean(false);
  private final AtomicBoolean snapshotDownloadInProgress = new AtomicBoolean(false);
  private final AtomicBoolean catchingUp                 = new AtomicBoolean(false);
  // Set to true after applyTransaction hits a genuinely unrecoverable, node-wide condition: a JVM
  // Error (OOM, StackOverflow - the JVM itself is unstable), an unknown committed entry type (#4798,
  // rolling-upgrade safety), or an unexpected error on an entry with no single target database
  // (e.g. SECURITY_USERS_ENTRY). In those cases the state machine's in-memory schema/page state can
  // be inconsistent (issue #4219: mid-load OOM leaves bucketMap cleared but not repopulated), so any
  // subsequent apply would cascade into "Bucket with id X was not found" errors before the async
  // server.stop() completes. Once tripped, applyTransaction fails fast without touching database
  // state and the recovery path is the asynchronous server shutdown plus a snapshot resync on the
  // next start.
  //
  // NOTE (issue #4797): an unexpected error applying an entry for a SINGLE database no longer trips
  // this node-wide flag. Because one ArcadeStateMachine multiplexes every database, halting the whole
  // node for one database's bad entry froze replication for all co-located databases. Such failures
  // are now quarantined per-database (see applyWithRetry): the affected database is marked diverged
  // and resynced from the leader while the node stays up and healthy databases keep replicating.
  private final AtomicBoolean haltedAfterCriticalError = new AtomicBoolean(false);

  // Database names whose state has diverged from the committed Raft log (a WALVersionGapException
  // was detected while applying an entry for them). While a database is in this set, unexpected
  // Throwables in applyWithRetry for THAT database are wrapped as ReplicationException (recoverable
  // resync) instead of propagating to the fatal server-halt path (issue #4740): operating on
  // inconsistent page state after a WAL gap often throws NPE, ClassCastException, or similar errors
  // that would otherwise halt the server even though the node is merely waiting for a snapshot
  // resync. Scoped per-database so a gap in one database never masks a genuine bug raised while
  // applying an entry for an unrelated, healthy database. Cleared when a snapshot resync completes
  // (it resyncs all databases) and restores consistent state.
  private final Set<String> divergedDatabases = ConcurrentHashMap.newKeySet();

  // Bounded escalation (issue #4740): a node that can never resync (no stable leader reachable)
  // must not stay in "swallow unexpected errors" mode forever, silently degrading. Each error
  // swallowed on a diverged database increments this; once it exceeds the threshold the next
  // unexpected error is allowed to propagate to the fatal halt path so a truly stuck node surfaces
  // loudly rather than quietly. Reset to 0 whenever a snapshot resync clears the diverged set.
  // Deliberately JVM-wide (not per-database): the threshold is a coarse "this node is stuck, halt
  // loudly" backstop, so a shared budget across all diverged databases is the intended behaviour -
  // one very noisy diverged database crossing the threshold should still halt the node.
  private final        AtomicInteger divergedSwallowedErrors      = new AtomicInteger(0);
  private static final int           MAX_DIVERGED_SWALLOWED_ERRORS = 100;

  // Log-flood throttle for a diverged database's "snapshot resync in progress" notice. Once a WAL
  // version gap has quarantined a database, EVERY subsequent committed entry for it hits the same gap
  // until the snapshot download lands - potentially thousands of entries on a busy database. Logging a
  // SEVERE (with stack trace) per entry both floods the log and, on small nodes, steals the CPU/IO the
  // snapshot download needs to heal the node (observed in the field: ~30 SEVERE/s for 20s starving a
  // ~1 MB/s resync). This map records the last time the throttled notice was emitted per database so it
  // fires at most once per window. Entries are cleared when the database's divergence clears.
  private final        Map<String, Long> lastDivergedResyncLogByDb        = new ConcurrentHashMap<>();
  private static final long              DIVERGED_RESYNC_LOG_THROTTLE_MS   = 5_000L;

  // Locally-originated transactions whose leader-side phase 2 was abandoned because replication
  // returned an INDETERMINATE result (the entry was dispatched to Ratis but submitAndWait timed out
  // before quorum was confirmed - see ReplicationDispatchedTimeoutException). Keyed by
  // "<databaseName>/<walTxId>". If such an entry later reaches quorum and is applied here,
  // applyTxEntry MUST apply it locally instead of origin-skipping it, otherwise the write lands on
  // every follower but never on this leader: a silent, permanent divergence (issue #4790). Marking
  // is always safe: it only changes behaviour IF the entry actually commits on this node's state
  // machine (applying is then correct because the followers have it); if the entry never commits,
  // the mark is inert and is pruned by TTL. Bounded by time-based pruning on insert.
  private final        Map<String, AbandonedPhase2> abandonedLocalTransactions = new ConcurrentHashMap<>();
  // Entries older than this are pruned on the next mark. Generous because a dispatched-but-stuck
  // entry can take a long time to either commit or be overwritten by a new leader.
  private static final long              ABANDONED_TX_TTL_MS           = 10 * 60 * 1000L;

  // In-flight leader-side phase 2 applies, ticket -> the applied index observed when the commit
  // started (its "replay floor"). A locally-originated entry is origin-skipped by applyTxEntry
  // because RaftReplicatedDatabase.commit's phase 2 writes the pages instead - but phase 2 runs
  // AFTER Raft commits the entry, so between the two this node has advanced lastAppliedIndex past
  // an entry whose pages are not on disk yet. takeSnapshot() must not hand that index to Ratis as a
  // durability checkpoint: the marker it writes is the only thing reinitialize() consults on
  // restart, so a checkpoint covering an unapplied entry makes the write unreplayable and lost
  // forever on this node (issue #5407). Registering the floor BEFORE replication and clamping
  // takeSnapshot() to it keeps the entry inside the replay window until phase 2 confirms.
  private final Map<Long, PendingPhase2> pendingLocalPhase2       = new ConcurrentHashMap<>();
  private final AtomicLong               pendingLocalPhase2Ticket = new AtomicLong();
  // A ticket is only released once its pages are settled, so one that is never released pins the
  // checkpoint - and therefore Raft log purge - until the node restarts. That is the intended
  // durability trade, but it must not be silent: without a signal an operator meets it as disk
  // pressure. Warn (throttled) once a held ticket outlives the threshold.
  private static final long STALLED_PHASE2_WARN_AFTER_MS = 5 * 60 * 1000L;
  private static final long STALLED_PHASE2_WARN_EVERY_MS = 60 * 1000L;
  private final AtomicLong  lastStalledPhase2WarnMs      = new AtomicLong();

  /** One in-flight leader-side phase 2: the replay floor to protect, and when it started. */
  private record PendingPhase2(long replayFloor, long startedAtMs) {
  }

  /**
   * One abandoned locally-originated transaction: the phase-2 ticket its commit is still holding,
   * and when the mark was inserted (for TTL pruning). Carrying the ticket is what lets
   * {@link #applyTxEntry} release it once the entry finally applies here, instead of leaving the
   * snapshot checkpoint - and therefore Raft log purging - pinned until the node restarts (#5410).
   */
  private record AbandonedPhase2(long phase2Ticket, long insertedAt) {
  }

  /**
   * Sentinel for "this entry carries no phase-2 ticket to release". Real tickets come from an
   * {@link AtomicLong#incrementAndGet()} and are therefore always positive.
   */
  static final long NO_PHASE2_TICKET = -1L;

  /**
   * Sentinel for "this transaction was never marked abandoned", i.e. the origin-skip case. Kept
   * distinct from {@link #NO_PHASE2_TICKET} on purpose: a transaction CAN be abandoned while holding
   * no ticket (the commit took none because this node was not the leader), and conflating the two
   * would make {@link #applyTxEntry} origin-skip an abandoned entry and reintroduce the #4790 lost
   * write.
   */
  static final long NO_ABANDONED_MARK = Long.MIN_VALUE;


  public void setServer(final ArcadeDBServer server) {
    this.server = server;
    reconciler.setServer(server);
  }

  /** The database reconciliation collaborator, used by {@code GetClusterHandler} and {@code ClusterAlerts}. */
  public DatabaseReconciler getReconciler() {
    return reconciler;
  }

  public void setRaftHAServer(final RaftHAServer raftHAServer) {
    this.raftHAServer = raftHAServer;
  }

  /** Owning Raft HA server. Package-private: used by the recovery-rewiring regression test (issue #4839). */
  RaftHAServer getRaftHAServer() {
    return raftHAServer;
  }

  /**
   * Initialises the state machine using Ratis-native SimpleStateMachineStorage so that snapshot
   * index tracking is delegated to the framework instead of a hand-rolled text file.
   */
  @Override
  public void initialize(final RaftServer raftServer, final RaftGroupId groupId, final RaftStorage raftStorage) throws IOException {
    super.initialize(raftServer, groupId, raftStorage);
    // Start the LifeCycle so getLifeCycleState() returns RUNNING while the state machine is active.
    // StateMachineUpdater.reload() asserts getLifeCycleState() == PAUSED (after pause() is called by
    // SnapshotInstallationHandler) at Ratis StateMachineUpdater.java:230. Without this start-up the
    // lifecycle stays in NEW and that precondition throws IllegalStateException (issue #4754).
    getLifeCycle().transition(LifeCycle.State.STARTING);
    getLifeCycle().transition(LifeCycle.State.RUNNING);
    storage.init(raftStorage);
    reinitialize();
    // Recover any snapshot installations that were interrupted by a crash
    if (server != null) {
      final String dbDir = server.getConfiguration().getValueAsString(
          GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      if (dbDir != null) {
        final Path databasesDirectory = Path.of(dbDir);
        SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDirectory);
        // Finish any deletion a crash or a shutdown cut short: the directories are reserved, so nothing else
        // will ever look at them.
        deferredDatabaseDeleter.sweepOrphanedStagingDirectories(databasesDirectory);
      }
    }
    LogManager.instance().log(this, Level.INFO, "ArcadeStateMachine initialized (groupId=%s)", groupId);
  }

  /**
   * Transitions the state machine to {@link LifeCycle.State#PAUSED} so that
   * {@code StateMachineUpdater.reload()} can proceed. Called by Ratis's
   * {@code SnapshotInstallationHandler} after {@link #notifyInstallSnapshotFromLeader} completes,
   * before signalling the updater to reload.
   * <p>
   * Idempotent: if the lifecycle is already PAUSED (e.g. a concurrent path already paused it),
   * the call is a no-op. If the lifecycle is in any unexpected state, a WARNING is logged and
   * the transition is skipped rather than crashing the caller.
   * <p>
   * <b>Invariant (verified against Ratis 3.2.2 source):</b> All three callers of
   * {@code StateMachine.pause()} in Ratis 3.2.2 are paired with a subsequent
   * {@link #reinitialize()} call that transitions the lifecycle back to RUNNING:
   * <ul>
   *   <li>{@code SnapshotInstallationHandler}: notification path (ArcadeDB's path) - pairs with
   *       {@code state.reloadStateMachine()} which triggers {@code reload()} then
   *       {@code reinitialize()}.</li>
   *   <li>{@code ServerState.installSnapshot()}: chunk-based path (not used when
   *       {@code HA_INSTALL_SNAPSHOT=false}) - same reload chain after last chunk.</li>
   *   <li>{@code RaftServerImpl.pause()}: external server-pause API - pairs with
   *       {@code RaftServerImpl.resume()} which calls {@code reinitialize()} directly.</li>
   * </ul>
   * If a future Ratis version introduces a {@code pause()} call without a matching
   * {@code reinitialize()}, the state machine would be stuck in PAUSED permanently.
   */
  @Override
  public void pause() {
    final LifeCycle.State current = getLifeCycleState();
    if (current == LifeCycle.State.RUNNING) {
      getLifeCycle().transition(LifeCycle.State.PAUSING);
      getLifeCycle().transition(LifeCycle.State.PAUSED);
    } else if (current != LifeCycle.State.PAUSED) {
      LogManager.instance().log(this, Level.WARNING,
          "pause() called in unexpected lifecycle state %s; skipping transition", current);
    }
  }

  /**
   * Restores {@link #lastAppliedIndex} from the latest Ratis {@link SimpleStateMachineStorage}
   * snapshot metadata. Called during {@link #initialize} and again if the state machine storage
   * is reset (e.g., during Ratis recovery via {@link RaftHAServer#restartRatisIfNeeded}).
   * <p>
   * When called from {@code StateMachineUpdater.reload()} after a snapshot install, the lifecycle
   * is in {@link LifeCycle.State#PAUSED} and this method transitions it back to
   * {@link LifeCycle.State#RUNNING} so the updater can resume applying log entries.
   */
  public void reinitialize() throws IOException {
    final long persistedApplied = readPersistedAppliedIndex();

    final var snapshotInfo = storage.getLatestSnapshot();
    if (snapshotInfo != null) {
      final long snapshotIndex = snapshotInfo.getIndex();
      final long snapshotGapTolerance = server != null
          ? server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_GAP_TOLERANCE)
          : GlobalConfiguration.HA_SNAPSHOT_GAP_TOLERANCE.getValueAsLong();
      if (persistedApplied >= 0 && snapshotIndex > persistedApplied + snapshotGapTolerance) {
        LogManager.instance().log(this, Level.INFO,
            "Snapshot index %d is ahead of persisted applied index %d, will download from leader when available",
            snapshotIndex, persistedApplied);
        needsSnapshotDownload.set(true);

        final long watchdogTimeoutMs = computeSnapshotWatchdogTimeoutMs();
        // Watchdog: if notifyLeaderChanged() doesn't fire within the configured timeout, trigger download directly
        lifecycleExecutor.submit(() -> {
          try {
            Thread.sleep(watchdogTimeoutMs);
            if (needsSnapshotDownload.compareAndSet(true, false)) {
              LogManager.instance().log(this, Level.WARNING,
                  "Snapshot download watchdog: no leader change after %dms, triggering download directly", watchdogTimeoutMs);
              triggerSnapshotDownload();
            }
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Snapshot download watchdog failed", e);
          }
        });
      }
      lastAppliedIndex.set(snapshotIndex);
      // If the on-disk marker carries an inflated term (issues #575, #593), this seed records it as-is
      // (the previous applied TermIndex is null here, so no violation is possible) and the first
      // re-applied entry realigns it via the tolerant updateLastAppliedTermIndex override. The stale
      // snapshot.<inflatedTerm>_<index> filename persists across restarts until the next snapshot
      // rolls it over: cosmetic, expected.
      updateLastAppliedTermIndex(snapshotInfo.getTerm(), snapshotIndex);
    } else
      lastAppliedIndex.set(-1);

    // When called from StateMachineUpdater.reload() after a snapshot install, the lifecycle is
    // PAUSED (pause() was called by SnapshotInstallationHandler). Transition back to RUNNING so
    // the updater can resume applying log entries. This is a no-op during the normal startup path
    // (lifecycle is already RUNNING when initialize() calls reinitialize()).
    if (getLifeCycleState() == LifeCycle.State.PAUSED) {
      getLifeCycle().transition(LifeCycle.State.STARTING);
      getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  /**
   * Ratis's {@link BaseStateMachine} enforces a strict, term-first monotonic invariant on
   * {@code lastAppliedTermIndex}: every update must be {@code >=} the previous one, comparing TERM
   * before INDEX. When the invariant is violated it halts the {@code StateMachineUpdater} thread,
   * which on ArcadeDB crash-loops the whole node and wedges leader election for the entire cluster.
   * <p>
   * One violation is <b>benign and must be tolerated</b>: a follower-installed snapshot can seed an
   * inflated applied TERM. {@link #notifyInstallSnapshotFromLeader} records the term of the first log
   * entry AFTER the snapshot as the snapshot's term, but that entry is not yet in the follower's log
   * at install time and a later leadership-change reconciliation can settle it on a LOWER term. When
   * Ratis then applies the real next committed entry, its {@code index} advances (genuine forward
   * progress) while its {@code term} is lower than the over-recorded snapshot term - tripping the
   * invariant even though nothing is actually wrong (issues #575, #593: a production cluster stuck with
   * all nodes {@code VOTING_FOR_ME} after {@code snapshot.11_39707283} vs a term-10 entry at
   * 39707284).
   * <p>
   * A committed Raft log never has a strictly lower term at a strictly higher index, so this exact
   * shape - index up, term down - cannot represent a genuine log inconsistency; it only arises from an
   * over-recorded snapshot term. We therefore realign the recorded term downward (via Ratis's own
   * unchecked {@link #setLastAppliedTermIndex}) and continue, logging a WARNING so operators still see
   * it. Every other ordering (index not advancing, or term not regressing) is delegated to
   * {@code super} unchanged, so real invariant violations still fail loudly.
   * <p>
   * This also makes recovery from an already-inflated on-disk marker automatic: on restart,
   * {@link #reinitialize()} seeds the inflated term while the previous applied TermIndex is still
   * {@code null} (no violation possible), and the first re-applied entry then takes the tolerant
   * branch above - the node self-heals without any manual marker rename. The stale marker filename
   * is cosmetic and is replaced by the next snapshot.
   */
  @Override
  protected boolean updateLastAppliedTermIndex(final TermIndex newTI) {
    final TermIndex oldTI = getLastAppliedTermIndex();
    if (isBenignSnapshotTermRegression(oldTI, newTI)) {
      LogManager.instance().log(this, Level.WARNING,
          "Tolerating applied-term realignment %s -> %s: the index advances (real progress) but the term "
              + "regressed from an over-recorded snapshot term; accepting the correction instead of halting "
              + "the state machine (issues #575, #593)", oldTI, newTI);
      // Mirrors BaseStateMachine's advancing-update path (store + return true) but without the strict
      // term-first assertion. Verified against the Ratis 3.2.2 source, which reads:
      //
      //   final TermIndex oldTI = lastAppliedTermIndex.getAndSet(newTI);
      //   if (!newTI.equals(oldTI)) {
      //     ... Preconditions.assertTrue(newTI.compareTo(oldTI) >= 0, ...);
      //     return true;                                    // advancing path: NO future completion
      //   }
      //   synchronized (transactionFutures) { ... complete(null) ... }  // ONLY the equal/no-op path
      //
      // i.e. super completes pending queryStale() futures only on the no-op path (newTI equals oldTI),
      // never on an advancing update, so this branch has no bookkeeping to replicate: the pending
      // futures complete on the next duplicate update, which is not a benign regression and therefore
      // delegates to super (pinned by the pendingStaleQueryFuture... regression test).
      // RE-VERIFY THIS on any Ratis upgrade.
      //
      // The read-then-set is not atomic (super uses a single getAndSet) but cannot interleave: of the
      // three call sites, applyTransaction runs on the single StateMachineUpdater thread, and both
      // snapshot seeds run while that thread is not applying entries - reinitialize() is invoked by
      // the updater itself (startup or reload() while PAUSED) and notifyInstallSnapshotFromLeader
      // pauses the state machine for the install.
      setLastAppliedTermIndex(newTI);
      return true;
    }
    return super.updateLastAppliedTermIndex(newTI);
  }

  /**
   * Returns {@code true} for the one applied-term-index transition ArcadeDB tolerates over Ratis's
   * strict monotonic check: the {@code index} strictly advances while the {@code term} strictly
   * regresses - the fingerprint of an over-recorded (inflated) snapshot term being corrected by the
   * real next committed entry (issues #575, #593). All other transitions return {@code false} and stay
   * subject to Ratis's invariant enforcement. Package-private and static for direct unit testing.
   * <p>
   * SAFETY PRECONDITION: this shape is provably benign only because every caller feeds either COMMITTED
   * log entries (a committed Raft log never carries a strictly lower term at a strictly higher index) or
   * the lifecycle-serialized snapshot seed. Never route synthetic or uncommitted TermIndex values
   * through {@code updateLastAppliedTermIndex}: a genuine bug with this exact shape would be silently
   * tolerated (WARNING only) instead of failing loudly.
   */
  static boolean isBenignSnapshotTermRegression(final TermIndex oldTI, final TermIndex newTI) {
    return oldTI != null && newTI != null
        && newTI.getIndex() > oldTI.getIndex()
        && newTI.getTerm() < oldTI.getTerm();
  }

  /**
   * Called by Ratis on the leader when a client request is received, before the entry is
   * replicated. Sets a marker in the {@link TransactionContext} so that {@link #applyTransaction}
   * can identify entries that were originated (and pre-applied) by this node in the current
   * lifecycle, without relying on a runtime {@code isLeader()} check that is susceptible to
   * TOCTOU races if leadership changes between submission and apply.
   * <p>
   * Only requests submitted by THIS node's own {@code RaftClient} are marked as locally-originated.
   * Requests forwarded from a follower's {@code RaftClient} carry a different {@code ClientId} and
   * must NOT be marked, because Phase 2 never ran on this node for follower-submitted transactions.
   */
  @Override
  public TransactionContext startTransaction(final RaftClientRequest request) throws IOException {
    final RaftHAServer raft = this.raftHAServer;
    final boolean isLocalOrigin = raft != null
        && raft.getClient() != null
        && raft.getClient().getId().equals(request.getClientId());

    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request)
        .setStateMachineContext(isLocalOrigin ? Boolean.TRUE : null)
        .build();
  }

  @Override
  public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();
    final ByteString data = entry.getStateMachineLogEntry().getLogData();
    final TermIndex termIndex = TermIndex.valueOf(entry);
    final long index = termIndex.getIndex();

    // Refuse to apply once a prior entry tripped the critical-error halt. Continuing would
    // operate on the inconsistent in-memory state left behind by the failed apply and cascade
    // into additional SEVERE errors before the async server.stop() completes (#4219).
    if (haltedAfterCriticalError.get())
      return CompletableFuture.failedFuture(new ReplicationException(
          "State machine halted after critical error at earlier index; refusing to apply index " + index));

    // Captured after decode so the catch blocks can tell whether a ReplicationException is the expected
    // resync-in-progress signal for an already-quarantined database (throttled at the source) or a
    // genuine replication error that must still be logged loudly. Null until decode succeeds.
    String targetDatabase = null;
    try {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(data);
      targetDatabase = decoded.databaseName();

      if (decoded.type() == null) {
        // A committed entry whose leading type byte is unrecognised (e.g. written by a newer node
        // during a rolling upgrade) is NOT safe to skip. Advancing lastAppliedIndex past it would
        // permanently discard a committed mutation on this node, and because the index still moved
        // forward no lag/gap recovery would ever notice - a silent divergence (issue #4798). Halt
        // loudly instead: leave lastAppliedIndex untouched so the entry is replayed once this node
        // is upgraded to a version that understands the type, and surface the problem to operators.
        LogManager.instance().log(this, Level.SEVERE,
            "CRITICAL: Unknown Raft log entry type at index %d (likely written by a newer node version). "
                + "Refusing to skip a committed entry and halting to prevent silent state divergence; "
                + "upgrade this node to a compatible version to resume.", index);
        triggerCriticalHalt();
        return CompletableFuture.failedFuture(new ReplicationException(
            "Unknown Raft log entry type at index " + index + "; node halted to prevent silent divergence"));
      }

      final boolean originatedLocally = Boolean.TRUE.equals(trx.getStateMachineContext());

      applyWithRetry(index, decoded.databaseName(), () -> {
        switch (decoded.type()) {
        case TX_ENTRY -> applyTxEntry(decoded, index, originatedLocally);
        case SCHEMA_ENTRY -> applySchemaEntry(decoded, index, originatedLocally);
        case INSTALL_DATABASE_ENTRY -> applyInstallDatabaseEntry(decoded);
        case DROP_DATABASE_ENTRY -> applyDropDatabaseEntry(decoded);
        case SECURITY_USERS_ENTRY -> applySecurityUsersEntry(decoded);
        case BOOTSTRAP_FINGERPRINT_ENTRY -> applyBootstrapFingerprintEntry(decoded, index);
        }
      });

      final long previousApplied = lastAppliedIndex.getAndSet(index);
      updateLastAppliedTermIndex(termIndex.getTerm(), index);
      // Record the index globally AND against the database this entry targeted, so the per-database
      // bootstrap replay-skip can trust a value that is not mixed across databases (issue #4824).
      // decoded.databaseName() is null only for database-agnostic entries (e.g. SECURITY_USERS_ENTRY),
      // which advance the global position only. A DROP entry removes the database, so the global
      // position advances and its per-database entry is evicted in a single atomic write (avoids
      // growing the map for the node lifetime with names of dropped databases).
      if (decoded.type() == RaftLogEntryType.DROP_DATABASE_ENTRY)
        writePersistedAppliedIndexDroppingDatabase(index, decoded.databaseName());
      else
        writePersistedAppliedIndex(index, decoded.databaseName());

      // Wake up any threads waiting for this index (READ_YOUR_WRITES, waitForLocalApply)
      final RaftHAServer raftHA = this.raftHAServer;
      if (raftHA != null) {
        raftHA.notifyApplied();

        // Detect hot resync on followers
        if (!raftHA.isLeader()) {
          final long gap = index - previousApplied;
          if (gap > 1 && catchingUp.compareAndSet(false, true))
            HALog.log(this, HALog.BASIC, "Follower catching up: gap=%d (previous=%d, current=%d)",
                gap, previousApplied, index);
          if (catchingUp.get()) {
            final long commitIndex = raftHA.getCommitIndex();
            if (commitIndex > 0 && index >= commitIndex) {
              catchingUp.set(false);
              HALog.log(this, HALog.BASIC, "Hot resync complete: applied=%d >= commit=%d", index, commitIndex);
            }
          }
        }
      }
      return CompletableFuture.completedFuture(Message.valueOf("OK"));

    } catch (final ReplicationException e) {
      // A resync-required signal for an already-quarantined database repeats on every committed entry
      // until the snapshot download lands, and whoever quarantined the database (applyTxEntry on a WAL
      // gap, or applyWithRetry's quarantine path) has already logged it loudly at the source. Don't
      // also dump a full stack trace here per entry (the field-observed flood). Genuine replication
      // errors on a database that is NOT diverged still log loudly with the cause.
      if (targetDatabase == null || !isDatabaseDiverged(targetDatabase))
        LogManager.instance().log(this, Level.SEVERE, "Replication error at index %d: %s", e, index, e.getMessage());
      return CompletableFuture.failedFuture(e);
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(this, Level.WARNING, "Invalid raft log entry at index %d: %s", index, e.getMessage());
      return CompletableFuture.failedFuture(e);
    } catch (final Throwable e) {
      // Unexpected errors (NPE, ClassCastException, OOM, etc.) indicate a bug that could cause
      // state divergence if silently swallowed. Crash the server so the node recovers via snapshot.
      LogManager.instance().log(this, Level.SEVERE,
          """
          CRITICAL: Unexpected error applying Raft log entry at index %d. \
          Shutting down to prevent state divergence.""", e, index);
      triggerCriticalHalt();
      return CompletableFuture.failedFuture(e instanceof Exception ex ? ex : new RuntimeException(e));
    }
  }

  /**
   * Trips the critical-error halt and asynchronously stops the server so the node recovers via a
   * snapshot/log replay on the next start. Used by {@link #applyTransaction} for both unexpected
   * apply errors and unknown (un-decodable) committed entry types (issue #4798).
   * <p>
   * The halt flag is set BEFORE the async {@code server.stop()} starts so the StateMachineUpdater's
   * next {@code applyTransaction} call short-circuits instead of cascading on inconsistent state.
   * Callers must NOT advance or persist {@link #lastAppliedIndex} before invoking this: leaving the
   * index untouched is what lets the offending entry be replayed (instead of silently skipped) once
   * the node restarts on a compatible version.
   */
  private void triggerCriticalHalt() {
    haltedAfterCriticalError.set(true);
    final Thread stopThread = new Thread(() -> {
      try {
        if (server != null)
          server.stop();
      } catch (final Throwable t) {
        LogManager.instance().log(this, Level.SEVERE, "Emergency stop failed", t);
      }
    }, "arcadedb-emergency-stop");
    stopThread.setDaemon(true);
    stopThread.start();
  }

  /**
   * Convenience overload that runs the dispatch without scoping the diverged-state guard to a
   * specific database (equivalent to {@code applyWithRetry(index, null, applyAction)}). Used where
   * the entry has no single target database.
   */
  // @VisibleForTesting
  void applyWithRetry(final long index, final Runnable applyAction) {
    applyWithRetry(index, null, applyAction);
  }

  /**
   * Runs the apply dispatch with bounded in-place retry for transient/retryable conditions.
   * <p>
   * A {@link NeedRetryException} (e.g. an MVCC {@link com.arcadedb.exception.ConcurrentModificationException}
   * from a page-version race) is NOT state divergence: the apply is deterministic and idempotent
   * (page-version / file-existence guards), so a retry can win the race. We retry up to
   * {@link GlobalConfiguration#TX_RETRIES} times; only if the condition persists do we escalate to a
   * {@link ReplicationException}, which {@link #applyTransaction} turns into a snapshot resync.
   * Crucially, a retryable error never reaches the fatal {@code catch (Throwable)} branch that stops
   * the server - "retry" must never mean "crash the node".
   * <p>
   * Note on backoff: {@link GlobalConfiguration#TX_RETRY_DELAY} defaults to 100ms and was tuned for
   * MVCC contention among many concurrent user-transaction threads. The Raft {@code StateMachineUpdater}
   * is a single sequential thread, so a smaller (or zero) delay is perfectly safe on this path and only
   * reduces the worst-case latency per entry; the value is read live so it can be tuned independently.
   *
   * @param index        the Raft log index being applied (diagnostics only)
   * @param databaseName the database the entry targets, used to scope the diverged-state guard
   *                     (may be {@code null} for entry types without a single target database)
   * @param applyAction  the apply dispatch to run
   * @throws ReplicationException if the retryable condition persists after all attempts
   */
  // @VisibleForTesting
  void applyWithRetry(final long index, final String databaseName, final Runnable applyAction) {
    final int maxRetries = Math.max(0, server != null
        ? server.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES)
        : GlobalConfiguration.TX_RETRIES.getValueAsInteger());
    final int retryDelay = server != null
        ? server.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY)
        : GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();

    NeedRetryException lastRetry = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        applyAction.run();
        return;
        // Catch the whole NeedRetryException hierarchy on purpose: the subclass normally reachable here
        // is the engine's MVCC ConcurrentModificationException (page-version race), which a retry can
        // win, and the broad type stays forward-compatible with future retryable errors.
        //
        // ServerIsNotTheLeaderException is the one exception (issue #4743): it means the apply tried to
        // WRITE the schema, which this node is not allowed to do while replaying someone else's entry.
        // That is deterministic - retrying re-runs the identical illegal write - so the old bounded
        // retry burned four attempts and then escalated a healthy database to a full snapshot resync
        // over a condition a resync cannot fix. Report it as a plain apply failure so the per-database
        // quarantine below decides, once, with the real error in the log.
      } catch (final ServerIsNotTheLeaderException e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Raft apply at index %d attempted a schema write, which is only legal on the leader. This is deterministic, "
                + "so it is NOT retried: %s",
            index, e.getMessage());
        handleUnexpectedApplyError(index, databaseName, e);
      } catch (final NeedRetryException e) {
        lastRetry = e;
        LogManager.instance().log(this, Level.WARNING,
            "Retryable error applying Raft log entry at index %d (attempt %d/%d): %s",
            index, attempt + 1, maxRetries + 1, e.getMessage());
        if (attempt < maxRetries && retryDelay > 0) {
          try {
            Thread.sleep(1 + ThreadLocalRandom.current().nextInt(retryDelay));
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            LogManager.instance().log(this, Level.WARNING,
                "Raft apply retry interrupted at index %d after %d attempt(s); aborting retry loop (likely shutdown)",
                index, attempt + 1);
            break;
          }
        }
      } catch (final ReplicationException re) {
        // Already a resync signal (e.g. the WAL-gap escalation from applyTxEntry); propagate it
        // unchanged so it reaches applyTransaction's catch (ReplicationException) handler without
        // being re-wrapped or counted against the bounded-escalation budget below.
        throw re;
      } catch (final RuntimeException t) {
        // Catch RuntimeException (not Throwable) on purpose: applyAction is a Runnable, so the only
        // things it can throw are RuntimeException or Error. JVM Errors (OutOfMemoryError,
        // StackOverflowError, ...) mean the JVM itself is unstable and must never be swallowed as a
        // recoverable resync condition - leaving them uncaught lets them propagate unchanged to
        // applyTransaction's fatal halt path so the node stops loudly rather than masking a corrupt
        // runtime.
        handleUnexpectedApplyError(index, databaseName, t);
      }
    }

    // The retryable condition persisted across all attempts. Escalate to a resync (recoverable) -
    // never fall through to the fatal catch (Throwable) branch that stops the server.
    throw new ReplicationException(
        "Retryable error persisted at index " + index + " after " + (maxRetries + 1)
            + " attempts; escalating to snapshot resync", lastRetry);
  }

  /**
   * Terminal handling for an apply error that no retry can fix. Always throws.
   * <p>
   * Per-database quarantine (issue #4797): a single {@link ArcadeStateMachine} multiplexes every
   * database on the node, so tripping the node-wide critical halt for one entry would freeze the apply
   * pipeline for ALL co-located databases. When the failing entry targets a single database
   * ({@code databaseName} non-null and non-empty) the failure is isolable: quarantine that database
   * (mark it diverged and trigger a targeted snapshot resync) and report the error as a recoverable
   * {@link ReplicationException} instead of the fatal {@code catch (Throwable)} path that would halt
   * the server. The node stays up, healthy databases keep replicating, and only the affected database
   * is reinstalled from the leader. This subsumes the earlier issue #4740 behaviour (an unexpected
   * error on an already-diverged database is a resync condition): the only change is that the FIRST
   * unexpected error on a healthy database now quarantines it rather than halting the node.
   * <p>
   * Entries with no single target database ({@code databaseName} null or empty, e.g. a
   * {@code SECURITY_USERS_ENTRY}) are NOT isolable to one database's state, so their failure still
   * propagates to the node-wide fatal halt.
   */
  private void handleUnexpectedApplyError(final long index, final String databaseName, final RuntimeException t) {
    if (databaseName != null && !databaseName.isEmpty()) {
      // Mark the database diverged on the first error so subsequent errors for it route here too.
      // add() returns true only the first time, which is when we kick off the targeted resync.
      if (divergedDatabases.add(databaseName)) {
        LogManager.instance().log(this, Level.SEVERE,
            "Unexpected error applying Raft entry for database '%s' at index %d; quarantining the database and "
                + "triggering a targeted snapshot resync instead of halting the node (issue #4797): %s",
            databaseName, index, t.getMessage());
        triggerDatabaseResync(databaseName);
      } else {
        LogManager.instance().log(this, Level.SEVERE,
            "Unexpected error at index %d while database '%s' is quarantined (snapshot resync in progress); "
                + "treating as resync condition: %s",
            index, databaseName, t.getMessage());
      }
      // Bounded escalation: a node that can never resync (no stable leader) must not swallow
      // errors forever and degrade silently. Once the swallow count exceeds the threshold, let
      // the error propagate to the fatal halt path so a truly stuck node surfaces loudly.
      if (divergedSwallowedErrors.incrementAndGet() > MAX_DIVERGED_SWALLOWED_ERRORS) {
        LogManager.instance().log(this, Level.SEVERE,
            "Quarantined database '%s' swallowed over %d unexpected errors without resyncing (index %d); escalating to fatal halt: %s",
            databaseName, MAX_DIVERGED_SWALLOWED_ERRORS, index, t.getMessage());
        throw t;
      }
      throw new ReplicationException(
          "Apply error on database '" + databaseName + "' at index " + index + "; per-database snapshot resync in progress", t);
    }
    throw t;
  }

  /**
   * Records a snapshot checkpoint so Ratis can compact the log up to the last-applied index.
   * <p>
   * The ArcadeDB database files on disk are inherently the snapshot state - every committed
   * transaction is already durably flushed by the {@link com.arcadedb.engine.TransactionManager}.
   * Returning the last-applied index here tells Ratis it may purge log entries up to that index,
   * reducing log disk usage over time.
   * <p>
   * <b>Why a marker file is written (issue #4829):</b> the returned index is the Ratis contract
   * "state up to here is durable, you may purge the log up to it". Returning it without also writing
   * a {@code snapshot.<term>_<index>} file would leave {@link SimpleStateMachineStorage#getLatestSnapshot()}
   * (which discovers snapshots by scanning for those files) returning {@code null} forever. With
   * auto-snapshot + {@code purgeUptoSnapshotIndex} enabled, Ratis would purge log entries up to the
   * returned index even though no snapshot exists; after a restart {@link #reinitialize()} would seed
   * {@code lastAppliedIndex = -1} and Ratis would try to replay from the start of a log whose early
   * entries were already purged - permanently orphaning applied state. We therefore persist a real
   * (empty) marker BEFORE returning the purge index, the same marker {@link #notifyInstallSnapshotFromLeader}
   * writes on the follower install path. If the marker cannot be written we report
   * {@link RaftLog#INVALID_LOG_INDEX} so Ratis does not purge a log with no backing snapshot.
   */
  @Override
  public long takeSnapshot() {
    long currentIndex = lastAppliedIndex.get();
    if (currentIndex < 0)
      return RaftLog.INVALID_LOG_INDEX;

    // Never checkpoint past an entry whose leader-side phase 2 has not confirmed (issue #5407): the
    // entry is Raft-committed and lastAppliedIndex has moved past it, but its pages reach disk only
    // when commit2ndPhase runs. Clamping to the oldest in-flight commit's floor keeps such an entry
    // above the checkpoint, so a restart replays it (with originatedLocally=false) instead of
    // treating it as durable and dropping it permanently.
    final long pendingFloor = lowestPendingLocalPhase2Floor();
    if (pendingFloor < currentIndex) {
      currentIndex = pendingFloor;
      final long oldestStartedAtMs = oldestPendingLocalPhase2StartMs();
      if (oldestStartedAtMs != Long.MAX_VALUE)
        warnIfPhase2StallingCompaction(oldestStartedAtMs, currentIndex);
    }
    if (currentIndex < 0)
      return RaftLog.INVALID_LOG_INDEX;

    // Regressing the marker below an existing one would let Ratis replay from an index whose log
    // entries a previous checkpoint already authorised for purging. Skip this round instead; the
    // next snapshot after phase 2 drains advances it normally.
    final var latest = storage.getLatestSnapshot();
    if (latest != null && currentIndex < latest.getIndex()) {
      HALog.log(this, HALog.BASIC,
          "Skipping snapshot checkpoint at index %d: a phase-2 apply is still in flight and the existing marker is at %d",
          currentIndex, latest.getIndex());
      return RaftLog.INVALID_LOG_INDEX;
    }

    // NOTE: after a clamp, term is the CURRENT applied term while currentIndex is an older index, so
    // the marker name can pair a term with an index that predates it. That is deliberate and already
    // tolerated: reinitialize() seeds an inflated marker term as-is and the tolerant
    // updateLastAppliedTermIndex override realigns it on the first replayed entry (issues #575/#593),
    // and notifyInstallSnapshotFromLeader writes an upper-bound term for the same reason. Do not
    // "correct" it by looking up the term at currentIndex - that entry may already be purged.
    final TermIndex applied = getLastAppliedTermIndex();
    final long term = applied != null && applied.getTerm() > 0 ? applied.getTerm() : 0L;
    if (!registerSnapshotMarker(term, currentIndex)) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not persist snapshot marker at index %d; not authorising log purge", currentIndex);
      return RaftLog.INVALID_LOG_INDEX;
    }
    HALog.log(this, HALog.BASIC, "ArcadeStateMachine: snapshot checkpoint at index %d (term %d)", currentIndex, term);
    return currentIndex;
  }

  /**
   * Writes an empty Ratis snapshot marker file at {@code (term, index)} and registers it as the
   * latest snapshot in {@link #storage}, so {@link SimpleStateMachineStorage#getLatestSnapshot()}
   * can rediscover it after a restart (it scans for {@code snapshot.<term>_<index>} files).
   * <p>
   * ArcadeDB's real snapshot is the set of database files on disk - every committed transaction is
   * already durably flushed by the {@link com.arcadedb.engine.TransactionManager} - so the marker is
   * a zero-byte placeholder whose name carries the {@code (term, index)} that Ratis's snapshot-index
   * bookkeeping and log-purge contract point at. No {@code .md5} companion is written, so the
   * rediscovered {@link SingleFileSnapshotInfo} carries a null digest, the same as a fresh boot;
   * ArcadeDB never exercises Ratis's chunk-verification path (it resyncs over HTTP via
   * {@link DatabaseReconciler}), so the empty file is safe across restarts.
   * <p>
   * Used by both {@link #takeSnapshot()} (leader-side periodic compaction checkpoint) and
   * {@link #notifyInstallSnapshotFromLeader} (follower-side install). Only the most recent marker is
   * retained; older zero-byte markers are pruned best-effort.
   *
   * @return {@code true} if the marker was written and registered, {@code false} on I/O failure
   */
  private boolean registerSnapshotMarker(final long term, final long index) {
    try {
      final File snapshotFile = storage.getSnapshotFile(term, index);
      final File parentDir = snapshotFile.getParentFile();
      if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
        LogManager.instance().log(this, Level.WARNING,
            "Could not create snapshot storage directory %s; snapshot registration failed", parentDir);
        return false;
      }
      if (!snapshotFile.exists())
        snapshotFile.createNewFile();
      storage.updateLatestSnapshot(new SingleFileSnapshotInfo(
          new FileInfo(snapshotFile.toPath(), null), term, index));
      // Keep only the latest marker; older zero-byte markers are obsolete once a newer one exists.
      // SnapshotRetentionPolicy declares getNumSnapshotsRetained() as a default method (not abstract),
      // so it is not a functional interface and cannot be supplied as a lambda.
      try {
        storage.cleanupOldSnapshots(new SnapshotRetentionPolicy() {
          @Override
          public int getNumSnapshotsRetained() {
            return 1;
          }
        });
      } catch (final IOException cleanupEx) {
        LogManager.instance().log(this, Level.FINE,
            "Could not clean up old snapshot markers: %s", cleanupEx.getMessage());
      }
      return true;
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Failed to write Raft snapshot marker at (term=%d, index=%d): %s", term, index, e.getMessage());
      return false;
    }
  }

  /**
   * Called by Ratis when the leader changes for this group. Logs the new leader and
   * this node's role using human-readable display names. Also starts or stops the
   * replica lag monitor depending on whether this node is the new leader.
   */
  @Override
  public void notifyLeaderChanged(final RaftGroupMemberId groupMemberId, final RaftPeerId newLeaderId) {
    super.notifyLeaderChanged(groupMemberId, newLeaderId);

    final long previousElectionTime = lastElectionTime;
    final long now = System.currentTimeMillis();
    electionCount.incrementAndGet();
    lastElectionTime = now;

    if (raftHAServer == null || newLeaderId == null)
      return;

    final RaftPeerId prevId = previousLeaderId;
    previousLeaderId = newLeaderId;

    final String leaderName = raftHAServer.getPeerDisplayName(newLeaderId);
    // Use the actual Raft term (not the lagging last-applied term) so we can tell a genuine
    // re-election (term advanced) from a same-term re-notification that Ratis sometimes fires.
    final long currentTerm = raftHAServer.getCurrentTerm();
    final long prevTerm = lastNotifiedLeaderTerm;
    lastNotifiedLeaderTerm = currentTerm;

    if (prevId == null) {
      // First leader observed since startup - no churn signal yet.
      LogManager.instance().log(this, Level.INFO, "Leader elected: %s (term=%d)", leaderName, currentTerm);
    } else if (prevId.equals(newLeaderId)) {
      // The same node is leader again. Only a term advance means an actual step-down/re-election
      // cycle; a same-term re-notification (currentTerm == prevTerm) is a Ratis bookkeeping callback,
      // not churn, so do not alarm the operator about it.
      if (currentTerm <= prevTerm && currentTerm >= 0) {
        LogManager.instance().log(this, Level.FINE,
            "Leader re-notified: %s (term=%d, no term change)", leaderName, currentTerm);
      } else {
        // A real re-election kept the same leader: the previous leader stopped being leader, another
        // node started an election with a higher term, and the original leader won the next round (it
        // has the most up-to-date log). Two distinct causes look identical from here, so name both
        // (issue #4743): a heartbeat stall (CPU/GC pause, disk stall, network blip, appender threads
        // busy under bulk-load replication), or a state-machine step-down, which is what Ratis does
        // when a single log entry is rejected - notably an entry above arcadedb.ha.appendBufferSize.
        // The second cause shows no resource pressure at all and repeats on a fixed cadence as the
        // same oversized entry is retried, so blaming CPU/GC alone sends operators tuning the wrong
        // knob. Confirm with the arcadedb.ha.follower.* heartbeat-lag metrics before tuning.
        final long sinceLast = previousElectionTime > 0 ? now - previousElectionTime : -1;
        LogManager.instance().log(this, Level.WARNING,
            """
            Leader churn: %s re-elected (term=%d, %d ms since last leader change). \
            Either a heartbeat stall triggered an election (CPU/GC pauses, disk stalls, network blips, or \
            appender threads saturated by bulk-load replication - check the arcadedb.ha.follower.* metrics, \
            then raise arcadedb.ha.electionTimeoutMin/Max, reduce batch size, or add CPU/IO headroom), or \
            Ratis made the leader step down because it rejected a log entry - look for a preceding \
            'exceeds the max buffer limit' / 'too large' error and raise arcadedb.ha.appendBufferSize or \
            shrink the transaction.""",
            leaderName, currentTerm, sinceLast);
      }
    } else {
      // Different node became leader. Normal failover (network, server restart, etc.).
      final String prevName = raftHAServer.getPeerDisplayName(prevId);
      LogManager.instance().log(this, Level.INFO, "Leader changed: %s -> %s (term=%d)",
          prevName, leaderName, currentTerm);
    }

    // Recreate the RaftClient so its gRPC channels perform fresh DNS resolution.
    // After a network partition, channels to isolated peers enter TRANSIENT_FAILURE
    // with exponential back-off (up to ~120 s). Refreshing on every leader change
    // ensures the client can reach all peers as soon as the partition heals.
    // Pass the newly elected leader's peer ID so the fresh client routes its very first
    // write directly to the leader rather than probing peers.
    raftHAServer.refreshRaftClient(newLeaderId);

    if (newLeaderId.equals(raftHAServer.getLocalPeerId())) {
      LogManager.instance().log(this, Level.INFO, "This node is now LEADER");
      raftHAServer.startLagMonitor();
      raftHAServer.printClusterConfiguration();

      // Clear the follower-side reconcile states (LEADER_MISSING / FAILED) and failure counters now that this node
      // is the leader, so their cluster alerts do not linger (issue #4727). ACQUIRED is harmless history and kept.
      reconciler.clearFollowerReconcileStatesOnBecomeLeader();

      // Issue #4147: drive offline cluster bootstrap if conditions match (commit index still 0,
      // arcadedb.ha.bootstrapFromLocalDatabase=true). Runs on a background thread so a slow peer or a
      // bootstrap-state RPC timeout does not stall Raft's normal leader-change processing on this node.
      // Note: the background pass itself may park this single-threaded lifecycleExecutor briefly - it
      // waits for the freshly-elected leader's Raft division to expose a readable commit index (~100 ms
      // typically, up to commitIndexReadinessTimeoutMs on a broken read) - so tasks submitted afterward
      // (e.g. the snapshot download below) queue behind it in that rare worst case.
      lifecycleExecutor.submit(() -> {
        try {
          raftHAServer.runBootstrapIfEligible();
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.WARNING,
              "Bootstrap election threw on leader-change handler: %s", null, t.getMessage());
        }
      });
    } else {
      LogManager.instance().log(this, Level.INFO, "This node is now REPLICA (leader: %s)", leaderName);
      raftHAServer.stopLagMonitor();
    }

    // If a snapshot gap was detected during reinitialize(), trigger the download now
    // that we know who the leader is (primary path; the 30s watchdog is the fallback).
    if (needsSnapshotDownload.compareAndSet(true, false)) {
      LogManager.instance().log(this, Level.INFO,
          "Leader change detected, triggering pending snapshot download from leader %s", leaderName);
      lifecycleExecutor.submit(this::triggerSnapshotDownload);
    }

    // Wake up any threads waiting for leadership change (e.g. leaveCluster)
    final Object notifier = raftHAServer.getLeaderChangeNotifier();
    synchronized (notifier) {
      notifier.notifyAll();
    }
  }

  /**
   * Called by Ratis when the follower's log is too far behind the leader's compacted log.
   * Individual log entries are no longer available, so a full database snapshot must be
   * downloaded from the leader. Delegates to {@link SnapshotInstaller#install} for crash-safe
   * installation with marker files and atomic directory swap.
   * <p>
   * Runs asynchronously via {@link CompletableFuture#supplyAsync} to avoid blocking the
   * Ratis state machine thread.
   */
  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      final RaftProtos.RoleInfoProto roleInfoProto, final TermIndex firstTermIndexInLog) {

    LogManager.instance().log(this, Level.INFO,
        "HA resync started (mode=snapshot, reason=leader snapshot install): firstLogIndex=%s", firstTermIndexInLog);

    // Runs on the JDK common ForkJoinPool via supplyAsync(). Apache-ratis uses a dedicated pool
    // to avoid blocking Ratis internal threads, so this offload IS necessary - we must not run
    // the snapshot download synchronously on the caller. The remaining concern is the common
    // pool itself: it is shared with user-supplied scripts (Gremlin, Polyglot), and a long
    // snapshot download could starve user code under exceptional conditions. Snapshot installs
    // are rare (only on follower (re)joining) and serialised inside Ratis, so this is operationally
    // tolerable today. See QueryEngineManager class javadoc - "No JDK common ForkJoinPool" rule -
    // for the migration target; the cleanup item is to fork onto a dedicated executor (sized via
    // a future {@code arcadedb.haSnapshotInstallThreads} knob) once we add one.
    return CompletableFuture.supplyAsync(() -> {
      // Participate in the same single-flight protocol as triggerSnapshotDownload() so that
      // isSnapshotDownloadPending() returns true during this install and the HealthMonitor's
      // recoverFromPersistentLag() does not initiate a new concurrent triggerSnapshotDownload().
      // We use CAS (not unconditional set) to avoid clearing a flag owned by a concurrently
      // running triggerSnapshotDownload():
      //  - if we win (flag false->true): we own the flag and MUST clear it in finally.
      //  - if we lose (flag already true, another download in progress): we skip the flag and let
      //    the other download complete; we still proceed with reconcileDatabasesFromLeader() because
      //    the two installs both pull from the same leader and SnapshotInstaller is crash-safe with
      //    atomic directory swaps. NOTE: the two calls are not serialized by this flag; ordering
      //    is only guaranteed when we win the CAS. Eliminating the residual race requires a mutex
      //    or waiting on the in-flight download, which is deferred as a future improvement.
      final boolean acquiredSnapshotFlag = snapshotDownloadInProgress.compareAndSet(false, true);
      try {
        final RaftPeerId leaderId = RaftPeerId.valueOf(
            roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());
        final String leaderHttpAddr = raftHAServer.getPeerHttpAddress(leaderId);
        final String leaderHttpsAddr = raftHAServer.getPeerHttpsAddress(leaderId);

        if (leaderHttpAddr == null)
          throw new RuntimeException("Cannot determine leader HTTP address for snapshot download");

        final String clusterToken = raftHAServer.getClusterToken();

        reconciler.reconcileDatabasesFromLeader(leaderHttpAddr, leaderHttpsAddr, clusterToken);

        // Compute the installed snapshot TermIndex. firstTermIndexInLog is the first log entry
        // AFTER the snapshot, so the snapshot covers all entries up to getIndex()-1.
        // Returning firstTermIndexInLog itself (as the old code did) caused two bugs:
        // 1. SnapshotInstallationHandler called state.reloadStateMachine(firstTermIndexInLog) which
        //    purged log entries up to firstTermIndexInLog.getIndex() instead of getIndex()-1.
        // 2. StateMachineUpdater.reload() calls getLatestSnapshot().getIndex() and expects it to match
        //    the TermIndex we return; returning firstTermIndexInLog while storage was never updated
        //    caused NullPointerException (and before that, IllegalStateException from the PAUSED check).
        final long snapshotIndex = Math.max(0L, firstTermIndexInLog.getIndex() - 1);
        // Use firstTermIndexInLog.getTerm() as the snapshot term. The true last-entry term inside
        // the snapshot is opaque to us (ArcadeDB ships database files, not Ratis snapshot chunks),
        // so we use the term of the first available log entry as a safe upper bound. This value is
        // only used to name the marker file (snapshot.term_index) and as metadata for Ratis's
        // snapshotIndex tracking; it does not affect data correctness.
        final long snapshotTerm = firstTermIndexInLog.getTerm();
        final TermIndex installedTermIndex = TermIndex.valueOf(snapshotTerm, snapshotIndex);

        // Register the snapshot in SimpleStateMachineStorage. StateMachineUpdater.reload() calls
        // getLatestSnapshot() immediately after reinitialize() and requires a non-null result.
        // registerSnapshotMarker() writes the empty marker file and updates the latest-snapshot
        // reference; see its javadoc for why a file-less, null-digest marker is safe for ArcadeDB.
        if (!registerSnapshotMarker(snapshotTerm, snapshotIndex))
          throw new IOException("Failed to register snapshot marker at index " + snapshotIndex);

        // Advance the local applied-index to the snapshot point so that the StateMachineUpdater
        // knows which log entries have been consumed by this install. A full state-machine install
        // brings EVERY present database to the snapshot point, so record the snapshot index for each
        // of them too (not just the global position) - this keeps the per-database bootstrap
        // replay-skip honest after a full resync (issue #4824).
        lastAppliedIndex.set(snapshotIndex);
        updateLastAppliedTermIndex(snapshotTerm, snapshotIndex);
        writePersistedAppliedIndexForAllDatabases(snapshotIndex);

        LogManager.instance().log(this, Level.INFO,
            "HA resync finished (mode=snapshot, result=ok): snapshotIndex=%d", snapshotIndex);
        clearDivergedState();
        return installedTermIndex;

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error during snapshot installation from leader", e);
        throw new RuntimeException("Error during Raft snapshot installation", e);
      } finally {
        if (acquiredSnapshotFlag)
          snapshotDownloadInProgress.set(false);
      }
    });
  }

  public long getElectionCount() {
    return electionCount.get();
  }

  public long getLastElectionTime() {
    return lastElectionTime;
  }

  public long getStartTime() {
    return startTime;
  }

  /**
   * Returns the snapshot watchdog timeout in milliseconds. The value is the configured
   * {@link GlobalConfiguration#HA_SNAPSHOT_WATCHDOG_TIMEOUT}, floored at
   * {@link #WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER} times {@link GlobalConfiguration#HA_ELECTION_TIMEOUT_MAX}
   * to avoid premature triggering on high-latency WAN clusters.
   */
  long computeSnapshotWatchdogTimeoutMs() {
    final long configured = server != null
        ? server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT)
        : GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.getValueAsLong();
    final long electionTimeoutMax = server != null
        ? server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX)
        : GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getValueAsInteger();
    final long floor = electionTimeoutMax * WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER;
    return Math.max(configured, floor);
  }

  /**
   * Applies a committed WAL transaction to the local database.
   * <p>
   * <b>Origin-skip optimization:</b> On the leader, the transaction was already applied locally
   * via {@link RaftReplicatedDatabase#commit}'s Phase 2 ({@code commit2ndPhase}), so the state
   * machine skips it. On replicas, this is the primary path for applying transaction data.
   * <p>
   * <b>Ordering guarantee:</b> WAL capture (Phase 1) happens before Raft replication. Local
   * apply (Phase 2) happens after Raft commit. The leader skips the state machine apply because
   * Phase 2 already wrote the pages when replication succeeded.
   * <p>
   * <b>{@code ignoreErrors=true} rationale:</b> During Raft log replay on restart, log entries
   * may already be applied to the database files (Ratis last-applied tracking can lag behind
   * durable page writes). Page-version guards in {@code applyChanges} detect and skip
   * already-applied pages; version-gap warnings are still logged.
   */
  /**
   * Marks a locally-originated transaction as abandoned by the leader's phase 2 because replication
   * returned an indeterminate result ({@link ReplicationDispatchedTimeoutException}). If the entry
   * later commits, {@link #applyTxEntry} applies it here instead of origin-skipping it (issue #4790).
   * Called from {@link RaftReplicatedDatabase#commit()} on the dispatched-timeout path.
   * <p>
   * {@code phase2Ticket} is the still-held ticket of the commit that abandoned this transaction, or
   * {@link #NO_PHASE2_TICKET} when it took none. Recording it here is the correlation the ticket
   * lacks at {@link #beginLocalPhase2()} time (the WAL txId does not exist yet), and it is what lets
   * the eventual apply release the ticket instead of pinning the checkpoint until restart (#5410).
   */
  void markLocalTransactionAbandoned(final String databaseName, final long walTxId, final long phase2Ticket) {
    final long now = System.currentTimeMillis();
    // Prune stale marks (entries that were dispatched but never committed, e.g. the slot was
    // overwritten by a new leader) so the map cannot grow unbounded. A pruned mark deliberately does
    // NOT release its ticket: pruning is not proof the entry never committed, and if it does commit
    // later it will now be origin-skipped (unapplied here), so it must stay inside the replay window.
    if (!abandonedLocalTransactions.isEmpty())
      abandonedLocalTransactions.values().removeIf(abandoned -> now - abandoned.insertedAt() > ABANDONED_TX_TTL_MS);
    abandonedLocalTransactions.put(abandonedKey(databaseName, walTxId), new AbandonedPhase2(phase2Ticket, now));
    HALog.log(this, HALog.BASIC,
        "Marked locally-originated tx %d on database '%s' for local apply on commit (replication was indeterminate, #4790)",
        walTxId, databaseName);
  }

  /**
   * Consumes the abandoned mark for a locally-originated transaction, returning the phase-2 ticket
   * to release once its pages are written (possibly {@link #NO_PHASE2_TICKET} when its commit held
   * none), or {@link #NO_ABANDONED_MARK} when the transaction was not abandoned at all - the
   * origin-skip case, where phase 2 already wrote the pages and released its own ticket.
   * <p>
   * Consuming is one-shot so a later replay of the same entry correctly origin-skips again.
   */
  long consumeAbandonedLocalTransaction(final String databaseName, final long walTxId) {
    final AbandonedPhase2 abandoned = abandonedLocalTransactions.remove(abandonedKey(databaseName, walTxId));
    return abandoned != null ? abandoned.phase2Ticket() : NO_ABANDONED_MARK;
  }

  private static String abandonedKey(final String databaseName, final long walTxId) {
    return databaseName + "/" + walTxId;
  }

  /**
   * Registers a leader-side phase 2 that is about to start replicating, and returns the ticket that
   * {@link #endLocalPhase2(long)} must release. The recorded floor is the applied index observed
   * now, i.e. BEFORE this transaction's entry exists in the Raft log, so the entry is guaranteed to
   * land above it and stay inside the replay window that {@link #takeSnapshot()} preserves.
   * <p>
   * Reading a floor that is lower than the eventual entry index is always safe: it only widens the
   * replay window, and replay is idempotent (page-version guards in {@code applyChanges} skip pages
   * that are already at or beyond the WAL version).
   */
  long beginLocalPhase2() {
    final long ticket = pendingLocalPhase2Ticket.incrementAndGet();
    pendingLocalPhase2.put(ticket, new PendingPhase2(lastAppliedIndex.get(), System.currentTimeMillis()));
    return ticket;
  }

  /**
   * Releases a ticket returned by {@link #beginLocalPhase2()}. Call it only once the entry's local
   * pages are settled - or once the entry provably never committed. A ticket left held keeps the
   * snapshot checkpoint pinned until the node restarts, which is the intended outcome when this node
   * is holding a committed entry it never applied.
   */
  void endLocalPhase2(final long ticket) {
    if (ticket == NO_PHASE2_TICKET)
      return;
    pendingLocalPhase2.remove(ticket);
  }

  /** Number of leader-side phase 2 applies still holding the snapshot checkpoint back. */
  int pendingLocalPhase2Count() {
    return pendingLocalPhase2.size();
  }

  /**
   * How long (ms) the oldest in-flight phase 2 has been holding the snapshot checkpoint, or {@code 0}
   * when none is in flight. Exposed for the {@code arcadedb.ha.phase2.*} gauges so a node whose log
   * compaction is pinned is visible on a dashboard rather than only in the throttled WARNING (#5410).
   */
  long oldestPendingLocalPhase2HeldMs() {
    final long oldest = oldestPendingLocalPhase2StartMs();
    return oldest == Long.MAX_VALUE ? 0L : Math.max(0L, System.currentTimeMillis() - oldest);
  }

  /**
   * The Raft replay floor currently pinning the snapshot checkpoint, or {@code -1} when nothing is
   * in flight. Companion gauge to {@link #oldestPendingLocalPhase2HeldMs()}: it names the index past
   * which the Raft log cannot be purged.
   */
  long lowestPendingLocalPhase2ReplayFloor() {
    final long lowest = lowestPendingLocalPhase2Floor();
    return lowest == Long.MAX_VALUE ? -1L : lowest;
  }

  /**
   * The lowest replay floor among the currently in-flight leader-side phase 2 applies, or
   * {@link Long#MAX_VALUE} when none is in flight. Scanned on demand rather than maintained
   * incrementally: {@link #takeSnapshot()} is the only reader and runs rarely (periodic compaction
   * or shutdown), while the map is sized by concurrent commits.
   */
  private long lowestPendingLocalPhase2Floor() {
    long lowest = Long.MAX_VALUE;
    for (final PendingPhase2 pending : pendingLocalPhase2.values())
      if (pending.replayFloor() < lowest)
        lowest = pending.replayFloor();
    return lowest;
  }

  /**
   * When the oldest in-flight phase 2 started, or {@link Long#MAX_VALUE} when none is in flight.
   * Kept separate from {@link #lowestPendingLocalPhase2Floor()} so both stay pure queries; the extra
   * pass costs nothing on the rare {@link #takeSnapshot()} path.
   */
  private long oldestPendingLocalPhase2StartMs() {
    long oldest = Long.MAX_VALUE;
    for (final PendingPhase2 pending : pendingLocalPhase2.values())
      if (pending.startedAtMs() < oldest)
        oldest = pending.startedAtMs();
    return oldest;
  }

  /**
   * Surfaces the one failure mode of the #5407 guard: a ticket held long enough to be stuck (its
   * commit neither settled its pages nor proved the entry absent) pins the checkpoint, so the Raft
   * log stops being purged until this node restarts. Throttled so a checkpoint attempt during a
   * genuinely long-running commit does not spam the log.
   * <p>
   * Evaluated on a snapshot attempt, not on a timer: the condition is only interesting when a
   * checkpoint is actually being held back, but it does mean the warning can lag a stuck ticket by
   * up to one compaction interval ({@code arcadedb.ha.snapshotInterval}, 5 min by default).
   */
  private void warnIfPhase2StallingCompaction(final long oldestStartedAtMs, final long lowestFloor) {
    final long heldForMs = System.currentTimeMillis() - oldestStartedAtMs;
    if (heldForMs < STALLED_PHASE2_WARN_AFTER_MS)
      return;
    final long now = System.currentTimeMillis();
    final long lastWarn = lastStalledPhase2WarnMs.get();
    if (now - lastWarn < STALLED_PHASE2_WARN_EVERY_MS || !lastStalledPhase2WarnMs.compareAndSet(lastWarn, now))
      return;
    LogManager.instance().log(this, Level.WARNING,
        """
        A local phase-2 apply has been unconfirmed for %d s (%d in flight): holding the Raft snapshot \
        checkpoint at index %d so the entry stays replayable. The Raft log will not be purged past that \
        index until this node restarts and replays it - watch disk usage on the Raft storage volume.""",
        heldForMs / 1000, pendingLocalPhase2.size(), lowestFloor);
  }

  private void applyTxEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex,
      final boolean originatedLocally) {
    // Fast path (the leader's hot path): a locally-originated entry was already applied via
    // commit2ndPhase() in RaftReplicatedDatabase, so skip to avoid double-apply. Using
    // originatedLocally (set by startTransaction) instead of isLeader() avoids TOCTOU races when
    // leadership changes between entry submission and state machine apply. After a crash and
    // restart, originatedLocally is always false (startTransaction was not called in this lifecycle),
    // so replayed entries are correctly re-applied with page-version guards providing idempotency.
    // We short-circuit BEFORE deserializing the WAL when no abandoned transactions are pending (the
    // common case), so the skip costs nothing extra on the hot path.
    if (originatedLocally && abandonedLocalTransactions.isEmpty()) {
      HALog.log(this, HALog.TRACE, "Skipping tx apply on originator for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final WALFile.WALTransaction walTx = deserializeWalTransaction(decoded.walData());

    // EXCEPTION (issue #4790): commit() may have abandoned its phase 2 because replication returned
    // an indeterminate result (entry dispatched to Ratis but the quorum wait timed out before quorum
    // was confirmed). For such an entry phase 2 never ran, so it must be applied HERE instead of
    // origin-skipped, otherwise this leader silently loses a write the followers already have. The
    // mark is consumed (removed) so a later replay of the same entry correctly skips again.
    // The ticket that commit() is still holding for this entry, when it was abandoned. Released
    // below once applyChanges has written the pages - never on the origin-skip branch above, where
    // releasing would reintroduce #5407.
    long abandonedPhase2Ticket = NO_PHASE2_TICKET;
    if (originatedLocally) {
      final long abandoned = consumeAbandonedLocalTransaction(decoded.databaseName(), walTx.txId);
      if (abandoned == NO_ABANDONED_MARK) {
        HALog.log(this, HALog.TRACE, "Skipping tx apply on originator for database '%s'", decoded.databaseName());
        return;
      }
      abandonedPhase2Ticket = abandoned;
      HALog.log(this, HALog.BASIC,
          "Applying locally-originated tx %d on database '%s' whose phase 2 was abandoned (replication indeterminate, #4790)",
          walTx.txId, decoded.databaseName());
    }

    HALog.log(this, HALog.DETAILED, "Applying tx %d to database '%s' (pages=%d)",
        walTx.txId, decoded.databaseName(), walTx.pages.length);

    try {
      db.getTransactionManager().applyChanges(walTx, decoded.bucketRecordDelta(), false);
    } catch (final WALVersionGapException e) {
      // Version gap: WAL page version > DB page version + 1 - an intermediate transaction
      // was never applied on this node. State has diverged; trigger snapshot resync.
      final AtomicInteger gapCounter = TEST_WAL_GAP_COUNTER;
      if (gapCounter != null)
        gapCounter.incrementAndGet();
      // Mark this database as diverged so subsequent unexpected errors don't trigger fatal halt
      // (issue #4740). Set.add() returns true only when the database was not already in the set, so
      // the FIRST gap logs loudly and triggers an immediate snapshot download (instead of waiting for
      // the HealthMonitor's periodic check). Every subsequent committed entry for this database will
      // hit the same gap until the resync lands: those log a throttled one-liner (no per-entry stack
      // trace) so the log is not flooded and the download is not starved of CPU/IO on small nodes.
      if (divergedDatabases.add(decoded.databaseName())) {
        LogManager.instance().log(this, Level.SEVERE,
            "WAL version gap on follower - state divergence detected, triggering snapshot resync (db=%s, txId=%d): %s",
            decoded.databaseName(), walTx.txId, e.getMessage());
        try {
          lifecycleExecutor.submit(this::triggerSnapshotDownload);
        } catch (final RejectedExecutionException ree) {
          LogManager.instance().log(this, Level.WARNING,
              "Cannot schedule immediate snapshot download after WAL gap (db=%s): executor is shut down",
              ree, decoded.databaseName());
        }
      } else if (shouldLogDivergedResync(decoded.databaseName())) {
        LogManager.instance().log(this, Level.INFO,
            "WAL version gap on database '%s' (snapshot resync in progress); skipping apply at index %d until resync completes",
            decoded.databaseName(), entryIndex);
      }
      throw new ReplicationException(
          "WAL version gap detected - snapshot resync required (db=" + decoded.databaseName() + ")", e);
    }

    // The abandoned entry's pages are now on disk, so the commit that gave up on it in phase 2 no
    // longer needs to hold the Raft replay window open: release its ticket and let log compaction
    // resume (#5410). Reached only when applyChanges returned normally - a failed apply leaves the
    // entry unapplied here and the ticket deliberately held. A no-op for every other entry.
    // lastAppliedIndex still trails this entry at this point (applyTransaction advances it after we
    // return), so a concurrent takeSnapshot cannot yet checkpoint over what we just applied.
    //
    // Residual edge: a WAL version gap above consumed the mark but threw, so the ticket stays held
    // and the entry origin-skips on every later replay - the snapshot resync the gap triggers is what
    // makes it durable, and nothing releases the ticket afterwards. The checkpoint then stays pinned
    // until this node restarts. Retaining is the safe direction (the resync may itself fail), and the
    // pinned checkpoint is surfaced by warnIfPhase2StallingCompaction and the arcadedb.ha.phase2.*
    // gauges rather than being silent.
    endLocalPhase2(abandonedPhase2Ticket);
  }

  /**
   * Applies a committed DDL (schema change) entry to the local database.
   * <p>
   * <b>Three-phase application order</b> (order matters for correctness):
   * <ol>
   *   <li><b>Create/remove physical files.</b> WAL pages reference file IDs that must already
   *       exist on the replica. File-existence guards make this idempotent on replay.</li>
   *   <li><b>Apply buffered WAL entries.</b> Index page writes that occurred during DDL on the
   *       leader are embedded in the schema entry. These target the files created in step 1.
   *       Page-version guards make this idempotent on replay.</li>
   *   <li><b>Update schema JSON and reload.</b> Writes the schema configuration and reloads
   *       types, buckets, and file IDs into memory. Naturally idempotent (overwrites with
   *       same content on replay).</li>
   * </ol>
   * <p>
   * Like {@link #applyTxEntry}, the originator skips this because schema changes were already
   * applied locally during the transaction.
   */
  private void applySchemaEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex,
      final boolean originatedLocally) {
    // Same origin-tracking as applyTxEntry: skip if this node originated the entry in the
    // current lifecycle (schema changes were already applied locally during the transaction).
    if (originatedLocally) {
      HALog.log(this, HALog.TRACE, "Skipping schema apply on originator for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());

    HALog.log(this, HALog.DETAILED,
        "Applying schema entry to database '%s' (entryIndex=%d): filesToAdd=%d, filesToRemove=%d, hasSchemaJson=%s",
        decoded.databaseName(), entryIndex,
        decoded.filesToAdd() != null ? decoded.filesToAdd().size() : 0,
        decoded.filesToRemove() != null ? decoded.filesToRemove().size() : 0,
        decoded.schemaJson() != null && !decoded.schemaJson().isEmpty());

    if (HALog.isEnabled(HALog.DETAILED)) {
      HALog.log(this, HALog.DETAILED, "Received SCHEMA_ENTRY filesToAdd=%s", decoded.filesToAdd());
      HALog.log(this, HALog.DETAILED, "Received SCHEMA_ENTRY filesToRemove=%s", decoded.filesToRemove());
      logFollowerSchemaPayloadDiagnostics(decoded.databaseName(), decoded.schemaJson(),
          decoded.filesToAdd());
    }

    // A TimeSeries compaction/maintenance entry carries only sealed-store blobs (+ the mutable-bucket
    // clear WAL) and never changes the schema or creates/removes paginated files. For such entries we
    // MUST NOT re-update + reload the schema: load() re-instantiates every TimeSeries engine (closing
    // shard executors with a 30s awaitTermination) on the Raft apply thread, stalling replication.
    // installSealedFileBytes already reopened the sealed store and the clear WAL applies to the live
    // mutable-bucket pages, so neither the schema update nor the reload is needed.
    final boolean sealedOnlyEntry = isEmptyMap(decoded.filesToAdd()) && isEmptyMap(decoded.filesToRemove())
        && decoded.sealedFileBlobs() != null && !decoded.sealedFileBlobs().isEmpty();

    // A non-final chunk of a schema change split across several entries (see
    // RaftTransactionBroker.splitSchemaEntry) only DELIVERS pages: the change is published by the last
    // chunk. Reloading the schema on such a chunk re-instantiates every component from a state that is
    // still half-delivered - and that is not merely wasted work on the single Raft apply thread, it is
    // STICKY: a compacted sub-index that cannot be resolved yet gets detached, and the later publication
    // reuses the same in-memory component, so the follower keeps serving only its mutable pages for good
    // (#5443: ~1897 of 60000 entries).
    //
    // The producer marks these chunks explicitly. Inferring them from "no schema JSON" was tried and is
    // WRONG: the first chunk carries filesToAdd and no schema JSON, which is indistinguishable from a
    // standalone DDL that adds files without changing the schema version - and skipping the reload for
    // that would leave the new files unregistered in the schema.
    final boolean deliveryOnlyEntry = decoded.moreChunksFollow();

    // A commit that ran inside a recordFileChanges() callback but created no file and left the schema
    // version untouched ships as a SCHEMA_ENTRY carrying nothing but WAL, because the buffering in
    // RaftReplicatedDatabase.commit() is what preserves ordering against the enclosing DDL. Such an
    // entry has nothing for load() to pick up - applyChanges below already updates page counts through
    // getFileByIdIfExists() - so the reload is pure cost on the single Raft apply thread, where it
    // re-instantiates every TimeSeries engine and closes shard executors with a 30s awaitTermination.
    // Same reasoning as sealedOnlyEntry above.
    final boolean walOnlyEntry = isEmptyMap(decoded.filesToAdd()) && isEmptyMap(decoded.filesToRemove())
        && (decoded.schemaJson() == null || decoded.schemaJson().isEmpty())
        && (decoded.sealedFileBlobs() == null || decoded.sealedFileBlobs().isEmpty())
        && decoded.walEntries() != null && !decoded.walEntries().isEmpty();

    try {
      if (decoded.filesToAdd() != null)
        createNewFiles(db, decoded.filesToAdd());

      // Install any TimeSeries sealed-store blobs BEFORE applying the WAL (issue #4382). The WAL
      // below carries the mutable-bucket clear; installing the sealed file first guarantees a query
      // never observes "cleared mutable + stale sealed" (the data-loss window).
      applySealedBlobs(db, decoded.sealedFileBlobs());

      if (!sealedOnlyEntry && decoded.schemaJson() != null && !decoded.schemaJson().isEmpty())
        db.getSchema().getEmbedded().update(new JSONObject(decoded.schemaJson()));

      // Apply WAL entries BEFORE the schema reload. New files created above are initially empty;
      // reloading before writing pages would see empty files and silently ignore them, leaving
      // compaction indexes unregistered in the schema after this method returns. Writing the
      // page content first ensures load() finds valid data and registers the files properly.
      // applyChanges() uses getFileByIdIfExists() so it safely skips the page-count update for
      // files not yet registered in the schema (they will be registered by the load() below).
      final List<byte[]> walEntries = decoded.walEntries();
      if (walEntries != null && !walEntries.isEmpty()) {
        final List<Map<Integer, Integer>> bucketDeltas = decoded.bucketDeltas();
        for (int i = 0; i < walEntries.size(); i++) {
          final byte[] walData = walEntries.get(i);
          final Map<Integer, Integer> bucketDelta = bucketDeltas != null && i < bucketDeltas.size()
              ? bucketDeltas.get(i)
              : Collections.emptyMap();
          final WALFile.WALTransaction walTx = deserializeWalTransaction(walData);
          // ignoreErrors=true: same rationale as applyTxEntry - replay safety during node restart
          db.getTransactionManager().applyChanges(walTx, bucketDelta, true);
        }
        HALog.log(this, HALog.DETAILED,
            "Applied %d buffered WAL entries from schema entry to database '%s'",
            walEntries.size(), decoded.databaseName());
      }

      // Retire the superseded files only AFTER the WAL (issue #4743). This used to run first, before the
      // schema update - and the schema update re-instantiates the affected components, so an LSM index
      // whose page 0 still named the file just deleted (the WAL that repoints it at the new compacted
      // file is applied above, i.e. later) resolved a file id that no longer existed. The follower logged
      // "Invalid sub-index for index '...' (error=File with id 'NNN' was not found)" over a state that was
      // purely transient, and the old self-repair in LSMTreeIndexMutable.onAfterLoad then tried to DROP
      // the index - a schema write a replica may not perform - which failed the apply and escalated the
      // whole database to a snapshot resync. Retiring last closes that window: by then page 0 already
      // names the new file.
      if (decoded.filesToRemove() != null)
        for (final Map.Entry<Integer, String> fileEntry : decoded.filesToRemove().entrySet()) {
          db.getPageManager().deleteFile(db, fileEntry.getKey());
          db.getFileManager().dropFile(fileEntry.getKey());
          db.getSchema().getEmbedded().removeFile(fileEntry.getKey());
        }

      // Reload schema after WAL pages are on disk so new index files have valid content
      // and are correctly registered (page counts, type links, in-memory structures).
      // Skipped for sealed-only TimeSeries compaction entries (see sealedOnlyEntry above), for
      // delivery-only chunks of a split schema change (see deliveryOnlyEntry above) and for WAL-only
      // entries (see walOnlyEntry above).
      if (!sealedOnlyEntry && !deliveryOnlyEntry && !walOnlyEntry)
        db.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, true);

    } catch (final IOException e) {
      throw new RuntimeException("Failed to apply schema entry for database '" + decoded.databaseName() + "'", e);
    }

    HALog.log(this, HALog.DETAILED, "Applied schema change to database '%s'", decoded.databaseName());
  }

  /**
   * Symmetric counterpart of {@code RaftReplicatedDatabase.logSchemaPayloadDiagnostics} on the
   * follower side: enumerates the {@code indexes} keys present in the inbound schema JSON and
   * flags those whose backing file is not in {@code filesToAdd}. Such names will fail to load
   * when {@code LocalSchema.load()} runs and surface as "Cannot find indexes [...]" warnings
   * (issue #4083).
   */
  private void logFollowerSchemaPayloadDiagnostics(final String dbName, final String schemaJson,
      final Map<Integer, String> filesToAdd) {
    if (schemaJson == null || schemaJson.isEmpty())
      return;

    try {
      final JSONObject root = new JSONObject(schemaJson);
      if (!root.has("types"))
        return;
      final JSONObject types = root.getJSONObject("types");

      final Set<String> shippedIndexNames = new HashSet<>();
      if (filesToAdd != null) {
        for (final String fullName : filesToAdd.values()) {
          final int firstDot = fullName.indexOf('.');
          shippedIndexNames.add(firstDot > 0 ? fullName.substring(0, firstDot) : fullName);
        }
      }

      for (final String typeName : types.keySet()) {
        if (!(types.get(typeName) instanceof JSONObject type))
          continue;
        if (!type.has("indexes"))
          continue;
        final JSONObject indexes = type.getJSONObject("indexes");
        for (final String idxName : indexes.keySet()) {
          final boolean shipped = shippedIndexNames.contains(idxName);
          HALog.log(this, HALog.DETAILED,
              "[%s.applySchema] schemaJson.types.%s.indexes['%s'] %s",
              dbName, typeName, idxName,
              shipped ? "= matched in filesToAdd" : "= NOT in filesToAdd (will likely 'Cannot find indexes')");
        }
      }
    } catch (final RuntimeException e) {
      HALog.log(this, HALog.DETAILED,
          "[%s.applySchema] schema JSON parse failed for diagnostics: %s", dbName, e.getMessage());
    }
  }

  /**
   * Creates new database files for each entry in {@code filesToAdd} that does not already exist.
   * Skips files that are already registered in the file manager or already present on disk with
   * non-zero content (idempotent re-apply after a crash before the applied-index was persisted).
   */
  private void createNewFiles(final DatabaseInternal db, final Map<Integer, String> filesToAdd) throws IOException {
    final String databasePath = db.getDatabasePath();
    for (final Map.Entry<Integer, String> fileEntry : filesToAdd.entrySet()) {
      final int fileId = fileEntry.getKey();
      final String fileName = fileEntry.getValue();
      // Skip if already registered in memory (idempotent)
      if (db.getFileManager().existsFile(fileId))
        continue;
      // Skip if the file already exists on disk with data (crash-safe: the prior run created it)
      final File osFile = new File(databasePath + File.separator + fileName);
      if (osFile.exists() && osFile.length() > 0)
        continue;
      db.getFileManager().getOrCreateFile(fileId, databasePath + File.separator + fileName);
    }
  }

  /**
   * Installs TimeSeries sealed-store blobs shipped by the leader (issue #4382): for each blob the
   * full {@code .ts.sealed} file is replaced atomically and the in-memory sealed store reopened.
   * Idempotent: re-applying the same blob (crash/restart replay) simply rewrites the identical file.
   */
  private void applySealedBlobs(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedBlob> blobs)
      throws IOException {
    if (blobs == null || blobs.isEmpty())
      return;
    for (final RaftLogEntryCodec.TsSealedBlob blob : blobs) {
      final LocalSchema schema = db.getSchema().getEmbedded();
      if (!schema.existsType(blob.typeName())) {
        // Should not happen: the type-creation entry has a lower Raft index and is applied first.
        LogManager.instance().log(this, Level.SEVERE,
            "Received TimeSeries sealed blob for unknown type '%s' (db=%s); skipping", null, blob.typeName(),
            decodedDbName(db));
        continue;
      }
      if (!(schema.getType(blob.typeName()) instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null) {
        LogManager.instance().log(this, Level.SEVERE,
            "Received TimeSeries sealed blob for non-timeseries or uninitialized type '%s' (db=%s); skipping", null,
            blob.typeName(), decodedDbName(db));
        continue;
      }
      tsType.getEngine().getShard(blob.shardIndex()).getSealedStore().installSealedFileBytes(blob.bytes());
      HALog.log(this, HALog.DETAILED, "Installed TimeSeries sealed blob for %s shard %d (%d bytes) on db '%s'",
          blob.typeName(), blob.shardIndex(), blob.bytes().length, decodedDbName(db));
    }
  }

  private static String decodedDbName(final DatabaseInternal db) {
    return db != null ? db.getName() : "?";
  }

  private static boolean isEmptyMap(final Map<?, ?> map) {
    return map == null || map.isEmpty();
  }

  private void applyInstallDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    final String databaseName = decoded.databaseName();
    final boolean forceSnapshot = decoded.forceSnapshot();

    if (forceSnapshot) {
      // Restore flow: replace files from the leader's snapshot even if the DB exists.
      // The leader's own files are already authoritative, so the leader skips the reinstall;
      // replicas close their local copy and pull the fresh snapshot from the leader.
      if (raftHAServer != null && raftHAServer.isLeader()) {
        HALog.log(this, HALog.TRACE, "Leader skips forceSnapshot reinstall for '%s'", databaseName);
        return;
      }

      final String leaderHttpAddr = raftHAServer.getLeaderHttpAddress();
      final String leaderHttpsAddr = raftHAServer.getLeaderHttpsAddress();
      final String clusterToken = raftHAServer.getClusterToken();
      try {
        // install() keeps the database open during the download and rolls back on failure, so a
        // failed restore never leaves it closed.
        SnapshotInstaller.install(databaseName, SnapshotInstaller.resolveDatabasePath(server, databaseName),
            leaderHttpAddr, leaderHttpsAddr, clusterToken, server);
      } catch (final IOException e) {
        throw new RuntimeException("Failed to install snapshot for restored database '" + databaseName + "'", e);
      }
      LogManager.instance().log(this, Level.INFO, "Database '%s' reinstalled via forceSnapshot from leader", databaseName);
      return;
    }

    // Normal create flow: skip if the database is already present locally.
    if (server.existsDatabase(databaseName)) {
      HALog.log(this, HALog.TRACE, "Database '%s' already present, skipping install-database entry", databaseName);
      return;
    }

    server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    LogManager.instance().log(this, Level.INFO, "Database '%s' created via Raft install-database entry", databaseName);
  }

  /**
   * Apply a {@link RaftLogEntryType#BOOTSTRAP_FINGERPRINT_ENTRY} on this peer (issue #4147 phase 5).
   * <p>
   * The committed entry names the peer chosen as the bootstrap source for {@code dbName} and
   * carries that source's {@code (fingerprint, lastTxId)}. Each peer compares its local state
   * against the committed baseline and decides:
   * <ul>
   *   <li><b>Match</b> (fingerprint and lastTxId both equal) - bootstrap locally, no bytes
   *       transfer, the database files on disk are already correct.</li>
   *   <li><b>Late newer joiner</b> (local lastTxId &gt; committed lastTxId) - this peer's data
   *       is fresher than the cluster's chosen baseline. We refuse to silently overwrite it and
   *       log a SEVERE pointing the operator at the recovery procedure.</li>
   *   <li><b>Mismatch</b> (any other case) - reinstall from the leader-shipped full snapshot.
   *       Subsequent transactions are picked up by native Ratis AppendEntries; no special
   *       transaction-delta path is needed because at first formation the Ratis log is empty.</li>
   * </ul>
   * The committed baseline is recorded in {@link #bootstrapBaselines} for status export and tests.
   * <p>
   * Package-private (not private) so ArcadeStateMachineBootstrapMismatchTest can exercise the
   * install-failure recovery path directly instead of via reflection.
   */
  // @VisibleForTesting
  void applyBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String dbName = decoded.databaseName();
    final String chosenFingerprint = decoded.bootstrapFingerprint();
    final long chosenLastTxId = decoded.bootstrapLastTxId();
    if (dbName == null || chosenFingerprint == null) {
      LogManager.instance().log(this, Level.WARNING,
          "BOOTSTRAP_FINGERPRINT_ENTRY missing required fields, skipping (db=%s, fp=%s)",
          dbName, chosenFingerprint);
      return;
    }
    recordBootstrapBaseline(dbName, new BootstrapBaseline(chosenFingerprint, chosenLastTxId));

    // Re-application during log replay on restart: if we've persisted an applied index at or
    // beyond this entry's index, the verification ran in a prior session and the local database
    // has since been forward-replicated past the baseline by Ratis AppendEntries. Re-running
    // the install path here would race leader-discovery (the StateMachineUpdater thread is
    // inside applyTransaction and blocks Ratis leader-info notifications), exhaust the snapshot
    // retry budget with null leader addresses, and trip the critical-error halt.
    // This is a PER-DATABASE decision, so it must consult THIS database's applied index, not the
    // global one: one ArcadeStateMachine multiplexes every database, and a co-located database that
    // advanced the global index past this entry must not suppress this database's verification
    // (issue #4824). Absent positive per-database evidence the verification re-runs, which is
    // idempotent (a fingerprint match returns immediately without moving any bytes).
    //
    // Upgrade note: a legacy plain-number applied-index file carries no per-database breakdown, so on
    // the FIRST restart after upgrading, this read returns -1 for every database and verification
    // re-runs for any bootstrap entry still above the latest Ratis snapshot. That is a bounded,
    // one-time cost and is safe: a matching local fingerprint returns immediately; a locally-fresher
    // copy (local lastTxId > baseline) hits the "refusing to overwrite local data" guard below (no
    // data loss, just a SEVERE log line); a genuinely-behind copy re-installs from the leader, which
    // is the correct action anyway. From the first post-upgrade apply onwards the per-database map is
    // authoritative.
    final long persistedApplied = readPersistedAppliedIndex(dbName);
    if (persistedApplied >= index) {
      HALog.log(this, HALog.BASIC,
          "Bootstrap baseline for '%s' already applied (persistedAppliedIndex=%d >= entryIndex=%d); skipping verification",
          dbName, persistedApplied, index);
      return;
    }

    if (!server.existsDatabase(dbName)) {
      // Late joiner with no local copy of this database. The follow-on INSTALL_DATABASE_ENTRY
      // (or natural Raft replay) will create the database and install the leader's snapshot;
      // we just record the baseline.
      LogManager.instance().log(this, Level.INFO,
          """
          Bootstrap baseline recorded for '%s' (lastTxId=%d); database not yet present locally, \
          will be created via leader-shipped snapshot""",
          dbName, chosenLastTxId);
      return;
    }

    // Compute local state.
    final String localFingerprint;
    final long localLastTxId;
    final String localPath;
    try {
      final ServerDatabase serverDb = server.getDatabase(dbName);
      final DatabaseInternal embedded = serverDb.getWrappedDatabaseInstance().getEmbedded();
      if (!(embedded instanceof LocalDatabase localDb)) {
        LogManager.instance().log(this, Level.WARNING,
            "BOOTSTRAP_FINGERPRINT_ENTRY for '%s': embedded database is not a LocalDatabase, skipping",
            dbName);
        return;
      }
      localPath = localDb.getDatabasePath();
      localFingerprint = BootstrapFingerprint.compute(new File(localPath));
      localLastTxId = localDb.getLastTransactionId();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not read local bootstrap state for '%s': %s; falling back to leader-shipped full snapshot",
          dbName, e.getMessage());
      installFromLeaderForBootstrap(dbName);
      return;
    }

    // Match: bootstrap locally, no bytes move.
    if (localLastTxId == chosenLastTxId && chosenFingerprint.equals(localFingerprint)) {
      LogManager.instance().log(this, Level.INFO,
          "Database '%s' bootstrapped locally (lastTxId=%d, fingerprint matches cluster baseline)",
          dbName, chosenLastTxId);
      return;
    }

    // Late newer joiner: the operator's data is fresher than the cluster's chosen baseline.
    // We will not silently overwrite it. Surface a SEVERE with the recovery procedure and leave
    // the local files in place. The operator can stop the cluster, copy this peer's data to the
    // others, and restart. Without this guard, a misconfigured rolling deploy could erase newer
    // transactions on a single pod by re-bootstrapping from older peers.
    if (localLastTxId > chosenLastTxId) {
      LogManager.instance().log(this, Level.SEVERE,
          """
          Database '%s': local lastTxId=%d is GREATER than cluster bootstrap lastTxId=%d. \
          This peer's data is fresher than the cluster's chosen baseline (committed \
          BOOTSTRAP_FINGERPRINT_ENTRY). Refusing to overwrite local data. To preserve it, \
          stop the cluster, copy this peer's database directory to every other peer, then \
          restart all peers.""",
          dbName, localLastTxId, chosenLastTxId);
      return;
    }

    // Mismatch: install the leader-shipped full snapshot. Runtime delta catch-up of the gap
    // beyond the baseline is handled natively by Ratis AppendEntries once the snapshot is in
    // place; at bootstrap time the Ratis log is empty on every peer so a transaction-level
    // delta cannot be served from it.
    LogManager.instance().log(this, Level.INFO,
        """
        Database '%s' bootstrap mismatch (local lastTxId=%d / fp=%s..., baseline lastTxId=%d / fp=%s...); \
        reinstalling from leader-shipped full snapshot""",
        dbName, localLastTxId, localFingerprint.substring(0, Math.min(8, localFingerprint.length())),
        chosenLastTxId, chosenFingerprint.substring(0, Math.min(8, chosenFingerprint.length())));
    try {
      installFromLeaderForBootstrap(dbName);
    } catch (final RuntimeException e) {
      // Applied on the Raft StateMachineUpdater thread: letting this propagate trips the critical-error
      // halt and shuts the server down, leaving the database closed. A transient leader unavailability
      // during restart must not do that - install downloads before touching the live files, so the
      // local copy is intact. Keep it and retry asynchronously.
      LogManager.instance().log(this, Level.SEVERE,
          "Failed to install snapshot during bootstrap for database '%s': %s. "
              + "Keeping the local copy and scheduling an async retry once a leader is reachable.",
          dbName, e.getMessage());
      // Safety net: install rolls back + reopens on failure; reopen here if left deregistered for any reason.
      // This branch should be unreachable on the normal failed-download case - install() is download-before-
      // close, so a download failure never touches the live files and leaves the DB open. It guards against
      // unexpected future changes (or a failure in a later install phase) that could leave it deregistered.
      if (!server.existsDatabase(dbName)) {
        try {
          server.getDatabase(dbName);
        } catch (final Exception reopenEx) {
          // Deliberate last resort: the database is both unusable and unreopenable, so there is nothing
          // safe to serve. Unlike the transient leader-unavailable case above (local copy intact, retried
          // async), this is unrecoverable locally, so we intentionally DO let it reach applyTransaction's
          // critical-error halt rather than mask data loss behind a node that keeps running.
          throw new RuntimeException("Cannot reopen database '" + dbName + "' after a failed bootstrap install", reopenEx);
        }
      }
      // Flag the pending download and run it off-thread; clearing the flag lets the HealthMonitor
      // persistent-lag backstop re-arm if this retry also fails on a still-quiet cluster.
      needsSnapshotDownload.set(true);
      // We are inside the catch on the Raft StateMachineUpdater thread: a RejectedExecutionException from
      // a shut-down executor (server stopping) must not escape, or it would reach applyTransaction's
      // critical-error halt - the very outcome this handler exists to prevent. The flag stays set, so the
      // HealthMonitor backstop still drives the download once the server is up again.
      try {
        lifecycleExecutor.submit(() -> {
          if (needsSnapshotDownload.compareAndSet(true, false))
            triggerSnapshotDownload();
          else
            // Another path (notifyLeaderChanged or the watchdog) already cleared the flag and is driving
            // the download; skip this retry. Logged so operators can trace why this submission did nothing.
            LogManager.instance().log(this, Level.INFO,
                "Bootstrap snapshot retry skipped for '%s': download already triggered by another path", dbName);
        });
      } catch (final RejectedExecutionException ree) {
        LogManager.instance().log(this, Level.WARNING,
            "Cannot schedule bootstrap snapshot retry for '%s': executor is shut down; "
                + "the HealthMonitor backstop will retry once the server is available", null, dbName);
      }
    }
  }

  /**
   * Close the local database and pull a full snapshot from the current leader. Same low-level
   * snapshot install machinery as {@code applyInstallDatabaseEntry(forceSnapshot=true)}.
   */
  private void installFromLeaderForBootstrap(final String dbName) {
    if (raftHAServer != null && raftHAServer.isLeader()) {
      // The leader has the chosen baseline by definition (it's the source). No need to install.
      HALog.log(this, HALog.TRACE, "Leader skips bootstrap snapshot install for '%s'", dbName);
      return;
    }

    try {
      // Resolve the leader address on each retry: the bootstrap-mismatch entry is applied
      // during Raft log replay on startup, which can race ahead of leader election on this peer.
      // install() keeps the local copy open during the download and rolls back on failure, so a
      // failed bootstrap install never leaves the database closed.
      final RaftHAServer raft = raftHAServer;
      final String clusterToken = raft != null ? raft.getClusterToken() : null;
      SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
          () -> raft != null ? raft.getLeaderHttpAddress() : null,
          () -> raft != null ? raft.getLeaderHttpsAddress() : null,
          clusterToken, server);
      LogManager.instance().log(this, Level.INFO,
          "Database '%s' reinstalled after bootstrap mismatch", dbName);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to install snapshot for bootstrap-mismatched database '" + dbName + "'", e);
    }
  }

  /**
   * Operator-triggered emergency recovery: drop the local copy of {@code dbName} and re-acquire a
   * fresh full snapshot from the current leader. This is the manual equivalent of the automatic
   * snapshot install path ({@link #notifyInstallSnapshotFromLeader}) and uses the same crash-safe
   * {@link SnapshotInstaller} machinery as {@link #installFromLeaderForBootstrap}.
   * <p>
   * The intended use case is a follower that has diverged from the leader (e.g. a
   * {@link WALVersionGapException} reported "snapshot resync required"): the diverged page versions
   * can never be reconciled by applying further deltas, so the only safe fix is to replace the local
   * files with the leader's authoritative copy. After install the local database matches the leader's
   * snapshot point; any Raft log entries replayed afterwards that predate the snapshot are skipped by
   * the page-version guard in {@code applyChanges}, and forward replication resumes normally.
   * <p>
   * Runs synchronously on the caller thread (the HTTP worker thread). Refuses to run on the leader
   * (it holds the authoritative copy) and when no leader is currently known.
   *
   * @param dbName name of the database to resync from the leader
   * @throws ReplicationException if Raft HA is not enabled, this node is the leader, the leader is
   *                              unknown, or the snapshot install fails
   */
  public void resyncDatabaseFromLeader(final String dbName) {
    final RaftHAServer raft = raftHAServer;
    if (raft == null)
      throw new ReplicationException("Cannot resync database '" + dbName + "': Raft HA is not enabled");

    if (raft.isLeader())
      throw new ReplicationException("Cannot resync database '" + dbName
          + "' on the leader: the leader holds the authoritative copy. Run the resync on the diverged follower.");

    if (raft.getLeaderHttpAddress() == null)
      throw new ReplicationException("Cannot resync database '" + dbName
          + "': the leader is currently unknown (election in progress?). Retry once a leader is elected.");

    LogManager.instance().log(this, Level.WARNING,
        "Operator-triggered resync of database '%s' from leader: dropping local copy and re-acquiring full snapshot", dbName);

    try {
      // Resolve the leader address on each retry (it can change mid-operation if leadership moves).
      // install() keeps the local copy open and serving during the download and only closes + swaps
      // once a complete snapshot is on disk, rolling back on failure. A failed resync therefore never
      // leaves the database closed (the cause of the operator-visible DatabaseIsClosedException).
      final String clusterToken = raft.getClusterToken();
      SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
          raft::getLeaderHttpAddress, raft::getLeaderHttpsAddress, clusterToken, server);
      LogManager.instance().log(this, Level.INFO, "Database '%s' resynced from leader on operator request", dbName);
    } catch (final IOException e) {
      throw new ReplicationException("Failed to resync database '" + dbName + "' from leader", e);
    }
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
   * True iff this state machine has never applied an application-level Raft log entry - in this
   * session or any prior one. This is the durable first-formation signal used by the offline
   * bootstrap protocol (issue #5099).
   * <p>
   * The raw Ratis commit index is not a reliable first-formation signal on its own: a leadership
   * transfer during bootstrap makes the new leader append internal no-op / configuration entries
   * that push its commit index above {@code 0} without committing any application data. Those
   * internal entries never flow through {@link #applyTransaction}, so neither the in-memory
   * {@link #lastAppliedIndex} nor the persisted applied index advances for them - while every real
   * mutation (TX / SCHEMA / INSTALL / DROP / SECURITY / BOOTSTRAP entry) advances both. The signal
   * therefore survives the internal term bump yet still turns {@code false} the instant any
   * application entry commits, preserving the issue #4800 guarantee that bootstrap can never
   * re-engage on a cluster that already holds data.
   * <p>
   * Both indices are consulted: the in-memory one for the current session, and the persisted one so
   * a restarted, already-bootstrapped cluster - whose {@code BOOTSTRAP_FINGERPRINT_ENTRY} has been
   * compacted below the Ratis snapshot and is not replayed - is never mistaken for a fresh one.
   * <p>
   * Defense in depth against a degraded read: the persisted {@code .raft/applied-index} file is
   * created only after at least one application entry has been applied (see
   * {@link #writePersistedAppliedIndex} and the snapshot-install path), so its mere presence proves
   * this node is not fresh. {@link #readPersistedAppliedIndex()} degrades a momentarily-unreadable or
   * corrupt file to {@code -1}; keying solely on that value could re-open the gate on a running
   * cluster whose file is transiently unreadable. We therefore treat an existing file as "already
   * applied" regardless of whether its contents parse, so a transient I/O error can never re-trigger
   * bootstrap on a cluster that already holds data.
   * <p>
   * Package-private: the sole caller is {@link BootstrapElection} in this package.
   */
  boolean hasNeverAppliedApplicationEntry() {
    if (lastAppliedIndex.get() >= 0)
      return false;
    final Path appliedIndexFile = getAppliedIndexFile();
    if (appliedIndexFile != null && Files.exists(appliedIndexFile))
      return false;
    // Reached only when the file path is unresolvable (server not wired yet, getAppliedIndexFile()
    // == null) or the file did not exist at the check above: read the persisted value as the final
    // signal. It is -1 for a genuinely fresh node.
    return readPersistedAppliedIndex() < 0;
  }

  // @VisibleForTesting
  void applyDropDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    final String databaseName = decoded.databaseName();

    // Idempotent on replay: if the database is already gone, nothing to do beyond evicting any
    // persisted baseline. applyBootstrapFingerprintEntry records a baseline by name even when the
    // database is not present locally (the late-joiner path), so a node can hold a persisted baseline
    // for a database it never had locally; evict it here so it does not linger in the file for the
    // node lifetime. This branch never calls drop(), so the eviction cannot precede a failed drop.
    if (!server.existsDatabase(databaseName)) {
      evictBootstrapBaseline(databaseName);
      HALog.log(this, HALog.TRACE, "Database '%s' already absent, skipping drop-database entry", databaseName);
      return;
    }

    // Only the rename below runs on the apply thread: the recursive delete costs one unlink per file and is
    // unbounded in the size of the database, and this loop is sequential and shared by every database
    // multiplexed on the state machine. Close, deregister and rename hold the databases lock as one unit -
    // mirroring the snapshot installer's swap - so no concurrent open can reopen the directory in between.
    final Path staged;
    synchronized (server.getDatabasesLock()) {
      // Resolved inside the lock: getDatabase reopens a database that is registered-but-closed, so resolving
      // it outside would let another holder of this lock deregister it between the lookup and the close, and
      // this thread would reopen the directory from disk only to close it again.
      final DatabaseInternal embedded = ((DatabaseInternal) server.getDatabase(databaseName)).getEmbedded();
      final Path databaseDirectory = Path.of(embedded.getDatabasePath());
      embedded.closeForDrop();
      server.removeDatabase(databaseName);
      // stageForDeletion falls back to deleting inline when the rename is impossible, and that fallback
      // belongs inside the lock even though it is slow: the directory still carries its live name, so
      // releasing the lock first would let a concurrent create of the same name meet a half-deleted one.
      staged = deferredDatabaseDeleter.stageForDeletion(databaseDirectory);
    }
    // Queued outside the lock: a saturated deletion queue runs the delete on this thread, and that must not
    // extend to holding the databases lock for the length of a recursive delete.
    if (staged != null)
      deferredDatabaseDeleter.deleteInBackground(staged);

    // Evict AFTER the drop succeeded, mirroring the applied-index drop eviction which runs only once
    // apply completes: if drop() had thrown and quarantined this database, the baseline must stay so a
    // restart can still recover it (evicting first would lose the baseline of a database that was not
    // actually dropped - the #5100 failure mode).
    evictBootstrapBaseline(databaseName);

    LogManager.instance().log(this, Level.INFO, "Database '%s' dropped via Raft drop-database entry%s", databaseName,
        staged != null ? " (files staged as '" + staged.getFileName() + "' for background deletion)" : "");
  }

  // @VisibleForTesting
  void setDeferredDatabaseDeleter(final DeferredDatabaseDeleter deleter) {
    final DeferredDatabaseDeleter previous = this.deferredDatabaseDeleter;
    this.deferredDatabaseDeleter = deleter;
    if (previous != null && previous != deleter)
      previous.close();
  }

  private void applySecurityUsersEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(this, Level.WARNING, "SECURITY_USERS_ENTRY has null payload, skipping");
      return;
    }
    server.getSecurity().applyReplicatedUsers(payload);
    HALog.log(this, HALog.DETAILED, "Applied SECURITY_USERS_ENTRY (%d bytes)", payload.length());
  }

  /**
   * Returns the GLOBAL persisted applied index: the highest Raft-log index applied across all
   * databases multiplexed on this state machine, or {@code -1} if none was persisted. This is a
   * Raft-log position, not a per-database guarantee; {@link #reinitialize()} compares it against the
   * (inherently global) Ratis snapshot index. Package-private for tests.
   */
  long readPersistedAppliedIndex() {
    ensureAppliedIndexLoaded();
    return globalAppliedIndex;
  }

  /**
   * Returns the persisted applied index for a single {@code dbName}, or {@code -1} when there is no
   * per-database evidence that this database was advanced (issue #4824). A legacy plain-number file
   * carries only the global value and therefore yields {@code -1} here: per-database decisions never
   * fall back to the global value, so a co-located database can never falsely satisfy them.
   * Package-private for tests.
   */
  long readPersistedAppliedIndex(final String dbName) {
    if (dbName == null)
      return -1;
    ensureAppliedIndexLoaded();
    final Long v = appliedIndexByDb.get(dbName);
    return v != null ? v : -1;
  }

  /**
   * Records {@code index} as the global applied position and, when {@code dbName} is non-null, as the
   * per-database applied position for that database, then serialises the bookkeeping to disk.
   * Package-private for tests.
   * <p>
   * Synchronised on {@link #appliedIndexFileLock}: the apply thread and the snapshot-install thread
   * (see {@link #writePersistedAppliedIndexForAllDatabases}) are the two writers, and the lock keeps
   * the in-memory update and the temp-file write+rename atomic with respect to each other so the
   * shared {@code applied-index.tmp} is never raced.
   */
  void writePersistedAppliedIndex(final long index, final String dbName) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      globalAppliedIndex = index;
      if (dbName != null)
        appliedIndexByDb.put(dbName, index);
      persistAppliedIndexFile();
    }
  }

  /**
   * Advances the global applied position to {@code index} and, in the SAME serialised write, evicts
   * {@code dbName} from the per-database map. Used for a {@code DROP_DATABASE_ENTRY}: the database is
   * gone, so its per-database entry must not linger and grow the map/persisted JSON for the node
   * lifetime (issue #4824). Folding the global advance and the eviction into one atomic write avoids
   * a crash window that could leave a stale per-database entry for a database that no longer exists.
   * Package-private for tests.
   */
  void writePersistedAppliedIndexDroppingDatabase(final long index, final String dbName) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      globalAppliedIndex = index;
      if (dbName != null)
        appliedIndexByDb.remove(dbName);
      persistAppliedIndexFile();
    }
  }

  /**
   * Records {@code index} as the global applied position and as the per-database position for every
   * database currently present on this node, then serialises once. Used by the full state-machine
   * snapshot install, after which every present database is at {@code index}. Synchronised on
   * {@link #appliedIndexFileLock} so it never races the apply-thread writer on the in-memory state or
   * the shared temp file.
   */
  void writePersistedAppliedIndexForAllDatabases(final long index) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      globalAppliedIndex = index;
      if (server != null)
        for (final String dbName : server.getDatabaseNames())
          appliedIndexByDb.put(dbName, index);
      persistAppliedIndexFile();
    }
  }

  /**
   * Lazily parses the persisted applied-index file once into the in-memory cache. Accepts both the
   * new JSON document ({@code {"global": n, "db": {"name": n, ...}}}) and a legacy plain-number file
   * (read as the global value with an empty per-database map). A missing/unreadable file simply means
   * "nothing persisted yet" (-1) and still latches the cache as loaded.
   * <p>
   * When the file path cannot yet be resolved (no server wired, so {@code getAppliedIndexFile()} is
   * {@code null}) the cache is NOT latched, so a later call retries once the server is available and
   * a persisted file is no longer masked. In the current wiring {@code setServer(...)} always runs
   * before the first read, so this only guards against a future reordering.
   */
  private void ensureAppliedIndexLoaded() {
    if (appliedIndexLoaded)
      return;
    synchronized (appliedIndexFileLock) {
      if (appliedIndexLoaded)
        return;
      final Path file = getAppliedIndexFile();
      if (file == null)
        return; // server not wired yet: do not latch, retry once the path is resolvable
      try {
        if (Files.exists(file)) {
          final String content = Files.readString(file).trim();
          if (!content.isEmpty()) {
            if (content.charAt(0) == '{') {
              final JSONObject json = new JSONObject(content);
              globalAppliedIndex = json.getLong("global", -1);
              final JSONObject perDb = json.getJSONObject("db", new JSONObject());
              for (final String name : perDb.keySet())
                appliedIndexByDb.put(name, perDb.getLong(name, -1));
            } else
              // Legacy format: a single plain number is the global Raft-log position.
              globalAppliedIndex = Long.parseLong(content);
          }
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Could not read persisted applied index: %s", e.getMessage());
      } finally {
        // The path was resolvable and we attempted a read: latch even on a parse failure so a corrupt
        // file is not re-read on every apply (it degrades to -1, re-running the idempotent verification).
        // Deliberate coupling: a corrupt file leaving globalAppliedIndex at -1 also makes
        // reinitialize()'s snapshot-gap check (persistedApplied >= 0 && ...) evaluate false, i.e. it
        // suppresses the "snapshot ahead, download from leader" path. This matches the pre-change
        // behavior (a parse failure already returned -1), so it is intentional, not a regression.
        appliedIndexLoaded = true;
      }
    }
  }

  /**
   * Serialises the in-memory applied-index bookkeeping to {@code .raft/applied-index} via a temp file
   * and atomic rename, so a crash mid-write never leaves a corrupt file.
   * <p>
   * Called once per applied entry (the file was already rewritten every apply before this change).
   * The per-database map is tiny (one entry per co-located database) and the small JSON it allocates
   * is dominated by the {@code createDirectories} + {@code writeString} + atomic {@code move} syscalls
   * that already ran every apply, so the extra allocation is negligible on the apply path.
   */
  private void persistAppliedIndexFile() {
    try {
      final Path file = getAppliedIndexFile();
      if (file == null)
        return;
      final JSONObject json = new JSONObject();
      json.put("global", globalAppliedIndex);
      final JSONObject perDb = new JSONObject();
      for (final Map.Entry<String, Long> entry : appliedIndexByDb.entrySet())
        perDb.put(entry.getKey(), entry.getValue());
      json.put("db", perDb);

      Files.createDirectories(file.getParent());
      final Path tmp = file.resolveSibling("applied-index.tmp");
      Files.writeString(tmp, json.toString());
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not write persisted applied index: %s", e.getMessage());
    }
  }

  private Path getAppliedIndexFile() {
    final Path raftDir = getRaftDir();
    return raftDir != null ? raftDir.resolve("applied-index") : null;
  }

  /**
   * The {@code .raft} state directory under the server database directory, or {@code null} when it
   * cannot yet be resolved (no server wired or no configured database directory). Shared by the
   * applied-index and bootstrap-baseline files so the two are guaranteed to be co-located.
   */
  private Path getRaftDir() {
    if (server == null)
      return null;
    final String dbDir = server.getConfiguration().getValueAsString(
        GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    if (dbDir == null)
      return null;
    return Path.of(dbDir, ".raft");
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
  private void ensureBootstrapBaselinesLoaded() {
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
              if (fingerprint != null)
                bootstrapBaselines.putIfAbsent(name, new BootstrapBaseline(fingerprint, entry.getLong("lastTxId", -1)));
            }
          }
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.FINE, "Could not read persisted bootstrap baselines: %s", e.getMessage());
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
  private void recordBootstrapBaseline(final String dbName, final BootstrapBaseline baseline) {
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
  private void evictBootstrapBaseline(final String dbName) {
    synchronized (bootstrapBaselinesFileLock) {
      ensureBootstrapBaselinesLoaded();
      if (bootstrapBaselines.remove(dbName) != null)
        persistBootstrapBaselinesFile();
    }
  }

  /**
   * Serialises {@link #bootstrapBaselines} to {@code .raft/bootstrap-baselines} via
   * {@link FileUtils#atomicWriteFile} (temp file, fsync, atomic rename with a non-atomic fallback and
   * temp cleanup), so a crash mid-write never leaves a corrupt file. Written only when a bootstrap
   * baseline is recorded or evicted (rare), not on the hot apply path. Callers hold
   * {@link #bootstrapBaselinesFileLock}.
   */
  private void persistBootstrapBaselinesFile() {
    try {
      final Path file = getBootstrapBaselinesFile();
      if (file == null)
        return;
      final JSONObject json = new JSONObject();
      for (final Map.Entry<String, BootstrapBaseline> e : bootstrapBaselines.entrySet()) {
        final JSONObject entry = new JSONObject();
        entry.put("fingerprint", e.getValue().fingerprint());
        entry.put("lastTxId", e.getValue().lastTxId());
        json.put(e.getKey(), entry);
      }
      FileUtils.atomicWriteFile(file.toFile(), json.toString());
    } catch (final Exception e) {
      // WARNING, not FINE: unlike the applied-index file (whose loss merely re-runs an idempotent
      // verification), a lost bootstrap baseline silently re-introduces #5100 - the baseline would be
      // invisible after the next restart. Surface a breadcrumb so an operator can notice.
      LogManager.instance().log(this, Level.WARNING, "Could not write persisted bootstrap baselines: %s", e.getMessage());
    }
  }

  private Path getBootstrapBaselinesFile() {
    final Path raftDir = getRaftDir();
    return raftDir != null ? raftDir.resolve("bootstrap-baselines") : null;
  }

  private void triggerSnapshotDownload() {
    if (raftHAServer == null || server == null)
      return;
    // Single-flight guard: multiple recovery paths (reinitialize watchdog, notifyLeaderChanged,
    // stale-follower recovery from the HealthMonitor) can request a download. Only one may run at
    // a time; concurrent requests are dropped. The flag also feeds isSnapshotDownloadPending() so
    // the stale-follower check does not re-arm while a download is already in flight.
    if (!snapshotDownloadInProgress.compareAndSet(false, true)) {
      // Another resync (from an overlapping WAL-gap / watchdog / leader-install path) is already
      // running; this request is folded into it. Log at INFO so a "triggering snapshot resync" SEVERE
      // always has a visible terminal disposition instead of vanishing silently (issue #5273).
      LogManager.instance().log(this, Level.INFO,
          "Snapshot resync already in progress; folding this request into the in-flight download (its completion will be logged once)");
      return;
    }
    try {
      final String leaderHttpAddr = raftHAServer.getLeaderHttpAddress();
      if (leaderHttpAddr == null) {
        LogManager.instance().log(this, Level.WARNING,
            "Cannot trigger snapshot download: leader HTTP address unknown");
        return;
      }
      final String leaderHttpsAddr = raftHAServer.getLeaderHttpsAddress();
      final String clusterToken = raftHAServer.getClusterToken();
      int resynced = 0;
      for (final String dbName : server.getDatabaseNames()) {
        // install() keeps the database open during the download and rolls back on failure, so a
        // watchdog-triggered resync never leaves it closed.
        if (server.existsDatabase(dbName)) {
          SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
              leaderHttpAddr, leaderHttpsAddr, clusterToken, server);
          resynced++;
        }
      }
      LogManager.instance().log(this, Level.INFO,
          "Snapshot resync completed: reinstalled %d database(s) from the leader; diverged state cleared", resynced);
      clearDivergedState();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Snapshot resync failed", e);
    } finally {
      snapshotDownloadInProgress.set(false);
    }
  }

  /**
   * Triggers a targeted snapshot resync of a single database from the leader (issue #4797).
   * <p>
   * Used when an unexpected error while applying an entry for {@code dbName} quarantines it: only the
   * affected database is reinstalled from the leader, leaving the healthy co-located databases on the
   * same shared {@link ArcadeStateMachine} untouched and the node running. On success the database is
   * removed from the diverged set via {@link #clearDivergedDatabase(String)}.
   * <p>
   * Participates in the same {@link #snapshotDownloadInProgress} single-flight protocol as
   * {@link #triggerSnapshotDownload()} so a targeted resync never overlaps a full download; if a full
   * download is already running it reinstalls this database too, so skipping here is safe. A skipped
   * resync is recovered by the {@link HealthMonitor} persistent-lag backstop ({@link #recoverFromPersistentLag()}).
   * No-op when there is no leader/server context or the lifecycle executor is shutting down.
   */
  private void triggerDatabaseResync(final String dbName) {
    if (raftHAServer == null || server == null)
      return;
    try {
      lifecycleExecutor.submit(() -> {
        if (!snapshotDownloadInProgress.compareAndSet(false, true)) {
          HALog.log(this, HALog.BASIC, "Snapshot download already in progress, skipping targeted resync of '%s'", dbName);
          return;
        }
        try {
          final String leaderHttpAddr = raftHAServer.getLeaderHttpAddress();
          if (leaderHttpAddr == null) {
            LogManager.instance().log(this, Level.WARNING,
                "Cannot resync quarantined database '%s': leader HTTP address unknown", dbName);
            return;
          }
          final String leaderHttpsAddr = raftHAServer.getLeaderHttpsAddress();
          final String clusterToken = raftHAServer.getClusterToken();
          // install() keeps the database open during the download and rolls back on failure, so a
          // targeted resync never leaves it closed.
          if (server.existsDatabase(dbName)) {
            SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
                leaderHttpAddr, leaderHttpsAddr, clusterToken, server);
            LogManager.instance().log(this, Level.INFO,
                "Targeted snapshot resync of quarantined database '%s' completed", dbName);
            clearDivergedDatabase(dbName);
          }
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE,
              "Targeted snapshot resync of quarantined database '" + dbName + "' failed", e);
        } finally {
          snapshotDownloadInProgress.set(false);
        }
      });
    } catch (final RejectedExecutionException ree) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot schedule targeted resync for database '%s': executor is shut down", ree, dbName);
    }
  }

  /**
   * Removes a single database from the diverged set after a targeted resync restored its state
   * (issue #4797). When the set becomes empty the bounded-escalation counter is reset, mirroring
   * {@link #clearDivergedState()} which clears everything after a full resync. The counter is shared
   * across databases, so it is only safe to reset once no database remains quarantined.
   */
  // @VisibleForTesting
  void clearDivergedDatabase(final String dbName) {
    divergedDatabases.remove(dbName);
    lastDivergedResyncLogByDb.remove(dbName);
    if (divergedDatabases.isEmpty())
      divergedSwallowedErrors.set(0);
  }

  /**
   * Returns {@code true} at most once per {@link #DIVERGED_RESYNC_LOG_THROTTLE_MS} window per database.
   * Used to rate-limit the "snapshot resync in progress" notice that would otherwise be emitted once per
   * committed entry while a database is quarantined after a WAL version gap, flooding the log and
   * starving the in-flight snapshot download on small nodes. Package-private for unit testing.
   */
  // @VisibleForTesting
  boolean shouldLogDivergedResync(final String dbName) {
    final long now = System.currentTimeMillis();
    final Long last = lastDivergedResyncLogByDb.get(dbName);
    if (last != null && now - last < DIVERGED_RESYNC_LOG_THROTTLE_MS)
      return false;
    lastDivergedResyncLogByDb.put(dbName, now);
    return true;
  }

  /**
   * Marks {@code dbName} as diverged from the committed Raft log (issue #4740). While a database is
   * diverged, unexpected Throwables raised while applying its entries are treated as recoverable
   * resync conditions in {@link #applyWithRetry} rather than fatal halts.
   */
  // @VisibleForTesting
  void markStateDiverged(final String dbName) {
    divergedDatabases.add(dbName);
  }

  /**
   * Clears the diverged-database set and the bounded-escalation counter after a snapshot resync has
   * restored consistent state across all databases. A resync always reinstalls every database from
   * the leader, so clearing the whole set (rather than a single database) matches what the resync
   * actually did.
   */
  // @VisibleForTesting
  void clearDivergedState() {
    divergedDatabases.clear();
    lastDivergedResyncLogByDb.clear();
    divergedSwallowedErrors.set(0);
  }

  // @VisibleForTesting
  boolean isDatabaseDiverged(final String dbName) {
    return divergedDatabases.contains(dbName);
  }

  // @VisibleForTesting
  int divergedSwallowedErrorCount() {
    return divergedSwallowedErrors.get();
  }

  // @VisibleForTesting
  boolean isHaltedAfterCriticalError() {
    return haltedAfterCriticalError.get();
  }

  /**
   * Returns {@code true} while this follower is replaying a burst of log entries to close a gap
   * with the leader (set in {@link #applyTransaction} when the applied-index jumps by more than one
   * and cleared once the applied index reaches the commit index). Used by the {@link HealthMonitor}
   * stale-follower check to avoid acting on lag that is actively shrinking.
   */
  public boolean isCatchingUp() {
    return catchingUp.get();
  }

  /**
   * Returns {@code true} if a snapshot download is queued (gap detected during {@code reinitialize})
   * or currently running. The {@link HealthMonitor} stale-follower check uses this to avoid
   * re-arming recovery while one is already in flight.
   */
  public boolean isSnapshotDownloadPending() {
    return needsSnapshotDownload.get() || snapshotDownloadInProgress.get();
  }

  /**
   * Returns {@code true} while this node may hold divergent data pending a snapshot resync: either a
   * snapshot download is queued/running ({@link #isSnapshotDownloadPending()}) or at least one database
   * is still marked diverged after a WAL version gap and awaiting its resync. Used to gate HA readiness
   * so a follower never advertises {@code /api/v1/ready} 200 while a resync is in flight (issue #5273);
   * the flag clears once {@link #clearDivergedState()} / {@link #clearDivergedDatabase(String)} run at
   * the end of a successful resync.
   */
  public boolean isResyncInProgress() {
    return isSnapshotDownloadPending() || !divergedDatabases.isEmpty();
  }

  /**
   * Re-arms a snapshot download from the leader for a follower that has been persistently lagging
   * without making progress (issue #3893). This covers the narrow window where a follower diverged
   * (apply failure) and its snapshot download also failed on a quiet cluster, so no new log entry
   * arrives to re-trigger recovery and the follower would otherwise stay diverged until restart.
   * <p>
   * Invoked by {@link HealthMonitor} after the lag has persisted for the configured duration.
   * No-op when this node is the leader, when there is no leader/server context, or when a download
   * is already pending or in progress.
   */
  public void recoverFromPersistentLag() {
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null || server == null || raftHA.isLeader())
      return;
    if (isSnapshotDownloadPending())
      return;
    LogManager.instance().log(this, Level.WARNING,
        "Persistent follower lag detected (applied=%d, commit=%d): re-arming snapshot download from leader",
        lastAppliedIndex.get(), raftHA.getCommitIndex());
    lifecycleExecutor.submit(this::triggerSnapshotDownload);
  }

  @Override
  public void close() throws IOException {
    lifecycleExecutor.shutdownNow();
    deferredDatabaseDeleter.close();
    super.close();
  }

  /**
   * Returns true if this node was restarted and the current entry might not have been applied
   * to database files before the crash/shutdown.
   * <p>
   * After a crash/restart, Ratis replays committed log entries through the state machine.
   * If this node becomes the new leader before replay completes, the leader-skip optimization
  /**
   * Deserializes a WAL transaction from raw bytes using the WALFile binary format.
   * <p>
   * Format: txId (long), timestamp (long), segmentCount (int), segmentSize (int),
   * then for each page segment: fileId (int), pageNumber (int), changesFrom (int),
   * changesTo (int), currentPageVersion (int), currentPageSize (int),
   * delta bytes (changesTo - changesFrom + 1).
   * <p>
   * One page contributes one segment per disjoint modified interval (issue #5470), so the same page can appear
   * several times, consecutively and at the same target version; {@code TransactionManager.applyChanges} folds them
   * back into a single page image.
   */
  static WALFile.WALTransaction deserializeWalTransaction(final byte[] data) {
    final ByteBuffer buf = ByteBuffer.wrap(data);
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    tx.txId = buf.getLong();
    tx.timestamp = buf.getLong();
    tx.forceApply = tx.txId < 0; // negative txId signals compaction page replication
    final int pageCount = buf.getInt();
    buf.getInt(); // segmentSize - not needed for deserialization

    // Reject a corrupted/misaligned entry instead of blowing up with a cryptic NegativeArraySizeException (issue #4420):
    // every WAL page occupies at least its 24-byte fixed header, so a page count exceeding the remaining bytes is corruption.
    if (pageCount < 0 || (long) pageCount * 6 * Integer.BYTES > buf.remaining())
      throw new ReplicationException("Corrupted WAL transaction entry: invalid page count " + pageCount);

    tx.pages = new WALFile.WALPage[pageCount];

    for (int i = 0; i < pageCount; i++) {
      final WALFile.WALPage page = new WALFile.WALPage();
      page.fileId = buf.getInt();
      page.pageNumber = buf.getInt();
      page.changesFrom = buf.getInt();
      page.changesTo = buf.getInt();
      page.currentPageVersion = buf.getInt();
      page.currentPageSize = buf.getInt();

      final int deltaSize = page.changesTo - page.changesFrom + 1;
      if (deltaSize <= 0 || page.changesFrom < 0 || deltaSize > buf.remaining())
        throw new ReplicationException("Corrupted WAL transaction entry: invalid delta range [" + page.changesFrom + ","
            + page.changesTo + "] for page " + page.fileId + ":" + page.pageNumber);
      final byte[] content = new byte[deltaSize];
      buf.get(content);
      page.currentContent = new Binary(content);

      tx.pages[i] = page;
    }

    return tx;
  }
}
