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
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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
   * Runs a leader-initiated snapshot install off the Ratis state-machine thread, which must not block.
   * <p>
   * This was {@code CompletableFuture.supplyAsync(...)} with no executor, i.e. the JDK common ForkJoinPool,
   * against the "No JDK common ForkJoinPool" rule at the head of {@code QueryEngineManager}'s class javadoc: that
   * pool is shared with user-supplied scripts (Gremlin, Polyglot) and with JDK internals, and a snapshot install
   * is a full database download - the longest-running thing the HA layer does. It has its own thread now
   * (issue #6202), which is also what lets the install wait on {@link #snapshotDownloadLock} instead of racing
   * the request-driven resyncs.
   * <p>
   * One worker, because Ratis serialises installs per division and {@code SnapshotInstaller} works over one set
   * of database directories; a bounded queue and {@code AbortPolicy} rather than caller-runs, because running on
   * the caller is precisely the outcome the offload exists to prevent - a rejection is turned into a failed
   * future so Ratis retries the install rather than the Ratis thread carrying the download.
   */
  private final ThreadPoolExecutor snapshotInstallExecutor = createSnapshotInstallExecutor();

  private static ThreadPoolExecutor createSnapshotInstallExecutor() {
    return new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(16), r -> {
      final Thread t = new Thread(r, "arcadedb-raft-snapshot-install");
      t.setDaemon(true);
      return t;
    }, new ThreadPoolExecutor.AbortPolicy());
  }

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

  /**
   * Databases that took the "local is fresher, refuse to overwrite" branch of
   * {@link #applyBootstrapFingerprintEntry} and were therefore NOT reinstalled from the cluster's
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
   * {@link #verifyBootstrapDivergence()}, surfaced to operators by {@code ClusterAlerts}, and cleared
   * exactly where this node's copy is actually replaced by the leader's.
   */
  private final Set<String> bootstrapUnreconciledDatabases = ConcurrentHashMap.newKeySet();

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

  /**
   * Serialises the resync paths against each other (issue #6202). {@link #snapshotDownloadInProgress} was the
   * only interlock, and it does not serialise: {@link #notifyInstallSnapshotFromLeader} proceeds when it LOSES
   * the CAS rather than standing down, because standing down would report an install it never performed. Two
   * downloads over one set of database directories were argued benign - both pull from the same leader and
   * {@code SnapshotInstaller} swaps atomically - but that argument is about today's installer, not about the
   * interlock, and it would outlive whoever remembers it. The lock states the invariant instead of deriving it.
   * <p>
   * The Ratis-initiated install waits for it; the two request-driven paths take it with {@code tryLock} and fold
   * into whatever holds it, exactly as they already fold into a lost CAS - they run on the single-threaded
   * {@link #lifecycleExecutor} and must not park it for the length of a download.
   * <p>
   * <b>Three of the six snapshot-pull paths are deliberately outside it</b>, and it is a choice rather than a
   * gap. {@code applyInstallDatabaseEntry}'s {@code forceSnapshot} branch and {@link #installFromLeaderForBootstrap}
   * run on the Ratis apply thread as part of applying a committed entry: they are already serialised against each
   * other by that single thread, they cannot fold (skipping leaves the database absent or diverged, which is the
   * state the entry exists to repair), and they must not park the apply loop - and with it replication for every
   * database on this node - for the length of a download it did not start. {@link #resyncDatabaseFromLeader} runs
   * on the operator's HTTP worker thread and reports its outcome to them synchronously, so folding would answer
   * "done" for work it did not do. What makes an actual overlap visible rather than silent is
   * {@code SnapshotInstaller}'s own {@code INSTALLS_IN_FLIGHT} set, which logs a WARNING naming the database when
   * two installs share one directory - the detector for the assumption this lock cannot enforce everywhere.
   */
  private final ReentrantLock snapshotDownloadLock = new ReentrantLock();

  // Highest Raft-log index whose data is actually present in the local databases while a flagged
  // stale-snapshot re-download is still outstanding; -1 when there is none (the normal case).
  //
  // reinitialize() can find a Ratis snapshot marker at an index the persisted applied-index file never
  // reached (snapshotIndex > persistedApplied + HA_SNAPSHOT_GAP_TOLERANCE). The entries in
  // (persistedApplied, snapshotIndex] were never applied here, yet seeding the marker makes Ratis
  // report snapshotIndex as this node's applied index - so RaftHAServer.getLastAppliedIndex() (the
  // predicate behind waitForAppliedIndex()/waitForLocalApply()) claims data this node does not hold and
  // a LINEARIZABLE / READ_YOUR_WRITES read inside the gap is served from the stale local state
  // (issue #6111). This field publishes the honest ceiling so those waiters clamp to it until the
  // flagged re-download actually lands; it is cleared only by a resync that restored the state, never
  // by merely starting one.
  //
  // Deliberately node-global, not per-database, and conservative on purpose: the gap is detected from
  // the global persisted position against the (inherently global) Ratis snapshot index, so which of the
  // co-located databases is actually short of the marker is not knowable here. A multi-database node
  // therefore clamps reads on every database while any gap is outstanding. The alternative - guessing
  // per-database from a global signal - is exactly the class of mistake issue #4824 fixed.
  private final AtomicLong    staleSnapshotAppliedFloor  = new AtomicLong(-1);
  // Wall-clock of the last retry submitted by retryUnfilledSnapshotGap(); 0 = none since the floor was
  // last cleared. Throttles the HealthMonitor-driven backstop, which ticks far more often than a full
  // multi-database resync costs.
  private final AtomicLong    lastStaleSnapshotRetryMs   = new AtomicLong();
  // Per-database read floor (issue #6760), the per-database counterpart of staleSnapshotAppliedFloor above.
  //
  // A leader-driven install may "give up" on a database that failed to refresh ACQUIRE_GIVE_UP_AFTER times in a
  // row: past that point the reconciler stops failing the whole install for it, so Ratis is not made to re-download
  // every healthy database on this node in a tight loop. That is the right call for the RETRY, but the install then
  // went on to record snapshotIndex as applied for EVERY database, clear the global floor and the diverged marks,
  // and return the installed TermIndex to Ratis - which purges the log. The node re-entered the ready set
  // advertising itself fully caught up while one database was still on its old copy, so a LINEARIZABLE (or
  // read-your-writes) read of THAT database passed the apply wait instantly and was served from stale state.
  //
  // Unlike the global floor, which is derived from a global signal and therefore has to clamp everything, this one
  // is published from a per-database verdict: exactly the databases the install did not bring to snapshotIndex are
  // clamped, and the healthy co-located ones keep serving unclamped reads. Entries are removed when the database is
  // genuinely refreshed (a later install, a targeted resync, or a full resync).
  private final ConcurrentHashMap<String, Long> staleDatabaseAppliedFloors = new ConcurrentHashMap<>();
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

  // Outcome slots for locally-originated transactions, keyed by "<databaseName>/<walTxId>". Two
  // threads race to claim the same slot and the winner decides who writes the entry's pages:
  //
  //   - the committing thread writes an AbandonedPhase2 when replication returned an INDETERMINATE
  //     result (the entry was dispatched to Ratis but submitAndWait timed out before quorum was
  //     confirmed - see ReplicationDispatchedTimeoutException). If such an entry later reaches
  //     quorum and is applied here, applyTxEntry MUST apply it locally instead of origin-skipping
  //     it, otherwise the write lands on every follower but never on this leader: a silent,
  //     permanent divergence (issue #4790);
  //   - the Raft apply thread writes an OriginSkipped when it passes a locally-originated entry and
  //     leaves the pages to phase 2.
  //
  // Whoever finds the other's slot already there knows the other side got in first, and the loser
  // takes over the work. That handshake is the whole point of routing BOTH sides through this one
  // map: before issue #6848 the abandoned mark was published only after the entry had already been
  // dispatched, so on a cold JVM the apply thread reached applyTxEntry first, found no mark and
  // origin-skipped an entry whose phase 2 never ran - the leader stayed one transaction behind its
  // followers for the rest of its uptime.
  //
  // Marking is always safe: it only changes behaviour IF the entry actually commits on this node's
  // state machine (applying is then correct because the followers have it); if the entry never
  // commits, the slot is inert and is pruned by TTL. Bounded by time-based pruning on insert.
  private final        Map<String, LocalTxOutcome> abandonedLocalTransactions = new ConcurrentHashMap<>();
  // Entries older than this are pruned on the next mark. Generous because a dispatched-but-stuck
  // entry can take a long time to either commit or be overwritten by a new leader.
  private static final long              ABANDONED_TX_TTL_MS           = 10 * 60 * 1000L;
  // The origin-skip slot is written on the leader's hot commit path, so it cannot afford the full
  // TTL scan the (rare) abandon path runs; throttled to once per window, this sweep keeps that path
  // O(1) while still bounding the map.
  //
  // It is a backstop, not the main disposal route. Ratis completes a write's client reply from the
  // applyTransaction future, so on the leader the apply - and therefore the slot - normally happens
  // BEFORE replicateTransaction returns and the committing thread's own finally removes it. What is
  // left for the sweep is the slots nobody came back for: a committing thread that died, and any exit
  // where the reply reached it by some other route than its own entry's apply.
  //
  // Nothing here is load-bearing on that Ratis ordering. If a reply ever overtook its apply, the only
  // consequence is a slot removed before it was written and then left for this sweep - map hygiene,
  // not correctness. The arbitration itself is settled by putIfAbsent in either order, which is the
  // whole reason it was moved into the map in the first place.
  //
  // The TTL those slots are held for is deliberately NOT tightened to "a few seconds". A slot must
  // outlive the whole window in which its committing thread can still abandon (2 x quorumTimeout
  // plus the grace wait, i.e. 30 s at the default arcadedb.ha.quorumTimeout of 10 s), because a slot
  // evicted inside that window would let the abandon claim a free key, roll back, and re-open the
  // #6848 lost write. ABANDONED_TX_TTL_MS clears that bar by an order of magnitude, which is the
  // point of reusing it.
  private static final long              ORIGIN_SKIP_PRUNE_EVERY_MS    = 60 * 1000L;
  private final        AtomicLong        lastOriginSkipPruneMs         = new AtomicLong();

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
   * One claim on a locally-originated transaction, written by whichever of the committing thread and
   * the Raft apply thread reaches {@link #abandonedLocalTransactions} first. {@code insertedAt} backs
   * the TTL pruning that bounds the map.
   */
  private sealed interface LocalTxOutcome permits AbandonedPhase2, OriginSkipped {
    long insertedAt();
  }

  /**
   * One abandoned locally-originated transaction: the phase-2 ticket its commit is still holding,
   * and when the mark was inserted (for TTL pruning). Carrying the ticket is what lets
   * {@link #applyTxEntry} release it once the entry finally applies here, instead of leaving the
   * snapshot checkpoint - and therefore Raft log purging - pinned until the node restarts (#5410).
   */
  private record AbandonedPhase2(long phase2Ticket, long insertedAt) implements LocalTxOutcome {
  }

  /**
   * The Raft apply thread passed this locally-originated entry and left its pages to phase 2. Its
   * only purpose is to be visible to a committing thread that abandons afterwards: finding it there
   * proves the entry committed AND that nothing else will ever write its pages on this node, so the
   * committer has to apply it itself (issue #6848).
   */
  private record OriginSkipped(long insertedAt) implements LocalTxOutcome {
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
   * <p>
   * <b>Stale marker (issue #6111):</b> when the marker index runs ahead of the persisted applied index
   * by more than {@link GlobalConfiguration#HA_SNAPSHOT_GAP_TOLERANCE}, the entries it covers were never
   * applied on this node and only the flagged re-download will bring them. The Ratis-facing applied
   * TermIndex is still seeded from the marker - it is the only replay position Ratis has, and
   * {@code StateMachineUpdater.reload()} requires it to match {@code getLatestSnapshot()} - but the
   * ArcadeDB-side {@link #lastAppliedIndex} stays on the honest persisted position, the read floor is
   * published for the apply waiters, and the "applied advanced" notification is withheld until a resync
   * has actually restored the state.
   * <p>
   * The floor guards <b>reads</b> only. Writes during the same window are already guarded, and by a
   * different mechanism: Ratis keeps feeding {@link #applyTransaction} the entries committed after the
   * marker, and applying one on top of a database that stops at {@code persistedApplied} fails its page
   * version check. {@link #applyTxEntry} converts that {@link WALVersionGapException} into a diverged
   * database plus an immediate snapshot resync instead of writing mismatched pages, so the gap can never
   * escalate from "stale data served" to "corrupted data written".
   */
  public void reinitialize() throws IOException {
    final long persistedApplied = readPersistedAppliedIndex();

    final var snapshotInfo = storage.getLatestSnapshot();
    if (snapshotInfo != null) {
      final long snapshotIndex = snapshotInfo.getIndex();
      final long snapshotGapTolerance = server != null
          ? server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_GAP_TOLERANCE)
          : GlobalConfiguration.HA_SNAPSHOT_GAP_TOLERANCE.getValueAsLong();
      final boolean staleSnapshot = persistedApplied >= 0 && snapshotIndex > persistedApplied + snapshotGapTolerance;
      if (staleSnapshot) {
        LogManager.instance().log(this, Level.INFO,
            "Snapshot index %d is ahead of persisted applied index %d, will download from leader when available",
            snapshotIndex, persistedApplied);
        needsSnapshotDownload.set(true);
        // Entries in (persistedApplied, snapshotIndex] are not on this node. Publish the honest ceiling
        // BEFORE the marker is seeded below, so no waiter can observe the seeded index without also
        // observing the floor that qualifies it (issue #6111).
        staleSnapshotAppliedFloor.set(persistedApplied);

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
      } else
        // The marker is backed by state this node really applied (or there is no persisted value to
        // contradict it), so nothing clamps the readers.
        staleSnapshotAppliedFloor.set(-1);

      // Only a trustworthy marker may seed the ArcadeDB-side counter: takeSnapshot() reads it as the
      // durability checkpoint it hands Ratis and pendingLocalPhase2 uses it as a replay floor, and
      // neither may claim entries this node never applied.
      lastAppliedIndex.set(staleSnapshot ? persistedApplied : snapshotIndex);
      // If the on-disk marker carries an inflated term (issues #575, #593), this seed records it as-is
      // (the previous applied TermIndex is null here, so no violation is possible) and the first
      // re-applied entry realigns it via the tolerant updateLastAppliedTermIndex override. The stale
      // snapshot.<inflatedTerm>_<index> filename persists across restarts until the next snapshot
      // rolls it over: cosmetic, expected.
      updateLastAppliedTermIndex(snapshotInfo.getTerm(), snapshotIndex);
      // Wake any threads blocked in RaftHAServer.waitForAppliedIndex()/waitForLocalApply(): this seed
      // can advance the applied index past a pending target (a follower catching up via snapshot
      // install), and notifyApplied() has no other caller on this path (issue #5846).
      //
      // Withheld on the stale-marker branch: nothing this node can serve advanced, so waking a waiter
      // whose target sits inside the gap is exactly what let it proceed on stale state (issue #6111).
      // The resync that fills the gap notifies once it completes.
      if (!staleSnapshot) {
        final RaftHAServer raftHA = this.raftHAServer;
        if (raftHA != null)
          raftHA.notifyApplied();
      }
    } else {
      lastAppliedIndex.set(-1);
      staleSnapshotAppliedFloor.set(-1);
    }

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
    // next snapshot after phase 2 drains (or after a pending stale-snapshot resync lands, issue #6111)
    // advances it normally.
    final var latest = storage.getLatestSnapshot();
    if (latest != null && currentIndex < latest.getIndex()) {
      HALog.log(this, HALog.BASIC,
          "Skipping snapshot checkpoint at index %d: the applied position trails the existing marker at %d "
              + "(phase-2 apply in flight, or a stale-snapshot resync still pending)",
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
   * Runs asynchronously on {@link #snapshotInstallExecutor} to avoid blocking the Ratis state machine thread.
   */
  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      final RaftProtos.RoleInfoProto roleInfoProto, final TermIndex firstTermIndexInLog) {

    LogManager.instance().log(this, Level.INFO,
        "HA resync started (mode=snapshot, reason=leader snapshot install): firstLogIndex=%s", firstTermIndexInLog);

    try {
      return CompletableFuture.supplyAsync(() -> installSnapshotFromLeader(roleInfoProto, firstTermIndexInLog),
          snapshotInstallExecutor);
    } catch (final RejectedExecutionException e) {
      // The offload exists so the download does not run on the Ratis thread, so a rejection must not be answered
      // by running it here. Ratis retries the install; a failed future leaves this node visibly behind until it
      // does, which is the state it is in (issue #6202).
      LogManager.instance().log(this, Level.SEVERE,
          "Cannot schedule the leader-initiated snapshot install: the install executor rejected it", e);
      return CompletableFuture.failedFuture(
          new IllegalStateException("Snapshot install executor rejected the task", e));
    }
  }

  /** The body of {@link #notifyInstallSnapshotFromLeader}, run on {@link #snapshotInstallExecutor}. */
  private TermIndex installSnapshotFromLeader(final RaftProtos.RoleInfoProto roleInfoProto,
      final TermIndex firstTermIndexInLog) {
    // Participate in the same single-flight protocol as triggerSnapshotDownload() so that
    // isSnapshotDownloadPending() returns true during this install and the HealthMonitor's
    // recoverFromPersistentLag() does not initiate a new concurrent triggerSnapshotDownload().
    // We use CAS (not unconditional set) to avoid clearing a flag owned by a concurrently
    // running triggerSnapshotDownload():
    //  - if we win (flag false->true): we own the flag and MUST clear it in finally.
    //  - if we lose (flag already true, another download in progress): we skip the flag but still perform the
    //    install, because standing down would report an install that never happened.
    // Losing the CAS used to mean the two downloads ran concurrently over one set of database directories -
    // benign only for as long as SnapshotInstaller keeps swapping atomically. snapshotDownloadLock states the
    // exclusion outright: this path waits for it (it owns its thread and may block), while the request-driven
    // paths fold into whatever holds it (issue #6202).
    final boolean acquiredSnapshotFlag = snapshotDownloadInProgress.compareAndSet(false, true);
    try {
      // Interruptibly, so close()'s shutdownNow() can unwind a thread parked behind an in-flight resync instead
      // of holding the shutdown open for the length of somebody else's download.
      snapshotDownloadLock.lockInterruptibly();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      if (acquiredSnapshotFlag)
        snapshotDownloadInProgress.set(false);
      throw new RuntimeException("Interrupted while waiting for an in-flight resync to finish", e);
    }
    try {
      final RaftPeerId leaderId = RaftPeerId.valueOf(
          roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());

      // The guards the manual resync path has always made, which this one had none of (issue #6202): a derived
      // address can name this node itself or the wrong peer, and reconcileDatabasesFromLeader would succeed,
      // the install would be recorded, the read floor dropped, and the node would return to the ready set
      // carrying whatever it copied. Refusing is the honest disposition - Ratis retries the install.
      final PeerDialAddress source = resolveSnapshotSource(leaderId);
      if (source.refused())
        throw new SnapshotRefusedException(source.refusal());

      final String leaderHttpAddr = source.httpAddress();
      // Both endpoints from the verdict: the reconciler prefers the encrypted one whenever it is non-null and
      // threads it into every branch, so a raw HTTPS address here would walk this path - the automatic one, the
      // one that had no checks at all before #6202 - straight back into the bug (issue #6221).
      final String leaderHttpsAddr = source.httpsAddress();
      final String clusterToken = raftHAServer.getClusterToken();

      // Databases the reconciler gave up on: it stopped failing the install for them, so they are NOT at the
      // snapshot index and must not be recorded as if they were (issue #6760).
      final Set<String> notInstalled = reconciler.reconcileDatabasesFromLeader(leaderHttpAddr, leaderHttpsAddr,
          clusterToken);

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
      writePersistedAppliedIndexForAllDatabases(snapshotIndex, notInstalled);
      // The install brought every database up to the snapshot point, so any read floor an earlier
      // stale marker published is now satisfied. Cleared BEFORE the notify below so a woken waiter
      // re-checks against the restored state instead of the floor (issue #6111).
      clearStaleSnapshotFloor();

      LogManager.instance().log(this, Level.INFO,
          "HA resync finished (mode=snapshot, result=%s): snapshotIndex=%d",
          notInstalled.isEmpty() ? "ok" : "partial", snapshotIndex);
      clearDivergedState();
      // A leader-driven install reinstalls every database present on this node, so a copy the bootstrap
      // overwrite guard had kept is gone and its divergence mark with it (issue #6124).
      clearAllBootstrapUnreconciled();
      // ... except the ones it did not reinstall. Re-arm those AFTER clearDivergedState()/clearStaleSnapshotFloor()
      // above, which are written for the all-databases-refreshed case (issue #6760).
      markDatabasesNotAtSnapshotIndex(notInstalled, snapshotIndex);

      // Wake any threads blocked in RaftHAServer.waitForAppliedIndex()/waitForLocalApply(): this
      // leader-driven snapshot install advances the applied index without going through
      // applyTransaction(), the only other notifyApplied() call site (issue #5846).
      //
      // LAST, after every floor and diverged mark of this install is in its final state. notifyApplied() holds
      // applyNotifier only long enough to notifyAll(), so a waiter can reacquire it and re-check
      // getTrustedAppliedIndex(db) immediately. Notifying any earlier than this leaves a window in which the
      // global floor is already cleared and the per-database one is not yet published - clearDivergedState()
      // above has just wiped it, or the first give-up never had one - so the woken waiter sees the raw Ratis
      // index, which already equals snapshotIndex, and a LINEARIZABLE or read-your-writes read of a database
      // this install did NOT refresh passes its wait and is served from the stale copy. That is precisely the
      // outcome issue #6760 exists to prevent, so the notify has to come after the re-arm, not before it.
      final RaftHAServer raftHA = this.raftHAServer;
      if (raftHA != null)
        raftHA.notifyApplied();

      return installedTermIndex;

    } catch (final SnapshotRefusedException e) {
      // A refusal is the expected, retried outcome this guard exists to produce, not a fault: Ratis re-drives
      // the install, so on a misconfigured cluster - or in the window right after an election, before the
      // leader-role flag catches up - it fires on every attempt. Logged at WARNING and without a stack trace,
      // like the same refusal on the two request-driven paths; a SEVERE per retry would trip log-based alerting
      // for a guard that is working as designed.
      LogManager.instance().log(this, Level.WARNING, SNAPSHOT_INSTALL_REFUSED + "%s", e.reason());
      throw new RuntimeException("Error during Raft snapshot installation", e);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error during snapshot installation from leader", e);
      throw new RuntimeException("Error during Raft snapshot installation", e);
    } finally {
      // Released in this order on purpose: the flag is the broader signal - isSnapshotDownloadPending() feeds the
      // HealthMonitor's decision not to start anything - so it must not read false while this install still holds
      // the lock. The converse window it leaves (lock free, flag still true) costs a concurrent request one folded
      // attempt, which the next retryUnfilledSnapshotGap() tick re-drives; swapping the two would only move the
      // window, not close it, and would move it to the side where something new can be started under a held lock.
      snapshotDownloadLock.unlock();
      if (acquiredSnapshotFlag)
        snapshotDownloadInProgress.set(false);
    }
  }

  /** Prefix of both the refusal exception's message and the WARNING it is logged with. */
  private static final String SNAPSHOT_INSTALL_REFUSED = "Refusing a leader-initiated snapshot install: ";

  /**
   * A snapshot resync that {@link #resolveSnapshotSource} refused before it started. A distinct type only so the
   * install path can tell it apart from a genuine installation failure in its catch chain and log it at the
   * severity its disposition deserves - the two request-driven paths return rather than throw, and never had to
   * make the distinction.
   */
  private static final class SnapshotRefusedException extends IllegalStateException {
    private final String reason;

    private SnapshotRefusedException(final String reason) {
      super(SNAPSHOT_INSTALL_REFUSED + reason);
      this.reason = reason;
    }

    /** The refusal on its own, so the log line can carry a literal prefix rather than a fully formatted message. */
    String reason() {
      return reason;
    }
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
   *
   * @return {@code true} when the mark now stands, so {@link #applyTxEntry} will write this entry's
   * pages; {@code false} when the Raft apply thread had already passed the entry and origin-skipped
   * it, which leaves the caller holding a committed entry nothing else will ever apply here - it must
   * apply the transaction itself (issue #6848).
   */
  boolean markLocalTransactionAbandoned(final String databaseName, final long walTxId, final long phase2Ticket) {
    final long now = System.currentTimeMillis();
    // Prune stale marks (entries that were dispatched but never committed, e.g. the slot was
    // overwritten by a new leader) so the map cannot grow unbounded. A pruned mark deliberately does
    // NOT release its ticket: pruning is not proof the entry never committed, and if it does commit
    // later it will now be origin-skipped (unapplied here), so it must stay inside the replay window.
    if (!abandonedLocalTransactions.isEmpty())
      abandonedLocalTransactions.values().removeIf(abandoned -> now - abandoned.insertedAt() > ABANDONED_TX_TTL_MS);

    final String key = abandonedKey(databaseName, walTxId);
    final LocalTxOutcome existing = abandonedLocalTransactions.putIfAbsent(key, new AbandonedPhase2(phase2Ticket, now));
    if (existing == null) {
      // The common case: the apply thread has not reached this entry yet, or the entry will never
      // commit. Either way the mark now stands and applyTxEntry is the one that will write the pages.
      HALog.log(this, HALog.BASIC,
          "Marked locally-originated tx %d on database '%s' for local apply on commit (replication was indeterminate, #4790)",
          walTxId, databaseName);
      return true;
    }

    if (existing instanceof final AbandonedPhase2 firstMark) {
      // Two commits abandoned the SAME transaction id, which the WAL counter is supposed to make
      // impossible. putIfAbsent keeps the first mark, so THIS caller's ticket is now held by nobody's
      // apply - the #5410 pinned-checkpoint condition, for one ticket. Keeping the first mark is the
      // safe direction (a held ticket costs log compaction, a wrongly released one costs a write), but
      // it must not be silent: a future change that breaks the uniqueness assumption has to be
      // diagnosable from the log rather than from an unexplained checkpoint that stops advancing.
      LogManager.instance().log(this, Level.WARNING,
          "Transaction id %d on database '%s' was marked abandoned twice (phase-2 tickets %d then %d). "
              + "WAL transaction ids are expected to be unique per commit; keeping the first mark, so ticket %d "
              + "stays held until this node restarts (#5410 checkpoint pinning).",
          walTxId, databaseName, firstMark.phase2Ticket(), phase2Ticket, phase2Ticket);
      return true;
    }

    // The apply thread got here first: it already passed this entry and origin-skipped it, so the
    // entry IS committed and nothing else is ever going to write its pages on this node. Clear the
    // slot and tell the caller it owns the apply (issue #6848).
    abandonedLocalTransactions.remove(key, existing);
    HALog.log(this, HALog.BASIC,
        "Locally-originated tx %d on database '%s' was origin-skipped before its abandoned mark was published; "
            + "the committing thread must apply it locally (#6848)",
        walTxId, databaseName);
    return false;
  }

  /**
   * Claims a committed, locally-originated entry on the Raft apply thread and reports what to do with
   * it: {@link #NO_ABANDONED_MARK} to origin-skip (phase 2 owns the pages), or the phase-2 ticket to
   * release after applying when the committing thread already abandoned this transaction.
   * <p>
   * The origin-skip answer is not merely returned, it is <b>published</b>: the slot left behind is how
   * a committing thread that abandons later discovers that the apply already happened without it and
   * that it must write the pages itself. Without that publication the two threads raced with no
   * arbiter and the leader silently dropped the write (issue #6848).
   */
  // @VisibleForTesting - the handshake with markLocalTransactionAbandoned is the load-bearing part
  // of #6848, and driving it through applyTransaction would need a whole Ratis division stubbed out.
  long claimLocalOriginatedEntry(final String databaseName, final long walTxId) {
    final long now = System.currentTimeMillis();
    final String key = abandonedKey(databaseName, walTxId);
    final LocalTxOutcome existing = abandonedLocalTransactions.putIfAbsent(key, new OriginSkipped(now));
    if (existing instanceof final AbandonedPhase2 abandoned) {
      abandonedLocalTransactions.remove(key, abandoned);
      return abandoned.phase2Ticket();
    }
    if (existing == null)
      pruneStrandedLocalTxOutcomes(now);
    return NO_ABANDONED_MARK;
  }

  /**
   * Drops origin-skip slots nothing came back for, at most once per
   * {@link #ORIGIN_SKIP_PRUNE_EVERY_MS}. Throttled because this runs on the leader's commit path,
   * where the unthrottled full scan {@link #markLocalTransactionAbandoned} can afford would be
   * charged to every transaction. See {@link #ORIGIN_SKIP_PRUNE_EVERY_MS} for why the slots it
   * collects are the exception rather than the rule, and why their TTL stays long.
   */
  // @VisibleForTesting - a backstop nothing reaches on a healthy path is exactly the code that rots
  // unnoticed, and the invariant that matters (it must never evict an abandoned mark, whose ticket
  // only the apply may release) is not observable through the handshake alone.
  void pruneStrandedLocalTxOutcomes(final long now) {
    final long last = lastOriginSkipPruneMs.get();
    if (now - last < ORIGIN_SKIP_PRUNE_EVERY_MS || !lastOriginSkipPruneMs.compareAndSet(last, now))
      return;
    abandonedLocalTransactions.values()
        .removeIf(outcome -> outcome instanceof OriginSkipped && now - outcome.insertedAt() > ABANDONED_TX_TTL_MS);
  }

  /**
   * Drops the slot a locally-originated transaction may hold once its outcome is settled by any exit
   * other than "abandoned". A no-op when no slot exists, which is the common case: the slot only
   * materializes when the Raft apply thread reached the entry before this method ran.
   */
  void forgetLocalOriginatedEntry(final String databaseName, final long walTxId) {
    if (abandonedLocalTransactions.isEmpty())
      return;
    final String key = abandonedKey(databaseName, walTxId);
    final LocalTxOutcome existing = abandonedLocalTransactions.get(key);
    if (existing instanceof OriginSkipped)
      abandonedLocalTransactions.remove(key, existing);
  }

  /**
   * Reads a replicated WAL transaction's id without deserializing the pages behind it: the id is the
   * first field {@link #deserializeWalTransaction(byte[])} writes, so it is the leading 8 bytes of the
   * payload. Used on the leader's commit path, where the full deserialization would be pure waste for
   * an entry that is about to be origin-skipped.
   */
  static long peekWalTransactionId(final byte[] walData) {
    if (walData == null || walData.length < Long.BYTES)
      throw new ReplicationException("Corrupted WAL transaction entry: truncated before the transaction id");
    return ByteBuffer.wrap(walData, 0, Long.BYTES).getLong();
  }

  /**
   * Consumes the abandoned mark for a locally-originated transaction, returning the phase-2 ticket
   * to release once its pages are written (possibly {@link #NO_PHASE2_TICKET} when its commit held
   * none), or {@link #NO_ABANDONED_MARK} when the transaction was not abandoned at all - the
   * origin-skip case, where phase 2 already wrote the pages and released its own ticket.
   * <p>
   * Consuming is one-shot so a later replay of the same entry correctly origin-skips again.
   * <p>
   * <b>This is NOT the branch {@link #applyTxEntry} takes</b> - it stopped being that in #6848.
   * Reading the mark is only half of what the apply thread has to do; the other half is publishing
   * its own decision, and only {@link #claimLocalOriginatedEntry} does both atomically. What survives
   * here is the read-and-clear on its own, for callers that want to inspect or drain one mark without
   * claiming the entry: the map's own unit tests, which pin the mark/ticket correlation #5410 added,
   * and which must keep testing exactly that and not the arbitration on top of it. Do not call this
   * from an apply path - it would decide without publishing, which is precisely the shape of the
   * #6848 lost write.
   */
  long consumeAbandonedLocalTransaction(final String databaseName, final long walTxId) {
    final LocalTxOutcome outcome = abandonedLocalTransactions.remove(abandonedKey(databaseName, walTxId));
    return outcome instanceof final AbandonedPhase2 abandoned ? abandoned.phase2Ticket() : NO_ABANDONED_MARK;
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
    // Origin skip (the leader's hot path): a locally-originated entry is normally applied via
    // commit2ndPhase() in RaftReplicatedDatabase, so skip it here to avoid a double-apply. Using
    // originatedLocally (set by startTransaction) instead of isLeader() avoids TOCTOU races when
    // leadership changes between entry submission and state machine apply. After a crash and
    // restart, originatedLocally is always false (startTransaction was not called in this lifecycle),
    // so replayed entries are correctly re-applied with page-version guards providing idempotency.
    //
    // EXCEPTION (issue #4790): commit() may have abandoned its phase 2 because replication returned
    // an indeterminate result (entry dispatched to Ratis but the quorum wait timed out before quorum
    // was confirmed). For such an entry phase 2 never ran, so it must be applied HERE instead of
    // origin-skipped, otherwise this leader silently loses a write the followers already have. The
    // mark is consumed (removed) so a later replay of the same entry correctly skips again.
    //
    // The claim below is a handshake, not a lookup (issue #6848): skipping also PUBLISHES the skip,
    // so a committing thread that abandons after this point can see that the apply already happened
    // without it and take over the write itself. Only the 8-byte transaction id is read here - the
    // WAL is deserialized further down, on the branch that actually applies it, so the skip stays as
    // cheap as the pre-#6848 short-circuit it replaces.
    // The ticket that commit() is still holding for this entry, when it was abandoned. Released
    // below once applyChanges has written the pages - never on the origin-skip branch, where
    // releasing would reintroduce #5407.
    long abandonedPhase2Ticket = NO_PHASE2_TICKET;
    if (originatedLocally) {
      final long abandoned = claimLocalOriginatedEntry(decoded.databaseName(), peekWalTransactionId(decoded.walData()));
      if (abandoned == NO_ABANDONED_MARK) {
        HALog.log(this, HALog.TRACE, "Skipping tx apply on originator for database '%s'", decoded.databaseName());
        return;
      }
      abandonedPhase2Ticket = abandoned;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final WALFile.WALTransaction walTx = deserializeWalTransaction(decoded.walData());

    if (originatedLocally)
      HALog.log(this, HALog.BASIC,
          "Applying locally-originated tx %d on database '%s' whose phase 2 was abandoned (replication indeterminate, #4790)",
          walTx.txId, decoded.databaseName());

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
   * <p>
   * A file id already registered under a DIFFERENT name is not that same idempotent case: the two
   * names cannot both be legitimate replays of this entry, so it means this node's file-id space has
   * diverged from the leader's (issue #6063) - for example, a first-formation bootstrap peer whose
   * local copy of a database was locally fresher than the cluster's chosen baseline and was therefore
   * left alone by {@link #applyBootstrapFingerprintEntry} rather than reinstalled, but whose file ids
   * were assigned by an independent history and can collide with a later, ordinary replicated schema
   * change (e.g. the bucket a brand new type creates). Silently skipping such a collision would leave
   * the new component never actually created on this node while the schema entry is merged anyway,
   * so the type is later dropped from the schema silently on load ("Cannot find bucket ..., removing
   * it from type configuration") - the type count divergence {@code DatabaseComparator} reports.
   * Throwing here instead routes through {@link #applyWithRetry}'s generic {@code RuntimeException}
   * handling straight to {@link #handleUnexpectedApplyError}, which quarantines the database and
   * triggers a full snapshot resync - replacing this node's whole file-id space rather than trying to
   * reconcile it entry by entry.
   */
  private void createNewFiles(final DatabaseInternal db, final Map<Integer, String> filesToAdd) throws IOException {
    final String databasePath = db.getDatabasePath();
    for (final Map.Entry<Integer, String> fileEntry : filesToAdd.entrySet()) {
      final int fileId = fileEntry.getKey();
      final String fileName = fileEntry.getValue();
      // Skip if already registered in memory (idempotent) - but only when it is truly the same file
      if (db.getFileManager().existsFile(fileId)) {
        final String existingName = db.getFileManager().getFile(fileId).getFileName();
        if (!existingName.equals(fileName))
          throw new SchemaException(
              "File id " + fileId + " for database '" + db.getName() + "' already names '" + existingName
                  + "' locally but a committed schema change expects it to name '" + fileName
                  + "' - this node's file-id space has diverged from the leader's");
        continue;
      }
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
  // Package-private rather than private so Issue6839TsSealedBlobRecoveryTest can drive the apply path directly:
  // the recovery it pins is entirely inside this method, and a 3-node IT would only add flakiness to prove it.
  void applySealedBlobs(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedBlob> blobs)
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
      if (!(schema.getType(blob.typeName()) instanceof LocalTimeSeriesType tsType)) {
        LogManager.instance().log(this, Level.SEVERE,
            "Received TimeSeries sealed blob for non-timeseries type '%s' (db=%s); skipping", null,
            blob.typeName(), decodedDbName(db));
        continue;
      }
      if (tsType.getEngine() == null) {
        // The type is registered with no engine (issue #6356), which used to end here: the blob was logged away,
        // and since a Raft entry is applied once and never re-shipped, that made every one of them a permanent
        // divergence - on the very state LocalSchema.readConfiguration() justifies keeping by pointing at HA as
        // the thing that would repair it. This blob IS that repair, so install it and retry (issue #6839).
        if (repairEngineWithSealedBlob(db, tsType, blob))
          HALog.log(this, HALog.DETAILED,
              "Repaired TimeSeries type %s shard %d from a replicated sealed blob (%d bytes) on db '%s'",
              blob.typeName(), blob.shardIndex(), blob.bytes().length, decodedDbName(db));
        continue;
      }
      tsType.getEngine().getShard(blob.shardIndex()).getSealedStore().installSealedFileBytes(blob.bytes());
      HALog.log(this, HALog.DETAILED, "Installed TimeSeries sealed blob for %s shard %d (%d bytes) on db '%s'",
          blob.typeName(), blob.shardIndex(), blob.bytes().length, decodedDbName(db));
    }
  }

  /**
   * Puts the leader's sealed bytes in place for a type registered with no engine and re-runs {@code initEngine()}.
   * Returns whether the type now has one.
   * <p>
   * The order is the point. Retrying {@code initEngine()} FIRST recovers nothing in the reported case: the
   * engine failed because this shard's {@code .ts.sealed} could not be opened, so re-opening the same file fails
   * for the same reason and there is still no store to install the blob through. The blob is the authoritative
   * copy of exactly that file, so it goes down first and {@code initEngine()} then opens the store over it -
   * which is also why nothing is installed afterwards: the file already IS the blob.
   * <p>
   * Written through a temporary and moved with {@code REPLACE_EXISTING}, the same way
   * {@code TimeSeriesSealedStore.installSealedFileBytes} does it, so a crash mid-write cannot leave a half-file
   * where a whole one used to be. The target name is derived from the type and shard rather than taken from
   * {@code blob.fileName()}: a path from a replicated payload must not select a file on this node.
   * <p>
   * A failure here is logged and reported, never thrown: one unrepairable type must not abort the apply of an
   * entry that may carry blobs for others, and the state it leaves behind is the state it started from - still
   * visible, still reported by {@code CHECK DATABASE}, still failing loudly on every read and write.
   */
  private boolean repairEngineWithSealedBlob(final DatabaseInternal db, final LocalTimeSeriesType tsType,
      final RaftLogEntryCodec.TsSealedBlob blob) {
    // tsType.getName(), not blob.typeName(): the two are equal here - tsType came from getType(blob.typeName()) -
    // but taking it from the resolved schema type means the name selecting a file on this node provably came from
    // the local schema and not from the wire, rather than only doing so as long as the caller keeps that lookup.
    final File target = new File(db.getDatabasePath(),
        TimeSeriesSealedStore.sealedFileNameFor(tsType.getName(), blob.shardIndex()));
    final File incoming = new File(target.getPath() + ".incoming");
    try {
      try (final FileOutputStream out = new FileOutputStream(incoming)) {
        out.write(blob.bytes());
        out.getFD().sync();
      }
      Files.move(incoming.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      tsType.initEngine();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Received TimeSeries sealed blob for type '%s' shard %d (db=%s) whose storage engine is unavailable, and "
              + "the engine could not be initialised over it: %s", e, blob.typeName(), blob.shardIndex(),
          decodedDbName(db), e.getMessage());
      return false;
    }

    if (!tsType.isEngineAvailable()) {
      LogManager.instance().log(this, Level.SEVERE,
          "TimeSeries type '%s' (db=%s) still has no storage engine after installing the replicated sealed blob "
              + "for shard %d; skipping", null, blob.typeName(), decodedDbName(db), blob.shardIndex());
      return false;
    }
    return true;
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

      // Same refusals as every other path that pulls a snapshot, through the same helper (issue #6202): a
      // derived address that names this node would "restore" the local copy from itself and report success,
      // which is worse than the failure the caller already handles below.
      final PeerDialAddress source = resolveSnapshotSource(raftHAServer.getLeaderId());
      if (source.refused())
        throw new RuntimeException("Cannot reinstall database '" + databaseName + "' from the leader: "
            + source.refusal());

      final String leaderHttpAddr = source.httpAddress();
      // The guard's own HTTPS endpoint rather than the raw resolver's: it is declared and derived independently
      // of the HTTP one, so the HTTP verdict does not cover it (issue #6221). Null falls back to plain HTTP.
      final String leaderHttpsAddr = source.httpsAddress();
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
      clearBootstrapUnreconciled(databaseName);
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
    final BootstrapBaseline local;
    try {
      local = readLocalBootstrapState(dbName);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not read local bootstrap state for '%s': %s; falling back to leader-shipped full snapshot",
          dbName, e.getMessage());
      installFromLeaderForBootstrap(dbName);
      return;
    }
    if (local == null) {
      LogManager.instance().log(this, Level.WARNING,
          "BOOTSTRAP_FINGERPRINT_ENTRY for '%s': embedded database is not a LocalDatabase, skipping",
          dbName);
      return;
    }
    final String localFingerprint = local.fingerprint();
    final long localLastTxId = local.lastTxId();

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
      // The refusal keeps this node's file-id space assigned by an independent history, and nothing
      // used to reconcile it afterwards (issue #6124). Record it durably so the condition survives the
      // restart that the per-database replay-skip stops this branch from re-evaluating, gets re-verified
      // against the leader by verifyBootstrapDivergence(), and is visible in the cluster status instead
      // of only in this one log line.
      markBootstrapUnreconciled(dbName);
      LogManager.instance().log(this, Level.SEVERE,
          """
          Database '%s': local lastTxId=%d is GREATER than cluster bootstrap lastTxId=%d. \
          This peer's data is fresher than the cluster's chosen baseline (committed \
          BOOTSTRAP_FINGERPRINT_ENTRY). Refusing to overwrite local data. To preserve it, \
          stop the cluster, copy this peer's database directory to every other peer, then \
          restart all peers. Until this node's copy is reconciled its file ids are out of step \
          with the rest of the cluster: to discard it and adopt the leader's copy instead, run \
          POST /api/v1/cluster/resync/%s on this node.""",
          dbName, localLastTxId, chosenLastTxId, dbName);
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
      // Resolved through the same guard as every other snapshot pull: the supplier answers null - which
      // install() treats as "no leader to pull from" and retries - rather than handing back an address that
      // names this node or no single peer (issue #6202).
      SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
          this::guardedLeaderHttpAddress, this::guardedLeaderHttpsAddress, clusterToken, server);
      LogManager.instance().log(this, Level.INFO,
          "Database '%s' reinstalled after bootstrap mismatch", dbName);
      clearBootstrapUnreconciled(dbName);
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

    final PeerDialAddress source = resolveSnapshotSource(raft.getLeaderId());
    if (source.refused())
      // The two checks this path used to make by hand (is this the leader, is the address known) are two of the
      // three the helper makes, and the third - an address that identifies no single peer - is the one an
      // operator most needs told about before a database is replaced (issue #6202).
      throw new ReplicationException("Cannot resync database '" + dbName + "': " + source.refusal());

    LogManager.instance().log(this, Level.WARNING,
        "Operator-triggered resync of database '%s' from leader: dropping local copy and re-acquiring full snapshot", dbName);

    try {
      // Resolve the leader address on each retry (it can change mid-operation if leadership moves) - and re-guard
      // it on each retry with it, or the refusal above is a point-in-time check that a later attempt walks
      // straight past onto this node's own address (issue #6202). The check above is still worth making: it turns
      // an already-doomed request into an immediate, descriptive refusal instead of a failed download.
      // install() keeps the local copy open and serving during the download and only closes + swaps
      // once a complete snapshot is on disk, rolling back on failure. A failed resync therefore never
      // leaves the database closed (the cause of the operator-visible DatabaseIsClosedException).
      final String clusterToken = raft.getClusterToken();
      SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
          this::guardedLeaderHttpAddress, this::guardedLeaderHttpsAddress, clusterToken, server);
      LogManager.instance().log(this, Level.INFO, "Database '%s' resynced from leader on operator request", dbName);
      // This is the action the bootstrap-divergence alert asks the operator for: the local copy the
      // overwrite guard kept has just been replaced, so the mark goes with it (issue #6124).
      clearBootstrapUnreconciled(dbName);
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
   * Reads this node's own {@code (fingerprint, lastTxId)} for {@code dbName} - the same pair the
   * bootstrap protocol compares peers on - or {@code null} when the database is not backed by a
   * {@link LocalDatabase} (nothing to fingerprint). Shared by the bootstrap verification in
   * {@link #applyBootstrapFingerprintEntry} and the periodic re-verification in
   * {@link #reconcileBootstrapDivergence}, so the two can never drift on what "local state" means.
   *
   * @throws Exception when the database cannot be opened or its directory cannot be read; callers
   *                   decide what an unreadable local copy means for them.
   */
  private BootstrapBaseline readLocalBootstrapState(final String dbName) throws Exception {
    final ServerDatabase serverDb = server.getDatabase(dbName);
    final DatabaseInternal embedded = serverDb.getWrappedDatabaseInstance().getEmbedded();
    if (!(embedded instanceof LocalDatabase localDb))
      return null;
    return new BootstrapBaseline(BootstrapFingerprint.compute(new File(localDb.getDatabasePath())),
        localDb.getLastTransactionId());
  }

  /**
   * Databases that took the bootstrap "local is fresher, refuse to overwrite" branch and have not been
   * reconciled with the cluster since (issue #6124), sorted for deterministic output. Read by
   * {@code ClusterAlerts} so the condition is visible in {@code GET /api/v1/cluster} rather than only in
   * a SEVERE line emitted once, at bootstrap, possibly several restarts ago.
   */
  public List<String> getBootstrapUnreconciledDatabases() {
    ensureBootstrapBaselinesLoaded();
    // The overwhelmingly common answer, and this is read on every Studio status poll: allocate nothing
    // for it.
    if (bootstrapUnreconciledDatabases.isEmpty())
      return Collections.emptyList();
    final List<String> names = new ArrayList<>(bootstrapUnreconciledDatabases);
    Collections.sort(names);
    return names;
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
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null || server == null || raftHA.isLeader())
      return;
    ensureBootstrapBaselinesLoaded();
    if (bootstrapUnreconciledDatabases.isEmpty())
      return;

    // Same two questions the other HealthMonitor-driven backstop asks before burning a throttle slot
    // (issue #6202): the address must identify a single peer and must not be our own.
    final String leaderHttpAddr = raftHA.getUnambiguousPeerHttpAddress(raftHA.getLeaderId());
    if (leaderHttpAddr == null || raftHA.isOwnHttpAddress(leaderHttpAddr))
      return; // no leader to compare against yet

    // Floored at the snapshot cadence so a WAN cluster that has widened its watchdog does not get probed
    // more often than it resyncs.
    if (!claimBootstrapDivergenceCheckSlot(System.currentTimeMillis(),
        Math.max(BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS, computeSnapshotWatchdogTimeoutMs())))
      return;

    final Set<String> pending = new HashSet<>(bootstrapUnreconciledDatabases);
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
      lifecycleExecutor.submit(() -> {
        final Map<String, BootstrapBaseline> leaderStates = BootstrapElection.fetchBootstrapState(
            leaderHttpAddr, clusterToken, pending, BOOTSTRAP_DIVERGENCE_PROBE_TIMEOUT_MS);
        if (leaderStates == null) {
          // The throttle slot is spent whether or not the probe answered, exactly as the stale-snapshot
          // backstop spends its own on a failed attempt: the next try is the next check window, not the
          // next health tick. Said plainly here so the wait is not read as seconds. The mark stays raised
          // in the meantime, so a failed probe never retires an alert.
          LogManager.instance().log(this, Level.INFO,
              "Could not verify bootstrap divergence of %s against leader %s; the divergence stays reported "
                  + "and the check is retried in the next window (>= %d ms)",
              pending, leaderHttpAddr, BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS);
          return;
        }
        reconcileBootstrapDivergence(leaderStates);
      });
    } catch (final RejectedExecutionException ree) {
      LogManager.instance().log(this, Level.WARNING,
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
    for (final String dbName : getBootstrapUnreconciledDatabases()) {
      final BootstrapBaseline leaderState = leaderStates.get(dbName);
      if (leaderState == null) {
        LogManager.instance().log(this, Level.INFO,
            "Bootstrap divergence of '%s' could not be verified: the leader reported no state for it", dbName);
        continue;
      }
      if (server == null || !server.existsDatabase(dbName)) {
        // Not currently loaded on this node. Deliberately neither cleared nor opened: existsDatabase
        // answers "is it in the registry", not "is it on disk", so retiring the alert here would drop a
        // real divergence for a database that is merely closed - and opening one just to fingerprint it
        // would undo an operator's decision to leave it closed. A database actually dropped loses its
        // mark with its baseline, on the DROP entry.
        continue;
      }
      final BootstrapBaseline local;
      try {
        local = readLocalBootstrapState(dbName);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Could not read local state of '%s' to verify bootstrap divergence: %s", dbName, e.getMessage());
        continue;
      }
      if (local == null)
        continue; // not a LocalDatabase: nothing to fingerprint, leave the mark alone

      if (leaderState.fingerprint() != null && leaderState.fingerprint().equals(local.fingerprint())) {
        clearBootstrapUnreconciled(dbName);
        LogManager.instance().log(this, Level.INFO,
            "Database '%s' is no longer diverged from the cluster: its content now matches the leader "
                + "(fingerprint=%s, lastTxId=%d). Clearing the bootstrap-divergence alert.",
            dbName, BootstrapElection.abbreviate(local.fingerprint()), local.lastTxId());
        continue;
      }

      LogManager.instance().log(this, Level.SEVERE,
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
    writePersistedAppliedIndexForAllDatabases(index, Set.of());
  }

  /**
   * Same, minus {@code excluded}: the databases a snapshot install gave up on are not at {@code index}, so recording
   * them there would make {@link #reinitialize()} skip exactly the replay that would have caught them up on the next
   * restart, and would silently launder a stale copy into "applied" (issue #6760). The GLOBAL position still
   * advances: it is the Raft-log position Ratis is being told about, and Ratis has been told.
   */
  void writePersistedAppliedIndexForAllDatabases(final long index, final Set<String> excluded) {
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      globalAppliedIndex = index;
      if (server != null)
        for (final String dbName : server.getDatabaseNames())
          if (!excluded.contains(dbName))
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
      // The overwrite-guard mark lives in the same file entry, so a dropped database must lose both or
      // the alert would outlive the database it names (issue #6124).
      final boolean wasUnreconciled = bootstrapUnreconciledDatabases.remove(dbName);
      if (bootstrapBaselines.remove(dbName) != null || wasUnreconciled)
        persistBootstrapBaselinesFile();
    }
  }

  /**
   * Durably records that {@code dbName} took the bootstrap "local is fresher, refuse to overwrite"
   * branch and is therefore diverged from the rest of the cluster (issue #6124). Written under the same
   * lock as the baselines because it is persisted in the same file entry; the caller has already
   * recorded the baseline, so the entry exists.
   */
  private void markBootstrapUnreconciled(final String dbName) {
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
   * {@link #reconcileBootstrapDivergence} when the two copies are confirmed identical. Idempotent, and
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
   * Clears every bootstrap-divergence mark (issue #6124). Used by the two full-resync paths, which
   * reinstall EVERY database present on this node from the leader - the same reasoning
   * {@link #clearDivergedState()} makes for the diverged set.
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
        // Written only when set, so a healthy cluster's file keeps exactly the shape it had before
        // issue #6124 and an older build reading it simply ignores the extra key.
        if (bootstrapUnreconciledDatabases.contains(e.getKey()))
          entry.put("unreconciled", true);
        json.put(e.getKey(), entry);
      }
      // Iterating the baselines alone is sufficient for the marks too: a mark is only ever added right
      // after its baseline was recorded, and the loader refuses one on an entry that carries no
      // fingerprint, so a marked database without a baseline cannot exist in memory to be missed here.
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

  /**
   * Answers whether a snapshot resync from {@code leaderId} may be attempted, and with which address.
   * <p>
   * Every resync path asks it - the manual {@link #triggerSnapshotDownload()}, the targeted
   * {@code triggerDatabaseResync(String)} and the Ratis-initiated {@link #notifyInstallSnapshotFromLeader} -
   * because a resync that pulls from the wrong node is not recoverable and not even visible: the reconcile
   * succeeds, the install is recorded, and the node returns to the ready set carrying whatever it copied. The
   * Ratis-initiated path had none of these checks at all (issue #6202), and duplicating them would have made a
   * fourth hand-maintained copy of a rule the first three already disagreed about.
   * <p>
   * The refusal that is specific to a resync is made here; the rest is the general question "may this node dial
   * that one, and at which address?", which {@link PeerDialAddress#resolve} answers for every caller that acts on
   * a resolved peer address unattended (issue #6221):
   * <ul>
   * <li><b>This node is the leader.</b> A node cannot repair itself from itself. {@code notifyLeaderChanged()}
   * submits a resync unconditionally - including on the node that just WON the election - and the leader address
   * then resolves to this node's own, so the "download" would copy this node's already-incomplete databases back
   * onto themselves and report success. That is merely pointless on some paths, but it lets
   * {@link #resolveStaleSnapshotFloorAfterResync} durably record the marker index as applied and drop the read
   * floor, re-opening issue #6111 and surviving restarts. A role check rather than an identity one: it fires
   * before the address is even resolved, and it is the one refusal that is about what this node <em>is</em>
   * rather than about where the other one lives.</li>
   * <li><b>Everything {@link PeerDialAddress#resolve} refuses</b> - an unknown leader, an address that identifies
   * no single peer (issue #6202), an address that is this node's own (issue #6191). The last is also the backstop
   * for the leader-role check above: leadership can move between the two, and {@code getLeaderId()} can already
   * report this node while {@code isLeader()} has not caught up.</li>
   * </ul>
   * Refusing leaves the node visibly behind - the floor stands, {@link #isResyncInProgress()} keeps it out of the
   * ready set, reads keep failing honestly - which is the state it is actually in. Ratis retries the install and
   * the {@link HealthMonitor} re-arms the manual path, so a refusal is not a dead end either.
   */
  private PeerDialAddress resolveSnapshotSource(final RaftPeerId leaderId) {
    // Read once into a local: the field is volatile and a teardown can null it between two reads, which would
    // turn a refusal into a NullPointerException on the install path.
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null)
      return PeerDialAddress.refuse("the HA server is not available on this node");

    if (raftHA.isLeader())
      return PeerDialAddress.refuse("this node is the leader, so there is no peer to pull from. "
          + "The request stays pending until leadership moves elsewhere (issue #6111)");

    return PeerDialAddress.resolve(raftHA, leaderId, "leader");
  }

  /**
   * The leader's HTTP address when a snapshot may be pulled from it, {@code null} when it may not. The
   * supplier-shaped form of {@link #resolveSnapshotSource}, for the {@code SnapshotInstaller.install} overloads
   * that re-resolve the address on every download attempt.
   * <p>
   * Guarding the call site once is not enough for those: the whole reason they take a supplier is that leadership
   * can move mid-operation, and an up-front check says nothing about the address attempt 3 will resolve. Every
   * attempt asks again (issue #6202).
   * <p>
   * This is the arm that logs the refusal - see {@link #guardedLeaderHttpsAddress()} for why that is exactly one
   * line per attempt whether or not SSL is on.
   */
  private String guardedLeaderHttpAddress() {
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null)
      return null;
    final PeerDialAddress source = resolveSnapshotSource(raftHA.getLeaderId());
    if (source.refused()) {
      LogManager.instance().log(this, Level.WARNING, "Refusing to pull a snapshot: %s", source.refusal());
      return null;
    }
    return source.httpAddress();
  }

  /**
   * The leader's HTTPS address under the same guard, or {@code null}. Needed as well as the HTTP arm because
   * {@code downloadWithRetry} prefers the HTTPS endpoint when SSL is enabled and only falls back to the HTTP one
   * when it comes back null - so guarding HTTP alone would leave the guard unreachable on an SSL cluster.
   * <p>
   * Silent by design, which is what keeps the pair to one log line per attempt: a refusal makes this return null,
   * and the caller then consults the HTTP arm, which logs. When it does NOT refuse there is nothing to log, and
   * the HTTP arm is not consulted at all.
   * <p>
   * The HTTPS endpoint is the guard's own, not the raw resolver's: it is read from a different field of
   * {@code HA_SERVER_LIST} than the HTTP one and derives onto a different local port, so a cluster that declares
   * distinct {@code http} ports and omits the {@code https} ones passes the HTTP check while every peer's HTTPS
   * endpoint still resolves to this node (issue #6221). Withheld, it returns null here and the download falls back
   * to the guarded HTTP endpoint, which is the route an unresolvable HTTPS endpoint has always taken.
   */
  private String guardedLeaderHttpsAddress() {
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null)
      return null;
    return resolveSnapshotSource(raftHA.getLeaderId()).httpsAddress();
  }

  // @VisibleForTesting
  void triggerSnapshotDownload() {
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
      // A leader-initiated install holds the lock without owning the flag when it lost the CAS, so the flag alone
      // does not prove nothing is running. Folded rather than awaited: this runs on the single-threaded
      // lifecycleExecutor, which must not be parked for the length of a download (issue #6202).
      if (!snapshotDownloadLock.tryLock()) {
        LogManager.instance().log(this, Level.INFO,
            "Snapshot resync already in progress (leader-initiated install); folding this request into it");
        return;
      }
      try {
        final PeerDialAddress source = resolveSnapshotSource(raftHAServer.getLeaderId());
        if (source.refused()) {
          LogManager.instance().log(this, Level.WARNING, "Refusing a snapshot resync: %s", source.refusal());
          return;
        }
        downloadAllDatabasesFrom(source);
      } finally {
        snapshotDownloadLock.unlock();
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Snapshot resync failed", e);
      // The node is still short of the entries the marker claims, so the read floor stays and the
      // request is re-armed rather than left cleared: a later leader change picks it up. The retry that
      // does NOT depend on an election is retryUnfilledSnapshotGap(), driven by the HealthMonitor tick -
      // note that recoverFromPersistentLag() cannot serve here, because the re-armed flag makes
      // isSnapshotDownloadPending() true and both it and isFollowerLaggingBeyond() then stand down
      // (issue #6111).
      if (staleSnapshotAppliedFloor.get() >= 0)
        needsSnapshotDownload.set(true);
    } finally {
      snapshotDownloadInProgress.set(false);
    }
  }

  /**
   * Reinstalls every present database from {@code source} and resolves the state a full resync clears. Takes the
   * whole verdict rather than one address so the encrypted endpoint reaching the installer is the guarded one
   * too (issue #6221). The caller has already established that it may be pulled from
   * ({@link #resolveSnapshotSource}) and holds {@link #snapshotDownloadLock}.
   */
  private void downloadAllDatabasesFrom(final PeerDialAddress source) throws IOException {
    final String leaderHttpAddr = source.httpAddress();
    final String leaderHttpsAddr = source.httpsAddress();
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
    // Every present database now carries the leader's copy, including any the bootstrap overwrite guard
    // had kept (issue #6124).
    clearAllBootstrapUnreconciled();
    // The databases now carry the leader's state, so a read floor published by a stale marker in
    // reinitialize() is satisfied. Record the marker index as the persisted applied position too:
    // without it the very same gap is re-detected on the next restart and the node re-downloads
    // forever. Then wake the waiters this resync unblocked (issue #6111).
    resolveStaleSnapshotFloorAfterResync(resynced);
  }

  /**
   * Completes a successful full resync with respect to the stale-snapshot read floor (issue #6111):
   * persists the marker index as the applied position of every present database - the resync brought
   * them all to it, and leaving the persisted value behind makes {@link #reinitialize()} re-detect the
   * same gap on the next restart - then clears the floor and wakes the waiters it was holding back.
   * No-op when no floor was outstanding.
   * <p>
   * {@code resynced} is logged rather than gated on: zero is legitimate for a node with no databases
   * open (nothing can be stale), and it matches what {@code notifyInstallSnapshotFromLeader} already
   * records over the same {@code getDatabaseNames()} set. It is worth seeing in the log, because a
   * floor resolved after reinstalling zero databases is the shape an unexpectedly-empty registry would
   * take.
   */
  private void resolveStaleSnapshotFloorAfterResync(final int resynced) {
    if (staleSnapshotAppliedFloor.get() < 0)
      return;
    LogManager.instance().log(this, Level.INFO,
        "Stale-snapshot read floor resolved after reinstalling %d database(s); reads are unclamped again", resynced);
    final var snapshotInfo = storage.getLatestSnapshot();
    if (snapshotInfo != null && snapshotInfo.getIndex() > readPersistedAppliedIndex()) {
      writePersistedAppliedIndexForAllDatabases(snapshotInfo.getIndex());
      // Accumulate rather than set: this runs on the lifecycleExecutor while the Ratis apply thread may
      // already have replayed past the marker, and the counter must never regress under it.
      lastAppliedIndex.accumulateAndGet(snapshotInfo.getIndex(), Math::max);
    }
    clearStaleSnapshotFloor();
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA != null)
      raftHA.notifyApplied();
  }

  /**
   * Drops the stale-snapshot read floor. Called only where a resync has actually restored the local
   * state up to the marker - never when one is merely requested or in flight (issue #6111).
   */
  // @VisibleForTesting
  void clearStaleSnapshotFloor() {
    staleSnapshotAppliedFloor.set(-1);
    lastStaleSnapshotRetryMs.set(0);
  }

  /**
   * Periodic backstop for an unfilled stale-snapshot gap (issue #6111), driven by the
   * {@link HealthMonitor} tick. Re-submits {@link #triggerSnapshotDownload()} while a read floor is
   * still outstanding and no download is running, so a node whose first attempt failed (leader HTTP
   * port unreachable while Raft gRPC is fine, disk full, leader not yet known) recovers on its own
   * instead of staying clamped and not-ready until the next leader election.
   * <p>
   * The existing stale-follower backstop cannot cover this: it is driven by
   * {@code commitIndex - appliedIndex}, and Ratis derives the applied index from the very marker that
   * is ahead, so a node with an open gap reports zero lag. {@link #recoverFromPersistentLag()} would
   * also refuse anyway, because {@link #isSnapshotDownloadPending()} stays true while the request is
   * re-armed.
   * <p>
   * Throttled to one attempt per {@link #computeSnapshotWatchdogTimeoutMs()} so a persistently failing
   * download is not retried on every tick: a full resync pulls every database from the leader, and the
   * HealthMonitor ticks far more often than that costs.
   */
  public void retryUnfilledSnapshotGap() {
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null || server == null || raftHA.isLeader())
      return;
    final long floor = staleSnapshotAppliedFloor.get();
    // A per-database floor is an unfilled gap too: it is published exactly when an install gave up on a database,
    // which is the case that otherwise never re-arms anything (issue #6760). Same throttle, same single-flight.
    if (floor < 0 && staleDatabaseAppliedFloors.isEmpty())
      return; // no unfilled gap
    if (snapshotDownloadInProgress.get())
      return; // one is genuinely running; it will clear the floor or re-arm the request
    // The same three questions resolveSnapshotSource() asks, so this cheap precheck cannot pass a request the
    // resync would then refuse and burn a throttle slot doing it (issue #6202): the leader role is checked at the
    // top of this method, and the resolved address must both identify a single peer and not be our own. Asked
    // through the same two helpers rather than restated, and without resolveSnapshotSource() itself, whose
    // unresolvable-local-address WARNING belongs to an attempt rather than to a HealthMonitor tick. When the
    // local address cannot be resolved isOwnHttpAddress answers false, so the request goes through and that
    // warning is emitted once by the resync, which is where it is actionable.
    final String leaderHttpAddr = raftHA.getUnambiguousPeerHttpAddress(raftHA.getLeaderId());
    if (leaderHttpAddr == null || raftHA.isOwnHttpAddress(leaderHttpAddr))
      return; // nowhere to download from yet; notifyLeaderChanged() drives the first attempt

    final long now = System.currentTimeMillis();
    final long retryIntervalMs = computeSnapshotWatchdogTimeoutMs();
    final long previous = lastStaleSnapshotRetryMs.get();
    if (previous != 0 && now - previous < retryIntervalMs)
      return;
    if (!lastStaleSnapshotRetryMs.compareAndSet(previous, now))
      return; // another tick won the throttle slot

    LogManager.instance().log(this, Level.WARNING,
        "Local state is still behind what the snapshot marker claims (read floor=%d, databases short of the "
            + "snapshot index=%s) with no download in flight: retrying the resync from the leader "
            + "(issues #6111, #6760)", floor, staleDatabaseAppliedFloors.keySet());
    try {
      lifecycleExecutor.submit(this::triggerSnapshotDownload);
    } catch (final RejectedExecutionException ree) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot schedule the stale-snapshot resync retry: executor is shut down", ree);
    }
  }

  /**
   * Highest Raft-log index whose data is genuinely present in the local databases while a flagged
   * stale-snapshot re-download is outstanding, or {@code -1} when none is (the normal case). Consumed by
   * {@link RaftHAServer#getTrustedAppliedIndex()} to clamp the LINEARIZABLE / READ_YOUR_WRITES apply
   * waiters, which would otherwise trust the marker index Ratis reports as applied (issue #6111).
   */
  public long getStaleSnapshotAppliedFloor() {
    return staleSnapshotAppliedFloor.get();
  }

  /**
   * Highest Raft-log index whose data is genuinely present in {@code dbName}, or {@code -1} when this database is
   * not known to be behind (the normal case). Consumed by
   * {@link RaftHAServer#getTrustedAppliedIndex(String)} so a read of a database a snapshot install gave up on is
   * clamped, while its healthy co-located databases are not (issue #6760).
   */
  public long getDatabaseAppliedFloor(final String dbName) {
    if (dbName == null)
      return -1;
    final Long floor = staleDatabaseAppliedFloors.get(dbName);
    return floor != null ? floor : -1;
  }

  /**
   * Records that a snapshot install completed WITHOUT bringing {@code databases} to {@code snapshotIndex}
   * (issue #6760).
   * <p>
   * Each one keeps its diverged mark - so the node does not advertise readiness while it holds a copy it knows is
   * behind - and publishes a read floor at its own honest applied position, so a LINEARIZABLE or read-your-writes
   * read targeting it fails or degrades instead of being served from the stale copy. Recovery is the existing
   * machinery: the mark keeps {@link #isResyncInProgress()} true, and {@link #retryUnfilledSnapshotGap()} re-drives
   * the resync on the HealthMonitor tick until the database is refreshed for real.
   */
  private void markDatabasesNotAtSnapshotIndex(final Set<String> databases, final long snapshotIndex) {
    for (final String dbName : databases) {
      // The persisted position was deliberately NOT advanced for this database above, so it still carries whatever
      // this node genuinely applied. -1 (never recorded) clamps to 0, which is the honest answer for a database
      // nothing is known about.
      final long floor = Math.max(0L, readPersistedAppliedIndex(dbName));
      staleDatabaseAppliedFloors.put(dbName, floor);
      markStateDiverged(dbName);
      LogManager.instance().log(this, Level.SEVERE,
          "Snapshot install did not bring database '%s' to snapshotIndex=%d: keeping it marked diverged and "
              + "clamping its LINEARIZABLE / read-your-writes reads at appliedIndex=%d until a resync succeeds. "
              + "The other databases on this node are unaffected. Check the leader's copy of '%s'.",
          dbName, snapshotIndex, floor, dbName);
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
          if (!snapshotDownloadLock.tryLock()) {
            HALog.log(this, HALog.BASIC,
                "Snapshot download already in progress (leader-initiated install), skipping targeted resync of '%s'",
                dbName);
            return;
          }
          try {
            // Same refusals as the two full-resync paths, through the same helper: a targeted resync reinstalls a
            // whole database from the resolved address, so an address naming this node or the wrong peer does the
            // same durable damage here (issue #6202).
            final PeerDialAddress source = resolveSnapshotSource(raftHAServer.getLeaderId());
            if (source.refused()) {
              LogManager.instance().log(this, Level.WARNING,
                  "Refusing a targeted snapshot resync of quarantined database '%s': %s", dbName, source.refusal());
              return;
            }
            final String leaderHttpAddr = source.httpAddress();
            final String leaderHttpsAddr = source.httpsAddress();
            final String clusterToken = raftHAServer.getClusterToken();
            // install() keeps the database open during the download and rolls back on failure, so a
            // targeted resync never leaves it closed.
            if (server.existsDatabase(dbName)) {
              SnapshotInstaller.install(dbName, SnapshotInstaller.resolveDatabasePath(server, dbName),
                  leaderHttpAddr, leaderHttpsAddr, clusterToken, server);
              LogManager.instance().log(this, Level.INFO,
                  "Targeted snapshot resync of quarantined database '%s' completed", dbName);
              clearDivergedDatabase(dbName);
              clearBootstrapUnreconciled(dbName);
            }
          } finally {
            snapshotDownloadLock.unlock();
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
    // The resync restored this database, so its read floor is satisfied (issue #6760).
    staleDatabaseAppliedFloors.remove(dbName);
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
    // A resync reinstalls every database, so every per-database read floor is satisfied too (issue #6760). The
    // snapshot-install path re-publishes the floors of the databases it could NOT reinstall right after calling
    // this, so clearing wholesale here stays correct.
    staleDatabaseAppliedFloors.clear();
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
   * <p>
   * An outstanding stale-snapshot read floor counts too (issue #6111): after a failed download neither
   * of the other two flags is set, yet the node is still missing the entries the snapshot marker claims
   * and must not advertise itself as ready.
   */
  public boolean isResyncInProgress() {
    return isSnapshotDownloadPending() || !divergedDatabases.isEmpty() || staleSnapshotAppliedFloor.get() >= 0
        || !staleDatabaseAppliedFloors.isEmpty();
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
    snapshotInstallExecutor.shutdownNow();
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
