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
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PageVersionReservations;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.timeseries.TimeSeriesSealedInstallLock;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.exception.ConcurrentModificationException;
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
import com.arcadedb.server.ha.raft.ratis.RatisSnapshotDigestWarningFilter;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ReplicatedSecurityConfigPersistenceException;
import com.arcadedb.server.security.ReplicatedUsersPersistenceException;
import com.arcadedb.server.security.SecurityGroupFileRepository;
import com.arcadedb.server.security.SecurityUserFileRepository;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.zip.CRC32;

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
 *   <li>{@code SECURITY_GROUPS_ENTRY} - replicate the group document across the cluster (issue #7373)</li>
 *   <li>{@code SECURITY_API_TOKENS_ENTRY} - replicate the API-token document across the cluster (issue #7373)</li>
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
 * <b>Crash recovery:</b> {@link SnapshotInstaller#recoverPendingSnapshotSwaps} is called from
 * {@link #initialize} to complete or roll back any snapshot installations that were interrupted by a
 * process crash. Not startup-only despite the name: {@code RaftHAServer.restartRatis} rebuilds the state
 * machine and calls {@link #initialize} again on every runtime Ratis restart it drives, with the server
 * still ONLINE and able to have its own live {@link SnapshotInstaller#install} in flight for the very
 * database this pass is scanning - {@link SnapshotInstaller#recoverPendingSnapshotSwaps} skips any
 * database an in-flight install is already holding rather than racing it (issue #7128).
 */
public class ArcadeStateMachine extends BaseStateMachine {

  /**
   * What {@link #applyTransaction} answers the Ratis client when a node-scoped security entry was NOT installed
   * because its compare-and-set precondition no longer held (issue #7509). Anything else - "OK", or no message
   * at all from a leader that predates this - means the entry was applied.
   * <p>
   * The verdict has to travel in the REPLY rather than be observed locally: the submitting node is not
   * necessarily the leader, and a follower's own apply of the entry can lag the reply it gets back. The reply
   * carries the LEADER's verdict, which is the authoritative one because applies are ordered and deterministic.
   */
  public static final String SECURITY_ENTRY_SUPERSEDED_REPLY = "SECURITY_ENTRY_SUPERSEDED";

  /**
   * Test-only WAL gap counter. When non-null, incremented each time a follower detects a
   * WAL page-version gap. Used by deterministic tests to verify no gap occurred.
   * <p>
   * Tests that set this MUST reset it to {@code null} in an {@code @AfterEach} method, otherwise
   * it leaks into subsequent tests in the same JVM.
   */
  public static volatile AtomicInteger TEST_WAL_GAP_COUNTER = null;

  /**
   * Test-only recorder of the PUBLISHING schema entries applied on a follower (issue #6990). When non-null, every
   * {@code SCHEMA_ENTRY} that tells the follower to reload its schema is recorded - the instalment/split chunks that
   * only deliver pages are deliberately not, since they are not the unit a DDL batch is measured in.
   * <p>
   * Records ENTRY INDEXES rather than counting occurrences, because {@code applyWithRetry} can re-run an apply that
   * failed: a plain counter would report a retried entry twice and turn an exact assertion into a flake.
   * <p>
   * Tests that set this MUST reset it to {@code null} in an {@code @AfterEach} method, otherwise it leaks into
   * subsequent tests in the same JVM.
   */
  public static volatile SchemaEntryRecorder TEST_SCHEMA_ENTRY_COUNTER = null;

  /**
   * See {@link #TEST_SCHEMA_ENTRY_COUNTER}. Deduplicating by Raft entry index is what makes
   * {@link #count()} the number of schema entries the leader PUBLISHED, rather than the number of times this node
   * happened to apply one.
   */
  public static final class SchemaEntryRecorder {
    private final Set<Long> appliedIndexes = ConcurrentHashMap.newKeySet();

    /**
     * Number of distinct publishing schema entries applied so far.
     */
    public int count() {
      return appliedIndexes.size();
    }

    private void record(final long entryIndex) {
      appliedIndexes.add(entryIndex);
    }
  }

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

  /**
   * The first persistent Raft log write failure Ratis reported through {@link #notifyLogFailed}, or {@code null}
   * while the log writer is healthy (issue #7037). Once the segmented log worker hits an I/O error - {@code No
   * space left on device} being the reported one - Ratis marks the log failed at that index and fails every later
   * append with {@code RaftLogIOException: Log already failed at index N}, while the division stays {@code RUNNING}:
   * no lifecycle state, lag or divergence check can see it, so the state machine keeps the mark and the
   * {@link HealthMonitor} restarts the server in place once the volume has room again. Set once per state-machine
   * lifetime: a restart builds a fresh state machine, which is what clears it.
   */
  private volatile RaftLogFailure raftLogFailure;
  /**
   * The Ratis {@code StateMachineUpdater} thread, recorded on every apply so callers that reach
   * {@link SnapshotInstaller} from an entry apply can tell they are on it (issue #7037): a snapshot request
   * blocks until that same thread takes it, so it must not be issued from there.
   */
  private volatile Thread         applyThread;

  /** A persistent Raft log write failure: the failed entry's index ({@code -1} for a whole segment) and the cause. */
  public record RaftLogFailure(long index, String cause, long timestamp) {
    /** One-line description for logs and the health monitor. */
    public String describe() {
      return (index >= 0 ? "at index " + index : "on a log segment") + ": " + cause;
    }
  }

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
   * Seeds the cluster security documents from the leader whenever a configuration change brings in a peer,
   * whichever admission path issued it (issue #7531). See {@link MembershipSecuritySeeder} for why the leader
   * and not the admitting node, and {@link #notifyConfigurationChanged} for the callback that drives it.
   * <p>
   * Not final so a test can substitute a recording seeder; production never replaces it.
   */
  private volatile MembershipSecuritySeeder membershipSecuritySeeder = new MembershipSecuritySeeder(
      this::isLocalNodeRaftLeader, this::securitySeedRetryBudgetMs, this::seedSecurityStateClusterWide);

  /**
   * Records whether this node was added to the Raft configuration while running, which is what arms the
   * security-convergence readiness gate (issue #7819). Replaced by {@link RaftHAServer} with the instance it
   * owns, so the answer survives an in-place Ratis restart; the default keeps a state machine with nothing wired
   * to it - every peer of the {@code MiniRaftCluster} harness - recording on its own.
   */
  private volatile RuntimeJoinDetector runtimeJoinDetector = new RuntimeJoinDetector();

  /**
   * Brings THIS node's security documents back in step when it rejoined without a membership change, or caught
   * up by a snapshot install that carried none of them (issue #7833). See {@link SecurityCatchUp}.
   */
  private final SecurityCatchUp securityCatchUp = new SecurityCatchUp();

  /**
   * The catch-up this node's own triggers drive. Package-private so a test in this package can put it in the
   * state a restart during a failover leaves it in and then watch what the real leader change does with it
   * (issue #8034); nothing in production reaches it any other way than through the two callbacks below.
   */
  SecurityCatchUp getSecurityCatchUp() {
    return securityCatchUp;
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

  /**
   * Databases whose directory {@link #installFromLeaderForBootstrap} is replacing from the leader's snapshot
   * right now (issue #7519).
   * <p>
   * The window it names is the one the bootstrap protocol opens on every peer that did not source the baseline:
   * from the moment this node decides its copy is not the cluster's, until the leader's copy is on disk. For
   * most of it the local copy is still OPEN and SERVING - {@code SnapshotInstaller.install} downloads before it
   * touches the live files, deliberately, so a failed download costs no availability - and the node's own
   * {@code snapshotInstallInProgress} 503 window covers only the file swap at the end of it, and only on HTTP.
   * So a client reaching this node during the download is served from a copy the cluster has already decided
   * against, on every protocol, with nothing anywhere on the request path saying so.
   * <p>
   * Registered by {@link #installFromLeaderForBootstrap} around the install and read by
   * {@link #bootstrapWindowReason()}, which is what takes the node out of the Kubernetes Service for the
   * duration.
   * <p>
   * <b>A depth per database, not a set</b> (review of PR #7964), and for the same reason
   * {@link SnapshotInstaller}'s own {@code INSTALLS_IN_FLIGHT} is one: with a set, two overlapping installs of
   * the same database would have the FIRST {@code finally} to run drop the name while the second was still
   * moving files, and the node would report itself ready in the middle of a directory replacement - silently,
   * with no log line and nothing a test would catch. The four production callers of
   * {@code installFromLeaderForBootstrap} sit on two single-threaded executors (the Ratis apply thread for the
   * two {@code applyBootstrapFingerprintEntry} arms, the {@code lifecycleExecutor} for
   * {@code retryBootstrapInstall} and {@code retryMissingBootstrapDatabase}), so neither pair can race itself
   * and an apply/lifecycle overlap needs a replayed baseline to meet a live retry for the same database. That
   * is narrow rather than impossible, and proving it impossible across two pools is worth less than the six
   * lines that make it not matter.
   */
  private final ConcurrentHashMap<String, Integer> bootstrapInstallsInFlight = new ConcurrentHashMap<>();

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
  //
  // Holds WHAT tripped it, not just that something did (issue #7872). The halt is now published in
  // GET /api/v1/cluster, and the index and reason are what tell an operator whether the answer is "upgrade this
  // node" (a newer peer committed an entry type it cannot decode) or "file a bug" (an unexpected apply error).
  // Set with compareAndSet so the FIRST halt is the one recorded, like raftLogFailure: every later apply is
  // refused by the guard below, and a cascade of follow-on failures would otherwise overwrite the cause.
  private final AtomicReference<CriticalHalt> haltedAfterCriticalError = new AtomicReference<>();

  /**
   * What tripped the node-wide critical halt (issue #7872).
   *
   * @param index     the Raft log index being applied when it tripped, or -1 when there was none
   * @param reason    one line naming the condition, in the vocabulary an operator can act on
   * @param timestamp when it tripped, as epoch milliseconds
   */
  public record CriticalHalt(long index, String reason, long timestamp) {
    /** One-line description for logs and the cluster status alert, the shape {@link RaftLogFailure#describe()} uses. */
    public String describe() {
      return (index >= 0 ? "at index " + index : "on an entry with no index") + ": " + reason;
    }
  }

  // Database names whose state has diverged from the committed Raft log (a WALVersionGapException
  // was detected while applying an entry for them). While a database is in this set, unexpected
  // Throwables in applyWithRetry for THAT database are wrapped as ReplicationException (recoverable
  // resync) instead of propagating to the fatal server-halt path (issue #4740): operating on
  // inconsistent page state after a WAL gap often throws NPE, ClassCastException, or similar errors
  // that would otherwise halt the server even though the node is merely waiting for a snapshot
  // resync. Scoped per-database so a gap in one database never masks a genuine bug raised while
  // applying an entry for an unrelated, healthy database. Cleared when a snapshot resync completes
  // (it resyncs all databases) and restores consistent state.
  // Keyed by database name, valued by WHY it was quarantined (issue #7741): a WAL version gap is a replication
  // problem and an undecodable local entry is a corrupt log segment on THIS node, and an operator told only the
  // first is sent to look at the leader for a bad disk of their own. The map is the set - keySet() is what every
  // membership test reads - so the cause cannot go missing or outlive the quarantine it describes.
  // DURABLE since issue #7735: mirrored into the "quarantine" object of .raft/applied-index by
  // quarantineDatabase() / clearDivergedDatabase() / clearDivergedState() and read back by
  // ensureAppliedIndexLoaded(). It has to be, because a quarantine deliberately leaves lastAppliedIndex on the
  // entry it skipped while every LATER entry advances it - so an in-memory-only mark meant a restart came back
  // RUNNING, 200 on /api/v1/ready, alerts:[] and permanently short of a committed mutation.
  private final Map<String, DivergenceCause> divergedDatabases = new ConcurrentHashMap<>();

  // Raised by close(), read by persistAppliedIndexFile(): a lifecycle task that is already past its last
  // interruption point when shutdownNow() lands must not recreate .raft/applied-index under a directory the
  // shutdown is removing (issue #7735). A closed state machine is never reused - RaftHAServer.restartRatis()
  // builds a new one - so nothing is lost by refusing the write. Written under appliedIndexFileLock, which every
  // writer holds across its whole check-and-write, so raising it also waits out a write already in flight.
  private volatile boolean closed;

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

  // Budget for taking the compaction write lock of the shards a sealed-store entry installs (issue #7337). The
  // only holders of the read half on a follower are a backup or a snapshot ship, both of which release it the
  // moment the sealed images have been read, so this is sized as a hang detector rather than as a queue: a wait
  // longer than this means something is not releasing, and failing the apply loudly is better than installing
  // a sealed store that a copy in flight can pair with the wrong page image.
  private static final long              SEALED_INSTALL_LOCK_TIMEOUT_MS    = 120_000L;

  // The transactions this node originated that are in flight between replication and the publication of their
  // pages, and the page versions the Raft log has assigned but this node has not applied yet (issue #6965).
  // Together they give the leader the same page-write order as every follower - its own entries are published at
  // their log position by the apply thread - and let it refuse, before the entry enters the log, a transaction that
  // was validated against a page version the log has already moved past. See LocalCommit and PageVersionLedger.
  private final LocalCommitRegistry localCommits = new LocalCommitRegistry();
  private final PageVersionLedger   pageVersions = new PageVersionLedger();

  /**
   * What {@link #preAppendTransaction} learned about a client entry, handed to {@link #applyTransaction} through the
   * Ratis transaction context so the entry is decoded once: whether this node's own client submitted it, and the
   * decoded payload.
   */
  private record AppendedEntry(boolean originatedLocally, RaftLogEntryCodec.DecodedEntry decoded, PageVersionLedger.EntryId entryId,
                               PageVersionLedger.Pages pages) {
  }


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
    // From here on this state machine writes zero-byte snapshot markers with no .md5 companion by
    // design (see registerSnapshotMarker), which makes Ratis warn once per marker on every checkpoint
    // and every restart. Silence just that one message before the storage that emits it is opened
    // (issue #6991). Idempotent, so the RaftHAServer.start() call and this one cannot stack.
    // Still needed after #7209 stopped ArcadeDB calling cleanupOldSnapshots() itself: the warn loop
    // lives in that method, and Ratis's own StateMachineUpdater calls it after every snapshot
    // (ratis-server 3.3.0, StateMachineUpdater.java:301).
    RatisSnapshotDigestWarningFilter.install();
    storage.init(raftStorage);
    // A node upgrading to the #7209 fix still carries every marker its earlier checkpoints left
    // behind. Drop them here so the directory scan starts bounded even on a node that never
    // checkpoints again.
    pruneSnapshotMarkersAtStartup();
    reinitialize();
    // Recover any snapshot installations that were interrupted by a crash
    if (server != null) {
      final String dbDir = server.getConfiguration().getValueAsString(
          GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
      if (dbDir != null) {
        final Path databasesDirectory = Path.of(dbDir);
        // The server is passed so the repair can take each database's maintenance slot while it moves its
        // files, the same slot an install takes (issue #7449). This runs again on every HealthMonitor-driven
        // Ratis restart, i.e. with the server ONLINE and a scheduled backup able to be in flight.
        SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDirectory, server);
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
      // durability checkpoint it hands Ratis, and it must not claim entries this node never applied.
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
   * can identify entries that were originated by this node in the current lifecycle, without relying
   * on a runtime {@code isLeader()} check that is susceptible to TOCTOU races if leadership changes
   * between submission and apply. For a transaction entry the marker tells the apply thread to look
   * for the committing thread's prepared pages and publish those (issue #6965); for a schema entry it
   * tells it to skip, because the leader applied the change locally under the database write lock.
   * A transaction entry is also validated here against the page versions the log assigned so far, and
   * refused through the context when it was validated against a superseded one (see {@link PageVersionLedger}).
   * <p>
   * Only requests submitted by THIS node's own {@code RaftClient} are marked as locally-originated.
   * Requests forwarded from a follower's {@code RaftClient} carry a different {@code ClientId} and
   * must NOT be marked, because nothing was prepared or applied on this node for them.
   */
  @Override
  public TransactionContext startTransaction(final RaftClientRequest request) throws IOException {
    final RaftHAServer raft = this.raftHAServer;
    final boolean isLocalOrigin = raft != null
        && raft.getClient() != null
        && raft.getClient().getId().equals(request.getClientId());

    final TransactionContext.Builder context = TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request);

    // A transaction entry is validated here against the page versions the log has assigned so far (issue #6965):
    // one validated on its originating node against a version the log has already moved past is refused before Ratis
    // touches it, through the context's exception. That reply is exactly the retryable
    // ConcurrentModificationException a single node raises, and refusing at this stage costs Ratis nothing - a
    // refusal thrown from preAppendTransaction, by contrast, leaks the leader's pending-write permit in Ratis 3.3.0,
    // and enough of them would wedge the leader for good. An accepted entry reserves its versions, so the next entry
    // on the same pages is checked against them rather than against a local copy that has not caught up yet.
    final ByteString data = request.getMessage() != null ? request.getMessage().getContent() : null;
    if (data == null || data.isEmpty() || RaftLogEntryType.fromId(data.byteAt(0)) != RaftLogEntryType.TX_ENTRY)
      return context.setStateMachineContext(isLocalOrigin ? Boolean.TRUE : null).build();

    final RaftLogEntryCodec.DecodedEntry decoded;
    try {
      decoded = RaftLogEntryCodec.decode(data);
    } catch (final RuntimeException e) {
      // Refuse rather than append an entry no node will be able to read; not a fault of this leader.
      return context.build().setException(e);
    }

    final PageVersionLedger.EntryId entryId = new PageVersionLedger.EntryId(request.getClientId(), request.getCallId());
    // Decoded once: the same page list serves the validation here, the confirmation at append and the release at apply.
    final PageVersionLedger.Pages pages;
    try {
      pages = PageVersionLedger.parse(decoded.walData());
    } catch (final RuntimeException e) {
      return context.build().setException(e);
    }
    final DatabaseInternal db = databaseForValidation(decoded.databaseName());
    if (db == null)
      // An entry that cannot be validated must not enter the log: applied unvalidated it would take the very
      // equal-version merge path this validation exists to close. The database is not open on this leader right
      // now (still installing, or being dropped), which is a transient the originator can retry.
      return context.build().setException(new NeedRetryException(
          "Database '" + decoded.databaseName() + "' is not available on the leader to validate the transaction. Please retry"));
    try {
      validateBeforeAppend(db, pages, entryId);
    } catch (final NeedRetryException e) {
      HALog.log(this, HALog.DETAILED, "Refusing tx %d on database '%s': %s",
          peekWalTransactionId(decoded.walData()), decoded.databaseName(), e.getMessage());
      return context.build().setException(e);
    }

    return context.setStateMachineContext(new AppendedEntry(isLocalOrigin, decoded, entryId, pages)).build();
  }

  /**
   * Ratis reports a log write failure here from the segmented log worker thread. Cheap and non-blocking by
   * contract; the recovery runs on the {@link HealthMonitor} thread, driven by {@link #getRaftLogFailure()}.
   * <p>
   * The check-then-set on {@link #raftLogFailure} is not atomic and does not need to be: Ratis runs one
   * {@code SegmentedRaftLogWorker} thread per division and every {@code notifyLogFailed} call for this state
   * machine comes from it, in order. The field is volatile only so the monitor thread sees the mark promptly.
   */
  @Override
  public void notifyLogFailed(final Throwable cause, final LogEntryProto failedEntry) {
    super.notifyLogFailed(cause, failedEntry);
    // Ratis reports every later task against the same pinned exception: the first failure is the one that matters.
    if (raftLogFailure != null)
      return;
    final long index = failedEntry != null ? failedEntry.getIndex() : -1L;
    final String message = cause != null ? cause.toString() : "unknown cause";
    raftLogFailure = new RaftLogFailure(index, message, System.currentTimeMillis());
    LogManager.instance().log(this, Level.SEVERE,
        "Raft log write failed %s. Ratis has marked the log failed: every later append is rejected until the server "
            + "is restarted. The health monitor will restart it in place once the Raft storage volume has room "
            + "again (issue #7037)", cause, index >= 0 ? "at index " + index : "on a log segment");
  }

  /** The first persistent Raft log write failure, or {@code null} while the log writer is healthy (issue #7037). */
  public RaftLogFailure getRaftLogFailure() {
    return raftLogFailure;
  }

  /** Whether the current thread is the Ratis apply thread this state machine last applied an entry on. */
  boolean isApplyThread() {
    return Thread.currentThread() == applyThread;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();
    final ByteString data = entry.getStateMachineLogEntry().getLogData();
    final TermIndex termIndex = TermIndex.valueOf(entry);
    final long index = termIndex.getIndex();
    applyThread = Thread.currentThread();

    // Refuse to apply once a prior entry tripped the critical-error halt. Continuing would
    // operate on the inconsistent in-memory state left behind by the failed apply and cascade
    // into additional SEVERE errors before the async server.stop() completes (#4219).
    final CriticalHalt halt = haltedAfterCriticalError.get();
    if (halt != null)
      return CompletableFuture.failedFuture(new ReplicationException(
          "State machine halted after critical error " + halt.describe() + "; refusing to apply index " + index));

    // Captured after decode so the catch blocks can tell whether a ReplicationException is the expected
    // resync-in-progress signal for an already-quarantined database (throttled at the source) or a
    // genuine replication error that must still be logged loudly. Null until decode succeeds.
    String targetDatabase = null;
    try {
      // Decoded once, at append time on the leader (preAppendTransaction); everywhere else decoded here.
      final Object context = trx.getStateMachineContext();
      final RaftLogEntryCodec.DecodedEntry decoded = context instanceof AppendedEntry appended ?
          appended.decoded() :
          RaftLogEntryCodec.decode(data);
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
        triggerCriticalHalt(index, "unknown Raft log entry type, most likely written by a newer node version; "
            + "upgrade this node to a version that understands it");
        return CompletableFuture.failedFuture(new ReplicationException(
            "Unknown Raft log entry type at index " + index + "; node halted to prevent silent divergence"));
      }

      final boolean originatedLocally = context instanceof AppendedEntry appended ?
          appended.originatedLocally() :
          Boolean.TRUE.equals(context);

      // Set by the three security applies when the entry's compare-and-set precondition no longer held, so the
      // reply below can tell the submitter its change did not land (issue #7509). A one-element array rather
      // than a field: applyWithRetry can re-run the lambda, and a field would outlive this entry.
      final boolean[] securitySuperseded = new boolean[1];

      applyWithRetry(index, decoded.databaseName(), () -> {
        securitySuperseded[0] = false;
        switch (decoded.type()) {
        case TX_ENTRY -> applyTxEntry(decoded, index, context instanceof AppendedEntry appended ? appended.pages() : null);
        case SCHEMA_ENTRY -> applySchemaEntry(decoded, index, originatedLocally);
        case INSTALL_DATABASE_ENTRY -> applyInstallDatabaseEntry(decoded, index);
        case DROP_DATABASE_ENTRY -> applyDropDatabaseEntry(decoded);
        case SECURITY_USERS_ENTRY -> securitySuperseded[0] = !applySecurityUsersEntry(decoded, index);
        case SECURITY_GROUPS_ENTRY -> securitySuperseded[0] = !applySecurityGroupsEntry(decoded, index);
        case SECURITY_API_TOKENS_ENTRY -> securitySuperseded[0] = !applySecurityApiTokensEntry(decoded, index);
        case BOOTSTRAP_FINGERPRINT_ENTRY -> applyBootstrapFingerprintEntry(decoded, index, originatedLocally);
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
      // A superseded security entry IS applied - as a no-op, identically on every node, so the applied index
      // advances exactly as it does for any other entry. Only the ANSWER differs, so the submitter learns its
      // document was built from a view the cluster had already moved past (issue #7509).
      return CompletableFuture.completedFuture(
          Message.valueOf(securitySuperseded[0] ? SECURITY_ENTRY_SUPERSEDED_REPLY : "OK"));

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
    } catch (final RaftLogEntryDecodeException e) {
      // A committed entry of a KNOWN type that this version cannot read: truncated, corrupt, or written in a
      // shape it does not understand. "Halt the node" and "skip the entry" are not the only two options
      // (issue #7138): when the envelope named a database, the failure is isolable exactly like an apply error
      // on that database, so quarantine it and let the leader resend it as a snapshot. The node stays up for
      // its other databases, and the entry is never silently skipped. Without a database name there is nothing
      // to quarantine, and handleUnexpectedApplyError escalates to the node-wide halt as before.
      //
      // This branch is for the ENVELOPE, decoded above before applyWithRetry is called. A decode failure raised
      // INSIDE the apply - applyTxEntry's WAL payload, since issue #7495 - has already been through
      // handleUnexpectedApplyError by the time it leaves applyWithRetry, which is why applyWithRetry re-types the
      // one case that would otherwise arrive here (see its catch of RaftLogEntryDecodeException). Widening the
      // catches between here and `catch (Throwable)` would undo that and charge one failure to the swallow budget
      // twice; theEscalationBudgetIsChargedOncePerUndecodableEntry is the test that says so.
      final String decodeDatabase = e.getDatabaseName();
      LogManager.instance().log(this, Level.SEVERE,
          "Cannot decode the committed Raft log entry at index %d (type=%s, database=%s). This is either a corrupt "
              + "entry or one written by a newer node in a shape this version cannot read: %s",
          e, index, e.getType(), decodeDatabase == null || decodeDatabase.isEmpty() ? "<none>" : decodeDatabase,
          e.getMessage());
      try {
        handleUnexpectedApplyError(index, decodeDatabase, e);
      } catch (final ReplicationException quarantined) {
        return CompletableFuture.failedFuture(quarantined);
      } catch (final RuntimeException fatal) {
        triggerCriticalHalt(index, "a committed entry of type " + e.getType() + " could not be decoded and the "
            + "envelope named no database to quarantine instead: " + e.getMessage());
        return CompletableFuture.failedFuture(fatal);
      }
      // handleUnexpectedApplyError always throws; unreachable, but the compiler needs a value.
      return CompletableFuture.failedFuture(e);
    } catch (final Throwable e) {
      // Unexpected errors (NPE, ClassCastException, OOM, etc.) indicate a bug that could cause
      // state divergence if silently swallowed. Crash the server so the node recovers via snapshot.
      LogManager.instance().log(this, Level.SEVERE,
          """
          CRITICAL: Unexpected error applying Raft log entry at index %d. \
          Shutting down to prevent state divergence.""", e, index);
      triggerCriticalHalt(index, "unexpected error applying a committed entry: " + e);
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
   * <p>
   * The cause is recorded with the flag (issue #7872), because the emergency stop below can fail - its only
   * failure handling is a log line - and a process that stays up with a dead state machine has to be able to say
   * so through {@code GET /api/v1/cluster} rather than only through a SEVERE that has already scrolled past.
   * {@code compareAndSet} keeps the FIRST cause: it is the one that explains the halt, and every apply after it
   * is refused by the guard in {@code applyTransaction} rather than being a new problem.
   *
   * @param index  the Raft log index being applied, or -1 when there is none
   * @param reason one line naming the condition, in the vocabulary an operator can act on
   */
  private void triggerCriticalHalt(final long index, final String reason) {
    haltedAfterCriticalError.compareAndSet(null, new CriticalHalt(index, reason, System.currentTimeMillis()));
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
        try {
          handleUnexpectedApplyError(index, databaseName, t);
        } catch (final RaftLogEntryDecodeException escalated) {
          // handleUnexpectedApplyError rethrows the ORIGINAL error once the swallow budget is exhausted, so a
          // node that can never resync halts instead of degrading silently. Since issue #7495 that original can
          // be a RaftLogEntryDecodeException raised INSIDE this lambda (applyTxEntry's WAL decode), and
          // applyTransaction catches that type separately - as the handler for an unreadable ENVELOPE, which is
          // decoded before applyWithRetry is ever called. Letting this one reach that catch would run
          // handleUnexpectedApplyError a second time for a single failure, charging the swallow twice against
          // the very budget that just tripped and logging the escalation twice. It has been handled here, so
          // hand the fatal path the same failure under a type that catch does not claim.
          throw new IllegalStateException(escalated.getMessage(), escalated);
        }
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
      // quarantineDatabase() returns true only the first time, which is when we kick off the targeted resync. The cause
      // recorded with it is what the operator-facing alert says (issue #7741): an entry this node cannot decode
      // is a corrupt local log segment, not a replication fault, and pointing at the leader for it wastes the
      // one person who can see the bad disk.
      if (quarantineDatabase(databaseName,
          t instanceof RaftLogEntryDecodeException ? DivergenceCause.UNDECODABLE_LOG_ENTRY : DivergenceCause.APPLY_ERROR)) {
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

    // Node-scoped entry (no single target database): there is no per-database state to quarantine and no
    // targeted resync that would repair it, so an unexpected error here still escalates to the node-wide halt
    // (issue #4798's reasoning: never silently skip a committed mutation). An apply that is provably fail-safe
    // classifies its OWN failure as recoverable before it reaches here - see applySecurityUsersEntry (issue
    // #7137) - so the entries that arrive at this line are the ones for which a halt is the honest answer.
    // Say which class of entry it was: without it, the SEVERE at the fatal branch names only an index.
    LogManager.instance().log(this, Level.SEVERE,
        "Unexpected error applying a node-scoped Raft entry at index %d (the entry targets no single database, so "
            + "there is nothing to quarantine); escalating to the node-wide halt: %s", index, t.getMessage());
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
   * <p>
   * <b>Refused while any database is quarantined (issue #7735):</b> a quarantine skips a committed entry on
   * purpose and lets later entries advance the applied index past it, so checkpointing that index would let
   * Ratis purge the one entry a restart still has to replay.
   */
  @Override
  public long takeSnapshot() {
    final long currentIndex = lastAppliedIndex.get();
    if (currentIndex < 0)
      return RaftLog.INVALID_LOG_INDEX;

    // The pages of an entry this node originated are published by the apply thread before lastAppliedIndex moves past
    // it (issue #6965), so the applied position is the durable position on the leader exactly as it is on a follower,
    // and no clamp for an in-flight leader-side phase 2 is needed any more (the #5407 ticket this replaced).
    //
    // That premise depends on an entry whose pages did NOT reach this node never advancing the index in the first
    // place, which is what issue #7602 restored: publishLocalCommit throws when the publication and the reconcile
    // both fail, so applyTransaction never reaches the getAndSet below for it and there is no advanced position
    // here to checkpoint. Before that it swallowed the double failure, and this method duly recorded a position
    // covering an entry the node does not hold - the exact state the removed #5407 clamp used to keep replayable.

    // Regressing the marker below an existing one would let Ratis replay from an index whose log
    // entries a previous checkpoint already authorised for purging. Skip this round instead; the
    // next snapshot (after a pending stale-snapshot resync lands, issue #6111) advances it normally.
    final var latest = storage.getLatestSnapshot();
    if (latest != null && currentIndex < latest.getIndex()) {
      HALog.log(this, HALog.BASIC,
          "Skipping snapshot checkpoint at index %d: the applied position trails the existing marker at %d "
              + "(a stale-snapshot resync still pending)",
          currentIndex, latest.getIndex());
      return RaftLog.INVALID_LOG_INDEX;
    }

    // A quarantine SKIPPED a committed entry: applyTransaction returned a failed future before the getAndSet
    // below the quarantine, so currentIndex covers entries that came AFTER one this node never applied.
    // Checkpointing it would authorise Ratis to purge the log through that entry, and the skip would become
    // permanent instead of replayable - the node comes back after a restart missing a committed mutation with
    // nothing left to replay (issue #7735). Refuse until a resync clears the quarantine, which is exactly what
    // the two refusals above already do for the conditions they name.
    //
    // The cost is a log that keeps growing while a database is quarantined. That is the intended trade: a
    // quarantine always needs a resync to clear (none of the four DivergenceCause values heals by itself), the
    // resync is triggered at the mark and re-driven by retryUnfilledSnapshotGap() on every HealthMonitor tick,
    // and the node is out of the ready set the whole time. Purging a log this node still needs is not cheaper
    // than the disk it saves.
    ensureAppliedIndexLoaded();
    if (!divergedDatabases.isEmpty()) {
      HALog.log(this, HALog.BASIC,
          "Skipping snapshot checkpoint at index %d: database(s) %s are quarantined, so the log must stay "
              + "replayable past the entry the quarantine skipped (issue #7735)",
          currentIndex, divergedDatabases.keySet());
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
   * {@link #notifyInstallSnapshotFromLeader} (follower-side install). Markers below {@code index} are
   * pruned best-effort before returning, see {@link #pruneObsoleteSnapshotMarkers}.
   *
   * <b>Not synchronised, deliberately.</b> Both callers can in principle register concurrently, and a
   * low-index registration whose file lands after a concurrent high-index prune leaves that one marker
   * behind: its own prune runs with its own (lower) {@code keepIndex} and so removes nothing. It is
   * self-healing - any later prune runs with a higher {@code keepIndex} and sweeps it - and it costs a
   * zero-byte file in the meantime. A lock here would add serialisation to the snapshot path to buy
   * that back. What the prune must never do is delete a marker that is still the latest, and the
   * strictly-below comparison in {@link #pruneObsoleteSnapshotMarkers} guarantees that without a lock:
   * {@code keepIndex} is never above the index {@code storage} reports as latest.
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
      // Keep only the marker just registered; older zero-byte markers are obsolete once a newer one
      // exists. This used to be storage.cleanupOldSnapshots(policy), which deletes nothing for
      // ArcadeDB (issue #7209): Ratis 3.3.0's SimpleStateMachineStorage advances its delete index
      // only after counting getNumSnapshotsRetained() markers that HAVE an .md5 companion, and this
      // state machine writes none by design (see above), so its deleteIdx stays -1 whatever policy
      // it is handed.
      pruneObsoleteSnapshotMarkers(parentDir, index);
      return true;
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Failed to write Raft snapshot marker at (term=%d, index=%d): %s", term, index, e.getMessage());
      return false;
    }
  }

  /**
   * Deletes every {@code snapshot.<term>_<index>} marker in {@code stateMachineDir} whose index is
   * <b>strictly below</b> {@code keepIndex}, best-effort (issue #7209).
   * <p>
   * ArcadeDB's markers are zero-byte placeholders, so an older one carries nothing a newer one does
   * not: {@link SimpleStateMachineStorage#getLatestSnapshot()} only ever reports the highest index it
   * finds. Left alone they accumulate one inode and one directory entry per checkpoint for the life of
   * the node, and every {@code getSingleFileSnapshotInfos()} scan - one per checkpoint, one per restart
   * - walks all of them.
   * <p>
   * Strictly below, never at or above, so the marker just written keeps its own file and a marker at a
   * higher index (one a concurrent {@link #notifyInstallSnapshotFromLeader} registered, or one this
   * call is superseded by) is never removed. Two markers that share an index and differ only in term
   * both survive on purpose: {@code updateLatestSnapshot} keeps the <i>previous</i> info on an equal
   * index, so the live latest-snapshot reference may point at exactly the lower-term file.
   * <p>
   * Only names matching {@link SimpleStateMachineStorage#SNAPSHOT_REGEX} are candidates, so
   * {@code .md5} companions and {@code .tmp}/{@code .corrupt} leftovers are left alone; Ratis sweeps
   * orphaned {@code .md5} files itself on the {@code cleanupOldSnapshots()} call its
   * {@code StateMachineUpdater} makes after every snapshot.
   * <p>
   * Best-effort by design, and the guarantee lives here rather than at the call sites: a failed delete
   * costs one stale directory entry, never correctness, so every failure - a {@code false} from
   * {@link File#delete()}, and any {@link RuntimeException} the filesystem raises on the way, such as a
   * {@code SecurityException} from {@link File#listFiles()} - is logged at FINE and swallowed. The
   * caller's snapshot registration still succeeds. Failing a checkpoint over a cosmetic cleanup would
   * block log purge, which is strictly worse than a leftover file.
   *
   * @return the number of markers actually deleted, {@code 0} if the sweep could not run at all
   */
  private int pruneObsoleteSnapshotMarkers(final File stateMachineDir, final long keepIndex) {
    try {
      return pruneObsoleteSnapshotMarkers0(stateMachineDir, keepIndex);
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.FINE,
          "Could not prune obsolete Raft snapshot markers in %s: %s", stateMachineDir, e.getMessage());
      return 0;
    }
  }

  /** The sweep itself; {@link #pruneObsoleteSnapshotMarkers} is the guard that makes it best-effort. */
  private int pruneObsoleteSnapshotMarkers0(final File stateMachineDir, final long keepIndex) {
    if (stateMachineDir == null)
      return 0;
    final File[] entries = stateMachineDir.listFiles();
    if (entries == null)
      return 0;

    int deleted = 0;
    for (final File entry : entries) {
      final Matcher matcher = SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(entry.getName());
      if (!matcher.matches())
        continue;
      final long markerIndex;
      try {
        markerIndex = Long.parseLong(matcher.group(2));
      } catch (final NumberFormatException ignored) {
        // A digit run too long to be a long: not a marker this state machine wrote. Leave it alone.
        continue;
      }
      if (markerIndex >= keepIndex)
        continue;
      if (entry.delete())
        deleted++;
      else
        LogManager.instance().log(this, Level.FINE,
            "Could not delete obsolete Raft snapshot marker %s", entry.getAbsolutePath());
    }

    if (deleted > 0)
      LogManager.instance().log(this, Level.FINE,
          "Pruned %d obsolete Raft snapshot marker(s) below index %d", deleted, keepIndex);
    return deleted;
  }

  /**
   * One-shot prune at {@link #initialize} time, so a node that accumulated markers before the #7209
   * fix does not carry them (and the directory scan over them) until its next checkpoint - or forever,
   * if it never takes another one.
   * <p>
   * Runs after {@code storage.init()}, so {@link SimpleStateMachineStorage#getLatestSnapshot()} already
   * reports the highest-index marker on disk; everything below it is obsolete. A node with no marker
   * yet reports {@code null} and nothing is pruned.
   * <p>
   * The directory check comes first on purpose. On a node whose state-machine directory does not exist
   * yet, {@code storage.init()} has already had {@code loadLatestSnapshot()} fail its directory scan
   * and log {@code "Failed to updateLatestSnapshot from ..."} - a WARNING the #6991 filter deliberately
   * lets through. Nothing is cached after that failure, so calling {@code getLatestSnapshot()} again
   * here would re-run the scan and log the same warning a second time on every fresh boot.
   */
  private void pruneSnapshotMarkersAtStartup() {
    try {
      final File stateMachineDir = storage.getSnapshotFile(0L, 0L).getParentFile();
      if (stateMachineDir == null || !stateMachineDir.isDirectory())
        return;
      final SingleFileSnapshotInfo latest = storage.getLatestSnapshot();
      if (latest == null)
        return;
      final int pruned = pruneObsoleteSnapshotMarkers(stateMachineDir, latest.getIndex());
      if (pruned > 0)
        LogManager.instance().log(this, Level.INFO,
            "Removed %d obsolete Raft snapshot marker(s) left by earlier checkpoints; the newest, at index %d, is retained",
            pruned, latest.getIndex());
    } catch (final RuntimeException e) {
      // The sweep guards itself; this covers the lookups above it (getSnapshotFile throws when Ratis
      // has no state-machine directory). Never let housekeeping fail a state-machine start.
      LogManager.instance().log(this, Level.FINE,
          "Could not prune obsolete Raft snapshot markers at startup: %s", e.getMessage());
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

    // One read of the volatile for the whole callback: the null check below is worth nothing if each of the
    // dereferences after it re-reads a field a concurrent teardown can null (issue #7253).
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null || newLeaderId == null)
      return;

    final RaftPeerId prevId = previousLeaderId;
    previousLeaderId = newLeaderId;

    final String leaderName = raftHA.getPeerDisplayName(newLeaderId);
    // Use the actual Raft term (not the lagging last-applied term) so we can tell a genuine
    // re-election (term advanced) from a same-term re-notification that Ratis sometimes fires.
    final long currentTerm = raftHA.getCurrentTerm();
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
      final String prevName = raftHA.getPeerDisplayName(prevId);
      LogManager.instance().log(this, Level.INFO, "Leader changed: %s -> %s (term=%d)",
          prevName, leaderName, currentTerm);
    }

    // Recreate the RaftClient so its gRPC channels perform fresh DNS resolution.
    // After a network partition, channels to isolated peers enter TRANSIENT_FAILURE
    // with exponential back-off (up to ~120 s). Refreshing on every leader change
    // ensures the client can reach all peers as soon as the partition heals.
    // Pass the newly elected leader's peer ID so the fresh client routes its very first
    // write directly to the leader rather than probing peers.
    raftHA.refreshRaftClient(newLeaderId);

    if (newLeaderId.equals(raftHA.getLocalPeerId())) {
      LogManager.instance().log(this, Level.INFO, "This node is now LEADER");
      raftHA.startLagMonitor();
      raftHA.printClusterConfiguration();

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
          raftHA.runBootstrapIfEligible();
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.WARNING,
              "Bootstrap election threw on leader-change handler: %s", null, t.getMessage());
        }
      });
    } else {
      LogManager.instance().log(this, Level.INFO, "This node is now REPLICA (leader: %s)", leaderName);
      raftHA.stopLagMonitor();

      // Issue #7833: this node may have come back while still a Raft member, in which case no configuration
      // entry was written and nothing seeded it the cluster's security documents. Once per start, and only as a
      // replica - the leader is the reference this asks against. Off this thread and off lifecycleExecutor: it
      // waits for catch-up and then dials the leader, neither of which belongs on a Ratis callback or on the
      // single-threaded executor the snapshot-download triggers queue on.
      securityCatchUp.onFirstLeaderObserved(this.server, raftHA);
    }

    // If a snapshot gap was detected during reinitialize(), trigger the download now
    // that we know who the leader is (primary path; the 30s watchdog is the fallback).
    if (needsSnapshotDownload.compareAndSet(true, false)) {
      LogManager.instance().log(this, Level.INFO,
          "Leader change detected, triggering pending snapshot download from leader %s", leaderName);
      lifecycleExecutor.submit(this::triggerSnapshotDownload);
    }

    // Wake up any threads waiting for leadership change (e.g. leaveCluster)
    final Object notifier = raftHA.getLeaderChangeNotifier();
    synchronized (notifier) {
      notifier.notifyAll();
    }
  }

  /**
   * Called by Ratis on every node that takes on a Raft configuration. Used to seed the cluster security
   * documents to a peer that just entered the committed configuration (issue #7531).
   * <p>
   * This is the one place all three admission paths meet. {@code POST /api/v1/cluster/peer} and
   * {@code connect cluster} also seed from the admitting node (issue #7521), but {@code KubernetesAutoJoin}
   * has no admitting node - on a StatefulSet scale-up the new pod issues {@code Mode.ADD} for itself - so
   * nothing seeded a self-joining pod at all. The documents involved ({@code server-users.jsonl},
   * {@code server-groups.json}, {@code server-api-tokens.json}) live under {@code <server-root>/config/} and
   * are carried by no snapshot install, so an unseeded member serves requests against its own copy of them.
   * <p>
   * {@link MembershipSecuritySeeder} carries the decision: leader only, and only for a configuration that
   * brought in a peer the previous one did not have.
   * <p>
   * It is also where a node learns that it was itself the peer brought in: {@link RuntimeJoinDetector} records
   * that, and it is what arms the security-convergence readiness gate on the joiner (issue #7819).
   * <p>
   * <b>Nothing here may throw or block.</b> Ratis calls this from two places in ratis-server 3.3.0 -
   * {@code RaftServerImpl.applyLogToStateMachine}, i.e. the state-machine apply loop, and
   * {@code SnapshotInstallationHandler.installSnapshotImpl}, i.e. the thread serving a leader-initiated
   * snapshot install - and neither is a thread that may carry a Raft round trip or an exception from
   * housekeeping. The seeder does its membership update under its own monitor for the same reason: the two
   * can arrive concurrently.
   */
  @Override
  public void notifyConfigurationChanged(final long term, final long index,
      final RaftProtos.RaftConfigurationProto newRaftConfiguration) {
    super.notifyConfigurationChanged(term, index, newRaftConfiguration);

    try {
      final List<RaftPeerId> peers = new ArrayList<>(newRaftConfiguration.getPeersCount());
      for (final RaftProtos.RaftPeerProto peer : newRaftConfiguration.getPeersList())
        peers.add(RaftPeerId.valueOf(peer.getId()));

      // Before the seeder, and on its own inputs: whether THIS node was just added arms its readiness gate
      // (issue #7819), and must not depend on the seed decision - which is the leader's, never the joiner's.
      final List<RaftPeerId> oldPeers = new ArrayList<>(newRaftConfiguration.getOldPeersCount());
      for (final RaftProtos.RaftPeerProto peer : newRaftConfiguration.getOldPeersList())
        oldPeers.add(RaftPeerId.valueOf(peer.getId()));
      runtimeJoinDetector.onConfiguration(getId(), peers, oldPeers, index);

      membershipSecuritySeeder.onConfigurationChanged(term, index, peers);
    } catch (final Throwable t) {
      // The apply loop is not the place to find out that housekeeping has a bug in it.
      LogManager.instance().log(this, Level.WARNING,
          "Could not evaluate the security seed for the Raft configuration at term=%d index=%d: %s", t, term, index,
          t.getMessage());
    }
  }

  /**
   * Whether this node currently holds the Raft LEADER role, read from the Ratis division rather than from
   * {@link RaftHAServer}.
   * <p>
   * The division is the same source {@code RaftHAServer.isLeader()} reads, and asking Ratis directly keeps the
   * membership seed working on a state machine that has no {@code RaftHAServer} wired to it - which is every
   * peer of the {@code MiniRaftCluster} harness the HA tests run against, whose {@code BaseMiniRaftTest} wires
   * {@code setServer} and never {@code setRaftHAServer}. Degrades to {@code false} on an unreadable division,
   * matching {@code RaftHAServer.isLeader()}.
   * <p>
   * Package-private so the membership-seed tests can drive a substituted seeder off the REAL role read rather
   * than off a second implementation of it.
   */
  boolean isLocalNodeRaftLeader() {
    try {
      final CompletableFuture<RaftServer> raftServer = getServer();
      final RaftGroupId groupId = getGroupId();
      if (raftServer == null || !raftServer.isDone() || groupId == null)
        return false;
      return raftServer.join().getDivision(groupId).getInfo().isLeader();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not read the Raft role for the security seed: %s",
          e.getMessage());
      return false;
    }
  }

  /** The time budget {@code arcadedb.ha.securitySeedRetryTimeout} gives the membership seed. */
  long securitySeedRetryBudgetMs() {
    final ArcadeDBServer srv = this.server;
    return srv == null ? 0L
        : srv.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT);
  }

  /**
   * Submits the three security documents through {@code ServerSecurity}, which reads each one under the
   * security monitor - reading here and submitting afterwards is the window a concurrent revocation slips
   * through, and a seed carries whole documents (issue #7373).
   *
   * @return the documents that could not be seeded, empty when all of them committed
   */
  private List<String> seedSecurityStateClusterWide(final long retryBudgetMs) {
    final ArcadeDBServer srv = this.server;
    // Both of these THROW rather than returning no failures. An empty list is how the seeder is told every
    // document committed, and "there was nothing here to seed with" must not be reported to an operator as a
    // successful seed - ServerSecurity.seedSecurityStateClusterWide answers an absent HA plugin with an empty
    // list of its own, so the check has to happen on this side of the call.
    if (srv == null || srv.getSecurity() == null)
      throw new IllegalStateException("this node has no security store to seed the joining peer from");
    if (srv.getHA() == null)
      throw new IllegalStateException(
          "this node has no HA plugin, so the security documents cannot be replicated to the joining peer");
    return srv.getSecurity().seedSecurityStateClusterWide(retryBudgetMs);
  }

  /**
   * Runs the cluster security seed on this node and reports what it could not commit (issues #7833, #7834).
   * <p>
   * The entry point {@link PostSecuritySeedHandler} and the local short circuit in
   * {@link ClusterSecuritySeedQuery} both end here, which is the point: there is one seeder per cluster and it
   * lives on the leader. See {@link MembershipSecuritySeeder#seedNowAndReport} for what an outstanding seed does
   * with a second request.
   *
   * @param reason what the seed is for, carried through to the log lines the run writes
   * @param mayReuseRecentSeed whether a seed that just finished may answer this request; see
   *               {@link MembershipSecuritySeeder#seedNowAndReport} for why only an admission may say true
   *
   * @throws IllegalStateException when no seed could be run or its outcome could not be read
   */
  public List<String> seedSecurityNowAndReport(final String reason, final long timeoutMs,
      final boolean mayReuseRecentSeed) {
    return membershipSecuritySeeder.seedNowAndReport(reason, timeoutMs, mayReuseRecentSeed);
  }

  /** Package-private test seam (issue #7531): substitutes the seeder the configuration callback drives. */
  /** Installs the detector {@link RaftHAServer} owns, so it outlives this state machine (issue #7819). */
  void setRuntimeJoinDetector(final RuntimeJoinDetector detector) {
    this.runtimeJoinDetector = detector;
  }

  /** Whether this node was added to the Raft configuration while running (issue #7819). */
  RuntimeJoinDetector getRuntimeJoinDetector() {
    return runtimeJoinDetector;
  }

  void setMembershipSecuritySeederForTesting(final MembershipSecuritySeeder seeder) {
    final MembershipSecuritySeeder previous = this.membershipSecuritySeeder;
    this.membershipSecuritySeeder = seeder;
    if (previous != null)
      previous.close();
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
      // Read the volatile ONCE for this whole install: resolveSnapshotSource() reads it into its own local so it
      // can refuse instead of throwing, and reading the field again below for the cluster token would reopen the
      // window that read is written to close - a teardown nulling it between the two turns a refusal into a
      // NullPointerException on the automatic resync path (issue #7253).
      final RaftHAServer raftHA = this.raftHAServer;
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
      final String clusterToken = raftHA != null ? raftHA.getClusterToken() : null;

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
      if (raftHA != null)
        raftHA.notifyApplied();

      // Issue #7833: a snapshot install is the one catch-up path that provably skips the security entries - the
      // three documents live under <server-root>/config/, outside the database directory, and no snapshot
      // carries them. Ask the leader whether this node is still in step, AFTER the install is fully recorded so
      // the request cannot be answered against a half-installed node.
      if (raftHA != null)
        securityCatchUp.afterSnapshotInstall(this.server, raftHA);

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
   * Registers a transaction this node originated, right before its entry is dispatched to Raft: the apply thread will
   * claim it when the entry reaches its position in the log and publish the prepared pages there (issue #6965).
   */
  boolean registerLocalCommit(final LocalCommit commit) {
    return localCommits.register(commit);
  }

  /**
   * Takes a registered transaction back from the apply thread.
   *
   * @return {@code false} when the apply thread already claimed it: the entry committed, and the committing thread must
   * wait for the outcome of the publication instead of rolling back
   */
  boolean withdrawLocalCommit(final LocalCommit commit) {
    return localCommits.withdraw(commit);
  }

  /**
   * What the apply thread does when it reaches the entry of a registered transaction. Exposed for unit tests that
   * drive the committing thread through {@code RaftReplicatedDatabase.replicateAndCommitLocally} against a bare state
   * machine, where no Ratis apply thread exists to claim the transaction.
   */
  // @VisibleForTesting
  LocalCommit claimLocalCommit(final String databaseName, final long walTxId, final byte[] walData) {
    return localCommits.claim(databaseName, walTxId, walData);
  }

  /** Transactions this node originated whose entry the apply thread has not reached yet. */
  int pendingLocalCommits() {
    return localCommits.size();
  }

  /** Age in milliseconds of the oldest such transaction, {@code 0} when none is in flight. */
  long oldestPendingLocalCommitMs() {
    return localCommits.oldestRegisteredMs();
  }

  /** Pages of the database whose next version the log assigned to an entry this node has not applied yet. */
  int reservedPageVersions(final String databaseName) {
    return pageVersions.reservedPages(databaseName);
  }

  /**
   * Reads a replicated WAL transaction's id without deserializing the pages behind it: the id is the
   * first field {@link #deserializeWalTransaction(byte[])} writes, so it is the leading 8 bytes of the
   * payload. It is the key the committing thread and the apply thread meet on (see {@link LocalCommit}),
   * read without materializing the pages behind it.
   */
  static long peekWalTransactionId(final byte[] walData) {
    if (walData == null || walData.length < Long.BYTES)
      throw new ReplicationException("Corrupted WAL transaction entry: truncated before the transaction id");
    return ByteBuffer.wrap(walData, 0, Long.BYTES).getLong();
  }

  /**
   * Ratis calls this on the leader for every client entry, under the log's write lock and in exactly the order the
   * entries take in the log. The validation already happened in {@link #startTransaction}; what remains is to confirm
   * the reservation at the point that fixes the entry's position, which is what tells a reservation backed by the log
   * from one left behind by a request Ratis dropped before appending it (issue #6965).
   */
  @Override
  public TransactionContext preAppendTransaction(final TransactionContext trx) throws IOException {
    if (trx.getStateMachineContext() instanceof AppendedEntry appended
        && !pageVersions.confirmAppended(appended.decoded().databaseName(), appended.pages(), appended.entryId())) {
      // The entry was delayed between its reservation and this append for longer than the ledger trusts an
      // unconfirmed reservation, and another entry took the page over in between: appending it now would put two
      // entries with the same target version in the log, the exact splice this ledger exists to prevent. Refusing
      // from here costs one Ratis pending-write permit (see startTransaction), which is why every other refusal
      // lives there; this one is the last line of defence for a window that only a wedged leader opens.
      final ConcurrentModificationException conflict = new ConcurrentModificationException(
          "Concurrent modification on database '" + appended.decoded().databaseName()
              + "': the transaction was delayed on the leader and its pages were taken over by a later transaction. "
              + "Please retry the operation");
      LogManager.instance().log(this, Level.WARNING,
          "Refusing to append tx %d on database '%s' whose page reservation expired before the append: %s",
          peekWalTransactionId(appended.decoded().walData()), appended.decoded().databaseName(), conflict.getMessage());
      throw new StateMachineException(conflict.getMessage(), conflict, false);
    }
    return trx;
  }

  /**
   * The validation {@link #startTransaction} performs on a transaction entry, against the pages of the given
   * database: refuses the entry with a {@link ConcurrentModificationException} when any of its pages was validated
   * against a version the log already moved past, reserves its versions otherwise.
   */
  // @VisibleForTesting
  void validateBeforeAppend(final DatabaseInternal db, final byte[] walData, final PageVersionLedger.EntryId entryId) {
    validateBeforeAppend(db, PageVersionLedger.parse(walData), entryId);
  }

  /**
   * Never lets anything but a {@link NeedRetryException} out: an entry {@link #startTransaction} cannot validate is
   * refused through the context, never by an exception escaping the hook. A page that cannot be read on the leader
   * (an I/O error, a database closing under the read) is therefore a retryable refusal too, with the cause attached.
   */
  private void validateBeforeAppend(final DatabaseInternal db, final PageVersionLedger.Pages pages,
      final PageVersionLedger.EntryId entryId) {
    try {
      pageVersions.validateAndReserve(db.getName(), pages, entryId, localVersionsOf(db));
    } catch (final NeedRetryException e) {
      throw e;
    } catch (final Exception e) {
      throw new NeedRetryException(
          "Cannot validate the transaction on the leader against database '" + db.getName() + "': " + e.getMessage()
              + ". Please retry", e);
    }
  }

  /** Releases the page versions an entry reserved, as the apply thread does once the entry is applied. */
  // @VisibleForTesting
  void releaseReservedVersions(final String databaseName, final byte[] walData) {
    pageVersions.release(databaseName, null, walData);
  }

  /**
   * The reservations the engine's phase-1 check consults, resolved through the Raft server on every call: an in-place
   * Ratis restart replaces the state machine, and with it the ledger, without closing the databases, so a hook bound
   * to one state machine instance would keep consulting a ledger nothing writes to any more.
   */
  private static final class LedgerReservations implements PageVersionReservations {
    private final String             databaseName;
    private final RaftHAServer       raft;
    private final ArcadeStateMachine fallback;

    private LedgerReservations(final String databaseName, final RaftHAServer raft, final ArcadeStateMachine fallback) {
      this.databaseName = databaseName;
      this.raft = raft;
      this.fallback = fallback;
    }

    @Override
    public int reservedVersion(final PageId pageId) {
      final ArcadeStateMachine current = raft != null ? raft.getStateMachine() : null;
      return (current != null ? current : fallback).pageVersions.reservedVersion(databaseName, pageId.getFileId(),
          pageId.getPageNumber());
    }
  }

  /** The database an entry targets, or {@code null} when it cannot be resolved here (the entry is then refused). */
  private DatabaseInternal databaseForValidation(final String databaseName) {
    if (databaseName == null)
      return null;
    try {
      return databaseFor(databaseName);
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.FINE,
          "Cannot resolve database '%s' to validate a transaction entry before append: %s", databaseName, e.getMessage());
      return null;
    }
  }

  /**
   * The local page versions of a database, as the ledger's seed for pages no in-flight entry reserved. Also installs
   * the ledger on the database, so the leader's own phase-1 validation refuses a page an in-flight entry reserved
   * without waiting for the round trip (see {@link com.arcadedb.engine.PageVersionReservations}).
   */
  private PageVersionLedger.LocalVersions localVersionsOf(final DatabaseInternal db) {
    if (db.getEmbedded() instanceof LocalDatabase local && !(local.getPageVersionReservations() instanceof LedgerReservations))
      local.setPageVersionReservations(new LedgerReservations(local.getName(), raftHAServer, this));

    final FileManager fileManager = db.getFileManager();
    final PageManager pageManager = db.getPageManager();
    return (fileId, pageNumber) -> {
      // A file that is gone, or that a racing schema change replaced with something that is not paged, is the same
      // retryable conflict the engine's own version check raises for it.
      if (!fileManager.existsFile(fileId) || !(fileManager.getFile(fileId) instanceof PaginatedComponentFile file))
        throw new ConcurrentModificationException(
            "Concurrent modification on page " + fileId + "/" + pageNumber + " of database '" + db.getName() + "': the file with id "
                + fileId + " does not exist anymore. Please retry the operation");
      return pageManager.getMostRecentVersionOfPage(new PageId(db, fileId, pageNumber), file.getPageSize());
    };
  }

  /**
   * The reservations of a leader that steps down are dropped: an entry Ratis still had pending either commits under the
   * next leader and is applied here like any other, or is truncated and never reaches the pages, and either way the
   * local copies are the truth by the time this node can lead again (Ratis makes a leader ready only once it has
   * applied every earlier entry).
   */
  @Override
  public void notifyNotLeader(final Collection<TransactionContext> pendingEntries) throws IOException {
    super.notifyNotLeader(pendingEntries);
    pageVersions.clearAll();
  }

  @Override
  public void notifyLeaderReady() {
    super.notifyLeaderReady();
    pageVersions.clearAll();
  }


  /**
   * Applies a committed transaction entry to the local database, at its position in the log.
   * <p>
   * A transaction this node originated is published from the pages its committing thread prepared, right here, by
   * the apply thread (issue #6965): every node then writes its pages in log order, the leader included, and the
   * leader's own commit can no longer race the apply of a neighbouring entry. Should the committing thread have
   * withdrawn the transaction in the meantime (it gave up on an unknown replication outcome and rolled back), or
   * should this be a replay after a restart, the entry is applied from its own WAL bytes like any follower does: the
   * page-version guards in {@code applyChanges} make that idempotent. The context's origin marker plays no part
   * here: the entry is matched to the registered transaction by its bytes.
   */
  private void applyTxEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex,
      final PageVersionLedger.Pages pages) {
    final String databaseName = decoded.databaseName();
    // A transaction this node originated is recognised by its own bytes: the registered transaction carries the WAL
    // it shipped, and an entry is claimed only when it carries the same. No origin marker, client id or context is
    // needed for that, so it holds whatever happened to the leadership, the Raft client or the context in between.
    final LocalCommit local = localCommits.claim(databaseName, walTransactionIdOfCommittedEntry(decoded, entryIndex),
        decoded.walData());
    RuntimeException applyFailure = null;
    try {
      if (local != null)
        publishLocalCommit(local, decoded, entryIndex);
      else
        applyReplicatedTransaction(decoded, entryIndex);
    } catch (final RuntimeException e) {
      // Kept only so the release below can attach its own failure to this one instead of dropping it; rethrown
      // unchanged, so the apply path sees exactly what it saw before.
      applyFailure = e;
      throw e;
    } finally {
      // Applied, published or reconciled, the local copy of every page of this entry now carries its version, so the
      // reservation taken at append time has done its job. A no-op on a follower, whose ledger is empty.
      //
      // Nothing thrown here may escape (issue #7741): with no Pages in hand - a leader applying an entry it did
      // not append - release() parses them out of the WAL payload, and a payload that cannot be parsed is exactly
      // the state the try block has just failed on. Letting that throw would REPLACE the failure being reported,
      // and since #7495 that failure is the RaftLogEntryDecodeException whose whole purpose is to quarantine the
      // database instead of skipping the entry silently. The release is bookkeeping; the apply result is not.
      //
      // Since issue #7984 the decode case does not reach here at all: release() is total over the WAL payload,
      // because bytes it cannot parse are bytes no reservation was ever taken from (see its javadoc). This catch
      // is what is left - a guard against anything else the bookkeeping could ever raise, not the thing that
      // makes the decode failure survivable.
      try {
        pageVersions.release(databaseName, pages, decoded.walData());
      } catch (final RuntimeException e) {
        // Attached to the apply's own failure when there is one, so the pair is diagnosable from a single stack
        // trace, and logged when the apply succeeded and there is nothing to attach it to. Reservations left
        // behind are NOT swept: dropIfStale exempts a reservation confirmed at append time, which every
        // reservation of an entry that reached this apply is. They are evicted when the database's ledger is
        // cleared - a snapshot install, a database drop, or the next change of leadership - and until then they
        // sit at the version the local copy now carries, which neither fences the page in the engine's phase-1
        // check (it compares strictly greater) nor mis-validates the next entry on it.
        if (applyFailure != null)
          applyFailure.addSuppressed(e);
        else
          LogManager.instance().log(this, Level.WARNING,
              "Cannot release the page-version reservations of the Raft entry at index %d (db=%s): %s. They are evicted "
                  + "when this database's ledger is next cleared; the entry's own outcome is unaffected",
              e, entryIndex, databaseName, e.getMessage());
      }
    }
  }

  /**
   * The WAL transaction id of a COMMITTED transaction entry, read the way the apply path has to read it.
   * <p>
   * {@link #peekWalTransactionId(byte[])} reports a payload too short to hold the id as a {@link ReplicationException}.
   * Its type is left alone here because its other call sites read the id for a log line rather than to decide
   * anything ({@code RaftReplicatedDatabase.localWalTxId} catches it and carries on with a sentinel). On the apply
   * path it was the wrong answer: {@code applyWithRetry} rethrows a
   * {@code ReplicationException} unchanged (it is the resync signal applyTxEntry raises on a WAL gap), so a committed
   * entry whose payload is truncated bypassed {@link #handleUnexpectedApplyError} entirely - no quarantine, no targeted
   * snapshot resync, and the failed future Ratis swallows while advancing its own applied index. The corrupt entry was
   * therefore skipped on this node and nothing said so.
   * <p>
   * A truncated payload IS what {@link RaftLogEntryDecodeException} exists for (issue #7138): a committed entry of a
   * KNOWN type this version cannot read. Raised as one, it reaches {@code applyWithRetry}'s {@code RuntimeException}
   * branch, quarantines this one database and lets the leader resend it as a snapshot, exactly as a decode failure of
   * the envelope itself already does (issue #7495).
   */
  private static long walTransactionIdOfCommittedEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex) {
    try {
      return peekWalTransactionId(decoded.walData());
    } catch (final RuntimeException e) {
      throw decodeFailure(decoded, entryIndex, "read the WAL transaction id of", e);
    }
  }

  /**
   * The WAL transaction a committed entry carries, decoded the way the apply path has to decode it: the same
   * reclassification {@link #walTransactionIdOfCommittedEntry} applies, for the same reason. {@code
   * deserializeWalTransaction} rejects a misaligned page count or delta range with a {@link ReplicationException}
   * (issue #4420), and on this path that exception type means "resync already in progress" to
   * {@code applyWithRetry}, which rethrows it without quarantining anything.
   * <p>
   * The catch is on {@code RuntimeException} rather than on {@code ReplicationException} alone because those
   * explicit checks are not the only way the decode can fail: a payload long enough to hold the transaction id
   * (8 bytes) but shorter than the header the decoder reads (24) runs out of buffer first and raises a
   * {@code BufferUnderflowException}. Both shapes are one thing - a committed entry this node cannot read - and
   * both now say so, rather than the diagnosis depending on how far into the payload the corruption happened to
   * start. Nothing but the decode of a {@code byte[]} runs inside the try, so the wider catch cannot capture an
   * unrelated failure.
   */
  private static WALFile.WALTransaction walTransactionOfCommittedEntry(final RaftLogEntryCodec.DecodedEntry decoded,
      final long entryIndex) {
    return walTransactionOfCommittedEntry(decoded, decoded.walData(), entryIndex, RaftLogEntryType.TX_ENTRY, null);
  }

  /**
   * The same decode for a WAL payload a committed entry of any type carries, over an EXPLICIT {@code walData}
   * (issue #7695).
   * <p>
   * A {@code SCHEMA_ENTRY} can carry a whole batch of buffered WAL entries - the {@code recordFileChanges()} path
   * through {@code RaftReplicatedDatabase}'s schema WAL buffer - and {@code applySchemaEntry} decoded them with a
   * bare {@link #deserializeWalTransaction} call, which is the very bypass the one-payload form above exists to
   * close: a {@link ReplicationException} from a misaligned page count or delta range, or a
   * {@code BufferUnderflowException} from a payload shorter than the 24-byte header, left {@code applyWithRetry}
   * without ever reaching {@link #handleUnexpectedApplyError}, so the database was not quarantined, no targeted
   * snapshot resync was triggered, and the corrupt entry was skipped on this node with nothing saying so.
   *
   * @param which names the failing payload within a multi-payload entry, so the log line points at one buffered
   *              WAL entry of the batch rather than at "the entry". Null for an entry that carries exactly one
   */
  private static WALFile.WALTransaction walTransactionOfCommittedEntry(final RaftLogEntryCodec.DecodedEntry decoded,
      final byte[] walData, final long entryIndex, final RaftLogEntryType type, final String which) {
    try {
      return deserializeWalTransaction(walData);
    } catch (final RuntimeException e) {
      throw decodeFailure(decoded, entryIndex, "decode the WAL payload of", type, which, e);
    }
  }

  private static RaftLogEntryDecodeException decodeFailure(final RaftLogEntryCodec.DecodedEntry decoded,
      final long entryIndex, final String what, final RuntimeException cause) {
    return decodeFailure(decoded, entryIndex, what, RaftLogEntryType.TX_ENTRY, null, cause);
  }

  /**
   * The decode failure of a committed entry, typed as the entry that carried it (issue #7695). The type is what
   * routes the failure: {@link RaftLogEntryDecodeException} carries it to the per-database quarantine of issue
   * #7138 together with the database name, and a reader of the log needs to know WHICH kind of entry could not be
   * read - a transaction, or a DDL statement's buffered WAL - because the two say different things about what
   * the resync has to replace.
   */
  private static RaftLogEntryDecodeException decodeFailure(final RaftLogEntryCodec.DecodedEntry decoded,
      final long entryIndex, final String what, final RaftLogEntryType type, final String which,
      final RuntimeException cause) {
    return new RaftLogEntryDecodeException(
        "Cannot " + what + " the committed " + entryDescription(type) + " entry for database '"
            + decoded.databaseName() + "' at index " + entryIndex + (which == null ? "" : " (" + which + ")") + ": "
            + cause.getMessage(), type, decoded.databaseName(), cause);
  }

  /** How a decode failure names the entry it could not read. */
  private static String entryDescription(final RaftLogEntryType type) {
    return type == RaftLogEntryType.SCHEMA_ENTRY ? "schema" : "transaction";
  }

  /**
   * The local database an entry targets. The one seam an apply resolves a database through, so a unit test can
   * drive {@link #applyTransaction} - or {@link #applySchemaEntry}, which went through {@code server.getDatabase}
   * directly until issue #7695 needed a harness that could reach its buffered-WAL loop - against a database it
   * opened itself.
   */
  // @VisibleForTesting
  DatabaseInternal databaseFor(final String databaseName) {
    return (DatabaseInternal) server.getDatabase(databaseName);
  }

  /**
   * Publishes the prepared pages of a transaction this node originated. A failure is recorded on the claim for the
   * committing thread to surface, and the pages are reconciled from the entry's WAL bytes, which are what every other
   * node applied.
   * <p>
   * <b>When the reconcile fails too, this throws</b> (issue #7602). It used to log and return, and the cost of
   * that was the one thing an apply path must never do: {@code applyTransaction} advanced {@code lastAppliedIndex}
   * over an entry whose pages are not on this node, answered OK, and the next {@code takeSnapshot} checkpointed
   * the advanced position - so the entry was neither applied nor replayable, on the very node that originated it,
   * with one SEVERE line as the only evidence. The #5407 replay floor that used to keep such an entry replayable
   * was removed with the #6965 rework, which is why the swallow stopped being survivable.
   * {@link #applyReplicatedTransaction} throws on the identical double failure and reaches the quarantine, so
   * throwing here is what makes the two paths answer alike rather than a new disposition for this one.
   *
   * @throws LocalCommitNotAppliedException when the pages could neither be published nor reconciled
   */
  private void publishLocalCommit(final LocalCommit local, final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex) {
    HALog.log(this, HALog.DETAILED, "Publishing locally-originated tx %d on database '%s' at log index %d",
        local.walTxId(), decoded.databaseName(), entryIndex);
    boolean published = false;
    Throwable failure = null;
    boolean reconciled = false;
    Exception reconcileFailure = null;
    try {
      final Consumer<String> phase2Fault = RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT;
      if (phase2Fault != null)
        phase2Fault.accept(decoded.databaseName());

      local.transaction().publishCommittedPages(local.phase1());
      published = true;
    } catch (final Throwable t) {
      failure = t;
      if (t instanceof Error error)
        // An Error (out of memory, a linkage failure) is not something to reconcile from: the claim is still resolved
        // in the finally so the committing thread wakes, then the Error reaches applyTransaction's fatal-halt path.
        throw error;
      try {
        LogManager.instance().log(this, Level.SEVERE,
            "Publishing the pages of locally-originated tx %d on database '%s' failed at log index %d after the entry was "
                + "committed cluster-wide; reconciling the local pages from the replicated payload: %s",
            local.walTxId(), decoded.databaseName(), entryIndex, t.getMessage());
        databaseFor(decoded.databaseName()).getTransactionManager()
            .applyChanges(deserializeWalTransaction(decoded.walData()), decoded.bucketRecordDelta(), true);
        reconciled = true;
      } catch (final Error reconcileError) {
        throw reconcileError;
      } catch (final Exception reconcileError) {
        reconcileFailure = reconcileError;
        LogManager.instance().log(this, Level.SEVERE,
            "Reconciling the pages of tx %d on database '%s' from the replicated payload also failed: %s",
            local.walTxId(), decoded.databaseName(), reconcileError.getMessage());
      }
    } finally {
      // The committing thread waits on this claim without a timeout: whatever happened above, it is resolved here.
      // Resolved in the FINALLY, so it happens before the throw below as well: a committing thread parked on this
      // claim must be woken whether the apply thread goes on to quarantine the database or not.
      if (published)
        local.published();
      else
        local.failed(failure, reconciled);
    }

    // Neither the prepared pages nor the replicated payload reached this database, so this entry is NOT applied
    // here and must not be recorded as if it were (issue #7602). Thrown after the claim is resolved and outside
    // the finally, so it cannot replace an Error already on its way out of the block above.
    if (!published && !reconciled)
      throw localCommitNotApplied(local, decoded, entryIndex, failure, reconcileFailure);
  }

  /**
   * The failure a doubly-failed local publication is reported with (issue #7602), carrying both halves: the
   * publication failure as the cause, the reconcile failure suppressed on it. Two failures, one exception, so the
   * quarantine log line and the stack trace an operator reads describe the whole of what happened.
   */
  private static LocalCommitNotAppliedException localCommitNotApplied(final LocalCommit local,
      final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex, final Throwable publishFailure,
      final Exception reconcileFailure) {
    final LocalCommitNotAppliedException notApplied = new LocalCommitNotAppliedException(
        "Locally-originated tx " + local.walTxId() + " on database '" + decoded.databaseName() + "' at log index "
            + entryIndex + " could neither be published from the prepared transaction nor reconciled from the"
            + " replicated payload, so this node does not hold an entry the cluster committed: "
            + (publishFailure == null ? "no error detail" : publishFailure.getMessage()),
        publishFailure);
    if (reconcileFailure != null)
      notApplied.addSuppressed(reconcileFailure);
    return notApplied;
  }

  /**
   * Applies a transaction entry from its WAL bytes, the path every follower takes.
   * <p>
   * <b>{@code ignoreErrors=false} rationale:</b> a version gap here means an intermediate entry was never applied on
   * this node, which is state divergence: it triggers a snapshot resync instead of being skipped.
   */
  private void applyReplicatedTransaction(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex) {
    final DatabaseInternal db = databaseFor(decoded.databaseName());
    final WALFile.WALTransaction walTx = walTransactionOfCommittedEntry(decoded, entryIndex);

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
      // (issue #4740). quarantineDatabase() returns true only when the database was not already quarantined, so
      // the FIRST gap logs loudly and triggers an immediate snapshot download (instead of waiting for
      // the HealthMonitor's periodic check). Every subsequent committed entry for this database will
      // hit the same gap until the resync lands: those log a throttled one-liner (no per-entry stack
      // trace) so the log is not flooded and the download is not starved of CPU/IO on small nodes.
      if (quarantineDatabase(decoded.databaseName(), DivergenceCause.WAL_VERSION_GAP)) {
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
  // @VisibleForTesting - the database is resolved through the databaseFor() seam below so a test can drive this
  // against a database it opened itself, the way applyTxEntry's null-server harness already can (issue #7695).
  void applySchemaEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex,
      final boolean originatedLocally) {
    // Same origin-tracking as applyTxEntry: skip if this node originated the entry in the
    // current lifecycle (schema changes were already applied locally during the transaction).
    if (originatedLocally) {
      HALog.log(this, HALog.TRACE, "Skipping schema apply on originator for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = databaseFor(decoded.databaseName());

    HALog.log(this, HALog.DETAILED,
        "Applying schema entry to database '%s' (entryIndex=%d): filesToAdd=%d, filesToRemove=%d, schemaPayload=%s",
        decoded.databaseName(), entryIndex,
        decoded.filesToAdd() != null ? decoded.filesToAdd().size() : 0,
        decoded.filesToRemove() != null ? decoded.filesToRemove().size() : 0,
        decoded.schemaDelta() != null ? "delta" :
            decoded.schemaJson() != null && !decoded.schemaJson().isEmpty() ? "document" : "none");

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
    // A schema delta (issue #6989) disqualifies an entry from this shortcut for the same reason it disqualifies
    // it from walOnlyEntry below: the change is carried OUTSIDE schemaJson, so an entry holding one does publish
    // a schema change and does need the reload. No producer ships both today - the compaction path always
    // carries the whole document - and this keeps that from becoming a silent skip if one ever does.
    final boolean sealedOnlyEntry = isEmptyMap(decoded.filesToAdd()) && isEmptyMap(decoded.filesToRemove())
        && decoded.schemaDelta() == null
        && (isNotEmpty(decoded.sealedFileBlobs()) || isNotEmpty(decoded.sealedFileChunks()));

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

    final SchemaEntryRecorder schemaEntryRecorder = TEST_SCHEMA_ENTRY_COUNTER;
    if (schemaEntryRecorder != null && !deliveryOnlyEntry)
      schemaEntryRecorder.record(entryIndex);

    // A commit that ran inside a recordFileChanges() callback but created no file and left the schema
    // version untouched ships as a SCHEMA_ENTRY carrying nothing but WAL, because the buffering in
    // RaftReplicatedDatabase.commit() is what preserves ordering against the enclosing DDL. Such an
    // entry has nothing for load() to pick up - applyChanges below already updates page counts through
    // getFileByIdIfExists() - so the reload is pure cost on the single Raft apply thread, where it
    // re-instantiates every TimeSeries engine and closes shard executors with a 30s awaitTermination.
    // Same reasoning as sealedOnlyEntry above.
    //
    // A delta entry (issue #6989) carries its schema change OUTSIDE schemaJson, so it must not be mistaken for
    // one of these: it publishes a schema change and the reload below is exactly what registers it.
    final boolean walOnlyEntry = isEmptyMap(decoded.filesToAdd()) && isEmptyMap(decoded.filesToRemove())
        && (decoded.schemaJson() == null || decoded.schemaJson().isEmpty()) && decoded.schemaDelta() == null
        && !isNotEmpty(decoded.sealedFileBlobs()) && !isNotEmpty(decoded.sealedFileChunks())
        && decoded.walEntries() != null && !decoded.walEntries().isEmpty();

    // Hold the compaction write lock of every shard this entry installs sealed bytes for, from before the
    // install until after the WAL that clears the matching mutable bucket (issue #7337). The leader ships the
    // two together so they are atomic with respect to each other, but the follower applying them is also a node
    // a backup or a snapshot ship can be running on, and THAT pairing was unguarded: TimeSeriesCompactionPause
    // holds each shard's compaction READ lock, which excludes a local compaction and excluded nothing here,
    // because installSealedFile takes only the store's own directoryLock. A copy taken across this window could
    // capture a pre-clear page image with a post-install sealed image and restore with every one of those
    // samples twice, silently. Taking the same lock a local compaction takes is what makes the pause mean on a
    // follower what it already means on a standalone database.
    try (final TimeSeriesSealedInstallLock sealedInstallLock = TimeSeriesSealedInstallLock.acquire(db,
        sealedShardsOf(decoded), SEALED_INSTALL_LOCK_TIMEOUT_MS)) {
      if (decoded.filesToAdd() != null)
        createNewFiles(db, decoded.filesToAdd());

      // Install any TimeSeries sealed-store blobs BEFORE applying the WAL (issue #4382). The WAL
      // below carries the mutable-bucket clear; installing the sealed file first guarantees a query
      // never observes "cleared mutable + stale sealed" (the data-loss window).
      applySealedBlobs(db, decoded.sealedFileBlobs());

      // A sealed store too large for one entry arrives as an ordered sequence of slices (issue #4416). Every
      // slice but the last only stages bytes; the last one installs the reassembled file, and it rides THIS
      // entry - the publishing one - so the install still happens before the clear WAL below, closing the same
      // data-loss window applySealedBlobs closes for a store that fits inline.
      applySealedChunks(db, decoded.sealedFileChunks());

      if (decoded.schemaDelta() != null)
        applySchemaDelta(db, decoded);
      else if (!sealedOnlyEntry && decoded.schemaJson() != null && !decoded.schemaJson().isEmpty())
        db.getSchema().getEmbedded().update(new JSONObject(decoded.schemaJson()));

      // File ids this entry wrote pages into. The incremental refresh below re-runs the load hooks of the
      // already-registered components among them, which is what re-reads an LSM mutable index' page 0 - the full
      // rebuild used to get that for free by re-instantiating every component in the database (#6988).
      final Set<Integer> walTouchedFileIds = new HashSet<>();

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
          // Wrapped, not bare (issue #7695): a ReplicationException from a misaligned page count or delta range,
          // or a BufferUnderflowException from a payload shorter than the header, would otherwise leave
          // applyWithRetry through its ReplicationException arm - which rethrows unchanged as a resync signal -
          // and skip the per-database quarantine entirely, so the corrupt entry was dropped on this node and
          // nothing said so. Named per buffered entry: a schema entry carries a batch of them.
          final WALFile.WALTransaction walTx = walTransactionOfCommittedEntry(decoded, walData, entryIndex,
              RaftLogEntryType.SCHEMA_ENTRY, "buffered WAL entry " + (i + 1) + " of " + walEntries.size());
          if (walTx.pages != null)
            for (final WALFile.WALPage page : walTx.pages)
              walTouchedFileIds.add(page.fileId);
          // ignoreErrors=true: same rationale as applyTxEntry - replay safety during node restart
          db.getTransactionManager().applyChanges(walTx, bucketDelta, true);
        }
        HALog.log(this, HALog.DETAILED,
            "Applied %d buffered WAL entries from schema entry to database '%s'",
            walEntries.size(), decoded.databaseName());
      }

      // RELEASED HERE AND NOT AT THE END OF THE BLOCK: the span that has to be indivisible is [sealed image
      // installed, mutable bucket cleared], and it closes with the WAL above. What follows - retiring superseded
      // files, reloading the schema - touches no sealed store, and schema.load() re-instantiates every TimeSeries
      // engine and closes its shard executors, which is not work to be doing while holding a shard's own
      // compaction lock (issue #7337). Idempotent, so the try-with-resources below is still the safety net on
      // every path out of here.
      sealedInstallLock.close();

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
      //
      // The refresh is INCREMENTAL whenever the entry can be expressed that way (#6988): load() re-instantiates a
      // Component for every file in the database and reads page 0 of each, so running it once per DDL statement
      // costs O(entries x total files) - quadratic in the number of types, and all of it serialized on the single
      // Ratis apply thread (a 1209-type schema took ~2h53m to replicate, issue #6982). loadIncremental() touches
      // only the files this entry created or wrote to, and refuses - returning false, having changed nothing - for
      // every entry whose effect it cannot express (a retired file, a compacted index, a bloom filter, a new
      // dictionary), which is what keeps the ordering guarantees of #4743 and #5443 on the full-rebuild path.
      if (!sealedOnlyEntry && !deliveryOnlyEntry && !walOnlyEntry) {
        final LocalSchema schema = db.getSchema().getEmbedded();
        final boolean incremental = incrementalSchemaApplyEnabled()
            && schema.loadIncremental(ComponentFile.MODE.READ_WRITE, keysOrNull(decoded.filesToRemove()),
            walTouchedFileIds);
        if (!incremental)
          schema.load(ComponentFile.MODE.READ_WRITE, true);
      }

    } catch (final IOException e) {
      throw new RuntimeException("Failed to apply schema entry for database '" + decoded.databaseName() + "'", e);
    }

    HALog.log(this, HALog.DETAILED, "Applied schema change to database '%s'", decoded.databaseName());
  }

  /**
   * The shards a schema entry installs sealed bytes for, inline or sliced (issue #7337). A slice that only
   * stages bytes is included along with the one that installs: locking a shard that turns out not to be replaced
   * by this entry costs one uncontended lock, and deciding it from {@code last} would make the lock depend on a
   * flag the decoder could mis-set.
   * <p>
   * Package-private so a test can pin that BOTH carriers are covered: an inline blob and a slice sequence
   * install the same file, and covering only the first would leave every sealed store too large for one Raft
   * entry - the ones a tear costs most on - unguarded.
   */
  static List<TimeSeriesSealedInstallLock.ShardRef> sealedShardsOf(
      final RaftLogEntryCodec.DecodedEntry decoded) {
    final List<TimeSeriesSealedInstallLock.ShardRef> shards = new ArrayList<>();
    if (decoded.sealedFileBlobs() != null)
      for (final RaftLogEntryCodec.TsSealedBlob blob : decoded.sealedFileBlobs())
        shards.add(new TimeSeriesSealedInstallLock.ShardRef(blob.typeName(), blob.shardIndex()));
    if (decoded.sealedFileChunks() != null)
      for (final RaftLogEntryCodec.TsSealedChunk chunk : decoded.sealedFileChunks())
        shards.add(new TimeSeriesSealedInstallLock.ShardRef(chunk.typeName(), chunk.shardIndex()));
    return shards;
  }

  /**
   * Applies a schema change that arrived as a DELTA rather than as a whole document (issue #6989): merges it
   * into this node's own schema and hands the result to {@code LocalSchema.update()}, which is the same call the
   * whole-document path makes, so everything downstream of it - the file rewrite, the plan-cache invalidation
   * and the {@code load()} below - is unchanged.
   * <p>
   * <b>{@code baseVersion} is a diagnostic, not a gate.</b> A follower's {@code versionSerial} legitimately runs
   * AHEAD of the document the leader shipped: the {@code load()} that follows every schema entry re-saves the
   * schema whenever it repaired anything, and each save increments the counter. Refusing on a mismatch would
   * therefore refuse the common case - and there is nothing to refuse INTO, because a delta entry carries no
   * whole document to fall back to.
   * <p>
   * What makes that safe is the delta itself rather than the version: it carries the leader's authoritative key
   * sets, so the merged document has the leader's structure whatever the receiver started from, and only the
   * content of children the leader considered unchanged is inherited locally. Genuine divergence stays the
   * business of the WAL-version-gap detection and {@code checkDatabase}, which is where it was before this
   * entry type existed.
   */
  // @VisibleForTesting
  void applySchemaDelta(final DatabaseInternal db, final RaftLogEntryCodec.DecodedEntry decoded)
      throws IOException {
    final SchemaDelta.Payload delta = decoded.schemaDelta();
    final LocalSchema schema = db.getSchema().getEmbedded();

    HALog.log(this, HALog.DETAILED,
        "Applying a %d-char schema delta to database '%s' (leader base version %d, local version %d)",
        delta.deltaJson().length(), db.getName(), delta.baseVersion(), schema.getVersion());

    final JSONObject merged = SchemaDelta.apply(schema.toJSON(), new JSONObject(delta.deltaJson()));

    // The MERGED document is what this entry publishes, so it - not the empty schemaJson slot - is what the
    // #4083 cross-reference has to look at. Only reached at DETAILED.
    if (HALog.isEnabled(HALog.DETAILED))
      logFollowerSchemaPayloadDiagnostics(db.getName(), merged.toString(), decoded.filesToAdd());

    schema.update(merged);
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
   * <p>
   * <b>A blob this node cannot put in place raises</b> (issues #8070 and #8172), after every other blob in the
   * entry has been attempted. See {@link SealedStoreNotInstalledException} for why it must: a Raft entry is
   * applied once and never re-shipped, so logging the failure and carrying on consumed the one payload that could
   * have repaired the type and left it engine-less for good. That covers all three ways a blob can fail to land -
   * a failed engine repair (#8070), a type this node does not have, and a type that exists but is not a
   * TIMESERIES one. The last two are guarded by a Raft-ordering invariant (the type-creation entry carries a
   * lower index and is applied first), so they should not happen; #8172 is about what it costs when the invariant
   * does not hold - for example a rolling upgrade shipping a type this node's build cannot construct - which is a
   * silent, permanent divergence on replicated data against one SEVERE log line.
   */
  // Package-private rather than private so Issue6839TsSealedBlobRecoveryTest can drive the apply path directly:
  // the recovery it pins is entirely inside this method, and a 3-node IT would only add flakiness to prove it.
  void applySealedBlobs(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedBlob> blobs)
      throws IOException {
    if (blobs == null || blobs.isEmpty())
      return;
    // Accumulated rather than thrown on the spot: the entry may carry blobs for other types, and one unrepairable
    // type must not stop the ones that CAN be installed from being installed (issue #8070).
    final List<String> unrepaired = new ArrayList<>();
    for (final RaftLogEntryCodec.TsSealedBlob blob : blobs) {
      final LocalSchema schema = db.getSchema().getEmbedded();
      if (!schema.existsType(blob.typeName())) {
        // Should not happen: the type-creation entry has a lower Raft index and is applied first. When it DOES
        // happen the payload is consumed and never re-shipped, so the entry is refused rather than checkpointed
        // over a sealed store this node never installed (issue #8172, the rule #7602 wrote down).
        LogManager.instance().log(this, Level.SEVERE,
            "Received TimeSeries sealed blob for unknown type '%s' (db=%s); refusing the entry", null,
            blob.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, blob.typeName(), blob.shardIndex(), "unknown type");
        continue;
      }
      if (!(schema.getType(blob.typeName()) instanceof LocalTimeSeriesType tsType)) {
        LogManager.instance().log(this, Level.SEVERE,
            "Received TimeSeries sealed blob for non-timeseries type '%s' (db=%s); refusing the entry", null,
            blob.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, blob.typeName(), blob.shardIndex(), "not a TIMESERIES type");
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
        else
          addUnrepaired(unrepaired, blob.typeName(), blob.shardIndex(), "engine repair failed");
        continue;
      }
      tsType.getEngine().getShard(blob.shardIndex()).getSealedStore().installSealedFileBytes(blob.bytes());
      HALog.log(this, HALog.DETAILED, "Installed TimeSeries sealed blob for %s shard %d (%d bytes) on db '%s'",
          blob.typeName(), blob.shardIndex(), blob.bytes().length, decodedDbName(db));
    }

    if (!unrepaired.isEmpty())
      throw sealedStoreNotInstalled(db, unrepaired, "blob");
  }

  /**
   * Records one (type, shard) the entry could not put in place, tagged with WHY, for the refusal raised after the
   * loop. De-duplicated because one entry can carry several slices of the same (type, shard) and an operator
   * reading the refusal needs the list of what is missing, not a list of how many payloads named it.
   */
  private static void addUnrepaired(final List<String> unrepaired, final String typeName, final int shardIndex,
      final String reason) {
    final String entry = typeName + " shard " + shardIndex + " (" + reason + ")";
    if (!unrepaired.contains(entry))
      unrepaired.add(entry);
  }

  /**
   * The refusal a consumed-and-unusable sealed payload is reported with (issue #8070).
   * <p>
   * Names every (type, shard) the entry could not put in place, because the entry is refused as a whole and an
   * operator reading one line has to see the whole of what this node is missing. The individual failures have
   * already been logged at SEVERE with their stack traces by {@code repairEngineWithSealedFile}; this is the
   * sentence that says what it COSTS, which is the half the per-failure log cannot know.
   */
  private static SealedStoreNotInstalledException sealedStoreNotInstalled(final DatabaseInternal db,
      final List<String> unrepaired, final String shipment) {
    return new SealedStoreNotInstalledException(
        "The replicated TimeSeries sealed store shipped as a " + shipment + " could not be installed on database '"
            + decodedDbName(db) + "' for " + String.join(", ", unrepaired)
            + ". A Raft entry is applied once and never re-shipped, so this entry must NOT be recorded as applied:"
            + " quarantining the database and resyncing it from the leader is the only path back (issue #8070)");
  }

  /**
   * Reassembles and installs a sealed store the leader shipped SLICED because it does not fit one Raft entry
   * (issue #4416), the counterpart of {@link #applySealedBlobs}.
   * <p>
   * WHY THE STAGING FILE IS ON DISK and not a buffer in this state machine. The slices arrive as separate Raft
   * entries, and this node persists its applied index between them, so a follower that restarts mid-sequence must
   * not need the earlier slices again - it will never be sent them. A file survives that restart; a field does
   * not. It is also what keeps a multi-gigabyte sealed store from having to fit in heap on the way in.
   * <p>
   * WHAT MAKES THE SEQUENCE SAFE. Raft applies entries in index order on every node, so the slices arrive in the
   * order the leader cut them. {@code offset == 0} TRUNCATES the staging file - through the write handle, so the
   * truncation cannot be silently skipped the way a failed delete could - which is what makes a sequence abandoned
   * by a leader that died mid-shipment cost nothing: the next leader's first slice discards whatever it left. Any
   * other slice must find the staging file exactly {@code offset} bytes long - anything else means the
   * sequence this node holds is not the sequence the leader is sending, and the only honest answer is to stop
   * trusting local state and resync, which is what {@link ReplicationException} asks for (it is caught in
   * {@code applyWithRetry} and turned into a targeted snapshot resync rather than halting the node).
   * <p>
   * The last slice is verified against the WHOLE file the leader hashed before it is installed: length first,
   * then CRC32. Per-slice CRCs are already checked by the decoder, but they only prove each piece survived the
   * wire, not that what this node assembled is what the leader had.
   * <p>
   * ONLY THE LAST SLICE IS FSYNCED, and the earlier ones deliberately are not. A power loss between applying a
   * slice and the OS flushing it can therefore leave a staging file SHORTER than this node's persisted applied
   * index implies. That is not a hole, it is what the checks above are for: the next slice finds a staging file
   * that is not {@code offset} bytes long, or the last one finds the wrong length or CRC, and either way the
   * sequence is refused and the database resyncs rather than installing a truncated store. Paying an fsync per
   * slice would buy a faster recovery from an event that already costs a restart, at the price of one flush per
   * entry on the single Raft apply thread for every sliced compaction. The final slice IS synced because after it
   * the file is moved into place and nothing checks it again.
   * <p>
   * The install itself is the one {@code applySealedBlobs} performs - an atomic move onto the store's file - and
   * the target path is derived from THIS node's schema, never from the file name in the payload: a name arriving
   * over the wire has no business selecting a path here.
   */
  // Package-private for the same reason as applySealedBlobs: the whole mechanism lives in this method, and a
  // 3-node IT can prove it end to end but cannot pin the broken-sequence arms without contriving a failure.
  void applySealedChunks(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedChunk> chunks)
      throws IOException {
    if (chunks == null || chunks.isEmpty())
      return;
    // Same accumulation as applySealedBlobs, for the same reason (issue #8070).
    final List<String> unrepaired = new ArrayList<>();
    for (final RaftLogEntryCodec.TsSealedChunk chunk : chunks) {
      final LocalSchema schema = db.getSchema().getEmbedded();
      if (!schema.existsType(chunk.typeName())) {
        // Should not happen: the type-creation entry has a lower Raft index and is applied first. Refused, not
        // stepped over, for the reason the blob path gives (issue #8172).
        LogManager.instance().log(this, Level.SEVERE,
            "Received TimeSeries sealed slice for unknown type '%s' (db=%s); refusing the entry", null,
            chunk.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, chunk.typeName(), chunk.shardIndex(), "unknown type");
        continue;
      }
      if (!(schema.getType(chunk.typeName()) instanceof LocalTimeSeriesType tsType)) {
        LogManager.instance().log(this, Level.SEVERE,
            "Received TimeSeries sealed slice for non-timeseries type '%s' (db=%s); refusing the entry", null,
            chunk.typeName(), decodedDbName(db));
        addUnrepaired(unrepaired, chunk.typeName(), chunk.shardIndex(), "not a TIMESERIES type");
        continue;
      }

      final File target = new File(db.getDatabasePath(),
          TimeSeriesSealedStore.sealedFileNameFor(tsType.getName(), chunk.shardIndex()));
      final File staging = new File(target.getPath() + SEALED_STAGING_SUFFIX);

      if (chunk.offset() > 0L && (!staging.exists() || staging.length() != chunk.offset()))
        throw new ReplicationException(String.format(
            "TimeSeries sealed slice for '%s' shard %d (db=%s) starts at offset %d but this node has staged %s; "
                + "the slice sequence is broken, resyncing the database from the leader",
            chunk.typeName(), chunk.shardIndex(), decodedDbName(db), chunk.offset(),
            staging.exists() ? staging.length() + " bytes" : "nothing"));

      try (final RandomAccessFile out = new RandomAccessFile(staging, "rw")) {
        // The first slice truncates, and it does so through the open handle rather than by deleting the file
        // first. Deleting was the obvious way to write this and it is the wrong one: a delete that fails - a
        // handle another process still holds on Windows, a permissions hiccup - can only be logged and stepped
        // over, and RandomAccessFile.write never SHRINKS a file, so the tail of a longer abandoned sequence
        // would survive underneath the new one. The guards below still catch that (the next slice's offset
        // check, or the final length check), but only by forcing a full resync - which is precisely the cost
        // "an abandoned sequence costs nothing" says a restart does not pay. setLength cannot be stepped over:
        // it truncates or it throws, and throwing here IS the resync signal.
        if (chunk.offset() == 0L)
          out.setLength(0);
        out.seek(chunk.offset());
        out.write(chunk.bytes());
        if (chunk.last())
          out.getFD().sync();
      }

      if (!chunk.last()) {
        HALog.log(this, HALog.DETAILED,
            "Staged TimeSeries sealed slice for %s shard %d at offset %d (%d bytes of %d) on db '%s'",
            chunk.typeName(), chunk.shardIndex(), chunk.offset(), chunk.bytes().length, chunk.fileLength(),
            decodedDbName(db));
        continue;
      }

      final long stagedLength = staging.length();
      if (stagedLength != chunk.fileLength()) {
        deleteSealedStagingFile(staging);
        throw new ReplicationException(String.format(
            "TimeSeries sealed store for '%s' shard %d (db=%s) reassembled to %d bytes but the leader shipped %d; "
                + "resyncing the database from the leader",
            chunk.typeName(), chunk.shardIndex(), decodedDbName(db), stagedLength, chunk.fileLength()));
      }

      final long assembledCrc = crc32Of(staging);
      if (assembledCrc != chunk.fileCrc()) {
        deleteSealedStagingFile(staging);
        throw new ReplicationException(String.format(
            "TimeSeries sealed store for '%s' shard %d (db=%s) reassembled with CRC %d, the leader shipped %d; "
                + "resyncing the database from the leader",
            chunk.typeName(), chunk.shardIndex(), decodedDbName(db), assembledCrc, chunk.fileCrc()));
      }

      if (tsType.getEngine() == null) {
        // Same repair as a whole-file blob performs (issue #6839): the slices ARE the authoritative copy of the
        // file whose failure to open left this type without an engine, so they go down first and initEngine()
        // opens the store over them.
        if (repairEngineWithSealedFile(db, tsType, chunk.shardIndex(), staging))
          HALog.log(this, HALog.DETAILED,
              "Repaired TimeSeries type %s shard %d from a %d-byte replicated sealed store shipped in slices on db '%s'",
              chunk.typeName(), chunk.shardIndex(), stagedLength, decodedDbName(db));
        else
          addUnrepaired(unrepaired, chunk.typeName(), chunk.shardIndex(), "engine repair failed");
        continue;
      }

      tsType.getEngine().getShard(chunk.shardIndex()).getSealedStore().installSealedFile(staging);
      HALog.log(this, HALog.DETAILED,
          "Installed TimeSeries sealed store for %s shard %d (%d bytes, shipped in slices) on db '%s'",
          chunk.typeName(), chunk.shardIndex(), stagedLength, decodedDbName(db));
    }

    // The sliced path had the identical defect, and it is the worse half of it: the slices were assembled, the
    // whole file was verified against the leader's length and CRC, and the last entry of the sequence was then
    // checkpointed over a repair that did not happen (issue #8070).
    if (!unrepaired.isEmpty())
      throw sealedStoreNotInstalled(db, unrepaired, "slice sequence");
  }

  /** Where a sealed store shipped in slices is reassembled, beside the file it will replace. */
  static final String SEALED_STAGING_SUFFIX = ".parts";

  private static long crc32Of(final File file) throws IOException {
    final CRC32 crc = new CRC32();
    final byte[] buffer = new byte[64 * 1024];
    try (final RandomAccessFile in = new RandomAccessFile(file, "r")) {
      for (int read = in.read(buffer); read > 0; read = in.read(buffer))
        crc.update(buffer, 0, read);
    }
    return crc.getValue();
  }

  /**
   * Removes a staging file a refused reassembly left behind. BEST EFFORT, and unlike the first slice's truncation
   * that is fine here: nothing downstream depends on this succeeding. The next sequence starts with an
   * {@code offset == 0} slice, which truncates through its own write handle whether or not this delete worked, so
   * a failure costs disk until the next compaction and never correctness. That is exactly the difference from the
   * first-slice case, where a silently skipped delete DID leave a longer sequence's tail under a shorter one.
   */
  private void deleteSealedStagingFile(final File staging) {
    if (staging.exists() && !staging.delete())
      LogManager.instance().log(this, Level.WARNING,
          "Failed to delete stale TimeSeries sealed staging file '%s'; it costs disk until the next slice "
              + "sequence truncates it, and nothing else depends on it being gone", null, staging.getAbsolutePath());
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
   * A failure here is logged and REPORTED, never thrown: one unrepairable type must not abort the apply of an
   * entry that may carry blobs for others, and the state it leaves behind is the state it started from - still
   * visible, still reported by {@code CHECK DATABASE}, still failing loudly on every read and write.
   * <p>
   * Reporting it is not the same as tolerating it. The caller collects what could not be repaired, finishes the
   * entry's other blobs, and then raises {@link SealedStoreNotInstalledException} so the entry is not checkpointed
   * as applied over a repair that did not happen (issue #8070) - a Raft entry is applied once and never
   * re-shipped, so the blob that was the repair would otherwise be consumed with nothing left to resend it.
   */
  private boolean repairEngineWithSealedBlob(final DatabaseInternal db, final LocalTimeSeriesType tsType,
      final RaftLogEntryCodec.TsSealedBlob blob) {
    // tsType.getName(), not blob.typeName(): the two are equal here - tsType came from getType(blob.typeName()) -
    // but taking it from the resolved schema type means the name selecting a file on this node provably came from
    // the local schema and not from the wire, rather than only doing so as long as the caller keeps that lookup.
    final File target = new File(db.getDatabasePath(),
        TimeSeriesSealedStore.sealedFileNameFor(tsType.getName(), blob.shardIndex()));
    final File incoming = new File(target.getPath() + ".incoming");
    try (final FileOutputStream out = new FileOutputStream(incoming)) {
      out.write(blob.bytes());
      out.getFD().sync();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Received TimeSeries sealed blob for type '%s' shard %d (db=%s) whose storage engine is unavailable, and "
              + "the engine could not be initialised over it: %s", e, blob.typeName(), blob.shardIndex(),
          decodedDbName(db), e.getMessage());
      return false;
    }
    return repairEngineWithSealedFile(db, tsType, blob.shardIndex(), incoming);
  }

  /**
   * The half of {@link #repairEngineWithSealedBlob} that runs once the leader's copy of the sealed file is a file
   * on this node: move it into place and re-run {@code initEngine()} over it.
   * <p>
   * Shared with the sliced path (issue #4416), which reassembles the leader's copy on disk rather than in heap and
   * so arrives here with a path instead of an array. {@code source} is CONSUMED on success.
   * <p>
   * A repaired type is also re-scheduled for maintenance (issue #6948). The only other place that schedules an
   * existing type is {@code LocalSchema.readConfiguration()}, and it skips precisely the types this method
   * repairs: at schema load their engine was unavailable, so the gate there never fired for them. Without this
   * call the type comes back readable and writable yet permanently unmaintained for the life of the process -
   * {@code compactAll()}, {@code applyRetention()} and {@code applyDownsampling()} have no other caller, so the
   * mutable bucket grows unbounded and configured retention and downsampling silently stop being applied. The
   * leader-only skip that makes this harmless on a follower lives INSIDE the recurring task, not in
   * {@code schedule()}, so a healthy follower keeps a ticking task ready for the moment it is elected; a repaired
   * one would have none. {@code schedule()} replaces any existing task for the type name, so a repeated repair is
   * safe.
   */
  private boolean repairEngineWithSealedFile(final DatabaseInternal db, final LocalTimeSeriesType tsType,
      final int shardIndex, final File source) {
    final File target = new File(db.getDatabasePath(),
        TimeSeriesSealedStore.sealedFileNameFor(tsType.getName(), shardIndex));
    try {
      Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      tsType.initEngine();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Received TimeSeries sealed store for type '%s' shard %d (db=%s) whose storage engine is unavailable, and "
              + "the engine could not be initialised over it: %s", e, tsType.getName(), shardIndex,
          decodedDbName(db), e.getMessage());
      return false;
    }

    if (!tsType.isEngineAvailable()) {
      LogManager.instance().log(this, Level.SEVERE,
          "TimeSeries type '%s' (db=%s) still has no storage engine after installing the replicated sealed store "
              + "for shard %d; skipping", null, tsType.getName(), decodedDbName(db), shardIndex);
      return false;
    }

    scheduleMaintenanceAfterRepair(db, tsType);
    return true;
  }

  /**
   * Re-arms automatic compaction, retention and downsampling for a type that has just been repaired (issue #6948).
   * <p>
   * Scheduled with {@code schema.getDatabase()} rather than the {@code db} parameter, so this task holds the same
   * instance the two pre-existing {@code schedule()} call sites hold: the one {@code LocalSchema} was built with,
   * which lives exactly as long as the schema does. {@code db} here is the server's wrapper, and wrappers are
   * replaceable - a task holding a superseded one through the scheduler's {@code WeakReference} would cancel
   * itself the moment that wrapper became garbage, which is this very bug again by another route. The replication
   * flags the recurring task needs are NOT taken from this reference: {@code runMaintenance} resolves
   * {@code getWrappedDatabaseInstance()} on every tick, for the same reason the compaction path underneath it
   * does.
   * <p>
   * Kept off the success path's error handling on purpose: the repair itself has already succeeded and the type is
   * usable again, so failing to ALSO schedule it must be logged and swallowed rather than turned into "the repair
   * failed" - the data is in place either way, and the state a thrown exception would leave is strictly worse than
   * the one it would be reporting.
   * <p>
   * The catch is deliberately wider than the {@code RejectedExecutionException} that
   * {@code LocalSchema.readConfiguration()} catches at its own {@code schedule()} call site, and the difference is
   * the caller, not the callee. That one runs during a database open, where an escaping runtime exception fails the
   * open and says so. This one runs inside the Raft apply path, whose whole contract here is that one type must not
   * abort the apply of an entry that may carry blobs for others - the same reason
   * {@link #repairEngineWithSealedBlob} reports failure rather than throwing. So no <em>exception</em> may escape,
   * a programming error included.
   * <p>
   * {@code Exception} and not {@code Throwable}, deliberately: an {@code Error} says the JVM itself is no longer in
   * a state this node can reason about, and the apply path's "do not let one type abort the entry" contract is not
   * a licence to keep applying Raft entries through one. Errors still propagate.
   * <p>
   * What IS swallowed is logged at SEVERE, with its stack trace and its exception class, and names the consequence
   * precisely, so it does not disappear: swallowing it here must not also hide it, and what it leaves behind - a
   * type maintained by nothing - is the very defect this method exists to prevent. That is louder than the sibling
   * catch in {@code LocalSchema.readConfiguration()} on purpose, and for the same reason the catch is wider: there
   * the alternative was the open failing loudly on its own, here nothing else will ever say a word.
   */
  private void scheduleMaintenanceAfterRepair(final DatabaseInternal db, final LocalTimeSeriesType tsType) {
    try {
      final LocalSchema schema = db.getSchema().getEmbedded();
      schema.getTimeSeriesMaintenanceScheduler().schedule(schema.getDatabase(), tsType);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Repaired TimeSeries type '%s' (db=%s) but could not re-schedule its automatic maintenance; compaction, "
              + "retention and downsampling stay off for it until the database is reopened: %s: %s", e,
          tsType.getName(), decodedDbName(db), e.getClass().getSimpleName(), e.getMessage());
    }
  }

  private static String decodedDbName(final DatabaseInternal db) {
    return db != null ? db.getName() : "?";
  }

  private static boolean isEmptyMap(final Map<?, ?> map) {
    return map == null || map.isEmpty();
  }

  private static boolean isNotEmpty(final List<?> list) {
    return list != null && !list.isEmpty();
  }

  private static Set<Integer> keysOrNull(final Map<Integer, String> map) {
    return map == null ? null : map.keySet();
  }

  /**
   * Safety valve for {@link LocalSchema#loadIncremental} (issue #6988). Read per entry rather than cached so an
   * operator can turn the incremental refresh off on a running server and have the very next applied entry go back
   * to the full rebuild, without a restart.
   */
  // @VisibleForTesting - Issue6988SchemaIncrementalApplySettingTest pins the "read per entry, never cached" half,
  // which is the half a reader cannot tell from the call site and which Issue6988FullRebuildFallbackIT cannot show
  // (it sets the value once, at server start).
  boolean incrementalSchemaApplyEnabled() {
    // The server-scoped value when this state machine is wired to one, the global default otherwise: tests drive
    // applySchemaEntry with no server attached, and they must exercise the same path production does.
    final ArcadeDBServer currentServer = server;
    return currentServer != null ?
        currentServer.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY) :
        GlobalConfiguration.HA_SCHEMA_INCREMENTAL_APPLY.getValueAsBoolean();
  }

  // @VisibleForTesting
  void applyInstallDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex) {
    final String databaseName = decoded.databaseName();
    final boolean forceSnapshot = decoded.forceSnapshot();
    // An install replaces the database's files, so no reservation taken against the previous copy can still hold.
    pageVersions.clear(databaseName);

    if (forceSnapshot) {
      // Replay guard (issue #7143). Ratis re-feeds every entry between the last snapshot marker and
      // shutdown, and unlike the normal-create branch below the force branch has no existence check to
      // stop it - existence is precisely what it ignores. So without this a restart re-downloaded and
      // re-installed the whole database on every replay: correct in outcome, but a node restarting
      // repeatedly re-pulls a multi-GB database on each attempt, lengthening every start and competing
      // for the bandwidth the cluster needs to recover. The in-place Ratis restart (restartRatisIfNeeded,
      // one per health-monitor tick) can repeat that without the process ever exiting.
      //
      // The same per-database evidence applyBootstrapFingerprintEntry uses: a persisted applied index at
      // or beyond this entry proves a previous session ran this install to completion and that Raft has
      // since replicated this database forward from there. It is deliberately PER-DATABASE - one state
      // machine multiplexes every database, so a co-located database that advanced the global index must
      // not suppress this one's reinstall (issue #4824) - and a legacy plain-number applied-index file
      // yields -1, which re-installs exactly as before.
      //
      // Both halves of that evidence are statements about a PREVIOUS session, and neither says the database
      // is here NOW (issue #7221). The index lives in <databaseDirectory>/.raft/applied-index, a sibling of
      // the per-database directories rather than a file inside them, so deleting one database's directory
      // leaves its entry in the map intact. The wipe-and-resync recovery an operator reaches for when a
      // follower's copy is bad - stop the node, delete the copy, start it again - then hit a guard that
      // skipped the reinstall and a log line claiming a reinstall the filesystem contradicted. So the skip
      // also requires the database to be registered here, the same question the normal-create arm below
      // asks; a node whose registry has no such database re-downloads, as it did before #7143.
      //
      // The registry, not the filesystem, is what is consulted - so the wording below says "registered"
      // rather than "present", which is the check that actually ran. A database dropped through Raft is
      // not the case this re-opens: applyTransaction routes a DROP_DATABASE_ENTRY through
      // writePersistedAppliedIndexDroppingDatabase, which evicts the per-database entry, so the read
      // below already returns -1 for a dropped database and the skip was never reachable for one.
      //
      // Both volatile collaborators are read once into locals and used from there for the rest of this
      // forceSnapshot branch (the normal-create arm below is untouched and keeps reading the field).
      // createStateMachine() (RaftHAServer:1416-1421) is the single production wiring point and sets the two on
      // consecutive lines, so "server is null" and "raftHAServer is null" are the same not-yet-wired state
      // rather than two independent ones - which is why they get the same treatment here instead of one being
      // captured and the other re-read.
      //
      // What keeps SnapshotInstaller.resolveDatabasePath below from seeing a null server is safe publication,
      // NOT a chain of volatile reads: this arm reads server BEFORE it reads raftHAServer, and observing the
      // later-written field non-null says nothing about a read that already happened, so that argument would
      // not hold. The one that does: createStateMachine() sets both fields on the machine before the reference
      // escapes it (RaftHAServer:1416-1421, assigned at :384 and :1478), so Ratis has no state machine to call
      // applyTransaction on until both writes are done. The precondition is now written down on
      // resolveDatabasePath itself, since six other call sites lean on it without saying so.
      final ArcadeDBServer localServer = this.server;

      // Restore flow: replace files from the leader's snapshot even if the DB exists. The leader's own files are
      // already authoritative, so the leader skips the reinstall; replicas close their local copy and pull the
      // fresh snapshot from the leader.
      //
      // Checked FIRST, ahead of the replay guard below, because it is unconditional: a leader takes no action
      // whatever the guard decides, and the guard's own WARNING announces a reinstall from the leader. Logged
      // before the skip, that line recorded an action that never happened - on the node whose log an operator
      // reads to find out what the cluster did with the entry (issue #7302).
      //
      // The volatile field is read ONCE into a local. resolveSnapshotSource guards a null HA server and refuses
      // cleanly, but evaluating raftHAServer.getLeaderId() as its ARGUMENT dereferenced the field before that
      // guard could run, so the refusal it exists to produce arrived as a NullPointerException instead; the
      // same held for getClusterToken() below. A null local yields a null leader id, which
      // PeerDialAddress.resolve refuses as "the leader is unknown".
      //
      // Null is reachable, not hypothetical: no production caller nulls the field (grep for setRaftHAServer -
      // RaftHAServer:1419 is the only one), but it starts null and a state machine that has not been rewired
      // yet still carries null. Forgetting exactly that rewire on the recovery path is the regression
      // Issue4839RecoveryRewiresStateMachineIT exists to catch.
      final RaftHAServer raftHA = this.raftHAServer;
      if (raftHA != null && raftHA.isLeader()) {
        HALog.log(this, HALog.TRACE, "Leader skips forceSnapshot reinstall for '%s'", databaseName);
        return;
      }

      final long persistedApplied = readPersistedAppliedIndex(databaseName);
      if (persistedApplied >= entryIndex) {
        if (localServer != null && localServer.existsDatabase(databaseName)) {
          LogManager.instance().log(this, Level.INFO,
              "Database '%s' already reinstalled by this entry in a previous session (persistedAppliedIndex=%d >= "
                  + "entryIndex=%d) and is registered on this node; skipping the snapshot re-download",
              databaseName, persistedApplied, entryIndex);
          return;
        }
        LogManager.instance().log(this, Level.WARNING,
            "Database '%s' was reinstalled by this entry in a previous session (persistedAppliedIndex=%d >= "
                + "entryIndex=%d) but is not registered on this node now; reinstalling it from the leader",
            databaseName, persistedApplied, entryIndex);
      }

      // Same refusals as every other path that pulls a snapshot, through the same helper (issue #6202): a
      // derived address that names this node would "restore" the local copy from itself and report success,
      // which is worse than the failure the caller already handles below.
      final PeerDialAddress source = resolveSnapshotSource(raftHA != null ? raftHA.getLeaderId() : null);
      if (source.refused())
        throw new RuntimeException("Cannot reinstall database '" + databaseName + "' from the leader: "
            + source.refusal());

      final String leaderHttpAddr = source.httpAddress();
      // The guard's own HTTPS endpoint rather than the raw resolver's: it is declared and derived independently
      // of the HTTP one, so the HTTP verdict does not cover it (issue #6221). Null falls back to plain HTTP.
      final String leaderHttpsAddr = source.httpsAddress();
      final String clusterToken = raftHA != null ? raftHA.getClusterToken() : null;
      try {
        // install() keeps the database open during the download and rolls back on failure, so a
        // failed restore never leaves it closed.
        SnapshotInstaller.install(databaseName, SnapshotInstaller.resolveDatabasePath(localServer, databaseName),
            leaderHttpAddr, leaderHttpsAddr, clusterToken, localServer);
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
   * Two rules keep the protocol honest on a cluster that starts taking writes while it is being born (issue
   * #7011: the formation-time election samples whatever local databases exist at collect time, and the
   * application typically creates and seeds its databases right after leader election):
   * <ul>
   *   <li><b>Superseded baseline.</b> A database that an application entry earlier in the log already
   *       created or mutated on this node has its whole history inside the Raft log: the baseline sampled for
   *       it is stale by construction and replication, not bootstrap, is what keeps the copies in step. The
   *       entry is ignored for that database. The decision keys on this database's persisted applied index
   *       being below the entry's index, which is the same on every peer because the log order is, so every
   *       peer ignores or honours the same entry.</li>
   *   <li><b>Bootstrap source.</b> The peer that committed the entry sampled the baseline from its own copy,
   *       which is the copy the snapshot ships to everyone else. Its copy advancing past the sampled
   *       {@code lastTxId} between the sample and the local apply (18 ms in the report) is the expected
   *       outcome, not a fresher stray copy, so the overwrite refusal does not apply to it: the refusal is
   *       only meaningful on a peer that did not source the baseline.</li>
   * </ul>
   * Neither rule is reachable on a genuine first formation - no application entry precedes the baseline
   * there, and the source's copy equals the baseline - so the #4800 and #6124 guarantees are unchanged.
   * <p>
   * Package-private (not private) so ArcadeStateMachineBootstrapMismatchTest can exercise the
   * install-failure recovery path directly instead of via reflection.
   *
   * @param originatedLocally whether this node submitted the entry, i.e. is the elected bootstrap source
   *                          (see {@link #startTransaction}); {@code false} on replay, where the
   *                          per-database replay-skip below settles the question instead. The marker is set
   *                          only on the node whose own client submitted the request, which is always the
   *                          source: {@code BootstrapElection} never commits on behalf of a remote source -
   *                          it transfers leadership to it and that node commits the entry once it leads
   */
  // @VisibleForTesting
  void applyBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index,
      final boolean originatedLocally) {
    final String dbName = decoded.databaseName();
    final String chosenFingerprint = decoded.bootstrapFingerprint();
    final long chosenLastTxId = decoded.bootstrapLastTxId();
    if (dbName == null || chosenFingerprint == null) {
      LogManager.instance().log(this, Level.WARNING,
          "BOOTSTRAP_FINGERPRINT_ENTRY missing required fields, skipping (db=%s, fp=%s)",
          dbName, chosenFingerprint);
      return;
    }

    final long persistedApplied = readPersistedAppliedIndex(dbName);
    if (persistedApplied >= 0 && persistedApplied < index) {
      // Superseded (issue #7011): an application entry for this database was applied before this baseline was
      // committed, so the database was created or mutated through Raft on this cluster and its copies are
      // kept in step by replication. Honouring the baseline would either refuse the copy as "fresher" (it is
      // not: it is the replicated state) or reinstall it from a snapshot the following entries then re-apply.
      // The baseline is not recorded: it describes a state this cluster never adopted.
      LogManager.instance().log(this, Level.INFO,
          "Bootstrap baseline for '%s' (lastTxId=%d) ignored: the database already has Raft history on this node "
              + "(applied index %d precedes the baseline at index %d), so replication keeps it in step",
          dbName, chosenLastTxId, persistedApplied, index);
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
    // The premise of the skip below is asked BEFORE the skip, not one line after it (issue #7298). The sibling
    // guard in applyInstallDatabaseEntry had the identical defect and #7221 fixed it there only: a persisted
    // applied index says a PREVIOUS session applied this entry, and says nothing about whether its effect is
    // still on this node. The index lives in <databaseDirectory>/.raft/applied-index, a sibling of the
    // per-database directories rather than a file inside them, so the wipe-and-resync recovery this repo's own
    // runbook prescribes - stop the follower, delete the bad copy, start it again - leaves the entry intact
    // while the database it describes is gone.
    final boolean registeredLocally = server.existsDatabase(dbName);

    if (persistedApplied >= index && registeredLocally) {
      HALog.log(this, HALog.BASIC,
          "Bootstrap baseline for '%s' already applied (persistedAppliedIndex=%d >= entryIndex=%d) and the database is "
              + "registered on this node; skipping verification",
          dbName, persistedApplied, index);
      return;
    }

    if (!registeredLocally) {
      if (persistedApplied >= index) {
        // The database WAS here - a previous session applied this very entry against it - and is not here now.
        // Falling through to the "late joiner" arm below would be wrong for a second reason beyond the log line:
        // that arm waits for a follow-on INSTALL_DATABASE_ENTRY, and a bootstrap-baselined database has none in
        // the log. It predates the cluster; the baseline entry is precisely the record of a database that was
        // never created through Raft. So nothing else in the log brings it back, and the node would keep running
        // permanently short of a database the cluster believes it has. Pull it from the leader, which is the
        // action #7221's own fix takes in the same situation on the install path.
        LogManager.instance().log(this, Level.WARNING,
            "Bootstrap baseline for '%s' was applied in a previous session (persistedAppliedIndex=%d >= entryIndex=%d) "
                + "but the database is not registered on this node now; reinstalling it from the leader",
            dbName, persistedApplied, index);
        installFromLeaderForBootstrapWithRetry(dbName, false);
        return;
      }

      // Late joiner with no local copy of this database, and no evidence it ever had one. The follow-on
      // INSTALL_DATABASE_ENTRY (or natural Raft replay) will create the database and install the leader's
      // snapshot; we just record the baseline.
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

    // The bootstrap source (issue #7011): this node committed the entry, so the baseline IS its own copy as it
    // stood at sampling time, and everyone else's snapshot comes from this copy. Writes accepted between the
    // sample and this apply legitimately move it past the baseline; refusing it here poisoned the leader of a
    // cluster that seeded its databases right after election, and the cluster never converged.
    if (originatedLocally) {
      if (localLastTxId >= chosenLastTxId)
        LogManager.instance().log(this, Level.INFO,
            "Database '%s' is the bootstrap source on this node (local lastTxId=%d, baseline lastTxId=%d): the local copy "
                + "is the baseline, nothing to verify",
            dbName, localLastTxId, chosenLastTxId);
      else
        // A committed transaction id never moves backwards, so the source's copy cannot sit behind the baseline it
        // sampled from that same copy. Should it ever happen (a database replaced under the running server), the
        // copy is still the one every other peer installs from: keep it and say so, rather than fall through to
        // the mismatch branch and have this node reinstall from itself.
        LogManager.instance().log(this, Level.WARNING,
            "Database '%s' is the bootstrap source on this node but its local lastTxId=%d is BELOW the baseline "
                + "lastTxId=%d it sampled; keeping the local copy, which is what the other peers install from",
            dbName, localLastTxId, chosenLastTxId);
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
    installFromLeaderForBootstrapWithRetry(dbName, true);
  }

  /**
   * Runs {@link #installFromLeaderForBootstrap} for one of the bootstrap arms and converts a failure into an
   * asynchronous retry rather than letting it reach {@link #applyTransaction}'s critical-error halt.
   * <p>
   * Applied on the Raft {@code StateMachineUpdater} thread: letting a failure propagate shuts the server down
   * and leaves the database closed, and a transient leader unavailability during restart must not do that -
   * {@code install()} downloads before touching the live files, so a failed download leaves whatever was here
   * exactly as it was.
   * <p>
   * Extracted for issue #7298, which gave this recovery a SECOND caller: the replay-skip arm, when the database
   * the skip is about turns out not to be on this node any more. The two callers differ in one fact that decides
   * both the safety net and the retry, so it is a parameter rather than a re-derived guess -
   * {@code server.existsDatabase} inside the catch cannot tell "the install deregistered it" from "it was never
   * here".
   *
   * @param hadLocalCopy whether a local copy of {@code dbName} existed when the install was started. When it did,
   *                     a database left deregistered is reopened and the ordinary full-resync path carries the
   *                     retry. When it did not, neither applies: reopening would throw on a database that does
   *                     not exist, and {@code triggerSnapshotDownload} only reinstalls the databases this server
   *                     already has registered, so the retry has to be this same targeted install again
   */
  private void installFromLeaderForBootstrapWithRetry(final String dbName, final boolean hadLocalCopy) {
    // A holder for the WHOLE operation, the scheduled retry included, so the issue #7519 readiness gate does not
    // lapse in the gap between a failed install and the retry that replaces the copy (CodeRabbit on PR #7964).
    // The one installFromLeaderForBootstrap takes for itself is released by its own finally; this one outlives it.
    //
    // NOT the durable #6124 unreconciled mark, which an earlier revision of this fix borrowed for the same job
    // (review of PR #7964). That set means "this node kept a copy FRESHER than the baseline and needs an operator
    // to choose a side": ClusterAlerts raises it as CRITICAL, tells the operator their data diverged and
    // recommends stopping the cluster and copying a directory to every peer. A first download that found no
    // leader yet - the ordinary case at formation, since the entry is applied while election on this peer may
    // still be settling - is none of those things and retries itself. Holding readiness must not also raise a
    // false CRITICAL with a drastic remedy.
    beginBootstrapInstall(dbName);
    boolean retryOwnsTheHolder = false;
    try {
      installFromLeaderForBootstrap(dbName);
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Failed to install snapshot during bootstrap for database '%s': %s. Scheduling an async retry once a "
              + "leader is reachable.", dbName, e.getMessage());

      if (hadLocalCopy) {
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
      }

      // We are inside the catch on the Raft StateMachineUpdater thread: a RejectedExecutionException from
      // a shut-down executor (server stopping) must not escape, or it would reach applyTransaction's
      // critical-error halt - the very outcome this handler exists to prevent.
      try {
        lifecycleExecutor.submit(() -> retryBootstrapInstall(dbName, hadLocalCopy));
        // The retry now owns the holder and releases it in its own finally, whatever it decides to do.
        retryOwnsTheHolder = true;
      } catch (final RejectedExecutionException ree) {
        // The remediation differs by branch, and naming the wrong one is the defect this whole change is about.
        // With a local copy the needsSnapshotDownload flag is set above, so the HealthMonitor backstop genuinely
        // picks the download up on the next start. WITHOUT one that flag was never set, and the backstop only
        // reinstalls databases the server has REGISTERED - which this one is not - so promising it here would
        // tell an operator a recovery path exists that does not cover them.
        if (hadLocalCopy)
          LogManager.instance().log(this, Level.WARNING,
              "Cannot schedule bootstrap snapshot retry for '%s': executor is shut down; "
                  + "the HealthMonitor backstop will retry once the server is available", null, dbName);
        else
          LogManager.instance().log(this, Level.SEVERE,
              "Cannot schedule the reinstall of database '%s', which was applied on this node in a previous session "
                  + "and is missing now: the executor is shut down. Nothing retries it automatically - the node is "
                  + "stopping, and on the next start this entry replays and reinstalls it. If it does not come back, "
                  + "run POST /api/v1/cluster/resync/%s on this node once a leader is reachable.",
              null, dbName, dbName);
      }
    } finally {
      // Released here on every path the retry did NOT take ownership of: the install succeeded, or it failed and
      // the executor was already shut down so nothing will retry. A holder nothing ever releases would wedge the
      // node out of the Service for good, which is worse than the gap it would be covering.
      if (!retryOwnsTheHolder)
        endBootstrapInstall(dbName);
    }
  }

  /**
   * The off-thread half of {@link #installFromLeaderForBootstrapWithRetry}. Never throws: it runs on the
   * {@code lifecycleExecutor}, where an escaping exception is only logged by the executor and helps nobody.
   */
  private void retryBootstrapInstall(final String dbName, final boolean hadLocalCopy) {
    try {
      retryBootstrapInstallHoldingTheGate(dbName, hadLocalCopy);
    } finally {
      // Releases the holder installFromLeaderForBootstrapWithRetry handed over when it scheduled this retry, so
      // the node has been continuously out of the Service from the first install to the end of this one, however
      // this one ended (issue #7519, CodeRabbit on PR #7964).
      endBootstrapInstall(dbName);
    }
  }

  /** The body of {@link #retryBootstrapInstall}, which owns the readiness holder around it. */
  private void retryBootstrapInstallHoldingTheGate(final String dbName, final boolean hadLocalCopy) {
    if (hadLocalCopy) {
      if (needsSnapshotDownload.compareAndSet(true, false))
        triggerSnapshotDownload();
      else
        // Another path (notifyLeaderChanged or the watchdog) already cleared the flag and is driving
        // the download; skip this retry. Logged so operators can trace why this submission did nothing.
        LogManager.instance().log(this, Level.INFO,
            "Bootstrap snapshot retry skipped for '%s': download already triggered by another path", dbName);
      return;
    }

    // No local copy, so the full resync above has nothing to iterate over: it reinstalls the databases this
    // server has REGISTERED, and this one is exactly the one it does not have. Retry the targeted install.
    if (server.existsDatabase(dbName)) {
      LogManager.instance().log(this, Level.INFO,
          "Bootstrap snapshot retry skipped for '%s': the database is registered again, another path installed it", dbName);
      return;
    }
    try {
      installFromLeaderForBootstrap(dbName);
    } catch (final RuntimeException e) {
      // Recorded DURABLY before giving up, in the same set and the same file the #6124 overwrite guard uses
      // (CodeRabbit on PR #7756). Logging alone was not enough and the reason is this method's own premise:
      // applyTransaction persists this database's applied index whatever happens here, so the entry reads as
      // applied while the database is absent, and the replay that would retry it is not guaranteed to survive
      // the next Ratis snapshot. The mark is what outlives that - it is persisted in .raft/bootstrap-baselines,
      // published by ClusterAlerts, and re-verified on the HealthMonitor's bootstrap-divergence tick, which is
      // where the bounded retry now lives (reconcileBootstrapDivergence). installFromLeaderForBootstrap clears
      // it on the success path, so nothing has to remember to.
      markBootstrapUnreconciled(dbName);
      // One retry HERE, then hand the problem to that tick: looping on the single-threaded lifecycleExecutor
      // would hold every other lifecycle task for as long as the leader stays unreachable.
      LogManager.instance().log(this, Level.SEVERE,
          "Database '%s' was applied on this node in a previous session but is missing now, and reinstalling it from "
              + "the leader failed again: %s. The node is running WITHOUT it, and says so in the cluster status "
              + "until it is back (alert 'bootstrap-database-missing'). The periodic bootstrap-divergence check "
              + "retries the install once this node is a FOLLOWER - a node cannot install a database from itself, so "
              + "if this node is the leader, transfer leadership first (POST /api/v1/cluster/leader). To force it, "
              + "run POST /api/v1/cluster/resync/%s on this node once a leader that holds it is reachable.",
          e, dbName, e.getMessage(), dbName);
    }
  }

  /** Test convenience: applies the entry as a peer that did not source the baseline. */
  // @VisibleForTesting
  void applyBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    applyBootstrapFingerprintEntry(decoded, index, false);
  }

  /**
   * Close the local database and pull a full snapshot from the current leader. Same low-level
   * snapshot install machinery as {@code applyInstallDatabaseEntry(forceSnapshot=true)}.
   * <p>
   * The leader short-circuit below ASSERTS its premise rather than assuming it (issue #7901). It was written for
   * one caller - the fingerprint-mismatch arm, where this node has a local copy that differs from the cluster
   * baseline - and for that caller "the leader is the bootstrap source, so it already holds the chosen baseline"
   * is true by construction. Issue #7298 gave the method a second caller with the opposite premise: the database
   * is not on this node, and this node being the Raft leader does not make it appear. Returning normally there
   * meant no retry was scheduled, no durable mark recorded, and one TRACE line stood as the entire record of a
   * node permanently short of a database the cluster believes it has - the exact outcome #7298 existed to
   * prevent. Throwing routes that case into the handling already written for it.
   */
  private void installFromLeaderForBootstrap(final String dbName) {
    // One read for both the leader check and the cluster token below (issue #7253).
    final RaftHAServer raft = this.raftHAServer;
    if (raft != null && raft.isLeader()) {
      // "Present" means registered OR on disk, the same pair getBootstrapUnreconciled classifies on: a leader
      // whose copy is merely closed is still the source every peer installs from, and making it throw here would
      // mark it unreconciled and retry a download over files that are perfectly good.
      if (!isDatabasePresentLocally(dbName))
        throw new IllegalStateException("Database '" + dbName + "' is missing on this node and this node is the Raft "
            + "leader, so there is nowhere to install it from. Transfer leadership (POST /api/v1/cluster/leader) to "
            + "a node that holds it, then force the install here (POST /api/v1/cluster/resync/" + dbName + ")");
      // The leader has the chosen baseline by definition (it's the source). No need to install.
      HALog.log(this, HALog.TRACE, "Leader skips bootstrap snapshot install for '%s'", dbName);
      return;
    }

    // The node is out of the Service from here until the install terminates, one way or the other (issue
    // #7519). Registered BEFORE the install rather than inside it because the window this opens is the whole
    // install - the download most of all, which is where the local copy is still open and serving the very
    // bytes the cluster has decided against - and not the file swap at the end, which
    // SnapshotInstaller.install already guards with the node-wide snapshotInstallInProgress 503 (HTTP only).
    beginBootstrapInstall(dbName);
    try {
      // Resolve the leader address on each retry: the bootstrap-mismatch entry is applied
      // during Raft log replay on startup, which can race ahead of leader election on this peer.
      // install() keeps the local copy open during the download and rolls back on failure, so a
      // failed bootstrap install never leaves the database closed.
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
    } finally {
      // In a finally, and deliberately also on the failure path: a node that could not reinstall is not
      // installing any more, and holding readiness on a condition nothing clears would wedge it out of the
      // Service for good. What it holds then is a copy the cluster did not adopt, which is what the
      // unreconciled mark records and what bootstrapWindowReason() reports in its own right.
      endBootstrapInstall(dbName);
    }
  }

  /**
   * Adds one holder to {@code dbName}'s bootstrap-install depth (issue #7519). Two callers take one, and they
   * nest: {@link #installFromLeaderForBootstrap} holds one for its own install, and
   * {@link #installFromLeaderForBootstrapWithRetry} holds one for the whole operation including the retry it may
   * schedule. Each pairs its own with {@link #endBootstrapInstall} in a {@code finally}, which is what the depth
   * is for - with a set, the inner release would drop the name while the outer operation was still running.
   */
  private void beginBootstrapInstall(final String dbName) {
    bootstrapInstallsInFlight.merge(dbName, 1, Integer::sum);
  }

  /**
   * Removes one holder, and the entry itself once the last holder leaves - so the map does not keep the name of
   * every database this node ever reinstalled for the node's lifetime, the same rule the per-database applied
   * index and the bootstrap baselines already follow.
   */
  private void endBootstrapInstall(final String dbName) {
    bootstrapInstallsInFlight.computeIfPresent(dbName, (name, depth) -> depth > 1 ? depth - 1 : null);
  }

  /**
   * Databases this node is installing from the leader's bootstrap snapshot right now, sorted for deterministic
   * output (issue #7519) - every one of them, whether it replaces a copy this node holds or reinstalls one it lost.
   * <p>
   * Package-private, and read by {@code ClusterAlerts.NodeStatus} so {@code GET /api/v1/cluster} publishes it as
   * the {@code bootstrap-install-in-progress} alert and the {@code bootstrapInstalls} member (issue #8044): until
   * then the readiness gate was its only consumer, and the status document the gate's 503 body points at said
   * nothing about it.
   */
  List<String> getBootstrapInstallsInFlight() {
    // The overwhelmingly common answer, and this is on the readiness-probe path: allocate nothing for it.
    if (bootstrapInstallsInFlight.isEmpty())
      return Collections.emptyList();
    final List<String> names = new ArrayList<>(bootstrapInstallsInFlight.keySet());
    Collections.sort(names);
    return names;
  }

  /**
   * Why this node must not be in the load balancer's pool because of the cluster's first-formation bootstrap, or
   * {@code null} when it may be (issue #7519).
   * <p>
   * The question it answers is narrower than "is anything bootstrap-related going on": <b>is this node serving a
   * copy of a database that the cluster's committed baseline did not adopt?</b> Two conditions make that true, and
   * they are the two halves of the same window:
   * <ul>
   *   <li><b>A held copy is being replaced.</b> This node's copy of the database did not match the committed
   *       baseline and is being replaced wholesale from the leader. The copy on disk is the one the cluster decided
   *       against for as long as that takes, and it is open and serving on every protocol for the length of the
   *       download - the node-wide {@code snapshotInstallInProgress} 503 covers only the swap at the end of it,
   *       and only HTTP.</li>
   *   <li><b>A copy was refused and never reconciled.</b> The "local is fresher, refuse to overwrite" branch
   *       kept this node's own copy on purpose, and its file-id space is assigned by an independent history from
   *       that point on (issue #6124). It is not a transient state - it survives restarts in
   *       {@code .raft/bootstrap-baselines} - and until an operator or an automatic remedy replaces the copy,
   *       everything this node serves for that database is data the cluster never adopted.</li>
   * </ul>
   * <b>A database this node does not hold is neither</b> (issue #8045). The #7298 replay-skip marks a database it
   * found gone and could not pull back in the same unreconciled set, and reinstalls it through the same install
   * path - but nothing is being served in the cluster's stead, so there is nothing to take out of the Service for.
   * Counting it held a node with nine good databases out of the Service for good over the tenth, under a remedy
   * ("discards the local copy") for a copy that does not exist, while the SEVERE line and the
   * {@code bootstrap-database-missing} alert told the operator the node was serving everything else. Both halves
   * are therefore classified by where the database is NOW, with the same presence test
   * {@link #getBootstrapUnreconciled(Set)} uses, and only a present copy counts.
   * <p>
   * Both conditions are recoverable, and both are cleared exactly where this node's copy is actually replaced by
   * the cluster's, so a node that recovers rejoins the Service on its own. That is the same shape as
   * {@link #getRaftLogFailure()}, the other terminal-until-remedied condition the readiness probe consults
   * unconditionally.
   * <p>
   * <b>It counts the databases and does not name them.</b> {@code GET /api/v1/ready} is the one route that
   * answers without authentication ({@code GetReadyHandler.isRequireAuthentication()} returns false), and this
   * string is its response body, so a name here is a database name handed to anything that can reach the port.
   * The authenticated {@code GET /api/v1/cluster} publishes both by name, filtered to the databases the caller may
   * see: the kept copies as the {@code bootstrap-diverged-databases} alert, and the installs as the
   * {@code bootstrap-install-in-progress} alert and the {@code bootstrapInstalls} member (issue #8044 - until then
   * the install half was published nowhere, and this sentence was not true of it). The sibling gates make the
   * same distinction without stating it: the log failure reports a log index and the security-convergence gate
   * reports document kinds.
   */
  public String bootstrapWindowReason() {
    // Not getBootstrapUnreconciled(): that allocates two lists and sorts a copy, and this runs on every readiness
    // probe. The empty checks come first so the overwhelmingly common answer stats no file.
    ensureBootstrapBaselinesLoaded();
    if (bootstrapInstallsInFlight.isEmpty() && bootstrapUnreconciledDatabases.isEmpty())
      return null;

    final int replacing = countPresentLocally(bootstrapInstallsInFlight.keySet());
    final int kept = countPresentLocally(bootstrapUnreconciledDatabases);
    if (replacing == 0 && kept == 0)
      return null;

    // BOTH are reported when both hold, rather than the first one found (review of PR #7964). They are different
    // databases in different states - an install replacing database A while database B sits unreconciled - and an
    // operator who reads only the install would go on believing the node comes back by itself when the install
    // finishes, which is the one case where it does not.
    final StringBuilder reason = new StringBuilder(256);
    if (replacing > 0)
      reason.append("The cluster's first-formation bootstrap is replacing ").append(replacing)
          .append(" database(s) on this node from the leader's snapshot: what is on disk is the copy the cluster's "
              + "committed baseline decided against, so this node must not serve it.");
    if (kept > 0) {
      if (replacing > 0)
        reason.append(' ');
      reason.append(kept)
          .append(" database(s) on this node hold a copy the cluster's committed bootstrap baseline did not adopt, "
              + "and nothing has reconciled them since: their file ids are out of step with every other peer. "
              + "POST /api/v1/cluster/resync/<database> on this node discards the local copy and adopts the "
              + "leader's.");
    }
    // Said once, whichever arms fired: the authenticated route is where the names are, and it is the answer to
    // "which databases" for both conditions alike.
    return reason.append(" GET /api/v1/cluster names them.").toString();
  }

  /**
   * How many of {@code names} this node holds a copy of, by the same test {@link #getBootstrapUnreconciled(Set)}
   * classifies on (issue #8045). A live view is fine to iterate: both backing collections are concurrent, and a
   * name added or removed mid-count only moves the answer to the next probe.
   */
  private int countPresentLocally(final Iterable<String> names) {
    int count = 0;
    for (final String name : names)
      if (isDatabasePresentLocally(name))
        ++count;
    return count;
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
   * <p>
   * The WHOLE marked set, both of the conditions {@link BootstrapUnreconciled} separates. A caller that renders
   * it to an operator wants that split instead - see there for why.
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
   * The marked set split by the one fact that decides what an operator should do about it: whether this node
   * still holds a copy of the database (issue #7902).
   * <p>
   * Both halves reach {@link #markBootstrapUnreconciled} and are indistinguishable in the set itself, but they
   * are opposite conditions:
   * <ul>
   *   <li>{@link #keptLocalCopy()} - the #6124 overwrite guard kept a copy FRESHER than the cluster's baseline.
   *       The data is here and intact; what is wrong is that its file ids came from a history no peer shares.</li>
   *   <li>{@link #missingLocally()} - the #7298 replay-skip found the database gone and could not pull it back.
   *       There is no copy here at all.</li>
   * </ul>
   * Reporting the second under the first's text told an operator that a database this node does not have was
   * "kept", and recommended copying this node's directory to every peer - which for a missing database would
   * overwrite every good copy in the cluster.
   *
   * @param seen which databases the caller may be told about by NAME; {@code null} for the unrestricted operator
   *             view. It reduces the names only: {@link #missingCount()} is the raw figure, because whether this
   *             node is serving a database is a node-level fact and not a per-tenant one, exactly as
   *             {@code localResync.inProgress} is
   */
  public BootstrapUnreconciled getBootstrapUnreconciled(final Set<String> seen) {
    ensureBootstrapBaselinesLoaded();
    // Same fast path as above, and for the same reason: this runs on every Studio status poll, and the answer
    // is almost always "nothing is marked". Nothing is allocated and no file is stat'ed for it.
    if (bootstrapUnreconciledDatabases.isEmpty())
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
      if (!isDatabasePresentLocally(dbName)) {
        ++missingTotal;
        if (seen == null || seen.contains(dbName))
          missing.add(dbName);
      } else if (seen == null || seen.contains(dbName))
        kept.add(dbName);
    }
    return new BootstrapUnreconciled(kept, missing, missingTotal);
  }

  /**
   * The two halves of the bootstrap-unreconciled set, as {@link #getBootstrapUnreconciled(Set)} classifies them.
   *
   * @param keptLocalCopy  marked databases this node still holds a copy of, reduced to the names the caller may
   *                       see
   * @param missingLocally marked databases that are not on this node at all, reduced the same way
   * @param missingCount   how many databases are missing BEFORE that reduction, so a caller scoped to no database
   *                       still learns that this node is running short of some
   */
  public record BootstrapUnreconciled(List<String> keptLocalCopy, List<String> missingLocally, int missingCount) {
    static final BootstrapUnreconciled NONE = new BootstrapUnreconciled(List.of(), List.of(), 0);

    public BootstrapUnreconciled {
      keptLocalCopy = List.copyOf(keptLocalCopy);
      missingLocally = List.copyOf(missingLocally);
    }
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
    final PeerDialAddress leaderDial = PeerDialAddress.resolve(raftHA, raftHA.getLeaderId(), "leader");
    if (leaderDial.refused())
      return; // no leader to compare against yet
    final String leaderHttpAddr = leaderDial.httpAddress();
    final String leaderHttpsAddr = leaderDial.httpsAddress();
    // Read once, off the volatile, so the task cannot see a different server than the one this tick checked.
    final ArcadeDBServer probeServer = this.server;

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
            probeServer, leaderHttpAddr, leaderHttpsAddr, clusterToken, pending, BOOTSTRAP_DIVERGENCE_PROBE_TIMEOUT_MS);
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
        // Checked BEFORE the missing-directory retry below, deliberately: a leader that reports no state for
        // this database is not a leader worth pulling it from - it does not have it open, and an install
        // sourced from it would fail or, worse, succeed with nothing. The retry is throttled and self-healing,
        // so deferring to the next tick costs one interval and nothing else; the alternative, retrying against
        // a leader that just said it cannot speak for this database, costs a download every tick forever.
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
        //
        // The one case that is NOT "merely closed" is the database whose directory is gone too, which is
        // how a marked database looks after the replay-skip's reinstall failed (issue #7298). There is no
        // local copy to protect and nothing else retries it - the applied index already reads as applied -
        // so this tick is the bounded retry, throttled to BOOTSTRAP_DIVERGENCE_CHECK_INTERVAL_MS and
        // ending by itself the moment the install succeeds and clears the mark.
        if (!databaseDirectoryExists(dbName))
          retryMissingBootstrapDatabase(dbName);
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
   * Whether this node holds a copy of {@code dbName} at all: registered in the server, OR merely closed with its
   * directory still on disk. The question two separate decisions turn on (review on PR #7953), so it is named
   * once rather than spelled out at each - inverted, which is how it is actually written at both.
   * <ul>
   *   <li>{@link #installFromLeaderForBootstrap} refuses the leader short-circuit when this is false: a leader
   *       that does not hold the database cannot be the source it is about to install from.</li>
   *   <li>{@link #getBootstrapUnreconciled(Set)} reports the database as missing rather than kept when this is
   *       false, which decides which remedy an operator is given.</li>
   * </ul>
   * Neither can use {@code existsDatabase} alone: that answers "is it in the registry", so a database an operator
   * deliberately left closed would read as gone, and both decisions would then act against files that are
   * perfectly good.
   * <p>
   * An unwired {@code server} answers {@code true}, and it has to. "Present" is the conservative answer for both
   * callers - it is the one that leaves local files alone and recommends nothing - which is the same doctrine
   * {@link #databaseDirectoryExists} follows for a path it cannot resolve. Answering {@code false} there would
   * make a state machine with no server throw out of the leader short-circuit and report every marked database as
   * missing; both call sites spelled this check out with a leading {@code server != null} for exactly that reason,
   * and folding them into this helper is what made the null case easy to invert by accident.
   */
  private boolean isDatabasePresentLocally(final String dbName) {
    return server == null || server.existsDatabase(dbName) || databaseDirectoryExists(dbName);
  }

  /**
   * Whether {@code dbName}'s directory is on disk with something in it, which is the question {@code existsDatabase}
   * does not answer: it reports registry membership, so a closed-but-present database and one that was deleted look
   * the same to it. Only the second is safe to reinstall over.
   * <p>
   * <b>An EMPTY directory is not a copy</b> (issue #8045). {@code SnapshotInstaller.install} creates the database
   * directory to stage its download in, and a failed download - the likely outcome of the #7298 reinstall, which
   * runs when no leader may be reachable yet - cleans its staging out of it and leaves the directory itself behind.
   * Counted as present, that one failure flipped a missing database into a KEPT one for good: the
   * {@code bootstrap-diverged-databases} alert told the operator to copy this node's (empty) directory to every
   * peer, the readiness gate held the node out of the Service under the kept-copy text, and the periodic #7298
   * retry stopped retrying, because it only runs for a directory that is gone. Anything at all in the directory
   * still counts - a closed database, a torn install the installer's own recovery reconciles - so this only
   * narrows "present" by the one shape that provably holds nothing.
   * <p>
   * A path that cannot be resolved or read answers {@code true} - "present" is the conservative answer here,
   * because it is the one that leaves the local files alone.
   */
  private boolean databaseDirectoryExists(final String dbName) {
    try {
      final String path = SnapshotInstaller.resolveDatabasePath(server, dbName);
      if (path == null)
        return true;
      final Path dir = Path.of(path);
      if (!Files.isDirectory(dir))
        return false;
      try (final DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
        return entries.iterator().hasNext();
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not resolve the directory of '%s' while verifying bootstrap divergence: %s; assuming it is present",
          dbName, e.getMessage());
      return true;
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
    LogManager.instance().log(this, Level.WARNING,
        "Database '%s' is marked unreconciled and its directory is absent on this node; retrying the install from "
            + "the leader", dbName);
    try {
      installFromLeaderForBootstrap(dbName);
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Reinstalling the missing database '%s' from the leader failed again: %s. Keeping the mark; the next "
              + "bootstrap-divergence check retries it", dbName, e.getMessage());
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
    // Whatever the database's pages were reserved at, the pages are going away: evict its ledger here so the
    // per-database map does not keep the names of dropped databases for the node's lifetime (same rule as the
    // persisted applied index above), and a database recreated under the same name starts from a clean ledger.
    pageVersions.clear(databaseName);

    // Idempotent on replay: if the database is already gone, nothing to do beyond evicting any
    // persisted baseline. applyBootstrapFingerprintEntry records a baseline by name even when the
    // database is not present locally (the late-joiner path), so a node can hold a persisted baseline
    // for a database it never had locally; evict it here so it does not linger in the file for the
    // node lifetime. This branch never calls drop(), so the eviction cannot precede a failed drop.
    if (!server.existsDatabase(databaseName)) {
      evictBootstrapBaseline(databaseName);
      clearDroppedDatabaseQuarantine(databaseName);
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
    clearDroppedDatabaseQuarantine(databaseName);

    LogManager.instance().log(this, Level.INFO, "Database '%s' dropped via Raft drop-database entry%s", databaseName,
        staged != null ? " (files staged as '" + staged.getFileName() + "' for background deletion)" : "");
  }

  /**
   * Retires the quarantine bookkeeping of a database a DROP entry has just removed: the per-database read floor
   * (issue #6760) and the diverged marker that goes with it.
   * <p>
   * Both exist to hold back readers of a database a snapshot install could NOT bring up to date, until a targeted
   * resync restores it. A dropped database has no such obligation left - there is nothing to resync and nothing to
   * read - and the floor is only ever cleared by {@link #clearDivergedDatabase}, which runs when a resync succeeds.
   * So without this, the floor of a database that was quarantined when it was dropped outlives it for the node's
   * lifetime, with two consequences (review of PR #7649):
   * <ul>
   *   <li>{@link RaftHAServer#getTrustedAppliedIndex(String)} clamps to that floor forever, so the local-apply wait
   *   {@code RaftReplicatedDatabase.dropInReplicas} takes could never be satisfied - the drop would report failure
   *   through its full quorum timeout even though the directory is gone;</li>
   *   <li>a database later recreated under the same name would inherit the dead floor and have its LINEARIZABLE
   *   reads pinned behind it.</li>
   * </ul>
   * The same reasoning the eviction of {@code pageVersions} and the bootstrap baseline in this method already
   * applies - do not keep per-database state for a name that no longer has a database.
   */
  private void clearDroppedDatabaseQuarantine(final String databaseName) {
    if (getDatabaseAppliedFloor(databaseName) < 0 && !isDatabaseDiverged(databaseName))
      return;

    HALog.log(this, HALog.BASIC,
        "Database '%s' was quarantined when it was dropped: retiring its read floor and diverged marker, "
            + "since a dropped database has no resync left to wait for (issue #6760)", databaseName);
    clearDivergedDatabase(databaseName);
  }

  // @VisibleForTesting
  void setDeferredDatabaseDeleter(final DeferredDatabaseDeleter deleter) {
    final DeferredDatabaseDeleter previous = this.deferredDatabaseDeleter;
    this.deferredDatabaseDeleter = deleter;
    if (previous != null && previous != deleter)
      previous.close();
  }

  /**
   * Applies a replicated user list.
   * <p>
   * A local PERSISTENCE failure here - {@code ServerSecurity.applyReplicatedUsers} cannot write
   * {@code server-users.jsonl} because the config volume is full, read-only or NFS-hiccuping - is deliberately
   * NOT a node-halt condition (issue #7137). Every other failure of that method still is; see the note at the
   * catch below. It reaches {@code handleUnexpectedApplyError} with the empty
   * database name the codec gives this entry, which skips the per-database quarantine of #4797 and lands in
   * the node-wide {@code catch (Throwable)} halt; and because the halt leaves the applied index untouched on
   * purpose, the next start replays the same entry and halts again. With an environmental cause that is an
   * indefinite crash loop of the whole node - every co-located database - triggered by nothing worse than a
   * password change on the leader.
   * <p>
   * Failing the entry is enough because nothing here can diverge the databases this node replicates: the
   * failure is confined to one file of server-local configuration. It is also not a security hole, because
   * {@code applyReplicatedUsers} publishes the new list in memory BEFORE reporting the write failure, so a
   * revoked account or a changed password takes effect on this node immediately - only the durability of that
   * change is outstanding.
   * <p>
   * <b>That durability cannot be counted on to come back on its own</b> (issue #7227). The failing entry does
   * not record itself as applied, but it does not halt the node either - that is the whole point of this arm -
   * so the NEXT entry moves {@link #lastAppliedIndex} past it, and {@link #takeSnapshot()} checkpoints from
   * that counter. Whether the entry is ever replayed therefore depends entirely on the SNAPSHOT MARKER, which
   * is the only thing {@link #reinitialize()} seeds the replay position from (see {@code ha-raft/CLAUDE.md}):
   * once any snapshot past this index is taken the entry is gone for good, and a graceful {@code stop()} takes
   * one unconditionally, as does {@code RaftLogCompactionScheduler} on its interval. A restart that beats all
   * of those - a kill shortly after the failure - does replay it.
   * <p>
   * The operator cannot know which of those two happened, so the instruction does not depend on it:
   * <b>reissue the user change on the leader</b> once the volume is fixed. Reapplying a list the node already
   * holds is a no-op, and waiting for a replay that may never come leaves the file stale indefinitely - which
   * is what the SEVERE below and the contract note on {@code ServerSecurity.applyReplicatedUsers} say too.
   * Pinned by {@code Issue7227SecurityEntryAppliedPositionMovesPastFailureTest} for the in-process half and by
   * {@code Issue7252SecurityEntryReplayAfterRestartTest} for BOTH branches of the restart, so a later change that
   * made the replay deterministic - or removed it - would fail one of them rather than leave this paragraph
   * quietly wrong in one direction (issue #7252).
   * <p>
   * The classification lives here, at the apply site, rather than in the generic handler: whether a failure
   * can diverge replicated state is a property of the apply, not of the entry's database scoping, so a future
   * node-scoped entry that CAN diverge still reaches the halt it needs.
   *
   * @return false when the entry carried a compare-and-set precondition that no longer holds, so the user list
   * was deliberately NOT installed (issue #7509). The decision is the same on every node, because the payload
   * and the state it is compared against are both replicated and applies are ordered
   */
  private boolean applySecurityUsersEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(this, Level.WARNING, "SECURITY_USERS_ENTRY has null payload, skipping");
      return true;
    }
    try {
      // An entry with no precondition takes the unconditional apply verbatim - that is a seed, and it is also
      // every entry a node that predates issue #7509 wrote. Only a conditional entry goes through the
      // compare-and-set overload, so nothing about the pre-#7509 path changes shape.
      final String precondition = decoded.securityPrecondition();
      if (precondition == null)
        server.getSecurity().applyReplicatedUsers(payload);
      else if (!server.getSecurity().applyReplicatedUsers(payload, precondition))
        return false;
      runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, index);
    } catch (final ReplicatedUsersPersistenceException e) {
      // In force in memory before the write failed, so it is what this node enforces: installed (issue #8317).
      runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.USERS, index);
      LogManager.instance().log(this, Level.SEVERE,
          "Could not fully apply a replicated user list on this node: %s. The node keeps running and, when the "
              + "list reached memory, is already enforcing it - but it is not durable: a restart before this is "
              + "fixed reads the previous '%s'. The usual cause is a local write failure: check that the "
              + "configuration directory holding that file is writable and has free space",
          e, e.getMessage(), SecurityUserFileRepository.FILE_NAME);
      // The message has to match what actually happened, because it is the half most likely to travel - into
      // another node's log, an HA status payload, an incident writeup - without the SEVERE above beside it.
      // "Stays up with its previous users" was true before the ordering fix and is now the opposite of the
      // guarantee this fix exists to provide (issue #7137).
      throw new ReplicationException(
          "Failed to persist the replicated user list locally; the node is already enforcing the new list in "
              + "memory, only its durability to disk failed", e);
    }
    // Note what is NOT caught: a payload this node cannot parse, or a user entry it cannot construct, throws
    // from applyReplicatedUsers BEFORE any mutation. That is not "the disk is full", it is "this node cannot
    // read a committed entry its peers applied", and it still reaches the node-wide halt - the case #4798
    // argues must never be skipped quietly. Catching RuntimeException here would have downgraded it silently.
    HALog.log(this, HALog.DETAILED, "Applied SECURITY_USERS_ENTRY (%d bytes)", payload.length());
    return true;
  }

  /**
   * Applies a replicated {@code server-groups.json} document (issue #7373).
   * <p>
   * Failure classification is the same split {@link #applySecurityUsersEntry} makes, and for the same reason: a
   * local WRITE failure happens after the document is already in force on this node, so nothing it replicates can
   * diverge and halting would turn a full or read-only config volume into a crash loop - report it and stay up.
   * A document this node cannot READ is the opposite case: it is a committed entry the peers applied and this one
   * cannot, which must not be skipped quietly (issue #4798), so it is deliberately NOT caught here and reaches the
   * node-wide halt.
   * <p>
   * The durability left outstanding by the caught case does not come back on its own - see the long note on
   * {@link #applySecurityUsersEntry}, which applies verbatim: reissue the group change on the leader once the
   * volume is fixed.
   */
  private boolean applySecurityGroupsEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(this, Level.WARNING, "SECURITY_GROUPS_ENTRY has null payload, skipping");
      return true;
    }
    try {
      final String precondition = decoded.securityPrecondition();
      if (precondition == null)
        server.getSecurity().applyReplicatedGroups(payload);
      else if (!server.getSecurity().applyReplicatedGroups(payload, precondition))
        return false;
      runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, index);
    } catch (final ReplicatedSecurityConfigPersistenceException e) {
      runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.GROUPS, index);
      LogManager.instance().log(this, Level.SEVERE,
          "Could not fully apply a replicated group document on this node: %s. The node keeps running and is "
              + "already authorizing against the new groups - but they are not durable: a restart before this is "
              + "fixed reads the previous '%s'. The usual cause is a local write failure: check that the "
              + "configuration directory holding that file is writable and has free space",
          e, e.getMessage(), SecurityGroupFileRepository.FILE_NAME);
      throw new ReplicationException(
          "Failed to persist the replicated group document locally; the node is already authorizing against the "
              + "new groups in memory, only their durability to disk failed", e);
    }
    HALog.log(this, HALog.DETAILED, "Applied SECURITY_GROUPS_ENTRY (%d bytes)", payload.length());
    return true;
  }

  /**
   * Applies a replicated {@code server-api-tokens.json} document (issue #7373). Same failure classification as
   * {@link #applySecurityGroupsEntry}.
   * <p>
   * Worth being explicit about what the non-halting arm means here, because this entry can carry a REVOCATION: the
   * new token set is in force on this node from the moment the apply returns, so the revoked token stops
   * authenticating here even when the write failed. What is outstanding is only that a restart would read the
   * stale file back - which is why the operator instruction is to reissue the revocation, not to wait.
   */
  private boolean applySecurityApiTokensEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String payload = decoded.usersJson();
    if (payload == null) {
      LogManager.instance().log(this, Level.WARNING, "SECURITY_API_TOKENS_ENTRY has null payload, skipping");
      return true;
    }
    try {
      final String precondition = decoded.securityPrecondition();
      if (precondition == null)
        server.getSecurity().applyReplicatedApiTokens(payload);
      else if (!server.getSecurity().applyReplicatedApiTokens(payload, precondition))
        return false;
      runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, index);
    } catch (final ReplicatedSecurityConfigPersistenceException e) {
      runtimeJoinDetector.onSecurityDocumentInstalled(RuntimeJoinDetector.API_TOKENS, index);
      LogManager.instance().log(this, Level.SEVERE,
          "Could not fully apply a replicated API-token document on this node: %s. The node keeps running and is "
              + "already enforcing the new token set - a revoked token does NOT authenticate here any more - but it "
              + "is not durable: a restart before this is fixed reads the previous '%s' and the revocation has to be "
              + "reissued. The usual cause is a local write failure: check that the configuration directory holding "
              + "that file is writable and has free space",
          e, e.getMessage(), ApiTokenConfiguration.FILE_NAME);
      throw new ReplicationException(
          "Failed to persist the replicated API-token document locally; the node is already enforcing the new token "
              + "set in memory, only its durability to disk failed", e);
    }
    HALog.log(this, HALog.DETAILED, "Applied SECURITY_API_TOKENS_ENTRY (%d bytes)", payload.length());
    return true;
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
   * The in-memory applied counter {@link #takeSnapshot()} checkpoints from. This - not
   * {@link #readPersistedAppliedIndex()} - is the value that decides what a restarted node replays, because the
   * replay position comes solely from the snapshot marker and the marker comes from here. Package-private for
   * tests; see {@code ha-raft/CLAUDE.md} on why the two must not be confused.
   */
  // @VisibleForTesting
  long readAppliedIndexCounter() {
    return lastAppliedIndex.get();
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
              restorePersistedQuarantine(json.getJSONObject("quarantine", new JSONObject()));
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
   * Seeds {@link #divergedDatabases} from the {@code quarantine} object of the persisted applied-index file
   * (issue #7735). Called from {@link #ensureAppliedIndexLoaded()} while it holds {@link #appliedIndexFileLock},
   * which is the same lock every quarantine mutation takes, so a mark or a clear can neither race the restore nor
   * be undone by it.
   * <p>
   * <b>Why it has to be persisted at all.</b> A quarantine deliberately does NOT advance
   * {@link #lastAppliedIndex} for the entry that tripped it - {@code applyTransaction} returns a failed future
   * before the {@code getAndSet} - while every later entry does. The in-memory mark was the only thing standing
   * between that and a node that restarts, forgets, reports {@code alerts: []} and serves a database permanently
   * missing a committed mutation.
   * <p>
   * {@code putIfAbsent}, mirroring the bootstrap-baseline loader. Every quarantine mutation loads before it
   * mutates, so in practice the restored cause is already in the map when a later mark arrives and the FIRST
   * cause wins across the restart exactly as it does within a run (issue #7741): the restored one describes
   * the failure that quarantined the database, while a mark after it comes from an entry that hit the same
   * wall on a database already waiting for a resync.
   * <p>
   * An unrecognised cause name - written by a newer build - degrades to {@link DivergenceCause#APPLY_ERROR}
   * rather than dropping the entry: the cause only changes what the alert SAYS, while dropping it would
   * reinstate exactly the silent divergence this exists to prevent.
   */
  private void restorePersistedQuarantine(final JSONObject quarantine) {
    for (final String name : quarantine.keySet()) {
      final String causeName = quarantine.getString(name, null);
      DivergenceCause cause = DivergenceCause.APPLY_ERROR;
      if (causeName != null)
        try {
          cause = DivergenceCause.valueOf(causeName);
        } catch (final IllegalArgumentException e) {
          LogManager.instance().log(this, Level.FINE,
              "Unknown divergence cause '%s' persisted for database '%s'; keeping the quarantine under %s",
              causeName, name, cause);
        }
      if (divergedDatabases.putIfAbsent(name, cause) == null)
        LogManager.instance().log(this, Level.SEVERE,
            "Database '%s' is still quarantined from a previous run (%s): this node refuses readiness and will "
                + "resync it from the leader rather than serve a copy that is missing a committed entry (issue #7735)",
            name, cause.getDescription());
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
  private boolean persistAppliedIndexFile() {
    if (closed)
      return false; // see close(): a task that outlived the shutdown must not recreate the file
    try {
      final Path file = getAppliedIndexFile();
      if (file == null)
        return false;
      final JSONObject json = new JSONObject();
      json.put("global", globalAppliedIndex);
      final JSONObject perDb = new JSONObject();
      for (final Map.Entry<String, Long> entry : appliedIndexByDb.entrySet())
        perDb.put(entry.getKey(), entry.getValue());
      json.put("db", perDb);
      // The quarantine travels in the SAME atomic write as the applied position it qualifies (issue #7735), so a
      // crash can never leave a file that says "applied up to N" without saying "and database X was skipped on the
      // way". Omitted when nothing is quarantined, which is the overwhelmingly common case, so the file shape an
      // older build reads back is byte-for-byte what it wrote.
      if (!divergedDatabases.isEmpty()) {
        final JSONObject quarantine = new JSONObject();
        for (final Map.Entry<String, DivergenceCause> entry : divergedDatabases.entrySet())
          quarantine.put(entry.getKey(), entry.getValue().name());
        json.put("quarantine", quarantine);
      }

      Files.createDirectories(file.getParent());
      final Path tmp = file.resolveSibling("applied-index.tmp");
      Files.writeString(tmp, json.toString());
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      return true;
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not write persisted applied index: %s", e.getMessage());
      return false;
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
    // Read once and use that local everywhere below, including inside downloadAllDatabasesFrom, which needs the
    // cluster token: the guard here is only a guard if nothing after it re-reads the field (issue #7253).
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null || server == null)
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
        final PeerDialAddress source = resolveSnapshotSource(raftHA.getLeaderId());
        if (source.refused()) {
          LogManager.instance().log(this, Level.WARNING, "Refusing a snapshot resync: %s", source.refusal());
          return;
        }
        downloadAllDatabasesFrom(source, raftHA.getClusterToken());
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
  private void downloadAllDatabasesFrom(final PeerDialAddress source, final String clusterToken) throws IOException {
    final String leaderHttpAddr = source.httpAddress();
    final String leaderHttpsAddr = source.httpsAddress();
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
   * <p>
   * <b>A quarantine counts as an unfilled gap too (issue #7735).</b> Since the quarantine is persisted, a node
   * restarts still quarantined, and the {@code triggerDatabaseResync} at the mark belongs to the JVM that raised
   * it - so without this tick a restored quarantine would hold the node out of the ready set with nothing
   * driving the recovery. When a quarantine is the ONLY thing outstanding the retry is a targeted resync per
   * quarantined database rather than the full download, which is what the quarantine path itself does.
   */
  public void retryUnfilledSnapshotGap() {
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null || server == null || raftHA.isLeader())
      return;
    final long floor = staleSnapshotAppliedFloor.get();
    // A per-database floor is an unfilled gap too: it is published exactly when an install gave up on a database,
    // which is the case that otherwise never re-arms anything (issue #6760). Same throttle, same single-flight.
    //
    // A quarantine with no floor beside it is the third case (issue #7735). It used to be re-driven only by the
    // triggerDatabaseResync() at the mark, which is a single attempt in this JVM - so a quarantine RESTORED from
    // disk after a restart had nothing driving it at all, and the node would have stayed out of the ready set
    // for good. None of the four DivergenceCause values heals by itself, so a quarantine that is still recorded
    // on a tick is always a resync waiting to be retried.
    ensureAppliedIndexLoaded();
    final Set<String> quarantined = divergedDatabases.isEmpty() ? Set.of() : new HashSet<>(divergedDatabases.keySet());
    if (floor < 0 && staleDatabaseAppliedFloors.isEmpty() && quarantined.isEmpty())
      return; // no unfilled gap and nothing quarantined
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

    if (floor < 0 && staleDatabaseAppliedFloors.isEmpty()) {
      // Only a quarantine is outstanding (issue #7735), so a TARGETED resync of each quarantined database is
      // both sufficient and far cheaper than pulling every database on the node. The full download below stays
      // the answer whenever a read floor is outstanding too, because a floor says the node-wide snapshot marker
      // itself is ahead of what was applied.
      LogManager.instance().log(this, Level.WARNING,
          "Database(s) %s are still quarantined from the committed Raft log with no download in flight: retrying "
              + "the targeted resync from the leader (issue #7735)", quarantined);
      for (final String dbName : quarantined)
        triggerDatabaseResync(dbName);
      return;
    }

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
    // One quarantine write for the whole batch (code review on PR #8146). Since the quarantine became durable
    // (#7735) a per-database markStateDiverged() would re-serialise the applied-index file and fsync+rename it
    // once per database, back to back, while holding the lock the apply thread also needs - N synchronous
    // rewrites where this loop used to do pure in-memory work. The set is what one install gave up on, so it
    // can be more than a couple on a node with many co-located databases.
    quarantineDatabases(databases, DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE);

    for (final String dbName : databases) {
      // The persisted position was deliberately NOT advanced for this database above, so it still carries whatever
      // this node genuinely applied. -1 (never recorded) clamps to 0, which is the honest answer for a database
      // nothing is known about.
      final long floor = Math.max(0L, readPersistedAppliedIndex(dbName));
      staleDatabaseAppliedFloors.put(dbName, floor);
      // The cause is named above, because the alert quotes it: nothing failed while APPLYING anything here, the
      // install is what did not finish the job, and an operator sent to look for an apply error would find none
      // (issue #7741).
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
    // A cheap early-out, not the guard: nothing below relies on this read, which is why it may be a separate one.
    if (raftHAServer == null || server == null)
      return;
    try {
      lifecycleExecutor.submit(() -> {
        // The one read for this operation, taken HERE and not at submit time. This is the only place the
        // read-once rule crosses an async boundary, and capturing the reference when the task was QUEUED would
        // buy the thing the rule exists to prevent: a resync running against an instance a teardown replaced
        // while it sat in the queue. Reading it when the work actually starts gives both halves - one instance
        // for the whole operation, and that instance current as of the operation (issue #7253).
        final RaftHAServer raftHA = this.raftHAServer;
        if (raftHA == null) {
          HALog.log(this, HALog.BASIC,
              "Skipping targeted resync of '%s': the HA server was torn down before the task ran", dbName);
          return;
        }
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
            final PeerDialAddress source = resolveSnapshotSource(raftHA.getLeaderId());
            if (source.refused()) {
              LogManager.instance().log(this, Level.WARNING,
                  "Refusing a targeted snapshot resync of quarantined database '%s': %s", dbName, source.refusal());
              return;
            }
            final String leaderHttpAddr = source.httpAddress();
            final String leaderHttpsAddr = source.httpsAddress();
            final String clusterToken = raftHA.getClusterToken();
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
    // Under the applied-index lock and after the load, so the on-disk quarantine is dropped with the in-memory
    // one (issue #7735). Without this the resync would heal the database and the node would still come back
    // quarantined after the next restart - the inverse of the bug, and just as wrong.
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      if (divergedDatabases.remove(dbName) != null)
        persistAppliedIndexFile();
    }
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
    markStateDiverged(dbName, DivergenceCause.APPLY_ERROR);
  }

  /**
   * {@link #markStateDiverged(String)} naming why, which is what the cluster status document reports
   * (issue #7741).
   * <p>
   * The FIRST cause a quarantine is recorded with is the one it keeps: {@code putIfAbsent} deliberately, which is
   * the behaviour the {@code Set.add()} this replaced already had. A quarantined database goes on failing - every
   * later committed entry for it hits the same wall - so the last cause would be noise from a database that is
   * already waiting for a resync, while the first is the one that describes what went wrong. The map is cleared
   * when the resync lands, so the next quarantine records afresh (code review on PR #7747).
   */
  // @VisibleForTesting
  void markStateDiverged(final String dbName, final DivergenceCause cause) {
    quarantineDatabase(dbName, cause);
  }

  /**
   * The single writer of a quarantine: records {@code dbName} as diverged under {@code cause} and durably
   * persists the quarantine, returning {@code true} only for the call that established it (issue #7735).
   * <p>
   * Every path that quarantines a database goes through here - the WAL version gap in
   * {@link #applyReplicatedTransaction}, the apply error and the undecodable entry in
   * {@link #handleUnexpectedApplyError}, and the incomplete snapshot install in
   * {@code markDatabasesNotAtSnapshotIndex} through {@link #markStateDiverged(String, DivergenceCause)} - so
   * there is one place the durability can be missing from rather than four.
   * <p>
   * Written under {@link #appliedIndexFileLock}, the same lock {@link #ensureAppliedIndexLoaded()} and the
   * applied-index writers take, so the mark and the applied position it qualifies land in one atomic rename.
   * The load-before-mutate keeps the other databases' applied positions intact when the file is rewritten.
   * <p>
   * The file write is best-effort, exactly like the applied-index write it shares, but a failure is reported at
   * WARNING here rather than at the FINE the shared writer logs: this is the one write the whole fix depends on,
   * and an operator whose disk refused it needs to know the quarantine will not survive the next restart. The
   * backstop is the snapshot refusal in {@link #takeSnapshot()}, which reads the in-memory map and so still
   * holds: with the log unpurged past the skipped entry, a restart replays into the same quarantine instead of
   * into a silent gap.
   *
   * @return {@code true} when this call established the quarantine, {@code false} when one was already recorded
   */
  // @VisibleForTesting
  boolean quarantineDatabase(final String dbName, final DivergenceCause cause) {
    if (dbName == null || dbName.isEmpty())
      return false;
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      if (divergedDatabases.putIfAbsent(dbName, cause) != null)
        return false;
      if (!persistAppliedIndexFile() && !closed)
        LogManager.instance().log(this, Level.WARNING,
            "Database '%s' is quarantined (%s) but the quarantine could NOT be written to %s: a restart before "
                + "this is fixed comes back without it. The log is not checkpointed while a database is "
                + "quarantined, so the skipped entry stays replayable, but check that the .raft directory is "
                + "writable and has free space (issue #7735)",
            dbName, cause, getAppliedIndexFile());
      return true;
    }
  }

  /**
   * {@link #quarantineDatabase} for a whole batch, in ONE file write (code review on PR #8146).
   * <p>
   * Same lock, same load-before-mutate, same first-cause-wins semantics; the only difference is that the file is
   * rewritten once for the set rather than once per member. Used by the snapshot-install path, which learns
   * about every database it gave up on at the same moment.
   * <p>
   * Deliberately does NOT drive a resync per database the way {@link #handleUnexpectedApplyError} does: its one
   * caller publishes a read floor beside each mark and leaves recovery to
   * {@link #retryUnfilledSnapshotGap()}, which is what #6760 chose.
   *
   * @return the databases this call newly quarantined, empty when every one of them already was
   */
  // @VisibleForTesting
  Set<String> quarantineDatabases(final Collection<String> dbNames, final DivergenceCause cause) {
    if (dbNames == null || dbNames.isEmpty())
      return Set.of();
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      final Set<String> added = new HashSet<>();
      for (final String dbName : dbNames)
        if (dbName != null && !dbName.isEmpty() && divergedDatabases.putIfAbsent(dbName, cause) == null)
          added.add(dbName);
      if (added.isEmpty())
        return Set.of();
      if (!persistAppliedIndexFile() && !closed)
        LogManager.instance().log(this, Level.WARNING,
            "Database(s) %s are quarantined (%s) but the quarantine could NOT be written to %s: a restart before "
                + "this is fixed comes back without it. The log is not checkpointed while a database is "
                + "quarantined, so the skipped entries stay replayable, but check that the .raft directory is "
                + "writable and has free space (issue #7735)",
            added, cause, getAppliedIndexFile());
      return added;
    }
  }

  /**
   * Clears the diverged-database set and the bounded-escalation counter after a snapshot resync has
   * restored consistent state across all databases. A resync always reinstalls every database from
   * the leader, so clearing the whole set (rather than a single database) matches what the resync
   * actually did.
   */
  // @VisibleForTesting
  void clearDivergedState() {
    // Same reasoning as clearDivergedDatabase: the persisted copy has to go with the in-memory one (issue #7735).
    synchronized (appliedIndexFileLock) {
      ensureAppliedIndexLoaded();
      if (!divergedDatabases.isEmpty()) {
        divergedDatabases.clear();
        persistAppliedIndexFile();
      }
    }
    lastDivergedResyncLogByDb.clear();
    divergedSwallowedErrors.set(0);
    // A resync reinstalls every database, so every per-database read floor is satisfied too (issue #6760). The
    // snapshot-install path re-publishes the floors of the databases it could NOT reinstall right after calling
    // this, so clearing wholesale here stays correct.
    staleDatabaseAppliedFloors.clear();
  }

  // @VisibleForTesting
  boolean isDatabaseDiverged(final String dbName) {
    // A quarantine restored from disk must be visible to the very first caller after a restart (issue #7735).
    // Latched after the first read, so this costs a plain field test on the apply path.
    ensureAppliedIndexLoaded();
    return divergedDatabases.containsKey(dbName);
  }

  // @VisibleForTesting
  int divergedSwallowedErrorCount() {
    return divergedSwallowedErrors.get();
  }

  // @VisibleForTesting
  boolean isHaltedAfterCriticalError() {
    return haltedAfterCriticalError.get() != null;
  }

  /**
   * What tripped the node-wide critical halt, or {@code null} while this state machine is still applying entries
   * (issue #7872).
   * <p>
   * Public, unlike {@link #isHaltedAfterCriticalError()}, which was and stays a test seam. The halt is a terminal,
   * operator-visible condition pinning {@code /api/v1/ready} at 503, and until this it was recorded nowhere a
   * machine could read it: {@code GET /api/v1/cluster} answered 200 with an empty {@code alerts} array on a node
   * whose state machine had stopped for good, which is the state the #7136 invariant exists to make impossible.
   */
  public CriticalHalt getCriticalHalt() {
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
    return getLocalResyncState().inProgress();
  }

  /**
   * Immutable report of everything {@link #isResyncInProgress()} is made of, so the cluster status document can
   * publish the local node's own health instead of only the cluster's (issue #7136).
   * <p>
   * The invariant this type exists to make checkable: <b>anything that makes readiness return 503 appears in
   * {@code GET /api/v1/cluster}</b>. {@link #isResyncInProgress()} - the readiness gate - is {@link #inProgress()}
   * on this very record, so a new resync condition cannot be added to one without appearing in the other. Before
   * it, a follower that had quarantined a database on a WAL version gap answered that endpoint with
   * {@code raftState: "RUNNING"} and {@code alerts: []} while Kubernetes was pulling it out of the Service.
   *
   * @param snapshotDownloadQueued     a snapshot download is flagged but has not started
   * @param snapshotDownloadInProgress a snapshot download is running
   * @param snapshotAppliedFloor       the node-wide stale-snapshot read floor, or {@code -1} when there is none
   *                                   (issue #6111)
   * @param databaseAppliedFloors      per-database read floors left by a snapshot install that could not bring
   *                                   them up to date, keyed by database name (issue #6760)
   * @param divergenceCauses           the databases quarantined and awaiting a resync (issues #4740, #4797), each
   *                                   with WHY it was quarantined (issue #7741)
   */
  public record LocalResyncState(boolean snapshotDownloadQueued, boolean snapshotDownloadInProgress,
                                 long snapshotAppliedFloor, Map<String, Long> databaseAppliedFloors,
                                 Map<String, DivergenceCause> divergenceCauses) {

    public LocalResyncState {
      databaseAppliedFloors = Map.copyOf(databaseAppliedFloors);
      divergenceCauses = Map.copyOf(divergenceCauses);
    }

    /**
     * The quarantined databases, sorted so a status poll payload is stable between ticks on an unchanged node.
     * <p>
     * DERIVED from {@link #divergenceCauses} rather than carried beside it (code review on PR #7747): the two
     * were the same set spelled twice, and a future caller updating one and not the other would have published a
     * name with no cause or a cause with no name. Computed per call, which costs an allocation only on a node
     * that is actually holding something back - the same trade {@link #getLocalResyncState} makes.
     */
    public List<String> divergedDatabases() {
      if (divergenceCauses.isEmpty())
        return List.of();
      final List<String> names = new ArrayList<>(divergenceCauses.keySet());
      Collections.sort(names);
      return names;
    }

    /**
     * Whether this node may hold divergent data pending a resync, and therefore must not advertise itself as
     * ready. The sole definition of that predicate: {@link ArcadeStateMachine#isResyncInProgress()} delegates
     * here rather than re-deriving it.
     */
    public boolean inProgress() {
      return snapshotDownloadQueued || snapshotDownloadInProgress || !divergenceCauses.isEmpty()
          || snapshotAppliedFloor >= 0 || !databaseAppliedFloors.isEmpty();
    }
  }

  /**
   * Snapshots the four components of {@link #isResyncInProgress()} for the cluster status document and the
   * readiness gate (issue #7136). Copies rather than views: a poll rendering the document must not see the set
   * change under it, and the caller must not be able to reach into the state machine's own collections.
   * <p>
   * Called only from the readiness probe, the health tick and the status endpoint, never from an apply path. A
   * healthy node - the overwhelming majority of those calls - allocates only the record itself; a node that is
   * actually resyncing pays for a sorted copy and the record's own defensive copy of it, which is a fair price
   * on a path that runs a handful of times a second at most.
   */
  public LocalResyncState getLocalResyncState() {
    // The readiness probe and the cluster status document both read this, and both must see a quarantine
    // restored from disk on the very first poll after a restart (issue #7735). Latched, so a healthy node pays
    // a plain field test.
    ensureAppliedIndexLoaded();
    // A healthy node - the overwhelming majority of calls, since the readiness probe polls this - copies
    // nothing: both immutable empties are shared constants and the record's own copyOf calls return them
    // unchanged. Only a node that actually has something in flight pays for the copies.
    final Map<String, DivergenceCause> causes = divergedDatabases.isEmpty()
        ? Map.of() : new HashMap<>(divergedDatabases);
    final Map<String, Long> floors = staleDatabaseAppliedFloors.isEmpty()
        ? Map.of() : new HashMap<>(staleDatabaseAppliedFloors);
    return new LocalResyncState(needsSnapshotDownload.get(), snapshotDownloadInProgress.get(),
        staleSnapshotAppliedFloor.get(), floors, causes);
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
    // Set BEFORE the executors are asked to stop: shutdownNow() interrupts a task, it does not unwind one that is
    // already past its last interruption point, and such a task can still reach clearDivergedState() /
    // writePersistedAppliedIndex() and recreate .raft/applied-index after the directory it lives in is gone
    // (issue #7735). A closed state machine is never reused - restartRatis() builds a new one - so there is no
    // write left that is worth making.
    //
    // Under appliedIndexFileLock, not as a bare volatile write (CodeRabbit on PR #8146): every caller of
    // persistAppliedIndexFile() holds that lock for the whole check-and-write, so taking it here BOTH waits for a
    // writer that already passed the closed check and guarantees that every later one observes the flag. A bare
    // write leaves the window this guard exists to close - a writer between the check and createDirectories().
    synchronized (appliedIndexFileLock) {
      closed = true;
    }
    lifecycleExecutor.shutdownNow();
    snapshotInstallExecutor.shutdownNow();
    membershipSecuritySeeder.close();
    securityCatchUp.close();
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
