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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PageVersionReservations;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.raft.ratis.RatisSnapshotDigestWarningFilter;
import com.arcadedb.server.monitor.CountingRejectionPolicy;
import com.arcadedb.server.monitor.PoolMetrics;
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;
import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.ClientId;
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
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.regex.Matcher;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
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
   * Test-only: runs on the thread taking a checkpoint, right before {@link #takeSnapshot()} forces the data files (issue
   * #11989), so a test can see what was still left to force on the apply thread. Always null in production.
   * <p>
   * Tests that set this MUST reset it to {@code null} in an {@code @AfterEach} method.
   */
  static volatile Consumer<ArcadeStateMachine> TEST_BEFORE_CHECKPOINT_SYNC = null;

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

    void record(final long entryIndex) {
      appliedIndexes.add(entryIndex);
    }
  }

  final SimpleStateMachineStorage storage          = new SimpleStateMachineStorage();
  final AtomicLong                lastAppliedIndex = new AtomicLong(-1);
  private final    AtomicLong                electionCount    = new AtomicLong(0);
  // The lowest index of a committed entry left for the replay on restart because its database was closed under the
  // apply thread while the node is shutting down (issue #9550); Long.MAX_VALUE while there is none. Later entries of
  // other databases still advance lastAppliedIndex past it, so takeSnapshot() clamps its checkpoint below it: the replay
  // position comes only from the snapshot marker (see ha-raft/CLAUDE.md). Never raised back: the node is going away, and
  // neither a restart nor RaftHAServer.restartRatis() reuses this instance (both go through createStateMachine()).
  private final    AtomicLong                replayFloor      = new AtomicLong(Long.MAX_VALUE);
  // How long a checkpoint waits in all, across every open database, for the pages pending in the asynchronous flush
  // before it refuses (issue #11258). A refused checkpoint only keeps the log one interval longer, so the bound protects
  // the apply thread, not the data
  static final long              SNAPSHOT_DATA_FLUSH_WAIT_MS = 5_000L;
  // How often a checkpoint refused for a database closed with its pages left to the recovery says so at WARNING (#12025)
  private static final long                  PAGES_LEFT_TO_RECOVERY_REMINDER_MS = 300_000L;
  // When it last did; Long.MIN_VALUE / 2 so the first refusal always does. Only the checkpoint reads and writes it
  private volatile long                      pagesLeftToRecoveryReminderMs      = Long.MIN_VALUE / 2;
  // Set once isNodeShuttingDown() has seen the JVM running its shutdown hooks (issue #9550); never cleared.
  private volatile boolean                   jvmShutdownSeen;
  // The highest index a leader-driven Raft install covered (issues #8577, #8651): a replayed entry at or below it is
  // stale whatever it names, so it is the one boundary every apply path can consult, including the ones that name no
  // database (which have no install gate) and Ratis's own metadata entries (which never reach applyTransaction).
  // In memory only, and never seeded by reinitialize(): after a restart Ratis replays from the snapshot marker onward,
  // and a marker reinitialize() distrusts (staleSnapshot) must not suppress the entries below it. An in-process
  // restartRatis() builds a fresh state machine, so the boundary does not outlive it either.
  final AtomicLong                installedRaftBoundary  = new AtomicLong(-1);
  // The boundary the first refusal below it was reported for at WARNING, so each boundary logs once (issue #8651).
  // Guarded by appliedPositionLock.
  private          long                      boundaryRefusalWarned  = -1L;
  // Serialises "is this index below the boundary?" with the move of the applied position it guards (issue #8651):
  // the install records the boundary and seeds the applied position under it, and every applied-position update
  // checks and moves under it, so the apply thread can never read the boundary before the install and then move the
  // position after it. Uncontended except during an install, when the apply thread waits out the install's durable
  // write. LOCK ORDER: appliedPositionLock, then appliedIndexFileLock - never call updateLastAppliedTermIndex while
  // holding appliedIndexFileLock.
  final Object                    appliedPositionLock    = new Object();

  private volatile long                      lastElectionTime = 0;
  private final    long                      startTime        = System.currentTimeMillis();
  // Tracks the previous leader so leader-change logs can show "X -> Y" instead of just "Y".
  // Useful when diagnosing churn: if X == Y across multiple changes, the leader is bouncing.
  private volatile RaftPeerId                previousLeaderId = null;
  // Tracks the highest term observed so notifyTermIndexUpdated can log only the first time we
  // see each term (otherwise it fires on every config/metadata entry, which is noisy).
  private final    AtomicLong                highestTermSeen  = new AtomicLong(-1);
  // Publications whose outcome a proposer is waiting for (issue #11240), settled by the entries this node applies
  private final    Set<PendingPublication>   pendingPublications = ConcurrentHashMap.newKeySet();
  // Raft term seen at the last notifyLeaderChanged. Lets us tell a genuine re-election (term
  // advanced) from a same-term re-notification, so we only warn about real leader churn (#4809 follow-up).
  private volatile long                      lastNotifiedLeaderTerm = -1;

  volatile ArcadeDBServer server;
  volatile RaftHAServer   raftHAServer;

  /**
   * Counts the full schema loads this node's apply thread ran in place of the incremental refresh (issue #10020). Shared
   * with the {@link RaftHAServer}, which builds a new state machine on every in-place restart and must not reset it; a
   * state machine wired to no server (a unit test) keeps its own.
   */
  volatile SchemaFullLoadRecorder schemaFullLoadRecorder = new SchemaFullLoadRecorder();

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

  static final String LIFECYCLE_THREAD_NAME        = "arcadedb-sm-lifecycle";
  static final String SNAPSHOT_INSTALL_THREAD_NAME = "arcadedb-raft-snapshot-install";

  /**
   * How long {@link #close()} waits, in total, for the tasks its executors and its helpers' executors are still running
   * (issues #8182 and #8364). Bounded because a download blocked in a socket read is past every interruption point and
   * would otherwise hold the stop for as long as the leader takes to answer; the task keeps running past the bound and
   * is only logged.
   */
  static final long CLOSE_AWAIT_MS = 5_000L;

  /** What a task left running past {@link #CLOSE_AWAIT_MS} on one of the state machine's own executors may do. */
  private static final String DATABASE_DIRECTORY_WRITES = "write under the database directory";

  // The current worker of each executor below, recorded by its thread factory, so close() can tell by identity - not
  // by the thread name every state machine in the JVM shares - that it is running on one of them (issue #8182). Each
  // executor has at most one worker at a time, and a worker running a task is never replaced, so the latest thread
  // the factory made is the one running any task that calls close().
  private volatile Thread lifecycleWorker;
  private volatile Thread snapshotInstallWorker;

  // What Executors.newSingleThreadExecutor builds, spelled out so the pool's row can read it (issue #8856).
  final ThreadPoolExecutor lifecycleExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<>(), r -> {
    final Thread t = new Thread(r, LIFECYCLE_THREAD_NAME);
    lifecycleWorker = t;
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

  private ThreadPoolExecutor createSnapshotInstallExecutor() {
    return new ThreadPoolExecutor(0, 1, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(16), r -> {
      final Thread t = new Thread(r, SNAPSHOT_INSTALL_THREAD_NAME);
      snapshotInstallWorker = t;
      t.setDaemon(true);
      return t;
    }, CountingRejectionPolicy.abort());
  }

  /**
   * The {@code pool=sm_lifecycle} executor row (issue #8856): the single worker the leader-facing downloads, the
   * bootstrap retries and the re-verifications queue on. Unbounded, so it never rejects running work: queue depth is
   * the signal, a download queued behind another.
   */
  PoolStats getLifecyclePoolStats() {
    return PoolMetrics.statsOf(lifecycleExecutor);
  }

  /** The {@code pool=snapshot_install} executor row (issue #8856); see {@link #getSnapshotInstallRejections}. */
  PoolStats getSnapshotInstallPoolStats() {
    return PoolMetrics.statsOf(snapshotInstallExecutor);
  }

  /**
   * Leader-initiated snapshot installs the install executor refused while running (issue #8856). Each one became a
   * failed future Ratis retries, so a rejection is not lost for good, but the node stays behind until the retry.
   */
  long getSnapshotInstallRejections() {
    // A metrics scrape must never throw: read the count only from the policy this class installs.
    return snapshotInstallExecutor.getRejectedExecutionHandler() instanceof CountingRejectionPolicy policy ?
        policy.getSaturations() :
        0L;
  }

  /** The {@code pool=database_deleter} executor row (issue #8856), read through whichever deleter is installed. */
  PoolStats getDatabaseDeleterPoolStats() {
    return dropEntries.deferredDatabaseDeleter.getPoolStats();
  }

  /**
   * Seeds the cluster security documents from the leader whenever a configuration change brings in a peer,
   * whichever admission path issued it (issue #7531). See {@link MembershipSecuritySeeder} for why the leader
   * and not the admitting node, and {@link #notifyConfigurationChanged} for the callback that drives it.
   * <p>
   * Not final so a test can substitute a recording seeder; production never replaces it.
   */
  private volatile MembershipSecuritySeeder membershipSecuritySeeder = new MembershipSecuritySeeder(
      this::isLocalNodeRaftLeader, this::securitySeedRetryBudgetMs, this::seedSecurityStateClusterWide,
      this::peersWithholdingSecurityEntries);

  /**
   * Records whether this node was added to the Raft configuration while running, which is what arms the
   * security-convergence readiness gate (issue #7819). Replaced by {@link RaftHAServer} with the instance it
   * owns, so the answer survives an in-place Ratis restart; the default keeps a state machine with nothing wired
   * to it - every peer of the {@code MiniRaftCluster} harness - recording on its own.
   */
  volatile RuntimeJoinDetector runtimeJoinDetector = new RuntimeJoinDetector();

  /**
   * Brings THIS node's security documents back in step when it rejoined without a membership change, or caught
   * up by a snapshot install that carried none of them (issue #7833). See {@link SecurityCatchUp}.
   */
  final SecurityCatchUp securityCatchUp = new SecurityCatchUp();

  /**
   * The security entries whose effects on this node have not reached the disk, which {@link #takeSnapshot()} keeps
   * replayable (issue #7752). See {@link SecurityDurabilityFloor}.
   */
  final SecurityDurabilityFloor securityDurabilityFloor = new SecurityDurabilityFloor(System::nanoTime,
      this::releaseSecurityHoldOf);

  /**
   * The catch-up this node's own triggers drive. Package-private so a test in this package can put it in the
   * state a restart during a failover leaves it in and then watch what the real leader change does with it
   * (issue #8034); nothing in production reaches it any other way than through the two callbacks below.
   */
  SecurityCatchUp getSecurityCatchUp() {
    return securityCatchUp;
  }

  /**
   * The seeder currently installed, read per scrape for the {@code pool=security_seed} executor row (issue #7856).
   * Through this accessor rather than captured once, since a test may substitute it and an in-place Ratis restart
   * replaces the state machine that holds it.
   */
  MembershipSecuritySeeder getMembershipSecuritySeeder() {
    return membershipSecuritySeeder;
  }

  /** The window of {@link #deferCompactionAfterUnpublishableChange} after the first failure (issue #9558). */
  static final long COMPACTION_BACK_OFF_BASE_MS     = 10 * 60_000L;
  /** The widest window {@link #deferCompactionAfterUnpublishableChange} grows to. */
  static final long COMPACTION_BACK_OFF_MAX_MS      = 6 * 60 * 60_000L;
  /** Doublings past the base after which the window stops growing; keeps the shift well clear of overflow. */
  private static final int COMPACTION_BACK_OFF_MAX_DOUBLINGS = 16;

  /** Until when, and after how many failures in a row, the compactions of a database are held off (issue #9558). */
  private record CompactionBackOff(long untilMs, int failures) {
  }

  // Keyed by database name and empty on every healthy node, so the check a compaction makes is one isEmpty() (issue #9558)
  private final ConcurrentHashMap<String, CompactionBackOff> compactionBackOffs = new ConcurrentHashMap<>();
  // Clock of the compaction back-off, in milliseconds. Monotonic, because it only ever measures an interval: a wall clock
  // stepped back would stretch a hold-off, one stepped forward would end it early. Package-private and mutable only so
  // tests can drive the window without sleeping.
  LongSupplier compactionBackOffClock = () -> System.nanoTime() / 1_000_000L;

  // Wall-clock of the END of the last leadership hand-off attempted by handOffLeadershipWhileReplacingDatabase(); 0 =
  // none yet. Throttles the retry of a hand-off that failed, so a cluster with no eligible peer is not put through an
  // election on every health tick. Measured from the end, not the start (issue #8556): an attempt can outlast the
  // interval, and stamping its start let the next one begin as soon as it returned.
  private volatile long          lastReplacingLeaderHandOffEndMs;
  // Hand-offs that failed in a row since the last one that moved leadership; widens the interval (issue #8556).
  private volatile int           replacingLeaderHandOffFailures;
  // Hand-offs that MOVED leadership while the condition behind them never cleared on this node (issue #8529); widens
  // the interval like the failures above. A gap every node shares - a database missing on all of them, a stale-snapshot
  // gap after a cluster-wide crash - has no peer that can close it: each hand-off succeeds, and the next leader hands
  // off in turn. Without it leadership would rotate at the base interval for as long as the gap lasts. Forgotten only
  // once this node no longer has the gap (resetReplacingLeaderHandOffBackOff), not when it stops being leader.
  // Per node, so it bounds, not stops, the rotation: once every node's count has grown, a cluster of N nodes that share
  // the gap still sees up to N hand-offs per REPLACING_LEADER_HAND_OFF_MAX_INTERVAL_MS. Stopping it would need each
  // node to know whether its target shares the gap, which nothing tells it today (issue #8665).
  private volatile int           leaderHandOffsMovedWhileGapPersisted;
  // Claimed by the caller running a hand-off, so a concurrent caller does not start a second one meanwhile.
  private final    AtomicBoolean replacingLeaderHandOffRunning = new AtomicBoolean();
  // Clock of the hand-off throttle. Package-private and mutable only so tests can drive the interval without sleeping.
  LongSupplier replacingLeaderHandOffClock = System::currentTimeMillis;

  /** Minimum interval between two leadership hand-offs of a leader replacing a database (issue #8491). */
  static final long REPLACING_LEADER_HAND_OFF_INTERVAL_MS = 10_000L;

  /** Longest interval the back-off of failed hand-offs of a leader replacing a database reaches (issue #8556). */
  static final long REPLACING_LEADER_HAND_OFF_MAX_INTERVAL_MS = 120_000L;

  /**
   * Where the count of failed hand-offs in a row stops growing (issue #8556). Any count past the one that reaches
   * {@link #REPLACING_LEADER_HAND_OFF_MAX_INTERVAL_MS} gives the same interval; the cap only keeps the counter bounded.
   */
  static final int REPLACING_LEADER_HAND_OFF_MAX_FAILURES_COUNTED = 30;

  /** Budget of one leadership hand-off of a leader replacing a database (issue #8491). */
  static final long REPLACING_LEADER_HAND_OFF_TIMEOUT_MS = 10_000L;

  /**
   * A database's install lock (issue #7958), which also remembers the highest Raft index the apply thread applied to
   * that database while holding it (issue #8454).
   * <p>
   * The index is recorded BEFORE the lock is released, which is what an install needs: it takes the lock and reads
   * the index, and has to see every entry that went to the copy it is about to replace. {@code lastAppliedIndex} is
   * not enough on its own, because {@code applyTransaction} advances it only after it has released the lock, so an
   * install that takes the lock in that gap would read the index of the entry BEFORE the one just applied.
   */
  static final class InstallApplyGate extends ReentrantLock {
    private volatile long appliedUnderGate = -1L;
    private volatile long installedIndex   = -1L;
    private volatile long servedCopyIndex  = -1L;

    /** Called by the apply thread, holding this lock, for every entry it applied (or tried to) to the database. */
    void recordApplied(final long index) {
      if (index > appliedUnderGate)
        appliedUnderGate = index;
    }

    long appliedUnderGate() {
      return appliedUnderGate;
    }

    /**
     * Called by an install, still holding this lock, once the copy it just put in place is known to cover every
     * entry up to {@code index} (issue #8577). Recorded BEFORE the lock is released, so an entry that was already
     * waiting behind the gate - or arrives right after - sees it the moment it re-acquires the gate: nothing at or
     * below it can slip through and be re-applied to a copy that already carries it.
     */
    void recordInstalled(final long index) {
      if (index > installedIndex)
        installedIndex = index;
    }

    /**
     * The highest index the installed copy of this database is known to cover, or {@code -1} when no install has
     * recorded one. An entry at or below it is a no-op (issue #8577): either this node's own pre-install local log
     * being replayed after a restart, or a duplicate notification, and applying it would write data the installed
     * copy already carries.
     */
    long installedIndex() {
      return installedIndex;
    }

    /**
     * Called by an install, still holding this lock, with the applied index the leader reported for the copy it just
     * put in place ({@code SnapshotManager.APPLIED_INDEX_HEADER}), or {@code -1} when the leader did not report one
     * (issue #8579). REPLACES the previous value rather than keeping the highest: the copy on disk is the last one
     * installed, and a later install can legitimately be served at a lower index than an earlier one (a new leader
     * that has not applied as far, accepted because this node's own floor was lower), in which case the entries
     * between the two are no longer in the copy and must be applied again. An install that downloaded nothing, or
     * whose leader did not report an index, records {@code -1}: that forgets a boundary the copy may still honour,
     * which only costs re-applying entries, never skipping one.
     * <p>
     * Never cleared: Raft indexes only grow, so once the apply thread has passed it nothing at or below it arrives
     * again except a replay of the log, and that replay goes to the same copy, which still carries those entries.
     */
    void recordServedCopy(final long index) {
      servedCopyIndex = index;
    }

    /**
     * The applied index the leader reported for the copy of this database installed last, or {@code -1} when there
     * is none (issue #8579). Every entry for this database up to it is already in that copy: the leader applied it and
     * published its pages before it read that index and captured the copy (issue #8454).
     */
    long servedCopyIndex() {
      return servedCopyIndex;
    }
  }

  /**
   * Test-only hook called with the database name when the apply thread finds that database's install lock held and
   * is about to wait on it (issue #7958). {@code null} in production (one reference read per contended apply).
   */
  static volatile Consumer<String> applyWaitsForInstallForTesting = null;

  /**
   * Databases whose copy the committed bootstrap baseline ordered replaced, whose replacement has failed at least
   * once, and which nothing has replaced since (issue #8367). Each name owns exactly one holder in
   * {@link BootstrapInstaller#bootstrapInstallsInFlight}, handed over by {@link BootstrapInstaller#installFromLeaderForBootstrapWithRetry} when the first
   * install failed, so the node stays out of the Service and the #8363 request gate keeps refusing clients for as
   * long as the rejected copy is what is on disk.
   * <p>
   * Before this set, the holder was released in the one scheduled retry's {@code finally}, whatever the retry did:
   * a second failed download put the node back in the pool serving the copy the cluster had decided against, and -
   * since that retry consumed {@code needsSnapshotDownload} and {@code triggerSnapshotDownload()} re-arms it only
   * under a stale-snapshot read floor - nothing ever tried the replacement again.
   * <p>
   * Released only by {@link BootstrapInstaller#settleBootstrapReplacement}, which every path that actually replaces this node's copy
   * with the leader's calls (the same paths that clear the #6124 unreconciled mark), and by the DROP of the
   * database. Re-driven by {@link #retryPendingBootstrapReplacements()} on the {@link HealthMonitor} tick.
   * <p>
   * Deliberately NOT the {@link BootstrapBaselines#bootstrapUnreconciledDatabases} mark: that set means "this node kept a FRESHER copy
   * and an operator must choose a side" and is published as a CRITICAL with a drastic remedy, which misdiagnoses the
   * ordinary "no leader yet" failure (PR #7964). Never populated by the #6124 "local is fresher, refuse to overwrite"
   * branch either, which returns before any install is attempted.
   * <p>
   * <b>Durable</b> (issue #8411), as its own {@code pendingReplacement} flag of the database's entry in
   * {@code .raft/bootstrap-baselines}, next to - and independent of - the {@code unreconciled} one. Kept in memory
   * only, a restart before the replacement landed lost it for good: {@code applyTransaction} has already persisted
   * the baseline entry's index as applied, so its replay is skipped for a registered database, and once a Ratis
   * snapshot is taken past it the entry is not replayed at all. The node came back serving the copy the committed
   * baseline rejected, in the Service and with nothing retrying the replacement. The loader restores the name
   * together with the one readiness holder it owns, so the node comes back out of the Service and the health tick
   * goes on retrying. Every add and every removal goes through {@link BootstrapBaselines#bootstrapBaselinesFileLock} and rewrites the
   * file.
   */
  final Set<String> bootstrapReplacementsPending = ConcurrentHashMap.newKeySet();

  /**
   * Re-verification of the closed copies this node keeps marked unverified (issue #8606): the first retry follows the
   * previous attempt by this much, and each round that leaves a copy still marked doubles it, up to
   * {@link #UNVERIFIED_COPY_REVERIFY_MAX_INTERVAL_MS}. A new leader, and a request this node refused for a marked copy,
   * restart the ladder, so the next health tick after this interval asks again.
   */
  static final long UNVERIFIED_COPY_REVERIFY_INTERVAL_MS     = 30_000L;
  static final long UNVERIFIED_COPY_REVERIFY_MAX_INTERVAL_MS = 600_000L;

  // Wall-clock of the last re-verification round claimed by reverifyUnverifiedClosedCopies(); 0 = ask at the next tick.
  final AtomicLong    lastUnverifiedCopyReverifyMs     = new AtomicLong();
  // Rounds in a row that left a marked copy in place. Written by the lifecycle executor and by a refused request, without
  // a lock on purpose: it is advisory, and a reset racing a round's own update costs at most one round early or late.
  final AtomicInteger unverifiedCopyReverifyFailures   = new AtomicInteger();

  /** How one reinstall of an unverified closed copy ended, for the backoff of {@link #reverifyUnverifiedClosedCopies}. */
  enum ReinstallOutcome {
    /** Installed, or settled by another path since the listing. */
    SETTLED,
    /** The leader closed it again, or the install failed: the copy keeps its mark. */
    LEFT_MARKED,
    /** Another download held the single flight: nothing was learned about the leader. */
    BUSY
  }

  /** Per-database bootstrap baseline as it appears in the committed Raft log entry. */
  public record BootstrapBaseline(String fingerprint, long lastTxId) {
  }

  /**
   * Database reconciliation collaborator (issue #4727, extracted in #4748). Owns the per-database
   * auto-acquisition status, the failure/give-up bookkeeping, and the reconcile orchestration the state machine
   * delegates to from {@link #notifyInstallSnapshotFromLeader}. Exposed via {@link #getReconciler()} so
   * {@code GetClusterHandler} and {@code ClusterAlerts} can read the per-database statuses.
   */
  final DatabaseReconciler reconciler = new DatabaseReconciler();

  final AtomicBoolean needsSnapshotDownload      = new AtomicBoolean(false);
  final AtomicBoolean snapshotDownloadInProgress = new AtomicBoolean(false);
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
   * gap. {@code applyInstallDatabaseEntry}'s {@code forceSnapshot} branch and {@link BootstrapInstaller#installFromLeaderForBootstrap}
   * run on the Ratis apply thread as part of applying a committed entry: they are already serialised against each
   * other by that single thread, they cannot fold (skipping leaves the database absent or diverged, which is the
   * state the entry exists to repair), and they must not park the apply loop - and with it replication for every
   * database on this node - for the length of a download it did not start. {@link #resyncDatabaseFromLeader} runs
   * on the operator's HTTP worker thread and reports its outcome to them synchronously, so folding would answer
   * "done" for work it did not do. What makes an actual overlap visible rather than silent is
   * {@code SnapshotInstaller}'s own {@code INSTALLS_IN_FLIGHT} set, which logs a WARNING naming the database when
   * two installs share one directory - the detector for the assumption this lock cannot enforce everywhere.
   */
  final ReentrantLock snapshotDownloadLock = new ReentrantLock();

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
  final AtomicLong    staleSnapshotAppliedFloor  = new AtomicLong(-1);
  // Wall-clock of the last retry submitted by retryUnfilledSnapshotGap(); 0 = none since the floor was
  // last cleared. Throttles the HealthMonitor-driven backstop, which ticks far more often than a full
  // multi-database resync costs.
  final AtomicLong    lastStaleSnapshotRetryMs   = new AtomicLong();
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
  // DURABLE since issue #8137, like the quarantine it always travels with: mirrored into the "floors" object of
  // .raft/applied-index under appliedIndexFileLock, in the same write as the quarantine, and read back by
  // ensureAppliedIndexLoaded(). In memory only, a restart kept the database quarantined but dropped its clamp, so a
  // LINEARIZABLE read reaching the node directly was served from the copy the install had given up on.
  final ConcurrentHashMap<String, Long> staleDatabaseAppliedFloors = new ConcurrentHashMap<>();
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
  final AtomicReference<CriticalHalt> haltedAfterCriticalError = new AtomicReference<>();

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
  final Map<String, DivergenceCause> divergedDatabases = new ConcurrentHashMap<>();

  // Raised by close(), read by persistAppliedIndexFile(): a lifecycle task that is already past its last
  // interruption point when shutdownNow() lands must not recreate .raft/applied-index under a directory the
  // shutdown is removing (issue #7735). A closed state machine is never reused - RaftHAServer.restartRatis()
  // builds a new one - so nothing is lost by refusing the write. Written under appliedIndexFileLock, which every
  // writer holds across its whole check-and-write, so raising it also waits out a write already in flight.
  volatile boolean closed;

  static final int           MAX_DIVERGED_SWALLOWED_ERRORS = 100; // package-private for tests

  // Budget for taking the compaction write lock of the shards a sealed-store entry installs (issue #7337). The
  // only holders of the read half on a follower are a backup or a snapshot ship, both of which release it the
  // moment the sealed images have been read, so this is sized as a hang detector rather than as a queue: a wait
  // longer than this means something is not releasing, and failing the apply loudly is better than installing
  // a sealed store that a copy in flight can pair with the wrong page image.
  static final long              SEALED_INSTALL_LOCK_TIMEOUT_MS    = 120_000L;

  // The transactions this node originated that are in flight between replication and the publication of their
  // pages, and the page versions the Raft log has assigned but this node has not applied yet (issue #6965).
  // Together they give the leader the same page-write order as every follower - its own entries are published at
  // their log position by the apply thread - and let it refuse, before the entry enters the log, a transaction that
  // was validated against a page version the log has already moved past. See LocalCommit and PageVersionLedger.
  private final LocalCommitRegistry localCommits = new LocalCommitRegistry();
  final PageVersionLedger   pageVersions = new PageVersionLedger();

  /**
   * What {@link #preAppendTransaction} learned about a client entry, handed to {@link #applyTransaction} through the
   * Ratis transaction context so the entry is decoded once: whether this node's own client submitted it, and the
   * decoded payload.
   */
  private record AppendedEntry(boolean originatedLocally, RaftLogEntryCodec.DecodedEntry decoded, PageVersionLedger.EntryId entryId,
                               PageVersionLedger.Pages pages) {
  }

  // The collaborators that hold the parts of the state machine with a responsibility of their own. Each one keeps a
  // reference back to this instance for the state it shares with the rest of the apply path.
  final SchemaEntryApplier schemaEntries = new SchemaEntryApplier(this);
  final AppliedIndexFile appliedIndexes = new AppliedIndexFile(this);
  final BootstrapBaselines baselines = new BootstrapBaselines(this);
  final ConfigurationEntryApplier configurationEntries = new ConfigurationEntryApplier(this);
  final DropDatabaseApplier dropEntries = new DropDatabaseApplier(this);
  final SecurityEntryApplier securityEntries = new SecurityEntryApplier(this);
  final DatabaseQuarantine quarantine = new DatabaseQuarantine(this);
  final SnapshotResync resync = new SnapshotResync(this);
  final DatabaseReplacement replacement = new DatabaseReplacement(this);
  final BootstrapInstaller bootstrapInstalls = new BootstrapInstaller(this);
  final BootstrapPasses bootstrapPasses = new BootstrapPasses(this);
  final BootstrapDivergenceCheck bootstrapDivergence = new BootstrapDivergenceCheck(this);
  final UnverifiedCopyReverifier unverifiedCopies = new UnverifiedCopyReverifier(this);
  final EntryApplyRetry applyRetry = new EntryApplyRetry(this);
  final LeaderSnapshotInstall leaderSnapshotInstall = new LeaderSnapshotInstall(this);
  final SchemaChangeHistory schemaHistory = new SchemaChangeHistory(this);
  final ExclusiveDatabaseWindows exclusiveWindows = new ExclusiveDatabaseWindows(this);
  final CriticalHaltHandler criticalHalt = new CriticalHaltHandler(this);
  final MaterializedViewRecovery viewRecovery = new MaterializedViewRecovery(this);

  public void setServer(final ArcadeDBServer server) {
    this.server = server;
    reconciler.setServer(server);
    // The Ratis-initiated installs go through the same #7958 install lock as every other install.
    reconciler.setInstallGate(replacement::runUnderInstallGate);
  }

  /** The database reconciliation collaborator, used by {@code GetClusterHandler} and {@code ClusterAlerts}. */
  public DatabaseReconciler getReconciler() {
    return reconciler;
  }

  public void setRaftHAServer(final RaftHAServer raftHAServer) {
    this.raftHAServer = raftHAServer;
  }

  // @VisibleForTesting - and RaftHAServer, which hands every state machine it builds the one recorder it owns
  void setSchemaFullLoadRecorder(final SchemaFullLoadRecorder schemaFullLoadRecorder) {
    this.schemaFullLoadRecorder = schemaFullLoadRecorder;
  }

  SchemaFullLoadRecorder getSchemaFullLoadRecorder() {
    return schemaFullLoadRecorder;
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
    // Before any catch-up trigger can run: a re-seed a snapshot install forced, and that had not committed when this node
    // stopped, is made again (issue #7752). Beside the snapshot markers, off the configuration volume that failed.
    securityCatchUp.keepForcedSeedIn(storage.getSnapshotFile(0L, 0L).getParentFile());
    reinitialize();
    // Eagerly, not on first use: a bootstrap replacement left pending by the previous session must hold readiness and
    // the request gate from the first request on, not from whichever read happens to load the file (issue #8411).
    baselines.ensureBootstrapBaselinesLoaded();
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
        dropEntries.deferredDatabaseDeleter.sweepOrphanedStagingDirectories(databasesDirectory);
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
   * is reset (e.g., during Ratis recovery via {@link RatisInPlaceRestart#restartRatisIfNeeded}).
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
    final long persistedApplied = appliedIndexes.readPersistedAppliedIndex();

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
    // What the schema did up to the seeded position is not in this instance's memory (issue #10067)
    schemaHistory.schemaHistoryIncompleteUpTo(lastAppliedIndex.get());

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
   * This node's own latest Raft snapshot boundary (issue #8360), i.e. what {@link #takeSnapshot()} or a prior
   * leader-driven install last registered via {@link #registerSnapshotMarker}. Served to a peer over
   * {@code POST /api/v1/cluster/bootstrap-state} so a follower installing a snapshot FROM this node (when this
   * node is the leader) can register the snapshot under the same term Ratis on this node will send as
   * {@code previous} for that index, instead of approximating it from a neighboring log entry's term. {@code null}
   * when no snapshot has been taken yet.
   */
  public TermIndex getLatestSnapshotTermIndex() {
    final var latest = storage.getLatestSnapshot();
    return latest != null ? latest.getTermIndex() : null;
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
    synchronized (appliedPositionLock) {
      return updateLastAppliedTermIndexLocked(newTI);
    }
  }

  /**
   * Moves the applied position to the term and index a snapshot install left this node at, through the same
   * {@link #updateLastAppliedTermIndex(TermIndex)} override every other move goes through. The two-argument form is
   * protected in Ratis, so {@link LeaderSnapshotInstall} reaches it through here.
   */
  void updateLastAppliedTermIndexAfterInstall(final long term, final long index) {
    updateLastAppliedTermIndex(term, index);
  }

  private boolean updateLastAppliedTermIndexLocked(final TermIndex newTI) {
    // A stale replayed entry that Ratis applies through notifyTermIndexUpdated (its own metadata and configuration
    // entries) never reaches applyTransaction's install skip. Left to super it trips the monotonic check on the
    // StateMachineUpdater thread, which dies and closes the division (issue #8651). STRICTLY below: the install's own
    // seed is AT the boundary, and an entry at it is an equal-position no-op that super handles.
    final long boundary = installedRaftBoundary.get();
    if (newTI != null && newTI.getIndex() < boundary) {
      // The first refusal per boundary at WARNING: the rest of a stale replay would flood the log, but a boundary that
      // is ever wrong must not be invisible, since its only other symptom is an applied index that stops advancing.
      final boolean first = boundaryRefusalWarned != boundary;
      boundaryRefusalWarned = boundary;
      LogManager.instance().log(this, first ? Level.WARNING : Level.FINE,
          "Ignoring applied-position update %s: below the installed snapshot boundary %d, a stale entry from this "
              + "node's pre-install log (issue #8651)", newTI, boundary);
      return false;
    }
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

  /** Records the index a leader-driven install covers; never lowers it (issue #8651). */
  // @VisibleForTesting
  void recordInstalledRaftBoundary(final long index) {
    installedRaftBoundary.accumulateAndGet(index, Math::max);
  }

  /** Whether {@code index} is at or below what a leader-driven install already covers (issue #8651). */
  // @VisibleForTesting
  boolean isBelowInstalledRaftBoundary(final long index) {
    return index <= installedRaftBoundary.get();
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
    final ClientId localClientId = localClientId();
    final boolean isLocalOrigin = localClientId != null && localClientId.equals(request.getClientId());

    final TransactionContext.Builder context = TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request);

    // A schema entry allocates file ids from its proposer's local file manager, so it may only enter the log from the
    // session that allocated them, on this leader, in the term that session was bound to (issue #9547). Checked only when
    // the origin of the request can be told at all: with no client of our own there is nothing to compare against.
    final ByteString content = request.getMessage() != null ? request.getMessage().getContent() : null;
    if (localClientId != null && content != null && !content.isEmpty()
        && RaftLogEntryType.fromId(content.byteAt(0)) == RaftLogEntryType.SCHEMA_ENTRY) {
      final NeedRetryException refusal;
      try {
        refusal = schemaHistory.staleSchemaProposalRefusal(RaftLogEntryCodec.peekDatabaseName(content), isLocalOrigin);
      } catch (final RuntimeException e) {
        return context.build().setException(e);
      }
      if (refusal != null) {
        LogManager.instance().log(this, Level.WARNING, "%s", refusal.getMessage());
        return context.build().setException(refusal);
      }
    }

    // The drop and the install of a restore are fenced to the leadership that held the database for it (issue #10974): a restore
    // whose leader lost the leadership would otherwise reach the next leader through the Ratis client's resend, and replace the
    // database under the writes that leader accepted. Decoded only for these two types, which carry nothing but a name and a flag
    if (localClientId != null && content != null && !content.isEmpty()) {
      final RaftLogEntryType type = RaftLogEntryType.fromId(content.byteAt(0));
      if (type == RaftLogEntryType.DROP_DATABASE_ENTRY || type == RaftLogEntryType.INSTALL_DATABASE_ENTRY) {
        final NeedRetryException refusal;
        try {
          final RaftLogEntryCodec.DecodedEntry fenced = RaftLogEntryCodec.decode(content);
          refusal = schemaHistory.staleRestoreRefusal(fenced.databaseName(), fenced.restoreFenceTerm(), isLocalOrigin);
        } catch (final RuntimeException e) {
          return context.build().setException(e);
        }
        if (refusal != null) {
          LogManager.instance().log(this, Level.WARNING, "%s", refusal.getMessage());
          return context.build().setException(refusal);
        }
      }
    }

    // A transaction entry is validated here against the page versions the log has assigned so far (issue #6965):
    // one validated on its originating node against a version the log has already moved past is refused before Ratis
    // touches it, through the context's exception. That reply is exactly the retryable
    // ConcurrentModificationException a single node raises, and refusing at this stage costs Ratis nothing - a
    // refusal thrown from preAppendTransaction, by contrast, leaks the leader's pending-write permit in Ratis 3.3.0,
    // and enough of them would wedge the leader for good. An accepted entry reserves its versions, so the next entry
    // on the same pages is checked against them rather than against a local copy that has not caught up yet.
    final ByteString data = content;
    if (data == null || data.isEmpty() || RaftLogEntryType.fromId(data.byteAt(0)) != RaftLogEntryType.TX_ENTRY)
      return context.setStateMachineContext(isLocalOrigin ? Boolean.TRUE : null).build();

    final RaftLogEntryCodec.DecodedEntry decoded;
    try {
      decoded = RaftLogEntryCodec.decode(data);
    } catch (final RuntimeException e) {
      // Refuse rather than append an entry no node will be able to read; not a fault of this leader.
      return context.build().setException(e);
    }

    // Not validated against a copy this node is replacing (issue #8022): a node elected in the middle of an install
    // (issue #8491) would otherwise reserve versions read from the pages it is about to discard, and the install then
    // clears those reservations while their entries still wait behind the install lock. Refused like any other entry
    // the leader cannot validate right now; the originator retries against the next leader.
    final String databaseName = decoded.databaseName();
    if (databaseName != null && replacement.databasesBeingReplaced.containsKey(databaseName))
      return context.build().setException(new NeedRetryException(
          "Database '" + databaseName + "' is being replaced with a copy from another node, so the leader cannot "
              + "validate the transaction against it right now. Please retry"));

    // Not accepted while this leader runs a DDL, a drop or a reinstall on the database (issue #7438). A DDL publishes its
    // pages locally BEFORE its SCHEMA_ENTRY is appended, so an entry validated against the pre-DDL version in between
    // claims the same next version as the DDL's embedded WAL and the followers would splice the two; the write lock that
    // excludes the leader's own writers says nothing about a replica's. Only entries of other nodes: the leader's own
    // writers are already excluded by that lock. The check is repeated after the reservation below.
    // A restore holding the database (issue #10974) refuses them for the same reason, see holdForRestore.
    if (!isLocalOrigin && exclusiveWindows.refusesEntriesOfOtherNodes(databaseName)) {
      HALog.log(this, HALog.DETAILED, "Refusing a transaction of another node on database '%s': the leader is running an exclusive operation on it",
          databaseName);
      return context.build().setException(ExclusiveDatabaseWindows.exclusiveOperationRefusal(databaseName));
    }

    // Not accepted when it was prepared before the last schema change this node applied (issue #8686). The position is recorded at
    // APPLY time, and the window between a DDL's append and its apply is closed by the leader-exclusive registration above, which
    // spans the whole DDL; a snapshot install records none, so nothing is refused until the next schema change (as before). It is valid against the
    // page versions - it comes after the change in the log - but its WAL was built against the schema the originator held,
    // so it carries no page changes for an index the change created, and every node would apply it as it stands and leave the
    // record out of that index. Refused BEFORE the reservation below, like the checks above, so it costs Ratis nothing and
    // leaks no permit; the originator waits for the change to be applied and retries under the new schema.
    // Only for entries of OTHER nodes: a DDL runs on the leader, so a transaction of the leader's own that contains one (a
    // CREATE TYPE followed by an INSERT in one transaction) prepared its later records under the new schema although it began
    // before it, and would be refused for a change it made itself. A leader transaction left open across a DDL of another thread
    // is not covered: out of scope here, and the same as on a standalone database.
    if (!isLocalOrigin && databaseName != null && decoded.txPreparedAtIndex() >= 0) {
      final AtomicLong schemaChangeIndex = schemaHistory.lastSchemaChangeIndex.get(databaseName);
      final long schemaChanged = schemaChangeIndex != null ? schemaChangeIndex.get() : -1L;
      if (schemaChanged > decoded.txPreparedAtIndex()) {
        HALog.log(this, HALog.DETAILED,
            "Refusing a transaction on database '%s' prepared at index %d: a schema change was applied at index %d",
            databaseName, decoded.txPreparedAtIndex(), schemaChanged);
        return context.build().setException(
            new ReplicatedSchemaConflictException(databaseName, decoded.txPreparedAtIndex(), schemaChanged));
      }
    }

    // Not accepted from another node while this leader's copy refuses writes (issue #10965): an 'overwrite' restore puts that
    // refusal on the leader's target (issue #10689), and a replica commits its phase 1 locally, where nothing knows of it, so
    // its entry would be acknowledged during the download and destroyed by the swap. The leader's own commits are refused in
    // the database itself. Lock-free: one volatile read of the refusals. Repeated after the reservation below.
    // RestoreAndBackupProtectionTest.ReplicaEntryRefusedDuringRestoreClaim counts the databaseFor() lookups of this method (this one is the first,
    // the validation lookup below the second): adding a lookup before the reservation means updating that test.
    if (!isLocalOrigin && databaseName != null) {
      final NeedRetryException writesRefused = writesRefusedOnLeader(databaseForValidation(databaseName));
      if (writesRefused != null) {
        HALog.log(this, HALog.DETAILED, "Refusing a transaction of another node on database '%s': %s", databaseName,
            writesRefused.getMessage());
        return context.build().setException(writesRefused);
      }
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
          CommittedEntryDecoding.peekWalTransactionId(decoded.walData()), decoded.databaseName(), e.getMessage());
      return context.build().setException(e);
    }

    // The second half of the check above, which alone leaves a window: an entry can pass it just before the operation
    // registers and reserve just after. Registration happens BEFORE the operation waits for the reservations in flight
    // (beginLeaderExclusive), so an entry either sees the registration here and backs out, or its reservation is one the
    // operation waits for. Neither order lets it slip in unseen.
    if (!isLocalOrigin && exclusiveWindows.refusesEntriesOfOtherNodes(databaseName)) {
      pageVersions.release(databaseName, pages, decoded.walData());
      return context.build().setException(ExclusiveDatabaseWindows.exclusiveOperationRefusal(databaseName));
    }

    // The same second half for the write refusal (issue #10965): the claim may go up between the first check and the reservation
    if (!isLocalOrigin) {
      final NeedRetryException writesRefused = writesRefusedOnLeader(db);
      if (writesRefused != null) {
        pageVersions.release(databaseName, pages, decoded.walData());
        return context.build().setException(writesRefused);
      }
    }

    return context.setStateMachineContext(new AppendedEntry(isLocalOrigin, decoded, entryId, pages)).build();
  }

  /**
   * The retryable refusal for an entry of another node when {@code db}, this leader's copy, refuses writes (a restore is replacing
   * it, issue #10965), or {@code null} when it accepts them or is not resolved here (the caller refuses that case on its own).
   */
  private static NeedRetryException writesRefusedOnLeader(final DatabaseInternal db) {
    if (db != null && db.getEmbedded() instanceof LocalDatabase local) {
      final String reason = local.getWriteRefusal();
      if (reason != null)
        return new NeedRetryException(
            "Database '" + local.getName() + "' does not accept writes on the leader right now: " + reason + ". Please retry");
    }
    return null;
  }

  // SchemaChangeHistory holds the implementation of these; they stay here for the callers outside this class.

  NeedRetryException staleSchemaProposalRefusal(final String databaseName, final boolean isLocalOrigin) {
    return schemaHistory.staleSchemaProposalRefusal(databaseName, isLocalOrigin);
  }

  NeedRetryException staleRestoreRefusal(final String databaseName, final long restoreFenceTerm, final boolean isLocalOrigin) {
    return schemaHistory.staleRestoreRefusal(databaseName, restoreFenceTerm, isLocalOrigin);
  }

  void bindSchemaSession(final String databaseName, final long term) {
    schemaHistory.bindSchemaSession(databaseName, term);
  }

  void unbindSchemaSession(final String databaseName, final long term) {
    schemaHistory.unbindSchemaSession(databaseName, term);
  }

  void recordSchemaChangeApplied(final String databaseName, final long index) {
    schemaHistory.recordSchemaChangeApplied(databaseName, index);
  }

  void schemaHistoryIncompleteUpTo(final long index) {
    schemaHistory.schemaHistoryIncompleteUpTo(index);
  }

  boolean schemaHistoryCovers(final long fromExclusive) {
    return schemaHistory.schemaHistoryCovers(fromExclusive);
  }

  long lastSchemaChangeIndexOf(final String databaseName) {
    return schemaHistory.lastSchemaChangeIndexOf(databaseName);
  }

  int schemaChangesAppliedBetween(final String databaseName, final long fromExclusive, final long toInclusive) {
    return schemaHistory.schemaChangesAppliedBetween(databaseName, fromExclusive, toInclusive);
  }

  /** The id of the Raft client this node submits its own entries through, or {@code null} when it has none yet. */
  // @VisibleForTesting
  ClientId localClientId() {
    final RaftHAServer raft = this.raftHAServer;
    final RaftClient client = raft != null ? raft.getClient() : null;
    return client != null ? client.getId() : null;
  }

  /** The Raft term this node is in, or {@code -1} when it cannot be read. */
  // @VisibleForTesting
  long currentRaftTerm() {
    final RaftHAServer raft = this.raftHAServer;
    return raft != null ? raft.getCurrentTerm() : -1L;
  }

  // ExclusiveDatabaseWindows holds the implementation of these; they stay here for the callers outside this class.

  boolean holdForRestore(final String databaseName, final long drainTimeoutMs) {
    return exclusiveWindows.holdForRestore(databaseName, drainTimeoutMs);
  }

  void releaseRestoreHold(final String databaseName) {
    exclusiveWindows.releaseRestoreHold(databaseName);
  }

  boolean isHeldForRestore(final String databaseName) {
    return exclusiveWindows.isHeldForRestore(databaseName);
  }

  void shareRestoreHolds(final Set<String> holds) {
    exclusiveWindows.shareRestoreHolds(holds);
  }

  boolean beginLeaderExclusive(final String databaseName, final long drainTimeoutMs) {
    return exclusiveWindows.beginLeaderExclusive(databaseName, drainTimeoutMs);
  }

  void endLeaderExclusive(final String databaseName) {
    exclusiveWindows.endLeaderExclusive(databaseName);
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

  /**
   * The Ratis {@code StateMachineUpdater} thread, recorded by {@link #applyTransaction}, when it has terminated;
   * {@code null} while it is alive or has not been seen yet (issue #8652). A dead one is the updater that died - a node
   * that applies nothing more and, once Ratis closes the division from the dying thread, rejects every append. Cheap
   * enough for every health tick: one volatile read and {@link Thread#isAlive()}. A restart builds a fresh state machine
   * that has seen no thread yet, which is what clears it.
   * <p>
   * Not covered: an updater that dies in {@link #notifyTermIndexUpdated} before its first {@code applyTransaction} (the
   * {@code Failed updateLastAppliedTermIndex} shape of a poisoned log) never records a thread, and is left to the
   * CLOSING/CLOSED lifecycle checks.
   */
  String describeDeadApplyThread() {
    final Thread thread = applyThread;
    if (thread == null || thread.isAlive())
      return null;
    return "the Ratis StateMachineUpdater thread '" + thread.getName() + "' has terminated";
  }

  /**
   * Runs on the {@code StateMachineUpdater} thread for the entries Ratis applies itself (metadata and configuration
   * entries). A throwable escaping here is what kills the updater and closes the division (issue #8651), so it is logged
   * with its cause before it is rethrown: Ratis reports it only as "caught a Throwable" on its own logger. The calling
   * thread is deliberately not recorded as the updater (issue #8652): this callback is not guaranteed to run on that
   * thread alone, and a short-lived caller must never read as a dead updater.
   * <p>
   * It moves the applied index, so it wakes the threads waiting for one (issue #11742), as {@link #applyTransaction} does. A
   * bookmark this version answers with names the entry the state machine applied, not the metadata entry Ratis appends after
   * it, so a read waiting for such a bookmark is released by {@link #applyTransaction}. The waiters this callback still
   * releases are those whose target can be a Ratis-only index: a bookmark answered by a node of an earlier version, the Ratis
   * index that stands in for a bookmark while the state machine has applied nothing, and the read index a linearizable read
   * on a follower waits for, the leader's commit index, which counts the metadata entries too. Without the notification
   * they saw such an index only at their next recheck, up to {@code APPLY_WAIT_RECHECK_INTERVAL_MS} later.
   */
  @Override
  public void notifyTermIndexUpdated(final long term, final long index) {
    // A configuration or no-op entry of a later term settles a pending publication as well (issue #11240)
    settlePublicationsBefore(term);
    try {
      super.notifyTermIndexUpdated(term, index);
    } catch (final Throwable t) {
      LogManager.instance().log(this, Level.SEVERE,
          "Updating the applied position to (t:%d, i:%d) failed on the Ratis StateMachineUpdater, which now stops and "
              + "closes the division; the health monitor restarts it in place (issue #8652)", t, term, index);
      throw t;
    }
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA != null)
      raftHA.notifyApplied();
  }

  /** Records the calling thread as the updater, as {@link #applyTransaction} does, for tests (issue #8652). */
  // @VisibleForTesting
  void recordApplyThreadForTesting() {
    applyThread = Thread.currentThread();
  }

  /** The thread this state machine last saw apply an entry, so a test can end the real updater (issue #8652). */
  // @VisibleForTesting
  Thread getApplyThreadForTesting() {
    return applyThread;
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

      // Before the entry is applied: one of a later term means a pending publication of an earlier term was truncated, and
      // its proposer must not keep the layout it published (issue #11240)
      settlePublicationsBefore(termIndex.getTerm());

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
        criticalHalt.triggerCriticalHalt(index, "unknown Raft log entry type, most likely written by a newer node version; "
            + "upgrade this node to a version that understands it", null);
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

      // An entry that names no database (the security entries decode to an empty name) has no real install gate to
      // consult, so the global boundary is what marks it stale (issue #8651). Its effects (the security documents)
      // are refreshed by the leader catch-up that follows the install (issue #7833), and applying it now would write
      // an older document over that state and move lastAppliedIndex and the persisted applied index backward. No
      // notifyApplied() either: nothing advanced, and the install already notified for its seed.
      final boolean namesNoDatabase = decoded.databaseName() == null || decoded.databaseName().isEmpty();
      if (namesNoDatabase && isBelowInstalledRaftBoundary(index)) {
        LogManager.instance().log(this, Level.FINE,
            "Skipping entry %d that names no database: already covered by the installed snapshot boundary %d "
                + "(issue #8651)", index, installedRaftBoundary.get());
        return CompletableFuture.completedFuture(Message.valueOf("OK"));
      }

      // Not applied to a copy an install is replacing (issue #7958): see installApplyGates.
      final InstallApplyGate installGate = replacement.enterInstallApplyGate(decoded.databaseName(), index);
      if (installGate != null && index <= installGate.installedIndex()) {
        // Covered by a snapshot an install already brought this database's copy to (issue #8577): this is either
        // this node's own pre-install local log being replayed after a restart, or a duplicate notification.
        // Applying it now would write data the installed copy already carries, and worse, would move
        // lastAppliedIndex/the Ratis applied position backward relative to what the install just recorded,
        // tripping Ratis's monotonic updateLastAppliedTermIndex check and halting the node. Skip as a no-op:
        // nothing is applied, and lastAppliedIndex, the Ratis applied term/index and the persisted applied
        // index are all left untouched, since the install already accounts for this index.
        installGate.unlock();
        LogManager.instance().log(this, Level.FINE,
            "Skipping entry %d for database '%s': already covered by the installed snapshot boundary %d (issue #8577)",
            index, decoded.databaseName(), installGate.installedIndex());
        return CompletableFuture.completedFuture(Message.valueOf("OK"));
      }
      // An entry of an incarnation of this database that a drop removed and a later create replaced (issue #11919): the
      // replay of a log from a snapshot marker older than that drop. Its effect is superseded by the drop, and applied it
      // would reach the copy the later create made - the old create is skipped because the name exists, its schema
      // entries reload the old schema over the new files, and its drop renames the new copy away. Skipped like an entry
      // the copy already carries (issue #8579): nothing is applied, the applied position still advances. Decided under the
      // install gate: an install that acquires a copy records its boundary before it releases the gate, so an entry that
      // waited for that install reads the boundary the install recorded, not the one before it
      final boolean ofEarlierIncarnation = !namesNoDatabase && appliedIndexes.isOfEarlierIncarnation(decoded.databaseName(), index);
      // Already in the copy an install just put in place (issue #8579): the leader that served it had applied this
      // entry before capturing the copy, so applying it again would at best redo what the copy carries, and at worst
      // roll a newer copy back to an older state until the replay catches up (a schema document is written whole).
      // Unlike the #8577 skip above, the entry still advances the applied position below exactly as if it had been
      // applied: an install that is not Ratis's own moves neither lastAppliedIndex nor the Ratis applied position, so
      // this entry is the next one in order and leaving it unrecorded would stall every reader waiting on it.
      final boolean carriedByServedCopy = installGate != null && index <= installGate.servedCopyIndex()
          && replacement.isCarriedByInstalledCopy(decoded.type(), originatedLocally);
      final Runnable dispatch = () -> {
        securitySuperseded[0] = false;
        switch (decoded.type()) {
        case TX_ENTRY -> applyTxEntry(decoded, index, context instanceof AppendedEntry appended ? appended.pages() : null);
        case SCHEMA_ENTRY -> {
          schemaEntries.applySchemaEntry(decoded, index, originatedLocally);
          // After the apply, so a node that reads the position sees the schema it stands for. The originator skips the
          // apply (it made the change locally) and still records it: it is the leader that compares against it.
          if (SchemaChangeHistory.changesSchema(decoded))
            schemaHistory.recordSchemaChangeApplied(decoded.databaseName(), index);
        }
        case INSTALL_DATABASE_ENTRY -> bootstrapInstalls.applyInstallDatabaseEntry(decoded, index);
        case DROP_DATABASE_ENTRY -> dropEntries.applyDropDatabaseEntry(decoded, index);
        case SECURITY_USERS_ENTRY -> securitySuperseded[0] = !securityEntries.applySecurityUsersEntry(decoded, index);
        case SECURITY_GROUPS_ENTRY -> securitySuperseded[0] = !securityEntries.applySecurityGroupsEntry(decoded, index);
        case SECURITY_API_TOKENS_ENTRY -> securitySuperseded[0] = !securityEntries.applySecurityApiTokensEntry(decoded, index);
        case BOOTSTRAP_FINGERPRINT_ENTRY -> bootstrapInstalls.applyBootstrapFingerprintEntry(decoded, index, originatedLocally);
        case DATABASE_CONFIGURATION_ENTRY -> configurationEntries.applyDatabaseConfigurationEntry(decoded, index);
        }
      };
      boolean staleNoDatabaseEntry = false;
      try {
        if (ofEarlierIncarnation)
          logEntryOfEarlierIncarnation(decoded, index);
        else if (carriedByServedCopy)
          LogManager.instance().log(this, Level.FINE,
              "Not re-applying entry %d to database '%s': the copy installed from the leader, served at applied index %d, "
                  + "already carries it (issue #8579)", index, decoded.databaseName(), installGate.servedCopyIndex());
        else if (namesNoDatabase)
          // Re-checked, and the document written, under the lock the install records its boundary under (issue #8651):
          // an install that lands between the check above and the write would otherwise let this stale document
          // overwrite what the post-install security catch-up brings, which runs after the boundary is recorded.
          synchronized (appliedPositionLock) {
            if (isBelowInstalledRaftBoundary(index))
              staleNoDatabaseEntry = true;
            else
              applyRetry.applyWithRetry(index, decoded.databaseName(), dispatch);
          }
        else
          applyRetry.applyWithRetry(index, decoded.databaseName(), dispatch);
      } finally {
        if (installGate != null) {
          // Recorded even when the apply failed: the floor an install derives from it can only be too high, which
          // costs a retry, never too low, which would cost the entry (issue #8454).
          installGate.recordApplied(index);
          installGate.unlock();
        }
      }
      if (staleNoDatabaseEntry)
        return CompletableFuture.completedFuture(Message.valueOf("OK"));

      // After the apply, so the proposer persists its swap over a state the entry is already applied to (issue #11240)
      if (!ofEarlierIncarnation && decoded.type() == RaftLogEntryType.SCHEMA_ENTRY && !pendingPublications.isEmpty())
        confirmPublication(decoded, termIndex.getTerm(), index);

      final long previousApplied;
      synchronized (appliedPositionLock) {
        // Below a Raft install's boundary only for a database that install did not cover (not installed, missing on
        // the leader, or created since): its gate recorded no boundary, so the entry was applied to it, which is
        // right, since nothing replaced its copy. The global positions are the install's, though, and must not move
        // backward (issue #8651): only the database's own applied position is recorded.
        final boolean belowInstall = isBelowInstalledRaftBoundary(index);
        previousApplied = belowInstall ? lastAppliedIndex.get() : lastAppliedIndex.getAndSet(index);
        updateLastAppliedTermIndex(termIndex.getTerm(), index);
        // Record the index globally AND against the database this entry targeted, so the per-database
        // bootstrap replay-skip can trust a value that is not mixed across databases (issue #4824).
        // decoded.databaseName() is null only for database-agnostic entries (e.g. SECURITY_USERS_ENTRY),
        // which advance the global position only. A DROP entry removes the database, so the global
        // position advances and its per-database entry is evicted in a single atomic write (avoids
        // growing the map for the node lifetime with names of dropped databases).
        final boolean drop = decoded.type() == RaftLogEntryType.DROP_DATABASE_ENTRY;
        if (ofEarlierIncarnation) {
          // Issue #11919: the database's own position, and its create index, stay those of the copy the later create made
          if (!belowInstall)
            appliedIndexes.writePersistedAppliedIndex(index, null);
        } else if (belowInstall)
          appliedIndexes.writePersistedDatabaseAppliedIndex(index, decoded.databaseName(), drop);
        else if (drop)
          appliedIndexes.writePersistedAppliedIndexDroppingDatabase(index, decoded.databaseName());
        else
          appliedIndexes.writePersistedAppliedIndex(index, decoded.databaseName());
        // What a replay tells the entries of an earlier incarnation apart by (issue #11919)
        if (!ofEarlierIncarnation && createdDatabase(decoded))
          appliedIndexes.recordDatabaseCreated(index, decoded.databaseName());
      }

      // A security document pinned by a failed write is written again from time to time, on this thread, so a volume
      // that is writable again stops pinning the log without waiting for the next security change (issue #7752)
      final ArcadeDBServer srv = this.server;
      if (srv != null)
        securityDurabilityFloor.retryIfDue(srv.getSecurity());
      // The same for a database configuration whose configuration.json could not be written (issue #10124)
      configurationEntries.retryHeldConfigurationsIfDue();
      // And for a dropped database whose files could not be removed (issue #12611)
      dropEntries.retryUndeletedDropsIfDue();
      // And the creation boundaries, whose last write failed (issue #11919)
      if (appliedIndexes.isCreatedIndexFilePending())
        appliedIndexes.retryCreatedIndexFile();

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
      // An entry left for the replay on restart (issue #9550) was already logged, once, at WARNING where it was decided.
      if (!(e instanceof EntryLeftForReplayException) && (targetDatabase == null || !isDatabaseDiverged(targetDatabase)))
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
        applyRetry.handleUnexpectedApplyError(index, decodeDatabase, e);
      } catch (final ReplicationException quarantined) {
        return CompletableFuture.failedFuture(quarantined);
      } catch (final RuntimeException fatal) {
        criticalHalt.triggerCriticalHalt(index, "a committed entry of type " + e.getType() + " could not be decoded and the "
            + "envelope named no database to quarantine instead: " + e.getMessage(), null);
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
      criticalHalt.triggerCriticalHalt(index, "unexpected error applying a committed entry: " + e, e);
      return CompletableFuture.failedFuture(appliedEntryFailure(index, e));
    }
  }

  /**
   * What the future of a COMMITTED entry fails with, never a bare {@link NeedRetryException} (issue #12365). Ratis wraps the
   * failure of {@link #applyTransaction} in a {@link StateMachineException} for the submitter, exactly as it wraps the refusal
   * {@link #startTransaction} and {@link #preAppendTransaction} raise BEFORE the entry is appended, and
   * {@link com.arcadedb.server.ha.raft.ratis.RatisRefusedEntryErrorFilter#refusedBeforeAppend} reads a
   * {@link StateMachineException} whose direct cause is a {@link NeedRetryException} as that refusal: the submitter withdraws
   * its commit and answers the client a {@link ConcurrentModificationException} for an entry the log holds and every node
   * applies, so a client that retries writes it twice. {@code applyWithRetry} keeps every {@link NeedRetryException} inside, so
   * only an unexpected escape reaches here; it is carried one level down, inside a {@link ReplicationException}, where the
   * refusal test does not look and the submitter answers the outcome it cannot know.
   */
  static Exception appliedEntryFailure(final long index, final Throwable e) {
    if (e instanceof NeedRetryException)
      return new ReplicationException("The committed Raft log entry at index " + index + " failed while being applied: " + e.getMessage(), e);
    return e instanceof Exception ex ? ex : new RuntimeException(e);
  }

  // CriticalHaltHandler holds the implementation of these; they stay here for the callers outside this class.

  void setEmergencyStopForTesting(final Runnable emergencyStop) {
    criticalHalt.setEmergencyStopForTesting(emergencyStop);
  }

  void setJvmHalterForTesting(final IntConsumer jvmHalter) {
    criticalHalt.setJvmHalterForTesting(jvmHalter);
  }

  void setFatalErrorGraceMillisForTesting(final long millis) {
    criticalHalt.setFatalErrorGraceMillisForTesting(millis);
  }

  // EntryApplyRetry holds the implementation of these; they stay here for the callers outside this class.

  void applyWithRetry(final long index, final Runnable applyAction) {
    applyRetry.applyWithRetry(index, applyAction);
  }

  void applyWithRetry(final long index, final String databaseName, final Runnable applyAction) {
    applyRetry.applyWithRetry(index, databaseName, applyAction);
  }

  long lowestHeldEntry() {
    return applyRetry.lowestHeldEntry();
  }

  /**
   * Releases the hold kept for a security document's failed write once {@link SecurityDurabilityFloor} has seen it, and
   * its compare-and-set record, reach the disk again (issue #7752): the retry rewrote it, so nothing is left for the
   * replay to repair, and waiting for the next entry of the same document would keep the log pinned on a quiet cluster.
   */
  private void releaseSecurityHoldOf(final String document) {
    switch (document) {
    case ReplicatedSecurityFingerprintRepository.USERS -> applyRetry.releaseHeldEntry(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_USERS_ENTRY));
    case ReplicatedSecurityFingerprintRepository.GROUPS -> applyRetry.releaseHeldEntry(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_GROUPS_ENTRY));
    case ReplicatedSecurityFingerprintRepository.API_TOKENS ->
        applyRetry.releaseHeldEntry(EntryApplyRetry.heldSecurityKey(RaftLogEntryType.SECURITY_API_TOKENS_ENTRY));
    default -> {
    }
    }
  }

  /**
   * Records a snapshot checkpoint so Ratis can compact the log up to the last-applied index.
   * <p>
   * The ArcadeDB database files on disk are inherently the snapshot state, once they are durable: neither a replicated
   * apply nor a local commit fsyncs a data file, so the pages of every open database are flushed and forced first
   * (issue #11258, {@link #syncDataFilesOfOpenDatabases(Level)}). Returning the last-applied index here tells Ratis it may
   * purge log entries up to that index, reducing log disk usage over time, and a restart replays none of them.
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
   * <p>
   * <b>Clamped below an entry left for replay (issue #9550):</b> an entry whose database was closed under the apply
   * thread while the node is shutting down is not applied and not quarantined either; the checkpoint stops at the index
   * right before it, for the same reason.
   * <p>
   * <b>Clamped below an entry held for the replay (issue #10086):</b> an entry that did not advance the applied index must
   * leave a quarantine, a replay floor or a halt behind, and the ones that cannot be quarantined - a security document or a
   * database configuration in force in memory whose file could not be written, a retry loop that gave up on an entry that
   * names no database - hold the checkpoint the same way, until a later entry supersedes them.
   */
  @Override
  public long takeSnapshot() {
    // An entry left for the replay on restart (issue #9550) did not advance lastAppliedIndex, but a later entry of
    // another database did. The checkpoint stops right before it, so the restart replays it; every entry below the floor
    // was applied, quarantined (refused below) or left for replay itself (a lower floor). The shutdown snapshot Ratis
    // takes in StateMachineUpdater.stop() is the one this mostly applies to.
    // The applied index is read BEFORE the floor: the apply thread raises the floor before any later entry advances the
    // index, so an index that already covers a later entry is always read together with the floor below it. Read the
    // other way round, a checkpoint racing the apply thread could pair the old floor with the advanced index.
    // The entries held for the replay for another reason (issue #10086) lower it the same way: skipped or not made durable
    // while a later one advanced the index.
    final long appliedIndex = lastAppliedIndex.get();
    final long lowestHeld = applyRetry.lowestHeldEntry();
    if (lowestHeld != Long.MAX_VALUE)
      applyRetry.remindThatEntriesAreHeld(lowestHeld);
    // A security entry whose document or compare-and-set record did not reach the disk is kept replayable the same way
    // (issue #7752). The same ordering argument holds: the apply pins the entry (SecurityDurabilityFloor.afterInstall)
    // before lastAppliedIndex moves past it, so an applied index read first is never paired with a floor that does not
    // yet cover it.
    // A compaction whose entry is applied here but whose swap its proposer has not persisted yet is held the same way
    // (issue #11240), raised before the applied index moves past it as well
    final long floor = Math.min(Math.min(Math.min(replayFloor.get(), lowestHeld), securityDurabilityFloor.floor()),
        lowestUnpersistedPublication());
    final long currentIndex = floor == Long.MAX_VALUE ? appliedIndex : Math.min(appliedIndex, floor - 1);
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
    // quarantine always needs a resync to clear (none of the DivergenceCause values heals by itself), the
    // resync is triggered at the mark and re-driven by retryUnfilledSnapshotGap() on every HealthMonitor tick,
    // and the node is out of the ready set the whole time. Purging a log this node still needs is not cheaper
    // than the disk it saves.
    appliedIndexes.ensureAppliedIndexLoaded();
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

    // The pages of every entry up to the checkpoint go to disk before the marker does (issue #11258). Neither the apply
    // nor a local commit fsyncs a data file: a replicated apply writes its pages straight to the files and a follower
    // writes no WAL, a leader's pages go through the asynchronous flush and its WAL is not forced either by default. The
    // restart replays only what follows the marker, so a power loss after a marker over pages still in the OS cache lost
    // them with nothing left to replay: the empty dictionary and bucket files, with crash recovery `completed` over them,
    // of the Jepsen LazyFS runs. appliedIndex was read above, and the apply writes or enqueues an entry's pages before
    // lastAppliedIndex moves past it, so every page of an entry at or below currentIndex is pending or written by now.
    final Consumer<ArcadeStateMachine> beforeCheckpointSync = TEST_BEFORE_CHECKPOINT_SYNC;
    if (beforeCheckpointSync != null)
      beforeCheckpointSync.accept(this);
    if (!syncDataFilesOfOpenDatabases(Level.WARNING)) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not make the data files of every open database durable at index %d; not authorising log purge", currentIndex);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // A database that is not open was forced when it closed, unless the close was crash-equivalent (issue #12025): the
    // flush wait gave up, an fsync failed, the database was fenced. Such a close leaves its pages to the next open's
    // recovery, but the WAL of a replicated commit is not forced (#11746) and a follower writes none, so the pages of the
    // entries it applied since the previous marker may be in the OS cache only. lastAppliedIndex is node-global, and the
    // entries of another database carry it past them: checkpointing it would authorise Ratis to purge the only copy a power
    // loss leaves. Refused until a later instance of that database has forced its files (the sync pass above releases it)
    // or the database is gone. Checked AFTER the sync pass: a close lists its database before it reports itself closed, so
    // an instance the pass skipped as closed is always listed here.
    final List<String> leftToRecovery = databasesClosedWithPagesLeftToRecovery();
    if (!leftToRecovery.isEmpty()) {
      // At WARNING at most every five minutes, as the held-entry reminder: the Ratis auto-trigger retries a refused
      // checkpoint as entries keep arriving, and the condition lasts until the database is reopened or dropped
      final long now = System.currentTimeMillis();
      final boolean remind = now - pagesLeftToRecoveryReminderMs >= PAGES_LEFT_TO_RECOVERY_REMINDER_MS;
      if (remind)
        pagesLeftToRecoveryReminderMs = now;
      final String message = "Skipping snapshot checkpoint at index %d: database(s) %s closed without making their pages "
          + "durable, so the log must stay replayable until they are reopened and forced (issue #12025)";
      if (remind)
        LogManager.instance().log(this, Level.WARNING, message, currentIndex, leftToRecovery);
      else
        HALog.log(this, HALog.BASIC, message, currentIndex, leftToRecovery);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // The columnar marks of every entry up to the checkpoint go to disk before the marker does (plan 3, Task 22). A follower
    // writes no local WAL, so the cleanWALFiles hook that persists the recovery set never covers a replicated change, and a
    // restart replays only what follows the marker: the pages an applied entry marked would otherwise live only in memory.
    // The apply marks before lastAppliedIndex moves past the entry, and appliedIndex was read above, so every mark of an
    // entry at or below currentIndex happens-before this persist. A set that cannot be written refuses the checkpoint, as
    // the WAL hook keeps its WAL file.
    if (!persistColumnarRecoverySets()) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not persist the columnar recovery set of every open database at index %d; not authorising log purge", currentIndex);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // A position at or past the marker goes to disk before the marker does (issue #11517). The per-entry writes of the
    // applied-index file are not fsynced, so a power loss could otherwise leave an older position behind, which the
    // restart's snapshot-gap check reads as a gap below the marker and answers with a full resync from the leader. A
    // later unsynced write can still leave the file empty, which reads as nothing persisted and trusts the marker. Not a
    // condition of the checkpoint: the marker's correctness does not depend on this file.
    appliedIndexes.syncAppliedIndexFileBeforeMarker();

    if (!registerSnapshotMarker(term, currentIndex)) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not persist snapshot marker at index %d; not authorising log purge", currentIndex);
      return RaftLog.INVALID_LOG_INDEX;
    }
    HALog.log(this, HALog.BASIC, "ArcadeStateMachine: snapshot checkpoint at index %d (term %d)", currentIndex, term);
    return currentIndex;
  }

  /**
   * Forces the data files of every open database ahead of a checkpoint, on the caller's thread (issue #11989).
   * {@link #takeSnapshot()} runs on the Ratis StateMachineUpdater, which is also the apply thread: everything it forces
   * there holds every commit of the node until it is done, which after a compaction interval of writes can take seconds.
   * Forcing the files here first, from the thread that asks for the checkpoint, leaves {@link #takeSnapshot()} only what
   * was written in between: it still forces that, so the guarantee of the marker does not depend on this call. A failure
   * is not reported here for the same reason, the checkpoint reports it, and nothing thrown here may keep the caller from
   * asking for the checkpoint.
   * <p>
   * Nothing is forced when Ratis is going to answer the request without a checkpoint, that is when fewer than
   * {@code creationGap} entries were applied since the latest snapshot (the test of {@code RaftServerImpl.takeSnapshotAsync},
   * on the same two indexes): a node below the gap pays no fsync for a tick that purges nothing. An entry applied between
   * this test and Ratis's own can still take the checkpoint without the early pass; it then forces everything itself, as
   * it did before. The same holds if a later Ratis decides differently: this test only chooses where the forcing is paid,
   * a wrong answer costs one early pass too many or one stall on the apply thread, never the durability of the marker.
   *
   * @param creationGap the gap the snapshot request carries; not positive means Ratis's configured one, which is not
   *                    known here, so the files are forced
   */
  void syncDataFilesAheadOfCheckpoint(final long creationGap) {
    if (creationGap > 0) {
      final TermIndex applied = getLastAppliedTermIndex();
      final var latest = storage.getLatestSnapshot();
      if (applied == null || applied.getIndex() - (latest != null ? latest.getIndex() : 0L) < creationGap)
        return;
    }
    try {
      syncDataFilesOfOpenDatabases(Level.FINE);
    } catch (final RuntimeException e) {
      // A database closed or swapped by an install under this pass: the checkpoint forces what is open by then
      LogManager.instance().log(this, Level.FINE, "Could not force the data files ahead of the checkpoint: %s", e.getMessage());
    }
  }

  /**
   * Makes the pages written so far by every open database durable in its data files, for {@link #takeSnapshot()} (issue
   * #11258). Iterates the engine's registry of open instances, as {@link #persistColumnarRecoverySets()} does and for the
   * same reasons. Waits up to {@link #SNAPSHOT_DATA_FLUSH_WAIT_MS} in all, across every database, for the pages pending in
   * the asynchronous flush, not at all while a backup suspends it, then forces the files written since their last fsync: a database with
   * nothing written since the previous checkpoint costs no fsync. A database that is not open was forced when it closed,
   * except after a crash-equivalent close, which {@link #databasesClosedWithPagesLeftToRecovery()} reports (issue #12025).
   *
   * @param failureLevel the level a database whose pages are not all durable is logged at
   *
   * @return false when a database still had pages pending at the deadline or an fsync failed
   */
  private boolean syncDataFilesOfOpenDatabases(final Level failureLevel) {
    if (this.server == null)
      return true;
    // One deadline for the whole checkpoint, not one per database: the apply thread waits at most this long however many
    // databases the node hosts
    final long deadline = System.currentTimeMillis() + SNAPSHOT_DATA_FLUSH_WAIT_MS;
    for (final Database open : DatabaseFactory.getActiveDatabaseInstances())
      if (open instanceof LocalDatabase local && local.isOpen() && !local.getPageManager()
          .flushAndSyncDatabase(local, Math.max(0L, deadline - System.currentTimeMillis()))) {
        // A failed fsync is logged at SEVERE with its exception by FileManager.syncFiles(), naming the file
        LogManager.instance().log(this, failureLevel,
            "Database '%s': its pages are not all durable yet (flush still pending after %dms, flush suspended by a backup, "
                + "or an fsync failed, logged above)", local.getName(), SNAPSHOT_DATA_FLUSH_WAIT_MS);
        return false;
      }
    return true;
  }

  /**
   * The databases of this server whose last close left their pages to the next open's recovery without forcing them, and
   * that no later instance has forced since (issue #12025), for {@link #takeSnapshot()}. Read from the engine's list, which
   * covers every database of the JVM: only the ones under this server's database directory are this node's, since a JVM
   * hosting several servers must not have one server's close hold another's log.
   */
  List<String> databasesClosedWithPagesLeftToRecovery() {
    if (this.server == null)
      return List.of();
    final List<Path> listed = DatabaseFactory.getDatabasesClosedWithPagesLeftToRecovery();
    if (listed.isEmpty())
      return List.of();
    final String dbDir = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    if (dbDir == null)
      return List.of();
    final Path databasesDirectory = Path.of(dbDir).toAbsolutePath().normalize();
    final List<String> result = new ArrayList<>();
    for (final Path path : listed)
      if (path.startsWith(databasesDirectory))
        result.add(databasesDirectory.relativize(path).toString());
    return result;
  }

  /**
   * Persists the columnar recovery set of every open database, for {@link #takeSnapshot()}. Iterates the engine's registry of
   * open instances, never {@code server.getDatabase}: that may take the registry lock the snapshot installer holds across a
   * whole swap, and may open a database a checkpoint has no reason to open. The registry cannot miss a database registered
   * from any path (an embedder's {@code registerDatabase}); in a JVM that hosts several servers it also covers the others'
   * databases, which costs them one extra, harmless persist. A database that is not open has nothing in memory to persist;
   * one without a COLUMNAR index answers at once.
   * <p>
   * Runs on the caller's thread, which for the auto-trigger and the shutdown snapshot is the Ratis StateMachineUpdater: the
   * apply pauses for one recovery-set write (tmp file, fsync, rename) per database with a projection, in sequence.
   *
   * @return false when one of them could not write its set
   */
  private boolean persistColumnarRecoverySets() {
    if (this.server == null)
      return true;
    for (final Database open : DatabaseFactory.getActiveDatabaseInstances())
      if (open instanceof LocalDatabase local && local.isOpen()) {
        final ColumnarIndexManager columnar = local.getColumnarIndexManager();
        if (columnar != null && !columnar.persistRecoverySet())
          return false;
      }
    return true;
  }

  /**
   * Writes an empty Ratis snapshot marker file at {@code (term, index)} and registers it as the
   * latest snapshot in {@link #storage}, so {@link SimpleStateMachineStorage#getLatestSnapshot()}
   * can rediscover it after a restart (it scans for {@code snapshot.<term>_<index>} files).
   * <p>
   * ArcadeDB's real snapshot is the set of database files on disk - made durable before the marker by
   * {@link #takeSnapshot()} and by the snapshot install, which fsyncs what it extracts - so the marker is
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
  boolean registerSnapshotMarker(final long term, final long index) {
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
      // Issue #8491: named here, where the election that caused it is, rather than only at the hand-off the next
      // health check runs (handOffLeadershipWhileReplacingDatabase), which is off this Ratis callback thread.
      if (!replacement.databasesBeingReplaced.isEmpty())
        LogManager.instance().log(this, Level.WARNING,
            "This node was elected leader while it is replacing database(s) %s with the leader's copy; the next health "
                + "check hands leadership to a peer that holds the data", replacement.getDatabasesBeingReplaced());
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
    // A re-notification of the same leader in the same term, or in a term that cannot be read, is not news (issue #11294)
    final boolean sameLeaderNotifiedAgain = newLeaderId.equals(prevId) && (currentTerm < 0 || currentTerm <= prevTerm);
    final boolean followingANewLeader = !newLeaderId.equals(raftHA.getLocalPeerId()) && !sameLeaderNotifiedAgain;

    // Issue #11550: the pending deltas of the per-record materialized views died with the previous leader, so the commits whose
    // pass had not run are in the source types and not in the views. The refresh is the leader's (issue #11031): this one owes
    // every such view a recovery full pass. Not on a re-notification of the same leader in the same term: nothing was lost then
    if (newLeaderId.equals(raftHA.getLocalPeerId()) && !sameLeaderNotifiedAgain) {
      try {
        lifecycleExecutor.submit(() -> viewRecovery.recoverMaterializedViewsAsNewLeader(raftHA));
      } catch (final RejectedExecutionException ree) {
        HALog.log(this, HALog.BASIC, "Cannot schedule the materialized view recovery on the leader change: executor is shut down");
      }
    }

    if (needsSnapshotDownload.compareAndSet(true, false)) {
      LogManager.instance().log(this, Level.INFO,
          "Leader change detected, triggering pending snapshot download from leader %s", leaderName);
      lifecycleExecutor.submit(this::triggerSnapshotDownload);
    } else if (followingANewLeader) {
      // Issue #11294: a targeted resync of a quarantined database, or the retry of a read floor, that was refused
      // because no leader was known (or the leader had no address yet) used to wait for the next HealthMonitor tick,
      // which is throttled to one attempt per snapshot-watchdog interval. A leader just became known, which is the one
      // thing that refusal was waiting for, so the attempt runs now. Queued on the single-threaded lifecycleExecutor,
      // behind any resync still running there, so it never overlaps one; it asks the same questions the tick asks and
      // does nothing on a node with nothing outstanding. Not on a re-notification of the same leader in the same term, nor
      // in a term that cannot be read: nothing a refusal waits on is known to have changed then, and the throttle is there
      // for exactly such repeats.
      try {
        lifecycleExecutor.submit(() -> resync.retryUnfilledSnapshotGap(true));
      } catch (final RejectedExecutionException ree) {
        HALog.log(this, HALog.BASIC, "Cannot schedule the resync retry on the leader change: executor is shut down");
      }
    }
  }

  // MaterializedViewRecovery holds the implementation of these; they stay here for the callers outside this class.

  void recoverMaterializedViewsOfOpenedDatabase(final String dbName, final DatabaseInternal db) {
    viewRecovery.recoverMaterializedViewsOfOpenedDatabase(dbName, db);
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

    // A peer that joins must be asked again before a transaction states its prepared-at index (issue #8686): the cached answer
    // describes the membership that has just changed.
    final RaftHAServer membershipHolder = raftHAServer;
    if (membershipHolder != null) {
      membershipHolder.invalidateTxPreparedAtCapability();
      membershipHolder.invalidateFullTextCorpusDeltaCapability();
      // A peer that joins may predate the fixed-width float arrays: no record is written in that form until it answers (#10856)
      membershipHolder.invalidateFixedWidthFloatArraysCapability();
      // A consistent read waiting here asks again whether this node is still a member (issue #9606)
      membershipHolder.notifyMembershipChanged();
    }

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

      // The misdirected-request listener learns a peer joined after start here, before this node can dial it, so the
      // repair of a channel to it never shares the slot strangers' refusals take (issue #10144)
      if (membershipHolder != null)
        membershipHolder.onRaftConfigurationApplied(peers, oldPeers);

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

  /**
   * The peers the capability gate of the group and API-token entries is refusing for (issue #7511), as the seeder
   * reads them to name the peer a failed seed waited for and to run that seed again (issue #13447).
   */
  private List<String> peersWithholdingSecurityEntries() {
    final RaftHAServer raftHA = this.raftHAServer;
    return raftHA == null ? List.of() : SecurityEntryCapabilityGate.peersWithholdingSecurityEntries(this.server, raftHA);
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

  /**
   * {@link #seedSecurityNowAndReport} answered only by a seed that starts after this call (issue #8689); see
   * {@link MembershipSecuritySeeder#seedAfterInFlightAndReport}.
   */
  public List<String> seedSecurityAfterInFlightAndReport(final String reason, final long timeoutMs) {
    return membershipSecuritySeeder.seedAfterInFlightAndReport(reason, timeoutMs);
  }

  /** Installs the detector {@link RaftHAServer} owns, so it outlives this state machine (issue #7819). */
  void setRuntimeJoinDetector(final RuntimeJoinDetector detector) {
    this.runtimeJoinDetector = detector;
  }

  /** Whether this node was added to the Raft configuration while running (issue #7819). */
  RuntimeJoinDetector getRuntimeJoinDetector() {
    return runtimeJoinDetector;
  }

  /** Package-private test seam (issue #7531): substitutes the seeder the configuration callback drives. */
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
      return CompletableFuture.supplyAsync(() -> leaderSnapshotInstall.installSnapshotFromLeader(roleInfoProto, firstTermIndexInLog),
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

  /** Prefix of both the refusal exception's message and the WARNING it is logged with. */
  static final String SNAPSHOT_INSTALL_REFUSED = "Refusing a leader-initiated snapshot install: ";

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
    final long electionTimeoutMax = RaftPropertiesBuilder.electionTimeoutMaxFor(
        server != null ? server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN)
            : GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN.getValueAsInteger(),
        server != null ? server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX)
            : GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getValueAsInteger());
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

  /** Whether {@link #close()} ran: a closed state machine is never reused and applies nothing more (issue #8781). */
  boolean isClosed() {
    return closed;
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
   * Ratis calls this on the leader for every client entry, under the log's write lock and in exactly the order the
   * entries take in the log. The validation already happened in {@link #startTransaction}; what remains is to confirm
   * the reservation at the point that fixes the entry's position, which is what tells a reservation backed by the log
   * from one left behind by a request Ratis dropped before appending it (issue #6965).
   */
  @Override
  public TransactionContext preAppendTransaction(final TransactionContext trx) throws IOException {
    // The last line of defence of the exclusive window (issue #7438), at the point that fixes the log order. Not airtight:
    // an operation that registers between this read and the append still lets a stalled entry through, which takes a
    // request stalled for longer than the drain grace AND an operation starting in the same instant. In detail: an entry of
    // another node that was accepted before the operation began, and was slow enough to reach the append after it did
    // (a long pause, a backed-up client queue) - which is also why the drain wait can afford to stop counting a
    // reservation that stayed unconfirmed past its grace - must not enter the log now. Refused like the expired
    // reservation below, at the cost of one Ratis pending-write permit, for a window only a stalled request opens.
    if (trx.getStateMachineContext() instanceof AppendedEntry appended && !appended.originatedLocally()
        && exclusiveWindows.refusesEntriesOfOtherNodes(appended.decoded().databaseName())) {
      pageVersions.release(appended.decoded().databaseName(), appended.pages(), appended.decoded().walData());
      // Logged because each of these costs a Ratis pending-write permit (see startTransaction): a leak, if this window
      // turns out to be wider than assumed, must be diagnosable.
      LogManager.instance().log(this, Level.WARNING,
          "Refusing to append tx %d of another node on database '%s': the leader began an exclusive operation on it after "
              + "the entry was accepted", CommittedEntryDecoding.peekWalTransactionId(appended.decoded().walData()), appended.decoded().databaseName());
      final NeedRetryException refusal = ExclusiveDatabaseWindows.exclusiveOperationRefusal(appended.decoded().databaseName());
      throw new StateMachineException(refusal.getMessage(), refusal, false);
    }
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
          CommittedEntryDecoding.peekWalTransactionId(appended.decoded().walData()), appended.decoded().databaseName(), conflict.getMessage());
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
   * Runs a commit that publishes its pages here ahead of the log under the validation of the database, see
   * {@link PageVersionLedger#publishOutsideTheLog}. The ledger's reservation hook is installed on the database first: the
   * commit takes the validation lock before its version checks, through that hook, and a database no entry was validated
   * on yet has none, so the commit would check and publish without the lock (issue #11921).
   * <p>
   * Under the Raft server's publication gate, on the state machine current there: an in-place restart replaces the state
   * machine, and with it the ledger the validations use, only between publications (issue #11921). A caller that resolved
   * this instance before such a restart publishes on the new one.
   */
  <T> T publishOutsideTheLog(final DatabaseInternal db, final Callable<T> commit) throws Exception {
    final RaftHAServer raft = raftHAServer;
    if (raft == null)
      return publishOnThisLedger(db, commit);
    return raft.runPublication(() -> {
      final ArcadeStateMachine current = raft.getStateMachine();
      return (current != null ? current : this).publishOnThisLedger(db, commit);
    });
  }

  private <T> T publishOnThisLedger(final DatabaseInternal db, final Callable<T> commit) throws Exception {
    installReservations(db);
    return pageVersions.publishOutsideTheLog(db.getName(), commit);
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

    @Override
    public void beforeVersionChecks() {
      final ArcadeStateMachine current = raft != null ? raft.getStateMachine() : null;
      (current != null ? current : fallback).pageVersions.lockForPublication(databaseName);
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
   * Installs the ledger on the database, so the leader's own phase-1 validation refuses a page an in-flight entry reserved
   * without waiting for the round trip (see {@link com.arcadedb.engine.PageVersionReservations}), and a commit
   * {@link #publishOutsideTheLog} runs takes the validation lock before its version checks.
   */
  private void installReservations(final DatabaseInternal db) {
    if (db.getEmbedded() instanceof LocalDatabase local && !(local.getPageVersionReservations() instanceof LedgerReservations))
      local.setPageVersionReservations(new LedgerReservations(local.getName(), raftHAServer, this));
  }

  /** The local page versions of a database, as the ledger's seed for pages no in-flight entry reserved. Also installs the ledger. */
  private PageVersionLedger.LocalVersions localVersionsOf(final DatabaseInternal db) {
    installReservations(db);

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
    // INVARIANT (issue #7438): applying an entry takes NO database lock. A leader DDL waits for the reservations of
    // entries in flight to be applied while it holds the database write lock (beginLeaderExclusive); an apply path that
    // took the read lock would make every DDL spend its whole drain budget and then be refused.
    final String databaseName = decoded.databaseName();
    // A transaction this node originated is recognised by its own bytes: the registered transaction carries the WAL
    // it shipped, and an entry is claimed only when it carries the same. No origin marker, client id or context is
    // needed for that, so it holds whatever happened to the leadership, the Raft client or the context in between.
    final LocalCommit local = localCommits.claim(databaseName, CommittedEntryDecoding.walTransactionIdOfCommittedEntry(decoded, entryIndex),
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
   * The local database an entry targets. The one seam an apply resolves a database through, so a unit test can
   * drive {@link #applyTransaction} - or {@link #applySchemaEntry}, which went through {@code server.getDatabase}
   * directly until issue #7695 needed a harness that could reach its buffered-WAL loop - against a database it
   * opened itself.
   */
  // @VisibleForTesting
  DatabaseInternal databaseFor(final String databaseName) {
    return (DatabaseInternal) serverDatabaseFor(databaseName);
  }

  /**
   * The {@link ServerDatabase} behind {@link #databaseFor}: serves an open database, and reopens a closed one only while
   * the node is not shutting down (issue #9550). Also what the bootstrap verification reads its local copy through
   * (issue #9568), so a {@code BOOTSTRAP_FINGERPRINT_ENTRY} applied during a shutdown cannot reopen a closed database.
   */
  private ServerDatabase serverDatabaseFor(final String databaseName) {
    try {
      // An open database is served as before: the server's lock-free fast path, and no load.
      return server.getDatabase(databaseName, false, false);
    } catch (final DatabaseNotAvailableException notOpen) {
      // Not open here (closed, or not loaded yet). Never reopened while the node is shutting down (issue #9550): the
      // close may still be unwinding, in which case the open fails with "Found active instance ... already in use", and
      // a database opened now would be left open by a server that already closed its own.
      if (isNodeShuttingDown())
        throw new DatabaseIsClosedException(
            "Database '" + databaseName + "' is closed and this node is shutting down, so it is not reopened to apply a "
                + "Raft entry; the entry is left for the replay on restart");
      return server.getDatabaseForOperator(databaseName);
    }
  }

  /**
   * Whether this node is shutting down (issue #9550): the Raft HA service was asked to stop, the server is stopping, or
   * the JVM is running its shutdown hooks. The last one is what covers the server's own hook giving up on the lifecycle
   * lock and returning without stopping Raft, while the engine's hook goes on to close every database under the live
   * apply thread. Each signal is one-way: none of them is withdrawn once raised.
   */
  // @VisibleForTesting
  boolean isNodeShuttingDown() {
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA != null && raftHA.isShutdownRequested())
      return true;
    final ArcadeDBServer srv = this.server;
    if (srv != null && srv.getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      return true;
    // Cached once seen: a JVM shutdown is never withdrawn, and a burst of failing entries would otherwise probe per entry
    if (jvmShutdownSeen)
      return true;
    if (!isJvmShuttingDown())
      return false;
    jvmShutdownSeen = true;
    return true;
  }

  /**
   * Whether the JVM has started running its shutdown hooks. The runtime refuses a new hook from then on, which is the
   * only public signal it gives, so this probe registers a throwaway hook and takes it back; deliberately indirect. Costs a thread object and two synchronized calls, so it is read only on the failure
   * paths of an apply, never per entry.
   */
  static boolean isJvmShuttingDown() {
    final Thread probe = new Thread(() -> {
    }, "arcadedb-shutdown-probe");
    try {
      Runtime.getRuntime().addShutdownHook(probe);
    } catch (final IllegalStateException shuttingDown) {
      return true;
    } catch (final RuntimeException cannotTell) {
      // A runtime that refuses hooks for another reason (a security policy) says nothing about a shutdown: answer "no",
      // which keeps the disposition this node had before issue #9550
      return false;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(probe);
    } catch (final IllegalStateException shuttingDown) {
      // The shutdown started between the two calls; the probe is an empty runnable, so running it costs nothing.
      return true;
    }
    return false;
  }

  /**
   * Whether an apply failure on {@code databaseName} is the database having been closed under the apply thread while
   * the node is shutting down (issue #9550). Then nothing diverged and the entry is still in the Raft log, so it is left
   * for the replay on restart rather than quarantining the database. A failure on a database that is still open, or on
   * a node that keeps running, is not this: it keeps the quarantine.
   */
  boolean isClosedUnderShutdown(final String databaseName, final Throwable failure) {
    if (databaseName == null || databaseName.isEmpty() || !isNodeShuttingDown())
      return false;
    // Deliberately broad on the second arm: once the database is closed under a shutting-down node, whatever the apply
    // raised is taken as caused by the close, since the entry's pages cannot be written either way. A genuine fault that
    // happens to coincide with the close is therefore deferred, not quarantined, and meets the same fault again when the
    // entry is replayed on restart, where the quarantine still applies.
    return causedByClosedDatabase(failure, 0) || !isDatabaseOpenHere(databaseName);
  }

  /** Whether {@code t}, its causes or its suppressed exceptions include a {@link DatabaseIsClosedException}. */
  private static boolean causedByClosedDatabase(final Throwable t, final int depth) {
    if (t == null || depth > 16)
      return false;
    if (t instanceof DatabaseIsClosedException)
      return true;
    for (final Throwable suppressed : t.getSuppressed())
      if (causedByClosedDatabase(suppressed, depth + 1))
        return true;
    return t.getCause() != t && causedByClosedDatabase(t.getCause(), depth + 1);
  }

  /** Whether {@code databaseName} is registered and open on this server. Never opens it. */
  boolean isDatabaseOpenHere(final String databaseName) {
    final ArcadeDBServer srv = this.server;
    if (srv == null)
      return true;
    try {
      return srv.getDatabase(databaseName, false, false).isOpen();
    } catch (final DatabaseNotAvailableException e) {
      return false;
    } catch (final RuntimeException e) {
      // Cannot tell: answer "open", which keeps the quarantine, the disposition this node had before issue #9550.
      return true;
    }
  }

  /**
   * Leaves the entry at {@code index} for the replay on restart (issue #9550): records the replay floor
   * {@link #takeSnapshot()} clamps its checkpoint to, and returns the exception the apply fails with. The applied index
   * does not move over the entry, since the apply fails.
   */
  EntryLeftForReplayException leaveForReplay(final long index, final String databaseName, final Throwable failure) {
    // WARNING once, for the first entry left: every entry still in flight for a closed database follows it, and the
    // first one is the one that says why. The rest are FINE, so a burst at shutdown does not flood the log.
    final boolean first = replayFloor.getAndAccumulate(index, Math::min) == Long.MAX_VALUE;
    LogManager.instance().log(this, first ? Level.WARNING : Level.FINE,
        "Raft entry at index %d for database '%s' was not applied: the database was closed while this node is shutting "
            + "down. It is left in the Raft log for the replay on restart, and no snapshot checkpoint will cover it "
            + "(issue #9550): %s", index, databaseName, failure.getMessage());
    return new EntryLeftForReplayException(
        "Raft entry at index " + index + " for database '" + databaseName + "' left for the replay on restart: the "
            + "database was closed while this node is shutting down", failure);
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

      // the entry is durable in the Raft log: its WAL record is not flushed on this thread, see publishReplicatedCommittedPages
      local.transaction().publishReplicatedCommittedPages(local.phase1());
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
        // STRICT: a page this node cannot bring to the entry's version is not reconciled. Skipping it and applying
        // the rest recorded the entry as held while part of it was not, so the reconcile fails instead and the entry
        // takes the doubly-failed path below (issue #7602), which quarantines the database for a resync.
        databaseFor(decoded.databaseName()).getTransactionManager()
            .applyChanges(CommittedEntryDecoding.deserializeWalTransaction(decoded.walData()), decoded.bucketRecordDelta(), decoded.corpusDeltas(),
                false);
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
    final WALFile.WALTransaction walTx = CommittedEntryDecoding.walTransactionOfCommittedEntry(decoded, entryIndex);

    HALog.log(this, HALog.DETAILED, "Applying tx %d to database '%s' (pages=%d)",
        walTx.txId, decoded.databaseName(), walTx.pages.length);

    try {
      db.getTransactionManager().applyChanges(walTx, decoded.bucketRecordDelta(), decoded.corpusDeltas(), false);
    } catch (final WALVersionGapException e) {
      throw divergedOnWalVersionGap(decoded.databaseName(), walTx.txId, entryIndex, e);
    }
  }

  /**
   * The one answer to a WAL page more than one version ahead of the local copy, whichever entry carried it: an
   * intermediate write never reached this node, so the database is quarantined and resynced from a snapshot. The
   * returned exception is what the caller throws.
   */
  ReplicationException divergedOnWalVersionGap(final String databaseName, final long txId, final long entryIndex,
      final WALVersionGapException e) {
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
    if (quarantine.quarantineDatabase(databaseName, DivergenceCause.WAL_VERSION_GAP)) {
      LogManager.instance().log(this, Level.SEVERE,
          "WAL version gap on follower - state divergence detected, triggering snapshot resync (db=%s, txId=%d): %s",
          databaseName, txId, e.getMessage());
      // Targeted at the one database whose WAL has a gap (issue #11489): a gap in one database says nothing about
      // the others, and the full download reinstalled every database on the node (~40 unrelated ones in the report).
      triggerDatabaseResync(databaseName);
      resync.handOffLeadershipIfLeader(databaseName);
    } else if (quarantine.shouldLogDivergedResync(databaseName)) {
      LogManager.instance().log(this, Level.INFO,
          "WAL version gap on database '%s' (snapshot resync in progress); skipping apply at index %d until resync completes",
          databaseName, entryIndex);
    }
    return new ReplicationException("WAL version gap detected - snapshot resync required (db=" + databaseName + ")", e);
  }

  // SchemaEntryApplier holds the implementation of these; they stay here for the callers outside this class.

  void applySchemaEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex, final boolean originatedLocally) {
    schemaEntries.applySchemaEntry(decoded, entryIndex, originatedLocally);
  }

  void applySchemaDelta(final DatabaseInternal db, final RaftLogEntryCodec.DecodedEntry decoded) throws IOException {
    schemaEntries.applySchemaDelta(db, decoded);
  }

  void applySealedBlobs(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedBlob> blobs) throws IOException {
    schemaEntries.applySealedBlobs(db, blobs);
  }

  void applySealedChunks(final DatabaseInternal db, final List<RaftLogEntryCodec.TsSealedChunk> chunks) throws IOException {
    schemaEntries.applySealedChunks(db, chunks);
  }

  boolean incrementalSchemaApplyEnabled() {
    return schemaEntries.incrementalSchemaApplyEnabled();
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
   * handling straight to {@link EntryApplyRetry#handleUnexpectedApplyError}, which quarantines the database and
   * triggers a full snapshot resync - replacing this node's whole file-id space rather than trying to
   * reconcile it entry by entry.
   */
  void createNewFiles(final DatabaseInternal db, final Map<Integer, String> filesToAdd) throws IOException {
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

  /** Where a sealed store shipped in slices is reassembled, beside the file it will replace. */
  static final String SEALED_STAGING_SUFFIX = ".parts";

  // BootstrapInstaller holds the implementation of these; they stay here for the callers outside this class.

  void applyInstallDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex) {
    bootstrapInstalls.applyInstallDatabaseEntry(decoded, entryIndex);
  }

  void applyBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index, final boolean originatedLocally) {
    bootstrapInstalls.applyBootstrapFingerprintEntry(decoded, index, originatedLocally);
  }

  void applyBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    bootstrapInstalls.applyBootstrapFingerprintEntry(decoded, index);
  }

  List<String> getPendingBootstrapReplacements() {
    return bootstrapInstalls.getPendingBootstrapReplacements();
  }

  public void retryPendingBootstrapReplacements() {
    bootstrapInstalls.retryPendingBootstrapReplacements();
  }

  List<String> getBootstrapInstallsInFlight() {
    return bootstrapInstalls.getBootstrapInstallsInFlight();
  }

  // DatabaseReplacement holds the implementation of these; they stay here for the callers outside this class.

  boolean isHoldingInstallApplyGate() {
    return replacement.isHoldingInstallApplyGate();
  }

  InstallApplyGate installApplyGate(final String dbName) {
    return replacement.installApplyGate(dbName);
  }

  void runUnderInstallGate(final String dbName, final DatabaseReconciler.InstallAction install) throws IOException {
    replacement.runUnderInstallGate(dbName, install);
  }

  void runUnderInstallGate(final String dbName, final long installedBoundaryIndex,
      final DatabaseReconciler.InstallAction install) throws IOException {
    replacement.runUnderInstallGate(dbName, installedBoundaryIndex, install);
  }

  void runUnderInstallGate(final String dbName, final LongConsumer underGate, final long installedBoundaryIndex,
      final DatabaseReconciler.InstallAction install) throws IOException {
    replacement.runUnderInstallGate(dbName, underGate, installedBoundaryIndex, install);
  }

  public List<String> getDatabasesBeingReplaced() {
    return replacement.getDatabasesBeingReplaced();
  }

  /**
   * Whether this node, were it the leader, would hold a database it cannot serve and that a peer can (issue #8529) -
   * the condition {@link #handOffLeadershipWhileReplacingDatabase()} moves leadership away from. Any of:
   * <ul>
   *   <li>a database being replaced with the leader's copy (issue #8491, {@link DatabaseReplacement#databasesBeingReplaced});</li>
   *   <li>a pending bootstrap replacement (issue #8367): the copy on disk is the one the committed baseline rejected,
   *       and it stays out of service until it is replaced - which a leader cannot do from itself;</li>
   *   <li>a database the committed bootstrap baseline says the cluster holds and that is not on this node at all
   *       (issue #7298): {@link BootstrapInstaller#installFromLeaderForBootstrap} refuses on a leader. Not the other half of the
   *       unreconciled mark - a copy the #6124 guard KEPT is here and served;</li>
   *   <li>an unfilled stale-snapshot gap (issue #6111): entries this node's databases never received, which the resync
   *       that fills it refuses to run on a leader. Node-global, like the floor itself. The targeted transfer only
   *       picks a peer that is not lagging, which rules out a peer behind on the log but NOT one that shares the same
   *       gap: lag is judged from the log index, and a gap is invisible to it. A gap every node shares is bounded by
   *       the back-off of hand-offs that moved leadership while the gap persisted, not avoided.</li>
   * </ul>
   * A quarantined database is not in the list: {@link RaftHAServer} hands that one off itself (issue #8483).
   * <p>
   * Read on every health tick, so allocation-free and cheap when the answer is "no": three reads of an empty
   * collection or an unset floor. Only a non-empty unreconciled set costs a directory check per marked database.
   */
  boolean hasLeaderServiceGap() {
    if (!replacement.databasesBeingReplaced.isEmpty() || staleSnapshotAppliedFloor.get() >= 0)
      return true;
    baselines.ensureBootstrapBaselinesLoaded();
    if (!bootstrapReplacementsPending.isEmpty())
      return true;
    if (baselines.bootstrapUnreconciledDatabases.isEmpty())
      return false;
    for (final String dbName : baselines.bootstrapUnreconciledDatabases)
      if (!isDatabasePresentLocally(dbName))
        return true;
    return false;
  }

  /**
   * The conditions {@link #hasLeaderServiceGap()} tests, one line each for the log; empty when there are none.
   * <p>
   * MUST stay a strict mirror of that predicate: every condition it tests has a line here, and nothing else does. The
   * hand-off reads this list after the predicate and treats an empty one as "the gap closed in between", so a condition
   * the predicate reports and this list omits would make every hand-off a silent no-op.
   * {@code LeadershipHandOffReachabilityTest} pins the two against each other for every condition.
   */
  List<String> describeLeaderServiceGaps() {
    final List<String> gaps = new ArrayList<>(4);
    final List<String> replacing = replacement.getDatabasesBeingReplaced();
    if (!replacing.isEmpty())
      gaps.add("replacing " + replacing + " with the leader's copy (issue #8491)");
    final long floor = staleSnapshotAppliedFloor.get();
    if (floor >= 0)
      gaps.add("unfilled stale-snapshot gap: its databases hold no entry past index " + floor + " (issue #6111)");
    final List<String> pending = bootstrapInstalls.getPendingBootstrapReplacements();
    if (!pending.isEmpty())
      gaps.add("still holding the copy of " + pending + " the bootstrap baseline rejected (issue #8367)");
    final BootstrapUnreconciled unreconciled = bootstrapDivergence.getBootstrapUnreconciled(null);
    if (!unreconciled.missingLocally().isEmpty())
      gaps.add("missing " + unreconciled.missingLocally() + ", which the bootstrap baseline committed (issue #7298)");
    return gaps;
  }

  /**
   * Hands leadership to a peer when this node is the leader and cannot serve a database a peer can (issue #8529, see
   * {@link #hasLeaderServiceGap()}), the first of which was a leader replacing one of its databases with the leader's
   * copy (issue #8491). Driven by the {@link HealthMonitor} tick, through {@link RaftHAServer}, which runs it on the
   * executor the other automatic hand-offs share so that none of them races it (issue #8557).
   * <p>
   * A node can be elected in the middle of such an install: an operator resync ({@link #resyncDatabaseFromLeader})
   * or a targeted resync of a quarantined database leaves its Raft log complete, and Raft elects on the log alone.
   * From then on the install has nowhere to download from - the leader address it resolves on each retry is its own,
   * which the guard refuses - and every client request on the database is refused on this node while the copy is
   * being replaced (issue #8363). The majority is healthy and the cluster rejects every write to that database.
   * <p>
   * Handing leadership to a peer fixes both at once: the peer holds the data and serves the writes, and the
   * install's next download attempt resolves that peer as its source and completes. It is the targeted transfer
   * {@link RaftHAServer#transferLeadership(long)} runs, which only picks a peer that is not lagging and that answered
   * this leader recently (issue #8556).
   * <p>
   * No-op when {@link #hasLeaderServiceGap()} is false, on a follower, and until
   * {@link #replacingLeaderHandOffIntervalMs} has passed since the previous attempt ENDED - an interval that doubles
   * with each failure in a row (issue #8556), and with each hand-off that moved leadership while the gap stayed open
   * on this node (issue #8529) - so a cluster where no peer can take over, or where every peer shares the gap, is not
   * put through an election on every tick. The name predates #8529 and is kept for the {@link HealthMonitor} hook.
   * Blocks the caller for up to
   * {@link #REPLACING_LEADER_HAND_OFF_TIMEOUT_MS} while a hand-off runs.
   *
   * @return whether leadership moved to another node
   */
  public boolean handOffLeadershipWhileReplacingDatabase() {
    if (!hasLeaderServiceGap()) {
      resetReplacingLeaderHandOffBackOff();
      return false;
    }
    final RaftHAServer raftHA = this.raftHAServer;
    if (raftHA == null || !raftHA.isLeader()) {
      resetReplacingLeaderHandOffBackOff();
      return false;
    }

    // Read before the throttle slot is claimed: an install that finished since the first read must not spend the slot,
    // or a replacement starting right after it would wait out a whole interval for a hand-off that never ran.
    final List<String> gaps = describeLeaderServiceGaps();
    if (gaps.isEmpty()) {
      // The last gap closed between the two reads: the episode is over, as on the fast path above.
      resetReplacingLeaderHandOffBackOff();
      return false;
    }

    if (!replacingLeaderHandOffDue())
      return false;
    // Single attempt, no retry loop: a lost claim means a concurrent caller (the health tick, or a direct call) is
    // running the hand-off itself.
    if (!replacingLeaderHandOffRunning.compareAndSet(false, true))
      return false;
    boolean attempted = false;
    boolean moved = false;
    try {
      // Re-checked under the claim: a caller that finished between the check above and the claim has just stamped it.
      if (!replacingLeaderHandOffDue())
        return false;

      LogManager.instance().log(this, Level.WARNING,
          "This node is the leader but cannot serve every database a peer can: %s. A leader cannot install the "
              + "missing data from itself, so it stays that way for as long as it leads. Handing leadership to a peer "
              + "that holds the data (issues #8491, #8529)", gaps);
      attempted = true;
      try {
        // No bare step-down when no peer is eligible (issue #8665): every peer that reported the same gap is screened
        // out of the candidates, and a step-down with no target would hand the leadership to whichever node the
        // election picks, gap or not. No eligible peer is "no peer took over", which backs off.
        moved = raftHA.transferLeadership(REPLACING_LEADER_HAND_OFF_TIMEOUT_MS, false);
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Leadership hand-off of a leader that cannot serve every database (%s) failed: %s. Retrying in %d ms",
            gaps, e.getMessage(),
            replacingLeaderHandOffIntervalMs(replacingLeaderHandOffFailures + 1));
        return false;
      }
      if (moved)
        LogManager.instance().log(this, Level.INFO,
            "Leadership handed off (%s); the missing data is installed from the new leader", gaps);
      else
        LogManager.instance().log(this, Level.WARNING,
            "Could not hand off leadership (%s): no peer took over. Retrying in %d ms; to move "
                + "it by hand, run POST /api/v1/cluster/leader", gaps,
            replacingLeaderHandOffIntervalMs(replacingLeaderHandOffFailures + 1));
      return moved;
    } finally {
      // Stamped when the attempt ENDS (issue #8556), whatever it returned or threw, so the interval is a real pause
      // between attempts rather than a period that a slow attempt can fill end to end.
      if (attempted) {
        lastReplacingLeaderHandOffEndMs = Math.max(1L, replacingLeaderHandOffClock.getAsLong());
        replacingLeaderHandOffFailures = moved ? 0 : Math.min(replacingLeaderHandOffFailures + 1,
            REPLACING_LEADER_HAND_OFF_MAX_FAILURES_COUNTED);
        if (moved)
          leaderHandOffsMovedWhileGapPersisted = Math.min(leaderHandOffsMovedWhileGapPersisted + 1,
              REPLACING_LEADER_HAND_OFF_MAX_FAILURES_COUNTED);
      }
      replacingLeaderHandOffRunning.set(false);
    }
  }

  /**
   * Forgets the failures in a row once the condition they were counted for is over - no leader service gap, or this
   * node is no longer the leader - so the next episode starts from the base interval (issue #8556). The hand-offs
   * that moved leadership are forgotten only once the gap itself is gone (issue #8529). The pause after the last
   * attempt still applies. One or two volatile reads when there is nothing to reset.
   * <p>
   * Takes the same claim an attempt takes, so it can never zero the count under an attempt in flight, whose own
   * failure would then restart the streak (code review on PR #8597). Losing the claim to an attempt skips the reset,
   * which that attempt's outcome supersedes anyway.
   * <p>
   * Package-private: {@link RaftHAServer#queueReplacingDatabaseHandOff} calls it on the ticks that queue nothing.
   */
  void resetReplacingLeaderHandOffBackOff() {
    // The moved hand-offs are forgotten only once the gap itself is gone (issue #8529), not merely because this node
    // stopped leading: that is what each of them achieved, and a gap no peer can close brings leadership back here.
    // Evaluated only when there is something to forget, so a healthy node's tick stays one volatile read; a node that
    // handed off and still has its gap re-checks it on each tick until it closes, a stat per marked database at most.
    final boolean forgetMoved = leaderHandOffsMovedWhileGapPersisted > 0 && !hasLeaderServiceGap();
    if (replacingLeaderHandOffFailures == 0 && !forgetMoved)
      return;
    if (!replacingLeaderHandOffRunning.compareAndSet(false, true))
      return;
    try {
      replacingLeaderHandOffFailures = 0;
      if (forgetMoved)
        leaderHandOffsMovedWhileGapPersisted = 0;
    } finally {
      replacingLeaderHandOffRunning.set(false);
    }
  }

  /** Whether the throttle of {@link #handOffLeadershipWhileReplacingDatabase()} admits an attempt now. */
  private boolean replacingLeaderHandOffDue() {
    final long lastEnd = lastReplacingLeaderHandOffEndMs;
    return lastEnd == 0 || replacingLeaderHandOffClock.getAsLong() - lastEnd >= replacingLeaderHandOffIntervalMs(
        Math.max(replacingLeaderHandOffFailures, leaderHandOffsMovedWhileGapPersisted));
  }

  /**
   * The pause after a hand-off of a leader replacing a database before the next one (issue #8556):
   * {@link #REPLACING_LEADER_HAND_OFF_INTERVAL_MS} after one that moved leadership or after the first failure, then
   * doubling with each further failure in a row, up to {@link #REPLACING_LEADER_HAND_OFF_MAX_INTERVAL_MS}. A hand-off
   * that cannot succeed - no peer can take over - must not run at a steady duty cycle: every attempt that aims a
   * targeted transfer at a peer that cannot win refuses every write on this leader while it is pending.
   */
  static long replacingLeaderHandOffIntervalMs(final int consecutiveFailures) {
    if (consecutiveFailures <= 1)
      return REPLACING_LEADER_HAND_OFF_INTERVAL_MS;
    final int doublings = Math.min(consecutiveFailures - 1, 16);
    return Math.min(REPLACING_LEADER_HAND_OFF_INTERVAL_MS << doublings, REPLACING_LEADER_HAND_OFF_MAX_INTERVAL_MS);
  }

  /**
   * The applied index a snapshot this node serves is at least as current as (issue #8454): every entry up to it has
   * been applied here, its pages published, before this is read, so a copy captured after the read carries them.
   * Sent to the installing follower as {@link SnapshotManager#APPLIED_INDEX_HEADER}.
   * <p>
   * Not true of a database this node has quarantined, whose skipped entry this index already counts: the handler
   * refuses to serve such a database at all (issue #8468, see {@link #quarantineCause(String)}).
   */
  long appliedIndexForServedSnapshot() {
    return lastAppliedIndex.get();
  }

  // UnverifiedCopyReverifier holds the implementation of these; they stay here for the callers outside this class.

  public void reverifyUnverifiedClosedCopies() {
    unverifiedCopies.reverifyUnverifiedClosedCopies();
  }

  public void restartUnverifiedClosedCopyReverification() {
    unverifiedCopies.restartUnverifiedClosedCopyReverification();
  }

  /**
   * Test barrier: returns once every task submitted to the single-threaded {@code lifecycleExecutor} before this
   * call has finished, by queueing a no-op behind them and waiting for it.
   */
  // @VisibleForTesting
  void awaitLifecycleTasksForTesting(final long timeoutMs) throws Exception {
    lifecycleExecutor.submit(() -> {
    }).get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Whether {@code dbName}'s directory is held by the first-formation bootstrap right now (issue #8363): an install
   * replacing it is running, or a failed one has handed its holder to the retry it scheduled. The second half is why
   * {@code RaftReplicatedDatabase} asks this as well as {@link SnapshotInstaller#isInstallInFlight(String)}: between
   * a failed download and its retry no install is registered there, and the copy on disk is still the one the
   * committed baseline decided against.
   * <p>
   * Asked on every client request, so the common answer - nothing in flight at all - costs one map read.
   */
  public boolean isBootstrapInstallInFlight(final String dbName) {
    // One volatile read once loaded; restores a replacement left pending before a restart (issue #8411).
    baselines.ensureBootstrapBaselinesLoaded();
    return !bootstrapInstalls.bootstrapInstallsInFlight.isEmpty() && bootstrapInstalls.bootstrapInstallsInFlight.containsKey(dbName);
  }

  // BootstrapPasses holds the implementation of these; they stay here for the callers outside this class.

  List<String> getBootstrapPassesDeciding() {
    return bootstrapPasses.getBootstrapPassesDeciding();
  }

  void announceBootstrapPass(final String passId, final Collection<String> dbNames, final long holdMs) {
    bootstrapPasses.announceBootstrapPass(passId, dbNames, holdMs);
  }

  void holdOwnBootstrapPass(final String passId, final Collection<String> dbNames) {
    bootstrapPasses.holdOwnBootstrapPass(passId, dbNames);
  }

  void concludeBootstrapPass(final String passId, final Collection<String> committed) {
    bootstrapPasses.concludeBootstrapPass(passId, committed);
  }

  public boolean isBootstrapPassPending(final String dbName) {
    return bootstrapPasses.isBootstrapPassPending(dbName);
  }

  public String bootstrapWindowReason() {
    return bootstrapPasses.bootstrapWindowReason();
  }

  /**
   * Operator-triggered emergency recovery: drop the local copy of {@code dbName} and re-acquire a
   * fresh full snapshot from the current leader. This is the manual equivalent of the automatic
   * snapshot install path ({@link #notifyInstallSnapshotFromLeader}) and uses the same crash-safe
   * {@link SnapshotInstaller} machinery as {@link BootstrapInstaller#installFromLeaderForBootstrap}.
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
    resyncDatabaseFromLeader(dbName, null);
  }

  /**
   * As {@link #resyncDatabaseFromLeader(String)}, carrying a leader-driven {@code order} (issue #8490) that is checked
   * against this node's own term and applied index twice: before anything is logged or purged, and again holding the
   * database's install lock right before the download starts - the last moment the apply thread can still have moved
   * this database, since it waits on that lock for the rest of the install. {@code null} (an operator's resync) is
   * unconditional.
   *
   * @throws StaleResyncOrderException when the order no longer holds; the local copy is left in place
   */
  public void resyncDatabaseFromLeader(final String dbName, final StalledResyncOrder order) {
    final RaftHAServer raft = raftHAServer;
    if (raft == null)
      throw new ReplicationException("Cannot resync database '" + dbName + "': Raft HA is not enabled");

    if (raft.isLeader())
      throw new ReplicationException("Cannot resync database '" + dbName
          + "' on the leader: the leader holds the authoritative copy. Run the resync on the diverged follower.");

    final PeerDialAddress source = resync.resolveSnapshotSource(raft.getLeaderId());
    if (source.refused())
      // The two checks this path used to make by hand (is this the leader, is the address known) are two of the
      // three the helper makes, and the third - an address that identifies no single peer - is the one an
      // operator most needs told about before a database is replaced (issue #6202).
      throw new ReplicationException("Cannot resync database '" + dbName + "': " + source.refusal());

    if (order != null)
      SnapshotResync.checkStalledResyncOrder(raft, dbName, order, -1L);

    LogManager.instance().log(this, Level.WARNING,
        order != null
            ? "Leader-driven resync of database '%s' (stalled replica recovery): dropping local copy and re-acquiring full snapshot"
            : "Operator-triggered resync of database '%s' from leader: dropping local copy and re-acquiring full snapshot",
        dbName);

    try {
      // Resolve the leader address on each retry (it can change mid-operation if leadership moves) - and re-guard
      // it on each retry with it, or the refusal above is a point-in-time check that a later attempt walks
      // straight past onto this node's own address (issue #6202). The check above is still worth making: it turns
      // an already-doomed request into an immediate, descriptive refusal instead of a failed download.
      // install() keeps the local copy open and serving during the download and only closes + swaps
      // once a complete snapshot is on disk, rolling back on failure. A failed resync therefore never
      // leaves the database closed (the cause of the operator-visible DatabaseIsClosedException).
      final String clusterToken = raft.getClusterToken();
      replacement.installLeaderCopy(dbName, resync::guardedLeaderHttpAddress, resync::guardedLeaderHttpsAddress, clusterToken,
          order == null ? null : appliedUnderGate -> SnapshotResync.checkStalledResyncOrder(raft, dbName, order, appliedUnderGate));
      LogManager.instance().log(this, Level.INFO, "Database '%s' resynced from leader on %s", dbName,
          order != null ? "the leader's stalled-replica recovery" : "operator request");
      // This is the action the bootstrap-divergence alert asks the operator for: the local copy the
      // overwrite guard kept has just been replaced, so the mark goes with it (issue #6124) - and so does a
      // bootstrap replacement still pending on it (issue #8367).
      baselines.clearBootstrapUnreconciled(dbName);
      bootstrapInstalls.settleBootstrapReplacement(dbName);
    } catch (final IOException e) {
      throw new ReplicationException("Failed to resync database '" + dbName + "' from leader", e);
    }
  }

  // SnapshotResync holds the implementation of these; they stay here for the callers outside this class.

  void publishStaleSnapshotFloor(final long floor) {
    resync.publishStaleSnapshotFloor(floor);
  }

  AtomicLong staleSnapshotRetryStampForTesting() {
    return resync.staleSnapshotRetryStampForTesting();
  }

  void clearStaleSnapshotFloor() {
    resync.clearStaleSnapshotFloor();
  }

  public void retryUnfilledSnapshotGap() {
    resync.retryUnfilledSnapshotGap();
  }

  void retryUnfilledSnapshotGap(final boolean leaderChanged) {
    resync.retryUnfilledSnapshotGap(leaderChanged);
  }

  public long getStaleSnapshotAppliedFloor() {
    return resync.getStaleSnapshotAppliedFloor();
  }

  public long getDatabaseAppliedFloor(final String dbName) {
    return resync.getDatabaseAppliedFloor(dbName);
  }

  void settleDivergedStateAfterInstall(final Set<String> notInstalled, final long snapshotIndex) {
    resync.settleDivergedStateAfterInstall(notInstalled, snapshotIndex);
  }

  void completeSnapshotInstall(final long snapshotIndex, final Set<String> notInstalled) {
    resync.completeSnapshotInstall(snapshotIndex, notInstalled);
  }

  boolean completeSnapshotInstall(final long snapshotIndex, final Set<String> notInstalled, final Set<String> notRefreshed) {
    return resync.completeSnapshotInstall(snapshotIndex, notInstalled, notRefreshed);
  }

  boolean settleDivergedStateAfterInstall(final Set<String> notInstalled, final long snapshotIndex, final boolean recordAppliedPositions) {
    return resync.settleDivergedStateAfterInstall(notInstalled, snapshotIndex, recordAppliedPositions);
  }

  boolean settleDivergedStateAfterInstall(final Set<String> notInstalled, final long snapshotIndex,
      final boolean recordAppliedPositions, final Set<String> notRefreshed) {
    return resync.settleDivergedStateAfterInstall(notInstalled, snapshotIndex, recordAppliedPositions, notRefreshed);
  }

  StaleSnapshotAcceptance acceptStaleSnapshotFloor(final String acceptedBy) throws IOException {
    return resync.acceptStaleSnapshotFloor(acceptedBy);
  }

  public void recoverFromPersistentLag() {
    resync.recoverFromPersistentLag();
  }

  // BootstrapBaselines holds the implementation of these; they stay here for the callers outside this class.

  public BootstrapBaseline getBootstrapBaseline(final String dbName) {
    return baselines.getBootstrapBaseline(dbName);
  }

  void markBootstrapUnreconciled(final String dbName) {
    baselines.markBootstrapUnreconciled(dbName);
  }

  void clearBootstrapUnreconciled(final String dbName) {
    baselines.clearBootstrapUnreconciled(dbName);
  }

  void clearAllBootstrapUnreconciled() {
    baselines.clearAllBootstrapUnreconciled();
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
  // @VisibleForTesting (package-private, issue #8843)
  BootstrapBaseline readLocalBootstrapState(final String dbName) throws Exception {
    return readLocalBootstrapState(dbName, BootstrapFingerprint.SETTLE_MAX_WAIT_MILLIS);
  }

  /** {@link #readLocalBootstrapState(String)} with an explicit flush-settle bound - see {@link BootstrapDivergenceCheck#settleBudget}. */
  BootstrapBaseline readLocalBootstrapState(final String dbName, final long maxSettleMillis) throws Exception {
    // Never reopens a closed database while the node is shutting down (issue #9568)
    return localBootstrapState(serverDatabaseFor(dbName), maxSettleMillis);
  }

  /**
   * The {@code (fingerprint, lastTxId)} pair of one open database, or {@code null} when it is not backed by a
   * {@link LocalDatabase}. The ONE place every bootstrap participant reads its own state from - the state machine's
   * verification and re-verification, the election's local sample, and the {@code bootstrap-state} answer to a peer -
   * so the pair means the same thing wherever two of them are compared.
   * <p>
   * The fingerprint is taken with {@link BootstrapFingerprint#computeSettled}, never over the raw files (issue #8843):
   * the database is open, its last commits may still be queued in the asynchronous page flush, and two samples of one
   * unchanged copy either side of that flush would otherwise read a matching peer as a mismatched one. The wait is
   * bounded by the backlog found at the call and by {@link BootstrapFingerprint#SETTLE_MAX_WAIT_MILLIS}, so the Raft
   * apply thread and a leader under sustained writes are never held for the duration of the writes. A caller that
   * reads several databases in one sweep passes {@link BootstrapDivergenceCheck#settleBudget} of ONE deadline shared by the sweep instead, so a
   * wedged disk or a suspended flush costs the sweep that bound once, not once per database.
   * <p>
   * {@code lastTxId} is read after the fingerprint, not atomically with it: a commit landing in between is counted by
   * the id and missing from the digest. Only a database under concurrent writes can see that, and its fingerprint
   * drifts with every commit anyway - the pair is meaningful at the cold-start boundary, where nothing is writing.
   */
  static BootstrapBaseline localBootstrapState(final ServerDatabase serverDb) {
    return localBootstrapState(serverDb, BootstrapFingerprint.SETTLE_MAX_WAIT_MILLIS);
  }

  static BootstrapBaseline localBootstrapState(final ServerDatabase serverDb, final long maxSettleMillis) {
    final DatabaseInternal embedded = serverDb.getWrappedDatabaseInstance().getEmbedded();
    if (!(embedded instanceof LocalDatabase localDb))
      return null;
    return new BootstrapBaseline(BootstrapFingerprint.computeSettled(localDb, maxSettleMillis),
        localDb.getLastTransactionId());
  }

  // BootstrapDivergenceCheck holds the implementation of these; they stay here for the callers outside this class.

  public List<String> getBootstrapUnreconciledDatabases() {
    return bootstrapDivergence.getBootstrapUnreconciledDatabases();
  }

  public BootstrapUnreconciled getBootstrapUnreconciled(final Set<String> seen) {
    return bootstrapDivergence.getBootstrapUnreconciled(seen);
  }

  public void verifyBootstrapDivergence() {
    bootstrapDivergence.verifyBootstrapDivergence();
  }

  boolean claimBootstrapDivergenceCheckSlot(final long now, final long intervalMs) {
    return bootstrapDivergence.claimBootstrapDivergenceCheckSlot(now, intervalMs);
  }

  void reconcileBootstrapDivergence(final Map<String, BootstrapBaseline> leaderStates) {
    bootstrapDivergence.reconcileBootstrapDivergence(leaderStates);
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
   * Whether this node holds a copy of {@code dbName} at all: registered in the server, OR merely closed with its
   * directory still on disk. The question two separate decisions turn on (review on PR #7953), so it is named
   * once rather than spelled out at each - inverted, which is how it is actually written at both.
   * <ul>
   *   <li>{@link BootstrapInstaller#installFromLeaderForBootstrap} refuses the leader short-circuit when this is false: a leader
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
  boolean isDatabasePresentLocally(final String dbName) {
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
  boolean databaseDirectoryExists(final String dbName) {
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
    final Path appliedIndexFile = appliedIndexes.getAppliedIndexFile();
    if (appliedIndexFile != null && Files.exists(appliedIndexFile))
      return false;
    // Reached only when the file path is unresolvable (server not wired yet, getAppliedIndexFile()
    // == null) or the file did not exist at the check above: read the persisted value as the final
    // signal. It is -1 for a genuinely fresh node.
    return appliedIndexes.readPersistedAppliedIndex() < 0;
  }

  // ConfigurationEntryApplier holds the implementation of these; they stay here for the callers outside this class.

  void setConfigurationRetryClock(final LongSupplier clock) {
    configurationEntries.setConfigurationRetryClock(clock);
  }

  /** Whether {@code decoded} is a create (not a restore) whose database exists here once it is applied (issue #11919). */
  private boolean createdDatabase(final RaftLogEntryCodec.DecodedEntry decoded) {
    final ArcadeDBServer srv = server;
    return decoded.type() == RaftLogEntryType.INSTALL_DATABASE_ENTRY && !decoded.forceSnapshot() && srv != null
        && srv.existsDatabase(decoded.databaseName());
  }

  /**
   * Says why an entry of a dropped incarnation is not applied (issue #11919): the drop, whose replay used to rename the
   * current copy away, at INFO, the rest of what that incarnation wrote at FINE.
   */
  private void logEntryOfEarlierIncarnation(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    LogManager.instance().log(this, decoded.type() == RaftLogEntryType.DROP_DATABASE_ENTRY ? Level.INFO : Level.FINE,
        "Not applying the %s entry at index %d to database '%s': it is below the index %d the copy this node holds starts "
            + "at, created there or acquired from the leader (issue #11919)", decoded.type(), index, decoded.databaseName(),
        appliedIndexes.createdIndex(decoded.databaseName()));
  }

  // DropDatabaseApplier holds the implementation of these; they stay here for the callers outside this class.

  /**
   * For the tests that drive the drop outside the log: with no index of its own, a failed drop has no entry to hold. The
   * hold it registers at {@link RaftLog#INVALID_LOG_INDEX} is below every install boundary, so {@link #lowestHeldEntry()}
   * drops it and no checkpoint is lowered by it.
   */
  // @VisibleForTesting
  void applyDropDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    dropEntries.applyDropDatabaseEntry(decoded, RaftLog.INVALID_LOG_INDEX);
  }

  // @VisibleForTesting
  void applyDropDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    dropEntries.applyDropDatabaseEntry(decoded, index);
  }

  // @VisibleForTesting
  void setUndeletedDropRetryClock(final LongSupplier clock) {
    dropEntries.setUndeletedDropRetryClock(clock);
  }

  void setLocalDropVerbs(final LocalDropVerbs localDropVerbs) {
    dropEntries.setLocalDropVerbs(localDropVerbs);
  }

  void setDeferredDatabaseDeleter(final DeferredDatabaseDeleter deleter) {
    dropEntries.setDeferredDatabaseDeleter(deleter);
  }

  // AppliedIndexFile holds the implementation of these; they stay here for the callers outside this class.

  long readPersistedAppliedIndex() {
    return appliedIndexes.readPersistedAppliedIndex();
  }

  long readAppliedIndexCounter() {
    return appliedIndexes.readAppliedIndexCounter();
  }

  long readPersistedAppliedIndex(final String dbName) {
    return appliedIndexes.readPersistedAppliedIndex(dbName);
  }

  void writePersistedAppliedIndex(final long index, final String dbName) {
    appliedIndexes.writePersistedAppliedIndex(index, dbName);
  }

  void writePersistedDatabaseAppliedIndex(final long index, final String dbName, final boolean drop) {
    appliedIndexes.writePersistedDatabaseAppliedIndex(index, dbName, drop);
  }

  void writePersistedAppliedIndexDroppingDatabase(final long index, final String dbName) {
    appliedIndexes.writePersistedAppliedIndexDroppingDatabase(index, dbName);
  }

  void writePersistedAppliedIndexForAllDatabases(final long index) {
    appliedIndexes.writePersistedAppliedIndexForAllDatabases(index);
  }

  void writePersistedAppliedIndexForAllDatabases(final long index, final Set<String> excluded) {
    appliedIndexes.writePersistedAppliedIndexForAllDatabases(index, excluded);
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
        final PeerDialAddress source = resync.resolveSnapshotSource(raftHA.getLeaderId());
        if (source.refused()) {
          LogManager.instance().log(this, Level.WARNING, "Refusing a snapshot resync: %s", source.refusal());
          return;
        }
        resync.downloadAllDatabasesFrom(source, raftHA.getClusterToken());
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
  // @VisibleForTesting
  void triggerDatabaseResync(final String dbName) {
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
            final PeerDialAddress source = resync.resolveSnapshotSource(raftHA.getLeaderId());
            if (source.refused()) {
              LogManager.instance().log(this, Level.WARNING,
                  "Refusing a targeted snapshot resync of quarantined database '%s': %s", dbName, source.refusal());
              return;
            }
            final String leaderHttpAddr = source.httpAddress();
            final String leaderHttpsAddr = source.httpsAddress();
            final String clusterToken = raftHA.getClusterToken();
            // install() keeps the database open during the download and rolls back on failure, so a
            // targeted resync never leaves it closed. A database closed on this node is reinstalled too (issue
            // #8464): its directory is still here and the next request reopens it, so skipping it left the
            // quarantine standing over a copy nothing would ever repair.
            //
            // A database with no copy at all is installed too (issue #8067). It used to be left alone on the
            // argument that there is nothing here to serve stale, but the quarantine is not only about serving: it
            // holds the node out of the ready set and stops the log from being checkpointed, and nothing but this
            // resync lifts it. A quarantined database is one the cluster still has - a DROP entry retires the
            // quarantine with the database (clearDroppedDatabaseQuarantine) - so the missing copy is exactly what
            // the leader's snapshot restores; this is how a leader that lost a database, quarantined it and handed
            // off (applyInstallDatabaseEntry) gets it back. A leader that cannot serve it fails the install below,
            // which leaves the quarantine standing for the next tick, the same as for a present copy.
            final boolean presentLocally = isDatabasePresentLocally(dbName);
            try {
              replacement.installLeaderCopy(dbName, leaderHttpAddr, leaderHttpsAddr, clusterToken);
            } catch (final LeaderDoesNotHoldDatabaseException e) {
              // A quarantined copy the leader does not hold would otherwise be retried against the same 404 on every
              // health tick, holding the node out of the ready set for good - which is also how a quarantine raised
              // before this fix, and restored from disk, gets lifted. Closed (issue #8559) or registered (issue
              // #8588) alike: the leader-driven auto-acquire install lifts the quarantine of a registered one too.
              // With no copy on this node (issue #8067) there is nothing to mark as an unverified closed copy.
              try {
                resync.reportDatabaseTheLeaderDoesNotHold(dbName, presentLocally && !server.existsDatabase(dbName), e);
              } catch (final IOException markFailure) {
                // The quarantine stays standing: without the mark it is what keeps the closed copy from being
                // reopened (issue #8589).
                LogManager.instance().log(this, Level.SEVERE,
                    "Targeted snapshot resync could not mark database '%s' as an unverified closed copy: keeping it "
                        + "quarantined instead (issue #8589)", markFailure, dbName);
              }
              return;
            }
            LogManager.instance().log(this, Level.INFO,
                "Targeted snapshot resync of quarantined database '%s' completed", dbName);
            reconciler.clearLeaderMissing(dbName);
            quarantine.clearDivergedDatabase(dbName);
            baselines.clearBootstrapUnreconciled(dbName);
            bootstrapInstalls.settleBootstrapReplacement(dbName);
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

  // DatabaseQuarantine holds the implementation of these; they stay here for the callers outside this class.

  void clearDivergedDatabase(final String dbName) {
    quarantine.clearDivergedDatabase(dbName);
  }

  DivergedAcceptance acceptDivergedDatabase(final String dbName, final String acceptedBy) throws IOException {
    return quarantine.acceptDivergedDatabase(dbName, acceptedBy);
  }

  boolean shouldLogDivergedResync(final String dbName) {
    return quarantine.shouldLogDivergedResync(dbName);
  }

  boolean quarantineUnpublishedSchemaChange(final String dbName, final String session, final Throwable refusal) {
    return quarantine.quarantineUnpublishedSchemaChange(dbName, session, refusal);
  }

  void markStateDiverged(final String dbName) {
    quarantine.markStateDiverged(dbName);
  }

  void markStateDiverged(final String dbName, final DivergenceCause cause) {
    quarantine.markStateDiverged(dbName, cause);
  }

  boolean quarantineDatabase(final String dbName, final DivergenceCause cause) {
    return quarantine.quarantineDatabase(dbName, cause);
  }

  List<String> quarantineDatabasesWithIncompleteRecovery() {
    return quarantine.quarantineDatabasesWithIncompleteRecovery();
  }

  boolean recheckOpenedDatabase(final String dbName, final DatabaseInternal db) {
    return quarantine.recheckOpenedDatabase(dbName, db);
  }

  boolean quarantineAfterIncompleteRecovery(final String dbName) {
    return quarantine.quarantineAfterIncompleteRecovery(dbName);
  }

  boolean quarantineAfterUnavailableUniqueIndex(final String dbName, final List<String> indexNames) {
    return quarantine.quarantineAfterUnavailableUniqueIndex(dbName, indexNames);
  }

  boolean quarantineAfterEmptiedDictionary(final String dbName) {
    return quarantine.quarantineAfterEmptiedDictionary(dbName);
  }

  void clearDivergedState() {
    quarantine.clearDivergedState();
  }

  DivergenceCause quarantineCause(final String dbName) {
    return quarantine.quarantineCause(dbName);
  }

  Set<String> getQuarantinedDatabaseNames() {
    return quarantine.getQuarantinedDatabaseNames();
  }

  int divergedSwallowedErrorCount() {
    return quarantine.divergedSwallowedErrorCount();
  }

  /**
   * What {@link #acceptDivergedDatabase} lifted: the quarantine's cause ({@code null} when only a read floor stood), the
   * read floor ({@code -1} when none stood) and the database's last applied index ({@code -1} when none is recorded).
   */
  record DivergedAcceptance(DivergenceCause cause, long readFloor, long appliedIndex) {
  }

  /**
   * What {@link #acceptStaleSnapshotFloor} lifted: the node-wide read floor that stood, the snapshot marker index it was
   * short of ({@code -1} when no marker is on disk any more) and the applied position now recorded for the node.
   */
  record StaleSnapshotAcceptance(long readFloor, long snapshotIndex, long acceptedIndex) {
  }

  /**
   * Quarantines {@code dbName} on this node because a schema session changed it locally and the entry that was to
   * publish the change was definitely refused (issue #9555), and starts the targeted resync that replaces the local copy
   * with the committed one.
   * <p>
   * A DDL ({@code recordFileChanges}) or a compaction ({@code runWithCompactionReplication}) runs on the proposer FIRST
   * and ships afterwards: the file ids are allocated, the files written and the schema or index layout swapped before
   * the entry goes out. When the leader refuses that entry before appending it - a stale session (#9547), a group
   * commit that never dispatched it - no other node will ever hold the change, while this one does. Nothing else
   * marked that: the node was caught only when a later committed entry reused one of the file ids (the #6063 collision
   * check) or a replicated page write found a file in another role. Quarantining here does the same thing those
   * checks end up doing, without waiting for one of them to trip.
   * <p>
   * Only for a DEFINITE refusal. An indeterminate one ({@link ReplicationDispatchedTimeoutException}) may still commit,
   * and then the local change is exactly what the log says; the caller must not get here for it.
   * <p>
   * Since issue #9558 also for a change that could not be published at all - an entry too large to replicate, a session
   * whose callback failed after its buffered commits were applied here. Such a failure is usually DETERMINISTIC: the
   * same compaction on the next schedule fails the same way. The quarantine is still right, because the divergence is
   * just as real, but what keeps it from looping is {@link #deferCompactionAfterUnpublishableChange}, which the caller
   * records beside it.
   * <p>
   * The same three steps as {@link EntryApplyRetry#handleUnexpectedApplyError}: quarantine, targeted resync, and - when this node is
   * the leader, which a resync cannot use as its own source - a leadership hand-off. And the same exception: a node that
   * is the only voter has no peer to resync from, so the quarantine could never be lifted; it is reported instead.
   *
   * @return {@code true} when this call quarantined the database
   */
  /**
   * A schema entry its proposer published from this node and whose outcome it may have to wait for (issue #11240): a
   * publication that timed out is either committed or truncated by a leadership change, and the entries this node applies
   * tell which, because they are applied in log order. The proposer's entry is bound to the term it was proposed in (issue
   * #9547), so it is committed when this node applies it, and truncated when this node applies an entry of a later term
   * first: every entry ahead of that one in the log is settled, and the proposal is not among them.
   */
  static final class PendingPublication {
    final         String                     databaseName;
    final         long                       term;
    private final CompletableFuture<Boolean> outcome = new CompletableFuture<>();
    private volatile String                  schemaJson;
    // The log index this node applied the entry at, once it did: the checkpoint stays below it until the proposer has
    // persisted what the entry publishes and forgets the publication (see takeSnapshot)
    private volatile long                    appliedAtIndex = -1L;

    private PendingPublication(final String databaseName, final long term) {
      this.databaseName = databaseName;
      this.term = term;
    }

    /** The schema document the publishing entry carries, which tells it apart from another entry of the same term. */
    void identify(final String schemaJson) {
      this.schemaJson = schemaJson;
    }

    /**
     * @return {@code TRUE} once the entry is committed and applied here, {@code FALSE} once it is truncated, {@code null}
     * when neither happened within {@code timeoutMs}
     */
    Boolean awaitOutcome(final long timeoutMs) throws InterruptedException {
      try {
        return outcome.get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (final TimeoutException e) {
        return null;
      } catch (final ExecutionException e) {
        // never completed exceptionally
        return null;
      }
    }

    private boolean matches(final RaftLogEntryCodec.DecodedEntry decoded, final long entryTerm) {
      if (entryTerm != term || decoded.moreChunksFollow() || !databaseName.equals(decoded.databaseName()))
        return false;
      final String expected = schemaJson;
      return expected == null || decoded.schemaJson() == null || expected.equals(decoded.schemaJson());
    }
  }

  /**
   * Registers a publication of {@code databaseName} its proposer may wait on, BEFORE the entry is dispatched, so an apply
   * that races the proposer's timeout is not missed (issue #11240). Forgotten with {@link #forgetPublication}.
   */
  PendingPublication expectPublication(final String databaseName, final long term) {
    final PendingPublication pending = new PendingPublication(databaseName, term);
    pendingPublications.add(pending);
    return pending;
  }

  void forgetPublication(final PendingPublication pending) {
    pendingPublications.remove(pending);
  }

  /** Settles as truncated every pending publication of a term before {@code entryTerm} (issue #11240). */
  private void settlePublicationsBefore(final long entryTerm) {
    if (pendingPublications.isEmpty())
      return;
    for (final PendingPublication pending : pendingPublications)
      if (pending.term < entryTerm && pending.outcome.complete(Boolean.FALSE))
        LogManager.instance().log(this, Level.WARNING,
            "The schema entry of database '%s' proposed in term %d was truncated: this node applies an entry of term %d first "
                + "(issue #11240)", pending.databaseName, pending.term, entryTerm);
  }

  /**
   * Settles as committed the pending publication the applied {@code decoded} entry is (issue #11240). Called before the
   * applied position moves past {@code index}, so a checkpoint that reads the advanced position also reads the floor
   * {@link #lowestUnpersistedPublication()} derives from it.
   */
  private void confirmPublication(final RaftLogEntryCodec.DecodedEntry decoded, final long entryTerm, final long index) {
    for (final PendingPublication pending : pendingPublications)
      if (pending.matches(decoded, entryTerm)) {
        pending.appliedAtIndex = index;
        pending.outcome.complete(Boolean.TRUE);
      }
  }

  /**
   * The lowest log index of an applied publication whose proposer has not persisted it yet, or {@link Long#MAX_VALUE}
   * (issue #11240). An HA compaction saves its schema and drops the files it replaced only after its entry is applied here,
   * and on the proposer the apply skips the entry: until the proposer has persisted the swap, only a replay of the entry
   * could restore it after a crash, so the log must not be purged through it. This covers the short window while the
   * proposer persists; one whose persistence failed hands the entry to the holds of {@link #lowestHeldEntry()} instead
   * ({@link #holdUnpersistedPublication}). A Raft install past the entry carries what it published, so it no longer holds.
   */
  long lowestUnpersistedPublication() {
    if (pendingPublications.isEmpty())
      return Long.MAX_VALUE;
    final long boundary = installedRaftBoundary.get();
    long lowest = Long.MAX_VALUE;
    for (final PendingPublication pending : pendingPublications) {
      final long index = pending.appliedAtIndex;
      if (index > boundary && index < lowest)
        lowest = index;
    }
    return lowest;
  }

  /**
   * Keeps the entry of a publication its proposer could not persist replayable until a restart replays it, a Raft install
   * covers it or the database is dropped (issue #11240): one of the holds of {@link #lowestHeldEntry()}, which drops it past
   * an install and reminds the operator while it stands. Raised before the publication is forgotten, so the entry is never
   * left uncovered in between.
   */
  void holdUnpersistedPublication(final PendingPublication pending) {
    final long index = pending.appliedAtIndex;
    if (index > -1) {
      final String key = unpersistedPublicationKey(pending.databaseName);
      final long lowest = applyRetry.heldEntryFloors.merge(key, index, Math::min);
      LogManager.instance().log(this, lowest == index ? Level.SEVERE : Level.FINE,
          "Raft entry at index %d of database '%s' publishes a compaction this node could not persist: no snapshot checkpoint "
              + "will cover it, so a restart replays it (issue #11240)", index, pending.databaseName);
    }
    forgetPublication(pending);
  }

  static String unpersistedPublicationKey(final String databaseName) {
    return "compaction:" + databaseName;
  }

  /**
   * Holds off the compactions of {@code dbName} on this node after one of them changed the database locally and then
   * failed DETERMINISTICALLY on its way to the log (issue #9558) - an entry too large to replicate, say - and returns how
   * long for.
   * <p>
   * Why this is needed beside the quarantine: the resync the quarantine starts brings back the very state the
   * compaction started from, so the next schedule would run the same compaction, fail the same way and quarantine
   * again - a resync loop on a node that keeps leading, and a leadership ping-pong when it hands off and leadership
   * comes back. Only a failure that provably recurs is held off ({@code RaftReplicatedDatabase.isDeterministicPublishFailure}):
   * a retryable refusal (issue #9555), an interrupt or an I/O error is quarantined all the same, but not held off.
   * <p>
   * The window doubles with every failure in a row, from {@link #COMPACTION_BACK_OFF_BASE_MS} up to
   * {@link #COMPACTION_BACK_OFF_MAX_MS}, and is forgotten by {@link #clearCompactionHoldOff} the first time a compaction of
   * the database publishes. Per node and in memory, the way {@code leaderHandOffsMovedWhileGapPersisted} is: it bounds,
   * not stops, a cluster where every node shares the cause - each of them fails at most once per window - and a restart,
   * which is how a raised {@code arcadedb.ha.appendBufferSize} takes effect, starts afresh.
   */
  long deferCompactionAfterUnpublishableChange(final String dbName) {
    if (dbName == null)
      return 0L;
    final long now = compactionBackOffClock.getAsLong();
    final CompactionBackOff next = compactionBackOffs.compute(dbName, (name, previous) -> {
      final int failures = previous == null ? 1 : Math.min(previous.failures() + 1, COMPACTION_BACK_OFF_MAX_DOUBLINGS + 1);
      final long window = Math.min(COMPACTION_BACK_OFF_MAX_MS, COMPACTION_BACK_OFF_BASE_MS << (failures - 1));
      return new CompactionBackOff(now + window, failures);
    });
    final long window = next.untilMs() - now;
    LogManager.instance().log(this, Level.SEVERE,
        "Compactions of database '%s' are held off on this node for %d s (failure %d in a row): the last one could not "
            + "be published for a reason that does not go away by itself, and running it again would only repeat the "
            + "quarantine and resync it caused. Fix the cause the previous message names - typically raise "
            + "arcadedb.ha.appendBufferSize - and restart this node to retry at once (issue #9558)",
        dbName, window / 1000, next.failures());
    return window;
  }

  /** Whether the compactions of {@code dbName} are held off on this node now, see {@link #deferCompactionAfterUnpublishableChange}. */
  boolean isCompactionHeldOff(final String dbName) {
    if (dbName == null || compactionBackOffs.isEmpty())
      return false;
    final CompactionBackOff backOff = compactionBackOffs.get(dbName);
    return backOff != null && compactionBackOffClock.getAsLong() < backOff.untilMs();
  }

  /**
   * Forgets the compaction hold-off of {@code dbName}: a compaction of it was published, so the failures in a row are
   * over, or it was dropped, so a database recreated under the name starts clean. Allocation-free when there is none.
   */
  void clearCompactionHoldOff(final String dbName) {
    if (dbName != null && !compactionBackOffs.isEmpty())
      compactionBackOffs.remove(dbName);
  }

  /** The refusal's class and message, so an operator can tell a stale-session refusal from a lost quorum. */
  static String describe(final Throwable refusal) {
    return refusal == null ? "no detail" : refusal.getClass().getSimpleName() + ": " + refusal.getMessage();
  }

  // @VisibleForTesting
  boolean isDatabaseDiverged(final String dbName) {
    // A quarantine restored from disk must be visible to the very first caller after a restart (issue #7735).
    // Latched after the first read, so this costs a plain field test on the apply path.
    appliedIndexes.ensureAppliedIndexLoaded();
    return divergedDatabases.containsKey(dbName);
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
    appliedIndexes.ensureAppliedIndexLoaded();
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
    synchronized (appliedIndexes.appliedIndexFileLock) {
      closed = true;
    }
    // Every executor is interrupted before any is waited for, so their tasks unwind in parallel within the one bound.
    // The helpers are read once: a test may swap one while the state machine runs, and the instance interrupted here
    // must be the one waited for below.
    final MembershipSecuritySeeder seeder = membershipSecuritySeeder;
    final DeferredDatabaseDeleter deleter = dropEntries.deferredDatabaseDeleter;
    lifecycleExecutor.shutdownNow();
    snapshotInstallExecutor.shutdownNow();
    seeder.close();
    securityCatchUp.close();
    deleter.close();
    // shutdownNow() drops the queued tasks and interrupts the running one, and does NOT wait for it (issue #8182).
    // A snapshot install past its last interruption point went on creating <db>/.snapshot-new and the pending
    // marker after close() had returned, i.e. after the caller believed the database directory was quiet - JUnit's
    // @TempDir teardown in the unit tests, and a restartRatis() about to start a new state machine installing into
    // the same directory in production. The helpers' single workers are waited for too (issue #8364): the deleter
    // goes on removing entries under the database directory, the seeder on submitting security documents
    // cluster-wide and the catch-up on applying them locally. One deadline for all five, so the bound is the whole
    // wait.
    //
    // shutdownNow() also interrupts the caller when the caller is one of those workers (a task closing its own state
    // machine). That interrupt is taken off for the waits and put back only after the LAST one: left on, or restored
    // any earlier, it would short-circuit every wait still to come, whose tasks run on other threads and are exactly
    // what this barrier is for (reviews of PR #8366 and issue #8364). The wait for the caller's OWN executor is
    // skipped instead - it cannot terminate while its worker is here waiting for it.
    final boolean callerInterrupted = Thread.interrupted();
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLOSE_AWAIT_MS);
    // Every wait runs whatever the ones before it met: an interrupt landing during one must not skip the next, or the
    // task on that executor is left running exactly as before (review of PR #8366). The deadline still bounds the
    // whole thing, and the interrupt is restored once all of them are done.
    boolean waitInterrupted = !ExecutorTermination.await(this, lifecycleExecutor, lifecycleWorker,
        LIFECYCLE_THREAD_NAME, deadline, DATABASE_DIRECTORY_WRITES);
    waitInterrupted |= !ExecutorTermination.await(this, snapshotInstallExecutor, snapshotInstallWorker,
        SNAPSHOT_INSTALL_THREAD_NAME, deadline, DATABASE_DIRECTORY_WRITES);
    waitInterrupted |= !seeder.awaitTermination(deadline);
    waitInterrupted |= !securityCatchUp.awaitTermination(deadline);
    waitInterrupted |= !deleter.awaitTermination(deadline);
    if (callerInterrupted || waitInterrupted)
      Thread.currentThread().interrupt();
    super.close();
  }
}
