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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DataEncryption;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.EmbeddedModifier;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.LocalTransactionExplicitLock;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordCallback;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordFactory;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.UnreferencedFiles;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.opencypher.optimizer.statistics.GraphStatisticsCache;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.select.Select;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * A {@link DatabaseInternal} wrapper that intercepts commit() to submit WAL changes through Raft consensus.
 * On the leader, the transaction is committed locally after Raft consensus is reached.
 * On replicas, the transaction is forwarded to the leader via the Raft client.
 */
public class RaftReplicatedDatabase implements DatabaseInternal, HAReplicatedDatabase {
  /**
   * Carries transaction state between Phase 1 (WAL capture under lock) and
   * Replication (without lock) and Phase 2 (local apply under lock).
   * Package-private so unit tests can build one for {@link #replicateAndCommitLocally}.
   */
  record ReplicationPayload(
      TransactionContext tx,
      TransactionContext.TransactionPhase1 phase1,
      byte[] walData,
      Map<Integer, Integer> bucketDeltas
  ) {
  }

  // Thread-local buffers used to accumulate WAL data when commit() is called inside
  // a recordFileChanges() callback. The buffered entries are then embedded in the
  // SCHEMA_ENTRY so replicas receive them atomically with the file-creation step.
  private static final ThreadLocal<List<byte[]>>                schemaWalBuffer         = ThreadLocal.withInitial(ArrayList::new);
  private static final ThreadLocal<List<Map<Integer, Integer>>> schemaBucketDeltaBuffer = ThreadLocal.withInitial(ArrayList::new);
  // Set to TRUE only on the thread executing a recordFileChanges() DDL callback, so that
  // commit() knows to buffer WAL to schemaWalBuffer rather than replicate via Raft.
  // This MUST be thread-local: using the shared getRecordedChanges() != null check would
  // cause concurrent user-transaction threads to buffer their WAL and lose it, producing
  // WALVersionGapException on followers (version gap from unreplicated writes).
  private static final ThreadLocal<Boolean>                     isSchemaCommitThread    = new ThreadLocal<>();
  // Accumulates TimeSeries sealed-store blobs recorded during a runWithCompactionReplication session
  // (issue #4382). Drained into the SCHEMA_ENTRY shipped at the end of the session so followers
  // install the rewritten sealed files atomically with the buffered mutable-bucket clear WAL.
  private static final ThreadLocal<List<RaftLogEntryCodec.TsSealedBlob>> compactionSealedBuffer =
      ThreadLocal.withInitial(ArrayList::new);
  /**
   * Non-null only while a {@code recordFileChanges} session on THIS thread is shipping its buffered WAL in
   * instalments (issue #6136). Thread-local for the same reason the buffers are, and null everywhere else - notably
   * in {@code runWithCompactionReplication}, which buffers too but produces a bounded amount of it and chunks its
   * synthetic WAL as it serializes.
   */
  private static final ThreadLocal<SchemaInstalmentState>                schemaInstalments      = new ThreadLocal<>();
  /**
   * Schema-WAL instalments this DATABASE has shipped since it was opened, and what they cost (issues #6136, #6144).
   * <p>
   * PER DATABASE, not per JVM, which is the whole reason these are instance fields: a multi-database server would
   * otherwise report one number for every database on it, and "which database stalls its writers while it ships"
   * is precisely the question being asked. Exported as Micrometer gauges tagged {@code database=<name>} through
   * {@code RaftHAServer.getSchemaInstalmentSamples()}.
   * <p>
   * DURATION is the number that matters more than the count. Each instalment is a quorum round trip taken while the
   * database write lock is held (see {@link #flushSchemaWalBufferIfFull}), so it is what a stalled writer is waiting
   * on and what eats into the {@code arcadedb.ha.quorumTimeout} budget; a count alone cannot tell 200 fast
   * instalments from 3 that each waited on a slow quorum member, which is why the max is kept alongside the total.
   * <p>
   * Zero on a node that never crossed the threshold, which is every node until an index is rebuilt or a DDL callback
   * writes more than half a Raft entry's worth of pages. Monotonic for the life of the open database and reset by a
   * close/reopen, like every other counter on this instance.
   */
  private final        AtomicLong                                        schemaInstalmentsShipped = new AtomicLong();
  private final        AtomicLong                                        schemaInstalmentTotalMs  = new AtomicLong();
  private final        AtomicLong                                        schemaInstalmentMaxMs    = new AtomicLong();

  /**
   * Sealed-store slices this database has shipped since it opened (issue #4416). Zero on every node whose sealed
   * stores fit one Raft entry, which is what makes it worth reading: a non-zero value says compaction is now
   * shipping a store that would previously have been refused, and how much Raft traffic that costs per cycle.
   */
  private final        AtomicLong                                        sealedChunksShipped      = new AtomicLong();
  private final        AtomicLong                                        sealedChunksMaxSequence  = new AtomicLong();

  /**
   * The schema document this node last SHIPPED to the followers, and the length of the last WHOLE document it
   * shipped (issue #6989). Together they are what lets the next schema change go out as a delta:
   * {@link SchemaDelta#compute} diffs against the former, and the latter says whether the result is small
   * enough to be worth shipping instead of the document.
   * <p>
   * Written only after {@code replicateSchema} has returned - an entry that never reached the log did not move
   * the followers - and only from inside a recording session, which is exclusive per database. Volatile for
   * visibility across the threads that take that session in turn, not for atomicity between the two.
   * <p>
   * <b>Staleness is caught by the RAFT TERM the cache was filled in.</b> Only the leader ships schema entries,
   * and a new leader always runs in a strictly higher term, so within one term this node is the only node that
   * can have moved the followers - and across a term boundary the cache says nothing about what the other leader
   * shipped. {@link #schemaDeltaFor} therefore compares the cached term against the current one and falls back
   * to the whole document when they differ, which covers leadership loss, a re-election, and a step-down that
   * this node won again.
   * <p>
   * The schema VERSION cannot do that job: {@code saveConfiguration()} defers while a transaction is active, so
   * the version a DDL run inside {@code db.transaction(...)} produces lands AFTER the entry has shipped, and a
   * version-based guard would refuse every delta. It is carried in the payload for diagnostics only.
   * <p>
   * <b>Cost:</b> one parsed schema document retained per replicated database, which for the multi-MB schema this
   * issue is about is tens of MB of heap. That is why it is held only while a delta could actually ship - the
   * setting on AND every peer advertising the capability, issue #7219; see {@link #rememberReplicatedSchema} -
   * and why the base is kept parsed rather than as text: a diff
   * against text would have to re-parse it on every DDL, which is one of the very costs the delta exists to
   * avoid.
   */
  private volatile     JSONObject                                        lastReplicatedSchema     = null;
  private volatile     long                                              lastReplicatedSchemaTerm = -1L;
  private volatile     int                                               lastFullSchemaLength     = 0;

  /**
   * How many schema changes this database has shipped as a DELTA, and how many as a whole document (issue
   * #6989). Read together they are the operator's answer to "is the delta path actually engaged?", which a
   * boolean setting alone cannot give: the leader falls back to the document on its own whenever the cached
   * base is not what the followers hold, so a cluster with {@code arcadedb.ha.schemaDelta} on can still be
   * shipping documents - and a ratio that never moves is the symptom to chase.
   */
  private final        AtomicLong                                        schemaDeltasShipped      = new AtomicLong();
  private final        AtomicLong                                        schemaDocumentsShipped   = new AtomicLong();

  /** Throttle window for the "deltas are on but withheld" report (issue #7219), per database. */
  private static final long                                              SCHEMA_DELTA_WITHHELD_LOG_THROTTLE_MS = 5 * 60_000L;

  /**
   * When {@link #logSchemaDeltaWithheld} last reported that a peer is holding deltas back. Plain volatile rather
   * than an atomic: a duplicate line costs nothing and the schema-replication path should not take a lock for a
   * diagnostic.
   */
  private volatile     long                                              lastSchemaDeltaWithheldLog = 0L;

  /**
   * Memoized unreferenced-file count behind the (file modification count, schema version) gate (issue #6168).
   * <p>
   * Lives HERE, on the database instance, rather than in a map on the server: the gauge that reads it is refreshed
   * per open replicated database, so a per-instance holder needs no keying and no eviction - it is collected with
   * the database it describes. See {@link UnreferencedFiles.MemoizedCount} for why the cached value cannot go stale.
   */
  private final        UnreferencedFiles.MemoizedCount                   unreferencedFiles        = new UnreferencedFiles.MemoizedCount();

  /**
   * Bookkeeping for a {@code recordFileChanges} session that ships its WAL incrementally (issue #6136).
   * <p>
   * An index rebuild inside such a session commits once per {@code IndexBuilder.BUILD_BATCH_SIZE} records, and
   * every one of those commits buffered a full WAL image that was only drained when the callback returned: peak
   * leader heap was the whole rebuilt index, plus a repeat of every page touched by more than one batch. This
   * accumulates the same thing but flushes it as an ordered prefix once it crosses a threshold, so heap is bounded
   * by the threshold instead of by the size of the index.
   */
  private static final class SchemaInstalmentState {
    /**
     * Raw buffered bytes at which the next instalment goes out: {@code RaftTransactionBroker.walChunkBudget()},
     * shared with the compaction path so the two cannot drift apart. Zero until the session's FIRST BUFFERED
     * commit resolves it.
     * <p>
     * Resolved once, and lazily, for two different reasons. Once, because it cannot change under a session and the
     * alternative puts a {@code requireRaftServer().getTransactionBroker()} hop on every batch commit of an index
     * build. Lazily, because {@code requireRaftServer()} throws a retryable {@code NeedRetryException} while the
     * Raft server is transiently absent (restart, failover), and resolving it when the session OPENS would fail a
     * {@code recordFileChanges} that buffers nothing and would have completed as a harmless no-op. A session that
     * does buffer needs the server to replicate anyway, so nothing is lost by asking then.
     */
    private long                       threshold;
    /** Raw bytes of WAL currently sitting in {@link #schemaWalBuffer}. */
    private long                       bufferedBytes;
    /**
     * Files an instalment told the followers to create - or MAY have, see the increment site - so the final entry
     * neither re-ships them nor forgets to retire one that the session went on to drop:
     * {@code FileManager.dropFile} CANCELS the recorded create when both happen inside one session, which would
     * otherwise leave the file on the followers only.
     */
    private final Map<Integer, String> shippedFiles = new LinkedHashMap<>();
    /**
     * How many instalments were STARTED; the final entry is unconditional once this is non-zero. Counted before
     * the entry is submitted rather than after, for the reason given at the increment: an instalment over the cap
     * is several entries, and a failure part-way through one still leaves the followers holding what its first
     * chunk created.
     */
    private int                        instalments;
    /**
     * Milliseconds this session spent inside {@code replicateSchemaInstalment}, for the end-of-session summary
     * (issue #6144). Session-scoped rather than read off the database counters, which are cumulative and would
     * describe every session since the database opened.
     */
    private long                       elapsedMs;
  }

  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  /**
   * Registry of leader-side exception class names to a factory that rebuilds the same type from its
   * message, used by {@link #reconstructLeaderException} on forwarded-command errors. Reconstructing
   * the exact type (rather than collapsing to a common supertype) preserves retry semantics for
   * callers: a {@link com.arcadedb.exception.ConcurrentModificationException} or
   * {@link LockTimeoutException} stays a {@link NeedRetryException} subtype and therefore retryable,
   * while a non-retryable {@link TimeoutException} stays distinct instead of being mistaken for a
   * retryable one. It also lets callers catch the specific type directly.
   * <p>
   * Only {@link ArcadeDBException} subtypes with a single {@code (String)} constructor belong here:
   * non-{@code ArcadeDBException} types (e.g. {@code SecurityException}) would not be caught by the
   * {@code catch (ArcadeDBException)} on the forwarding path and would be re-wrapped anyway, and
   * types that need structured arguments (e.g. {@link DuplicatedKeyException}) are reconstructed
   * explicitly in {@link #reconstructLeaderException}. New entries are safe to add as one line each;
   * extending this map is preferred over reflectively instantiating an arbitrary class name from the
   * response. {@link Map#ofEntries} is used (rather than {@link Map#of}) because the latter caps at
   * ten pairs.
   * <p>
   * ArcadeDB's {@code ConcurrentModificationException} is referenced by its fully qualified name, and
   * {@link java.util.ConcurrentModificationException} is intentionally NOT imported, so that a bare
   * {@code ConcurrentModificationException} anywhere in this file cannot silently bind to the JDK type
   * (which the engine never throws) - the root cause of issue #5018.
   */
  private static final Map<String, Function<String, RuntimeException>> LEADER_EXCEPTION_FACTORIES = Map.ofEntries(
      Map.entry(NeedRetryException.class.getName(), NeedRetryException::new),
      Map.entry(com.arcadedb.exception.ConcurrentModificationException.class.getName(), com.arcadedb.exception.ConcurrentModificationException::new),
      Map.entry(LockTimeoutException.class.getName(), LockTimeoutException::new),
      Map.entry(TimeoutException.class.getName(), TimeoutException::new),
      Map.entry(TransactionException.class.getName(), TransactionException::new),
      // #5064: preserves the 'committed cluster-wide, do NOT retry' contract when a follower forwards a
      // write to the leader - without this entry the 409 body would collapse to a generic
      // TransactionException on the follower and the client would lose the do-not-retry signal.
      Map.entry(TransactionCommittedRemotelyException.class.getName(), TransactionCommittedRemotelyException::new),
      Map.entry(CommandExecutionException.class.getName(), CommandExecutionException::new),
      Map.entry(CommandParsingException.class.getName(), CommandParsingException::new),
      Map.entry(CommandSQLParsingException.class.getName(), CommandSQLParsingException::new),
      Map.entry(CommandSemanticException.class.getName(), CommandSemanticException::new),
      Map.entry(QueryNotIdempotentException.class.getName(), QueryNotIdempotentException::new),
      Map.entry(ValidationException.class.getName(), ValidationException::new),
      Map.entry(SchemaException.class.getName(), SchemaException::new));

  /** Poll cadence while waiting for a leader to be (re)elected before forwarding a write (issue #4728 follow-up). */
  private static final long LEADER_WAIT_POLL_INTERVAL_MS = 100;

  /**
   * Emits the "no security context, forwarding as root" notice only once per JVM so embedded
   * deployments that legitimately issue writes from background threads don't get log-spammed.
   */
  private final AtomicBoolean forwardAsRootWarned = new AtomicBoolean(false);

  /**
   * Emits the "the leader resolved to this node's own address" notice only once per database, so a
   * misconfigured cluster under load reports the configuration to fix instead of one line per refused
   * write (issue #6191).
   */
  private final AtomicBoolean selfForwardWarned = new AtomicBoolean(false);

  /**
   * Emits the "a peer forwarded a write here and this node is not the leader either" notice only once per
   * database. The refusal itself travels back to the caller; this is so the node that proved the address
   * wrong also says so in its own log (issue #6191).
   */
  private final AtomicBoolean forwardedAgainWarned = new AtomicBoolean(false);

  /**
   * Test-only fault-injection hook. Fires after Raft replication succeeds but BEFORE the committing thread completes
   * the local commit (the pages were already published by the state machine at the entry's log position, #6965).
   * Set to a non-null Consumer to simulate a leader crash in this window. Always null in production.
   */
  static volatile Consumer<String> TEST_POST_REPLICATION_HOOK = null;

  /**
   * Test-only fault-injection hook. Fires on the leader just before the pages of a locally-originated transaction are
   * published - on the Raft apply thread, at the entry's log position, after Raft has already committed the entry.
   * Throw from the consumer to simulate a leader-side phase-2 commit failure while the followers are already ahead
   * (issue #4740). Always null in production.
   */
  static volatile Consumer<String> TEST_PHASE2_COMMIT_FAULT = null;

  public record ReadConsistencyContext(Database.READ_CONSISTENCY consistency, long readAfterIndex) {
  }

  private static final ThreadLocal<ReadConsistencyContext> READ_CONSISTENCY_CONTEXT = new ThreadLocal<>();

  public static ReadConsistencyContext getReadConsistencyContext() {
    return READ_CONSISTENCY_CONTEXT.get();
  }

  /**
   * Static helper for setting the read consistency context from tests or non-instance contexts.
   */
  static void applyReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    READ_CONSISTENCY_CONTEXT.set(new ReadConsistencyContext(consistency, readAfterIndex));
  }

  /**
   * Static helper for clearing the read consistency context from tests or non-instance contexts.
   */
  static void removeReadConsistencyContext() {
    READ_CONSISTENCY_CONTEXT.remove();
  }

  @Override
  public void setReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    READ_CONSISTENCY_CONTEXT.set(new ReadConsistencyContext(consistency, readAfterIndex));
  }

  @Override
  public void clearReadConsistencyContext() {
    READ_CONSISTENCY_CONTEXT.remove();
  }

  @Override
  public long getLastAppliedIndex() {
    return raftHAServer != null ? raftHAServer.getLastAppliedIndex() : -1;
  }

  private final ArcadeDBServer server;
  private final LocalDatabase  proxied;
  private final RaftHAServer   raftHAServer;

  public RaftReplicatedDatabase(final ArcadeDBServer server, final LocalDatabase proxied, final RaftHAServer raftHAServer) {
    this.server = server;
    this.proxied = proxied;
    this.raftHAServer = raftHAServer;
    this.proxied.setWrappedDatabaseInstance(this);
  }

  @Override
  public boolean isReplicated() {
    return true;
  }

  private RaftHAServer requireRaftServer() {
    final RaftHAServer s = raftHAServer;
    if (s == null)
      throw new NeedRetryException("Raft HA server is not available (server may be restarting)");
    return s;
  }

  /**
   * Commits the current transaction through Raft consensus.
   * <p>
   * <b>Two-phase flow with lock release during replication:</b>
   * <ol>
   *   <li><b>Phase 1 (read lock held):</b> Capture WAL bytes and bucket record deltas
   *       via {@code commit1stPhase}. The read lock ensures a consistent snapshot of the
   *       transaction's page changes.</li>
   *   <li><b>Replication (no lock held):</b> Submit the WAL entry to Raft via
   *       {@link RaftTransactionBroker#replicateTransaction} and wait for quorum. Releasing the lock
   *       here allows concurrent transactions to proceed through Phase 1 while this
   *       transaction waits for Raft consensus, significantly improving throughput.</li>
   *   <li><b>Phase 2 (on the Raft apply thread):</b> the state machine publishes the pages when the entry
   *       reaches its position in the log - on the leader from the pages this transaction prepared
   *       ({@code publishCommittedPages}), on replicas from the entry's WAL bytes (issue #6965). Once the entry is
   *       acknowledged, this thread completes the transaction ({@code completeCommit}) on the leader, or waits for
   *       the local apply on a replica.</li>
   * </ol>
   * <p>
   * <b>Phase 2 failure handling:</b> If the local publication fails after Raft has committed the entry,
   * the entry is already in the log and other replicas will apply it. The pages are reconciled from the
   * replicated payload, the leader logs SEVERE and steps down rather than continue with diverged state.
   * <p>
   * <b>Schema WAL buffering:</b> When {@code commit()} is called from inside a
   * {@code recordFileChanges()} callback, the files being created do not yet exist on replicas.
   * Instead of sending a {@code TX_ENTRY} that would fail, the WAL data is buffered in
   * {@link #schemaWalBuffer} and embedded in the {@code SCHEMA_ENTRY} sent after the callback.
   */
  @Override
  public void commit() {
    proxied.incrementStatsWriteTx();

    final boolean leader = isLeader();

    // Wait for any in-flight FileManager recording session before dispatching a TX_ENTRY.
    // The leader's compaction path (runWithCompactionReplication) creates new index files
    // and ships them via SCHEMA_ENTRY only after the compaction callback returns. If a user
    // transaction commits during that window, its TX_ENTRY may reach Raft before the
    // SCHEMA_ENTRY that creates the file on followers, where TransactionManager.applyChanges
    // silently skips pages whose fileId does not exist - dropping the writes and surfacing
    // later as missing index entries (issue #4083). The schema-commit thread itself uses the
    // schemaWalBuffer below and must not wait on its own session.
    if (leader && !Boolean.TRUE.equals(isSchemaCommitThread.get()))
      waitForActiveRecordingSession();

    // When commit() is called from inside a recordFileChanges() DDL callback on the leader,
    // the files being created do not yet exist on replicas. Sending a TX_ENTRY now would
    // fail on replicas because the target files are missing. Instead, buffer the WAL data
    // here and embed it in the SCHEMA_ENTRY that recordFileChanges() sends after the callback.
    // NOTE: we check the thread-local flag (not the shared getRecordedChanges() != null) so that
    // concurrent user transactions on OTHER threads are never affected by an active recording
    // session (e.g. compaction) and continue to replicate normally via TX_ENTRY.
    if (leader && Boolean.TRUE.equals(isSchemaCommitThread.get())) {
      proxied.executeInReadLock(() -> {
        proxied.checkTransactionIsActive(false);
        final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
        final TransactionContext tx = current.getLastTransaction();
        try {
          final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
          if (phase1 != null) {
            tx.commit2ndPhase(phase1);
            final byte[] wal = phase1.result.toByteArray();
            schemaWalBuffer.get().add(wal);
            schemaBucketDeltaBuffer.get().add(new HashMap<>(tx.getBucketRecordDelta()));
            final SchemaInstalmentState state = schemaInstalments.get();
            if (state != null)
              state.bufferedBytes += wal.length;
          } else
            tx.reset();
          if (getSchema().getEmbedded().isDirty())
            getSchema().getEmbedded().saveConfiguration();
        } finally {
          current.popIfNotLastTransaction();
        }
        return null;
      });
      // Ships OUTSIDE the read lock: the instalment is a Raft round trip, and it needs none of what the block
      // above holds. The enclosing recordFileChanges callback still owns the database WRITE lock throughout - it
      // has for the whole build already - so this adds round trips to a window that is fully exclusive anyway,
      // and it is what keeps the buffer bounded rather than growing with the whole rebuilt index (issue #6136).
      flushSchemaWalBufferIfFull();
      return;
    }

    // --- PHASE 1 (read lock): capture WAL bytes and delta ---
    final ReplicationPayload payload = proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();
      try {
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(leader);

        if (phase1 != null) {
          final byte[] walData = phase1.result.toByteArray();
          final Map<Integer, Integer> bucketDeltas = new HashMap<>(tx.getBucketRecordDelta());
          return new ReplicationPayload(tx, phase1, walData, bucketDeltas);
        }

        // Read-only transaction: nothing to replicate.
        tx.reset();
        if (leader && getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
        current.popIfNotLastTransaction();
        return null;
      } catch (final ArcadeDBException e) {
        rollback();
        throw e;
      } catch (final Exception e) {
        rollback();
        throw new TransactionException("Error on commit distributed transaction (phase 1)", e);
      }
    });

    // Read-only transaction: nothing more to do.
    if (payload == null)
      return;

    // The state machine publishes this node's own entry at its position in the Raft log (issue #6965): the prepared
    // transaction is registered with it right before the entry is dispatched, and this thread finishes the commit once
    // the entry is acknowledged. Only the leader has a state machine that applies its own entries this way.
    replicateAndCommitLocally(payload, leader, leader ? stateMachineOrNull() : null);
  }

  /**
   * Replicates the captured payload through Raft and finishes the commit locally.
   * <p>
   * On the leader the pages are NOT published by this thread: the transaction is registered with the state machine
   * before the entry is dispatched, and the Raft apply thread publishes the prepared pages when the entry reaches its
   * position in the log (issue #6965). That gives the leader the same page-write order as every follower. Before,
   * phase 2 ran here after the acknowledgement and raced the apply of the neighbouring entries on the same pages, so
   * a delta validated against a stale page version could be spliced over a committed one on every node. Once the
   * entry is acknowledged this thread only completes the bookkeeping ({@link TransactionContext#completeCommit()}).
   * <p>
   * The registration is a handshake (see {@link LocalCommit}): every failure exit withdraws it, and a withdrawal that
   * fails because the apply thread already claimed the transaction means the entry committed after all - the outcome
   * of the publication is then awaited and the commit completes, whatever the replication call reported.
   * <p>
   * Package-private for direct unit testing: which exit does what with the registration is the load-bearing part,
   * and driving every branch through {@link #commit()} would need the whole phase-1 capture stubbed out.
   */
  // @VisibleForTesting
  void replicateAndCommitLocally(final ReplicationPayload payload, final boolean leader, final ArcadeStateMachine stateMachine) {
    final LocalCommit local = leader && stateMachine != null ?
        stateMachine.registerLocalCommit(new LocalCommit(getName(), localWalTxId(payload), payload.tx(), payload.phase1())) :
        null;

    // --- REPLICATION (no lock held): send WAL to Raft and wait for quorum ---
    long committedLogIndex = -1;
    try {
      final RaftHAServer raft = requireRaftServer();
      committedLogIndex = raft.getTransactionBroker()
          .replicateTransaction(getName(), payload.walData(), payload.bucketDeltas());
    } catch (final MajorityCommittedAllFailedException e) {
      // MAJORITY committed but the ALL-quorum watch failed: the entry is durable cluster-wide and this leader's state
      // machine has applied it (the MAJORITY acknowledgement follows the local apply), so the local commit is completed
      // before the failure is reported, to prevent a permanent divergence of the leader.
      HALog.log(this, HALog.BASIC,
          "ALL quorum watch failed after MAJORITY commit; completing the local commit to prevent leader divergence: db=%s",
          getName());
      concludeAfterMajorityCommit(local, stateMachine, payload);
      throw e;
    } catch (final ReplicationDispatchedTimeoutException e) {
      // INDETERMINATE outcome (issue #4790): the entry was dispatched to Ratis but the quorum wait timed out before we
      // learned its fate. If the apply thread has not claimed the transaction, withdraw it and roll back: should the
      // entry commit regardless, the apply thread applies it from its own WAL bytes, the way a follower does, so the
      // write is never lost on this node. If the apply thread DID claim it, the entry is committed and its pages are
      // being published right now: the outcome is known after all and the commit completes normally (#6848).
      if (local == null || stateMachine.withdrawLocalCommit(local)) {
        rollback();
        throw e;
      }
      LogManager.instance().log(this, Level.WARNING,
          "Replication of tx %d on database '%s' timed out after the entry had already been applied by the state "
              + "machine; completing the commit locally", local.walTxId(), getName());
      concludeLocalCommit(local, payload);
      return;
    } catch (final ArcadeDBException e) {
      // Replication failed outright - including the leader refusing the entry before appending it, because it was
      // validated against a page version the log had already moved past (a retryable ConcurrentModificationException,
      // issue #6965). The entry never reached the log, unless the apply thread proves otherwise by holding a claim.
      if (local == null || stateMachine.withdrawLocalCommit(local)) {
        rollback();
        if (!leader && e instanceof ReplicatedPageConflictException conflict)
          // The page this replica validated against is behind the log: the retry only stands a chance once the entry
          // that moved it on is applied here, and with the apply trailing the leader by a couple of entries a retry
          // that does not wait is refused every time. Cheaper than a refused round trip.
          awaitPageVersion(conflict);
        throw e;
      }
      concludeLocalCommit(local, payload);
      return;
    } catch (final Exception e) {
      if (local == null || stateMachine.withdrawLocalCommit(local)) {
        rollback();
        throw new TransactionException("Error on commit distributed transaction (replication)", e);
      }
      concludeLocalCommit(local, payload);
      return;
    }

    // Test-only fault injection: simulate a leader crash between the acknowledgement and the completion of the commit
    final Consumer<String> postReplicationHook = TEST_POST_REPLICATION_HOOK;
    if (postReplicationHook != null)
      postReplicationHook.accept(getName());

    if (!leader) {
      // #5503: this replica never runs phase 2 - the state machine writes the pages asynchronously - so
      // the local page cache still holds the pre-commit version of every page this transaction touched.
      // reset() below releases the commit locks taken in phase 1, and the next transaction to take them
      // would read that stale version, pass its own version check and ship a delta stamped with the same
      // next version, which the state machine then splices onto this one. Wait for THIS entry's index:
      // the replica's own commit index still trails the leader here, so waiting for it would not cover
      // the entry just written.
      //
      // The field is read directly instead of through requireRaftServer(): the cluster has already
      // committed this transaction, so a server that disappeared under a concurrent shutdown must not
      // turn into a NeedRetryException telling the caller to retry a write that is durably committed.
      final RaftHAServer raft = raftHAServer;
      if (raft != null && committedLogIndex > 0) {
        raft.waitForAppliedIndex(getName(), committedLogIndex);

        // The wait degrades to best-effort on timeout, and releasing the locks below with the pages still
        // behind is exactly the pre-#5503 condition. waitForAppliedIndex does log, but as a READ_YOUR_WRITES
        // consistency warning, which reads like a stale-read risk rather than a page-corruption one - so say
        // plainly here what is being risked and what to look at.
        // getLastAppliedIndex() reports -1 ("unknown") while an in-place restart re-initializes the Ratis
        // division (#5271). That is not a race with another committer, so do not raise the alarm for it -
        // the wait above will simply have run its full deadline, which is inherent to the restart window.
        final long applied = raft.getLastAppliedIndex();
        if (applied >= 0 && applied < committedLogIndex)
          LogManager.instance().log(this, Level.WARNING,
              "Replica commit on database '%s' is releasing its commit locks before entry %d was applied locally "
                  + "(applied=%d). Concurrent transactions on these files can now validate against stale page "
                  + "versions - the condition behind issue #5503. Investigate the state machine apply lag.",
              getName(), committedLogIndex, applied);
      }
      payload.tx().reset();
      final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      ctx.popIfNotLastTransaction();
      return;
    }

    // Ratis acknowledges an entry only after the state machine applied it, so by now the apply thread has claimed the
    // transaction and published its pages. A registration still unclaimed here means no apply thread ran for this
    // entry - a Raft server torn down or stubbed out between dispatch and acknowledgement - and waiting would hang
    // the caller for good: withdraw it and publish on this thread instead, the way every leader commit did before
    // issue #6965. A withdrawal that fails is the normal case (the claim happened) and the outcome is awaited.
    if (local != null && !stateMachine.withdrawLocalCommit(local)) {
      concludeLocalCommit(local, payload);
      return;
    }
    if (local != null)
      LogManager.instance().log(this, Level.WARNING,
          "Entry of tx %d on database '%s' was acknowledged without the state machine applying it; publishing its pages "
              + "on the committing thread", local.walTxId(), getName());

    // No state machine wired (the Raft server is still starting), or none applied the entry: nobody publishes the
    // pages at the log position, so this thread does.
    commitLocallyWithoutStateMachine(payload);
  }

  /** Upper bound on the wait for a refused page to catch up locally: a committed entry is a heartbeat away, not more. */
  private static final long CONFLICT_CATCH_UP_TIMEOUT_MS = 2_000L;

  /** Best effort: waits for the page the leader refused this replica on to reach, locally, the version the cluster is at. */
  private void awaitPageVersion(final ReplicatedPageConflictException conflict) {
    final RaftHAServer raft = raftHAServer;
    if (raft == null || conflict.getClusterVersion() < 0)
      return;
    try {
      final FileManager fileManager = proxied.getFileManager();
      if (!fileManager.existsFile(conflict.getFileId()))
        return;
      final int pageSize = ((PaginatedComponentFile) fileManager.getFile(conflict.getFileId())).getPageSize();
      final PageId pageId = new PageId(proxied, conflict.getFileId(), conflict.getPageNumber());
      final PageManager pageManager = proxied.getPageManager();
      raft.awaitApplied(() -> {
        try {
          return pageManager.getMostRecentVersionOfPage(pageId, pageSize) >= conflict.getClusterVersion();
        } catch (final IOException e) {
          return true;
        }
      }, Math.min(raft.getQuorumTimeout(), CONFLICT_CATCH_UP_TIMEOUT_MS));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Exception ignored) {
      // The refusal is what the caller needs to see; a failed catch-up only costs it another round trip.
    }
  }

  /**
   * Finishes a commit whose entry the apply thread claimed: waits for the publication of its pages and completes the
   * transaction, or surfaces the failure the way issue #5064 requires - committed cluster-wide, do not retry.
   */
  private void concludeLocalCommit(final LocalCommit local, final ReplicationPayload payload) {
    final LocalCommit.Outcome outcome = awaitPublication(local);

    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        // #5064: from here the transaction is durably committed CLUSTER-WIDE - a local failure below releases resources
        // without rolling back user-held record identities (a retry would insert duplicates of records the cluster
        // already committed).
        payload.tx().setRemotelyCommitted(true);

        if (outcome == LocalCommit.Outcome.PUBLISHED) {
          try {
            payload.tx().completeCommit();
            if (getSchema().getEmbedded().isDirty())
              getSchema().getEmbedded().saveConfiguration();
            return null;
          } catch (final Exception e) {
            // The pages are published: only the bookkeeping after them failed (completeCommit fenced and reset the
            // transaction when it was its own step that failed).
            LogManager.instance().log(this, Level.SEVERE, phase2CommitFailureMessage(e), getName(), payload.tx(), e.getMessage());
            recoverLeadershipAfterPhase2Failure(payload.tx().toString());
            throw new TransactionCommittedRemotelyException(
                "Transaction " + payload.tx() + " is committed cluster-wide and its pages are published, but completing it "
                    + "locally failed. Do NOT retry: reload the records and continue", e);
          }
        }

        final Throwable failure = local.failure();
        LogManager.instance().log(this, Level.SEVERE, phase2CommitFailureMessage(failure), getName(), payload.tx(),
            failure.getMessage());
        // Same failure regime commit2ndPhase applies inline: fence past the WAL append, release without touching the
        // record identities the cluster committed.
        payload.tx().concludeFailedPhase2(failure);
        recoverLeadershipAfterPhase2Failure(payload.tx().toString());
        // #5064: the user must be able to distinguish 'retry me' from 'already committed cluster-wide'.
        final String reconcileOutcome = local.reconciled() ? " (local pages reconciled from the replicated payload)"
            : " (local reconciliation ALSO failed - this node steps down and repairs on rejoin)";
        throw new TransactionCommittedRemotelyException(
            "Transaction " + payload.tx() + " is committed cluster-wide but the local apply failed"
                + reconcileOutcome + ". Do NOT retry: reload the records and continue", failure);
      } finally {
        current.popIfNotLastTransaction();
      }
    });
  }

  /**
   * Waits for the apply thread to conclude the publication of a claimed transaction. The wait is not given up on: the
   * transaction's pages and file locks belong to the apply thread until it concludes, and releasing them under it
   * would let the next committer on those files validate against a publication still in flight. The publication is a
   * validation, a WAL append and a page-cache put, so a wait past the quorum timeout is worth a warning, not an abort.
   */
  private LocalCommit.Outcome awaitPublication(final LocalCommit local) {
    final RaftHAServer raft = raftHAServer;
    final long timeout = Math.max(1_000L, raft != null ? raft.getQuorumTimeout() :
        server != null ? server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT) : 10_000L);
    boolean interrupted = false;
    try {
      while (true) {
        try {
          final LocalCommit.Outcome outcome = local.awaitOutcome(timeout);
          if (outcome != LocalCommit.Outcome.PENDING)
            return outcome;
          LogManager.instance().log(this, Level.WARNING,
              "Still waiting for the state machine to publish the pages of tx %d on database '%s' (registered %d ms ago)",
              local.walTxId(), getName(), System.currentTimeMillis() - local.registeredAtMs());
        } catch (final InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted)
        Thread.currentThread().interrupt();
    }
  }

  /**
   * Completes the local commit after a MAJORITY commit whose ALL-quorum watch failed. The caller reports the watch
   * failure itself, so a local failure here is logged and recovered from (reconcile, step down) rather than surfaced.
   */
  private void concludeAfterMajorityCommit(final LocalCommit local, final ArcadeStateMachine stateMachine,
      final ReplicationPayload payload) {
    try {
      // Same rule as the acknowledged path: a registration the apply thread never claimed means no apply ran for the
      // entry here, so this thread publishes rather than waiting for a claim that cannot come.
      if (local != null && !stateMachine.withdrawLocalCommit(local))
        concludeLocalCommit(local, payload);
      else
        commitLocallyWithoutStateMachine(payload);
    } catch (final TransactionCommittedRemotelyException e) {
      LogManager.instance().log(this, Level.SEVERE,
          "Local commit failed during ALL-quorum recovery (db=%s, txId=%s): %s", getName(), payload.tx(), e.getMessage());
    }
  }

  /**
   * Publishes the pages on this thread, the pre-#6965 phase 2, for the one case where no state machine can do it at
   * the log position: the Raft server is not wired yet. A failure after the quorum accepted the entry is reconciled
   * from the replicated payload and surfaced as {@link TransactionCommittedRemotelyException} (issue #5064).
   */
  private void commitLocallyWithoutStateMachine(final ReplicationPayload payload) {
    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        payload.tx().setRemotelyCommitted(true);

        // Test-only fault injection: simulate a phase-2 commit failure while followers are ahead.
        final Consumer<String> phase2Fault = TEST_PHASE2_COMMIT_FAULT;
        if (phase2Fault != null)
          phase2Fault.accept(getName());

        payload.tx().commit2ndPhase(payload.phase1());

        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, phase2CommitFailureMessage(e), getName(), payload.tx(), e.getMessage());
        // NOTE (#5075 review): this catch also fires when commit2ndPhase SUCCEEDED and only the
        // saveConfiguration() after it threw. Reconciling then replays the payload WAL against pages the
        // commit already published - safe by the #4926 replay semantics: an equal-version entry re-applies
        // the same absolute bytes (idempotent), a lower-version one is skipped.
        final boolean reconciled = reconcileLeaderPagesAfterPhase2Failure(payload);
        recoverLeadershipAfterPhase2Failure(payload.tx().toString());
        final String reconcileOutcome = reconciled ? " (local pages reconciled from the replicated payload)"
            : " (local reconciliation ALSO failed - this node steps down and repairs on rejoin)";
        throw new TransactionCommittedRemotelyException(
            "Transaction " + payload.tx() + " is committed cluster-wide but the local apply failed"
                + reconcileOutcome + ". Do NOT retry: reload the records and continue", e);
      } finally {
        current.popIfNotLastTransaction();
      }
      return null;
    });
  }

  /** The local state machine, or {@code null} when the Raft server is not wired yet (e.g. during startup). */
  private ArcadeStateMachine stateMachineOrNull() {
    final RaftHAServer raft = raftHAServer;
    return raft != null ? raft.getStateMachine() : null;
  }

  /**
   * Sentinel for "this payload's WAL transaction id could not be read". The id is the key the committing thread and
   * the Raft apply thread meet on (see {@link LocalCommit}); a registration under an unreadable id could never be
   * claimed, so the commit is still registered but nothing is lost when it is not: the apply thread then applies the
   * entry from its WAL bytes, and the committing thread's withdrawal succeeds.
   * <p>
   * {@code Long.MIN_VALUE} is that number here because a replicated WAL transaction id is either the
   * per-database counter ({@code TransactionManager.getNextTransactionId()}, an {@code AtomicLong}
   * starting at zero) or the literal {@code -1} that flags a compaction/sealed-store entry for
   * {@code forceApply} - the only negative value any writer puts in that header. Nothing negates the
   * counter, so a real id can never collide with this sentinel and be mistaken for an unreadable
   * payload.
   */
  private static final long UNKNOWN_WAL_TX_ID = Long.MIN_VALUE;

  /**
   * Reads the WAL transaction id of a captured payload - the correlation key shared with the Raft
   * apply thread. Best-effort: a payload we cannot parse yields {@link #UNKNOWN_WAL_TX_ID} rather
   * than masking the replication failure that is already on its way to the caller.
   */
  private long localWalTxId(final ReplicationPayload payload) {
    try {
      return ArcadeStateMachine.peekWalTransactionId(payload.walData());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not read the WAL transaction id of a replicated entry (db=%s): %s", getName(), e.getMessage());
      return UNKNOWN_WAL_TX_ID;
    }
  }

  private static final int  STEP_DOWN_MAX_RETRIES    = 3;
  private static final long STEP_DOWN_RETRY_DELAY_MS = 500;

  /**
   * Selects the SEVERE log template for a phase-2 commit failure that happened AFTER Raft had already
   * replicated the entry (followers applied it, the leader did not). A page-version conflict - the
   * engine's {@link com.arcadedb.exception.ConcurrentModificationException}, NOT
   * {@link java.util.ConcurrentModificationException} - gets the more specific wording because it
   * points at a locking bug rather than an arbitrary failure. Both templates take
   * {@code (db, txId, errorMessage)} as their three arguments.
   * <p>
   * The type test is written against the fully qualified engine exception on purpose: before issue
   * #5018 the check bound to the JDK {@code java.util.ConcurrentModificationException} (an unrelated
   * type the engine never throws), so the page-conflict branch was dead and every phase-2 conflict
   * was logged with the generic wording.
   */
  static String phase2CommitFailureMessage(final Throwable e) {
    if (e instanceof com.arcadedb.exception.ConcurrentModificationException)
      return """
          Phase 2 commit failed AFTER successful Raft replication with a page version conflict (db=%s, txId=%s). \
          A page was concurrently modified under file lock - this may indicate a locking bug. \
          Followers have applied this transaction but the leader has not. \
          Stepping down to prevent stale reads. Error: %s""";

    return """
        Phase 2 commit failed AFTER successful Raft replication (db=%s, txId=%s). \
        Followers have applied this transaction but the leader has not. \
        Stepping down to prevent stale reads. Error: %s""";
  }

  /**
   * Attempts to bring the leader's pages into sync with the committed Raft entry after
   * {@code commit2ndPhase} has failed. The WAL bytes in {@code payload} are the same bytes
   * that Raft replicated to the followers; calling {@link com.arcadedb.engine.TransactionManager#applyChanges}
   * with them uses page-version guards so already-applied pages are skipped and un-applied
   * ones are written. After this call the leader's page versions match what the followers
   * applied, so when this node steps down and replays the log as a follower it will not
   * encounter a {@link com.arcadedb.exception.WALVersionGapException} for this entry.
   * <p>
   * Called by the phase-2 failure handlers while they still hold the read lock that the failed
   * {@code commit2ndPhase} ran under. {@code applyChanges} mutates pages under the per-page I/O
   * lock, so running it under the same read lock keeps the page-write coordination identical to
   * the normal phase-2 path and avoids racing a concurrent phase-1 snapshot.
   * <p>
   * Uses {@code ignoreErrors=true}: this is a best-effort reconciliation, so if some page is more
   * than one version behind (the leader was already lagging before this tx) {@code applyChanges}
   * skips that page rather than aborting the whole replay on the first gap. Every page it CAN apply
   * is applied; any page it cannot is left for the normal follower-side WAL-gap path to resync once
   * this node steps down (issue #4740 Fix 2 makes that recoverable instead of fatal).
   */
  private boolean reconcileLeaderPagesAfterPhase2Failure(final ReplicationPayload payload) {
    // #5075 review: this also runs when the post-append failure FENCED the database. applyChanges operates
    // at the FileManager/PageManager level and never passes through checkDatabaseIsOpen (the fence's only
    // choke point besides the pre-append guard), so reconciliation works on a fenced database - it is the
    // same page-level machinery recovery replay uses on reopen.
    try {
      final WALFile.WALTransaction walTx = ArcadeStateMachine.deserializeWalTransaction(payload.walData());
      proxied.getTransactionManager().applyChanges(walTx, payload.bucketDeltas(), true);
      LogManager.instance().log(this, Level.INFO,
          "Phase 2 failure: leader pages reconciled via WAL replay (db=%s, tx=%s)", getName(), payload.tx());
      return true;
    } catch (final Exception reconcileEx) {
      LogManager.instance().log(this, Level.SEVERE,
          "Phase 2 failure: leader page reconciliation also failed (db=%s, tx=%s): %s",
          getName(), payload.tx(), reconcileEx.getMessage());
      return false;
    }
  }

  private void recoverLeadershipAfterPhase2Failure(final String txDescription) {
    if (raftHAServer == null || !raftHAServer.isLeader())
      return;

    for (int attempt = 1; attempt <= STEP_DOWN_MAX_RETRIES; attempt++) {
      try {
        raftHAServer.stepDown();
        LogManager.instance().log(this, Level.WARNING,
            "Step-down succeeded on attempt %d/%d after phase-2 failure (db=%s, tx=%s)",
            attempt, STEP_DOWN_MAX_RETRIES, getName(), txDescription);
        return;
      } catch (final NotTheLeaderRefusalException notLeader) {
        // Leadership moved between the isLeader() check above and the transfer. Retrying cannot help - there is
        // nothing left to step down from - so stop immediately instead of burning the whole retry budget on a
        // condition that is already resolved (issue #7134 follow-up).
        LogManager.instance().log(this, Level.WARNING,
            "Step-down after phase-2 failure is moot: this node is no longer the leader (db=%s, tx=%s): %s",
            getName(), txDescription, notLeader.getMessage());
        return;
      } catch (final Exception stepDownEx) {
        LogManager.instance().log(this, Level.SEVERE,
            "Step-down attempt %d/%d failed after phase-2 failure (db=%s, tx=%s): %s",
            attempt, STEP_DOWN_MAX_RETRIES, getName(), txDescription, stepDownEx.getMessage());
        if (attempt < STEP_DOWN_MAX_RETRIES) {
          try {
            Thread.sleep(STEP_DOWN_RETRY_DELAY_MS);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    final boolean stopServer = server.getConfiguration()
        .getValueAsBoolean(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE);
    if (stopServer) {
      LogManager.instance().log(this, Level.SEVERE,
          """
          CRITICAL: All %d step-down attempts failed (db=%s, tx=%s). \
          Forcing server stop to prevent leader-follower divergence.""",
          STEP_DOWN_MAX_RETRIES, getName(), txDescription);
      final Thread stopThread = new Thread(() -> {
        try {
          server.stop();
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.SEVERE,
              "Server stop also failed (db=%s). Manual intervention required: %s",
              getName(), t.getMessage());
        }
      }, "arcadedb-emergency-stop");
      stopThread.setDaemon(true);
      stopThread.start();
    } else {
      LogManager.instance().log(this, Level.SEVERE,
          """
          CRITICAL: All %d step-down attempts failed (db=%s, tx=%s). \
          HA_STOP_SERVER_ON_REPLICATION_FAILURE=false, server continues in degraded state.""",
          STEP_DOWN_MAX_RETRIES, getName(), txDescription);
    }
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Object... args) {
    if (!isLeader()) {
      final QueryEngine queryEngine = proxied.getQueryEngineManager().getEngine(language, this);
      final QueryEngine.AnalyzedQuery analyzed = queryEngine.analyze(query);
      // Forward write commands to the leader. Executing writes locally on a follower would
      // bypass leader-coordinated mutations of shared state (e.g., the schema dictionary,
      // see issue #4039), where the local page cache lags behind the asynchronous state
      // machine apply and produces inconsistent IDs across the cluster.
      if (queryEngine.isExecutedByTheLeader() || analyzed.isDDL() || !analyzed.isIdempotent())
        return forwardCommandToLeaderViaRaft(language, query, null, args);
      // Read-only command executed locally on this follower: honor the read-consistency header
      // exactly like query() does. /api/v1/command can carry read-only statements (a SELECT), and
      // a LINEARIZABLE/READ_YOUR_WRITES caller must not get a silently weaker guarantee than via
      // /api/v1/query (the original Jepsen stale-read was a SELECT routed through command()).
      applyReadConsistencyForReadOnlyCommand(analyzed);
      return proxied.command(language, query, configuration, args);
    }

    // On the leader, a read-only command must also satisfy LINEARIZABLE (ReadIndex barrier). Only
    // analyze when a non-EVENTUAL read-consistency header is actually present, so the common write
    // path pays no extra parsing.
    applyReadConsistencyForReadOnlyCommandIfRequested(language, query);
    return proxied.command(language, query, configuration, args);
  }

  @Override
  public ResultSet command(final String language, final String query) {
    return command(language, query, server.getConfiguration());
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... args) {
    return command(language, query, server.getConfiguration(), args);
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> args) {
    return command(language, query, server.getConfiguration(), args);
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Map<String, Object> args) {
    if (!isLeader()) {
      final QueryEngine queryEngine = proxied.getQueryEngineManager().getEngine(language, this);
      final QueryEngine.AnalyzedQuery analyzed = queryEngine.analyze(query);
      if (queryEngine.isExecutedByTheLeader() || analyzed.isDDL() || !analyzed.isIdempotent())
        return forwardCommandToLeaderViaRaft(language, query, args, null);
      // Read-only command executed locally on this follower: honor the read-consistency header.
      applyReadConsistencyForReadOnlyCommand(analyzed);
      return proxied.command(language, query, configuration, args);
    }

    applyReadConsistencyForReadOnlyCommandIfRequested(language, query);
    return proxied.command(language, query, configuration, args);
  }

  @Override
  public DatabaseInternal getWrappedDatabaseInstance() {
    return this;
  }

  @Override
  public Map<String, Object> getWrappers() {
    return proxied.getWrappers();
  }

  @Override
  public void setWrapper(final String name, final Object instance) {
    proxied.setWrapper(name, instance);
  }

  // Global variables are NOT replicated through Raft: unlike every other mutating method on this class, the five
  // accessors below (getGlobalVariable, setGlobalVariable, setGlobalVariableIfAbsent, setGlobalVariableIfPresent,
  // getGlobalVariables) all delegate straight to the local node's own database, with no consensus proposal
  // involved at all. A value set on one node of an HA cluster is invisible to every other node, including after a
  // failover, so setGlobalVariableIfAbsent/setGlobalVariableIfPresent's per-node atomicity does NOT make Redis
  // "SET k v NX" (or any other caller) a real cluster-wide distributed lock. A full fix would mean replicating
  // global variables through Raft, which none of these five methods attempt (issue #6560). Each accessor below
  // carries its own Javadoc pointer to the full caveat on DatabaseInternal, since a Javadoc comment attaches only
  // to the single declaration it precedes and would not otherwise show up in generated docs/IDE hover for the
  // other four.

  /**
   * Not replicated in an HA cluster - see the caveat on {@link DatabaseInternal#getGlobalVariable(String)}.
   */
  @Override
  public Object getGlobalVariable(final String name) {
    return proxied.getGlobalVariable(name);
  }

  /**
   * Not replicated in an HA cluster - see the caveat on {@link DatabaseInternal#getGlobalVariable(String)}.
   */
  @Override
  public Object setGlobalVariable(final String name, final Object value) {
    return proxied.setGlobalVariable(name, value);
  }

  /**
   * Not a cluster-wide distributed lock in an HA cluster - see the caveat on
   * {@link DatabaseInternal#setGlobalVariableIfAbsent(String, Object)}.
   */
  @Override
  public Object setGlobalVariableIfAbsent(final String name, final Object value) {
    return proxied.setGlobalVariableIfAbsent(name, value);
  }

  /**
   * Not a cluster-wide distributed lock in an HA cluster - see the caveat on
   * {@link DatabaseInternal#setGlobalVariableIfAbsent(String, Object)}.
   */
  @Override
  public Object setGlobalVariableIfPresent(final String name, final Object value) {
    return proxied.setGlobalVariableIfPresent(name, value);
  }

  /**
   * Not replicated in an HA cluster - see the caveat on {@link DatabaseInternal#getGlobalVariable(String)}. Each
   * entry of the returned map is this node's own local value, not a cluster-wide view.
   */
  @Override
  public Map<String, Object> getGlobalVariables() {
    return proxied.getGlobalVariables();
  }

  @Override
  public void checkPermissionsOnDatabase(final SecurityDatabaseUser.DATABASE_ACCESS access) {
    proxied.checkPermissionsOnDatabase(access);
  }

  @Override
  public void checkPermissionsOnFile(final int fileId, final SecurityDatabaseUser.ACCESS access) {
    proxied.checkPermissionsOnFile(fileId, access);
  }

  @Override
  public void checkPermissionsOnType(final String typeName, final SecurityDatabaseUser.ACCESS access) {
    proxied.checkPermissionsOnType(typeName, access);
  }

  @Override
  public long getResultSetLimit() {
    return proxied.getResultSetLimit();
  }

  @Override
  public long getReadTimeout() {
    return proxied.getReadTimeout();
  }

  @Override
  public Map<String, Object> getStats() {
    return proxied.getStats();
  }

  @Override
  public LocalDatabase getEmbedded() {
    return proxied;
  }

  @Override
  public DatabaseContext.DatabaseContextTL getContext() {
    return proxied.getContext();
  }

  @Override
  public void close() {
    proxied.close();
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("Server proxied database instance cannot be drop");
  }

  @Override
  public void registerCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    proxied.registerCallback(event, callback);
  }

  @Override
  public void unregisterCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    proxied.unregisterCallback(event, callback);
  }

  @Override
  public void executeCallbacks(final CALLBACK_EVENT event) throws IOException {
    proxied.executeCallbacks(event);
  }

  @Override
  public GraphEngine getGraphEngine() {
    return proxied.getGraphEngine();
  }

  @Override
  public TransactionManager getTransactionManager() {
    return proxied.getTransactionManager();
  }

  @Override
  public void createRecord(final MutableDocument record) {
    proxied.createRecord(record);
  }

  @Override
  public void createRecord(final Record record, final String bucketName) {
    proxied.createRecord(record, bucketName);
  }

  @Override
  public void createRecordNoLock(final Record record, final String bucketName, final boolean discardRecordAfter) {
    proxied.createRecordNoLock(record, bucketName, discardRecordAfter);
  }

  @Override
  public RID restoreRecord(final Record record, final LocalBucket bucket, final long position) {
    return proxied.restoreRecord(record, bucket, position);
  }

  @Override
  public void updateRecord(final Record record) {
    proxied.updateRecord(record);
  }

  @Override
  public void updateRecordNoLock(final Record record, final boolean discardRecordAfter) {
    proxied.updateRecordNoLock(record, discardRecordAfter);
  }

  @Override
  public boolean deleteRecordNoLock(final Record record) {
    return proxied.deleteRecordNoLock(record);
  }

  @Override
  public void deleteEdgeSkippingEndpoint(final Edge edge, final RID skipEndpoint) {
    proxied.deleteEdgeSkippingEndpoint(edge, skipEndpoint);
  }

  @Override
  public DocumentIndexer getIndexer() {
    return proxied.getIndexer();
  }

  @Override
  public void kill() {
    proxied.kill();
  }

  @Override
  public WALFileFactory getWALFileFactory() {
    return proxied.getWALFileFactory();
  }

  @Override
  public StatementCache getStatementCache() {
    return proxied.getStatementCache();
  }

  @Override
  public ExecutionPlanCache getExecutionPlanCache() {
    return proxied.getExecutionPlanCache();
  }

  @Override
  public CypherStatementCache getCypherStatementCache() {
    return proxied.getCypherStatementCache();
  }

  @Override
  public CypherPlanCache getCypherPlanCache() {
    return proxied.getCypherPlanCache();
  }

  @Override
  public GraphStatisticsCache getGraphStatisticsCache() {
    return proxied.getGraphStatisticsCache();
  }

  @Override
  public String getName() {
    return proxied.getName();
  }

  @Override
  public ComponentFile.MODE getMode() {
    return proxied.getMode();
  }

  @Override
  public DatabaseAsyncExecutor async() {
    return proxied.async();
  }

  @Override
  public String getDatabasePath() {
    return proxied.getDatabasePath();
  }

  @Override
  public TransactionContext getTransaction() {
    return DatabaseInternal.super.getTransaction();
  }

  @Override
  public long getSize() {
    return proxied.getSize();
  }

  @Override
  public String getCurrentUserName() {
    return proxied.getCurrentUserName();
  }

  @Override
  public Select select() {
    return proxied.select();
  }

  @Override
  public GraphBatch.Builder batch() {
    return proxied.batch();
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return proxied.getConfiguration();
  }

  @Override
  public Record invokeAfterReadEvents(final Record record) {
    return record;
  }

  @Override
  public TransactionContext getTransactionIfExists() {
    return proxied.getTransactionIfExists();
  }

  @Override
  public boolean isTransactionActive() {
    return proxied.isTransactionActive();
  }

  @Override
  public int getNestedTransactions() {
    return proxied.getNestedTransactions();
  }

  @Override
  public boolean checkTransactionIsActive(final boolean createTx) {
    return proxied.checkTransactionIsActive(createTx);
  }

  @Override
  public boolean isAsyncProcessing() {
    return proxied.isAsyncProcessing();
  }

  @Override
  public void waitForAsyncCompletion() {
    proxied.waitForAsyncCompletion();
  }

  @Override
  public AsyncQuiesce quiesceAsync() {
    return proxied.quiesceAsync();
  }

  @Override
  public LocalTransactionExplicitLock acquireLock() {
    return proxied.acquireLock();
  }

  @Override
  public void transaction(final TransactionScope txBlock) {
    proxied.transaction(txBlock);
  }

  @Override
  public boolean isAutoTransaction() {
    return proxied.isAutoTransaction();
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    proxied.setAutoTransaction(autoTransaction);
  }

  @Override
  public void begin() {
    proxied.begin();
  }

  @Override
  public void begin(final TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    proxied.begin(isolationLevel);
  }

  @Override
  public void rollback() {
    proxied.rollback();
  }

  @Override
  public void rollbackAllNested() {
    proxied.rollbackAllNested();
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    proxied.scanType(typeName, polymorphic, callback);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    proxied.scanType(typeName, polymorphic, callback, errorRecordCallback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    proxied.scanBucket(bucketName, callback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    proxied.scanBucket(bucketName, callback, errorRecordCallback);
  }

  @Override
  public boolean existsRecord(final RID rid) {
    return proxied.existsRecord(rid);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    return proxied.lookupByRID(rid, loadContent);
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    return proxied.iterateType(typeName, polymorphic);
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    return proxied.iterateBucket(bucketName);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String keyName, final Object keyValue) {
    return proxied.lookupByKey(type, keyName, keyValue);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String[] keyNames, final Object[] keyValues) {
    return proxied.lookupByKey(type, keyNames, keyValues);
  }

  @Override
  public void deleteRecord(final Record record) {
    proxied.deleteRecord(record);
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    return proxied.countType(typeName, polymorphic);
  }

  @Override
  public long countBucket(final String bucketName) {
    return proxied.countBucket(bucketName);
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    return proxied.newDocument(typeName);
  }

  @Override
  public MutableEmbeddedDocument newEmbeddedDocument(final EmbeddedModifier modifier, final String typeName) {
    return proxied.newEmbeddedDocument(modifier, typeName);
  }

  @Override
  public MutableVertex newVertex(final String typeName) {
    return proxied.newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType,
      final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional,
      final Object... properties) {
    return proxied.newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames,
        destinationVertexKeyValues, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public QueryEngine getQueryEngine(final String language) {
    return proxied.getQueryEngine(language);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
      final Object[] sourceVertexKeyValues, final String destinationVertexType,
      final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional,
      final Object... properties) {
    return proxied.newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues,
        destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues, createVertexIfNotExist,
        edgeType, bidirectional, properties);
  }

  @Override
  public Schema getSchema() {
    return proxied.getSchema();
  }

  @Override
  public RecordEvents getEvents() {
    return proxied.getEvents();
  }

  @Override
  public FileManager getFileManager() {
    return proxied.getFileManager();
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinActiveTx) {
    return proxied.transaction(txBlock, joinActiveTx);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries) {
    return proxied.transaction(txBlock, joinCurrentTx, retries);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries,
      final OkCallback ok, final ErrorCallback error) {
    return proxied.transaction(txBlock, joinCurrentTx, retries, ok, error);
  }

  @Override
  public RecordFactory getRecordFactory() {
    return proxied.getRecordFactory();
  }

  @Override
  public BinarySerializer getSerializer() {
    return proxied.getSerializer();
  }

  @Override
  public PageManager getPageManager() {
    return proxied.getPageManager();
  }

  @Override
  public int hashCode() {
    return proxied.hashCode();
  }

  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Database))
      return false;

    final Database other = (Database) o;
    return Objects.equals(getDatabasePath(), other.getDatabasePath());
  }

  @Override
  public ResultSet query(final String language, final String query) {
    waitForReadConsistency();
    return proxied.query(language, query);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... args) {
    waitForReadConsistency();
    return proxied.query(language, query, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> args) {
    waitForReadConsistency();
    return proxied.query(language, query, args);
  }

  /**
   * Applies the read-consistency barrier for a command, but ONLY when the statement is read-only
   * (idempotent and not DDL). A read-only command (e.g. a SELECT sent to {@code /api/v1/command})
   * must honor {@code X-ArcadeDB-Read-Consistency} exactly like {@link #query} does; a mutating
   * command or DDL must NOT get a read barrier (it would be meaningless and could mask routing).
   */
  private void applyReadConsistencyForReadOnlyCommand(final QueryEngine.AnalyzedQuery analyzed) {
    if (analyzed != null && analyzed.isIdempotent() && !analyzed.isDDL())
      waitForReadConsistency();
  }

  /**
   * Like {@link #applyReadConsistencyForReadOnlyCommand}, but defers the (relatively expensive)
   * query analysis until we know a non-EVENTUAL read-consistency context is actually set. Used on
   * the leader's command() path so the common write path (no consistency header) pays no extra
   * parsing.
   */
  private void applyReadConsistencyForReadOnlyCommandIfRequested(final String language, final String query) {
    if (raftHAServer == null)
      return;
    final ReadConsistencyContext ctx = READ_CONSISTENCY_CONTEXT.get();
    if (ctx == null || ctx.consistency() == null || ctx.consistency() == Database.READ_CONSISTENCY.EVENTUAL)
      return;
    final QueryEngine queryEngine = proxied.getQueryEngineManager().getEngine(language, this);
    applyReadConsistencyForReadOnlyCommand(queryEngine.analyze(query));
  }

  private void waitForReadConsistency() {
    if (raftHAServer == null)
      return;

    final ReadConsistencyContext ctx = READ_CONSISTENCY_CONTEXT.get();
    if (ctx == null)
      return;

    final Database.READ_CONSISTENCY consistency = ctx.consistency();
    if (consistency == null || consistency == Database.READ_CONSISTENCY.EVENTUAL)
      return;

    // Scoped to THIS database: a snapshot install that gave up on one database publishes a read floor for it
    // alone, and the healthy co-located databases must keep serving unclamped reads (issue #6760).
    if (consistency == Database.READ_CONSISTENCY.READ_YOUR_WRITES) {
      if (!isLeader() && ctx.readAfterIndex() >= 0)
        raftHAServer.waitForAppliedIndex(getName(), ctx.readAfterIndex());
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      if (isLeader())
        raftHAServer.ensureLinearizableRead(getName());
      else
        raftHAServer.ensureLinearizableFollowerRead(getName());
    }
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Object... args) {
    return proxied.execute(language, script, args);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Map<String, Object> args) {
    return proxied.execute(language, script, server.getConfiguration(), args);
  }

  @Override
  public <RET> RET executeInReadLock(final Callable<RET> callable) {
    return proxied.executeInReadLock(callable);
  }

  @Override
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    return proxied.executeInWriteLock(callable);
  }

  @Override
  public <RET> RET executeLockingFiles(final Collection<Integer> fileIds, final Callable<RET> callable) {
    return proxied.executeLockingFiles(fileIds, callable);
  }

  @Override
  public void setDataEncryption(DataEncryption encryption) {
    DatabaseInternal.super.setDataEncryption(encryption);
  }

  @Override
  public boolean isReadYourWrites() {
    return proxied.isReadYourWrites();
  }

  @Override
  public Database setReadYourWrites(final boolean value) {
    proxied.setReadYourWrites(value);
    return this;
  }

  @Override
  public Database setTransactionIsolationLevel(final TRANSACTION_ISOLATION_LEVEL level) {
    return proxied.setTransactionIsolationLevel(level);
  }

  @Override
  public TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
    return proxied.getTransactionIsolationLevel();
  }

  @Override
  public Database setUseWAL(final boolean useWAL) {
    return proxied.setUseWAL(useWAL);
  }

  @Override
  public Database setWALFlush(final WALFile.FlushType flush) {
    return proxied.setWALFlush(flush);
  }

  @Override
  public boolean isAsyncFlush() {
    return proxied.isAsyncFlush();
  }

  @Override
  public Database setAsyncFlush(final boolean value) {
    return proxied.setAsyncFlush(value);
  }

  @Override
  public boolean isOpen() {
    return proxied.isOpen();
  }

  @Override
  public boolean isFencedForRecovery() {
    return proxied.isFencedForRecovery();
  }

  @Override
  public String toString() {
    return proxied.toString() + "[" + server.getServerName() + "]";
  }

  @Override
  public <RET> RET recordFileChanges(final Callable<Object> callback) {
    if (!isLeader())
      throw schemaChangesNeedTheLeader();

    // A recording session already open ON THIS THREAD means we are nested inside our own DDL or
    // compaction session (schema DDL nests routinely: the outer callback creates the type, an inner one
    // creates its buckets). The outer frame captures these file changes and ships them, so delegating is
    // correct. A session owned by ANOTHER thread carries no such promise, which is why the bare
    // "is a session open?" test cannot decide this - see acquireRecordingSession (#5728). The ownership
    // question is asked of this database's own FileManager rather than of isSchemaCommitThread, which is
    // static and would also answer yes for a thread nested in a DIFFERENT database's session.
    if (proxied.getFileManager().isRecordingChangesOnCurrentThread())
      return proxied.recordFileChanges(callback);

    // On the leader, record file changes and send them via Raft immediately
    // (like the legacy HA system) so replicas have the files before WAL pages arrive
    acquireRecordingSession();

    // Claiming the session can take seconds under contention, so leadership is re-checked afterwards:
    // running the callback on a node that became a follower in the meantime would apply the schema change
    // there and propose nothing, the same divergence this fix exists to prevent, just from the other end.
    if (!isLeader()) {
      proxied.getFileManager().stopRecordingChanges();
      throw schemaChangesNeedTheLeader();
    }

    final long schemaVersionBefore = proxied.getSchema().getEmbedded().getVersion();

    // Capture schema changes, then send via Raft after releasing the write lock
    final Map<Integer, String> addFiles = new HashMap<>();
    final Map<Integer, String> removeFiles = new HashMap<>();
    String serializedSchema = "";

    // Clear thread-local WAL buffers so any commits inside the callback are captured fresh,
    // and mark THIS thread as the schema-commit thread so commit() buffers rather than replicates.
    // The flag is saved and restored rather than removed on the way out: it is static, so a session on
    // another database opened from inside an outer callback would otherwise clear the outer frame's mark,
    // and the outer callback's remaining commits would ship separate TX_ENTRYs instead of riding its
    // SCHEMA_ENTRY - the ordering hazard of issue #4083.
    final Boolean outerSchemaCommitThread = isSchemaCommitThread.get();
    // Saved and restored for exactly the reason the mark above is: a session opened on ANOTHER database from
    // inside this callback must not take over this one's instalment bookkeeping (issue #6136).
    final SchemaInstalmentState outerInstalments = schemaInstalments.get();
    final SchemaInstalmentState instalmentState = new SchemaInstalmentState();
    schemaWalBuffer.get().clear();
    schemaBucketDeltaBuffer.get().clear();
    isSchemaCommitThread.set(Boolean.TRUE);
    schemaInstalments.set(instalmentState);

    // Set only once the session's FINAL entry has gone out. Everything an instalment shipped before that is
    // delivery-only, so leaving this false is what tells the finally block that the followers are holding an
    // abandoned prefix and it has to be retired - see retireAbandonedInstalments (issue #6136).
    boolean published = false;

    try {
      final RET result = proxied.recordFileChanges(callback);

      // Capture file changes
      final List<FileManager.FileChange> fileChanges = proxied.getFileManager().getRecordedChanges();
      final boolean schemaChanged = proxied.getSchema().getEmbedded().isDirty() ||
          schemaVersionBefore < 0 || proxied.getSchema().getEmbedded().getVersion() != schemaVersionBefore;

      if (fileChanges != null)
        for (final FileManager.FileChange c : fileChanges) {
          if (c.create)
            addFiles.put(c.fileId, c.fileName);
          else
            removeFiles.put(c.fileId, c.fileName);
        }

      reconcileInstalmentFiles(instalmentState.shippedFiles, addFiles, removeFiles);

      // Issue #6989: ship what the DDL changed rather than the whole schema document, when the cluster is
      // configured for it and the cached base is still what the followers hold. schemaDeltaFor() returns null
      // whenever either is not true, and then the whole document goes out exactly as before.
      final JSONObject fullSchema = schemaChanged ? proxied.getSchema().getEmbedded().toJSON() : null;
      final SchemaDelta.Payload schemaDelta = fullSchema != null ? schemaDeltaFor(fullSchema) : null;
      if (fullSchema != null && schemaDelta == null)
        serializedSchema = fullSchema.toString();

      // Collect any WAL entries buffered by commit() calls that occurred inside the callback
      final List<byte[]> walEntries = new ArrayList<>(schemaWalBuffer.get());
      final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>(schemaBucketDeltaBuffer.get());
      schemaWalBuffer.get().clear();
      schemaBucketDeltaBuffer.get().clear();

      // Send schema changes via Raft so replicas have the files before WAL pages arrive.
      // Embedded walEntries carry the initial page writes (e.g. index root pages) so
      // replicas apply them immediately after creating the files - in the correct order.
      //
      // walEntries must be part of this condition. commit() above already ran commit2ndPhase, so the
      // pages are on the leader; this buffer holds the ONLY copy that can reach a follower, and the
      // finally block clears it. A callback that writes records without creating a file or moving the
      // schema version would otherwise leave followers trailing by exactly those page versions, and the
      // next ordinary transaction touching one of those pages fails on them with WALVersionGapException,
      // marking the database diverged. The sibling runWithCompactionReplication guards the same buffers
      // with walEntries.isEmpty() included.
      // ... and unconditionally once an instalment went out: those entries are all marked as delivery-only, so
      // the session's final entry is the only thing that publishes the change. Leaving it out because nothing was
      // left to say would strand the followers on a half-delivered state they never reload from.
      final int shippedInstalments = instalmentState.instalments;
      if (!addFiles.isEmpty() || !removeFiles.isEmpty() || schemaChanged || !walEntries.isEmpty()
          || shippedInstalments > 0) {
        final RaftHAServer raft = requireRaftServer();
        raft.getTransactionBroker().replicateSchema(getName(), serializedSchema, addFiles, removeFiles, walEntries,
            bucketDeltas, Collections.emptyList(), Collections.emptyList(), schemaDelta);
        // Set HERE, not after the logging below: the change is published the moment that call returns, and a
        // diagnostic that threw would otherwise send the finally block into retireAbandonedInstalments to report a
        // divergence that does not exist. Harmless for on-disk state - the compensation only ever targets files
        // this node no longer has, and it still has them - but it would put an operator on a phantom hunt.
        //
        // Assigned a SECOND time after this block, and neither assignment is redundant: this one covers a throw
        // between here and there, the other covers the path where this block is not entered at all.
        published = true;

        // Deliberately BELOW the assignment above, for the reason it documents: this only records what the
        // followers now hold so the NEXT change can be diffed against it (issue #6989), and it must not be able
        // to send the finally block hunting a divergence that did not happen. It runs after the entry has gone
        // out for the converse reason - an entry that threw on its way to the log did not move the followers, so
        // the base must not advance past it.
        if (fullSchema != null)
          rememberReplicatedSchema(fullSchema, schemaDelta == null ? serializedSchema.length() : -1);

        HALog.log(this, HALog.DETAILED,
            "Schema changes replicated via Raft: addFiles=%d, removeFiles=%d, schemaChanged=%s, schemaPayload=%s, "
                + "embeddedWalEntries=%d, instalments=%d",
            addFiles.size(), removeFiles.size(), schemaChanged,
            schemaDelta != null ?
                "delta of " + schemaDelta.deltaJson().length() + " chars" :
                "document of " + serializedSchema.length() + " chars",
            walEntries.size(), shippedInstalments);

        // The document, not the payload: with a delta the payload is not a schema document at all, and the
        // #4083 cross-reference below is about the schema being published, not about how it is encoded. Only
        // reached at DETAILED, so the render this costs is one an operator asked for.
        if (HALog.isEnabled(HALog.DETAILED))
          logSchemaPayloadDiagnostics("recordFileChanges",
              fullSchema != null ? fullSchema.toString() : serializedSchema, addFiles, removeFiles);
      }

      // The SECOND of the two assignments (see the one inside the block above). This one covers the path where the
      // block was not entered at all: there was nothing to say, which can only happen when no instalment went out
      // either - an instalment forces the condition - so there is nothing for the compensation to do.
      //
      // ADDING CODE ABOVE THIS LINE: everything between the first assignment and here is code that can throw AFTER
      // the change has already been published, and this flag is what decides whether the finally block goes on to
      // retire the instalments' files. Decide deliberately which side of it your code belongs on rather than
      // dropping it in above.
      published = true;

      // Issue #6144: one INFO line for a session that had to ship incrementally, so the common case is visible with
      // no metrics backend at all and without first turning on detailed HA logging - which, for a stall, means
      // reproducing it. Only sessions that shipped print anything, so an ordinary DDL stays silent. Deliberately
      // BELOW the assignment above: it is a diagnostic, and one that threw before it would send the finally block
      // into retiring files that are in fact published.
      if (shippedInstalments > 0)
        LogManager.instance().log(this, Level.INFO,
            "Schema session on database '%s' shipped its WAL in %d instalment(s) totalling %d ms, each of them a "
                + "quorum round trip taken with the database write lock held (every writer on this database waits "
                + "them out). Lower arcadedb.ha.appendBufferSize multiplies them, raising it makes each one bigger",
            null, getName(), shippedInstalments, instalmentState.elapsedMs);

      return result;
    } finally {
      if (!published)
        retireAbandonedInstalments(instalmentState);
      if (outerSchemaCommitThread == null)
        isSchemaCommitThread.remove();
      else
        isSchemaCommitThread.set(outerSchemaCommitThread);
      if (outerInstalments == null)
        schemaInstalments.remove();
      else {
        // The WAL buffers are STATIC thread-locals shared with the outer frame, and the lines below clear them.
        // The outer frame's byte count must not go on describing WAL that is no longer there, or its next
        // threshold test would fire against a buffer that no longer holds what the count claims. Zeroing it keeps
        // the counter and the buffer telling the same story.
        //
        // NOTE this does not make a nested session on ANOTHER database safe - the outer frame's buffered WAL is
        // destroyed by that clear, which is a pre-existing property of sharing one static buffer and is why
        // isSchemaCommitThread is saved and restored around it. Nothing in the index-rebuild path nests that way.
        outerInstalments.bufferedBytes = 0;
        schemaInstalments.set(outerInstalments);
      }
      schemaWalBuffer.get().clear();
      schemaBucketDeltaBuffer.get().clear();
      proxied.getFileManager().stopRecordingChanges();
    }
  }

  /**
   * Retires what the instalments of a session that never reached its final entry left on the followers
   * (issue #6136).
   * <p>
   * WHY THIS IS NEEDED, and it is a risk the instalments introduce rather than one that was already there. Before
   * them, the whole buffered WAL sat in leader heap and NOTHING reached a follower until the callback returned, so
   * a build that threw part-way - a source record the builder cannot read, an I/O error, a quorum timeout on a
   * later instalment - left the followers untouched. Now the files an instalment announced and the pages it
   * delivered are already there, and the entry that would have published or retired them is never sent, because it
   * lives after the line that threw. Every instalment chunk is marked {@code moreChunksFollow} by design, so there
   * is no "this sequence is abandoned" signal a follower could act on by itself; the leader has to say so.
   * <p>
   * WHAT IT SENDS is one ordinary publishing entry - {@code moreChunksFollow} false - carrying nothing but
   * {@code filesToRemove}, and the removal list FOLLOWS THE LEADER rather than simply undoing every announcement.
   * That distinction is the whole correctness of this method, and getting it wrong swaps one divergence for
   * another:
   * <ul>
   *   <li>a file the leader NO LONGER HAS is retired. This is the case that matters, because it is the one the
   *       principal caller produces: {@code BucketIndexBuilder.create()} drops the half-built index from its own
   *       error handler, so the leader has already let the file go and only the followers are still holding it;</li>
   *   <li>a file the leader STILL HAS is left alone, and reported. A failed DDL can leave the leader holding a
   *       created-but-unpublished file - the schema was never saved, so nothing references it there either - and
   *       retiring it on the followers would take a state both sides agree on and make them disagree. The
   *       leader-side orphan is a pre-existing consequence of a DDL that throws, not something instalments
   *       introduce, and this method deliberately does not try to repair it.</li>
   * </ul>
   * Pages an instalment applied to PRE-EXISTING files are left alone for the same reason: {@code commit()} ran
   * {@code commit2ndPhase} before buffering, so the leader holds them too and the two sides already agree.
   * <p>
   * IT CANNOT THROW. It runs from a {@code finally} that is usually unwinding the build's own exception, and
   * masking that with a replication error would replace a diagnosable failure with a confusing one. A compensation
   * that fails is therefore logged at SEVERE naming every file it could not retire, which is what an operator
   * needs to clean them up by hand.
   * <p>
   * WHEN THE COMPENSATION ITSELF CANNOT RUN, named because it is the most Raft-idiomatic way for this path to fail
   * and it is a consequence instalments introduce. {@code recordFileChanges} checks {@code isLeader()} once, at the
   * top; nothing re-checks it per instalment, and {@code requireRaftServer()} only asserts that a server exists,
   * not that this node still leads. If leadership moves away after an instalment has shipped - failover, a brief
   * partition, a manual step-down, all normal Raft events rather than application bugs - this node can no longer
   * submit entries, so the removal below fails and lands in the SEVERE branch.
   * <p>
   * Before instalments that case cost nothing: nothing had reached a follower, so the final {@code replicateSchema}
   * simply failed and the caller saw a retryable error. Now it leaves files on the other nodes that only an
   * operator can remove, which is why the log line has to name them. Deliberately not "solved" here: re-checking
   * leadership per instalment would narrow the window without closing it (leadership can move between the check
   * and the submit), and a node that is no longer leader has no way to make the cluster do anything. The real fix
   * is for the new leader to reclaim unreferenced files, which is a garbage-collection feature this database does
   * not have. Tracked as issue #6143, together with the diagnostic that lets an operator FIND those files on the
   * nodes still holding them - {@code CHECK DATABASE} names them and every node publishes its own count as the
   * {@code arcadedb.ha.schema.unreferenced_files} gauge - since only this node logs anything about them.
   */
  private InstalmentRetirement retireAbandonedInstalments(final SchemaInstalmentState state) {
    if (state == null)
      return InstalmentRetirement.NOTHING_SHIPPED;

    return retireAbandonedInstalments(this, getName(), state.instalments, state.shippedFiles,
        proxied.getFileManager()::existsFile,
        filesToRemove -> requireRaftServer().getTransactionBroker().replicateSchema(getName(), "",
            Collections.emptyMap(), filesToRemove, Collections.emptyList(), Collections.emptyList()),
        this::isLeader);
  }

  /**
   * The compensation itself, with everything it touches passed in (issue #6143).
   * <p>
   * Split from the method above and made static so the branch that MATTERS most can be tested at all: a submitter
   * that throws is a node that lost leadership mid-session, and reproducing that on a real cluster needs a step-down
   * to land between an instalment and the unwind. It reports what it did through the return value rather than only
   * through the log, so a test asserts behaviour instead of scraping messages - and a caller could act on it.
   *
   * @param logContext    what the log lines are attributed to, so they still read as coming from the database
   * @param existsLocally answers whether THIS node still holds a given file id, the rule the whole method turns on
   * @param submitter     replicates the removal; the only thing here that can fail
   * @param stillLeader   used solely to phrase the failure, never to decide anything
   */
  static InstalmentRetirement retireAbandonedInstalments(final Object logContext, final String databaseName,
      final int instalments, final Map<Integer, String> shippedFiles, final IntPredicate existsLocally,
      final SchemaRetirementSubmitter submitter, final BooleanSupplier stillLeader) {
    if (instalments == 0)
      return InstalmentRetirement.NOTHING_SHIPPED;

    // Only what the leader has already let go of - see the javadoc: retiring a file the leader kept would turn an
    // agreed state into a diverged one.
    final Map<Integer, String> toRetire = new LinkedHashMap<>();
    final Map<Integer, String> keptByLeader = new LinkedHashMap<>();
    partitionAbandonedFiles(shippedFiles, existsLocally, toRetire, keptByLeader);

    if (!keptByLeader.isEmpty())
      LogManager.instance().log(logContext, Level.WARNING,
          "Schema session on database '%s' failed after shipping %d instalment(s); file(s) %s exist on every node "
              + "but no schema references them, because the change was never published. Left in place: this node "
              + "holds them too, so the cluster is consistent", null, databaseName, instalments,
          keptByLeader.values());

    if (toRetire.isEmpty()) {
      if (keptByLeader.isEmpty()) {
        // WAL-only instalments: those pages went to files that already existed and the leader committed them
        // locally before buffering, so there is nothing to undo. Still reported, because an interrupted
        // replicated session is not a normal event.
        LogManager.instance().log(logContext, Level.WARNING,
            "Schema session on database '%s' failed after shipping %d WAL instalment(s); they created no file, so "
                + "there is nothing to retire on the other nodes", null, databaseName, instalments);
        return InstalmentRetirement.NOTHING_TO_RETIRE;
      }
      return InstalmentRetirement.KEPT_BY_THIS_NODE;
    }

    try {
      submitter.retire(toRetire);

      LogManager.instance().log(logContext, Level.WARNING,
          "Schema session on database '%s' failed after shipping %d instalment(s); retired the %d file(s) they had "
              + "created on the other nodes, matching this node: %s", null, databaseName, instalments,
          toRetire.size(), toRetire.values());

      return InstalmentRetirement.RETIRED;

    } catch (final Exception e) {
      // Names the leadership case, because it is both the likeliest cause and the one where "retry the operation"
      // is the wrong advice: this node cannot make the cluster do anything any more, so the files stay until
      // somebody removes them. It also names how to find them, because the nodes that HOLD them log nothing.
      LogManager.instance().log(logContext, Level.SEVERE,
          "Schema session on database '%s' failed after shipping %d instalment(s) AND the compensating removal "
              + "could not be replicated%s. The other nodes are holding file(s) this node does not have and nothing "
              + "will reclaim them: %s. Run CHECK DATABASE, or read the arcadedb.ha.schema.unreferenced_files "
              + "gauge, on each node to find them, and remove them once the cluster is healthy", e, databaseName,
          instalments, stillLeader.getAsBoolean() ? "" : " because this node is no longer the leader",
          toRetire.values());

      return InstalmentRetirement.NOT_REPLICATED;
    }
  }

  /** What one compensation run did. Returned rather than only logged so the failure branches can be tested. */
  enum InstalmentRetirement {
    /** No instalment went out, so nothing on any other node is waiting to be undone. */
    NOTHING_SHIPPED,
    /** Instalments went out but created no file: their pages landed in files that already existed everywhere. */
    NOTHING_TO_RETIRE,
    /** Every announced file is still here, so both sides agree and retiring would be the divergence. */
    KEPT_BY_THIS_NODE,
    /** The other nodes were told to drop what this node no longer has. */
    RETIRED,
    /** The removal could not be replicated - typically lost leadership. The files stay until an operator acts. */
    NOT_REPLICATED
  }

  /** Replicates the compensating removal. Separate from the broker so a test can make it fail (issue #6143). */
  @FunctionalInterface
  interface SchemaRetirementSubmitter {
    void retire(Map<Integer, String> filesToRemove) throws Exception;
  }

  /**
   * Folds what the instalments already shipped into the session's FINAL file maps (issue #6136).
   * <p>
   * A file an instalment created is not re-announced - it exists on the followers already. More importantly, one
   * the session went on to DROP has to be retired explicitly: {@code FileManager.dropFile} CANCELS the recorded
   * create when both happen inside one session, so the final entry would say nothing about a file the followers
   * were told to create, and it would survive there and nowhere else.
   * <p>
   * A no-op when no instalment went out, which is every session small enough to ship in one entry.
   */
  // @VisibleForTesting
  static void reconcileInstalmentFiles(final Map<Integer, String> shippedFiles, final Map<Integer, String> addFiles,
      final Map<Integer, String> removeFiles) {
    if (shippedFiles == null || shippedFiles.isEmpty())
      return;

    for (final Map.Entry<Integer, String> shipped : shippedFiles.entrySet())
      if (addFiles.remove(shipped.getKey()) == null)
        removeFiles.putIfAbsent(shipped.getKey(), shipped.getValue());
  }

  /**
   * Ships the buffered schema WAL as an ordered instalment once it crosses the threshold (issue #6136).
   * <p>
   * WHAT THIS FIXES. {@code BucketIndexBuilder.create()} wraps the whole build in {@code recordFileChanges}, which
   * marks this thread so that {@code commit()} BUFFERS instead of replicating; {@code LSMTreeIndex.build()} then
   * commits once per {@code IndexBuilder.BUILD_BATCH_SIZE} records. Every one of those WAL images stayed in leader
   * heap until the callback returned, so peak heap was roughly the whole rebuilt index plus a repeat of every page
   * more than one batch touched. A {@code CHECK DATABASE FIX} that rebuilds the indexes of a large damaged type is
   * exactly that shape, on a node that is already the cluster's leader.
   * <p>
   * WHY IT IS SAFE, which is the whole question on this path - #4083, #4743 and #5492 all surfaced here as SILENT
   * follower divergence rather than as an exception. It ships nothing new: it is the ordered-prefix sequence
   * {@code splitSchemaEntry} has produced since #4743, emitted as the payload is produced rather than after it is
   * built. Files first, WAL in the middle, and the publishing fields - schema JSON, {@code filesToRemove} - only in
   * the session's final entry, so every prefix a follower can be left holding is self-consistent: the files exist
   * before pages land in them, and a partially written new file is unreferenced bytes until the last entry
   * publishes it. Followers need no new code, because that sequence is the one they already receive.
   * <p>
   * THE THRESHOLD is derived, not configured: half the maximum replicated entry size, the same expression
   * {@code runWithCompactionReplication} uses for its own chunk budget. It is SOFT - tested between buffered
   * commits, so one batch can overshoot it - which is why {@code replicateSchemaInstalment} can still split.
   * <p>
   * THE RISK IT DOES INTRODUCE, and how it is paid for: a build that throws after an instalment has gone out
   * leaves the followers holding files and pages the session will never publish, where before them nothing reached
   * a follower until the callback returned. {@link #retireAbandonedInstalments} sends the compensating removal.
   * <p>
   * WHAT IT COSTS, stated precisely because it is a real trade and not only a heap win. The recording session stays
   * open for the whole build, so a concurrent writer still waits on {@link #waitForActiveRecordingSession()} and,
   * before that, on the database write lock the callback holds. Bounding the heap does not shorten that window -
   * and each instalment LENGTHENS it, because {@code submitAndWait} is a quorum round trip taken while that write
   * lock is held, where the single final entry has always been submitted after the callback returned and the lock
   * was released. Normally that is milliseconds against a build measured in minutes. Against a slow or briefly
   * partitioned quorum member it is up to {@code HA_QUORUM_TIMEOUT} per instalment, and the whole database's
   * writers wait it out.
   * <p>
   * Accepted, because the alternative is not "no round trips": without instalments the one final entry carries the
   * whole index, {@code splitSchemaEntry} splits it, and the same number of round trips happens anyway - after the
   * lock, but only after the leader has held the entire rebuilt index in heap, which is the failure this exists to
   * prevent. Paying for a bounded heap with round trips inside a window that is already fully exclusive is the
   * better side of that trade. Making the window itself shorter means taking the build out of the recording
   * session altogether - a different change, on the same code path, worth its own judgement.
   */
  private void flushSchemaWalBufferIfFull() {
    final SchemaInstalmentState state = schemaInstalments.get();
    // bufferedBytes == 0 covers BOTH "nothing buffered yet" and "just flushed", and is tested before the threshold
    // is resolved rather than after. This method runs after EVERY commit inside the session, including ones that
    // buffered nothing (commit1stPhase returning null), so resolving first would call requireRaftServer() on a
    // no-op commit - the case the lazy resolution exists to keep working while the Raft server is transiently
    // absent. Zero bytes can never reach a positive threshold anyway, so nothing is lost by leaving early.
    if (state == null || state.bufferedBytes == 0)
      return;

    if (state.threshold == 0)
      state.threshold = requireRaftServer().getTransactionBroker().walChunkBudget();

    if (state.bufferedBytes < state.threshold)
      return;

    final RaftTransactionBroker broker = requireRaftServer().getTransactionBroker();

    // Only the files created since the previous instalment: an already-announced one exists on the followers, and
    // re-announcing it would make createNewFiles run over a file that is being written into.
    //
    // The split is carried FORWARD by the file manager rather than re-derived here (issue #6142): this used to walk
    // the whole cumulative recorded-changes list on every instalment and filter it against shippedFiles, which is
    // O(instalments x file changes) - harmless for an index rebuild, which records one or two file changes however
    // many instalments its WAL volume produces, but quadratic for a DDL that creates many files through this same
    // buffered path. Draining a queue makes the whole session cost one pass over its creations. An index into the
    // cumulative list would NOT have worked: FileManager.dropFile removes the cancelled create from the middle of
    // it, so a saved position stops meaning what it meant.
    final Map<Integer, String> newFiles = proxied.getFileManager().drainRecordedCreates();

    final List<byte[]> walEntries = new ArrayList<>(schemaWalBuffer.get());
    final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>(schemaBucketDeltaBuffer.get());

    // ANNOUNCED BEFORE IT IS SENT, and deliberately so. replicateSchemaInstalment is not one entry: an instalment
    // that overshoots the cap - which the SOFT threshold makes reachable, since it is tested between buffered
    // commits and one batch can carry more than the whole budget - goes through splitSchemaEntry and becomes
    // several submitAndWait calls, the FIRST of which carries the file creations. A failure on a later chunk would
    // then leave the followers holding files this bookkeeping had never recorded, so retireAbandonedInstalments
    // would not know to retire them, and on a first instalment it would not even log, having seen no instalment at
    // all. Recording the intent first makes the bookkeeping pessimistic: it says "the followers MAY hold these",
    // which is what a compensation needs. Over-retiring costs nothing - the follower's FileManager.dropFile
    // no-ops on a file id it does not have.
    state.shippedFiles.putAll(newFiles);
    ++state.instalments;
    schemaInstalmentsShipped.incrementAndGet();

    // Timed because the elapsed time IS the cost of this design (issue #6144): submitAndWait is a quorum round trip
    // taken while the database write lock is held, so this interval is what every other writer on the database is
    // waiting out. Measured around the send alone - the bookkeeping above and the buffer drain below are local.
    final long startedAtNanos = System.nanoTime();
    try {
      broker.replicateSchemaInstalment(getName(), newFiles, walEntries, bucketDeltas);
    } finally {
      // In a finally so a failed instalment is counted too: an instalment that timed out against a slow quorum
      // member held the write lock for the whole timeout, which is exactly the event an operator is looking for.
      final long elapsedMs = (System.nanoTime() - startedAtNanos) / 1_000_000;
      state.elapsedMs += elapsedMs;
      schemaInstalmentTotalMs.addAndGet(elapsedMs);
      schemaInstalmentMaxMs.accumulateAndGet(elapsedMs, Math::max);
    }

    // The WAL buffer, by contrast, is drained only AFTER the entry is committed: it holds the ONLY copy of those
    // pages that can reach a follower, so clearing it before a failure would turn a replication error into silent
    // divergence - the failure mode this whole code path exists to avoid. The two orders are opposite because the
    // two risks are: announcing too much costs a no-op removal, forgetting WAL costs a diverged follower.
    schemaWalBuffer.get().clear();
    schemaBucketDeltaBuffer.get().clear();
    state.bufferedBytes = 0;

    HALog.log(this, HALog.DETAILED,
        "Schema WAL instalment %d for database '%s' shipped: newFiles=%d, walEntries=%d",
        state.instalments, getName(), newFiles.size(), walEntries.size());
  }

  /**
   * Splits what the instalments announced into what must be retired on the other nodes and what must be left alone,
   * by the one rule that keeps the compensation from trading one divergence for another: THIS NODE OWNS THE TRUTH.
   * A file it no longer has is retired; a file it still has is left, because both sides holding an unpublished file
   * is a state they agree on and retiring it would end that agreement. See {@link #retireAbandonedInstalments}.
   * <p>
   * Static and side-effect-free so the bookkeeping can be pinned without a cluster - it is map arithmetic across a
   * session, which is exactly the kind of logic that regresses silently.
   *
   * @param existsLocally answers whether this node still holds a given file id
   */
  // @VisibleForTesting
  static void partitionAbandonedFiles(final Map<Integer, String> shippedFiles, final IntPredicate existsLocally,
      final Map<Integer, String> toRetire, final Map<Integer, String> keptLocally) {
    for (final Map.Entry<Integer, String> shipped : shippedFiles.entrySet())
      if (existsLocally.test(shipped.getKey()))
        keptLocally.put(shipped.getKey(), shipped.getValue());
      else
        toRetire.put(shipped.getKey(), shipped.getValue());
  }

  /** Sealed-store slices this database has shipped since it opened - see {@link #sealedChunksShipped}. */
  public long getSealedStoreChunksShipped() {
    return sealedChunksShipped.get();
  }

  /**
   * The most slices any ONE sealed store has been cut into since this database opened (issue #4416).
   * <p>
   * The cumulative {@link #getSealedStoreChunksShipped()} cannot answer the question an operator actually has,
   * which is whether a SINGLE store is producing a burst: a thousand slices reads the same there whether it was
   * one pathological store or a thousand healthy one-slice ones. This is the burst gauge. Anything at or above
   * {@link GlobalConfiguration#MAX_REPLICATED_SEALED_CHUNKS} means the WARNING in {@link #planSealedSlices} fired
   * and the leader is holding a FileManager recording session open across that many sequential quorum round
   * trips, which shows up as leader commit latency (see that constant's javadoc for exactly what it blocks).
   */
  public long getSealedStoreMaxSliceSequence() {
    return sealedChunksMaxSequence.get();
  }

  /**
   * How ONE sealed store will be shipped: metadata only, no bytes (issues #4416, #6933).
   * <p>
   * The slices are described here and CUT ON DEMAND by {@link #slice}, because materializing them all up front
   * doubled leader heap for the whole shipping loop - the source image stays strongly reachable throughout, so a
   * 600MB sealed store cost 1.2GB, and #6917 raised the per-shard ceiling from one 32MB entry to the
   * {@code Integer.MAX_VALUE} clamp in {@link GlobalConfiguration#maxReplicatedSealedStoreSize}. Cutting one
   * slice at a time bounds the extra copy by one slice.
   * <p>
   * A plan has a BODY of up to {@code count - 1} slices of {@code bodySliceSize} bytes and a final slice of
   * {@code tailSize} bytes. The two sizes differ because they are spent in different places: every body slice
   * gets a delivery-only entry to itself, so it may use the whole per-entry budget, while every store's final
   * slice has to share ONE publishing entry with every other store of the same maintenance session (#6933).
   *
   * @param count         how many slices carry the store; {@code 0} when it ships whole as a
   *                      {@link RaftLogEntryCodec.TsSealedBlob}
   * @param bodySliceSize how many bytes each slice but the last carries
   * @param tailSize      how many bytes the final - publishing - slice carries
   * @param fileCrc       CRC32 of the whole image, carried on every slice so the follower validates what it
   *                      reassembled against the leader's own copy
   * @param fileLength    the whole image's length
   */
  // @VisibleForTesting
  record SealedSlicePlan(int count, int bodySliceSize, int tailSize, long fileCrc, int fileLength) {

    /** Whether the store needs slicing at all, or ships whole exactly as it did before #4416. */
    boolean sliced() {
      return count > 0;
    }

    /**
     * Cuts slice {@code index} out of {@code blob}. The returned chunk is the ONLY copy this plan makes, and it
     * is the caller's to drop as soon as it has been shipped.
     */
    RaftLogEntryCodec.TsSealedChunk slice(final RaftLogEntryCodec.TsSealedBlob blob, final int index) {
      final boolean last = index == count - 1;
      final int bodyLength = fileLength - tailSize;
      final int from = last ? bodyLength : (int) Math.min(bodyLength, (long) index * bodySliceSize);
      final int to = last ? fileLength : (int) Math.min(bodyLength, (long) from + bodySliceSize);
      return new RaftLogEntryCodec.TsSealedChunk(blob.typeName(), blob.shardIndex(), blob.fileName(), fileLength,
          fileCrc, from, Arrays.copyOfRange(blob.bytes(), from, to), last);
    }
  }

  /** The plan of a store that ships whole. */
  private static final SealedSlicePlan NOT_SLICED = new SealedSlicePlan(0, 0, 0, 0L, 0);

  /**
   * What the codec writes for one sealed slice besides its payload, MEASURED rather than guessed (issue #6933):
   * the type and file names are variable-length, and a long type name lengthens the derived
   * {@code .ts.sealed} name with it, so a flat constant is a bound that a sufficiently verbose schema walks past.
   * <p>
   * Encoding a whole empty entry rather than the slice section alone overstates it by the entry header, which is
   * the safe direction: {@link GlobalConfiguration#REPLICATED_SEALED_CHUNK_FRAMING_BYTES} already reserves for
   * ONE slice, so this is only charged for the stores BEYOND the first.
   */
  private static int sealedSliceFraming(final RaftLogEntryCodec.TsSealedBlob blob) {
    final RaftLogEntryCodec.TsSealedChunk empty = new RaftLogEntryCodec.TsSealedChunk(blob.typeName(),
        blob.shardIndex(), blob.fileName(), 0L, 0L, 0L, EMPTY_SLICE_PAYLOAD, true);
    return RaftLogEntryCodec.encodeSchemaEntry("", "", Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), false, List.of(empty)).size();
  }

  private static final byte[] EMPTY_SLICE_PAYLOAD = new byte[0];

  /**
   * What the publishing entry spends on everything that is NOT sealed payload, measured by encoding it (issue
   * #6933). Both file maps are measured even though {@code splitSchemaEntry} may move {@code filesToAdd} to the
   * FIRST chunk of a split: overstating the header can only make the sealed payload smaller, understating it puts
   * the entry over the cap.
   */
  private static int publishingHeaderSize(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove) {
    return RaftLogEntryCodec.encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove,
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()).size();
  }

  /**
   * How many raw sealed bytes the PUBLISHING entry may carry in total (issue #6933) - all the whole blobs plus
   * every sliced store's final slice.
   * <p>
   * Two different bounds meet here and both have to hold. {@code sealedEntryBudget} is the POLICY one,
   * {@code arcadedb.ha.tsMaxSealedInlineSize} less its framing and compression reserve, and it says nothing about
   * a schema JSON riding beside the payload because a delivery-only entry carries none. The transport one does:
   * the publishing entry also carries the schema JSON and the file maps, and it is the WHOLE entry that must stay
   * under the cap. Subtracting the header from the policy budget instead would be wrong in the direction that
   * matters - with a deliberately small inline cap the header is larger than the whole budget, and a shard that
   * used to seal would stop sealing.
   *
   * @param sealedEntryBudget    {@link GlobalConfiguration#replicatedSealedChunkBudget}; {@code <= 0} means
   *                             slicing is off entirely
   * @param entryCap             the transport's maximum replicated entry size
   * @param publishingHeaderSize what {@link #publishingHeaderSize} measured for this session
   */
  // @VisibleForTesting
  static long publishingSealedCapacity(final long sealedEntryBudget, final long entryCap,
      final int publishingHeaderSize) {
    if (sealedEntryBudget <= 0)
      return 0;
    final long transportRoom = entryCap - publishingHeaderSize
        - GlobalConfiguration.REPLICATED_SEALED_CHUNK_FRAMING_BYTES - entryCap / 128;
    return Math.min(sealedEntryBudget, transportRoom);
  }

  /**
   * Plans how EVERY sealed store of one compaction/maintenance session is shipped (issue #6933), before a single
   * entry is committed.
   * <p>
   * WHY THIS IS A SESSION-WIDE DECISION AND NOT A PER-STORE ONE. {@code TimeSeriesEngine.runSealedMaintenanceReplicated}
   * runs retention and downsampling for every shard of a type inside ONE
   * {@link #runWithCompactionReplication(Callable)} session, so {@code recordedSealed} routinely holds several
   * stores. Everything that PUBLISHES rides one entry: the whole blobs, and the final slice of every sliced
   * store. Sizing each store's final slice against the whole per-entry budget - which is what #6917 did - asks
   * that one entry to carry N budgets, and {@code RaftTransactionBroker.splitSchemaEntry} cannot split it,
   * because what blows the cap is the header rather than the WAL: it computes {@code walBudget <= 0} and throws.
   * By then {@code replicateSealedChunk} has already committed every earlier slice of every store to the Raft
   * log, leaving the followers staging sealed stores they will never install while the leader has swapped its own
   * and cleared its mutable buckets.
   * <p>
   * So the publishing entry's sealed capacity is divided evenly among the stores: a store no larger than its
   * share ships whole, a larger one is sliced with a final slice capped at that share. The body slices keep the
   * full per-entry budget, because each of them travels alone. The resulting publishing payload is at most
   * {@code stores x share}, which is the capacity itself - it fits by construction, at any shard count.
   *
   * WHY THE CAPACITY IS DIVIDED EVENLY AND NOT BY SIZE. A size-weighted allocation would fit a session that an
   * even one refuses - one tiny store beside one huge one, on a cap tight enough that the huge one's even share is
   * smaller than the tiny store already is. Even division is chosen anyway because the property that matters here
   * is the one a reader can check in a line: no store contributes more than its share, so the total cannot exceed
   * the capacity, at any shard count and in any order. A weighted split has to be re-derived every time the
   * publishing entry's contents change, and it buys headroom only in a regime an operator reaches by lowering
   * {@code arcadedb.ha.tsMaxSealedInlineSize} far below the cap - which the refusal message tells them to undo.
   *
   * @param sealedEntryBudget  raw sealed bytes one DELIVERY-ONLY entry may carry, from
   *                           {@link GlobalConfiguration#replicatedSealedChunkBudget}; {@code <= 0} means a cap
   *                           too small to slice at all, where every store keeps shipping whole exactly as it did
   *                           before #4416 and {@code TimeSeriesShard}'s own guard is what bounds it
   * @param publishingCapacity raw sealed bytes the PUBLISHING entry may carry in total, from
   *                           {@link #publishingSealedCapacity}
   *
   * @throws ReplicatedEntryTooLargeException when the publishing entry cannot carry even one slice per store.
   *                                          Thrown HERE, where nothing has been shipped yet, rather than from
   *                                          the splitter after the delivery-only slices are already committed.
   */
  // @VisibleForTesting
  static List<SealedSlicePlan> planSealedShipping(final List<RaftLogEntryCodec.TsSealedBlob> blobs,
      final long sealedEntryBudget, final long publishingCapacity, final String databaseName) {
    final List<SealedSlicePlan> plans = new ArrayList<>(blobs.size());
    if (blobs.isEmpty())
      return plans;

    if (sealedEntryBudget <= 0) {
      // A cap too small to hold even one slice's framing. Slicing is impossible, so this is the pre-#4416 world:
      // ship whole and let TimeSeriesShard's guard keep the stores under one entry.
      for (int i = 0; i < blobs.size(); i++)
        plans.add(NOT_SLICED);
      return plans;
    }

    // Every store beyond the first pays for its own slice framing outright; the FIRST one pays only for whatever
    // it costs ABOVE the flat reserve already held back inside publishingSealedCapacity. Charging it in full would
    // reserve twice for the same slice and shrink the tail of every single-shard cluster for nothing, and not
    // charging it at all would leave the flat reserve as the last unmeasured assumption here - a type name of a
    // few thousand characters really does outgrow it, and nothing caps type-name length.
    long extraFraming = Math.max(0L,
        sealedSliceFraming(blobs.getFirst()) - GlobalConfiguration.REPLICATED_SEALED_CHUNK_FRAMING_BYTES);
    for (int i = 1; i < blobs.size(); i++)
      extraFraming += sealedSliceFraming(blobs.get(i));

    final long share = (publishingCapacity - extraFraming) / blobs.size();
    if (share <= 0)
      throw new ReplicatedEntryTooLargeException(String.format(
          """
          Schema change for database '%s' publishes %d TimeSeries sealed store(s) but only %d bytes of one Raft \
          entry are left for them once the schema JSON and the file maps are on it, %d each after their framing. \
          Raise arcadedb.ha.appendBufferSize - and with it arcadedb.ha.writeBufferSize, which must stay >= \
          appendBufferSize + 8 bytes.""",
          databaseName, blobs.size(), Math.max(0L, publishingCapacity), Math.max(0L, share)));

    for (final RaftLogEntryCodec.TsSealedBlob blob : blobs)
      plans.add(planSealedSlices(blob, sealedEntryBudget, share, databaseName));
    return plans;
  }

  /**
   * Plans the slices of ONE sealed-store image (issue #4416), or returns a plan that is not
   * {@link SealedSlicePlan#sliced()} when it fits its share of the publishing entry and must ship inline as a
   * {@link RaftLogEntryCodec.TsSealedBlob} exactly as before.
   * <p>
   * Static and side-effect-free so the arithmetic that a follower's reassembly depends on - contiguous offsets, a
   * whole-file CRC every slice agrees on, exactly one slice flagged {@code last} - can be pinned without a cluster.
   * <p>
   * Slices are cut against the budgets rather than against the entry cap directly: the codec compresses each one,
   * so a slice that fits uncompressed always fits encoded, and the framing the budget already subtracts covers
   * the rest. A store needing more than {@code MAX_REPLICATED_SEALED_CHUNKS} slices is still shipped - refusing
   * here would leave the follower holding a stale sealed file the leader has already replaced, which is
   * divergence - but it is reported, because the shard guard in {@code TimeSeriesShard.compactInternal} is
   * supposed to have kept it from ever getting this far and the maintenance path (retention, downsampling) has no
   * such guard.
   * <p>
   * When the tail fits without shrinking anything the slicing is UNIFORM - the shape #4416 shipped. That is not
   * quite "every single-shard cluster keeps its old byte layout": a lone store owns the whole publishing capacity,
   * but the capacity itself is now the entry cap less the measured schema JSON and file maps, so a store whose
   * uniform tail was within a schema JSON of the budget gets one more, smaller, final slice than it used to. That
   * IS the thin-margin half of #6933, not a side effect of it.
   *
   * @param bodyBudget how many raw bytes of sealed content one delivery-only entry may carry; {@code <= 0}
   *                   disables slicing
   * @param tailBudget how many of those this store may spend on the shared publishing entry; {@code <= 0}
   *                   disables slicing
   */
  // @VisibleForTesting
  static SealedSlicePlan planSealedSlices(final RaftLogEntryCodec.TsSealedBlob blob, final long bodyBudget,
      final long tailBudget, final String databaseName) {
    final byte[] bytes = blob.bytes() != null ? blob.bytes() : new byte[0];
    if (bodyBudget <= 0 || tailBudget <= 0 || bytes.length <= tailBudget)
      return NOT_SLICED;

    final CRC32 crc = new CRC32();
    crc.update(bytes);

    final int length = bytes.length;
    final int bodySliceSize = (int) Math.min(bodyBudget, Integer.MAX_VALUE);

    // The uniform shape first, because it is the one #4416 pinned and the one a single-shard type always gets.
    final int uniformCount = (int) (((long) length + bodySliceSize - 1) / bodySliceSize);
    final long uniformTail = length - (long) (uniformCount - 1) * bodySliceSize;

    final int count;
    final int tailSize;
    if (uniformTail <= tailBudget) {
      count = uniformCount;
      tailSize = (int) uniformTail;
    } else {
      // The tail would not fit this store's share of the publishing entry: cap it, and let the body absorb the
      // difference. The body is at least one byte long here, because length > tailBudget >= tailSize.
      tailSize = (int) Math.min(tailBudget, Integer.MAX_VALUE);
      count = (int) (((long) length - tailSize + bodySliceSize - 1) / bodySliceSize) + 1;
    }

    if (count > GlobalConfiguration.MAX_REPLICATED_SEALED_CHUNKS)
      LogManager.instance().log(RaftReplicatedDatabase.class, Level.WARNING,
          """
          Sealed TimeSeries store '%s' of database '%s' is %d bytes and needs %d replicated entries of %d bytes, \
          above the %d one compaction is expected to produce. Shipping it anyway - withholding it would leave the \
          other nodes on a sealed store this one has already replaced - but raise arcadedb.ha.appendBufferSize, or \
          shorten the retention on this type, before the burst starts costing write latency.""",
          null, blob.fileName(), databaseName, length, count, bodySliceSize,
          GlobalConfiguration.MAX_REPLICATED_SEALED_CHUNKS);

    return new SealedSlicePlan(count, bodySliceSize, tailSize, crc.getValue(), length);
  }

  /**
   * The eager form of {@link #planSealedSlices}, for the single-store case where the whole entry budget is this
   * store's to spend. Kept because the #4416 tests pin the arithmetic through it; the shipping path uses the plan
   * directly so it never holds more than one slice at a time.
   *
   * @param budget how many raw bytes of sealed content one entry may carry; {@code <= 0} disables slicing
   */
  // @VisibleForTesting
  static List<RaftLogEntryCodec.TsSealedChunk> sliceSealedBlob(final RaftLogEntryCodec.TsSealedBlob blob,
      final long budget, final String databaseName) {
    final SealedSlicePlan plan = planSealedSlices(blob, budget, budget, databaseName);
    if (!plan.sliced())
      return Collections.emptyList();

    final List<RaftLogEntryCodec.TsSealedChunk> slices = new ArrayList<>(plan.count());
    for (int i = 0; i < plan.count(); i++)
      slices.add(plan.slice(blob, i));
    return slices;
  }

  /** Schema-WAL instalments this database has shipped since it opened - see {@link #schemaInstalmentsShipped}. */
  public long getSchemaWalInstalmentsShipped() {
    return schemaInstalmentsShipped.get();
  }

  /** Milliseconds this database spent shipping schema-WAL instalments, write lock held throughout (issue #6144). */
  public long getSchemaWalInstalmentTotalTimeMs() {
    return schemaInstalmentTotalMs.get();
  }

  /** The longest single instalment this database has shipped, in milliseconds (issue #6144). */
  public long getSchemaWalInstalmentMaxTimeMs() {
    return schemaInstalmentMaxMs.get();
  }

  /**
   * Files this node holds that no schema component claims (issue #6143), memoized behind the gate described on
   * {@link UnreferencedFiles.MemoizedCount}: the walk runs only when a file or the schema has actually changed
   * since the last refresh.
   */
  public long getUnreferencedFilesCount() {
    return unreferencedFiles.get(proxied);
  }

  @Override
  public void recordTimeSeriesSealedChange(final String typeName, final int shardIndex, final String sealedFileName,
      final byte[] sealedBytes) {
    // Only meaningful while a runWithCompactionReplication session is active on this thread (it sets
    // isSchemaCommitThread); outside such a session there is nothing to drain the buffer, so ignore.
    if (!Boolean.TRUE.equals(isSchemaCommitThread.get()))
      return;
    compactionSealedBuffer.get().add(new RaftLogEntryCodec.TsSealedBlob(typeName, shardIndex, sealedFileName, sealedBytes));
  }

  @Override
  public boolean runWithCompactionReplication(final Callable<Boolean> compaction) throws IOException, InterruptedException {
    if (!isLeader()) {
      // Followers receive compacted state from the leader; running compaction independently
      // would create different file IDs and diverge from the leader's replication stream.
      return false;
    }

    if (!proxied.getFileManager().startRecordingChanges()) {
      // Another recordFileChanges/runWithCompactionReplication session is already active on this
      // node. Running the compaction now would either share the active session's recordedChanges
      // list (and lose its WAL pages, which are written outside the schema-commit-thread buffer)
      // or skip replication entirely - both produce leader/follower divergence: the leader's
      // mutable file gets renamed but followers either never see the new pages or never see the
      // new file at all. That is what surfaces later as the "Cannot find index ..." warning on
      // followers (issue #4063).
      //
      // Defer instead. Returning false bubbles up to LSMTreeIndex.compact(), which resets the
      // index status to AVAILABLE in its finally block; the next onAfterCommit will reschedule
      // the compaction once the contending recording session has released the file manager.
      HALog.log(this, HALog.DETAILED,
          "Skipping compaction for database '%s' because a recording session is in progress; will retry on next schedule",
          getName());
      return false;
    }

    // Mark this thread so commits executed inside the compaction (e.g. the TimeSeries Phase-4c
    // mutable-bucket clear) buffer their WAL into schemaWalBuffer instead of shipping a separate
    // TX_ENTRY. They then ride the SCHEMA_ENTRY below, atomically with any sealed-store blobs and
    // newly created files. Clear all buffers up-front so we only collect this session's entries.
    schemaWalBuffer.get().clear();
    schemaBucketDeltaBuffer.get().clear();
    compactionSealedBuffer.get().clear();
    isSchemaCommitThread.set(Boolean.TRUE);
    try {
      // #5443: remember how long every paginated file is BEFORE the compaction, so the pages it appends
      // to already-existing files can be shipped afterwards (see the loop below).
      final Map<Integer, Integer> pageCountsBefore = snapshotPageCounts();

      final boolean result = invokeCompaction(compaction);
      if (!result)
        return false;

      final Map<Integer, String> addFiles = new HashMap<>();
      final Map<Integer, String> removeFiles = new HashMap<>();
      final List<FileManager.FileChange> changes = proxied.getFileManager().getRecordedChanges();
      if (changes != null)
        for (final FileManager.FileChange change : changes) {
          if (change.create)
            addFiles.put(change.fileId, change.fileName);
          else
            removeFiles.put(change.fileId, change.fileName);
        }

      // Buffered WAL from commits that ran inside the compaction (e.g. the TimeSeries mutable-bucket
      // clear). These are correctly-versioned (positive txId) and apply over the existing follower
      // pages without a version gap. Each pairs index-aligned with its bucket-delta map.
      final List<byte[]> walEntries = new ArrayList<>(schemaWalBuffer.get());
      final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>(schemaBucketDeltaBuffer.get());

      // Synthetic WAL for genuinely new paginated files (e.g. LSM index compaction output).
      // txId=-1 on followers signals forceApply to bypass version-gap checks. The TimeSeries sealed
      // store is not a paginated file, so it contributes none here - it ships as a blob instead.
      // #4743: chunked against the maximum replicated entry size so a big compacted index does not
      // produce one oversized Raft entry (which would make the leader step down, over and over).
      final RaftTransactionBroker broker = requireRaftServer().getTransactionBroker();
      final long walChunkBudget = broker.walChunkBudget();
      for (final int fileId : addFiles.keySet())
        appendFilePagesAsWal(fileId, walChunkBudget, walEntries, bucketDeltas, 0);

      // #5443: a compaction does not only CREATE files - an incremental round APPENDS a new series to the
      // already-existing compacted file. Those pages are written eagerly and deliberately without WAL, and
      // the file is not in addFiles because it is not new, so nothing replicated them: the follower kept
      // the shorter file and every key in the appended series became unfindable there, silently, while the
      // records themselves replicated normally through their own transactions. Ship the appended range of
      // every pre-existing paginated component that grew during this session.
      for (final Map.Entry<Integer, Integer> grown : pagesGrownDuringSession(pageCountsBefore, addFiles).entrySet())
        appendFilePagesAsWal(grown.getKey(), walChunkBudget, walEntries, bucketDeltas, grown.getValue());

      final List<RaftLogEntryCodec.TsSealedBlob> recordedSealed = new ArrayList<>(compactionSealedBuffer.get());
      // Released here rather than only in the finally (#6933): the shipping loop below drops each sliced store's
      // image as it goes, and a second list still holding it would make that release do nothing. Nothing can add
      // to the buffer any more - the compaction callback has returned.
      compactionSealedBuffer.get().clear();

      if (addFiles.isEmpty() && removeFiles.isEmpty() && walEntries.isEmpty() && recordedSealed.isEmpty())
        return result;

      // #4416: a sealed store bigger than one Raft entry is shipped as an ordered sequence of slices instead of
      // being refused. Everything but the final slice of each store goes out HERE, ahead of the publishing entry,
      // as a delivery-only entry the follower merely stages; the final slice is handed to replicateSchema below
      // so the install lands in the same entry as the mutable-bucket clear WAL.
      //
      // WHAT A FAILURE PART-WAY THROUGH THIS LOOP LEAVES, since the loop makes several quorum round trips where
      // the whole-file path made one, and nothing here retries or rolls back. The compaction has already run
      // LOCALLY - the sealed file is swapped and the mutable-bucket clear is committed on this node, because
      // commit() inside the session buffers the WAL for shipping AFTER applying it here - so a throw leaves this
      // node sealed and the others still holding those samples in their (fully replicated) mutable bucket. Every
      // node still answers the same sample count; what diverges is where the samples live, and the page versions
      // this node advanced past theirs. The recovery is the one that already existed for a failed whole-file
      // compaction, not a new one: the next entry touching those pages hits a version gap on the followers and
      // escalates to a snapshot resync, and a follower's part-written staging file is truncated by the first
      // slice of the next sequence. Deliberately not retried here - a retry would re-ship a sealed image the next
      // compaction is about to rewrite anyway.
      // #6933: the whole session is planned BEFORE anything ships, because the publishing entry has to carry the
      // final slice of EVERY sliced store plus every whole blob, and a plan that cannot fit has to be refused
      // here rather than by the splitter after the delivery-only slices are already in the Raft log. The schema
      // JSON is serialized early for the same reason - its encoded size is part of what the sealed payload has
      // to leave room for.
      final JSONObject compactionSchema = proxied.getSchema().getEmbedded().toJSON();
      final String serializedSchema = compactionSchema.toString();
      final long sealedChunkBudget = GlobalConfiguration.replicatedSealedChunkBudget(proxied.getConfiguration());
      final List<SealedSlicePlan> sealedPlans = planSealedShipping(recordedSealed, sealedChunkBudget,
          publishingSealedCapacity(sealedChunkBudget, broker.maxEntrySize(),
              publishingHeaderSize(getName(), serializedSchema, addFiles, removeFiles)), getName());

      final List<RaftLogEntryCodec.TsSealedBlob> sealedBlobs = new ArrayList<>(recordedSealed.size());
      final List<RaftLogEntryCodec.TsSealedChunk> finalSealedChunks = new ArrayList<>();
      int sealedSlicesShipped = 0;
      for (int store = 0; store < recordedSealed.size(); store++) {
        final RaftLogEntryCodec.TsSealedBlob blob = recordedSealed.get(store);
        final SealedSlicePlan plan = sealedPlans.get(store);
        if (!plan.sliced()) {
          sealedBlobs.add(blob);
          continue;
        }
        // One slice materialized at a time (#6933): the source image is already one whole-file array on this
        // thread, and cutting the sequence up front made it two.
        for (int i = 0; i < plan.count() - 1; i++)
          broker.replicateSealedChunk(getName(), plan.slice(blob, i));
        finalSealedChunks.add(plan.slice(blob, plan.count() - 1));
        sealedSlicesShipped += plan.count();
        // Nothing needs this store's image any more - only its final slice publishes - so let an N-shard session
        // stop holding N whole-file arrays at once.
        recordedSealed.set(store, null);
        // Per-STORE, not per-session: the burst that costs latency is one store's sequence of round trips, and
        // summing across stores would hide a single pathological shard behind a busy-but-healthy cycle.
        sealedChunksMaxSequence.accumulateAndGet(plan.count(), Math::max);
      }
      sealedChunksShipped.addAndGet(sealedSlicesShipped);

      broker.replicateSchema(getName(), serializedSchema, addFiles, removeFiles, walEntries, bucketDeltas,
          sealedBlobs, finalSealedChunks);

      // A compaction entry always carries the whole document - its size is what the sealed payload was budgeted
      // against, so there is nothing to gain from a delta here - but it still MOVES the followers, so the base a
      // later delta is diffed against has to advance with it (issue #6989).
      rememberReplicatedSchema(compactionSchema, serializedSchema.length());

      HALog.log(this, HALog.DETAILED,
          "Compaction for database '%s' replicated via Raft: addFiles=%d, removeFiles=%d, walEntries=%d, "
              + "sealedBlobs=%d, sealedSlices=%d",
          getName(), addFiles.size(), removeFiles.size(), walEntries.size(), sealedBlobs.size(), sealedSlicesShipped);

      if (HALog.isEnabled(HALog.DETAILED))
        logSchemaPayloadDiagnostics("compaction", serializedSchema, addFiles, removeFiles);

      return result;
    } finally {
      // A bare remove() is enough here, unlike recordFileChanges: a compaction runs on its own scheduler
      // thread and defers (returns false above) whenever a session is already open, so it never nests
      // inside another session whose mark it could clear.
      isSchemaCommitThread.remove();
      schemaWalBuffer.get().clear();
      schemaBucketDeltaBuffer.get().clear();
      compactionSealedBuffer.get().clear();
      proxied.getFileManager().stopRecordingChanges();
    }
  }

  private static boolean invokeCompaction(final Callable<Boolean> compaction) throws IOException, InterruptedException {
    try {
      return compaction.call();
    } catch (final IOException | InterruptedException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new IOException("Compaction failed", e);
    }
  }

  /**
   * Polls until the leader's FileManager recording session ends. Bounded by
   * {@link GlobalConfiguration#HA_QUORUM_TIMEOUT} so we never deadlock if a recorder thread
   * crashed without releasing the session; on timeout we proceed with the original ordering
   * race rather than locking up writes indefinitely.
   * <p>
   * Note: the TimeSeries compaction/append deadlock (issue #4458) is fixed at the source -
   * {@code TimeSeriesShard.appendSamples} no longer holds the compaction read lock while calling
   * this method, so Phase 4c can complete and end the recording session promptly. This timeout
   * remains as a defensive safety net for any other recorder thread that crashes mid-session; it
   * is intentionally retained, not dead code.
   */
  private void waitForActiveRecordingSession() {
    if (proxied.getFileManager().getRecordedChanges() == null)
      return;
    final long timeout = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
    final long deadline = System.currentTimeMillis() + timeout;
    while (proxied.getFileManager().getRecordedChanges() != null) {
      if (System.currentTimeMillis() >= deadline) {
        HALog.log(this, HALog.BASIC,
            "commit waited %dms for FileManager recording session and gave up; proceeding with TX_ENTRY",
            timeout);
        return;
      }
      try {
        Thread.sleep(2);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private ServerIsNotTheLeaderException schemaChangesNeedTheLeader() {
    final String leaderAddr = raftHAServer != null ? raftHAServer.getLeaderHttpAddress() : null;
    return new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server",
        leaderAddr != null ? leaderAddr : "");
  }

  /**
   * Claims the file-manager recording session so this thread's schema change can be captured and shipped
   * as a {@code SCHEMA_ENTRY}, waiting for a session owned by another thread to be released first. In
   * practice that owner is an LSM compaction inside {@link #runWithCompactionReplication}: a second
   * schema change normally never gets this far, because the DDL entry points that take the database write
   * lock around the whole operation (e.g. {@code TypeBuilder.create}) serialize on it first.
   * <p>
   * The session used to be a single per-database slot with no owner, so a contended caller fell back to
   * running the DDL straight on the inner database: applied on the leader, proposed to nobody, nothing
   * thrown (#5728). The window it lost to is real work, not an instant - the owning session releases the
   * database write lock when its callback returns but keeps the session until its {@code replicateSchema()}
   * Raft round trip completes, which is why the divergence only appeared under load. Waiting is the only
   * correct answer here: unlike a compaction, a schema change cannot be deferred and rescheduled.
   * <p>
   * <b>The wait can run while the caller holds the database write lock</b>, since {@code TypeBuilder.create}
   * wraps the whole operation in {@code executeInWriteLock}. A contended schema change therefore stalls
   * writers on that database for as long as it polls. That is a deliberate trade: an availability pause
   * bounded by {@link GlobalConfiguration#HA_QUORUM_TIMEOUT}, in place of a leader that silently stops
   * replicating its schema. The bound is also what keeps the stall from becoming permanent if the session's
   * owner ever needs a lock this caller is holding - it gives up and throws rather than waiting forever.
   *
   * @throws TimeoutException if the session could not be claimed, so the caller fails loudly instead of
   *                          diverging. Interruption raises the same type - both mean "this schema change
   *                          never claimed the session", and callers already treat {@code TimeoutException}
   *                          as the non-retryable give-up signal - but says so in its message rather than
   *                          reporting an elapsed timeout that never elapsed.
   */
  private void acquireRecordingSession() {
    if (proxied.getFileManager().startRecordingChanges())
      return;

    final long timeout = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
    final long started = System.currentTimeMillis();
    final long deadline = started + timeout;
    while (System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(2);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new TimeoutException("Interrupted while waiting for the file recording session to replicate a schema change on database '"
            + getName() + "'");
      }
      if (proxied.getFileManager().startRecordingChanges()) {
        // Logged because this wait can block writers on the database: an operator seeing writes pause
        // should find the reason here rather than having to infer it.
        HALog.log(this, HALog.BASIC,
            "Schema change on database '%s' waited %dms for the file recording session held by another thread",
            getName(), System.currentTimeMillis() - started);
        return;
      }
    }
    throw new TimeoutException("Timeout of " + timeout
        + "ms waiting for the file recording session to replicate a schema change on database '" + getName() + "'");
  }

  /**
   * The schema change as a DELTA against what the followers already hold (issue #6989), or null when the whole
   * document has to go out instead.
   * <p>
   * Null is returned - and the whole document shipped - whenever any of these is true:
   * <ul>
   *   <li>{@code arcadedb.ha.schemaDelta} is off, or some peer in the Raft configuration has not proved it can
   *       decode a delta - unknown, unreachable, stale, or running a build that predates the section (issue
   *       #7219). See {@link #schemaDeltaEnabled()}; both halves of that answer land in this one arm.</li>
   *   <li>Nothing has been shipped yet in this instance's lifetime, so there is no base to diff against.</li>
   *   <li>The Raft term moved since the cache was filled, so another node has been leader in between and what
   *       the followers hold is whatever IT shipped - see {@link #lastReplicatedSchema}. An unknown term
   *       ({@code -1}, which is what a division being restarted in place reports) counts as moved.</li>
   *   <li>The delta is not meaningfully smaller than the document - a bulk drop, a settings change that
   *       rewrites everything - in which case shipping it would cost the size of the document plus the delta's
   *       own key sets.</li>
   * </ul>
   */
  private SchemaDelta.Payload schemaDeltaFor(final JSONObject fullSchema) {
    final JSONObject base = lastReplicatedSchema;
    final long term = currentRaftTerm();

    if (!baseIsUsable(schemaDeltaEnabled(), base != null, lastReplicatedSchemaTerm, term)) {
      if (base != null)
        HALog.log(this, HALog.DETAILED,
            "Shipping the whole schema for database '%s': the cached base was shipped in term %d, this node is "
                + "in term %d",
            getName(), lastReplicatedSchemaTerm, term);
      return null;
    }

    final String deltaJson = SchemaDelta.compute(base, fullSchema).toString();
    if (!deltaIsWorthShipping(deltaJson.length(), lastFullSchemaLength))
      return null;

    return new SchemaDelta.Payload(base.getLong(SchemaDelta.SCHEMA_VERSION, -1L), deltaJson);
  }

  /**
   * Whether the cached base can be diffed against at all: the setting is on, a base exists, and the Raft term
   * has not moved since it was cached.
   * <p>
   * Static and side-effect-free so the arms can be pinned without a cluster, the way
   * {@link #partitionAbandonedFiles} is: forcing a re-election in an integration test to reach the term arm is
   * expensive and flaky, and this is the branch whose failure mode is silent - it does not break replication,
   * it just quietly stops the feature from ever engaging, which is exactly how the two defects the integration
   * test caught behaved.
   *
   * @param currentTerm the term this node is in, or {@code -1} when it cannot be read (a division restarting in
   *                    place). Unknown counts as moved: we cannot show the cache is still valid.
   */
  // @VisibleForTesting
  static boolean baseIsUsable(final boolean enabled, final boolean hasBase, final long cachedTerm,
      final long currentTerm) {
    return enabled && hasBase && currentTerm >= 0 && currentTerm == cachedTerm;
  }

  /**
   * Whether a delta is small enough to be worth shipping in place of the document.
   * <p>
   * Halving is the bar because the Raft entry is not the only cost a delta avoids: the follower still parses and
   * rewrites whatever arrives, so a "delta" worth most of the document buys nothing and costs the authoritative
   * key sets on top.
   * <p>
   * The yardstick is deliberately loose. {@code lastFullSchemaLength} is refreshed only when a WHOLE document
   * ships, so on a schema that keeps growing through delta-only entries it drifts below the real document size
   * and the bar tightens. That errs the safe way - the worst case is one extra whole-document entry, which
   * immediately re-primes the yardstick - and tracking the true size would mean rendering the document on every
   * DDL, which is the cost this whole path exists to avoid.
   *
   * @param lastFullSchemaLength the length of the last whole document shipped, or {@code 0} when none has been
   *                             (nothing to compare against, so the delta is taken)
   */
  // @VisibleForTesting
  static boolean deltaIsWorthShipping(final int deltaLength, final int lastFullSchemaLength) {
    return lastFullSchemaLength <= 0 || deltaLength * 2L < lastFullSchemaLength;
  }

  /**
   * Whether this cluster may ship schema deltas right now: the setting allows it AND every peer in the Raft
   * configuration has proved it can decode one (issue #7219).
   * <p>
   * The setting is read off the SERVER's configuration, like every other {@code SCOPE.SERVER} setting this class
   * consults ({@code HA_QUORUM_TIMEOUT}, {@code HA_FORWARD_LEADER_WAIT_TIMEOUT_MS}): the per-database
   * {@code ContextConfiguration} does not carry what a server-scoped setting was set to, so reading it there
   * silently answers "default".
   * <p>
   * <b>The capability half is what makes the setting safe to default to on.</b> A delta rides an optional trailing
   * section of {@code SCHEMA_ENTRY}; a node predating that section stops decoding before it, sees an entry with an
   * empty {@code schemaJson}, applies nothing and diverges with no error. Until #7219 nothing could tell the leader
   * that such a peer was in the cluster, so the setting carried an operator instruction ("upgrade every node
   * first") that nothing enforced. Now the leader asks - {@link RaftHAServer#peersMissingCapability} - and any peer
   * that is unknown, unreachable, stale or explicitly without the capability sends the whole document instead.
   * <p>
   * Read on every schema change rather than cached, so a peer that stops answering stops receiving deltas from the
   * next DDL on, and one that finishes upgrading starts receiving them without a leader restart.
   */
  private boolean schemaDeltaEnabled() {
    if (server == null || !server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_SCHEMA_DELTA))
      return false;

    final RaftHAServer raft = raftHAServer;
    if (raft == null)
      // No Raft server to ask, so nothing has proved it can decode a delta. The same answer the capability
      // registry gives for an unknown peer, for the same reason.
      return false;

    final List<String> missing = raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA);
    if (missing.isEmpty())
      return true;

    logSchemaDeltaWithheld(missing);
    return false;
  }

  /**
   * Reports, at most once per {@link #SCHEMA_DELTA_WITHHELD_LOG_THROTTLE_MS} per database, that deltas are
   * configured but withheld and WHICH peers withheld them.
   * <p>
   * The whole failure mode this replaces was silent, and a mechanism that silently declines to engage is only
   * marginally better than one that silently diverges: an operator who has turned the setting on and sees no
   * change in entry sizes needs to be told that peer {@code arcadedb2} has not answered, not left to infer it.
   * Throttled because the alternative is a line per DDL, and a bulk migration is thousands of them.
   */
  private void logSchemaDeltaWithheld(final List<String> peersMissingTheCapability) {
    final long now = System.currentTimeMillis();
    final long last = lastSchemaDeltaWithheldLog;
    if (now - last < SCHEMA_DELTA_WITHHELD_LOG_THROTTLE_MS)
      return;
    // Racy by design: two DDL threads crossing the window at the same instant cost one duplicate line, which is
    // a far better trade than a lock on the schema-replication path.
    lastSchemaDeltaWithheldLog = now;
    LogManager.instance().log(this, Level.INFO,
        "Schema changes on database '%s' are shipping as whole documents even though %s is on: peer(s) %s have not "
            + "advertised the '%s' capability, so a delta could not be decoded there (issue #7219). Upgrade or "
            + "restore contact with those peers and deltas resume by themselves.",
        getName(), GlobalConfiguration.HA_SCHEMA_DELTA.getKey(), peersMissingTheCapability,
        PeerCapabilities.SCHEMA_DELTA);
  }

  /** The Raft term this node is in, or {@code -1} when it cannot be read (no server, division restarting). */
  private long currentRaftTerm() {
    final RaftHAServer raft = raftHAServer;
    return raft != null ? raft.getCurrentTerm() : -1L;
  }

  /**
   * Records the schema document the followers now hold, so the NEXT change can be diffed against it (issue
   * #6989). Call only once the entry carrying it has actually been replicated.
   *
   * <p>
   * <b>The three fields are written separately, not swapped atomically</b>, and what makes that safe is that both
   * callers - {@code recordFileChanges} and {@code runWithCompactionReplication} - hold this database's file
   * RECORDING SESSION, which is exclusive per database (compaction defers rather than nesting when a session is
   * already open). That invariant is inherited from a lock taken much earlier in the call chain and nothing at
   * this call site enforces it, so it is checked here rather than only asserted in prose: whoever changes the
   * recording-session mechanism gets a log line rather than a silently torn cache. Warn-only deliberately - a
   * torn base costs one extra whole-document entry, and failing replication over a diagnostic would be a far
   * worse trade.
   *
   * @param fullSchemaLength the length of the whole document when that is what was shipped, or {@code -1} when a
   *                         delta was shipped and the last known whole-document length therefore still stands as
   *                         the yardstick {@link #schemaDeltaFor} sizes the next delta against
   */
  private void rememberReplicatedSchema(final JSONObject fullSchema, final int fullSchemaLength) {
    if (!proxied.getFileManager().isRecordingChangesOnCurrentThread())
      LogManager.instance().log(this, Level.WARNING,
          "Schema delta base for database '%s' updated outside a file recording session; the session is what "
              + "serializes the two writers, so this cache can now be torn (issue #6989)", getName());

    // Only retained when deltas can actually ship: the document is the size of the schema, and a cluster that
    // cannot use it would otherwise pay several MB per replicated database for a base nothing will ever diff
    // against. Deliberately the SAME predicate the emission gate uses (issue #7219), so a cluster holding a base
    // is exactly a cluster that could ship a delta from it - a peer that has not advertised the capability
    // releases the base too, rather than leaving the leader paying for a diff it may not send.
    //
    // The cost of the predicate flipping back to true - the setting turned on at runtime, or the last peer
    // finishing its upgrade - is one more whole-document entry, because the base is null until then, which is
    // already one of the fallback arms.
    final boolean wanted = schemaDeltaEnabled();
    lastReplicatedSchema = wanted ? fullSchema : null;
    lastReplicatedSchemaTerm = wanted ? currentRaftTerm() : -1L;
    if (fullSchemaLength >= 0) {
      lastFullSchemaLength = fullSchemaLength;
      schemaDocumentsShipped.incrementAndGet();
    } else
      schemaDeltasShipped.incrementAndGet();
  }

  /** Schema changes this database has shipped as a delta since it opened - see {@link #schemaDeltasShipped}. */
  public long getSchemaDeltasShipped() {
    return schemaDeltasShipped.get();
  }

  /** Schema changes this database has shipped as a whole document since it opened. */
  public long getSchemaDocumentsShipped() {
    return schemaDocumentsShipped.get();
  }

  /**
   * Cross-references a {@code SCHEMA_ENTRY} payload to highlight when the in-memory schema JSON
   * being shipped names indexes whose backing file is NOT in {@code addFiles}. Such names are the
   * symptom mdre observed as "Cannot find indexes [...]" warnings on followers (issue #4083) -
   * with this trace the leader's outbound payload becomes inspectable so we can pin the
   * divergence to (a) wrong file naming captured during the recording session, (b) schema
   * mutations made outside the session, or (c) a missing addFiles entry from a prior compaction
   * that lingered in the in-memory schema.
   */
  private void logSchemaPayloadDiagnostics(final String origin, final String schemaJson,
      final Map<Integer, String> addFiles, final Map<Integer, String> removeFiles) {
    HALog.log(this, HALog.DETAILED, "[%s] addFiles=%s", origin, addFiles);
    HALog.log(this, HALog.DETAILED, "[%s] removeFiles=%s", origin, removeFiles);

    if (schemaJson == null || schemaJson.isEmpty())
      return;

    try {
      final JSONObject root = new JSONObject(schemaJson);
      final JSONObject types = root.has("types") ? root.getJSONObject("types") : null;
      if (types == null)
        return;

      // Build a set of index names (component name level) that are accounted for by the addFiles
      // entries shipped in this SCHEMA_ENTRY. addFiles values are full file names like
      // "BulkRace_0_<nanos>.13.65536.v0.umtidx"; the schema "indexes" key strips the trailing
      // dot-separated tail so we compare on a stable prefix.
      final Set<String> shippedIndexNames = new HashSet<>();
      for (final String fullName : addFiles.values()) {
        final int firstDot = fullName.indexOf('.');
        shippedIndexNames.add(firstDot > 0 ? fullName.substring(0, firstDot) : fullName);
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
              "[%s] schemaJson.types.%s.indexes['%s'] %s",
              origin, typeName, idxName, shipped ? "= matched in addFiles" : "= NOT in addFiles (orphan candidate)");
        }
      }
    } catch (final RuntimeException e) {
      HALog.log(this, HALog.DETAILED,
          "[%s] schema JSON parse failed for diagnostics: %s", origin, e.getMessage());
    }
  }

  /**
   * Page count of every registered paginated component, keyed by file id. Taken before a compaction so
   * {@link #pagesGrownDuringSession} can tell which pre-existing files it appended to (issue #5443).
   */
  private Map<Integer, Integer> snapshotPageCounts() {
    final Map<Integer, Integer> counts = new HashMap<>();
    for (final ComponentFile componentFile : proxied.getFileManager().getFiles()) {
      if (componentFile == null)
        continue;
      final int fileId = componentFile.getFileId();
      // getFileByIdIfExists(), not getFileById(): the latter THROWS on an id the schema does not know,
      // and this walks every file the FileManager holds - a dropped file leaves a null slot in the
      // schema's list, so one of those would abort the whole compaction replication instead of being
      // skipped, which is what the type test below reads as if it were doing.
      if (proxied.getSchema().getEmbedded().getFileByIdIfExists(fileId) instanceof PaginatedComponent component)
        counts.put(fileId, component.getTotalPages());
    }
    return counts;
  }

  /**
   * File ids of pre-existing paginated components that gained pages during the compaction session, mapped
   * to the page number the new pages start at. Files created by the session are excluded: they are shipped
   * whole. A file that shrank (or is new) contributes nothing.
   * <p>
   * <b>Invariant this relies on:</b> a compaction only ever APPENDS to a pre-existing file, and touches no
   * interior page other than the root page 0 that registers the appended series - which is why the caller
   * ships page 0 alongside the appended range. A compaction path that rewrote an interior page in place
   * would not be replicated by this, and the follower's index would go short again exactly as in #5443.
   * Any change to the compaction write path has to preserve that, or teach this method to track the pages
   * it dirtied instead of only how many it added.
   */
  private Map<Integer, Integer> pagesGrownDuringSession(final Map<Integer, Integer> before,
      final Map<Integer, String> createdFiles) {
    final Map<Integer, Integer> grown = new HashMap<>();
    for (final Map.Entry<Integer, Integer> entry : snapshotPageCounts().entrySet()) {
      final int fileId = entry.getKey();
      if (createdFiles.containsKey(fileId))
        continue;
      final Integer pagesBefore = before.get(fileId);
      if (pagesBefore != null && entry.getValue() > pagesBefore)
        grown.put(fileId, pagesBefore);
    }
    return grown;
  }

  /**
   * Serializes every page of a newly created paginated file (e.g. an LSM index compaction output) as
   * synthetic WAL, split into as many self-contained WAL transactions as needed to keep each one
   * within {@code maxChunkBytes}.
   * <p>
   * Issue #4743: this used to produce ONE WAL entry covering the whole file. Because the whole file
   * then rode a single {@code SCHEMA_ENTRY}, the Raft entry size grew with the index: a 517k-key
   * index produced a 21.5MB entry, and Ratis rejects a single log entry above
   * {@code arcadedb.ha.appendBufferSize} (4MB by default) with a {@code StateMachineException} that
   * makes the LEADER STEP DOWN - so a big-enough index made compaction topple every elected leader
   * in turn and the cluster never recovered. It also allocated the entire file on the heap at once.
   * <p>
   * Each chunk covers a contiguous page range and carries {@code txId=-1}, which makes followers
   * {@code forceApply} it without a version-gap check, so the chunks are order-independent among
   * themselves and individually idempotent on replay.
   *
   * @param maxChunkBytes soft cap for one chunk; always at least one page per chunk, so a single
   *                      page bigger than the cap still ships (and is caught by the group committer's
   *                      hard pre-check with an actionable error)
   *
   * @return number of chunks appended to {@code walOut}
   */
  private int appendFilePagesAsWal(final int fileId, final long maxChunkBytes, final List<byte[]> walOut,
      final List<Map<Integer, Integer>> bucketDeltasOut, final int fromPage) throws IOException {
    final PaginatedComponentFile file = (PaginatedComponentFile) proxied.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();

    // #5443: the page COUNT and the page CONTENT must both come from the page manager, never from the
    // file on disk. The count used below is PaginatedComponent.getTotalPages(), which is bumped synchronously at
    // commit and so includes pages that are published but not yet written; the FILE's own
    // PaginatedComponentFile.getTotalPages() advances only once a physical write has landed, so it only sees
    // what the asynchronous writer has already persisted: a compaction that has just published 13 pages
    // can still measure 10 on disk. Serializing from the file then shipped a TRUNCATED index - the
    // follower's compacted sub-index came out three pages short, and every key that lived in them became
    // unfindable there while the records themselves replicated normally. The component knows the real
    // count, and getImmutablePage() resolves each page through the cache and the pending-flush queue
    // before falling back to disk, so both are correct regardless of flush timing.
    // Only the component knows how many pages really exist: the file's own count is what is on disk, and
    // trusting it is exactly what left followers with a short index. No caller can hand us anything else -
    // the ids come from addFiles or from snapshotPageCounts(), which already filtered on PaginatedComponent -
    // so a miss here is a bug in the caller, and it fails rather than quietly shipping a truncated range.
    if (!(proxied.getSchema().getEmbedded().getFileByIdIfExists(fileId) instanceof PaginatedComponent component))
      throw new IllegalStateException(
          "Cannot replicate file id '" + fileId + "' of database '" + getName()
              + "': it is not a registered paginated component, so its real page count is unknown");

    final int totalPages = component.getTotalPages();

    if (totalPages <= fromPage)
      return 0;

    final int perPageWalSize = walPerPageSize(pageSize);

    // The root page carries the series registry: an appended series is invisible until page 0 says it
    // exists, so a partial ship must always include it. Without this the follower stores the new pages
    // and still reports the old entry count - which is exactly how issue #5443 stayed silent.
    if (fromPage > 0)
      appendPageRangeAsWal(fileId, pageSize, 0, 1, walOut, bucketDeltasOut);

    final long budget = maxChunkBytes - WAL_CHUNK_FRAMING_SIZE;
    final int pagesToShip = totalPages - fromPage;
    final int pagesPerChunk = budget >= perPageWalSize ? (int) Math.min(pagesToShip, budget / perPageWalSize) : 1;

    int chunks = 0;
    for (int firstPage = fromPage; firstPage < totalPages; firstPage += pagesPerChunk) {
      appendPageRangeAsWal(fileId, pageSize, firstPage, Math.min(pagesPerChunk, totalPages - firstPage),
          walOut, bucketDeltasOut);
      chunks++;
    }

    return chunks;
  }

  /**
   * Framing a WAL chunk carries: txId, timestamp, page count, segment size, then the trailing segment
   * size and magic number. Both the chunking maths and the buffer that gets written have to agree on
   * these three, so neither computes them for itself.
   */
  private static final int WAL_CHUNK_FRAMING_SIZE = 2 * Long.BYTES + 3 * Integer.BYTES + Long.BYTES;

  /** Bytes of a page that travel in the WAL: everything after the page header. */
  private static int walPageDeltaSize(final int pageSize) {
    return pageSize - BasePage.PAGE_HEADER_SIZE;
  }

  /** Per-page WAL cost: the six ints of the page record, plus the delta itself. */
  private static int walPerPageSize(final int pageSize) {
    return 6 * Integer.BYTES + walPageDeltaSize(pageSize);
  }

  /**
   * Serializes {@code pageCount} pages starting at {@code firstPage} as one synthetic WAL transaction.
   * <p>
   * Both the page count and the content come from the page manager, never from the file: the count is
   * {@code PaginatedComponent.getTotalPages()}, which includes pages pending persistence, where the file's own
   * {@code PaginatedComponentFile.getTotalPages()} advances only once a physical write has landed - and reading the
   * file directly can serialize a page that has not been written yet.
   */
  private void appendPageRangeAsWal(final int fileId, final int pageSize, final int firstPage,
      final int pageCount, final List<byte[]> walOut, final List<Map<Integer, Integer>> bucketDeltasOut)
      throws IOException {
    final int deltaSize = walPageDeltaSize(pageSize);
    final int segmentSize = pageCount * walPerPageSize(pageSize);

    final ByteBuffer walBuf = ByteBuffer.allocate(WAL_CHUNK_FRAMING_SIZE + segmentSize);
    walBuf.putLong(-1L); // txId=-1 → forceApply on followers
    walBuf.putLong(System.currentTimeMillis());
    walBuf.putInt(pageCount);
    walBuf.putInt(segmentSize);

    for (int pageNum = firstPage; pageNum < firstPage + pageCount; pageNum++) {
      final BasePage page = proxied.getPageManager()
          .getImmutablePage(new PageId(proxied, fileId, pageNum), pageSize, false, true);

      walBuf.putInt(fileId);
      walBuf.putInt(pageNum);
      walBuf.putInt(BasePage.PAGE_HEADER_SIZE);  // changesFrom = 8
      walBuf.putInt(pageSize - 1);               // changesTo
      walBuf.putInt((int) page.getVersion());
      walBuf.putInt(page.getContentSize());

      // Absolute bulk copy: it leaves both positions alone, so the shared page buffer is never
      // disturbed, and it does not care whether either side is heap-backed.
      walBuf.put(walBuf.position(), page.getContent(), BasePage.PAGE_HEADER_SIZE, deltaSize);
      walBuf.position(walBuf.position() + deltaSize);
    }

    walBuf.putInt(segmentSize);
    walBuf.putLong(WALFile.MAGIC_NUMBER);

    walOut.add(walBuf.array());
    bucketDeltasOut.add(Collections.emptyMap());
  }

  @Override
  public void saveConfiguration() throws IOException {
    proxied.saveConfiguration();
  }

  @Override
  public long getLastUpdatedOn() {
    return proxied.getLastUpdatedOn();
  }

  @Override
  public long getLastUsedOn() {
    return proxied.getLastUsedOn();
  }

  @Override
  public long getOpenedOn() {
    return proxied.getOpenedOn();
  }

  @Override
  public Map<String, Object> alignToReplicas() {
    return proxied.alignToReplicas();
  }

  @Override
  public SecurityManager getSecurity() {
    return proxied.getSecurity();
  }

  @Override
  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  @Override
  public String getLeaderHttpAddress() {
    return raftHAServer != null ? raftHAServer.getLeaderHttpAddress() : null;
  }

  @Override
  public HAServerPlugin.QUORUM getQuorum() {
    // Raft consensus is inherently majority-based
    return HAServerPlugin.QUORUM.MAJORITY;
  }

  @Override
  public void createInReplicas() {
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getTransactionBroker().replicateInstallDatabase(getName(), false);
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending install-database entry via Raft for database '" + getName() + "'", e);
    }
    LogManager.instance().log(this, Level.INFO, "Database '%s' install-database entry committed via Raft", getName());
  }

  @Override
  public void createInReplicas(final boolean forceSnapshot) {
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getTransactionBroker().replicateInstallDatabase(getName(), forceSnapshot);
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending install-database entry via Raft for database '" + getName() + "'", e);
    }
    LogManager.instance()
        .log(this, Level.INFO, "Database '%s' install-database (forceSnapshot=%s) entry committed via Raft", getName(),
            forceSnapshot);
  }

  @Override
  public void dropInReplicas() {
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getTransactionBroker().replicateDropDatabase(getName());
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending drop-database entry via Raft for database '" + getName() + "'", e);
    }
    LogManager.instance().log(this, Level.INFO, "Database '%s' drop-database entry committed via Raft", getName());
  }

  /**
   * Forwards a DDL or leader-only command to the Raft leader via HTTP POST to
   * {@code /api/v1/command/{dbName}}. The response JSON is parsed back into a
   * {@link ResultSet} so the caller sees results transparently.
   */
  private ResultSet forwardCommandToLeaderViaRaft(final String language, final String query,
      final Map<String, Object> mapArgs, final Object[] positionalArgs) {
    final RaftHAServer raft = requireRaftServer();

    // This request is already the result of a peer redirecting it to what it believed was the leader, and it
    // arrived on a node that is not the leader either. Redirecting it once more sends it round the cycle the
    // wrong address created; refuse instead, so the peer that forwarded it (and through it the client) gets a
    // typed, retryable answer in one hop rather than a hang (issue #6191).
    //
    // Deliberately before the leader-address wait below, unlike the self-address check that follows it: waiting
    // would only make sense if this node might forward the request onward once a leader appears, and it must
    // not. A leadership change in flight and an address that names the wrong node are indistinguishable from
    // here, so the caller retries - which re-resolves the leader from scratch - rather than this node posting a
    // non-idempotent write a second time on a path that cannot prove the first one did not execute.
    if (LeaderForwardContext.isAlreadyForwarded()) {
      // Said once in this node's own log too: the refusal travels back to the peer that forwarded the write
      // and from there to the client, so without this line the only node that can name the misconfiguration -
      // the one that proved the address wrong by receiving the request - says nothing about it anywhere.
      if (forwardedAgainWarned.compareAndSet(false, true))
        LogManager.instance().log(this, Level.WARNING,
            "A cluster peer forwarded a write to this node as the leader, but this node is not the leader (db=%s). "
                + "That peer resolved an HTTP address for the leader which does not identify it - unless leadership "
                + "just moved, declare every node's HTTP port explicitly with the 'host:raftPort:httpPort' syntax in "
                + "%s. The write is refused rather than forwarded on. This notice is logged only once per database.",
            getName(), GlobalConfiguration.HA_SERVER_LIST.getKey());
      throw new ServerIsNotTheLeaderException(
          "Refusing to forward a write that a cluster peer already forwarded to the leader: it arrived on this node, "
              + "which is not the leader. Either leadership moved while the request was in flight - retry - or the "
              + "HTTP address that peer resolved for the leader does not identify it, which is what declaring every "
              + "node's HTTP port ('host:raftPort:httpPort') in " + GlobalConfiguration.HA_SERVER_LIST.getKey()
              + " prevents", raft.getLeaderName());
    }

    // During cluster startup or a leader change there is a window with no elected leader. Rather than failing
    // the forwarded write immediately (which loses the caller's transaction - issue #4728 follow-up), wait a
    // bounded time for a leader to appear and forward as soon as one does. If this node becomes the leader
    // while waiting, getLeaderHttpAddress() returns its own address and the POST to self executes locally.
    final long leaderWaitMs = server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_FORWARD_LEADER_WAIT_TIMEOUT_MS);
    final String leaderHttpAddress = awaitLeaderAddress(raft::getLeaderHttpAddress, leaderWaitMs, LEADER_WAIT_POLL_INTERVAL_MS);
    if (leaderHttpAddress == null)
      throw new TransactionException("Cannot forward command to leader: leader HTTP address is not available "
          + "(no leader elected within " + leaderWaitMs + "ms; tune " + GlobalConfiguration.HA_FORWARD_LEADER_WAIT_TIMEOUT_MS.getKey() + ")");

    // The address resolved for the leader is this node's own, and this node is not the leader: the POST would
    // come back here, be forwarded again, and consume one more HTTP worker thread per hop. This is what the
    // derive fallback produces when the peers share a host and no 'http' port is declared - it pairs the
    // leader's Raft host with THIS node's HTTP port. Refuse with the typed error the HTTP and gRPC layers
    // already know how to report (issue #6191). The one case where the self-address is right - this node
    // became the leader while waiting above - is left alone: that POST executes locally and terminates.
    if (!raft.isLeader() && raft.isOwnHttpAddress(leaderHttpAddress)) {
      if (selfForwardWarned.compareAndSet(false, true))
        LogManager.instance().log(this, Level.WARNING,
            "The HTTP address resolved for the leader (%s) is this node's own, so a write forwarded to it would come "
                + "back here and be forwarded again. Writes issued on this node are refused until the cluster can tell "
                + "its peers' HTTP endpoints apart: declare every node's HTTP port explicitly with the "
                + "'host:raftPort:httpPort' syntax in %s. This notice is logged only once per database.",
            leaderHttpAddress, GlobalConfiguration.HA_SERVER_LIST.getKey());
      throw new ServerIsNotTheLeaderException(
          "Cannot forward the command: the HTTP address resolved for the leader (" + leaderHttpAddress
              + ") is this node's own, and this node is not the leader. Declare every node's HTTP port explicitly with "
              + "the 'host:raftPort:httpPort' syntax in " + GlobalConfiguration.HA_SERVER_LIST.getKey(),
          raft.getLeaderName());
    }

    final JSONObject body = new JSONObject();
    body.put("language", language);
    body.put("command", query);
    if (mapArgs != null && !mapArgs.isEmpty())
      body.put("params", new JSONObject(mapArgs));
    else if (positionalArgs != null && positionalArgs.length > 0) {
      // Use ordinal-map format {"0": v0, "1": v1, ...} so the leader's PostCommandHandler
      // can safely parse params as a Map regardless of the toMap(true) numeric-array optimization.
      // Sending a plain JSON array like [110] causes toMap(true) to return a primitive array
      // (long[] for integer-only, float[] for fractional - see issues #3864 and #4148), which
      // then cannot be cast to Map at the params-extraction site.
      final JSONObject ordinalParams = new JSONObject();
      for (int i = 0; i < positionalArgs.length; i++)
        ordinalParams.put("" + i, positionalArgs[i]);
      body.put("params", ordinalParams);
    }

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://" + leaderHttpAddress + "/api/v1/command/" + getName()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()));

    final String clusterToken = raftHAServer.getClusterToken();
    if (clusterToken != null && !clusterToken.isBlank()) {
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
      // One hop, and the receiving node knows it: if this address does not identify the leader, the node it
      // does reach refuses the command instead of resolving the same wrong address and forwarding it again
      // (issue #6191). Sent only with the token, because that is the only form in which a receiving node
      // trusts the marker - same pairing as PostServerCommandHandler's forward.
      builder.header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
    }

    String proxiedUser = proxied.getCurrentUserName();
    if (proxiedUser == null || proxiedUser.isBlank()) {
      // No per-thread security context: this is an in-process, embedded call (e.g. an embedded
      // scripting engine or an application background/worker thread) that bypassed the HTTP layer
      // where the request user is normally bound to the thread-local DatabaseContext. Such callers
      // are fully trusted: locally a null security context already grants full access (permission
      // checks are skipped), so we represent the operation to the leader as the root user. The
      // forwarding itself remains authenticated server-to-server via the shared cluster token, so
      // this is not a remote privilege-escalation vector. Mirrors BootstrapElection, which also
      // forwards server-internal operations as "root".
      proxiedUser = "root";
      if (forwardAsRootWarned.compareAndSet(false, true))
        LogManager.instance().log(this, Level.WARNING,
            "No authenticated user in the security context while forwarding a write to the leader (db=%s): "
                + "this is expected for embedded/in-process callers and the command is forwarded as 'root'. "
                + "This notice is logged only once.", getName());
    }
    builder.header("X-ArcadeDB-Forwarded-User", proxiedUser);

    try {
      final HttpResponse<String> response = HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200)
        throw reconstructLeaderException(response.statusCode(), response.body());

      return parseResultSetFromJson(response.body());
    } catch (final ArcadeDBException e) {
      throw e;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TransactionException("Interrupted while forwarding command to leader at " + leaderHttpAddress, e);
    } catch (final Exception e) {
      throw new TransactionException("Error forwarding command to leader at " + leaderHttpAddress, e);
    }
  }

  /**
   * Resolves the Raft leader address, polling for up to {@code timeoutMs} when none is known yet so a write
   * forwarded during a startup/leader-change election window is delayed rather than lost (issue #4728 follow-up).
   *
   * @param leaderAddressProbe supplier of the current leader address, or {@code null} when no leader is known
   * @param timeoutMs          maximum time to wait for a leader; {@code <= 0} disables waiting (fail-fast)
   * @param pollIntervalMs     poll cadence while waiting
   * @return the leader address, or {@code null} if none appeared within the timeout
   */
  static String awaitLeaderAddress(final Supplier<String> leaderAddressProbe, final long timeoutMs, final long pollIntervalMs) {
    String addr = leaderAddressProbe.get();
    if (addr != null || timeoutMs <= 0)
      return addr;

    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (addr == null) {
      final long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0)
        break;
      try {
        Thread.sleep(Math.min(pollIntervalMs, remaining));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      addr = leaderAddressProbe.get();
    }
    return addr;
  }

  /**
   * Parses the JSON response from the leader's /api/v1/command endpoint into a {@link ResultSet}.
   * Expected format: {@code {"result": [{...}, {...}, ...]}}
   */
  static ResultSet parseResultSetFromJson(final String json) {
    final JSONObject responseJson = new JSONObject(json);
    final InternalResultSet resultSet = new InternalResultSet();

    if (responseJson.has("result")) {
      final Object resultObj = responseJson.get("result");
      if (resultObj instanceof JSONArray resultArray) {
        for (int i = 0; i < resultArray.length(); i++) {
          final Object item = resultArray.get(i);
          if (item instanceof JSONObject jsonObj)
            resultSet.add(new ResultInternal(jsonObj.toMap()));
          else
            resultSet.add(new ResultInternal(Map.of("value", item)));
        }
      }
    }

    return resultSet;
  }

  /**
   * Parses the JSON error body returned by the leader and reconstructs the original exception so
   * the Follower throws the same type the Leader would have thrown locally. For example, a
   * {@link DuplicatedKeyException} is reconstructed with its index name, keys, and existing RID so
   * callers can catch it directly instead of having to inspect a generic
   * {@link TransactionException} message string. Other known types are reconstructed via
   * {@link #LEADER_EXCEPTION_FACTORIES} to keep their exact type (and retry semantics).
   * <p>
   * If the body is non-JSON, empty, or the exception class is not recognised, a generic
   * {@link TransactionException} wrapping the full response body is returned as a safe fallback.
   */
  static RuntimeException reconstructLeaderException(final int httpStatus, final String body) {
    final String message = "Leader returned HTTP " + httpStatus + " for forwarded command: " + body;
    String detail = null;
    String exceptionClass = null;
    String exceptionArgs = null;

    try {
      if (body != null && !body.isEmpty()) {
        final JSONObject json = new JSONObject(body);
        detail = json.getString("detail", json.getString("error", null));
        exceptionClass = json.getString("exception", null);
        exceptionArgs = json.getString("exceptionArgs", null);
      }
    } catch (final Exception ignored) {
      return new TransactionException(message);
    }

    if (exceptionClass == null)
      return new TransactionException(message);

    // ServerIsNotTheLeaderException carries the leader address as its second constructor argument (the HTTP
    // layer sends it as exceptionArgs), so it too is rebuilt explicitly. Without this arm a leader-side "I am
    // not the leader either" - the answer a node gives to a write forwarded onto it by a peer whose leader
    // address was wrong (issue #6191) - collapsed into a plain TransactionException, and the caller lost both
    // the retryability it inherits from NeedRetryException and the leader it names.
    if (ServerIsNotTheLeaderException.class.getName().equals(exceptionClass))
      return new ServerIsNotTheLeaderException(detail != null ? detail : message, exceptionArgs);

    // DuplicatedKeyException carries structured args (index name, keys, existing RID), so it is
    // reconstructed explicitly rather than from a plain message.
    if (DuplicatedKeyException.class.getName().equals(exceptionClass) && exceptionArgs != null) {
      final String[] parts = exceptionArgs.split("\\|", 3);
      if (parts.length == 3)
        try {
          return new DuplicatedKeyException(parts[0], parts[1], new RID(parts[2]));
        } catch (final Exception ignored) {
          // fall through if the RID token is malformed
        }
    }

    final Function<String, RuntimeException> factory = LEADER_EXCEPTION_FACTORIES.get(exceptionClass);
    if (factory != null)
      return factory.apply(detail != null ? detail : message);

    return new TransactionException(message);
  }
}
